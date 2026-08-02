use crate::{config, fsutil, paths};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const MAX_LOG_BYTES: u64 = 20 * 1024 * 1024;
pub const LOG_QUEUE_CAPACITY: usize = 4_096;
const ROTATED_LOG_RETENTION: usize = 5;

static STDERR_SUPPRESSION_DEPTH: AtomicUsize = AtomicUsize::new(0);
static LOG_WRITE_LOCK: Mutex<()> = Mutex::new(());
static LOG_WRITER: OnceLock<Option<LogWriter>> = OnceLock::new();
static LOG_RETENTION_INITIALIZED: OnceLock<()> = OnceLock::new();
static LOG_DIRECTORY_PERMISSION_WARNING_EMITTED: AtomicBool = AtomicBool::new(false);
static LOG_FILE_PERMISSION_WARNING_EMITTED: AtomicBool = AtomicBool::new(false);

pub const REDACT_KEYS: [&str; 14] = [
    "authorization",
    "access",
    "access_token",
    "refresh",
    "refresh_token",
    "id_token",
    "code",
    "code_verifier",
    "chatgpt-account-id",
    "cookie",
    "set-cookie",
    "x-api-key",
    "apikey",
    "api_key",
];

/// Additional sensitive fields that may occur inside provider payloads or structured log fields.
///
/// Keep the source list separate so callers that need the legacy header/query names can still
/// inspect `REDACT_KEYS`, but every recursive redaction path uses `is_sensitive_payload_key`.
/// Callers that need an identifier for correlation must log an explicit fingerprint instead of
/// relying on a raw account, user, or identity field.
pub(crate) const PAYLOAD_REDACT_KEYS: [&str; 15] = [
    "token",
    "bearer_token",
    "oauth_token",
    "oauth_access_token",
    "oauth_refresh_token",
    "client_secret",
    "secret",
    "password",
    "email",
    "user_id",
    "account_id",
    "identity",
    "identity_id",
    "subject",
    "sub",
];

pub(crate) fn is_sensitive_payload_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    REDACT_KEYS.contains(&normalized.as_str()) || PAYLOAD_REDACT_KEYS.contains(&normalized.as_str())
}

pub fn log_file() -> std::path::PathBuf {
    paths::log_file()
}

#[must_use]
pub struct StderrSuppressionGuard;

impl Drop for StderrSuppressionGuard {
    fn drop(&mut self) {
        STDERR_SUPPRESSION_DEPTH.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn suppress_stderr() -> StderrSuppressionGuard {
    STDERR_SUPPRESSION_DEPTH.fetch_add(1, Ordering::Relaxed);
    StderrSuppressionGuard
}

fn stderr_suppressed() -> bool {
    STDERR_SUPPRESSION_DEPTH.load(Ordering::Relaxed) > 0
}

fn should_mirror_to_stderr(level: &str, log_stderr: bool) -> bool {
    !stderr_suppressed() && (matches!(level, "warn" | "error") || log_stderr)
}

#[derive(Clone)]
pub struct Logger {
    service: String,
    base: serde_json::Map<String, Value>,
}

impl Logger {
    pub fn child(&self, bindings: serde_json::Map<String, Value>) -> Logger {
        let mut merged = self.base.clone();
        merged.extend(bindings);
        Logger {
            service: self.service.clone(),
            base: merged,
        }
    }

    pub fn debug(&self, msg: &str, fields: Option<serde_json::Map<String, Value>>) {
        self.emit("debug", msg, fields)
    }

    pub fn info(&self, msg: &str, fields: Option<serde_json::Map<String, Value>>) {
        self.emit("info", msg, fields)
    }

    pub fn warn(&self, msg: &str, fields: Option<serde_json::Map<String, Value>>) {
        self.emit("warn", msg, fields)
    }

    pub fn error(&self, msg: &str, fields: Option<serde_json::Map<String, Value>>) {
        self.emit("error", msg, fields)
    }

    fn emit(&self, level: &str, msg: &str, fields: Option<serde_json::Map<String, Value>>) {
        // Keep one coherent logging configuration snapshot for the whole
        // record. Loading it separately while recursively redacting fields can
        // otherwise turn every string value into a synchronous config read.
        let log_config = config::load_log_config();
        let mut body = serde_json::Map::new();
        body.insert("t".into(), Value::String(now_iso8601()));
        body.insert("level".into(), Value::String(level.to_string()));
        body.insert("service".into(), Value::String(self.service.clone()));
        body.insert("msg".into(), Value::String(msg.to_string()));
        body.insert(
            "configGeneration".into(),
            Value::Number(log_config.generation.into()),
        );

        let mut merged = self.base.clone();
        if let Some(fields) = fields {
            merged.extend(fields);
        }
        if !merged.is_empty() {
            body.insert(
                "fields".into(),
                redact_with_depth(Value::Object(merged), 0, log_config.verbose),
            );
        }

        let line = Value::Object(body).to_string();

        let mirror_to_stderr = should_mirror_to_stderr(level, log_config.stderr);
        if mirror_to_stderr {
            let _ = writeln!(io::stderr(), "{line}");
        }

        let _ = enqueue_log_line(line);
    }
}

pub fn create_logger(service: &str) -> Logger {
    Logger {
        service: service.to_string(),
        base: serde_json::Map::new(),
    }
}

#[derive(Debug)]
struct LogRecord {
    file: PathBuf,
    line: String,
}

enum LogCommand {
    Record(LogRecord),
    Flush(mpsc::Sender<io::Result<()>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueueResult {
    Enqueued,
    Dropped,
    Disconnected,
}

struct LogWriter {
    sender: SyncSender<LogCommand>,
    dropped: Arc<AtomicU64>,
}

impl LogWriter {
    fn spawn(capacity: usize) -> io::Result<Self> {
        Self::spawn_with_sink(capacity, write_log_line_to)
    }

    fn spawn_with_sink<F>(capacity: usize, mut sink: F) -> io::Result<Self>
    where
        F: FnMut(&Path, &str) -> io::Result<()> + Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_dropped = dropped.clone();
        std::thread::Builder::new()
            .name("ccproxy-log-writer".to_string())
            .spawn(move || run_log_writer(receiver, worker_dropped, capacity, &mut sink))?;
        Ok(Self { sender, dropped })
    }

    fn enqueue(&self, file: PathBuf, line: String) -> EnqueueResult {
        match self
            .sender
            .try_send(LogCommand::Record(LogRecord { file, line }))
        {
            Ok(()) => EnqueueResult::Enqueued,
            Err(TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                EnqueueResult::Dropped
            }
            Err(TrySendError::Disconnected(_)) => EnqueueResult::Disconnected,
        }
    }

    fn flush(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (ack_tx, ack_rx) = mpsc::channel();
        let mut command = LogCommand::Flush(ack_tx);
        loop {
            match self.sender.try_send(command) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    command = returned;
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(TrySendError::Disconnected(_)) => return false,
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        matches!(ack_rx.recv_timeout(remaining), Ok(Ok(())))
    }
}

fn run_log_writer<F>(
    receiver: Receiver<LogCommand>,
    dropped: Arc<AtomicU64>,
    capacity: usize,
    sink: &mut F,
) where
    F: FnMut(&Path, &str) -> io::Result<()>,
{
    let mut last_file = None;
    let mut write_error: Option<String> = None;
    while let Ok(command) = receiver.recv() {
        match command {
            LogCommand::Record(record) => {
                if let Err(error) = write_dropped_summary(&dropped, capacity, &record.file, sink) {
                    write_error.get_or_insert_with(|| error.to_string());
                }
                if let Err(error) = sink(&record.file, &record.line) {
                    write_error.get_or_insert_with(|| error.to_string());
                }
                last_file = Some(record.file);
            }
            LogCommand::Flush(ack) => {
                if let Some(file) = last_file.as_deref()
                    && let Err(error) = write_dropped_summary(&dropped, capacity, file, sink)
                {
                    write_error.get_or_insert_with(|| error.to_string());
                }
                let result = write_error
                    .as_ref()
                    .map_or_else(|| Ok(()), |error| Err(io::Error::other(error.clone())));
                let _ = ack.send(result);
            }
        }
    }
}

fn write_dropped_summary<F>(
    dropped: &AtomicU64,
    capacity: usize,
    file: &Path,
    sink: &mut F,
) -> io::Result<()>
where
    F: FnMut(&Path, &str) -> io::Result<()>,
{
    let count = dropped.load(Ordering::Acquire);
    if count == 0 {
        return Ok(());
    }
    let summary = serde_json::json!({
        "t": now_iso8601(),
        "level": "warn",
        "service": "logging",
        "msg": "log_records_dropped",
        "fields": {"count": count, "queueCapacity": capacity},
    })
    .to_string();
    sink(file, &summary)?;
    // Drops may race with the write. Subtract only the count represented by
    // this successful summary so later drops remain pending.
    dropped.fetch_sub(count, Ordering::AcqRel);
    Ok(())
}

fn enqueue_log_line(line: String) -> io::Result<()> {
    let file = log_file();
    LOG_RETENTION_INITIALIZED.get_or_init(|| {
        let _guard = LOG_WRITE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_rotated_logs(&file, ROTATED_LOG_RETENTION);
    });
    match LOG_WRITER.get_or_init(|| LogWriter::spawn(LOG_QUEUE_CAPACITY).ok()) {
        Some(writer) => match writer.enqueue(file, line) {
            EnqueueResult::Enqueued | EnqueueResult::Dropped => Ok(()),
            EnqueueResult::Disconnected => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "log writer thread stopped",
            )),
        },
        None => write_log_line_to(&file, &line),
    }
}

/// Wait until every log record queued before this call has been handled.
///
/// Returns `false` when the writer is unavailable, a sink write failed, or the
/// timeout elapses. New records emitted concurrently may remain queued after
/// this function returns.
pub fn flush(timeout: Duration) -> bool {
    match LOG_WRITER.get_or_init(|| LogWriter::spawn(LOG_QUEUE_CAPACITY).ok()) {
        Some(writer) => writer.flush(timeout),
        None => true,
    }
}

fn write_log_line_to(file: &Path, line: &str) -> io::Result<()> {
    let _guard = LOG_WRITE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(dir) = file.parent() {
        create_dir(dir, 0o700)?;
    }

    let existing_metadata = fs::metadata(file).ok();
    if existing_metadata.is_some()
        && let Err(error) = fsutil::set_mode_checked(file, 0o600)
    {
        warn_log_permission_error(&LOG_FILE_PERMISSION_WARNING_EMITTED, "log file", &error);
    }
    if existing_metadata.is_some_and(|meta| meta.len() > MAX_LOG_BYTES) {
        rotate_file(file)?;
    }

    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut out = options.open(file)?;
    if let Err(error) = fsutil::set_mode_checked(file, 0o600) {
        warn_log_permission_error(&LOG_FILE_PERMISSION_WARNING_EMITTED, "log file", &error);
    }
    let mut record = Vec::with_capacity(line.len() + 1);
    record.extend_from_slice(line.as_bytes());
    record.push(b'\n');
    out.write_all(&record)
}

fn rotate_file(path: &Path) -> io::Result<()> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    rotate_file_at(path, ts)?;
    Ok(())
}

fn rotate_file_at(path: &Path, timestamp: u128) -> io::Result<PathBuf> {
    let rotated = next_rotated_log_path(path, timestamp)?;
    fs::rename(path, &rotated)?;
    prune_rotated_logs(path, ROTATED_LOG_RETENTION);
    Ok(rotated)
}

fn next_rotated_log_path(path: &Path, timestamp: u128) -> io::Result<PathBuf> {
    let next_after_existing = rotated_log_files(path)
        .into_iter()
        .map(|(timestamp, _)| timestamp)
        .max()
        .map(|timestamp| {
            timestamp.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "rotated log timestamp space is exhausted",
                )
            })
        })
        .transpose()?;
    let mut timestamp = next_after_existing.map_or(timestamp, |next| next.max(timestamp));

    loop {
        let candidate = path.with_extension(timestamp.to_string());
        match fs::symlink_metadata(&candidate) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(candidate),
            Err(error) => return Err(error),
            Ok(_) => {
                timestamp = timestamp.checked_add(1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "rotated log timestamp space is exhausted",
                    )
                })?;
            }
        }
    }
}

fn prune_rotated_logs(path: &Path, retention: usize) {
    let mut files = rotated_log_files(path);
    files.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let remove = files.len().saturating_sub(retention);
    for (_, path) in files.into_iter().take(remove) {
        let _ = fs::remove_file(path);
    }
}

fn rotated_log_files(path: &Path) -> Vec<(u128, PathBuf)> {
    let Some(stem) = path.file_stem() else {
        return Vec::new();
    };
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let Ok(entries) = fs::read_dir(parent) else {
        return Vec::new();
    };

    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let candidate = entry.path();
            if candidate == path || candidate.file_stem() != Some(stem) {
                return None;
            }
            let suffix = candidate.extension()?.to_str()?;
            if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            let timestamp = suffix.parse::<u128>().ok()?;
            let metadata = fs::symlink_metadata(&candidate).ok()?;
            metadata
                .file_type()
                .is_file()
                .then_some((timestamp, candidate))
        })
        .collect()
}

fn create_dir(path: &Path, mode: u32) -> io::Result<()> {
    match fsutil::create_dir_all_with_mode(path, mode) {
        Ok(()) => Ok(()),
        // Permission tightening is best-effort for ordinary redacted logs.
        // If the directory is usable, surface the failure without stopping
        // the logging worker or the proxy.
        Err(error) if path.is_dir() => {
            warn_log_permission_error(
                &LOG_DIRECTORY_PERMISSION_WARNING_EMITTED,
                "log directory",
                &error,
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn warn_log_permission_error(emitted: &AtomicBool, target: &str, error: &io::Error) {
    if emitted
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        eprintln!("{}", log_permission_warning_message(target, error));
    }
}

fn log_permission_warning_message(target: &str, error: &io::Error) -> String {
    format!(
        "ccproxy warning: could not restrict {target} permissions (error kind: {:?})",
        error.kind()
    )
}

/// Return a bounded error category suitable for ordinary structured logs.
///
/// Persistence errors can contain credential paths or backend-specific text in their Display
/// representation. Callers that only need an operational category should log this value instead
/// of the raw error.
pub fn safe_persistence_error_kind(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<io::Error>() {
        return match error.kind() {
            io::ErrorKind::NotFound => "not_found",
            io::ErrorKind::PermissionDenied => "permission_denied",
            io::ErrorKind::AlreadyExists => "already_exists",
            io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => "invalid_data",
            io::ErrorKind::WriteZero => "write_zero",
            io::ErrorKind::OutOfMemory => "out_of_memory",
            _ => "io",
        };
    }
    if error.downcast_ref::<serde_json::Error>().is_some() {
        "serialization"
    } else {
        "storage"
    }
}

fn now_iso8601() -> String {
    let now = time::OffsetDateTime::now_utc();
    let format = time::format_description::parse_borrowed::<3>(
        "[year]-[month]-[day]T[hour]:[minute]:[second]Z",
    )
    .unwrap();
    now.format(&format).unwrap_or_else(|_| String::new())
}

pub fn redact_value(value: Value) -> Value {
    let verbose = config::log_verbose();
    redact_with_depth(value, 0, verbose)
}

fn redact_with_depth(value: Value, depth: u8, verbose: bool) -> Value {
    if depth > 6 {
        return Value::String("[depth-limit]".into());
    }

    match value {
        Value::String(s) => {
            if verbose {
                Value::String(s)
            } else {
                Value::String(truncate_log_string(s))
            }
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|v| redact_with_depth(v, depth + 1, verbose))
                .collect(),
        ),
        Value::Object(fields) => {
            let mut out = serde_json::Map::new();
            for (key, value) in fields {
                if is_sensitive_payload_key(&key) {
                    out.insert(key, redact_key_redaction(value));
                } else {
                    out.insert(key, redact_with_depth(value, depth + 1, verbose));
                }
            }
            Value::Object(out)
        }
        value => value,
    }
}

fn truncate_log_string(value: String) -> String {
    if value.len() <= 4000 {
        return value;
    }

    let end = floor_char_boundary(&value, 4000);
    format!("{}…[{} more]", &value[..end], value.len() - end)
}

fn floor_char_boundary(value: &str, max_bytes: usize) -> usize {
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    end
}

fn redact_key_redaction(value: Value) -> Value {
    match value {
        Value::String(s) => Value::String(format!("[redacted len={}]", s.len())),
        _ => Value::String("[redacted]".to_string()),
    }
}

pub fn redacted_keys() -> HashSet<&'static str> {
    REDACT_KEYS
        .iter()
        .chain(PAYLOAD_REDACT_KEYS.iter())
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier, Mutex};

    static STDERR_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn stderr_suppression_disables_level_mirroring() {
        let _lock = STDERR_TEST_LOCK.lock().unwrap();
        assert!(should_mirror_to_stderr("warn", false));

        {
            let _guard = suppress_stderr();
            assert!(!should_mirror_to_stderr("warn", false));
            assert!(!should_mirror_to_stderr("error", true));
        }

        assert!(should_mirror_to_stderr("warn", false));
    }

    #[test]
    fn stderr_suppression_supports_nested_guards() {
        let _lock = STDERR_TEST_LOCK.lock().unwrap();
        let outer = suppress_stderr();
        let inner = suppress_stderr();
        assert!(!should_mirror_to_stderr("warn", false));

        drop(inner);
        assert!(!should_mirror_to_stderr("warn", false));

        drop(outer);
        assert!(should_mirror_to_stderr("warn", false));
    }

    #[test]
    fn stderr_snapshot_controls_non_warning_mirroring() {
        let _lock = STDERR_TEST_LOCK.lock().unwrap();
        assert!(!should_mirror_to_stderr("info", false));
        assert!(should_mirror_to_stderr("info", true));
    }

    #[test]
    fn verbose_snapshot_applies_to_all_nested_strings() {
        let long = "x".repeat(4_001);
        let value = serde_json::json!({
            "outer": [{
                "payload": long,
                "authorization": "secret",
                "client_secret": "also-secret",
                "Email": "private@example.test",
            }],
        });

        let concise = redact_with_depth(value.clone(), 0, false);
        let concise_payload = concise["outer"][0]["payload"].as_str().unwrap();
        assert!(concise_payload.ends_with("…[1 more]"));
        assert_eq!(concise["outer"][0]["authorization"], "[redacted len=6]");
        assert_eq!(concise["outer"][0]["client_secret"], "[redacted len=11]");
        assert_eq!(concise["outer"][0]["Email"], "[redacted len=20]");

        let verbose = redact_with_depth(value, 0, true);
        assert_eq!(verbose["outer"][0]["payload"], long);
        assert_eq!(verbose["outer"][0]["authorization"], "[redacted len=6]");
        assert_eq!(verbose["outer"][0]["client_secret"], "[redacted len=11]");
        assert_eq!(verbose["outer"][0]["Email"], "[redacted len=20]");
    }

    #[test]
    fn truncation_preserves_utf8_boundaries() {
        for prefix_len in 3995..=4005 {
            let value = format!("{}😊handled", "a".repeat(prefix_len));
            let end = floor_char_boundary(&value, 4000);
            assert!(value.is_char_boundary(end));
            assert!(end <= 4000);
            assert!(4000 - end < 4);

            let text = truncate_log_string(value);
            assert!(text.contains("…["));
            assert!(serde_json::to_string(&text).is_ok());
        }
    }

    #[test]
    fn persistence_errors_are_reduced_to_bounded_categories() {
        let permission = anyhow::Error::from(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "/home/alice/private/auth.json",
        ));
        let malformed =
            anyhow::Error::from(serde_json::from_str::<serde_json::Value>("{").unwrap_err());
        let backend = anyhow::anyhow!("Keychain failed at /Users/alice/Library/Keychains");

        assert_eq!(
            safe_persistence_error_kind(&permission),
            "permission_denied"
        );
        assert_eq!(safe_persistence_error_kind(&malformed), "serialization");
        assert_eq!(safe_persistence_error_kind(&backend), "storage");
    }

    #[test]
    fn permission_warning_omits_dynamic_path_and_error_text() {
        let error = io::Error::new(
            io::ErrorKind::PermissionDenied,
            "/home/alice/private/proxy.log",
        );
        let warning = log_permission_warning_message("log file", &error);

        assert_eq!(
            warning,
            "ccproxy warning: could not restrict log file permissions (error kind: PermissionDenied)"
        );
        assert!(!warning.contains("alice"));
        assert!(!warning.contains("proxy.log"));
    }

    #[test]
    fn bounded_writer_drops_without_blocking_and_flushes_a_summary() {
        let (sink_started_tx, sink_started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink_observed = observed.clone();
        let mut first = true;
        let writer = LogWriter::spawn_with_sink(1, move |_file, line| {
            if first {
                first = false;
                let _ = sink_started_tx.send(());
                let _ = release_rx.recv();
            }
            sink_observed.lock().unwrap().push(line.to_string());
            Ok(())
        })
        .unwrap();
        let file = PathBuf::from("proxy.log");

        assert_eq!(
            writer.enqueue(file.clone(), "first".to_string()),
            EnqueueResult::Enqueued
        );
        sink_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            writer.enqueue(file.clone(), "second".to_string()),
            EnqueueResult::Enqueued
        );
        assert_eq!(
            writer.enqueue(file, "third".to_string()),
            EnqueueResult::Dropped
        );

        release_tx.send(()).unwrap();
        assert!(writer.flush(Duration::from_secs(1)));
        let observed = observed.lock().unwrap();
        assert_eq!(observed.first().map(String::as_str), Some("first"));
        assert_eq!(observed.last().map(String::as_str), Some("second"));
        let summary: Value = serde_json::from_str(&observed[1]).unwrap();
        assert_eq!(summary["msg"], "log_records_dropped");
        assert_eq!(summary["fields"]["count"], 1);
        assert_eq!(summary["fields"]["queueCapacity"], 1);
    }

    #[test]
    fn writer_flush_waits_for_all_preceding_records() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink_observed = observed.clone();
        let writer = LogWriter::spawn_with_sink(8, move |_file, line| {
            sink_observed.lock().unwrap().push(line.to_string());
            Ok(())
        })
        .unwrap();
        let file = PathBuf::from("proxy.log");
        for index in 0..4 {
            assert_eq!(
                writer.enqueue(file.clone(), format!("record-{index}")),
                EnqueueResult::Enqueued
            );
        }

        assert!(writer.flush(Duration::from_secs(1)));
        assert_eq!(
            *observed.lock().unwrap(),
            ["record-0", "record-1", "record-2", "record-3"]
        );
    }

    #[test]
    fn writer_flush_reports_sink_failure() {
        let writer = LogWriter::spawn_with_sink(8, |_file, _line| {
            Err(io::Error::other("simulated disk failure"))
        })
        .unwrap();
        assert_eq!(
            writer.enqueue(PathBuf::from("proxy.log"), "record".to_string()),
            EnqueueResult::Enqueued
        );
        assert!(!writer.flush(Duration::from_secs(1)));
    }

    #[test]
    fn failed_drop_summary_keeps_the_count_pending() {
        let dropped = AtomicU64::new(3);
        let mut failing_sink =
            |_file: &Path, _line: &str| Err(io::Error::other("simulated disk failure"));
        assert!(
            write_dropped_summary(&dropped, 8, Path::new("proxy.log"), &mut failing_sink).is_err()
        );
        assert_eq!(dropped.load(Ordering::Acquire), 3);

        let mut successful_sink = |_file: &Path, _line: &str| Ok(());
        write_dropped_summary(&dropped, 8, Path::new("proxy.log"), &mut successful_sink).unwrap();
        assert_eq!(dropped.load(Ordering::Acquire), 0);
    }

    #[test]
    fn rotated_log_cleanup_keeps_only_matching_newest_files() {
        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join("proxy.log");
        fs::write(&current, b"current").unwrap();
        for timestamp in 1..=7_u128 {
            fs::write(
                current.with_extension(timestamp.to_string()),
                timestamp.to_string(),
            )
            .unwrap();
        }
        let backup = temp.path().join("proxy.backup");
        let extra_extension = temp.path().join("proxy.8.tmp");
        let other_log = temp.path().join("other.1");
        let matching_directory = temp.path().join("proxy.0");
        fs::write(&backup, b"backup").unwrap();
        fs::write(&extra_extension, b"extra").unwrap();
        fs::write(&other_log, b"other").unwrap();
        fs::create_dir(&matching_directory).unwrap();

        prune_rotated_logs(&current, ROTATED_LOG_RETENTION);

        assert_eq!(fs::read(&current).unwrap(), b"current");
        for timestamp in 1..=2_u128 {
            assert!(!current.with_extension(timestamp.to_string()).exists());
        }
        for timestamp in 3..=7_u128 {
            assert!(current.with_extension(timestamp.to_string()).is_file());
        }
        assert!(backup.is_file());
        assert!(extra_extension.is_file());
        assert!(other_log.is_file());
        assert!(matching_directory.is_dir());
    }

    #[test]
    fn consecutive_rotations_are_unique_and_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join("proxy.log");

        for sequence in 0..7_u128 {
            fs::write(&current, sequence.to_string()).unwrap();
            let rotated = rotate_file_at(&current, 1_000).unwrap();
            assert!(rotated.is_file());
            assert!(!current.exists());
        }
        fs::write(&current, b"current").unwrap();

        let mut rotated = rotated_log_files(&current);
        rotated.sort_by_key(|(timestamp, _)| *timestamp);
        assert_eq!(
            rotated
                .iter()
                .map(|(timestamp, _)| *timestamp)
                .collect::<Vec<_>>(),
            vec![1_002, 1_003, 1_004, 1_005, 1_006]
        );
        assert_eq!(rotated.len(), ROTATED_LOG_RETENTION);
        for (sequence, (_, path)) in (2_u128..=6).zip(rotated) {
            assert_eq!(fs::read_to_string(path).unwrap(), sequence.to_string());
        }
        assert_eq!(fs::read(&current).unwrap(), b"current");
    }

    #[cfg(unix)]
    #[test]
    fn rotated_log_cleanup_never_follows_a_numeric_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join("proxy.log");
        let target = temp.path().join("must-survive");
        let link = current.with_extension("1");
        fs::write(&target, b"outside rotation set").unwrap();
        symlink(&target, &link).unwrap();

        prune_rotated_logs(&current, 0);

        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&target).unwrap(), b"outside rotation set");
    }

    #[cfg(unix)]
    #[test]
    fn log_writer_tightens_directory_and_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("logs");
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
        let file = directory.join("proxy.log");
        fs::write(&file, b"existing\n").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();

        write_log_line_to(&file, r#"{"msg":"redacted"}"#).unwrap();

        assert_eq!(
            fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn log_rotation_makes_existing_artifact_private_before_rename() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("proxy.log");
        let existing = fs::File::create(&file).unwrap();
        existing.set_len(MAX_LOG_BYTES + 1).unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();

        write_log_line_to(&file, r#"{"msg":"after rotation"}"#).unwrap();

        let rotated = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .find(|path| path != &file)
            .expect("oversized log should be rotated");
        assert_eq!(
            fs::metadata(rotated).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn concurrent_writes_preserve_complete_jsonl_records() {
        const THREADS: usize = 12;
        const RECORDS_PER_THREAD: usize = 80;

        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("proxy.log");
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut writers = Vec::new();

        for thread in 0..THREADS {
            let file = file.clone();
            let barrier = barrier.clone();
            writers.push(std::thread::spawn(move || {
                barrier.wait();
                for sequence in 0..RECORDS_PER_THREAD {
                    let record = serde_json::json!({
                        "thread": thread,
                        "sequence": sequence,
                        "payload": "x".repeat(2_048),
                    })
                    .to_string();
                    write_log_line_to(&file, &record).unwrap();
                }
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }

        let contents = fs::read_to_string(file).unwrap();
        assert!(contents.ends_with('\n'));
        let mut observed = HashSet::new();
        for line in contents.lines() {
            let record: Value = serde_json::from_str(line).unwrap();
            observed.insert((
                record["thread"].as_u64().unwrap(),
                record["sequence"].as_u64().unwrap(),
            ));
        }
        assert_eq!(observed.len(), THREADS * RECORDS_PER_THREAD);
        assert_eq!(contents.lines().count(), THREADS * RECORDS_PER_THREAD);
    }
}
