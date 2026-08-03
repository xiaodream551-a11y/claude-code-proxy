use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Cross-process guard for OAuth credential mutations.
///
/// The lock file is deliberately separate from the credential backend. That
/// lets Keychain-backed stores coordinate through a small file without
/// exposing credentials on disk.
pub struct AuthMutationLock {
    _file: Option<File>,
}

enum TryAcquireMutationLock {
    Acquired(Option<File>),
    Busy,
}

/// Leaves room for a competing process to finish its bounded token POST and
/// persist the rotation, while remaining well below both providers' default
/// 540-second request wall-clock budget.
pub const DEFAULT_AUTH_MUTATION_LOCK_WAIT: Duration = Duration::from_secs(45);
const AUTH_MUTATION_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(25);

pub fn auth_mutation_lock_wait_timeout(request_total_budget: Duration) -> Duration {
    DEFAULT_AUTH_MUTATION_LOCK_WAIT.min(request_total_budget)
}

impl AuthMutationLock {
    fn open(coordination_path: Option<&Path>) -> io::Result<Option<File>> {
        let Some(coordination_path) = coordination_path else {
            return Ok(None);
        };
        let path = mutation_lock_path(coordination_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map(Some)
    }

    pub fn acquire(coordination_path: Option<&Path>) -> io::Result<Self> {
        Self::acquire_with_timeout(coordination_path, DEFAULT_AUTH_MUTATION_LOCK_WAIT)
    }

    pub fn acquire_with_timeout(
        coordination_path: Option<&Path>,
        timeout: Duration,
    ) -> io::Result<Self> {
        let mut attempt = AuthLockAttemptLog::new(timeout);
        loop {
            match Self::try_acquire(coordination_path) {
                Ok(TryAcquireMutationLock::Acquired(file)) => {
                    attempt.finish("acquired", None);
                    return Ok(Self { _file: file });
                }
                Ok(TryAcquireMutationLock::Busy) => {
                    attempt.mark_contended();
                    let elapsed = attempt.elapsed();
                    if elapsed >= timeout {
                        return Err(attempt.timeout_error());
                    }
                    std::thread::sleep(
                        AUTH_MUTATION_LOCK_POLL_INTERVAL.min(timeout.saturating_sub(elapsed)),
                    );
                }
                Err(error) => {
                    attempt.finish("error", Some(auth_lock_error_kind(&error)));
                    return Err(error);
                }
            }
        }
    }

    pub async fn acquire_async(coordination_path: Option<&Path>) -> io::Result<Self> {
        Self::acquire_async_with_timeout(coordination_path, DEFAULT_AUTH_MUTATION_LOCK_WAIT).await
    }

    pub async fn acquire_async_with_timeout(
        coordination_path: Option<&Path>,
        timeout: Duration,
    ) -> io::Result<Self> {
        let mut attempt = AuthLockAttemptLog::new(timeout);
        loop {
            match Self::try_acquire(coordination_path) {
                Ok(TryAcquireMutationLock::Acquired(file)) => {
                    attempt.finish("acquired", None);
                    return Ok(Self { _file: file });
                }
                Ok(TryAcquireMutationLock::Busy) => {
                    attempt.mark_contended();
                    let elapsed = attempt.elapsed();
                    if elapsed >= timeout {
                        return Err(attempt.timeout_error());
                    }
                    tokio::time::sleep(
                        AUTH_MUTATION_LOCK_POLL_INTERVAL.min(timeout.saturating_sub(elapsed)),
                    )
                    .await;
                }
                Err(error) => {
                    attempt.finish("error", Some(auth_lock_error_kind(&error)));
                    return Err(error);
                }
            }
        }
    }

    fn try_acquire(coordination_path: Option<&Path>) -> io::Result<TryAcquireMutationLock> {
        let file = Self::open(coordination_path)?;
        match file.as_ref().map(FileExt::try_lock_exclusive) {
            Some(Err(error)) if is_exclusive_lock_busy(&error) => {
                drop(file);
                Ok(TryAcquireMutationLock::Busy)
            }
            Some(Err(error)) => Err(error),
            Some(Ok(())) | None => Ok(TryAcquireMutationLock::Acquired(file)),
        }
    }
}

struct AuthLockAttemptLog {
    started_at: Instant,
    timeout: Duration,
    contended: bool,
    finished: bool,
}

impl AuthLockAttemptLog {
    fn new(timeout: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            timeout,
            contended: false,
            finished: false,
        }
    }

    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    fn mark_contended(&mut self) {
        if self.contended {
            return;
        }
        self.contended = true;
        crate::logging::create_logger("oauth").info(
            "auth_lock_wait",
            Some(serde_json::Map::from_iter([(
                "timeoutMs".to_string(),
                serde_json::json!(duration_ms(self.timeout)),
            )])),
        );
    }

    fn finish(&mut self, outcome: &'static str, error_kind: Option<&'static str>) {
        log_auth_lock_outcome(
            outcome,
            self.elapsed(),
            self.timeout,
            self.contended,
            error_kind,
        );
        self.finished = true;
    }

    fn timeout_error(&mut self) -> io::Error {
        self.finish("timeout", Some("timed_out"));
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "timed out after {}ms waiting for the OAuth credential mutation lock",
                duration_ms(self.timeout)
            ),
        )
    }
}

impl Drop for AuthLockAttemptLog {
    fn drop(&mut self) {
        if self.contended && !self.finished {
            self.finish("cancelled", Some("cancelled"));
        }
    }
}

fn log_auth_lock_outcome(
    outcome: &'static str,
    waited: Duration,
    timeout: Duration,
    contended: bool,
    error_kind: Option<&'static str>,
) {
    let mut fields = serde_json::Map::from_iter([
        ("outcome".to_string(), serde_json::json!(outcome)),
        ("waitMs".to_string(), serde_json::json!(duration_ms(waited))),
        (
            "timeoutMs".to_string(),
            serde_json::json!(duration_ms(timeout)),
        ),
        ("contended".to_string(), serde_json::json!(contended)),
    ]);
    if let Some(error_kind) = error_kind {
        fields.insert("errorKind".to_string(), serde_json::json!(error_kind));
    }
    crate::logging::create_logger("oauth").info("auth_lock_outcome", Some(fields));
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn auth_lock_error_kind(error: &io::Error) -> &'static str {
    match error.kind() {
        io::ErrorKind::NotFound => "not_found",
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::WouldBlock => "would_block",
        io::ErrorKind::TimedOut => "timed_out",
        io::ErrorKind::Interrupted => "interrupted",
        _ => "other",
    }
}

fn is_exclusive_lock_busy(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::WouldBlock {
        return true;
    }
    // Windows: ERROR_SHARING_VIOLATION (32) and ERROR_LOCK_VIOLATION (33) are
    // the normal contended outcomes for try_lock_exclusive. Treating them as
    // hard failures races concurrent OAuth refreshes that share one lock file.
    matches!(error.raw_os_error(), Some(32 | 33))
}

#[derive(Serialize, Deserialize)]
struct RefreshPendingMarker {
    auth_generation_sha256: String,
}

pub fn generation_fingerprint<T: Serialize>(value: &T) -> anyhow::Result<[u8; 32]> {
    let encoded = serde_json::to_vec(value)?;
    Ok(Sha256::digest(encoded).into())
}

pub fn write_refresh_pending(
    coordination_path: Option<&Path>,
    generation: [u8; 32],
) -> anyhow::Result<()> {
    let Some(path) = coordination_path.map(refresh_pending_path) else {
        return Ok(());
    };
    crate::auth::write_atomically(
        path.to_string_lossy().as_ref(),
        &RefreshPendingMarker {
            auth_generation_sha256: hex::encode(generation),
        },
    )
}

pub fn read_refresh_pending(coordination_path: Option<&Path>) -> anyhow::Result<Option<[u8; 32]>> {
    let Some(path) = coordination_path.map(refresh_pending_path) else {
        return Ok(None);
    };
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let marker: RefreshPendingMarker = serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("OAuth refresh pending marker is invalid: {error}"))?;
    let decoded = hex::decode(marker.auth_generation_sha256)
        .map_err(|error| anyhow::anyhow!("OAuth refresh pending marker is invalid: {error}"))?;
    decoded
        .try_into()
        .map(Some)
        .map_err(|_| anyhow::anyhow!("OAuth refresh pending marker has an invalid fingerprint"))
}

pub fn clear_refresh_pending(coordination_path: Option<&Path>) -> anyhow::Result<()> {
    let Some(path) = coordination_path.map(refresh_pending_path) else {
        return Ok(());
    };
    match std::fs::remove_file(&path) {
        Ok(()) => {
            #[cfg(unix)]
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn mutation_lock_path(coordination_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.refresh.lock", coordination_path.display()))
}

pub fn refresh_pending_path(coordination_path: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}.refresh-pending.json",
        coordination_path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_lock_busy_recognizes_windows_contention_codes() {
        assert!(is_exclusive_lock_busy(&io::Error::new(
            io::ErrorKind::WouldBlock,
            "busy",
        )));
        assert!(is_exclusive_lock_busy(&io::Error::from_raw_os_error(32)));
        assert!(is_exclusive_lock_busy(&io::Error::from_raw_os_error(33)));
        assert!(!is_exclusive_lock_busy(&io::Error::from_raw_os_error(5)));
    }

    #[test]
    fn auth_lock_wait_never_exceeds_the_request_budget() {
        assert_eq!(
            auth_mutation_lock_wait_timeout(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        assert_eq!(
            auth_mutation_lock_wait_timeout(Duration::from_secs(540)),
            DEFAULT_AUTH_MUTATION_LOCK_WAIT
        );
    }

    #[test]
    fn pending_marker_roundtrips_and_clears() {
        let temp = tempfile::TempDir::new().unwrap();
        let auth_path = temp.path().join("codex/auth.json");
        let generation = [7_u8; 32];

        write_refresh_pending(Some(&auth_path), generation).unwrap();
        assert_eq!(
            read_refresh_pending(Some(&auth_path)).unwrap(),
            Some(generation)
        );
        clear_refresh_pending(Some(&auth_path)).unwrap();
        assert_eq!(read_refresh_pending(Some(&auth_path)).unwrap(), None);
    }

    #[test]
    fn blocking_lock_timeout_is_bounded_and_recovers_after_release() {
        let temp = tempfile::TempDir::new().unwrap();
        let auth_path = temp.path().join("blocking-private-session/auth.json");
        let held = AuthMutationLock::acquire(Some(&auth_path)).unwrap();

        let started_at = Instant::now();
        let error = match AuthMutationLock::acquire_with_timeout(
            Some(&auth_path),
            Duration::from_millis(75),
        ) {
            Ok(_) => panic!("the blocking lock must not wait forever"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started_at.elapsed() >= Duration::from_millis(50));
        assert!(started_at.elapsed() < Duration::from_secs(2));
        assert!(!error.to_string().contains("blocking-private-session"));

        drop(held);
        AuthMutationLock::acquire_with_timeout(Some(&auth_path), Duration::from_millis(500))
            .expect("the released blocking lock should be acquirable");
    }

    #[tokio::test]
    async fn async_lock_timeout_is_bounded_and_recovers_after_release() {
        let temp = tempfile::TempDir::new().unwrap();
        let auth_path = temp.path().join("private-session/auth.json");
        let held = AuthMutationLock::acquire(Some(&auth_path)).unwrap();

        let started_at = Instant::now();
        let error = match AuthMutationLock::acquire_async_with_timeout(
            Some(&auth_path),
            Duration::from_millis(75),
        )
        .await
        {
            Ok(_) => panic!("a separately opened descriptor must not wait forever"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            started_at.elapsed() >= Duration::from_millis(50),
            "the timeout must not fail before the configured wait"
        );
        assert!(
            started_at.elapsed() < Duration::from_secs(2),
            "the bounded lock wait should fail promptly"
        );
        assert!(!error.to_string().contains("private-session"));
        assert!(crate::logging::flush(Duration::from_secs(1)));
        let log = std::fs::read_to_string(crate::logging::log_file()).unwrap();
        let records = log
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|record| {
                record["service"] == "oauth"
                    && record
                        .pointer("/fields/timeoutMs")
                        .and_then(serde_json::Value::as_u64)
                        == Some(75)
            })
            .collect::<Vec<_>>();
        assert!(
            records
                .iter()
                .any(|record| record["msg"] == "auth_lock_wait")
        );
        assert!(records.iter().any(|record| {
            record["msg"] == "auth_lock_outcome"
                && record["fields"]["outcome"] == "timeout"
                && record["fields"]["errorKind"] == "timed_out"
        }));
        assert!(
            records
                .iter()
                .all(|record| !record.to_string().contains("private-session")),
            "auth lock logs must not include the coordination path"
        );

        drop(held);
        tokio::time::timeout(
            Duration::from_secs(1),
            AuthMutationLock::acquire_async_with_timeout(
                Some(&auth_path),
                Duration::from_millis(500),
            ),
        )
        .await
        .expect("the released lock should become available promptly")
        .expect("the released lock should be acquirable");
    }

    #[tokio::test]
    async fn cancelled_async_lock_wait_records_one_terminal_outcome() {
        let temp = tempfile::TempDir::new().unwrap();
        let auth_path = temp.path().join("cancelled-private-session/auth.json");
        let held = AuthMutationLock::acquire(Some(&auth_path)).unwrap();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(60),
                AuthMutationLock::acquire_async_with_timeout(
                    Some(&auth_path),
                    Duration::from_millis(12_345),
                ),
            )
            .await
            .is_err(),
            "the synthetic outer request deadline should cancel the lock wait"
        );
        assert!(crate::logging::flush(Duration::from_secs(1)));
        let log = std::fs::read_to_string(crate::logging::log_file()).unwrap();
        let records = log
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|record| {
                record["service"] == "oauth"
                    && record
                        .pointer("/fields/timeoutMs")
                        .and_then(serde_json::Value::as_u64)
                        == Some(12_345)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            records
                .iter()
                .filter(|record| record["msg"] == "auth_lock_wait")
                .count(),
            1
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| record["msg"] == "auth_lock_outcome")
                .count(),
            1
        );
        assert!(records.iter().any(|record| {
            record["fields"]["outcome"] == "cancelled"
                && record["fields"]["errorKind"] == "cancelled"
        }));
        assert!(
            records
                .iter()
                .all(|record| !record.to_string().contains("cancelled-private-session")),
            "cancelled auth lock logs must not include the coordination path"
        );

        drop(held);
    }
}
