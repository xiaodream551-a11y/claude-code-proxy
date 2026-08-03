use crate::{
    anthropic::json_error,
    logging::{Logger, create_logger, is_sensitive_payload_key},
    monitor::{EndpointKind, MonitorHandle},
    project,
    provider::{Provider, RequestByteLease, RequestContext, RequestLaneKey},
    registry::{Registry, normalize_incoming_model},
    session,
    timeutil::now_ms,
    traffic::{TrafficCaptureOptions, create_traffic_capture_async, traffic_session_fingerprint},
};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    response::Response,
    routing::{get, post},
};
use bytes::Bytes;
use http_body_util::{BodyExt, StreamBody};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_MAX_BUFFERED_REQUEST_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 64;
pub const DEFAULT_MAX_CONCURRENT_PER_PROVIDER: usize = 48;
pub const DEFAULT_MAX_CONCURRENT_PER_SESSION: usize = 24;
pub const DEFAULT_REQUEST_BODY_IDLE_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_REQUEST_BODY_TOTAL_TIMEOUT_MS: u64 = 30_000;
pub const MAX_SESSION_ID_BYTES: usize = 256;
pub const MAX_AGENT_ID_BYTES: usize = 256;
const MAX_ERROR_RESPONSE_BODY_BYTES: usize = 64 * 1024;
const ERROR_RESPONSE_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const ERROR_RESPONSE_BODY_TOTAL_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ERROR_CAPTURE_FILES: usize = 128;
const MAX_ERROR_REDACTION_DEPTH: u16 = 100;
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_MS: u64 = 600_000;
const COMPACTION_MODEL_HEADER: &str = "x-ccproxy-compaction-model";
const CLAUDE_CODE_SESSION_ID_HEADER: &str = "x-claude-code-session-id";
const CLAUDE_CODE_AGENT_ID_HEADER: &str = "x-claude-code-agent-id";
const CLAUDE_CODE_PARENT_AGENT_ID_HEADER: &str = "x-claude-code-parent-agent-id";
const REQUEST_LANE_KEY_DOMAIN: &[u8] = b"ccproxy-request-lane-key-v1\0";
const INCOMPLETE_SSE_ERROR: &str = "SSE response ended before message_stop";
const MAX_TRACKED_CLIENT_TOOL_RESULTS: usize = 256;
const MAX_TRACKED_TOOL_BLOCK_STARTS: usize = 256;
const MAX_PENDING_TOOL_LIFECYCLE_EVENTS: usize = MAX_TRACKED_TOOL_BLOCK_STARTS * 2;

fn compaction_model_override(
    headers: &HeaderMap,
    request: &crate::anthropic::schema::MessagesRequest,
) -> Option<String> {
    if !crate::providers::translate_shared::is_claude_code_compaction_request(request) {
        return None;
    }
    headers
        .get(COMPACTION_MODEL_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_string)
}

#[derive(Debug, Clone)]
pub struct ServerLimits {
    pub max_request_body_bytes: usize,
    pub max_buffered_request_bytes: usize,
    pub max_concurrent_requests: usize,
    pub max_concurrent_per_provider: usize,
    pub max_concurrent_per_session: usize,
    pub request_body_idle_timeout: Duration,
    pub request_body_total_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
}

impl ServerLimits {
    pub fn configured() -> Self {
        Self {
            max_request_body_bytes: crate::config::max_request_body_bytes(
                DEFAULT_MAX_REQUEST_BODY_BYTES,
            ),
            max_buffered_request_bytes: crate::config::max_buffered_request_bytes(
                DEFAULT_MAX_BUFFERED_REQUEST_BYTES,
            ),
            max_concurrent_requests: crate::config::max_concurrent_requests(
                DEFAULT_MAX_CONCURRENT_REQUESTS,
            ),
            max_concurrent_per_provider: crate::config::max_concurrent_per_provider(
                DEFAULT_MAX_CONCURRENT_PER_PROVIDER,
            ),
            max_concurrent_per_session: crate::config::max_concurrent_per_session(
                DEFAULT_MAX_CONCURRENT_PER_SESSION,
            ),
            request_body_idle_timeout: Duration::from_millis(
                crate::config::request_body_idle_timeout_ms(DEFAULT_REQUEST_BODY_IDLE_TIMEOUT_MS),
            ),
            request_body_total_timeout: Duration::from_millis(
                crate::config::request_body_total_timeout_ms(DEFAULT_REQUEST_BODY_TOTAL_TIMEOUT_MS),
            ),
            graceful_shutdown_timeout: Duration::from_millis(
                crate::config::graceful_shutdown_timeout_ms(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_MS),
            ),
        }
    }
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_buffered_request_bytes: DEFAULT_MAX_BUFFERED_REQUEST_BYTES,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            max_concurrent_per_provider: DEFAULT_MAX_CONCURRENT_PER_PROVIDER,
            max_concurrent_per_session: DEFAULT_MAX_CONCURRENT_PER_SESSION,
            request_body_idle_timeout: Duration::from_millis(DEFAULT_REQUEST_BODY_IDLE_TIMEOUT_MS),
            request_body_total_timeout: Duration::from_millis(
                DEFAULT_REQUEST_BODY_TOTAL_TIMEOUT_MS,
            ),
            graceful_shutdown_timeout: Duration::from_millis(DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_MS),
        }
    }
}

pub struct ServerConfig {
    pub bind_address: String,
    pub port: u16,
    pub monitor: Option<MonitorHandle>,
    pub allow_remote_unauthenticated: bool,
}

pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    serve_inner(config, std::future::pending::<()>()).await
}

pub async fn serve_with_shutdown(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    serve_inner(config, shutdown).await
}

async fn serve_inner(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = bind_proxy_listener_with_ack(
        &config.bind_address,
        config.port,
        config.allow_remote_unauthenticated,
    )
    .await?;
    serve_listener(listener, config.monitor, shutdown).await
}

pub async fn bind_proxy_listener(bind_address: &str, port: u16) -> anyhow::Result<TcpListener> {
    bind_proxy_listener_with_ack(bind_address, port, false).await
}

pub async fn bind_proxy_listener_with_ack(
    bind_address: &str,
    port: u16,
    allow_remote_unauthenticated: bool,
) -> anyhow::Result<TcpListener> {
    let ip = bind_address
        .parse::<std::net::IpAddr>()
        .map_err(|err| anyhow::anyhow!("invalid proxy bind address {bind_address:?}: {err}"))?;
    if !ip.is_loopback() && !allow_remote_unauthenticated {
        anyhow::bail!(
            "refusing unauthenticated non-loopback bind address {bind_address:?}; pass \
             --allow-remote-unauthenticated or set \
             CCP_ALLOW_REMOTE_UNAUTHENTICATED=1 only when a firewall or authenticating reverse \
             proxy protects this listener"
        );
    }
    let addr = std::net::SocketAddr::new(ip, port);
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow::anyhow!("failed to bind proxy listener on {addr}: {err}"))?;
    if !ip.is_loopback() {
        let local_addr = listener.local_addr().map_err(|err| {
            anyhow::anyhow!("failed to inspect bound proxy listener on {addr}: {err}")
        })?;
        create_logger("server").warn(
            "SECURITY WARNING: unauthenticated proxy bound to a non-loopback address",
            Some(serde_json::Map::from_iter([
                (
                    "bindAddress".to_string(),
                    json!(local_addr.ip().to_string()),
                ),
                ("port".to_string(), json!(local_addr.port())),
                ("remoteUnauthenticated".to_string(), json!(true)),
            ])),
        );
    }
    Ok(listener)
}

pub async fn serve_listener(
    listener: TcpListener,
    monitor: Option<MonitorHandle>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let limits = ServerLimits::configured();
    let graceful_timeout = limits.graceful_shutdown_timeout;
    serve_listener_with_timeout(listener, monitor, shutdown, graceful_timeout, limits).await
}

async fn serve_listener_with_timeout(
    listener: TcpListener,
    monitor: Option<MonitorHandle>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    graceful_timeout: Duration,
    limits: ServerLimits,
) -> anyhow::Result<()> {
    initialize_process_identity();
    let local_addr = listener.local_addr()?;
    create_logger("server").info(
        "server listening",
        Some(server_listening_fields(local_addr)),
    );
    initialize_config_fingerprint(
        &limits,
        Some(&local_addr.ip().to_string()),
        Some(local_addr.port()),
    );
    let (app, state) =
        app_with_limits_and_state(Arc::new(Registry::with_default_alias()), monitor, limits);
    let (shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel();
    let shutdown_observed = Arc::new(AtomicBool::new(false));
    let shutdown_observed_by_server = Arc::clone(&shutdown_observed);
    let shutdown_state = Arc::clone(&state);
    let graceful_shutdown = async move {
        shutdown.await;
        shutdown_observed_by_server.store(true, Ordering::Release);
        create_logger("server").info(
            "server_shutdown_started",
            Some(Map::from_iter([
                (
                    "activeRequests".to_string(),
                    json!(shutdown_state.active_requests()),
                ),
                (
                    "gracefulTimeoutMs".to_string(),
                    json!(graceful_timeout.as_millis()),
                ),
            ])),
        );
        let _ = shutdown_started_tx.send(());
    };
    let server = axum::serve(listener, app).with_graceful_shutdown(graceful_shutdown);
    let result = await_server_with_grace_timeout(
        server.into_future(),
        shutdown_started_rx,
        graceful_timeout,
    )
    .await;
    if result.is_ok() && shutdown_observed.load(Ordering::Acquire) {
        create_logger("server").info(
            "server_shutdown_completed",
            Some(Map::from_iter([(
                "activeRequests".to_string(),
                json!(state.active_requests()),
            )])),
        );
    }
    let _ = crate::logging::flush(Duration::from_secs(2));
    result
}

fn server_listening_fields(local_addr: std::net::SocketAddr) -> Map<String, Value> {
    Map::from_iter([
        ("port".to_string(), json!(local_addr.port())),
        (
            "bindAddress".to_string(),
            json!(local_addr.ip().to_string()),
        ),
    ])
}

async fn await_server_with_grace_timeout(
    server: impl Future<Output = std::io::Result<()>>,
    shutdown_started: tokio::sync::oneshot::Receiver<()>,
    graceful_timeout: Duration,
) -> anyhow::Result<()> {
    let timeout_after_shutdown = async move {
        if shutdown_started.await.is_ok() {
            tokio::time::sleep(graceful_timeout).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(server);
    tokio::pin!(timeout_after_shutdown);
    tokio::select! {
        biased;
        result = &mut server => result.map_err(anyhow::Error::from),
        () = &mut timeout_after_shutdown => {
            create_logger("server").warn(
                "server_shutdown_forced",
                Some(Map::from_iter([(
                    "gracefulTimeoutMs".to_string(),
                    json!(graceful_timeout.as_millis()),
                )])),
            );
            Err(anyhow::anyhow!(
                "server graceful shutdown exceeded {} ms",
                graceful_timeout.as_millis()
            ))
        },
    }
}

pub fn app(registry: Arc<Registry>) -> Router {
    app_with_monitor(registry, None)
}

pub fn app_with_monitor(registry: Arc<Registry>, monitor: Option<MonitorHandle>) -> Router {
    app_with_limits(registry, monitor, ServerLimits::default())
}

pub fn app_with_limits(
    registry: Arc<Registry>,
    monitor: Option<MonitorHandle>,
    limits: ServerLimits,
) -> Router {
    app_with_limits_and_state(registry, monitor, limits).0
}

fn app_with_limits_and_state(
    registry: Arc<Registry>,
    monitor: Option<MonitorHandle>,
    limits: ServerLimits,
) -> (Router, Arc<AppState>) {
    initialize_process_identity();
    initialize_config_fingerprint(&limits, None, None);
    let provider_construction_config_generation = registry.construction_config_generation();
    let provider_construction_config_generation_end = registry.construction_config_generation_end();
    let provider_construction_config_snapshot_stable =
        registry.construction_config_snapshot_stable();
    let state = Arc::new(AppState {
        registry,
        monitor,
        admission: AdmissionState::new(&limits),
        limits,
        provider_construction_config_generation,
        provider_construction_config_generation_end,
        provider_construction_config_snapshot_stable,
        config_generation_drift_warning: ConfigGenerationDriftWarning::new(
            provider_construction_config_generation,
            provider_construction_config_snapshot_stable,
        ),
    });
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/version", get(version))
        .route("/v1/models", get(models))
        .route("/v1/messages", post(handler_messages))
        .route("/v1/messages/count_tokens", post(handler_count_tokens))
        .fallback(fallback_handler)
        .with_state(Arc::clone(&state));
    (app, state)
}

struct AppState {
    registry: Arc<Registry>,
    monitor: Option<MonitorHandle>,
    admission: AdmissionState,
    limits: ServerLimits,
    provider_construction_config_generation: u64,
    provider_construction_config_generation_end: u64,
    provider_construction_config_snapshot_stable: bool,
    config_generation_drift_warning: ConfigGenerationDriftWarning,
}

struct ConfigGenerationDriftWarning {
    provider_construction_generation: u64,
    provider_construction_snapshot_stable: bool,
    warned_generations: Mutex<HashSet<u64>>,
}

impl ConfigGenerationDriftWarning {
    fn new(
        provider_construction_generation: u64,
        provider_construction_snapshot_stable: bool,
    ) -> Self {
        Self {
            provider_construction_generation,
            provider_construction_snapshot_stable,
            warned_generations: Mutex::new(HashSet::new()),
        }
    }

    fn observe(&self, current_generation: u64) -> bool {
        if self.provider_construction_snapshot_stable
            && current_generation == self.provider_construction_generation
        {
            return false;
        }

        // GET /version is a diagnostic path, so a small lock is preferable to
        // losing or repeating a warning if concurrent requests observe two
        // different generations and complete out of order.
        self.warned_generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(current_generation)
    }
}

struct AdmissionState {
    global: Arc<Semaphore>,
    request_bytes: Arc<Semaphore>,
    providers: Mutex<HashMap<String, Arc<Semaphore>>>,
    sessions: Mutex<HashMap<String, Weak<Semaphore>>>,
    per_provider: usize,
    per_session: usize,
}

impl AdmissionState {
    fn new(limits: &ServerLimits) -> Self {
        Self {
            global: Arc::new(Semaphore::new(limits.max_concurrent_requests)),
            request_bytes: Arc::new(Semaphore::new(limits.max_buffered_request_bytes)),
            providers: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            per_provider: limits.max_concurrent_per_provider,
            per_session: limits.max_concurrent_per_session,
        }
    }

    fn provider(&self, provider: &str) -> Arc<Semaphore> {
        let mut providers = self.providers.lock().expect("provider admission lock");
        providers
            .entry(provider.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.per_provider)))
            .clone()
    }

    fn session(&self, session_id: &str) -> Arc<Semaphore> {
        let mut sessions = self.sessions.lock().expect("session admission lock");
        if let Some(semaphore) = sessions.get(session_id).and_then(Weak::upgrade) {
            return semaphore;
        }
        if sessions.len() >= session::MAX_SESSIONS {
            sessions.retain(|_, semaphore| semaphore.strong_count() > 0);
        }
        let semaphore = Arc::new(Semaphore::new(self.per_session));
        sessions.insert(session_id.to_string(), Arc::downgrade(&semaphore));
        semaphore
    }

    fn acquire(&self, semaphore: Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
        semaphore.try_acquire_owned().ok()
    }

    fn active_requests(&self, maximum: usize) -> usize {
        maximum.saturating_sub(self.global.available_permits())
    }
}

#[derive(Default)]
struct RequestPermits {
    global: Option<OwnedSemaphorePermit>,
    provider: Option<OwnedSemaphorePermit>,
    session: Option<OwnedSemaphorePermit>,
    request_bytes: Option<OwnedSemaphorePermit>,
}

enum ProviderRouteSelection {
    Admitted {
        provider: Arc<dyn Provider>,
        permit: OwnedSemaphorePermit,
    },
    Saturated(&'static str),
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({ "ok": true }))
}

impl AppState {
    fn active_requests(&self) -> usize {
        self.admission
            .active_requests(self.limits.max_concurrent_requests)
    }
}

async fn version(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let current_config_generation = crate::config::load_config().config_generation;
    if state
        .config_generation_drift_warning
        .observe(current_config_generation)
    {
        create_logger("server").warn(
            "provider_config_generation_stale",
            Some(Map::from_iter([
                (
                    "providerConstructionConfigGeneration".to_string(),
                    Value::Number(state.provider_construction_config_generation.into()),
                ),
                (
                    "configGeneration".to_string(),
                    Value::Number(current_config_generation.into()),
                ),
                (
                    "providerConstructionConfigGenerationEnd".to_string(),
                    Value::Number(state.provider_construction_config_generation_end.into()),
                ),
                (
                    "providerConstructionSnapshotStable".to_string(),
                    Value::Bool(state.provider_construction_config_snapshot_stable),
                ),
                (
                    "action".to_string(),
                    Value::String("inspect_get_version_configReload_and_restart_if_needed".into()),
                ),
            ])),
        );
    }
    let mut info = version_info_with_config_generations(
        state.provider_construction_config_generation,
        state.provider_construction_config_generation_end,
        state.provider_construction_config_snapshot_stable,
        current_config_generation,
    );
    info.as_object_mut()
        .expect("version metadata is a JSON object")
        .insert("activeRequests".to_string(), json!(state.active_requests()));
    Json(info)
}

async fn models(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let created_at = model_catalog_created_at();
    let mut catalog = state.registry.all_supported_models();
    catalog.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    catalog.dedup_by(|left, right| left.0 == right.0);

    let first_id = catalog.first().map(|(model, _)| model.clone());
    let last_id = catalog.last().map(|(model, _)| model.clone());
    let data: Vec<Value> = catalog
        .into_iter()
        .map(|(model, _provider)| {
            let display_name = model.clone();
            json!({
                "type": "model",
                "id": model,
                "display_name": display_name,
                "created_at": created_at,
            })
        })
        .collect();

    Json(json!({
        "data": data,
        "has_more": false,
        "first_id": first_id,
        "last_id": last_id,
    }))
}

static MODEL_CATALOG_CREATED_AT: OnceLock<String> = OnceLock::new();

fn model_catalog_created_at() -> &'static str {
    MODEL_CATALOG_CREATED_AT.get_or_init(|| {
        let timestamp = env!("CCPROXY_BUILD_UNIX_EPOCH")
            .parse::<i64>()
            .ok()
            .and_then(|seconds| time::OffsetDateTime::from_unix_timestamp(seconds).ok())
            .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
        timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
    })
}

struct ProcessIdentity {
    started_at_ms: u64,
    path: Option<String>,
    sha256: Option<String>,
}

static PROCESS_IDENTITY: OnceLock<ProcessIdentity> = OnceLock::new();
static CONFIG_FINGERPRINT: OnceLock<String> = OnceLock::new();
static ERROR_CAPTURE_GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();

// Cache the executable hash before serving. Re-reading current_exe for each
// request can hash a newly overwritten Cellar path while an old PID is alive.
pub fn initialize_process_identity() {
    PROCESS_IDENTITY.get_or_init(|| {
        let started_at_ms = now_ms();
        let executable = std::env::current_exe().ok();
        let sha256 = executable.as_deref().and_then(hash_file_sha256);
        ProcessIdentity {
            started_at_ms,
            path: executable.map(|path| path.display().to_string()),
            sha256,
        }
    });
}

fn hash_file_sha256(path: &Path) -> Option<String> {
    let mut file = File::open(path).ok()?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Some(hex::encode(digest.finalize()))
}

fn initialize_config_fingerprint(
    limits: &ServerLimits,
    bind_address: Option<&str>,
    port: Option<u16>,
) {
    CONFIG_FINGERPRINT.get_or_init(|| {
        let config = crate::config::load_config();
        effective_config_fingerprint(
            limits,
            bind_address.unwrap_or(config.bind_address.as_str()),
            port.unwrap_or(config.port),
        )
    });
}

fn effective_config_fingerprint(limits: &ServerLimits, bind_address: &str, port: u16) -> String {
    let values = [
        format!("bindAddress={bind_address}"),
        format!("port={port}"),
        format!("maxRequestBodyBytes={}", limits.max_request_body_bytes),
        format!(
            "maxBufferedRequestBytes={}",
            limits.max_buffered_request_bytes
        ),
        format!("maxConcurrentRequests={}", limits.max_concurrent_requests),
        format!(
            "maxConcurrentPerProvider={}",
            limits.max_concurrent_per_provider
        ),
        format!(
            "maxConcurrentPerSession={}",
            limits.max_concurrent_per_session
        ),
        format!(
            "requestBodyIdleTimeoutMs={}",
            limits.request_body_idle_timeout.as_millis()
        ),
        format!(
            "requestBodyTotalTimeoutMs={}",
            limits.request_body_total_timeout.as_millis()
        ),
        format!(
            "gracefulShutdownTimeoutMs={}",
            limits.graceful_shutdown_timeout.as_millis()
        ),
    ];
    hex::encode(Sha256::digest(values.join("\n").as_bytes()))
}

fn process_identity() -> &'static ProcessIdentity {
    initialize_process_identity();
    PROCESS_IDENTITY
        .get()
        .expect("process identity initialized")
}

pub fn version_info() -> Value {
    let identity = process_identity();
    initialize_config_fingerprint(&ServerLimits::configured(), None, None);
    let config_fingerprint = CONFIG_FINGERPRINT
        .get()
        .expect("configuration fingerprint initialized");

    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "gitSha": env!("CCPROXY_GIT_SHA"),
        "gitDirty": env!("CCPROXY_GIT_DIRTY") == "true",
        "buildTimestamp": env!("CCPROXY_BUILD_UNIX_EPOCH").parse::<u64>().ok(),
        "pid": std::process::id(),
        "startedAtMs": identity.started_at_ms,
        "executable": identity.path.as_deref(),
        "binarySha256": identity.sha256.as_deref(),
        "configFingerprint": config_fingerprint,
        "configFingerprintScope": "server-routing",
        "capabilities": {
            "codexOutputBudget": "unsupported_by_private_gateway",
            "codexZeroMaxTokens": "rejected_before_dispatch",
            "codexStructuredOutput": "validated_against_original_schema",
            "codexUnconfirmedToolCall": "fail_closed_by_default",
            "nonEmptyStopSequences": "rejected_except_claude_code_auto_mode_xml",
            "samplingControls": "rejected_before_dispatch",
        },
    })
}

fn version_info_with_config_generations(
    provider_construction_config_generation: u64,
    provider_construction_config_generation_end: u64,
    provider_construction_config_snapshot_stable: bool,
    current_config_generation: u64,
) -> Value {
    let generation_changed = current_config_generation != provider_construction_config_generation;
    let mut info = version_info();
    info.as_object_mut()
        .expect("version metadata is a JSON object")
        .extend(Map::from_iter([
            (
                "configGeneration".to_string(),
                Value::Number(current_config_generation.into()),
            ),
            (
                "providerConstructionConfigGeneration".to_string(),
                Value::Number(provider_construction_config_generation.into()),
            ),
            (
                "providerConstructionConfigGenerationEnd".to_string(),
                Value::Number(provider_construction_config_generation_end.into()),
            ),
            (
                "providerConstructionSnapshotStable".to_string(),
                Value::Bool(provider_construction_config_snapshot_stable),
            ),
            (
                "configGenerationChangedSinceProviderConstruction".to_string(),
                Value::Bool(generation_changed),
            ),
            (
                "configReload".to_string(),
                json!({
                    "status": if !provider_construction_config_snapshot_stable {
                        "provider_construction_snapshot_unstable_restart_required"
                    } else if generation_changed {
                        "generation_changed_check_restart_required_fields"
                    } else {
                        "provider_generation_current"
                    },
                    "nextRequest": [
                        "log.*",
                        "codex.model/effort/reasoningSummary/serviceTier/responsesLite/parallelTools",
                        "codex.originator/userAgent/previousResponseId/unsafeSalvageToolCallOnClose",
                        "codex.totalTimeoutMs/streamHeartbeatMs/websocket*TimeoutMs/maxIdleWebSockets/idleWebSocketTtlMs",
                        "grok.totalTimeoutMs/streamHeartbeatMs",
                    ],
                    "restartRequired": [
                        "bindAddress/port/server.*",
                        "codex.baseUrl/transport/connectTimeoutMs/headerTimeoutMs/httpFirstByteTimeoutMs/bodyIdleTimeoutMs",
                        "grok.baseUrl/clientVersion/connectTimeoutMs/headerTimeoutMs/firstByteTimeoutMs/bodyIdleTimeoutMs",
                        "HTTP(S)_PROXY/ALL_PROXY/NO_PROXY and operating-system proxy settings",
                    ],
                }),
            ),
        ]));
    info
}

async fn handler_messages(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, false).await
}

async fn handler_count_tokens(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, true).await
}

struct BufferedRequestBody {
    bytes: Bytes,
    permit: Option<OwnedSemaphorePermit>,
}

enum RequestBodyReadError {
    TooLarge,
    ByteBudgetSaturated,
    TimedOut,
    Read(String),
}

async fn read_bounded_request_body(
    mut body: Body,
    request_limit: usize,
    byte_budget: Arc<Semaphore>,
    idle_timeout: Duration,
    total_timeout: Duration,
) -> Result<BufferedRequestBody, RequestBodyReadError> {
    let total_deadline = tokio::time::Instant::now() + total_timeout;
    let mut bytes = Vec::new();
    let mut byte_permit: Option<OwnedSemaphorePermit> = None;
    loop {
        let frame = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(total_deadline) => {
                return Err(RequestBodyReadError::TimedOut);
            }
            result = tokio::time::timeout(idle_timeout, body.frame()) => {
                match result {
                    Ok(frame) => frame,
                    Err(_) => return Err(RequestBodyReadError::TimedOut),
                }
            }
        };
        let Some(frame) = frame else {
            break;
        };
        let frame = frame.map_err(|error| RequestBodyReadError::Read(error.to_string()))?;
        let Some(data) = frame.data_ref() else {
            continue;
        };
        if bytes.len().saturating_add(data.len()) > request_limit {
            return Err(RequestBodyReadError::TooLarge);
        }
        if !data.is_empty() {
            let amount = u32::try_from(data.len()).map_err(|_| RequestBodyReadError::TooLarge)?;
            let permit = byte_budget
                .clone()
                .try_acquire_many_owned(amount)
                .map_err(|_| RequestBodyReadError::ByteBudgetSaturated)?;
            if let Some(existing) = byte_permit.as_mut() {
                existing.merge(permit);
            } else {
                byte_permit = Some(permit);
            }
            bytes.extend_from_slice(data);
        }
    }
    Ok(BufferedRequestBody {
        bytes: Bytes::from(bytes),
        permit: byte_permit,
    })
}

async fn dispatch_request(
    state: Arc<AppState>,
    req: Request<Body>,
    count_tokens: bool,
) -> Response {
    let req_id = Uuid::new_v4().to_string();
    let mut response = dispatch_request_with_id(state, req, count_tokens, req_id.clone()).await;
    if let Ok(value) = http::HeaderValue::from_str(&req_id) {
        response
            .headers_mut()
            .insert(http::HeaderName::from_static("request-id"), value);
    }
    response
}

async fn dispatch_request_with_id(
    state: Arc<AppState>,
    req: Request<Body>,
    count_tokens: bool,
    req_id: String,
) -> Response {
    let started_at = Instant::now();
    let log = create_logger("server");
    let (parts, request_body) = req.into_parts();
    let http::request::Parts {
        method,
        uri,
        headers,
        ..
    } = parts;
    let path = uri.path().to_string();
    let endpoint = if count_tokens {
        EndpointKind::CountTokens
    } else {
        EndpointKind::Messages
    };
    log.info(
        "request",
        Some(request_log_fields(&req_id, &method, &path, &uri)),
    );
    let parsed_session_id = session_id_from_headers(&headers);
    let monitored_session_id = parsed_session_id.as_ref().ok().cloned().flatten();
    if let Some(monitor) = state.monitor.as_ref() {
        monitor.request_started(&req_id, monitored_session_id, None, endpoint);
    }
    let mut request_guard = RequestMonitorGuard::new(
        state.monitor.clone(),
        req_id.clone(),
        log.clone(),
        started_at,
        count_tokens,
    );
    let session_id = match parsed_session_id {
        Ok(session_id) => session_id,
        Err(SessionIdError::TooLong) => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("x-claude-code-session-id exceeds the {MAX_SESSION_ID_BYTES}-byte limit"),
            );
            return finalize_immediate_failure(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                response,
            )
            .await;
        }
        Err(SessionIdError::InvalidEncoding) => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "x-claude-code-session-id must contain valid visible text",
            );
            return finalize_immediate_failure(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                response,
            )
            .await;
        }
    };
    let agent_id = match agent_id_from_headers(&headers) {
        Ok(agent_id) => agent_id,
        Err(AgentIdError::Empty) => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "x-claude-code-agent-id must not be empty",
            );
            return finalize_immediate_failure(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                response,
            )
            .await;
        }
        Err(AgentIdError::TooLong) => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("x-claude-code-agent-id exceeds the {MAX_AGENT_ID_BYTES}-byte limit"),
            );
            return finalize_immediate_failure(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                response,
            )
            .await;
        }
        Err(AgentIdError::InvalidEncoding) => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "x-claude-code-agent-id must contain valid visible ASCII text",
            );
            return finalize_immediate_failure(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                response,
            )
            .await;
        }
    };
    let lane_key = request_lane_key(session_id.as_deref(), agent_id.as_deref());
    if headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > state.limits.max_request_body_bytes as u64)
    {
        return request_body_too_large(
            &log,
            &mut request_guard,
            &req_id,
            count_tokens,
            started_at,
            state.limits.max_request_body_bytes,
        )
        .await;
    }

    let mut permits = RequestPermits::default();
    if let Some(session_id) = session_id.as_deref() {
        permits.session = state.admission.acquire(state.admission.session(session_id));
        if permits.session.is_none() {
            return admission_rejection(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                "session request limit is saturated",
            )
            .await;
        }
    }
    permits.global = state.admission.acquire(state.admission.global.clone());
    if permits.global.is_none() {
        return admission_rejection(
            &log,
            &mut request_guard,
            &req_id,
            None,
            None,
            count_tokens,
            started_at,
            "global request limit is saturated",
        )
        .await;
    }

    let body_bytes = match read_bounded_request_body(
        request_body,
        state.limits.max_request_body_bytes,
        state.admission.request_bytes.clone(),
        state.limits.request_body_idle_timeout,
        state.limits.request_body_total_timeout,
    )
    .await
    {
        Ok(buffered) => {
            permits.request_bytes = buffered.permit;
            buffered.bytes
        }
        Err(RequestBodyReadError::TooLarge) => {
            return request_body_too_large(
                &log,
                &mut request_guard,
                &req_id,
                count_tokens,
                started_at,
                state.limits.max_request_body_bytes,
            )
            .await;
        }
        Err(RequestBodyReadError::ByteBudgetSaturated) => {
            return admission_rejection(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                "global buffered request byte limit is saturated",
            )
            .await;
        }
        Err(RequestBodyReadError::TimedOut) => {
            let response = json_error(
                StatusCode::REQUEST_TIMEOUT,
                "invalid_request_error",
                "Request body exceeded its read timeout",
            );
            return finalize_immediate_failure(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                response,
            )
            .await;
        }
        Err(RequestBodyReadError::Read(error)) => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("Invalid request body: {error}"),
            );
            return finalize_immediate_failure(
                &log,
                &mut request_guard,
                &req_id,
                None,
                None,
                count_tokens,
                started_at,
                response,
            )
            .await;
        }
    };

    let now = now_ms();

    let mut body: crate::anthropic::schema::MessagesRequest = match parse_json_body(&body_bytes) {
        Ok(body) => body,
        Err(response) => {
            let status = response.status();
            log_response_started(
                &log,
                RequestLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    status: response.status(),
                    started_at,
                },
            );
            let (response, details) = record_failed_response(
                &log,
                FailedResponseLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    started_at,
                },
                *response,
            )
            .await;
            request_guard.failed(
                status,
                details
                    .as_ref()
                    .map(|details| details.message.as_str())
                    .unwrap_or("Invalid JSON")
                    .to_string(),
            );
            return response;
        }
    };

    if !count_tokens {
        log_client_tool_results(&log, &req_id, &body);
    }

    if let Some(monitor) = state.monitor.as_ref()
        && let Some(project) = project::name_from_request(
            body.extra.get("system"),
            body.messages.iter().rev().map(|message| &message.content),
        )
    {
        monitor.project_resolved(&req_id, project);
    }

    let requested_model = match body.model.clone() {
        Some(model) => model,
        None => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Missing \"model\" in request body. {}",
                    state.registry.unknown_model_message()
                ),
            );
            log_response_started(
                &log,
                RequestLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    status: response.status(),
                    started_at,
                },
            );
            let (response, details) = record_failed_response(
                &log,
                FailedResponseLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    started_at,
                },
                response,
            )
            .await;
            request_guard.failed(
                response.status(),
                details
                    .as_ref()
                    .map(|details| details.message.as_str())
                    .unwrap_or("Missing model")
                    .to_string(),
            );
            return response;
        }
    };

    let compaction_model = compaction_model_override(&headers, &body);
    let uses_compaction_model_override = compaction_model.is_some();
    let effective_model = compaction_model.unwrap_or_else(|| requested_model.clone());
    let normalized_model = normalize_incoming_model(&effective_model);
    if normalized_model != normalize_incoming_model(&requested_model) {
        log.info(
            "internal request model override",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), json!(&req_id)),
                ("reason".to_string(), json!("claude_code_compaction")),
                ("requestedModel".to_string(), json!(&requested_model)),
                ("model".to_string(), json!(&normalized_model)),
            ])),
        );
    }
    request_guard.set_route(None, Some(&normalized_model));
    body.model = Some(normalized_model.clone());
    let (selection, current) =
        session::route_session_request(session_id.as_deref(), &normalized_model, now, |affinity| {
            state
                .registry
                .provider_for_model(&normalized_model, affinity)
                .map(|provider| {
                    let provider_name = provider.name();
                    match state
                        .admission
                        .acquire(state.admission.provider(provider_name))
                    {
                        Some(permit) => {
                            let selection = ProviderRouteSelection::Admitted { provider, permit };
                            if uses_compaction_model_override {
                                session::SessionRoute::preserving_affinity(selection, provider_name)
                            } else {
                                session::SessionRoute::new(selection, provider_name)
                            }
                        }
                        None => session::SessionRoute::without_commit(
                            ProviderRouteSelection::Saturated(provider_name),
                        ),
                    }
                })
        });

    let selection = match selection {
        Some(selection) => selection,
        None => {
            log.warn(
                "unknown model",
                Some(serde_json::Map::from_iter([
                    ("reqId".to_string(), json!(&req_id)),
                    ("model".to_string(), json!(&normalized_model)),
                ])),
            );
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Unknown model \"{normalized_model}\". {}",
                    state.registry.unknown_model_message()
                ),
            );
            log_response_started(
                &log,
                RequestLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: Some(&normalized_model),
                    count_tokens,
                    status: response.status(),
                    started_at,
                },
            );
            let (response, details) = record_failed_response(
                &log,
                FailedResponseLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: Some(&normalized_model),
                    count_tokens,
                    started_at,
                },
                response,
            )
            .await;
            request_guard.failed(
                response.status(),
                details
                    .as_ref()
                    .map(|details| details.message.as_str())
                    .unwrap_or("Unknown model")
                    .to_string(),
            );
            return response;
        }
    };
    let provider = match selection {
        ProviderRouteSelection::Admitted { provider, permit } => {
            permits.provider = Some(permit);
            provider
        }
        ProviderRouteSelection::Saturated(provider_name) => {
            return admission_rejection(
                &log,
                &mut request_guard,
                &req_id,
                Some(provider_name),
                Some(&normalized_model),
                count_tokens,
                started_at,
                "provider request limit is saturated",
            )
            .await;
        }
    };
    let effort = crate::providers::translate_shared::read_effort(&body)
        .ok()
        .flatten()
        .map(str::to_string);
    request_guard.set_route(Some(provider.name()), Some(&normalized_model));
    if let Some(monitor) = state.monitor.as_ref() {
        if let Some(current) = current.as_ref() {
            monitor.session_sequence_resolved(&req_id, current.seq);
        }
        monitor.provider_selected(&req_id, provider.name(), &normalized_model, effort);
    }

    let traffic = create_traffic_capture_async(TrafficCaptureOptions {
        req_id: req_id.clone(),
        session_id: session_id.clone(),
        session_seq: current.as_ref().map(|s| s.seq),
        provider: Some(provider.name().to_string()),
        state_dir_override: None,
    })
    .await
    .map(Arc::new);

    if let Some(capture) = traffic.as_ref() {
        let query = redacted_query(&uri);
        if let Some(monitor) = state.monitor.as_ref() {
            monitor.traffic_capture_path(&req_id, capture.root().to_path_buf());
        }
        capture.write_json(
            "000-metadata",
            &json!({
                "reqId": &req_id,
                "sessionFingerprint": session_id
                    .as_deref()
                    .map(|session_id| traffic_session_fingerprint(Some(session_id))),
                "sessionSeq": current.as_ref().map(|s| s.seq),
                "kind": if count_tokens { "count_tokens" } else { "messages" },
                "provider": provider.name(),
                "model": &normalized_model,
                "method": method.as_str(),
                "path": &path,
                "query": &query,
                "headers": headers_to_record(&headers),
            }),
        );
        capture.write_json(
            "010-anthropic-request",
            &serde_json::to_value(&body).unwrap_or_else(|_| json!({})),
        );
    }

    let request_byte_lease = permits
        .request_bytes
        .take()
        .map(|permit| RequestByteLease::new(permit, body_bytes.len()));
    let context = RequestContext {
        req_id: req_id.clone(),
        session_id,
        lane_key,
        session_seq: current.map(|s| s.seq),
        provider: provider.name().to_string(),
        traffic,
        monitor: state.monitor.clone(),
        request_byte_lease: request_byte_lease.clone(),
    };

    let response = if count_tokens {
        provider.handle_count_tokens(body, context).await
    } else {
        provider.handle_messages(body, context).await
    };
    // Release the server's lease after dispatch. A provider may keep its clone
    // only through initial dispatch or replay; long-lived provider state uses
    // a separate bounded budget so request admission is not coupled to stream
    // duration.
    drop(request_byte_lease);
    log_response_started(
        &log,
        RequestLogContext {
            req_id: &req_id,
            provider: Some(provider.name()),
            model: Some(&normalized_model),
            count_tokens,
            status: response.status(),
            started_at,
        },
    );
    let status = response.status();
    if status.is_success() {
        return monitor_response_body(
            response,
            request_guard,
            ResponseLogContext {
                log,
                req_id,
                provider: Some(provider.name().to_string()),
                model: Some(normalized_model),
                count_tokens,
                started_at,
            },
            permits,
        );
    }

    let (response, details) = record_failed_response(
        &log,
        FailedResponseLogContext {
            req_id: &req_id,
            provider: Some(provider.name()),
            model: Some(&normalized_model),
            count_tokens,
            started_at,
        },
        response,
    )
    .await;
    request_guard.failed(
        status,
        details
            .as_ref()
            .map(|details| details.message.clone())
            .unwrap_or_else(|| format!("HTTP {}", status.as_u16())),
    );
    hold_response_permits(response, permits)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionIdError {
    TooLong,
    InvalidEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentIdError {
    Empty,
    TooLong,
    InvalidEncoding,
}

fn session_id_from_headers(headers: &HeaderMap) -> Result<Option<String>, SessionIdError> {
    let Some(value) = headers.get(CLAUDE_CODE_SESSION_ID_HEADER) else {
        return Ok(None);
    };
    let session_id = value
        .to_str()
        .map_err(|_| SessionIdError::InvalidEncoding)?;
    if session_id.is_empty() {
        return Ok(None);
    }
    if session_id.len() > MAX_SESSION_ID_BYTES {
        return Err(SessionIdError::TooLong);
    }
    Ok(Some(session_id.to_string()))
}

fn agent_id_from_headers(headers: &HeaderMap) -> Result<Option<String>, AgentIdError> {
    let mut values = headers.get_all(CLAUDE_CODE_AGENT_ID_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AgentIdError::InvalidEncoding);
    }
    let agent_id = value.to_str().map_err(|_| AgentIdError::InvalidEncoding)?;
    if agent_id.is_empty() {
        return Err(AgentIdError::Empty);
    }
    if agent_id.len() > MAX_AGENT_ID_BYTES {
        return Err(AgentIdError::TooLong);
    }
    if !agent_id.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(AgentIdError::InvalidEncoding);
    }
    Ok(Some(agent_id.to_string()))
}

fn request_lane_key(session_id: Option<&str>, agent_id: Option<&str>) -> Option<RequestLaneKey> {
    let session_id = session_id?;
    let session_id_len = u64::try_from(session_id.len()).ok()?;

    let mut digest = Sha256::new();
    digest.update(REQUEST_LANE_KEY_DOMAIN);
    digest.update(session_id_len.to_be_bytes());
    digest.update(session_id.as_bytes());
    match agent_id {
        None => digest.update([0]),
        Some(agent_id) => {
            let agent_id_len = u64::try_from(agent_id.len()).ok()?;
            digest.update([1]);
            digest.update(agent_id_len.to_be_bytes());
            digest.update(agent_id.as_bytes());
        }
    }
    Some(RequestLaneKey::from_digest(digest.finalize().into()))
}

#[allow(clippy::too_many_arguments)]
async fn admission_rejection(
    log: &Logger,
    request_guard: &mut RequestMonitorGuard,
    req_id: &str,
    provider: Option<&str>,
    model: Option<&str>,
    count_tokens: bool,
    started_at: Instant,
    reason: &str,
) -> Response {
    let mut response = json_error(
        StatusCode::TOO_MANY_REQUESTS,
        "overloaded_error",
        format!("Proxy is busy: {reason}"),
    );
    response.headers_mut().insert(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_static("1"),
    );
    finalize_immediate_failure(
        log,
        request_guard,
        req_id,
        provider,
        model,
        count_tokens,
        started_at,
        response,
    )
    .await
}

async fn request_body_too_large(
    log: &Logger,
    request_guard: &mut RequestMonitorGuard,
    req_id: &str,
    count_tokens: bool,
    started_at: Instant,
    limit: usize,
) -> Response {
    let response = json_error(
        StatusCode::PAYLOAD_TOO_LARGE,
        "invalid_request_error",
        format!("Request body exceeds the {limit}-byte limit"),
    );
    finalize_immediate_failure(
        log,
        request_guard,
        req_id,
        None,
        None,
        count_tokens,
        started_at,
        response,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finalize_immediate_failure(
    log: &Logger,
    request_guard: &mut RequestMonitorGuard,
    req_id: &str,
    provider: Option<&str>,
    model: Option<&str>,
    count_tokens: bool,
    started_at: Instant,
    response: Response,
) -> Response {
    let status = response.status();
    log_response_started(
        log,
        RequestLogContext {
            req_id,
            provider,
            model,
            count_tokens,
            status,
            started_at,
        },
    );
    let (response, details) = record_failed_response(
        log,
        FailedResponseLogContext {
            req_id,
            provider,
            model,
            count_tokens,
            started_at,
        },
        response,
    )
    .await;
    request_guard.failed(
        status,
        details
            .map(|details| details.message)
            .unwrap_or_else(|| format!("HTTP {}", status.as_u16())),
    );
    response
}

fn monitor_response_body(
    response: Response,
    guard: RequestMonitorGuard,
    log_context: ResponseLogContext,
    permits: RequestPermits,
) -> Response {
    let status = response.status();
    let is_event_stream = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
        });
    let (parts, body) = response.into_parts();
    let lifecycle = ResponseBodyLifecycle {
        guard,
        log_context,
        status,
        sse_detector: is_event_stream.then(SseErrorDetector::default),
        open_tool_blocks: HashMap::new(),
        provisionally_closed_tool_blocks: HashMap::new(),
        observed_tool_block_starts: 0,
        tool_tracking_truncated: false,
        tool_tracking_disabled: false,
        saw_message_stop: false,
        _permits: permits,
        terminal: false,
    };
    let stream = futures_util::stream::unfold(
        (body, lifecycle),
        move |(mut body, mut lifecycle)| async move {
            if lifecycle.terminal {
                return None;
            }
            match body.frame().await {
                Some(Ok(frame)) => {
                    if let Some(data) = frame.data_ref()
                        && let Some((error, consumed_bytes)) = lifecycle.detect_sse_error(data)
                    {
                        lifecycle.failed(error, true);
                        let frame =
                            frame.map_data(|data| data.slice(..consumed_bytes.min(data.len())));
                        return Some((Ok(frame), (body, lifecycle)));
                    }
                    Some((Ok(frame), (body, lifecycle)))
                }
                Some(Err(err)) => {
                    lifecycle.failed(err.to_string(), false);
                    Some((Err(err), (body, lifecycle)))
                }
                None => {
                    if lifecycle.sse_detector.is_some() && !lifecycle.saw_message_stop {
                        lifecycle.failed(INCOMPLETE_SSE_ERROR.to_string(), false);
                        let error = axum::Error::new(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            INCOMPLETE_SSE_ERROR,
                        ));
                        return Some((Err(error), (body, lifecycle)));
                    }
                    lifecycle.completed();
                    None
                }
            }
        },
    );
    Response::from_parts(parts, Body::new(StreamBody::new(stream)))
}

fn hold_response_permits(response: Response, permits: RequestPermits) -> Response {
    let (parts, body) = response.into_parts();
    let stream = futures_util::stream::unfold((body, permits), |(mut body, permits)| async move {
        body.frame().await.map(|frame| (frame, (body, permits)))
    });
    Response::from_parts(parts, Body::new(StreamBody::new(stream)))
}

struct ResponseLogContext {
    log: Logger,
    req_id: String,
    provider: Option<String>,
    model: Option<String>,
    count_tokens: bool,
    started_at: Instant,
}

struct ResponseBodyLifecycle {
    guard: RequestMonitorGuard,
    log_context: ResponseLogContext,
    status: StatusCode,
    sse_detector: Option<SseErrorDetector>,
    open_tool_blocks: HashMap<u64, ToolBlockTrace>,
    provisionally_closed_tool_blocks: HashMap<u64, ToolBlockTrace>,
    observed_tool_block_starts: usize,
    tool_tracking_truncated: bool,
    tool_tracking_disabled: bool,
    saw_message_stop: bool,
    _permits: RequestPermits,
    terminal: bool,
}

impl ResponseBodyLifecycle {
    fn detect_sse_error(&mut self, bytes: &[u8]) -> Option<(String, usize)> {
        let (error, error_consumed_bytes, tool_events) = {
            let detector = self.sse_detector.as_mut()?;
            let error = detector.push(bytes);
            let error_consumed_bytes = detector.take_error_consumed_bytes();
            let tool_events = detector.take_tool_events();
            (error, error_consumed_bytes, tool_events)
        };
        for event in tool_events {
            self.observe_tool_event(event);
        }
        error.map(|error| (error, error_consumed_bytes.unwrap_or(bytes.len())))
    }

    fn completed(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.interrupt_open_tools("response_eof");
        self.guard.completed(self.status);
        log_request_completed(&self.log_context, self.status);
    }

    fn failed(&mut self, error: String, in_band_sse: bool) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.interrupt_open_tools(if in_band_sse {
            "in_band_sse_error"
        } else {
            "response_body_error"
        });
        let error = sanitized_external_error(&error);
        self.guard.failed(self.status, error.clone());
        log_stream_failed(&self.log_context, self.status, &error, in_band_sse);
    }

    fn observe_tool_event(&mut self, event: SseToolEvent) {
        match event {
            SseToolEvent::Started(tool) => {
                if self.tool_tracking_disabled {
                    return;
                }
                if self.observed_tool_block_starts >= MAX_TRACKED_TOOL_BLOCK_STARTS {
                    self.disable_tool_tracking(ToolTrackingTruncationCause::BlockStartLimit);
                    return;
                }
                self.observed_tool_block_starts += 1;
                if let Some(previous) = self.provisionally_closed_tool_blocks.remove(&tool.index) {
                    log_tool_block_event(
                        &self.log_context,
                        "tool_block_interrupted",
                        &previous,
                        Some("index_reused"),
                    );
                }
                if let Some(previous) = self.open_tool_blocks.insert(tool.index, tool.clone()) {
                    log_tool_block_event(
                        &self.log_context,
                        "tool_block_interrupted",
                        &previous,
                        Some("index_reused"),
                    );
                }
                log_tool_block_event(&self.log_context, "tool_block_started", &tool, None);
            }
            SseToolEvent::Stopped { index } => {
                if self.tool_tracking_disabled {
                    return;
                }
                if let Some(tool) = self.open_tool_blocks.remove(&index) {
                    self.provisionally_closed_tool_blocks.insert(index, tool);
                }
            }
            SseToolEvent::MessageStopped => {
                self.saw_message_stop = true;
                let completed = std::mem::take(&mut self.provisionally_closed_tool_blocks);
                for (_, tool) in completed {
                    log_tool_block_event(&self.log_context, "tool_block_completed", &tool, None);
                }
                self.interrupt_open_tools("message_stop_before_block_stop");
            }
            SseToolEvent::TrackingTruncated => {
                self.disable_tool_tracking(ToolTrackingTruncationCause::PendingEventLimit);
            }
        }
    }

    fn disable_tool_tracking(&mut self, cause: ToolTrackingTruncationCause) {
        self.tool_tracking_disabled = true;
        self.interrupt_open_tools("tracking_capacity_exceeded");
        self.mark_tool_tracking_truncated(cause);
    }

    fn mark_tool_tracking_truncated(&mut self, cause: ToolTrackingTruncationCause) {
        if std::mem::replace(&mut self.tool_tracking_truncated, true) {
            return;
        }
        self.log_context.log.info(
            "tool_block_tracking_truncated",
            Some(serde_json::Map::from_iter([
                (
                    "reqId".to_string(),
                    serde_json::json!(self.log_context.req_id),
                ),
                (
                    "provider".to_string(),
                    serde_json::json!(self.log_context.provider),
                ),
                (
                    "model".to_string(),
                    serde_json::json!(self.log_context.model),
                ),
                (
                    "elapsedMs".to_string(),
                    serde_json::json!(self.log_context.started_at.elapsed().as_millis()),
                ),
                (
                    "blockStartLimit".to_string(),
                    serde_json::json!(MAX_TRACKED_TOOL_BLOCK_STARTS),
                ),
                (
                    "pendingEventLimit".to_string(),
                    serde_json::json!(MAX_PENDING_TOOL_LIFECYCLE_EVENTS),
                ),
                (
                    "truncationCause".to_string(),
                    serde_json::json!(cause.as_str()),
                ),
            ])),
        );
    }

    fn interrupt_open_tools(&mut self, reason: &'static str) {
        let open = std::mem::take(&mut self.open_tool_blocks);
        let provisional = std::mem::take(&mut self.provisionally_closed_tool_blocks);
        for (_, tool) in open.into_iter().chain(provisional) {
            log_tool_block_event(
                &self.log_context,
                "tool_block_interrupted",
                &tool,
                Some(reason),
            );
        }
    }
}

impl Drop for ResponseBodyLifecycle {
    fn drop(&mut self) {
        if !self.terminal {
            self.terminal = true;
            self.interrupt_open_tools("downstream_dropped");
            self.guard.abandoned("Downstream response body was dropped");
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ToolBlockTrace {
    index: u64,
    tool_kind: String,
    tool_name_metadata: Option<Value>,
    call_id_hash: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum SseToolEvent {
    Started(ToolBlockTrace),
    Stopped { index: u64 },
    MessageStopped,
    TrackingTruncated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ToolTrackingTruncationCause {
    BlockStartLimit,
    PendingEventLimit,
}

impl ToolTrackingTruncationCause {
    fn as_str(self) -> &'static str {
        match self {
            Self::BlockStartLimit => "block_start_limit",
            Self::PendingEventLimit => "pending_event_limit",
        }
    }
}

fn log_tool_block_event(
    ctx: &ResponseLogContext,
    message: &'static str,
    tool: &ToolBlockTrace,
    reason: Option<&'static str>,
) {
    let mut fields = serde_json::Map::from_iter([
        ("reqId".to_string(), json!(ctx.req_id)),
        ("provider".to_string(), json!(ctx.provider)),
        ("model".to_string(), json!(ctx.model)),
        (
            "elapsedMs".to_string(),
            json!(ctx.started_at.elapsed().as_millis()),
        ),
    ]);
    fields.extend(tool_block_metadata_fields(tool, reason));
    ctx.log.info(message, Some(fields));
}

fn tool_block_metadata_fields(
    tool: &ToolBlockTrace,
    reason: Option<&'static str>,
) -> serde_json::Map<String, Value> {
    let mut fields = serde_json::Map::from_iter([
        ("index".to_string(), json!(tool.index)),
        ("toolKind".to_string(), json!(tool.tool_kind)),
        (
            "toolNameMetadata".to_string(),
            json!(tool.tool_name_metadata),
        ),
        ("callIdHash".to_string(), json!(tool.call_id_hash)),
    ]);
    if let Some(reason) = reason {
        fields.insert("interruptReason".to_string(), json!(reason));
    }
    fields
}

fn safe_tool_name_metadata(value: &str) -> Option<Value> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    crate::diagnostics::tool_name_metadata(value)
}

fn diagnostic_identifier_hash(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Some(hex::encode(Sha256::digest(value.as_bytes()))[..16].to_string())
}

#[derive(Default)]
struct DiagnosticByteCounter(u64);

impl Write for DiagnosticByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialized_value_size(value: &Value) -> u64 {
    let mut counter = DiagnosticByteCounter::default();
    if serde_json::to_writer(&mut counter, value).is_ok() {
        counter.0
    } else {
        0
    }
}

fn log_client_tool_results(
    log: &Logger,
    req_id: &str,
    request: &crate::anthropic::schema::MessagesRequest,
) {
    let truncated_message_index = visit_client_tool_results(request, |result| {
        log.info(
            "client_tool_result",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), json!(req_id)),
                ("messageIndex".to_string(), json!(result.message_index)),
                ("blockIndex".to_string(), json!(result.block_index)),
                ("callIdHash".to_string(), json!(result.call_id_hash)),
                ("isError".to_string(), json!(result.is_error)),
                ("contentBytes".to_string(), json!(result.content_bytes)),
            ])),
        );
    });
    if let Some((message_index, logged_count)) = truncated_message_index {
        log.info(
            "client_tool_result_tracking_truncated",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), json!(req_id)),
                ("messageIndex".to_string(), json!(message_index)),
                (
                    "resultLimit".to_string(),
                    json!(MAX_TRACKED_CLIENT_TOOL_RESULTS),
                ),
                ("loggedCount".to_string(), json!(logged_count)),
            ])),
        );
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ClientToolResultTrace {
    message_index: usize,
    block_index: usize,
    call_id_hash: String,
    is_error: bool,
    content_bytes: u64,
}

fn visit_client_tool_results(
    request: &crate::anthropic::schema::MessagesRequest,
    mut observe: impl FnMut(ClientToolResultTrace),
) -> Option<(usize, usize)> {
    let (message_index, message) = request.messages.iter().enumerate().next_back()?;
    let blocks = message.content.as_array()?;
    let mut inspected_results = 0_usize;
    let mut logged_results = 0_usize;
    for (block_index, block) in blocks.iter().enumerate() {
        if block.get("type").and_then(Value::as_str) != Some("tool_result") {
            continue;
        }
        if inspected_results >= MAX_TRACKED_CLIENT_TOOL_RESULTS {
            return Some((message_index, logged_results));
        }
        inspected_results += 1;
        let Some(call_id_hash) = block
            .get("tool_use_id")
            .and_then(Value::as_str)
            .and_then(diagnostic_identifier_hash)
        else {
            continue;
        };
        let content = block.get("content").unwrap_or(&Value::Null);
        observe(ClientToolResultTrace {
            message_index,
            block_index,
            call_id_hash,
            is_error: block
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            content_bytes: serialized_value_size(content),
        });
        logged_results += 1;
    }
    None
}

#[derive(Deserialize)]
struct SseTypeProbe<'a> {
    #[serde(rename = "type", borrow)]
    event_type: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct SseToolStartProbe<'a> {
    index: Option<u64>,
    #[serde(borrow)]
    content_block: Option<SseToolBlockProbe<'a>>,
}

#[derive(Deserialize)]
struct SseToolBlockProbe<'a> {
    #[serde(rename = "type", borrow)]
    tool_kind: Option<Cow<'a, str>>,
    name: Option<Value>,
    id: Option<Value>,
}

#[derive(Deserialize)]
struct SseIndexProbe {
    index: Option<u64>,
}

#[derive(Deserialize)]
struct SseErrorMessageProbe {
    error: Option<Value>,
    message: Option<Value>,
}

fn sse_event_type<'a>(data: &'a str) -> Option<Cow<'a, str>> {
    match serde_json::from_str::<SseTypeProbe<'a>>(data) {
        Ok(payload) => payload.event_type,
        Err(_) => {
            // Preserve serde_json::Value's historical last-key-wins behavior for malformed
            // duplicate fields without paying for a full JSON tree on normal stream events.
            let payload = serde_json::from_str::<Value>(data).ok()?;
            payload
                .get("type")
                .and_then(Value::as_str)
                .map(|event_type| Cow::Owned(event_type.to_string()))
        }
    }
}

fn sse_tool_event(event_type: &str, data: &str) -> Option<SseToolEvent> {
    match event_type {
        "content_block_start" => match serde_json::from_str::<SseToolStartProbe<'_>>(data) {
            Ok(payload) => sse_tool_start_event(payload),
            Err(_) => sse_tool_event_from_value(event_type, data),
        },
        "content_block_stop" => match serde_json::from_str::<SseIndexProbe>(data) {
            Ok(payload) => Some(SseToolEvent::Stopped {
                index: payload.index?,
            }),
            Err(_) => sse_tool_event_from_value(event_type, data),
        },
        "message_stop" => Some(SseToolEvent::MessageStopped),
        _ => None,
    }
}

fn sse_tool_start_event(payload: SseToolStartProbe<'_>) -> Option<SseToolEvent> {
    let index = payload.index?;
    let block = payload.content_block?;
    let tool_kind = block.tool_kind?.into_owned();
    if !matches!(tool_kind.as_str(), "tool_use" | "server_tool_use") {
        return None;
    }
    Some(SseToolEvent::Started(ToolBlockTrace {
        index,
        tool_kind,
        tool_name_metadata: block
            .name
            .as_ref()
            .and_then(Value::as_str)
            .and_then(safe_tool_name_metadata),
        call_id_hash: block
            .id
            .as_ref()
            .and_then(Value::as_str)
            .and_then(diagnostic_identifier_hash),
    }))
}

fn sse_tool_event_from_value(event_type: &str, data: &str) -> Option<SseToolEvent> {
    let payload = serde_json::from_str::<Value>(data).ok()?;
    match event_type {
        "content_block_start" => {
            let index = payload.get("index").and_then(Value::as_u64)?;
            let block = payload.get("content_block")?.as_object()?;
            let tool_kind = block.get("type").and_then(Value::as_str)?;
            if !matches!(tool_kind, "tool_use" | "server_tool_use") {
                return None;
            }
            Some(SseToolEvent::Started(ToolBlockTrace {
                index,
                tool_kind: tool_kind.to_string(),
                tool_name_metadata: block
                    .get("name")
                    .and_then(Value::as_str)
                    .and_then(safe_tool_name_metadata),
                call_id_hash: block
                    .get("id")
                    .and_then(Value::as_str)
                    .and_then(diagnostic_identifier_hash),
            }))
        }
        "content_block_stop" => Some(SseToolEvent::Stopped {
            index: payload.get("index").and_then(Value::as_u64)?,
        }),
        _ => None,
    }
}

fn sse_error_message(data: &str) -> Option<String> {
    let payload = match serde_json::from_str::<SseErrorMessageProbe>(data) {
        Ok(payload) => payload,
        Err(_) => {
            let payload = serde_json::from_str::<Value>(data).ok()?;
            return payload
                .pointer("/error/message")
                .or_else(|| payload.get("message"))
                .and_then(Value::as_str)
                .filter(|message| !message.trim().is_empty())
                .map(str::to_string);
        }
    };
    payload
        .error
        .as_ref()
        .and_then(|error| error.get("message"))
        .or(payload.message.as_ref())
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .map(str::to_string)
}

const MAX_SSE_ERROR_EVENT_BYTES: usize = 256 * 1024;

#[derive(Default)]
struct SseErrorDetector {
    line: Vec<u8>,
    event: Option<String>,
    data: Vec<String>,
    event_bytes: usize,
    discard_line: bool,
    discard_event: bool,
    skip_lf: bool,
    tool_events: Vec<SseToolEvent>,
    pending_tool_lifecycle_events: usize,
    tool_events_truncated: bool,
    message_stop_queued: bool,
    error_consumed_bytes: Option<usize>,
}

impl SseErrorDetector {
    fn push(&mut self, bytes: &[u8]) -> Option<String> {
        self.error_consumed_bytes = None;
        for (offset, &byte) in bytes.iter().enumerate() {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if matches!(byte, b'\r' | b'\n') {
                if byte == b'\r' {
                    self.skip_lf = true;
                }
                if self.discard_line {
                    self.discard_line = false;
                    continue;
                }
                if self.discard_event {
                    if let Some(error) = self.finish_event() {
                        self.error_consumed_bytes = Some(offset + 1);
                        return Some(error);
                    }
                    continue;
                }
                let line = String::from_utf8_lossy(&self.line).into_owned();
                self.line.clear();
                if let Some(error) = self.process_line(&line) {
                    self.error_consumed_bytes = Some(offset + 1);
                    return Some(error);
                }
                continue;
            }

            if self.discard_event {
                self.discard_line = true;
                continue;
            }
            self.event_bytes = self.event_bytes.saturating_add(1);
            if self.event_bytes > MAX_SSE_ERROR_EVENT_BYTES {
                self.line.clear();
                self.data.clear();
                self.event = None;
                self.discard_line = true;
                self.discard_event = true;
                continue;
            }
            self.line.push(byte);
        }
        None
    }

    fn take_error_consumed_bytes(&mut self) -> Option<usize> {
        self.error_consumed_bytes.take()
    }

    fn take_tool_events(&mut self) -> Vec<SseToolEvent> {
        self.pending_tool_lifecycle_events = 0;
        self.message_stop_queued = false;
        let mut events = std::mem::take(&mut self.tool_events);
        if std::mem::take(&mut self.tool_events_truncated) {
            events.insert(0, SseToolEvent::TrackingTruncated);
        }
        events
    }

    fn queue_tool_event(&mut self, event: SseToolEvent) {
        if matches!(event, SseToolEvent::MessageStopped) {
            if !self.message_stop_queued {
                self.message_stop_queued = true;
                self.tool_events.push(event);
            }
            return;
        }
        if self.pending_tool_lifecycle_events < MAX_PENDING_TOOL_LIFECYCLE_EVENTS {
            self.pending_tool_lifecycle_events += 1;
            self.tool_events.push(event);
        } else {
            self.tool_events_truncated = true;
        }
    }

    fn process_line(&mut self, line: &str) -> Option<String> {
        if line.is_empty() {
            return self.finish_event();
        }
        if self.discard_event || line.starts_with(':') {
            return None;
        }

        let (field, value) = line
            .split_once(':')
            .map(|(field, value)| (field, value.strip_prefix(' ').unwrap_or(value)))
            .unwrap_or((line, ""));
        match field {
            "event" => self.event = Some(value.to_string()),
            "data" => {
                self.event_bytes = self
                    .event_bytes
                    .saturating_add(std::mem::size_of::<String>());
                if self.event_bytes > MAX_SSE_ERROR_EVENT_BYTES {
                    self.discard_event = true;
                    self.event = None;
                    self.data.clear();
                } else {
                    self.data.push(value.to_string());
                }
            }
            _ => {}
        }
        None
    }

    fn finish_event(&mut self) -> Option<String> {
        let event = self.event.take();
        let data = std::mem::take(&mut self.data).join("\n");
        let discarded = std::mem::take(&mut self.discard_event);
        self.event_bytes = 0;
        if discarded {
            return None;
        }

        let payload_type = sse_event_type(&data);
        if let Some(tool_event) = payload_type
            .as_deref()
            .and_then(|event_type| sse_tool_event(event_type, &data))
        {
            self.queue_tool_event(tool_event);
        }
        let is_error =
            event.as_deref() == Some("error") || payload_type.as_deref() == Some("error");
        if !is_error {
            return None;
        }

        Some(
            sse_error_message(&data)
                .unwrap_or_else(|| "Upstream stream returned an error event".to_string()),
        )
    }
}

struct RequestLogContext<'a> {
    req_id: &'a str,
    provider: Option<&'a str>,
    model: Option<&'a str>,
    count_tokens: bool,
    status: StatusCode,
    started_at: Instant,
}

fn log_response_started(log: &Logger, ctx: RequestLogContext<'_>) {
    log.info(
        "response_started",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), json!(ctx.req_id)),
            ("provider".to_string(), json!(ctx.provider)),
            ("model".to_string(), json!(ctx.model)),
            ("countTokens".to_string(), json!(ctx.count_tokens)),
            ("status".to_string(), json!(ctx.status.as_u16())),
            (
                "ms".to_string(),
                json!(ctx.started_at.elapsed().as_millis()),
            ),
        ])),
    );
}

fn response_log_fields(
    ctx: &ResponseLogContext,
    status: StatusCode,
    extra: Option<(&str, Value)>,
) -> serde_json::Map<String, Value> {
    let mut fields = serde_json::Map::from_iter([
        ("reqId".to_string(), json!(ctx.req_id)),
        ("provider".to_string(), json!(ctx.provider)),
        ("model".to_string(), json!(ctx.model)),
        ("countTokens".to_string(), json!(ctx.count_tokens)),
        ("status".to_string(), json!(status.as_u16())),
        (
            "ms".to_string(),
            json!(ctx.started_at.elapsed().as_millis()),
        ),
    ]);
    if let Some((key, value)) = extra {
        fields.insert(key.to_string(), value);
    }
    fields
}

fn log_request_completed(ctx: &ResponseLogContext, status: StatusCode) {
    ctx.log.info(
        "request_completed",
        Some(response_log_fields(ctx, status, None)),
    );
}

fn log_stream_failed(ctx: &ResponseLogContext, status: StatusCode, error: &str, in_band_sse: bool) {
    let mut fields = response_log_fields(ctx, status, Some(("message", json!(error))));
    fields.insert("phase".to_string(), json!("response_body"));
    fields.insert("inBandSse".to_string(), json!(in_band_sse));
    ctx.log.info("request_failed", Some(fields));
}

struct FailedResponseLogContext<'a> {
    req_id: &'a str,
    provider: Option<&'a str>,
    model: Option<&'a str>,
    count_tokens: bool,
    started_at: Instant,
}

struct FailedResponseDetails {
    message: String,
}

async fn record_failed_response(
    log: &Logger,
    ctx: FailedResponseLogContext<'_>,
    response: Response,
) -> (Response, Option<FailedResponseDetails>) {
    if response.status().is_success() {
        return (response, None);
    }

    let status = response.status();
    let (mut parts, body) = response.into_parts();
    let read = read_bounded_error_body(body).await;
    let response_body = response_body_value(&read.bytes);
    let message = error_message_from_response(&response_body).unwrap_or_else(|| {
        if read.timed_out {
            "Upstream error response body timed out".to_string()
        } else if read.truncated {
            "Upstream error response exceeded the proxy limit".to_string()
        } else if let Some(error) = read.error.as_deref() {
            format!("Failed to read upstream error response: {error}")
        } else {
            format!("HTTP {}", status.as_u16())
        }
    });
    let message = sanitized_external_error(&message);
    let body_read_error = read.error.as_deref().map(sanitized_external_error);
    let document = json!({
        "reqId": ctx.req_id,
        "provider": ctx.provider,
        "model": ctx.model,
        "countTokens": ctx.count_tokens,
        "status": status.as_u16(),
        "elapsedMs": ctx.started_at.elapsed().as_millis(),
        "message": message,
        "response": response_body,
        "bodyTruncated": read.truncated,
        "bodyTimedOut": read.timed_out,
        "bodyReadError": body_read_error,
    });
    let error_file = if should_capture_error_response(ctx.provider, status) {
        match write_error_capture(ctx.req_id, redact_error_value(document)).await {
            Ok(path) => path,
            Err(_) => {
                log.warn(
                    "error_capture_failed",
                    Some(serde_json::Map::from_iter([
                        ("reqId".to_string(), json!(ctx.req_id)),
                        ("provider".to_string(), json!(ctx.provider)),
                        ("status".to_string(), json!(status.as_u16())),
                        ("errorKind".to_string(), json!("capture_write")),
                    ])),
                );
                None
            }
        }
    } else {
        None
    };

    let mut fields = serde_json::Map::from_iter([
        ("reqId".to_string(), json!(ctx.req_id)),
        ("provider".to_string(), json!(ctx.provider)),
        ("model".to_string(), json!(ctx.model)),
        ("countTokens".to_string(), json!(ctx.count_tokens)),
        ("status".to_string(), json!(status.as_u16())),
        (
            "ms".to_string(),
            json!(ctx.started_at.elapsed().as_millis()),
        ),
        ("message".to_string(), json!(message)),
        ("bodyTruncated".to_string(), json!(read.truncated)),
        ("bodyTimedOut".to_string(), json!(read.timed_out)),
    ]);
    if let Some(error) = body_read_error.as_deref() {
        fields.insert("bodyReadError".to_string(), json!(error));
    }
    if let Some(file_name) = error_file
        .as_deref()
        .and_then(Path::file_name)
        .and_then(std::ffi::OsStr::to_str)
    {
        fields.insert("errorFileName".to_string(), json!(file_name));
    }
    log.info("request_failed", Some(fields));

    let abnormal = read.truncated || read.timed_out || read.error.is_some();
    let bytes = if abnormal {
        parts.headers.remove(http::header::CONTENT_LENGTH);
        parts.headers.remove(http::header::CONTENT_ENCODING);
        parts.headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        serde_json::to_vec(&json!({
            "type": "error",
            "error": {
                "type": "api_error",
                "message": message,
            }
        }))
        .unwrap_or_else(|_| b"{\"type\":\"error\"}".to_vec())
    } else {
        read.bytes
    };

    (
        Response::from_parts(parts, Body::from(bytes)),
        Some(FailedResponseDetails { message }),
    )
}

struct BoundedErrorBody {
    bytes: Vec<u8>,
    truncated: bool,
    timed_out: bool,
    error: Option<String>,
}

async fn read_bounded_error_body(mut body: Body) -> BoundedErrorBody {
    read_bounded_error_body_with_timeouts(
        &mut body,
        ERROR_RESPONSE_BODY_IDLE_TIMEOUT,
        ERROR_RESPONSE_BODY_TOTAL_TIMEOUT,
    )
    .await
}

async fn read_bounded_error_body_with_timeouts(
    body: &mut Body,
    idle_timeout: Duration,
    total_timeout: Duration,
) -> BoundedErrorBody {
    let total_deadline = tokio::time::Instant::now() + total_timeout;
    let mut out = BoundedErrorBody {
        bytes: Vec::new(),
        truncated: false,
        timed_out: false,
        error: None,
    };
    loop {
        let frame = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(total_deadline) => {
                out.timed_out = true;
                break;
            }
            result = tokio::time::timeout(idle_timeout, body.frame()) => match result {
                Ok(frame) => frame,
                Err(_) => {
                    out.timed_out = true;
                    break;
                }
            }
        };
        match frame {
            None => break,
            Some(Err(error)) => {
                out.error = Some(error.to_string());
                break;
            }
            Some(Ok(frame)) => {
                let Some(data) = frame.data_ref() else {
                    continue;
                };
                let remaining = MAX_ERROR_RESPONSE_BODY_BYTES.saturating_sub(out.bytes.len());
                if data.len() > remaining {
                    out.bytes.extend_from_slice(&data[..remaining]);
                    out.truncated = true;
                    break;
                }
                out.bytes.extend_from_slice(data);
            }
        }
    }
    out
}

fn response_body_value(bytes: &[u8]) -> Value {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => json!({ "json": value }),
        Err(_) => json!({ "text": String::from_utf8_lossy(bytes) }),
    }
}

fn error_message_from_response(response_body: &Value) -> Option<String> {
    response_body
        .get("json")
        .and_then(|body| body.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .or_else(|| {
            response_body
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
        })
        .map(std::string::ToString::to_string)
}

fn sanitized_external_error(value: &str) -> String {
    crate::providers::translate_shared::sanitize_external_error_detail(value)
        .unwrap_or_else(|| "Upstream error".to_string())
}

fn should_capture_error_response(provider: Option<&str>, status: StatusCode) -> bool {
    provider.is_some() && status.is_server_error()
}

async fn write_error_capture(req_id: &str, document: Value) -> Result<Option<PathBuf>, String> {
    let gate = ERROR_CAPTURE_GATE
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone();
    let permit = match gate.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return Ok(None),
    };
    let req_id = req_id.to_string();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        write_error_capture_blocking(&req_id, &document)
    })
    .await
    .map_err(|error| format!("error capture task failed: {error}"))?
    .map(Some)
    .map_err(|error| error.to_string())
}

fn write_error_capture_blocking(req_id: &str, document: &Value) -> std::io::Result<PathBuf> {
    let dir = crate::paths::state_dir().join("errors");
    write_error_capture_in_dir(&dir, req_id, document)
}

fn write_error_capture_in_dir(
    dir: &Path,
    req_id: &str,
    document: &Value,
) -> std::io::Result<PathBuf> {
    let payload = serde_json::to_vec_pretty(document).map_err(std::io::Error::other)?;
    crate::fsutil::create_dir_all_with_mode(dir, 0o700)?;
    prune_error_captures(dir);
    let path = dir.join(format!("{}-{}.json", now_ms(), sanitize_path_part(req_id)));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    if let Err(write_error) = file.write_all(&payload).and_then(|_| file.write_all(b"\n")) {
        drop(file);
        let cleanup_error = fs::remove_file(&path)
            .err()
            .filter(|error| error.kind() != std::io::ErrorKind::NotFound);
        let detail = match cleanup_error {
            Some(cleanup_error) => format!(
                "failed to write error capture {}: {write_error}; failed to remove partial file: {cleanup_error}",
                path.display()
            ),
            None => format!(
                "failed to write error capture {}: {write_error}",
                path.display()
            ),
        };
        return Err(std::io::Error::new(write_error.kind(), detail));
    }
    Ok(path)
}

fn prune_error_captures(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut files: Vec<(std::time::SystemTime, PathBuf)> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                return None;
            }
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((modified, path))
        })
        .collect();
    files.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let remove = files
        .len()
        .saturating_add(1)
        .saturating_sub(MAX_ERROR_CAPTURE_FILES);
    for (_, path) in files.into_iter().take(remove) {
        let _ = fs::remove_file(path);
    }
}

fn sanitize_path_part(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn redact_error_value(value: Value) -> Value {
    redact_error_value_with_depth(value, 0)
}

fn redact_error_value_with_depth(value: Value, depth: u16) -> Value {
    if depth > MAX_ERROR_REDACTION_DEPTH {
        return Value::String("[depth-limit]".to_string());
    }
    match value {
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| redact_error_value_with_depth(value, depth + 1))
                .collect(),
        ),
        Value::Object(fields) => {
            let mut out = Map::new();
            for (key, value) in fields {
                if is_sensitive_payload_key(&key) {
                    out.insert(key, redact_error_key(value));
                } else {
                    out.insert(key, redact_error_value_with_depth(value, depth + 1));
                }
            }
            Value::Object(out)
        }
        Value::String(value) => Value::String(sanitized_external_error(&value)),
        value => value,
    }
}

fn redact_error_key(value: Value) -> Value {
    match value {
        Value::String(value) => Value::String(format!("[redacted len={}]", value.len())),
        _ => Value::String("[redacted]".to_string()),
    }
}

struct RequestMonitorGuard {
    monitor: Option<MonitorHandle>,
    req_id: String,
    log: Logger,
    started_at: Instant,
    count_tokens: bool,
    provider: Option<String>,
    model: Option<String>,
    terminal: bool,
}

impl RequestMonitorGuard {
    fn new(
        monitor: Option<MonitorHandle>,
        req_id: String,
        log: Logger,
        started_at: Instant,
        count_tokens: bool,
    ) -> Self {
        Self {
            monitor,
            req_id,
            log,
            started_at,
            count_tokens,
            provider: None,
            model: None,
            terminal: false,
        }
    }

    fn set_route(&mut self, provider: Option<&str>, model: Option<&str>) {
        self.provider = provider.map(str::to_string);
        self.model = model.map(str::to_string);
    }

    fn completed(&mut self, status: StatusCode) {
        self.terminal = true;
        if let Some(monitor) = self.monitor.take() {
            monitor.request_completed(&self.req_id, status.as_u16(), None, None);
        }
    }

    fn failed(&mut self, status: StatusCode, error: String) {
        self.terminal = true;
        if let Some(monitor) = self.monitor.take() {
            monitor.request_failed(&self.req_id, Some(status.as_u16()), error);
        }
    }

    fn abandoned(&mut self, error: &str) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        if let Some(monitor) = self.monitor.take() {
            monitor.request_abandoned(&self.req_id, error);
        }
        self.log.info(
            "request_abandoned",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), json!(&self.req_id)),
                ("provider".to_string(), json!(&self.provider)),
                ("model".to_string(), json!(&self.model)),
                ("countTokens".to_string(), json!(self.count_tokens)),
                (
                    "ms".to_string(),
                    json!(self.started_at.elapsed().as_millis()),
                ),
                ("message".to_string(), json!(error)),
            ])),
        );
    }
}

impl Drop for RequestMonitorGuard {
    fn drop(&mut self) {
        self.abandoned("Request future ended before completion");
    }
}

fn headers_to_record(headers: &http::HeaderMap) -> Value {
    let mut out = Map::new();
    for (key, value) in headers {
        let name = key.as_str();
        if name.eq_ignore_ascii_case(CLAUDE_CODE_SESSION_ID_HEADER) {
            let fingerprint = value
                .to_str()
                .ok()
                .map(|session_id| traffic_session_fingerprint(Some(session_id)));
            out.insert(
                name.to_string(),
                fingerprint.map_or_else(
                    || Value::String("[redacted invalid header]".to_string()),
                    |fingerprint| Value::String(format!("[fingerprint={fingerprint}]")),
                ),
            );
            continue;
        }
        if name.eq_ignore_ascii_case(CLAUDE_CODE_AGENT_ID_HEADER)
            || name.eq_ignore_ascii_case(CLAUDE_CODE_PARENT_AGENT_ID_HEADER)
            || is_sensitive_payload_key(name)
        {
            out.insert(
                name.to_string(),
                Value::String(format!("[redacted len={}]", value.as_bytes().len())),
            );
            continue;
        }
        if let Ok(raw) = value.to_str() {
            out.insert(name.to_string(), Value::String(raw.to_string()));
        }
    }
    Value::Object(out)
}

fn redacted_query(uri: &http::Uri) -> Value {
    let mut out = Map::new();
    let Some(query) = uri.query() else {
        return Value::Object(out);
    };
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let key = key.into_owned();
        let value = if is_sensitive_payload_key(&key) {
            Value::String(format!("[redacted len={}]", value.len()))
        } else {
            Value::String(value.into_owned())
        };
        out.insert(key, value);
    }
    Value::Object(out)
}

fn request_log_fields(
    req_id: &str,
    method: &http::Method,
    path: &str,
    uri: &http::Uri,
) -> Map<String, Value> {
    let query_parameter_count = uri
        .query()
        .map(|query| url::form_urlencoded::parse(query.as_bytes()).count())
        .unwrap_or(0);
    Map::from_iter([
        ("reqId".to_string(), json!(req_id)),
        ("method".to_string(), json!(method.as_str())),
        ("path".to_string(), json!(path)),
        ("queryPresent".to_string(), json!(uri.query().is_some())),
        (
            "queryParameterCount".to_string(),
            json!(query_parameter_count),
        ),
    ])
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, Box<Response>>
where
    T: DeserializeOwned,
{
    if body.is_empty() {
        return Err(Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "Invalid JSON: empty body",
        )));
    }

    serde_json::from_slice::<T>(body).map_err(|err| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("Invalid JSON: {err}"),
        ))
    })
}

async fn fallback_handler(method: axum::http::Method, uri: axum::http::Uri) -> Response {
    json_error(
        StatusCode::NOT_FOUND,
        "not_found",
        format!("No route for {method} {}", uri.path()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::{StreamExt, stream};

    #[test]
    fn server_listening_log_keeps_only_bounded_network_metadata() {
        let fields = server_listening_fields("127.0.0.1:18765".parse().unwrap());

        assert_eq!(fields.len(), 2);
        assert_eq!(fields["bindAddress"], "127.0.0.1");
        assert_eq!(fields["port"], 18_765);
        assert!(!fields.contains_key("logDir"));
    }

    #[test]
    fn request_query_redacts_all_shared_sensitive_keys_case_insensitively() {
        let uri: http::Uri = "/v1/messages?safe=value&client_secret=top-secret&PaSsWoRd=hunter2"
            .parse()
            .unwrap();
        let query = redacted_query(&uri);

        assert_eq!(query["safe"], "value");
        assert_eq!(query["client_secret"], "[redacted len=10]");
        assert_eq!(query["PaSsWoRd"], "[redacted len=7]");
    }

    #[test]
    fn ordinary_request_log_omits_dynamic_query_names_and_values() {
        let uri: http::Uri = "/v1/messages?customer-secret=CANARY_VALUE&safe=project-name"
            .parse()
            .unwrap();
        let fields = request_log_fields("request-id", &http::Method::POST, "/v1/messages", &uri);
        let encoded = serde_json::to_string(&fields).unwrap();

        assert_eq!(fields["queryPresent"], true);
        assert_eq!(fields["queryParameterCount"], 2);
        assert!(!fields.contains_key("query"));
        assert!(!encoded.contains("customer-secret"));
        assert!(!encoded.contains("CANARY_VALUE"));
        assert!(!encoded.contains("project-name"));
    }

    #[test]
    fn provider_generation_drift_warning_is_once_per_new_generation() {
        let warning = Arc::new(ConfigGenerationDriftWarning::new(7, true));
        assert!(!warning.observe(7));

        for generation in [8, 9] {
            let observations = (0..16)
                .map(|_| {
                    let warning = warning.clone();
                    std::thread::spawn(move || warning.observe(generation))
                })
                .collect::<Vec<_>>();
            let emitted = observations
                .into_iter()
                .map(|observation| observation.join().unwrap())
                .filter(|emitted| *emitted)
                .count();
            assert_eq!(emitted, 1, "generation {generation}");
        }
    }

    #[test]
    fn version_marks_provider_generation_drift_without_claiming_hot_swap() {
        let info = version_info_with_config_generations(7, 7, true, 9);
        assert_eq!(info["providerConstructionConfigGeneration"], 7);
        assert_eq!(info["providerConstructionConfigGenerationEnd"], 7);
        assert_eq!(info["providerConstructionSnapshotStable"], true);
        assert_eq!(info["configGeneration"], 9);
        assert_eq!(
            info["configGenerationChangedSinceProviderConstruction"],
            true
        );
        assert_eq!(
            info["configReload"]["status"],
            "generation_changed_check_restart_required_fields"
        );
    }

    #[test]
    fn version_requires_restart_when_provider_construction_snapshot_was_unstable() {
        let info = version_info_with_config_generations(7, 8, false, 8);
        assert_eq!(info["providerConstructionConfigGeneration"], 7);
        assert_eq!(info["providerConstructionConfigGenerationEnd"], 8);
        assert_eq!(info["providerConstructionSnapshotStable"], false);
        assert_eq!(
            info["configReload"]["status"],
            "provider_construction_snapshot_unstable_restart_required"
        );

        let warning = ConfigGenerationDriftWarning::new(8, false);
        assert!(warning.observe(8));
        assert!(!warning.observe(8));
    }

    fn compaction_request() -> crate::anthropic::schema::MessagesRequest {
        serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{
                "role": "user",
                "content": "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\nYour entire response must be plain text: an <analysis> block followed by a <summary> block.\nYour task is to create a detailed summary of the conversation so far."
            }]
        }))
        .unwrap()
    }

    #[test]
    fn compaction_override_requires_both_header_and_compaction_prompt() {
        let mut headers = HeaderMap::new();
        headers.insert(COMPACTION_MODEL_HEADER, "grok-4.5-high".parse().unwrap());

        assert_eq!(
            compaction_model_override(&headers, &compaction_request()).as_deref(),
            Some("grok-4.5-high")
        );

        let ordinary = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "normal work"}]
        }))
        .unwrap();
        assert!(compaction_model_override(&headers, &ordinary).is_none());
        assert!(compaction_model_override(&HeaderMap::new(), &compaction_request()).is_none());
    }

    fn response_log_context(req_id: &str) -> ResponseLogContext {
        ResponseLogContext {
            log: create_logger("server-test"),
            req_id: req_id.to_string(),
            provider: Some("test".to_string()),
            model: Some("test-model".to_string()),
            count_tokens: false,
            started_at: Instant::now(),
        }
    }

    fn started_monitor(req_id: &str) -> MonitorHandle {
        let monitor = MonitorHandle::new(10);
        monitor.request_started(req_id, None, None, EndpointKind::Messages);
        monitor
    }

    fn request_guard(monitor: MonitorHandle, req_id: &str) -> RequestMonitorGuard {
        RequestMonitorGuard::new(
            Some(monitor),
            req_id.to_string(),
            create_logger("server-test"),
            Instant::now(),
            false,
        )
    }

    fn response_lifecycle(req_id: &str) -> ResponseBodyLifecycle {
        let monitor = started_monitor(req_id);
        ResponseBodyLifecycle {
            guard: request_guard(monitor, req_id),
            log_context: response_log_context(req_id),
            status: StatusCode::OK,
            sse_detector: Some(SseErrorDetector::default()),
            open_tool_blocks: HashMap::new(),
            provisionally_closed_tool_blocks: HashMap::new(),
            observed_tool_block_starts: 0,
            tool_tracking_truncated: false,
            tool_tracking_disabled: false,
            saw_message_stop: false,
            _permits: RequestPermits::default(),
            terminal: false,
        }
    }

    #[tokio::test]
    async fn response_body_stays_active_until_message_stop_and_eof() {
        let req_id = "stream-success";
        let monitor = started_monitor(req_id);
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream::iter(vec![
                Ok::<_, std::io::Error>(Bytes::from_static(
                    b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
                )),
                Ok(Bytes::from_static(
                    b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
                )),
            ])))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
            RequestPermits::default(),
        );

        let state = monitor.snapshot();
        assert_eq!(state.active.len(), 1);
        assert!(state.recent.is_empty());

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("event: ping"));
        let state = monitor.snapshot();
        assert!(state.active.is_empty());
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].status,
            crate::monitor::RequestStatus::Completed
        );
        assert_eq!(state.recent[0].http_status, Some(200));
    }

    #[tokio::test]
    async fn aborted_sse_producer_surfaces_body_error_and_failed_request() {
        let req_id = "stream-producer-aborted";
        let monitor = started_monitor(req_id);
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
        let producer = tokio::spawn(async move {
            tx.send(Ok(Bytes::from_static(
                b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
            )))
            .await
            .unwrap();
            std::future::pending::<()>().await;
        });
        let producer_stream = stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(producer_stream))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
            RequestPermits::default(),
        );
        let mut body = response.into_body();

        let first = body
            .frame()
            .await
            .expect("producer should emit its first frame")
            .expect("first frame should be valid");
        assert!(
            first
                .data_ref()
                .is_some_and(|data| data.starts_with(b"event: ping"))
        );
        assert_eq!(monitor.snapshot().active.len(), 1);

        producer.abort();
        assert!(producer.await.unwrap_err().is_cancelled());

        let error = body
            .frame()
            .await
            .expect("abrupt producer exit must surface a body error")
            .expect_err("missing message_stop must not be a successful EOF");
        assert!(error.to_string().contains(INCOMPLETE_SSE_ERROR));
        assert!(body.frame().await.is_none());

        let state = monitor.snapshot();
        assert!(state.active.is_empty());
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].status,
            crate::monitor::RequestStatus::Failed
        );
        assert_eq!(state.recent[0].http_status, Some(200));
        assert_eq!(state.recent[0].error.as_deref(), Some(INCOMPLETE_SSE_ERROR));
    }

    #[tokio::test]
    async fn rejected_provider_response_holds_admission_permit_until_body_drop() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let response = hold_response_permits(
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Body::from("bounded error"))
                .unwrap(),
            RequestPermits {
                global: Some(permit),
                ..RequestPermits::default()
            },
        );
        assert!(semaphore.clone().try_acquire_owned().is_err());
        drop(response);
        assert!(semaphore.try_acquire_owned().is_ok());
    }

    #[test]
    fn buffered_request_budget_follows_request_sized_provider_material_only() {
        let request_bytes = Arc::new(Semaphore::new(1));
        let global = Arc::new(Semaphore::new(1));
        let mut permits = RequestPermits {
            global: Some(global.clone().try_acquire_owned().unwrap()),
            request_bytes: Some(request_bytes.clone().try_acquire_owned().unwrap()),
            ..RequestPermits::default()
        };

        let server_lease = RequestByteLease::new(permits.request_bytes.take().unwrap(), 1);
        let provider_material_lease = server_lease.clone();
        drop(server_lease);

        assert!(request_bytes.clone().try_acquire_owned().is_err());
        assert!(global.try_acquire_owned().is_err());
        drop(provider_material_lease);
        assert!(request_bytes.try_acquire_owned().is_ok());
    }

    #[tokio::test]
    async fn split_in_band_sse_error_is_a_failed_request() {
        let req_id = "stream-error";
        let monitor = started_monitor(req_id);
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"event: er")),
            Ok(Bytes::from_static(
                b"ror\r\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"deadline exceeded\"}}\r",
            )),
            Ok(Bytes::from_static(b"\n\r\n")),
            Ok(Bytes::from_static(
                b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            )),
        ];
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(
                http::header::CONTENT_TYPE,
                "text/event-stream; charset=utf-8",
            )
            .body(Body::from_stream(stream::iter(chunks)))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
            RequestPermits::default(),
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("deadline exceeded"));
        assert!(!body.contains("message_stop"));
        let state = monitor.snapshot();
        assert!(state.active.is_empty());
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].status,
            crate::monitor::RequestStatus::Failed
        );
        assert_eq!(state.recent[0].http_status, Some(200));
        assert_eq!(state.recent[0].error.as_deref(), Some("deadline exceeded"));
    }

    #[tokio::test]
    async fn same_frame_in_band_sse_error_drops_every_trailing_event() {
        let req_id = "same-frame-stream-error";
        let monitor = started_monitor(req_id);
        let bytes = Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\n\
event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"bounded failure\"}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"MUST_NOT_ESCAPE\"}}\n\n\
event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        );
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(bytes))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
            RequestPermits::default(),
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("message_start"));
        assert!(body.contains("bounded failure"));
        assert!(!body.contains("MUST_NOT_ESCAPE"));
        assert!(!body.contains("message_stop"));

        let state = monitor.snapshot();
        assert!(state.active.is_empty());
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].status,
            crate::monitor::RequestStatus::Failed
        );
        assert_eq!(state.recent[0].error.as_deref(), Some("bounded failure"));
    }

    #[tokio::test]
    async fn in_band_sse_error_keeps_client_bytes_but_redacts_monitor_detail() {
        let req_id = "stream-secret-error";
        let monitor = started_monitor(req_id);
        let secret = "Bearer upstream-secret at /home/customer/private.txt";
        let payload = format!(
            "event: error\ndata: {{\"type\":\"error\",\"error\":{{\"message\":{}}}}}\n\n",
            serde_json::to_string(secret).unwrap()
        );
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(payload))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
            RequestPermits::default(),
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains(secret));
        let state = monitor.snapshot();
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].error.as_deref(),
            Some("[redacted upstream error detail]")
        );
        assert!(!state.recent[0].error.as_deref().unwrap().contains(secret));
    }

    #[tokio::test]
    async fn failed_response_detail_is_redacted_without_rewriting_bounded_client_body() {
        let secret = "Bearer upstream-secret at /home/customer/private.txt";
        let body = serde_json::to_vec(&json!({
            "type":"error",
            "error":{"type":"api_error", "message":secret}
        }))
        .unwrap();
        let response = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.clone()))
            .unwrap();
        let log = create_logger("server");
        let (response, details) = record_failed_response(
            &log,
            FailedResponseLogContext {
                req_id: "failed-secret-response",
                provider: None,
                model: None,
                count_tokens: false,
                started_at: Instant::now(),
            },
            response,
        )
        .await;

        assert_eq!(details.unwrap().message, "[redacted upstream error detail]");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
            Bytes::from(body)
        );
    }

    #[tokio::test]
    async fn non_sse_error_shaped_json_remains_a_successful_body() {
        let req_id = "json-success";
        let monitor = started_monitor(req_id);
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"type":"error","error":{"message":"ordinary payload"}}"#,
            ))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
            RequestPermits::default(),
        );

        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let state = monitor.snapshot();
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].status,
            crate::monitor::RequestStatus::Completed
        );
    }

    #[test]
    fn cr_only_sse_error_is_detected() {
        let mut detector = SseErrorDetector::default();
        let error = detector
            .push(
                b"event: error\rdata: {\"type\":\"error\",\"error\":{\"message\":\"cr deadline\"}}\r\r",
            )
            .expect("CR-only SSE must dispatch an error event");

        assert_eq!(error, "cr deadline");
    }

    #[test]
    fn data_type_error_without_event_field_is_detected() {
        let mut detector = SseErrorDetector::default();
        let error = detector
            .push(b"data: {\"type\":\"error\",\"message\":\"top-level failure\"}\n\n")
            .expect("the JSON type must identify an error without an event field");

        assert_eq!(error, "top-level failure");
    }

    #[test]
    fn malformed_error_detail_uses_the_bounded_fallback() {
        let mut detector = SseErrorDetector::default();
        let error = detector
            .push(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":17}}\n\n")
            .expect("the SSE event field must still identify an error");

        assert_eq!(error, "Upstream stream returned an error event");
    }

    #[test]
    fn narrow_probe_preserves_duplicate_key_and_message_whitespace_semantics() {
        let mut detector = SseErrorDetector::default();
        let error = detector
            .push(
                b"data: {\"type\":\"message_stop\",\"type\":\"error\",\"message\":\"  last wins  \"}\n\n",
            )
            .expect("the final duplicate type must retain Value last-wins behavior");
        assert_eq!(error, "  last wins  ");

        assert!(detector
            .push(
                b"data: {\"type\":\"content_block_start\",\"index\":1,\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-two\",\"name\":\"Read\"}}\n\n"
            )
            .is_none());
        let events = detector.take_tool_events();
        let [SseToolEvent::Started(tool)] = events.as_slice() else {
            panic!("expected one tool start from the final duplicate index");
        };
        assert_eq!(tool.index, 2);

        let error = detector
            .push(
                b"event: error\ndata: {\"type\":\"error\",\"message\":\"first\",\"message\":\"second\"}\n\n",
            )
            .expect("the final duplicate message must be retained");
        assert_eq!(error, "second");
    }

    #[test]
    fn whitespace_only_error_message_uses_the_bounded_fallback() {
        let mut detector = SseErrorDetector::default();
        let error = detector
            .push(b"event: error\ndata: {\"type\":\"error\",\"message\":\"   \"}\n\n")
            .expect("the event field must identify the error");

        assert_eq!(error, "Upstream stream returned an error event");
    }

    #[test]
    fn sse_tool_lifecycle_keeps_only_safe_metadata() {
        let mut detector = SseErrorDetector::default();
        assert!(
            detector
                .push(
                    b"event: content_block_start\n\
data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-secret-123\",\"name\":\"Brave Search; bad\",\"input\":{\"query\":\"CANARY_PROMPT\"}}}\n\n",
                )
                .is_none()
        );

        let events = detector.take_tool_events();
        assert_eq!(events.len(), 1);
        let SseToolEvent::Started(tool) = &events[0] else {
            panic!("expected a tool start event");
        };
        assert_eq!(tool.index, 2);
        assert_eq!(tool.tool_kind, "tool_use");
        assert_eq!(
            tool.tool_name_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("category"))
                .and_then(Value::as_str),
            Some("function")
        );
        assert_eq!(
            tool.tool_name_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("fingerprint"))
                .and_then(Value::as_str)
                .map(str::len),
            Some(16)
        );
        assert_eq!(tool.call_id_hash.as_deref().map(str::len), Some(16));
        assert_ne!(tool.call_id_hash.as_deref(), Some("call-secret-123"));
        let serialized = serde_json::to_string(&format!("{tool:?}")).unwrap();
        assert!(!serialized.contains("Brave_Search__bad"));
        assert!(!serialized.contains("CANARY_PROMPT"));

        assert!(
            detector
                .push(
                    b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"query\\\":\\\"CANARY_ARGUMENT\\\"}\"}}\n\n",
                )
                .is_none()
        );
        assert!(detector.take_tool_events().is_empty());

        assert!(
            detector
                .push(
                    b"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":2}\n\n",
                )
                .is_none()
        );
        assert_eq!(
            detector.take_tool_events(),
            vec![SseToolEvent::Stopped { index: 2 }]
        );

        assert!(
            detector
                .push(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
                .is_none()
        );
        assert_eq!(
            detector.take_tool_events(),
            vec![SseToolEvent::MessageStopped]
        );
    }

    #[test]
    fn client_tool_result_diagnostics_stop_before_log_formatting_can_amplify() {
        let blocks = (0..(MAX_TRACKED_CLIENT_TOOL_RESULTS + 5))
            .map(|index| {
                json!({
                    "type":"tool_result",
                    "tool_use_id":format!("call-{index}"),
                    "content":format!("result-{index}"),
                    "is_error":index % 2 == 0
                })
            })
            .collect::<Vec<_>>();
        let request: crate::anthropic::schema::MessagesRequest = serde_json::from_value(json!({
            "model":"gpt-5.6-sol",
            "max_tokens":32,
            "messages":[{"role":"user","content":blocks}]
        }))
        .unwrap();
        let mut exact_request = request.clone();
        exact_request
            .messages
            .last_mut()
            .unwrap()
            .content
            .as_array_mut()
            .unwrap()
            .truncate(MAX_TRACKED_CLIENT_TOOL_RESULTS);
        let mut exact_count = 0_usize;
        assert_eq!(
            visit_client_tool_results(&exact_request, |_| exact_count += 1),
            None
        );
        assert_eq!(exact_count, MAX_TRACKED_CLIENT_TOOL_RESULTS);

        let mut observed = Vec::new();

        let truncated_message_index =
            visit_client_tool_results(&request, |result| observed.push(result));

        assert_eq!(
            truncated_message_index,
            Some((0, MAX_TRACKED_CLIENT_TOOL_RESULTS))
        );
        assert_eq!(observed.len(), MAX_TRACKED_CLIENT_TOOL_RESULTS);
        assert_eq!(observed.first().unwrap().block_index, 0);
        assert_eq!(
            observed.last().unwrap().block_index,
            MAX_TRACKED_CLIENT_TOOL_RESULTS - 1
        );
        assert!(
            observed
                .iter()
                .all(|result| result.call_id_hash.len() == 16)
        );
        assert!(observed.iter().all(|result| result.content_bytes > 0));
    }

    #[test]
    fn malformed_tool_results_cannot_bypass_the_request_diagnostic_cap() {
        let mut blocks = (0..MAX_TRACKED_CLIENT_TOOL_RESULTS)
            .map(|_| json!({"type":"tool_result","content":"missing id"}))
            .collect::<Vec<_>>();
        blocks.push(json!({
            "type":"tool_result",
            "tool_use_id":"call-after-budget",
            "content":"must not be inspected"
        }));
        let request: crate::anthropic::schema::MessagesRequest = serde_json::from_value(json!({
            "model":"gpt-5.6-sol",
            "max_tokens":32,
            "messages":[{"role":"user","content":blocks}]
        }))
        .unwrap();
        let mut observed = Vec::new();

        let truncated = visit_client_tool_results(&request, |result| observed.push(result));

        assert_eq!(truncated, Some((0, 0)));
        assert!(observed.is_empty());
    }

    #[test]
    fn tool_stop_is_provisional_until_a_clean_message_stop() {
        let tool = ToolBlockTrace {
            index: 2,
            tool_kind: "tool_use".to_string(),
            tool_name_metadata: safe_tool_name_metadata("brave_search"),
            call_id_hash: Some("1234abcd".to_string()),
        };

        let mut failed = response_lifecycle("provisional-tool-failure");
        failed.observe_tool_event(SseToolEvent::Started(tool.clone()));
        failed.observe_tool_event(SseToolEvent::Stopped { index: 2 });
        assert!(failed.open_tool_blocks.is_empty());
        assert_eq!(failed.provisionally_closed_tool_blocks.get(&2), Some(&tool));
        failed.failed("upstream closed".to_string(), true);
        assert!(failed.provisionally_closed_tool_blocks.is_empty());

        let mut completed = response_lifecycle("provisional-tool-success");
        completed.observe_tool_event(SseToolEvent::Started(tool));
        completed.observe_tool_event(SseToolEvent::Stopped { index: 2 });
        completed.observe_tool_event(SseToolEvent::MessageStopped);
        assert!(completed.open_tool_blocks.is_empty());
        assert!(completed.provisionally_closed_tool_blocks.is_empty());
        completed.completed();
    }

    #[test]
    fn pending_tool_events_are_bounded_without_losing_message_stop() {
        let mut detector = SseErrorDetector::default();
        for index in 0..(MAX_PENDING_TOOL_LIFECYCLE_EVENTS + 5) {
            let event = format!(
                "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":{index},\"content_block\":{{\"type\":\"tool_use\",\"id\":\"call-{index}\",\"name\":\"Read\"}}}}\n\n"
            );
            assert!(detector.push(event.as_bytes()).is_none());
        }
        assert!(
            detector
                .push(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
                .is_none()
        );

        let events = detector.take_tool_events();
        assert_eq!(events.len(), MAX_PENDING_TOOL_LIFECYCLE_EVENTS + 2);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SseToolEvent::Started(_)))
                .count(),
            MAX_PENDING_TOOL_LIFECYCLE_EVENTS
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SseToolEvent::MessageStopped))
                .count(),
            1
        );
        assert_eq!(events.first(), Some(&SseToolEvent::TrackingTruncated));

        let mut lifecycle = response_lifecycle("pending-tool-event-overflow");
        for event in events {
            lifecycle.observe_tool_event(event);
        }
        assert!(lifecycle.tool_tracking_disabled);
        assert!(lifecycle.tool_tracking_truncated);
        assert!(lifecycle.saw_message_stop);
        assert!(lifecycle.open_tool_blocks.is_empty());
        assert!(lifecycle.provisionally_closed_tool_blocks.is_empty());

        assert!(detector
            .push(
                b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":999,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call-reset\",\"name\":\"Read\"}}\n\n"
            )
            .is_none());
        let reset_events = detector.take_tool_events();
        assert_eq!(reset_events.len(), 1);
        assert!(matches!(reset_events[0], SseToolEvent::Started(_)));
    }

    #[tokio::test]
    async fn tool_event_overflow_preserves_downstream_bytes_and_clean_completion() {
        let req_id = "tool-overflow-stream";
        let monitor = started_monitor(req_id);
        let mut raw = String::new();
        for index in 0..(MAX_PENDING_TOOL_LIFECYCLE_EVENTS + 5) {
            raw.push_str(&format!(
                "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":{index},\"content_block\":{{\"type\":\"tool_use\",\"id\":\"call-{index}\",\"name\":\"Read\"}}}}\n\n"
            ));
        }
        raw.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
        let raw = Bytes::from(raw);
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(raw.clone()))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
            RequestPermits::default(),
        );

        let downstream = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(downstream, raw);
        let state = monitor.snapshot();
        assert!(state.active.is_empty());
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].status,
            crate::monitor::RequestStatus::Completed
        );
        assert_eq!(state.recent[0].http_status, Some(200));
    }

    #[test]
    fn response_tool_tracking_has_a_hard_event_and_state_cap() {
        let mut lifecycle = response_lifecycle("bounded-tool-tracking");
        for index in 0..MAX_TRACKED_TOOL_BLOCK_STARTS {
            lifecycle.observe_tool_event(SseToolEvent::Started(ToolBlockTrace {
                index: index as u64,
                tool_kind: "tool_use".to_string(),
                tool_name_metadata: safe_tool_name_metadata("Read"),
                call_id_hash: Some(format!("{index:016x}")),
            }));
            if index < MAX_TRACKED_TOOL_BLOCK_STARTS / 2 {
                lifecycle.observe_tool_event(SseToolEvent::Stopped {
                    index: index as u64,
                });
            }
        }

        assert_eq!(
            lifecycle.open_tool_blocks.len(),
            MAX_TRACKED_TOOL_BLOCK_STARTS / 2
        );
        assert_eq!(
            lifecycle.provisionally_closed_tool_blocks.len(),
            MAX_TRACKED_TOOL_BLOCK_STARTS / 2
        );
        assert!(!lifecycle.tool_tracking_truncated);
        assert!(!lifecycle.tool_tracking_disabled);

        lifecycle.observe_tool_event(SseToolEvent::Started(ToolBlockTrace {
            index: MAX_TRACKED_TOOL_BLOCK_STARTS as u64,
            tool_kind: "tool_use".to_string(),
            tool_name_metadata: safe_tool_name_metadata("Read"),
            call_id_hash: Some("ffffffffffffffff".to_string()),
        }));
        assert_eq!(
            lifecycle.observed_tool_block_starts,
            MAX_TRACKED_TOOL_BLOCK_STARTS
        );
        assert!(lifecycle.open_tool_blocks.is_empty());
        assert!(lifecycle.tool_tracking_truncated);
        assert!(lifecycle.tool_tracking_disabled);

        lifecycle.observe_tool_event(SseToolEvent::Started(ToolBlockTrace {
            index: u64::MAX,
            tool_kind: "tool_use".to_string(),
            tool_name_metadata: safe_tool_name_metadata("Read"),
            call_id_hash: Some("eeeeeeeeeeeeeeee".to_string()),
        }));
        lifecycle.observe_tool_event(SseToolEvent::Stopped { index: u64::MAX });
        assert!(lifecycle.open_tool_blocks.is_empty());
        assert!(lifecycle.provisionally_closed_tool_blocks.is_empty());

        lifecycle.observe_tool_event(SseToolEvent::MessageStopped);
        assert!(lifecycle.saw_message_stop);
        assert!(lifecycle.open_tool_blocks.is_empty());
        assert!(lifecycle.provisionally_closed_tool_blocks.is_empty());
        lifecycle.completed();
    }

    #[test]
    fn index_reuse_cannot_bypass_the_cumulative_start_cap() {
        let mut lifecycle = response_lifecycle("bounded-index-reuse");
        let tool = ToolBlockTrace {
            index: 7,
            tool_kind: "tool_use".to_string(),
            tool_name_metadata: safe_tool_name_metadata("Read"),
            call_id_hash: Some("7777777777777777".to_string()),
        };
        for _ in 0..MAX_TRACKED_TOOL_BLOCK_STARTS {
            lifecycle.observe_tool_event(SseToolEvent::Started(tool.clone()));
        }
        assert_eq!(lifecycle.open_tool_blocks.len(), 1);
        assert!(!lifecycle.tool_tracking_disabled);

        lifecycle.observe_tool_event(SseToolEvent::Started(tool));
        assert_eq!(
            lifecycle.observed_tool_block_starts,
            MAX_TRACKED_TOOL_BLOCK_STARTS
        );
        assert!(lifecycle.open_tool_blocks.is_empty());
        assert!(lifecycle.tool_tracking_disabled);
        assert!(lifecycle.tool_tracking_truncated);
        lifecycle.completed();
    }

    #[test]
    fn message_stop_does_not_reset_the_cumulative_start_cap() {
        let mut lifecycle = response_lifecycle("bounded-multiple-message-stops");
        for window in 0..2_u64 {
            for offset in 0..(MAX_TRACKED_TOOL_BLOCK_STARTS / 2) as u64 {
                let index = window * 1_000 + offset;
                lifecycle.observe_tool_event(SseToolEvent::Started(ToolBlockTrace {
                    index,
                    tool_kind: "tool_use".to_string(),
                    tool_name_metadata: safe_tool_name_metadata("Read"),
                    call_id_hash: Some(format!("{index:016x}")),
                }));
                lifecycle.observe_tool_event(SseToolEvent::Stopped { index });
            }
            lifecycle.observe_tool_event(SseToolEvent::MessageStopped);
        }
        assert_eq!(
            lifecycle.observed_tool_block_starts,
            MAX_TRACKED_TOOL_BLOCK_STARTS
        );
        assert!(!lifecycle.tool_tracking_disabled);

        lifecycle.observe_tool_event(SseToolEvent::Started(ToolBlockTrace {
            index: u64::MAX,
            tool_kind: "tool_use".to_string(),
            tool_name_metadata: safe_tool_name_metadata("Read"),
            call_id_hash: Some("ffffffffffffffff".to_string()),
        }));
        assert!(lifecycle.tool_tracking_disabled);
        assert!(lifecycle.open_tool_blocks.is_empty());
        assert!(lifecycle.provisionally_closed_tool_blocks.is_empty());
        lifecycle.completed();
    }

    #[test]
    fn tracked_stops_do_not_consume_the_cumulative_start_budget() {
        let mut lifecycle = response_lifecycle("bounded-start-budget");
        for index in 0..MAX_TRACKED_TOOL_BLOCK_STARTS {
            lifecycle.observe_tool_event(SseToolEvent::Started(ToolBlockTrace {
                index: index as u64,
                tool_kind: "tool_use".to_string(),
                tool_name_metadata: safe_tool_name_metadata("Read"),
                call_id_hash: Some(format!("{index:016x}")),
            }));
            lifecycle.observe_tool_event(SseToolEvent::Stopped {
                index: index as u64,
            });
        }

        assert_eq!(
            lifecycle.observed_tool_block_starts,
            MAX_TRACKED_TOOL_BLOCK_STARTS
        );
        assert!(!lifecycle.tool_tracking_disabled);
        assert!(!lifecycle.tool_tracking_truncated);
        assert!(lifecycle.open_tool_blocks.is_empty());
        assert_eq!(
            lifecycle.provisionally_closed_tool_blocks.len(),
            MAX_TRACKED_TOOL_BLOCK_STARTS
        );

        lifecycle.observe_tool_event(SseToolEvent::MessageStopped);
        assert!(lifecycle.saw_message_stop);
        assert!(lifecycle.provisionally_closed_tool_blocks.is_empty());
        lifecycle.completed();
    }

    #[test]
    fn tool_block_log_schema_never_keeps_the_dynamic_name() {
        let dynamic_name = "mcp__customer_alpha__lookup";
        let tool = ToolBlockTrace {
            index: 7,
            tool_kind: "tool_use".to_string(),
            tool_name_metadata: safe_tool_name_metadata(dynamic_name),
            call_id_hash: Some("1234abcd5678ef90".to_string()),
        };

        for (event, reason) in [
            ("tool_block_started", None),
            ("tool_block_completed", None),
            ("tool_block_interrupted", Some("downstream_dropped")),
        ] {
            let fields = tool_block_metadata_fields(&tool, reason);
            assert!(fields.get("toolName").is_none(), "{event}");
            assert_eq!(fields["toolNameMetadata"]["category"], "mcp", "{event}");
            let fingerprint = fields["toolNameMetadata"]["fingerprint"]
                .as_str()
                .expect("tool-name fingerprint");
            assert_eq!(fingerprint.len(), 16, "{event}");
            assert!(
                fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "{event}"
            );
            assert_eq!(
                fields.get("interruptReason").and_then(Value::as_str),
                reason,
                "{event}"
            );
            assert!(!Value::Object(fields).to_string().contains(dynamic_name));
        }
    }

    #[test]
    fn oversized_many_line_event_is_discarded_and_parser_recovers() {
        let mut detector = SseErrorDetector::default();
        for _ in 0..(MAX_SSE_ERROR_EVENT_BYTES / 5 + 10) {
            assert!(detector.push(b"data:\n").is_none());
        }
        assert!(detector.discard_event);
        assert!(detector.data.is_empty());
        assert!(detector.push(b"\n").is_none());

        let error = detector
            .push(
                b"event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"recovered\"}}\n\n",
            )
            .expect("the parser should recover after the oversized event boundary");
        assert_eq!(error, "recovered");
    }

    #[test]
    fn large_semantic_delta_is_ignored_by_the_narrow_diagnostic_probe() {
        let mut detector = SseErrorDetector::default();
        let partial_json = "x".repeat(200 * 1024);
        let event = format!(
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"input_json_delta\",\"partial_json\":\"{partial_json}\"}}}}\n\n"
        );

        assert!(detector.push(event.as_bytes()).is_none());
        assert!(detector.take_tool_events().is_empty());
        assert!(detector.take_error_consumed_bytes().is_none());
        assert!(detector.line.is_empty());
        assert!(detector.data.is_empty());
    }

    #[test]
    fn local_rejections_and_rate_limits_do_not_create_error_capture_files() {
        assert!(!should_capture_error_response(
            None,
            StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(!should_capture_error_response(
            Some("grok"),
            StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(!should_capture_error_response(
            Some("codex"),
            StatusCode::PAYLOAD_TOO_LARGE
        ));
        assert!(should_capture_error_response(
            Some("codex"),
            StatusCode::BAD_GATEWAY
        ));
    }

    #[test]
    fn session_id_length_boundary_is_enforced() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-claude-code-session-id",
            http::HeaderValue::from_static(""),
        );
        assert_eq!(session_id_from_headers(&headers).unwrap(), None);

        headers.insert(
            "x-claude-code-session-id",
            http::HeaderValue::from_bytes(&[0x80]).unwrap(),
        );
        assert_eq!(
            session_id_from_headers(&headers),
            Err(SessionIdError::InvalidEncoding)
        );

        let accepted = "s".repeat(MAX_SESSION_ID_BYTES);
        headers.insert(
            "x-claude-code-session-id",
            http::HeaderValue::from_str(&accepted).unwrap(),
        );
        assert_eq!(
            session_id_from_headers(&headers).unwrap().as_deref(),
            Some(accepted.as_str())
        );

        let rejected = "s".repeat(MAX_SESSION_ID_BYTES + 1);
        headers.insert(
            "x-claude-code-session-id",
            http::HeaderValue::from_str(&rejected).unwrap(),
        );
        assert!(session_id_from_headers(&headers).is_err());
    }

    #[test]
    fn agent_id_header_is_strict_and_length_bounded() {
        let mut headers = HeaderMap::new();
        assert_eq!(agent_id_from_headers(&headers).unwrap(), None);

        headers.insert(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_static(""),
        );
        assert_eq!(agent_id_from_headers(&headers), Err(AgentIdError::Empty));

        headers.insert(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_bytes(&[0x80]).unwrap(),
        );
        assert_eq!(
            agent_id_from_headers(&headers),
            Err(AgentIdError::InvalidEncoding)
        );

        headers.insert(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_static("agent id"),
        );
        assert_eq!(
            agent_id_from_headers(&headers),
            Err(AgentIdError::InvalidEncoding)
        );

        let accepted = "a".repeat(MAX_AGENT_ID_BYTES);
        headers.insert(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_str(&accepted).unwrap(),
        );
        assert_eq!(
            agent_id_from_headers(&headers).unwrap().as_deref(),
            Some(accepted.as_str())
        );

        let rejected = "a".repeat(MAX_AGENT_ID_BYTES + 1);
        headers.insert(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_str(&rejected).unwrap(),
        );
        assert_eq!(agent_id_from_headers(&headers), Err(AgentIdError::TooLong));

        headers.clear();
        headers.append(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_static("agent-a"),
        );
        headers.append(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_static("agent-b"),
        );
        assert_eq!(
            agent_id_from_headers(&headers),
            Err(AgentIdError::InvalidEncoding)
        );
    }

    #[test]
    fn request_lane_key_is_stable_opaque_and_agent_scoped() {
        let session_id = "57c7c914-ada4-4f40-9672-985f950fbb66";
        let agent_a = "agent-ae50bd4cae9d19761";
        let agent_b = "agent-be50bd4cae9d19762";

        let main = request_lane_key(Some(session_id), None).unwrap();
        let resumed_main = request_lane_key(Some(session_id), None).unwrap();
        let first_agent = request_lane_key(Some(session_id), Some(agent_a)).unwrap();
        let resumed_agent = request_lane_key(Some(session_id), Some(agent_a)).unwrap();
        let sibling_agent = request_lane_key(Some(session_id), Some(agent_b)).unwrap();
        let other_session = request_lane_key(Some("other-session"), Some(agent_a)).unwrap();

        assert_eq!(main, resumed_main);
        assert_eq!(first_agent, resumed_agent);
        assert_ne!(main, first_agent);
        assert_ne!(first_agent, sibling_agent);
        assert_ne!(first_agent, other_session);
        assert_eq!(main.as_bytes().len(), 32);
        assert_eq!(main.to_hex().len(), 64);
        assert_eq!(format!("{main:?}"), "RequestLaneKey([opaque])");
        assert!(!main.to_hex().contains(session_id));
        assert!(!first_agent.to_hex().contains(agent_a));
        assert_eq!(request_lane_key(None, None), None);
        assert_eq!(request_lane_key(None, Some(agent_a)), None);
    }

    #[test]
    fn traffic_headers_redact_identity_and_credentials() {
        let session_id = "session-raw-canary-46c956c91e29";
        let agent_id = "agent-raw-canary-ae50bd4cae9d19761";
        let parent_agent_id = "parent-agent-raw-canary";
        let mut headers = HeaderMap::new();
        headers.insert(
            CLAUDE_CODE_SESSION_ID_HEADER,
            http::HeaderValue::from_str(session_id).unwrap(),
        );
        headers.insert(
            CLAUDE_CODE_AGENT_ID_HEADER,
            http::HeaderValue::from_str(agent_id).unwrap(),
        );
        headers.insert(
            CLAUDE_CODE_PARENT_AGENT_ID_HEADER,
            http::HeaderValue::from_str(parent_agent_id).unwrap(),
        );
        headers.insert(
            "x-safe-header",
            http::HeaderValue::from_static("safe-value"),
        );
        headers.insert(
            "authorization",
            http::HeaderValue::from_static("Bearer traffic-secret"),
        );
        headers.insert(
            "x-api-key",
            http::HeaderValue::from_static("traffic-api-secret"),
        );

        let recorded = headers_to_record(&headers);
        assert_eq!(
            recorded[CLAUDE_CODE_SESSION_ID_HEADER],
            format!(
                "[fingerprint={}]",
                traffic_session_fingerprint(Some(session_id))
            )
        );
        assert_eq!(
            recorded[CLAUDE_CODE_AGENT_ID_HEADER],
            format!("[redacted len={}]", agent_id.len())
        );
        assert_eq!(
            recorded[CLAUDE_CODE_PARENT_AGENT_ID_HEADER],
            format!("[redacted len={}]", parent_agent_id.len())
        );
        assert_eq!(recorded["authorization"], "[redacted len=21]");
        assert_eq!(recorded["x-api-key"], "[redacted len=18]");
        assert_eq!(recorded["x-safe-header"], "safe-value");
        let serialized = serde_json::to_string(&recorded).unwrap();
        assert!(!serialized.contains(session_id));
        assert!(!serialized.contains(agent_id));
        assert!(!serialized.contains(parent_agent_id));
        assert!(!serialized.contains("traffic-secret"));
        assert!(!serialized.contains("traffic-api-secret"));
    }

    #[tokio::test]
    async fn oversized_session_id_is_rejected_before_admission_state_grows() {
        let limits = ServerLimits::default();
        let registry = Arc::new(Registry::with_default_alias());
        let provider_construction_config_generation = registry.construction_config_generation();
        let provider_construction_config_generation_end =
            registry.construction_config_generation_end();
        let provider_construction_config_snapshot_stable =
            registry.construction_config_snapshot_stable();
        let monitor = MonitorHandle::new(10);
        let state = Arc::new(AppState {
            registry,
            monitor: Some(monitor.clone()),
            admission: AdmissionState::new(&limits),
            limits,
            provider_construction_config_generation,
            provider_construction_config_generation_end,
            provider_construction_config_snapshot_stable,
            config_generation_drift_warning: ConfigGenerationDriftWarning::new(
                provider_construction_config_generation,
                provider_construction_config_snapshot_stable,
            ),
        });
        let oversized = "SESSION_CANARY".repeat(MAX_SESSION_ID_BYTES / 8 + 1);
        assert!(oversized.len() > MAX_SESSION_ID_BYTES);
        let request = Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(http::header::CONTENT_TYPE, "application/json")
            .header("x-claude-code-session-id", &oversized)
            .body(Body::from(
                r#"{"model":"gpt-5.5","messages":[{"role":"user","content":"test"}]}"#,
            ))
            .unwrap();

        let response = dispatch_request_with_id(
            state.clone(),
            request,
            false,
            "oversized-session-test".to_string(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("exceeds the 256-byte limit"));
        assert!(!body.contains(&oversized));
        assert!(state.admission.sessions.lock().unwrap().is_empty());

        let snapshot = monitor.snapshot();
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.recent.len(), 1);
        assert!(snapshot.recent[0].session_id.is_none());
    }

    #[tokio::test]
    async fn malformed_agent_ids_are_rejected_before_admission_state_grows() {
        let limits = ServerLimits::default();
        let registry = Arc::new(Registry::with_default_alias());
        let provider_construction_config_generation = registry.construction_config_generation();
        let provider_construction_config_generation_end =
            registry.construction_config_generation_end();
        let provider_construction_config_snapshot_stable =
            registry.construction_config_snapshot_stable();
        let state = Arc::new(AppState {
            registry,
            monitor: None,
            admission: AdmissionState::new(&limits),
            limits,
            provider_construction_config_generation,
            provider_construction_config_generation_end,
            provider_construction_config_snapshot_stable,
            config_generation_drift_warning: ConfigGenerationDriftWarning::new(
                provider_construction_config_generation,
                provider_construction_config_snapshot_stable,
            ),
        });
        let oversized = "AGENT_ID_RAW_CANARY".repeat(MAX_AGENT_ID_BYTES / 8 + 1);
        assert!(oversized.len() > MAX_AGENT_ID_BYTES);
        let cases = [
            (
                http::HeaderValue::from_static(""),
                "must not be empty",
                None,
            ),
            (
                http::HeaderValue::from_bytes(&[0x80]).unwrap(),
                "must contain valid visible ASCII text",
                None,
            ),
            (
                http::HeaderValue::from_str(&oversized).unwrap(),
                "exceeds the 256-byte limit",
                Some(oversized.as_str()),
            ),
        ];

        for (index, (agent_id, expected_error, raw_canary)) in cases.into_iter().enumerate() {
            let request = Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header(http::header::CONTENT_TYPE, "application/json")
                .header("x-claude-code-session-id", "agent-validation-session")
                .header(CLAUDE_CODE_AGENT_ID_HEADER, agent_id)
                .body(Body::from(
                    r#"{"model":"gpt-5.5","messages":[{"role":"user","content":"test"}]}"#,
                ))
                .unwrap();

            let response = dispatch_request_with_id(
                state.clone(),
                request,
                false,
                format!("invalid-agent-id-{index}"),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains(expected_error), "unexpected response: {body}");
            if let Some(raw_canary) = raw_canary {
                assert!(!body.contains(raw_canary));
            }
        }

        assert!(state.admission.sessions.lock().unwrap().is_empty());
    }

    #[test]
    fn error_capture_redacts_nested_payload_secrets_case_insensitively() {
        let mut details = Map::new();
        let mut canaries = Vec::new();
        for (index, key) in crate::logging::PAYLOAD_REDACT_KEYS.iter().enumerate() {
            let canary = format!("PAYLOAD_SECRET_{index}");
            details.insert((*key).to_string(), Value::String(canary.clone()));
            canaries.push(canary);
        }
        details.insert(
            "access_token".to_string(),
            Value::String("BASE_SECRET".to_string()),
        );
        details.insert(
            "ClIeNt_SeCrEt".to_string(),
            Value::String("MIXED_CASE_SECRET".to_string()),
        );
        details.insert(
            "safe_field".to_string(),
            Value::String("safe-value".to_string()),
        );
        details.insert(
            "message".to_string(),
            Value::String("Bearer adjacent-secret at /home/customer/private.txt".to_string()),
        );
        let redacted = redact_error_value(json!({
            "response": {
                "json": {
                    "error": {
                        "details": Value::Object(details)
                    }
                }
            }
        }));
        let details = &redacted["response"]["json"]["error"]["details"];

        for key in crate::logging::PAYLOAD_REDACT_KEYS {
            assert!(
                details[key]
                    .as_str()
                    .is_some_and(|value| value.starts_with("[redacted len=")),
                "payload key was not redacted: {key}"
            );
        }
        assert_eq!(details["access_token"], "[redacted len=11]");
        assert_eq!(details["ClIeNt_SeCrEt"], "[redacted len=17]");
        assert_eq!(details["safe_field"], "safe-value");
        assert_eq!(details["message"], "[redacted upstream error detail]");

        let serialized = serde_json::to_string(&redacted).unwrap();
        for canary in canaries {
            assert!(!serialized.contains(&canary));
        }
        assert!(!serialized.contains("BASE_SECRET"));
        assert!(!serialized.contains("MIXED_CASE_SECRET"));
        assert!(!serialized.contains("adjacent-secret"));
        assert!(!serialized.contains("/home/customer"));
    }

    #[test]
    fn error_capture_redaction_has_a_depth_limit() {
        let mut value = json!({"safe_field": "too-deep"});
        for _ in 0..=MAX_ERROR_REDACTION_DEPTH {
            value = json!({"next": value});
        }
        let redacted = redact_error_value(value);
        assert!(
            serde_json::to_string(&redacted)
                .unwrap()
                .contains("[depth-limit]")
        );
    }

    #[cfg(unix)]
    #[test]
    fn error_capture_directory_and_file_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("errors");
        let path = write_error_capture_in_dir(
            &directory,
            "private-capture",
            &json!({"error": {"message": "sensitive"}}),
        )
        .unwrap();

        assert_eq!(
            fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn error_capture_directory_failure_is_returned() {
        let temp = tempfile::tempdir().unwrap();
        let not_a_directory = temp.path().join("not-a-directory");
        fs::write(&not_a_directory, b"block directory creation").unwrap();

        let error = write_error_capture_in_dir(
            &not_a_directory,
            "failed-capture",
            &json!({"error": {"message": "sensitive"}}),
        )
        .unwrap_err();

        assert_ne!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(
            fs::read(&not_a_directory).unwrap(),
            b"block directory creation"
        );
    }

    #[tokio::test]
    async fn rejected_response_body_is_bounded_and_marks_truncation() {
        let mut exact = Body::from(vec![b'x'; MAX_ERROR_RESPONSE_BODY_BYTES]);
        let exact = read_bounded_error_body_with_timeouts(
            &mut exact,
            Duration::from_millis(20),
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(exact.bytes.len(), MAX_ERROR_RESPONSE_BODY_BYTES);
        assert!(!exact.truncated);
        assert!(!exact.timed_out);

        let mut oversized = Body::from(vec![b'x'; MAX_ERROR_RESPONSE_BODY_BYTES + 1]);
        let oversized = read_bounded_error_body_with_timeouts(
            &mut oversized,
            Duration::from_millis(20),
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(oversized.bytes.len(), MAX_ERROR_RESPONSE_BODY_BYTES);
        assert!(oversized.truncated);
    }

    #[tokio::test]
    async fn rejected_response_body_idle_timeout_does_not_wait_forever() {
        let body_stream = stream::once(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, std::io::Error>(Bytes::from_static(b"late"))
        });
        let mut body = Body::from_stream(body_stream);
        let result = read_bounded_error_body_with_timeouts(
            &mut body,
            Duration::from_millis(5),
            Duration::from_millis(50),
        )
        .await;
        assert!(result.timed_out);
        assert!(result.bytes.is_empty());
    }

    #[tokio::test]
    async fn rejected_response_body_total_timeout_stops_a_trickle() {
        let body_stream = stream::unfold(0_u8, |value| async move {
            tokio::time::sleep(Duration::from_millis(3)).await;
            Some((Ok::<_, std::io::Error>(Bytes::from(vec![value])), value + 1))
        });
        let mut body = Body::from_stream(body_stream);
        let result = read_bounded_error_body_with_timeouts(
            &mut body,
            Duration::from_millis(10),
            Duration::from_millis(15),
        )
        .await;
        assert!(result.timed_out);
        assert!(!result.bytes.is_empty());
        assert!(result.bytes.len() < MAX_ERROR_RESPONSE_BODY_BYTES);
    }

    #[test]
    fn effective_config_fingerprint_changes_with_limit_values() {
        let first = ServerLimits::default();
        let mut second = first.clone();
        second.max_concurrent_requests += 1;

        assert_ne!(
            effective_config_fingerprint(&first, "127.0.0.1", 18765),
            effective_config_fingerprint(&second, "127.0.0.1", 18765)
        );

        let mut third = first.clone();
        third.graceful_shutdown_timeout += Duration::from_millis(1);
        assert_ne!(
            effective_config_fingerprint(&first, "127.0.0.1", 18765),
            effective_config_fingerprint(&third, "127.0.0.1", 18765)
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_timeout_bounds_a_stalled_server() {
        let (shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel();
        shutdown_started_tx.send(()).unwrap();
        let result = await_server_with_grace_timeout(
            std::future::pending::<std::io::Result<()>>(),
            shutdown_started_rx,
            Duration::from_millis(5),
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("graceful shutdown exceeded")
        );
    }

    #[tokio::test]
    async fn server_completion_wins_before_shutdown_timeout() {
        let (_shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel();
        await_server_with_grace_timeout(
            std::future::ready(Ok(())),
            shutdown_started_rx,
            Duration::ZERO,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_an_existing_stream_before_server_exit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let app = Router::new().route(
            "/stream",
            get(move || {
                let release_rx = Arc::clone(&release_rx);
                async move {
                    let release_rx = release_rx
                        .lock()
                        .await
                        .take()
                        .expect("the test stream is requested once");
                    Body::from_stream(stream::unfold(
                        (0_u8, Some(release_rx)),
                        |(phase, mut release_rx)| async move {
                            match phase {
                                0 => Some((
                                    Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(
                                        b"first",
                                    )),
                                    (1, release_rx),
                                )),
                                1 => {
                                    let _ = release_rx
                                        .take()
                                        .expect("release receiver remains available")
                                        .await;
                                    Some((Ok(Bytes::from_static(b"second")), (2, release_rx)))
                                }
                                _ => None,
                            }
                        },
                    ))
                }
            }),
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel();
        let (shutdown_observed_tx, shutdown_observed_rx) = tokio::sync::oneshot::channel();
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
            let _ = shutdown_started_tx.send(());
            let _ = shutdown_observed_tx.send(());
        });
        let server_task = tokio::spawn(await_server_with_grace_timeout(
            server.into_future(),
            shutdown_started_rx,
            Duration::from_secs(1),
        ));

        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = client
            .get(format!("http://{address}/stream"))
            .send()
            .await
            .unwrap();
        let mut body = response.bytes_stream();
        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );

        shutdown_tx.send(()).unwrap();
        shutdown_observed_rx.await.unwrap();
        assert!(
            !server_task.is_finished(),
            "graceful shutdown must wait for the active response body"
        );

        release_tx.send(()).unwrap();
        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"second")
        );
        assert!(body.next().await.is_none());
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejected_response_body_read_error_is_captured() {
        let body_stream = stream::iter([Err::<Bytes, _>(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))]);
        let mut body = Body::from_stream(body_stream);
        let result = read_bounded_error_body_with_timeouts(
            &mut body,
            Duration::from_millis(20),
            Duration::from_millis(100),
        )
        .await;
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("reset"))
        );
    }

    #[test]
    fn dropping_pre_response_guard_records_abandonment_once() {
        let req_id = "pre-response-drop";
        let monitor = started_monitor(req_id);
        drop(request_guard(monitor.clone(), req_id));

        let state = monitor.snapshot();
        assert!(state.active.is_empty());
        assert_eq!(state.recent.len(), 1);
        assert_eq!(
            state.recent[0].status,
            crate::monitor::RequestStatus::Failed
        );
        assert_eq!(
            state.recent[0].error.as_deref(),
            Some("Request future ended before completion")
        );
    }
}
