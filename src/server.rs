use crate::{
    anthropic::json_error,
    logging::{Logger, REDACT_KEYS, create_logger},
    monitor::{EndpointKind, MonitorHandle},
    project,
    provider::RequestContext,
    registry::{Registry, normalize_incoming_model},
    session::{self, SessionState},
    traffic::{TrafficCaptureOptions, create_traffic_capture},
};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::Response,
    routing::{get, post},
};
use http_body_util::{BodyExt, StreamBody};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use std::fs::{self, File};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use uuid::Uuid;

pub struct ServerConfig {
    pub bind_address: String,
    pub port: u16,
    pub monitor: Option<MonitorHandle>,
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
    let listener = bind_proxy_listener(&config.bind_address, config.port).await?;
    serve_listener(listener, config.monitor, shutdown).await
}

pub async fn bind_proxy_listener(bind_address: &str, port: u16) -> anyhow::Result<TcpListener> {
    let ip = bind_address
        .parse::<std::net::IpAddr>()
        .map_err(|err| anyhow::anyhow!("invalid proxy bind address {bind_address:?}: {err}"))?;
    let addr = std::net::SocketAddr::new(ip, port);
    TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow::anyhow!("failed to bind proxy listener on {addr}: {err}"))
}

pub async fn serve_listener(
    listener: TcpListener,
    monitor: Option<MonitorHandle>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let local_addr = listener.local_addr()?;
    let port = local_addr.port();
    create_logger("server").info(
        "server listening",
        Some(serde_json::Map::from_iter([
            ("port".to_string(), json!(port)),
            (
                "bindAddress".to_string(),
                json!(local_addr.ip().to_string()),
            ),
            (
                "logDir".to_string(),
                json!(
                    crate::paths::log_file()
                        .parent()
                        .map(|path| path.display().to_string())
                ),
            ),
        ])),
    );
    let app = app_with_monitor(Arc::new(Registry::with_default_alias()), monitor);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

pub fn app(registry: Arc<Registry>) -> Router {
    app_with_monitor(registry, None)
}

pub fn app_with_monitor(registry: Arc<Registry>, monitor: Option<MonitorHandle>) -> Router {
    let state = Arc::new(AppState { registry, monitor });
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/messages", post(handler_messages))
        .route("/v1/messages/count_tokens", post(handler_count_tokens))
        .fallback(fallback_handler)
        .with_state(state)
}

#[derive(Clone)]
struct AppState {
    registry: Arc<Registry>,
    monitor: Option<MonitorHandle>,
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({ "ok": true }))
}

async fn handler_messages(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, false).await
}

async fn handler_count_tokens(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, true).await
}

async fn dispatch_request(
    state: Arc<AppState>,
    req: Request<Body>,
    count_tokens: bool,
) -> Response {
    let started_at = Instant::now();
    let log = create_logger("server");
    let req_id = Uuid::new_v4().to_string();
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let path = uri.path().to_string();
    let query = redacted_query(&uri);
    let endpoint = if count_tokens {
        EndpointKind::CountTokens
    } else {
        EndpointKind::Messages
    };
    log.info(
        "request",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), json!(&req_id)),
            ("method".to_string(), json!(method.as_str())),
            ("path".to_string(), json!(&path)),
            ("query".to_string(), json!(&query)),
        ])),
    );
    let session_id = req
        .headers()
        .get("x-claude-code-session-id")
        .and_then(|value| value.to_str().ok())
        .map(std::string::ToString::to_string);
    if let Some(monitor) = state.monitor.as_ref() {
        monitor.request_started(&req_id, session_id.clone(), None, endpoint);
    }
    let mut request_guard = RequestMonitorGuard::new(
        state.monitor.clone(),
        req_id.clone(),
        log.clone(),
        started_at,
        count_tokens,
    );
    let now = current_millis();
    let body_bytes = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("Invalid JSON: {err}"),
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
                    .unwrap_or("Invalid JSON")
                    .to_string(),
            );
            return response;
        }
    };

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

    if let Some(project) = project::name_from_request(
        body.extra.get("system"),
        body.messages.iter().rev().map(|message| &message.content),
    ) && let Some(monitor) = state.monitor.as_ref()
    {
        monitor.project_resolved(&req_id, project);
    }

    let model = match body.model.as_deref() {
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

    let normalized_model = normalize_incoming_model(model);
    request_guard.set_route(None, Some(&normalized_model));
    body.model = Some(normalized_model.clone());
    let session_state = if let Some(session_id) = session_id.as_deref() {
        session::existing_session(Some(session_id), now)
    } else {
        None
    };

    let provider = state.registry.provider_for_model(
        &normalized_model,
        session_state
            .as_ref()
            .and_then(|state| state.affinity_provider.as_ref()),
    );

    let provider = match provider {
        Some(provider) => provider,
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

    let effort = crate::providers::translate_shared::read_effort(&body)
        .ok()
        .flatten()
        .map(str::to_string);
    request_guard.set_route(Some(provider.name()), Some(&normalized_model));
    let current = session::record_session_request(
        session_id.as_deref(),
        session_state.as_ref(),
        provider.name(),
        &normalized_model,
        now,
    );
    if let Some(monitor) = state.monitor.as_ref() {
        if let Some(current) = current.as_ref() {
            monitor.session_sequence_resolved(&req_id, current.seq);
        }
        monitor.provider_selected(&req_id, provider.name(), &normalized_model, effort);
    }

    let traffic = create_traffic_capture(TrafficCaptureOptions {
        req_id: req_id.clone(),
        session_id: session_id.clone(),
        session_seq: current.as_ref().map(|s| s.seq),
        provider: Some(provider.name().to_string()),
        state_dir_override: None,
    })
    .map(Arc::new);

    if let Some(capture) = traffic.as_ref() {
        if let Some(monitor) = state.monitor.as_ref() {
            monitor.traffic_capture_path(&req_id, capture.root().to_path_buf());
        }
        capture.write_json(
            "000-metadata",
            &json!({
                "reqId": &req_id,
                "sessionId": &session_id,
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

    let context = RequestContext {
        req_id: req_id.clone(),
        session_id,
        session_seq: current.map(|s| s.seq),
        provider: provider.name().to_string(),
        traffic,
        monitor: state.monitor.clone(),
    };

    let response = if count_tokens {
        provider.handle_count_tokens(body, context).await
    } else {
        provider.handle_messages(body, context).await
    };
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
    response
}

fn monitor_response_body(
    response: Response,
    guard: RequestMonitorGuard,
    log_context: ResponseLogContext,
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
                        && let Some(error) = lifecycle.detect_sse_error(data)
                    {
                        lifecycle.failed(error, true);
                    }
                    Some((Ok(frame), (body, lifecycle)))
                }
                Some(Err(err)) => {
                    lifecycle.failed(err.to_string(), false);
                    Some((Err(err), (body, lifecycle)))
                }
                None => {
                    lifecycle.completed();
                    None
                }
            }
        },
    );
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
    terminal: bool,
}

impl ResponseBodyLifecycle {
    fn detect_sse_error(&mut self, bytes: &[u8]) -> Option<String> {
        self.sse_detector.as_mut()?.push(bytes)
    }

    fn completed(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.guard.completed(self.status);
        log_request_completed(&self.log_context, self.status);
    }

    fn failed(&mut self, error: String, in_band_sse: bool) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.guard.failed(self.status, error.clone());
        log_stream_failed(&self.log_context, self.status, &error, in_band_sse);
    }
}

impl Drop for ResponseBodyLifecycle {
    fn drop(&mut self) {
        if !self.terminal {
            self.terminal = true;
            self.guard.abandoned("Downstream response body was dropped");
        }
    }
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
}

impl SseErrorDetector {
    fn push(&mut self, bytes: &[u8]) -> Option<String> {
        for &byte in bytes {
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
                        return Some(error);
                    }
                    continue;
                }
                let line = String::from_utf8_lossy(&self.line).into_owned();
                self.line.clear();
                if let Some(error) = self.process_line(&line) {
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

        let payload = serde_json::from_str::<Value>(&data).ok();
        let is_error = event.as_deref() == Some("error")
            || payload
                .as_ref()
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str)
                == Some("error");
        if !is_error {
            return None;
        }

        Some(
            payload
                .as_ref()
                .and_then(|value| value.pointer("/error/message"))
                .and_then(Value::as_str)
                .or_else(|| {
                    payload
                        .as_ref()
                        .and_then(|value| value.get("message"))
                        .and_then(Value::as_str)
                })
                .filter(|message| !message.trim().is_empty())
                .unwrap_or("Upstream stream returned an error event")
                .to_string(),
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
    let (parts, body) = response.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            log.info(
                "request_failed",
                Some(serde_json::Map::from_iter([
                    ("reqId".to_string(), json!(ctx.req_id)),
                    ("provider".to_string(), json!(ctx.provider)),
                    ("model".to_string(), json!(ctx.model)),
                    ("countTokens".to_string(), json!(ctx.count_tokens)),
                    ("status".to_string(), json!(status.as_u16())),
                    (
                        "ms".to_string(),
                        json!(ctx.started_at.elapsed().as_millis()),
                    ),
                    ("bodyReadError".to_string(), json!(err.to_string())),
                ])),
            );
            return (Response::from_parts(parts, Body::empty()), None);
        }
    };

    let response_body = response_body_value(&bytes);
    let message = error_message_from_response(&response_body)
        .unwrap_or_else(|| format!("HTTP {}", status.as_u16()));
    let document = json!({
        "reqId": ctx.req_id,
        "provider": ctx.provider,
        "model": ctx.model,
        "countTokens": ctx.count_tokens,
        "status": status.as_u16(),
        "elapsedMs": ctx.started_at.elapsed().as_millis(),
        "message": message,
        "response": response_body,
    });
    let error_file = write_error_capture(ctx.req_id, &redact_error_value(document));

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
    ]);
    if let Some(path) = error_file.as_ref() {
        fields.insert("errorFile".to_string(), json!(path.display().to_string()));
    }
    log.info("request_failed", Some(fields));

    (
        Response::from_parts(parts, Body::from(bytes)),
        Some(FailedResponseDetails { message }),
    )
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

fn write_error_capture(req_id: &str, document: &Value) -> Option<PathBuf> {
    let dir = crate::paths::state_dir().join("errors");
    fs::create_dir_all(&dir).ok()?;
    set_mode(&dir, 0o700);
    let path = dir.join(format!(
        "{}-{}.json",
        current_millis(),
        sanitize_path_part(req_id)
    ));
    let mut file = File::create(&path).ok()?;
    set_mode(&path, 0o600);
    let payload = serde_json::to_vec_pretty(document).ok()?;
    file.write_all(&payload).ok()?;
    file.write_all(b"\n").ok()?;
    Some(path)
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
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(redact_error_value).collect()),
        Value::Object(fields) => {
            let mut out = Map::new();
            for (key, value) in fields {
                if REDACT_KEYS.contains(&key.to_lowercase().as_str()) {
                    out.insert(key, redact_error_key(value));
                } else {
                    out.insert(key, redact_error_value(value));
                }
            }
            Value::Object(out)
        }
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
        if let Ok(raw) = value.to_str() {
            out.insert(key.as_str().to_string(), Value::String(raw.to_string()));
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
        let lower = key.to_lowercase();
        let value = if REDACT_KEYS.contains(&lower.as_str()) {
            Value::String(format!("[redacted len={}]", value.len()))
        } else {
            Value::String(value.into_owned())
        };
        out.insert(key, value);
    }
    Value::Object(out)
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

fn current_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn set_mode(path: &Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = fs::metadata(path) {
            let mut perm = meta.permissions();
            perm.set_mode(mode);
            let _ = fs::set_permissions(path, perm);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
}

#[allow(dead_code)]
fn _unused(session_state: Option<&SessionState>) {
    let _ = session_state;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::stream;

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

    #[tokio::test]
    async fn response_body_stays_active_until_successful_eof() {
        let req_id = "stream-success";
        let monitor = started_monitor(req_id);
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream::iter(vec![
                Ok::<_, std::io::Error>(Bytes::from_static(
                    b"event: ping\ndata: {\"type\":\"ping\"}\n\n",
                )),
            ])))
            .unwrap();
        let response = monitor_response_body(
            response,
            request_guard(monitor.clone(), req_id),
            response_log_context(req_id),
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
