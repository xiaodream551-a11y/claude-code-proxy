use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{Stream, StreamExt};
use http::StatusCode;
use tokio::sync::Mutex;

use super::auth::manager::{GrokAuthError, GrokAuthErrorKind, GrokAuthManager};
use super::auth::token_store::{StoredAuth, file_store};
use super::translate::request::GrokResponsesRequest;
use crate::retry::{MAX_RATE_LIMIT_RETRIES, compute_backoff_delay, should_retry_status, sleep};
use crate::traffic::TrafficCapture;

const DEFAULT_BASE_URL: &str = "https://cli-chat-proxy.grok.com/v1";
const MAX_BUFFERED_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WIRE_ATTEMPTS: u32 = MAX_RATE_LIMIT_RETRIES + 1;
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_HEADER_TIMEOUT_MS: u64 = 60_000;
pub const DEFAULT_FIRST_BYTE_TIMEOUT_MS: u64 = 60_000;
pub const DEFAULT_BODY_IDLE_TIMEOUT_MS: u64 = 300_000;

pub type GrokByteStream =
    Pin<Box<dyn Stream<Item = Result<bytes::Bytes, GrokError>> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrokErrorOrigin {
    Auth,
    Http,
    RequestTransport,
    StreamTransport,
    ResponseLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrokErrorStage {
    Auth,
    Connect,
    Header,
    Status,
    Body,
    Stream,
}

#[derive(Debug, Clone)]
pub struct GrokError {
    pub status: StatusCode,
    pub retry_after: Option<String>,
    pub message: String,
    pub origin: GrokErrorOrigin,
    pub stage: GrokErrorStage,
    pub retryable: bool,
}

impl GrokError {
    pub fn auth(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            retry_after: None,
            message: message.into(),
            origin: GrokErrorOrigin::Auth,
            stage: GrokErrorStage::Auth,
            retryable: false,
        }
    }

    pub fn http(
        status: StatusCode,
        retry_after: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            retry_after,
            message: message.into(),
            origin: GrokErrorOrigin::Http,
            stage: GrokErrorStage::Status,
            retryable: should_retry_status(status.as_u16()),
        }
    }

    fn auth_temporary(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            retry_after: None,
            message: message.into(),
            origin: GrokErrorOrigin::Auth,
            stage: GrokErrorStage::Auth,
            retryable: true,
        }
    }

    fn auth_rate_limited(retry_after: Option<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            retry_after,
            message: message.into(),
            origin: GrokErrorOrigin::Auth,
            stage: GrokErrorStage::Auth,
            retryable: true,
        }
    }

    pub fn request_transport(stage: GrokErrorStage, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            retry_after: None,
            message: message.into(),
            origin: GrokErrorOrigin::RequestTransport,
            stage,
            retryable: true,
        }
    }

    pub fn stream_transport(stage: GrokErrorStage, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            retry_after: None,
            message: message.into(),
            origin: GrokErrorOrigin::StreamTransport,
            stage,
            retryable: true,
        }
    }

    pub fn response_limit(stage: GrokErrorStage, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            retry_after: None,
            message: message.into(),
            origin: GrokErrorOrigin::ResponseLimit,
            stage,
            retryable: false,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub fn into_terminal(mut self, reason: &str) -> Self {
        self.retryable = false;
        self.message = format!("{} ({reason})", self.message);
        self
    }

    fn retry_budget_exhausted() -> Self {
        Self::request_transport(
            GrokErrorStage::Header,
            "Grok retry budget was already exhausted",
        )
        .into_terminal("retry exhausted")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GrokTimeouts {
    pub connect_ms: u64,
    pub header_ms: u64,
    pub first_byte_ms: u64,
    pub body_idle_ms: u64,
}

impl GrokTimeouts {
    pub fn configured() -> Self {
        Self {
            connect_ms: crate::config::grok_connect_timeout_ms(DEFAULT_CONNECT_TIMEOUT_MS),
            header_ms: crate::config::grok_header_timeout_ms(DEFAULT_HEADER_TIMEOUT_MS),
            first_byte_ms: crate::config::grok_first_byte_timeout_ms(DEFAULT_FIRST_BYTE_TIMEOUT_MS),
            body_idle_ms: crate::config::grok_body_idle_timeout_ms(DEFAULT_BODY_IDLE_TIMEOUT_MS),
        }
    }
}

impl Default for GrokTimeouts {
    fn default() -> Self {
        Self {
            connect_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            header_ms: DEFAULT_HEADER_TIMEOUT_MS,
            first_byte_ms: DEFAULT_FIRST_BYTE_TIMEOUT_MS,
            body_idle_ms: DEFAULT_BODY_IDLE_TIMEOUT_MS,
        }
    }
}

#[derive(Debug)]
pub struct GrokRetryState {
    wire_attempt: u32,
    transient_failures: u32,
    auth_refresh_succeeded: bool,
    terminal: bool,
    request_gate: Arc<Mutex<()>>,
}

impl Default for GrokRetryState {
    fn default() -> Self {
        Self::new()
    }
}

impl GrokRetryState {
    pub fn new() -> Self {
        Self {
            wire_attempt: 0,
            transient_failures: 0,
            auth_refresh_succeeded: false,
            terminal: false,
            request_gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn transient_failures(&self) -> u32 {
        self.transient_failures
    }

    pub fn can_retry_transient(&self) -> bool {
        !self.terminal
            && self.transient_failures < MAX_RATE_LIMIT_RETRIES
            && self.wire_attempt < MAX_WIRE_ATTEMPTS
    }

    pub fn note_transient_failure(&mut self) {
        self.transient_failures = self.transient_failures.saturating_add(1);
    }

    pub fn mark_terminal(&mut self) {
        self.terminal = true;
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    fn reserve_wire_attempt(&mut self) -> Option<u8> {
        if self.terminal || self.wire_attempt >= MAX_WIRE_ATTEMPTS {
            self.terminal = true;
            return None;
        }
        self.wire_attempt = self.wire_attempt.saturating_add(1);
        Some(self.wire_attempt as u8)
    }

    fn has_wire_attempt_capacity(&self) -> bool {
        !self.terminal && self.wire_attempt < MAX_WIRE_ATTEMPTS
    }
}

pub struct GrokClient {
    client: Arc<reqwest::Client>,
    auth: Arc<GrokAuthManager<crate::auth::FileAuthStore<StoredAuth>>>,
    url: String,
    client_version: String,
    timeouts: GrokTimeouts,
}

pub struct GrokResponse {
    response: reqwest::Response,
    timeouts: GrokTimeouts,
}

impl GrokResponse {
    pub fn into_stream(self) -> GrokByteStream {
        timed_byte_stream(self.response, self.timeouts)
    }

    pub async fn into_bytes(self) -> Result<Vec<u8>, GrokError> {
        let mut stream = timed_byte_stream(self.response, self.timeouts);
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if bytes.len().saturating_add(chunk.len()) > MAX_BUFFERED_RESPONSE_BYTES {
                return Err(GrokError::response_limit(
                    GrokErrorStage::Body,
                    "Grok upstream response exceeds the size limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

impl GrokClient {
    pub fn new(base_url: String, client_version: String) -> anyhow::Result<Self> {
        let timeouts = GrokTimeouts::configured();
        let client = Arc::new(
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_millis(timeouts.connect_ms))
                .build()?,
        );
        let auth = Arc::new(GrokAuthManager::new(file_store())?);
        Ok(Self::with_shared(
            url_for(base_url)?,
            client_version,
            client,
            auth,
            timeouts,
        ))
    }

    #[cfg(test)]
    pub fn new_for_test(
        base_url: String,
        client_version: String,
        timeouts: GrokTimeouts,
        auth: GrokAuthManager<crate::auth::FileAuthStore<StoredAuth>>,
    ) -> anyhow::Result<Self> {
        let client = Arc::new(
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_millis(timeouts.connect_ms))
                .build()?,
        );
        Ok(Self::with_shared(
            url_for(base_url)?,
            client_version,
            client,
            Arc::new(auth),
            timeouts,
        ))
    }

    fn with_shared(
        url: String,
        client_version: String,
        client: Arc<reqwest::Client>,
        auth: Arc<GrokAuthManager<crate::auth::FileAuthStore<StoredAuth>>>,
        timeouts: GrokTimeouts,
    ) -> Self {
        Self {
            client,
            auth,
            url,
            client_version,
            timeouts,
        }
    }

    pub async fn post(
        &self,
        body: &GrokResponsesRequest,
        traffic: Option<Arc<TrafficCapture>>,
    ) -> Result<GrokResponse, GrokError> {
        let retry = Arc::new(Mutex::new(GrokRetryState::new()));
        self.post_with_retry(body, traffic, retry).await
    }

    pub async fn post_with_retry(
        &self,
        body: &GrokResponsesRequest,
        traffic: Option<Arc<TrafficCapture>>,
        retry: Arc<Mutex<GrokRetryState>>,
    ) -> Result<GrokResponse, GrokError> {
        let request_gate = retry.lock().await.request_gate.clone();
        let _request_guard = request_gate.lock().await;
        if retry.lock().await.is_terminal() {
            return Err(GrokError::retry_budget_exhausted());
        }
        if let Some(capture) = traffic.as_ref() {
            let body_value = serde_json::to_value(body).unwrap_or(serde_json::Value::Null);
            capture.write_json("020-upstream-request", &body_value);
            capture.write_json(
                "021-upstream-request-metadata",
                &serde_json::json!({
                    "method": "POST",
                    "url": safe_url(&self.url),
                    "provider": "grok",
                    "transport": "http",
                    "headers": {
                        "accept":"text/event-stream",
                        "content-type":"application/json",
                        "authorization":"[redacted]",
                        "x-xai-token-auth":"[redacted]"
                    },
                    "body_bytes": serde_json::to_vec(body).map(|v| v.len()).unwrap_or(0),
                }),
            );
        }

        let mut auth = match self.auth.get_auth().await {
            Ok(auth) => auth,
            Err(error) => {
                let kind = auth_error_kind_name(error.kind());
                let error = auth_error(error);
                capture_terminal_failure(
                    traffic.as_deref(),
                    "auth",
                    kind,
                    0,
                    Some(error.status.as_u16()),
                    Some(&error.message),
                );
                return Err(error);
            }
        };

        loop {
            let attempt = {
                let mut state = retry.lock().await;
                state.reserve_wire_attempt()
            };
            let Some(attempt) = attempt else {
                return Err(GrokError::retry_budget_exhausted());
            };
            let outcome = self
                .attempt(&auth.access, body, attempt, traffic.as_deref())
                .await;

            match outcome {
                Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                    let mut state = retry.lock().await;
                    if state.auth_refresh_succeeded {
                        state.mark_terminal();
                        drop(state);
                        capture_terminal_failure(
                            traffic.as_deref(),
                            "auth",
                            "unauthorized",
                            attempt,
                            Some(401),
                            None,
                        );
                        return Err(auth_error(GrokAuthError::credentials_invalid(
                            "the refreshed access token was rejected",
                        )));
                    }
                    if !state.has_wire_attempt_capacity() {
                        state.mark_terminal();
                        drop(state);
                        let error = auth_error(GrokAuthError::credentials_invalid(
                            "the access token was rejected and no model retry attempts remain",
                        ));
                        capture_terminal_failure(
                            traffic.as_deref(),
                            "auth",
                            "unauthorized_retry_exhausted",
                            attempt,
                            Some(401),
                            Some(&error.message),
                        );
                        return Err(error);
                    }
                    drop(state);
                    if let Some(capture) = traffic.as_ref() {
                        capture.write_json(
                            "023-upstream-auth-refresh",
                            &serde_json::json!({
                                "attempt": attempt,
                                "status": 401,
                                "origin": "auth",
                                "stage": "unauthorized_refresh",
                            }),
                        );
                    }
                    auth = match self.auth.force_refresh(&auth.access).await {
                        Ok(auth) => {
                            retry.lock().await.auth_refresh_succeeded = true;
                            auth
                        }
                        Err(error) => {
                            let kind = auth_error_kind_name(error.kind());
                            let error = auth_error(error);
                            capture_terminal_failure(
                                traffic.as_deref(),
                                "auth",
                                kind,
                                attempt,
                                Some(error.status.as_u16()),
                                Some(&error.message),
                            );
                            return Err(error);
                        }
                    };
                    continue;
                }
                Ok(response) => {
                    return Ok(GrokResponse {
                        response,
                        timeouts: self.timeouts,
                    });
                }
                Err(error) if error.origin == GrokErrorOrigin::Auth => {
                    capture_terminal_failure(
                        traffic.as_deref(),
                        stage_name(error.stage),
                        "auth",
                        attempt,
                        Some(error.status.as_u16()),
                        Some(&error.message),
                    );
                    return Err(error);
                }
                Err(error) if error.is_retryable() => {
                    let mut state = retry.lock().await;
                    if !state.can_retry_transient() {
                        state.mark_terminal();
                        drop(state);
                        capture_terminal_failure(
                            traffic.as_deref(),
                            stage_name(error.stage),
                            "retry_exhausted",
                            attempt,
                            Some(error.status.as_u16()),
                            Some(&error.message),
                        );
                        log_retry_exhausted(attempt, &error);
                        return Err(error.into_terminal("retry exhausted"));
                    }
                    let delay = compute_backoff_delay(
                        state.transient_failures,
                        error.retry_after.as_deref(),
                    );
                    if delay.exceeds_budget {
                        state.mark_terminal();
                        drop(state);
                        capture_terminal_failure(
                            traffic.as_deref(),
                            stage_name(error.stage),
                            "retry_after_exceeds_budget",
                            attempt,
                            Some(error.status.as_u16()),
                            Some(&error.message),
                        );
                        return Err(error.into_terminal("retry delay exceeds budget"));
                    }
                    state.note_transient_failure();
                    let transient = state.transient_failures;
                    drop(state);
                    if let Some(capture) = traffic.as_ref() {
                        capture.write_json(
                            "023-upstream-retry",
                            &serde_json::json!({
                                "attempt": attempt,
                                "transient_failures": transient,
                                "wait_ms": delay.wait_ms,
                                "status": error.status.as_u16(),
                                "origin": origin_name(error.origin),
                                "stage": stage_name(error.stage),
                                "message": error.message,
                            }),
                        );
                    }
                    log_retry(attempt, transient, delay.wait_ms, &error);
                    sleep(delay.wait_ms).await;
                    continue;
                }
                Err(error) => {
                    capture_terminal_failure(
                        traffic.as_deref(),
                        stage_name(error.stage),
                        "final",
                        attempt,
                        Some(error.status.as_u16()),
                        Some(&error.message),
                    );
                    return Err(error);
                }
            }
        }
    }

    async fn attempt(
        &self,
        access: &str,
        body: &GrokResponsesRequest,
        attempt: u8,
        traffic: Option<&TrafficCapture>,
    ) -> Result<reqwest::Response, GrokError> {
        let started = Instant::now();
        let send_fut = self
            .client
            .post(&self.url)
            .header("accept", "text/event-stream")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {access}"))
            .header("x-xai-token-auth", "xai-grok-cli")
            .header("x-grok-client-identifier", "grok-shell")
            .header("x-grok-client-version", &self.client_version)
            .json(body)
            .send();

        let response =
            match tokio::time::timeout(Duration::from_millis(self.timeouts.header_ms), send_fut)
                .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    let stage = if error.is_connect() {
                        GrokErrorStage::Connect
                    } else {
                        GrokErrorStage::Header
                    };
                    let message = format!("Grok upstream request failed: {error}");
                    capture_attempt(
                        traffic,
                        attempt,
                        None,
                        origin_name(GrokErrorOrigin::RequestTransport),
                        stage_name(stage),
                        Some(&message),
                    );
                    return Err(GrokError::request_transport(stage, message));
                }
                Err(_) => {
                    let message = format!(
                        "Timed out waiting {}ms for Grok response headers",
                        self.timeouts.header_ms
                    );
                    capture_attempt(
                        traffic,
                        attempt,
                        None,
                        origin_name(GrokErrorOrigin::RequestTransport),
                        "header",
                        Some(&message),
                    );
                    return Err(GrokError::request_transport(
                        GrokErrorStage::Header,
                        message,
                    ));
                }
            };

        let status = response.status();
        if let Some(capture) = traffic {
            capture.write_json(
                "022-upstream-attempt",
                &serde_json::json!({
                    "attempt": attempt,
                    "status": status.as_u16(),
                    "elapsed_ms": started.elapsed().as_millis(),
                    "headers": safe_headers(response.headers()),
                }),
            );
        }

        if status == StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        if !status.is_success() {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            if let Some(capture) = traffic {
                let (body, truncated, timed_out) =
                    read_rejected_body(response, 64 * 1024, self.timeouts).await;
                let detail = serde_json::from_slice::<serde_json::Value>(&body)
                    .unwrap_or_else(|_| serde_json::json!({"body_bytes": body.len()}));
                capture.write_json(
                    "031-upstream-error-body",
                    &serde_json::json!({
                        "attempt": attempt,
                        "status": status.as_u16(),
                        "truncated": truncated,
                        "timed_out": timed_out,
                        "body": detail
                    }),
                );
            } else {
                let _ = read_rejected_body(response, 64 * 1024, self.timeouts).await;
            }
            return Err(GrokError::http(
                status,
                retry_after,
                "Grok upstream rejected the request",
            ));
        }

        if let Some(capture) = traffic {
            capture.write_json(
                "030-upstream-response-headers",
                &serde_json::json!({
                    "status": status.as_u16(),
                    "headers": safe_headers(response.headers()),
                }),
            );
        }
        Ok(response)
    }
}

fn timed_byte_stream(response: reqwest::Response, timeouts: GrokTimeouts) -> GrokByteStream {
    let stream = futures_util::stream::unfold(
        (response, timeouts, true),
        |(mut response, timeouts, first)| async move {
            let wait = if first {
                timeouts.first_byte_ms
            } else {
                timeouts.body_idle_ms
            };
            let next = tokio::time::timeout(Duration::from_millis(wait), response.chunk()).await;
            match next {
                Ok(Ok(Some(chunk))) => Some((Ok(chunk), (response, timeouts, false))),
                Ok(Ok(None)) => None,
                Ok(Err(error)) => Some((
                    Err(GrokError::stream_transport(
                        GrokErrorStage::Stream,
                        format!("Grok upstream stream failed: {error}"),
                    )),
                    (response, timeouts, false),
                )),
                Err(_) => {
                    let stage = if first {
                        GrokErrorStage::Body
                    } else {
                        GrokErrorStage::Stream
                    };
                    let message = if first {
                        format!(
                            "Timed out waiting {}ms for the first Grok response body byte",
                            timeouts.first_byte_ms
                        )
                    } else {
                        format!(
                            "Timed out waiting {}ms for the next Grok response body chunk",
                            timeouts.body_idle_ms
                        )
                    };
                    Some((
                        Err(GrokError::stream_transport(stage, message)),
                        (response, timeouts, false),
                    ))
                }
            }
        },
    );
    Box::pin(stream)
}

async fn read_rejected_body(
    response: reqwest::Response,
    limit: usize,
    timeouts: GrokTimeouts,
) -> (Vec<u8>, bool, bool) {
    let mut body = Vec::new();
    let mut first = true;
    let mut response = response;
    loop {
        let wait = if first {
            timeouts.first_byte_ms
        } else {
            timeouts.body_idle_ms
        };
        let next = tokio::time::timeout(Duration::from_millis(wait), response.chunk()).await;
        match next {
            Ok(Ok(Some(chunk))) => {
                first = false;
                let remaining = limit.saturating_sub(body.len());
                if chunk.len() > remaining {
                    body.extend_from_slice(&chunk[..remaining]);
                    return (body, true, false);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(Ok(None)) => return (body, false, false),
            Ok(Err(_)) => {
                let truncated = !body.is_empty() && body.len() >= limit;
                return (body, truncated, false);
            }
            Err(_) => {
                let truncated = body.len() >= limit;
                return (body, truncated, true);
            }
        }
    }
}

pub(super) fn capture_terminal_failure(
    traffic: Option<&TrafficCapture>,
    stage: &str,
    kind: &str,
    attempt: u8,
    status: Option<u16>,
    message: Option<&str>,
) {
    if let Some(capture) = traffic {
        capture.write_json(
            "060-grok-stream-error",
            &serde_json::json!({
                "stage": stage,
                "kind": kind,
                "attempt": attempt,
                "status": status,
                "message": message,
            }),
        );
    }
}

fn capture_attempt(
    traffic: Option<&TrafficCapture>,
    attempt: u8,
    status: Option<u16>,
    origin: &str,
    stage: &str,
    message: Option<&str>,
) {
    if let Some(capture) = traffic {
        capture.write_json(
            "022-upstream-attempt",
            &serde_json::json!({
                "attempt": attempt,
                "status": status,
                "origin": origin,
                "stage": stage,
                "message": message,
            }),
        );
    }
}

fn log_retry(attempt: u8, transient_failures: u32, wait_ms: u64, error: &GrokError) {
    crate::logging::create_logger("grok").info(
        "upstream_retry",
        Some(serde_json::Map::from_iter([
            ("attempt".into(), serde_json::json!(attempt)),
            (
                "transientFailures".into(),
                serde_json::json!(transient_failures),
            ),
            ("waitMs".into(), serde_json::json!(wait_ms)),
            ("status".into(), serde_json::json!(error.status.as_u16())),
            (
                "origin".into(),
                serde_json::json!(origin_name(error.origin)),
            ),
            ("stage".into(), serde_json::json!(stage_name(error.stage))),
            ("message".into(), serde_json::json!(error.message)),
        ])),
    );
}

fn log_retry_exhausted(attempt: u8, error: &GrokError) {
    crate::logging::create_logger("grok").info(
        "upstream_retry_exhausted",
        Some(serde_json::Map::from_iter([
            ("attempt".into(), serde_json::json!(attempt)),
            ("status".into(), serde_json::json!(error.status.as_u16())),
            (
                "origin".into(),
                serde_json::json!(origin_name(error.origin)),
            ),
            ("stage".into(), serde_json::json!(stage_name(error.stage))),
            ("message".into(), serde_json::json!(error.message)),
        ])),
    );
}

pub(super) fn origin_name(origin: GrokErrorOrigin) -> &'static str {
    match origin {
        GrokErrorOrigin::Auth => "auth",
        GrokErrorOrigin::Http => "http",
        GrokErrorOrigin::RequestTransport => "request_transport",
        GrokErrorOrigin::StreamTransport => "stream_transport",
        GrokErrorOrigin::ResponseLimit => "response_limit",
    }
}

pub(super) fn stage_name(stage: GrokErrorStage) -> &'static str {
    match stage {
        GrokErrorStage::Auth => "auth",
        GrokErrorStage::Connect => "connect",
        GrokErrorStage::Header => "header",
        GrokErrorStage::Status => "status",
        GrokErrorStage::Body => "body",
        GrokErrorStage::Stream => "stream",
    }
}

fn safe_headers(headers: &reqwest::header::HeaderMap) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for name in [
        "content-type",
        "content-length",
        "retry-after",
        "x-request-id",
    ] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            result.insert(
                name.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }
    serde_json::Value::Object(result)
}

fn safe_url(raw: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(raw) else {
        return "[invalid-url]".into();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.to_string()
}

fn url_for(base_url: String) -> anyhow::Result<String> {
    responses_url(&base_url)
}

fn responses_url(base_url: &str) -> anyhow::Result<String> {
    let base_url = if base_url.trim().is_empty() {
        DEFAULT_BASE_URL
    } else {
        base_url.trim()
    };
    let mut url = reqwest::Url::parse(base_url)?;
    let path = url.path().trim_end_matches('/');
    if !path.ends_with("/responses") {
        url.set_path(&format!("{path}/responses"));
    }
    Ok(url.to_string().trim_end_matches('/').to_string())
}

fn auth_error(error: GrokAuthError) -> GrokError {
    let kind = error.kind();
    let retry_after = error.retry_after().map(str::to_string);
    let message = error.to_string();
    match kind {
        GrokAuthErrorKind::CredentialsInvalid => GrokError::auth(format!(
            "{message}. Re-authenticate with the Grok CLI and import the session again"
        )),
        GrokAuthErrorKind::Temporary => GrokError::auth_temporary(message),
        GrokAuthErrorKind::RateLimited => GrokError::auth_rate_limited(retry_after, message),
    }
}

fn auth_error_kind_name(kind: GrokAuthErrorKind) -> &'static str {
    match kind {
        GrokAuthErrorKind::CredentialsInvalid => "credentials_invalid",
        GrokAuthErrorKind::Temporary => "auth_temporary",
        GrokAuthErrorKind::RateLimited => "auth_rate_limited",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traffic::test_capture;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn test_auth() -> StoredAuth {
        StoredAuth {
            access: "test-access".into(),
            refresh: "test-refresh".into(),
            expires_at_ms: now_ms() + 3_600_000,
            issuer: super::super::auth::login::CANONICAL_ISSUER.into(),
            client_id: super::super::auth::login::CLIENT_ID.into(),
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    async fn test_client(base_url: &str, timeouts: GrokTimeouts) -> (GrokClient, TempDir) {
        let temp = TempDir::new().unwrap();
        let primary = temp
            .path()
            .join("grok")
            .join("auth.json")
            .to_string_lossy()
            .into_owned();
        let legacy = temp
            .path()
            .join("legacy-grok")
            .join("auth.json")
            .to_string_lossy()
            .into_owned();
        let store = super::super::auth::token_store::GrokTokenStore::new(
            crate::auth::FileAuthStore::new(primary, legacy),
        );
        store.save_auth(test_auth()).unwrap();
        let auth = GrokAuthManager::new(store).unwrap();
        let client =
            GrokClient::new_for_test(base_url.to_string(), "test-version".into(), timeouts, auth)
                .unwrap();
        (client, temp)
    }

    async fn test_client_with_auth_issuer(
        base_url: &str,
        issuer: String,
        expires_at_ms: u64,
        timeouts: GrokTimeouts,
    ) -> (GrokClient, TempDir) {
        let temp = TempDir::new().unwrap();
        let primary = temp
            .path()
            .join("grok")
            .join("auth.json")
            .to_string_lossy()
            .into_owned();
        let legacy = temp
            .path()
            .join("legacy-grok")
            .join("auth.json")
            .to_string_lossy()
            .into_owned();
        let store = super::super::auth::token_store::GrokTokenStore::new(
            crate::auth::FileAuthStore::new(primary, legacy),
        );
        store
            .save_auth(StoredAuth {
                access: "expired-access".into(),
                refresh: "refresh-token".into(),
                expires_at_ms,
                issuer: issuer.clone(),
                client_id: super::super::auth::login::CLIENT_ID.into(),
            })
            .unwrap();
        let auth = GrokAuthManager::new_for_test(store, issuer).unwrap();
        let client =
            GrokClient::new_for_test(base_url.to_string(), "test-version".into(), timeouts, auth)
                .unwrap();
        (client, temp)
    }

    fn sample_body() -> GrokResponsesRequest {
        GrokResponsesRequest {
            model: "grok-4.5".into(),
            instructions: None,
            input: vec![],
            tools: None,
            tool_choice: None,
            store: false,
            stream: true,
            max_output_tokens: None,
            reasoning: None,
        }
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut buf = vec![0_u8; 32 * 1024];
        let mut total = 0usize;
        loop {
            let n = stream.read(&mut buf[total..]).await.unwrap();
            assert!(n > 0, "expected HTTP request bytes");
            total += n;
            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if total == buf.len() {
                buf.resize(buf.len() * 2, 0);
            }
        }
        buf.truncate(total);
        buf
    }

    #[test]
    fn responses_url_appends_responses_to_base_path() {
        assert_eq!(
            responses_url("http://127.0.0.1:8080/v1").unwrap(),
            "http://127.0.0.1:8080/v1/responses"
        );
    }

    #[test]
    fn responses_url_preserves_responses_endpoint() {
        assert_eq!(
            responses_url("https://example.com/custom/responses/").unwrap(),
            "https://example.com/custom/responses"
        );
    }

    #[test]
    fn responses_url_rejects_invalid_url() {
        assert!(responses_url(":invalid").is_err());
    }

    #[test]
    fn configured_timeouts_match_defaults() {
        let timeouts = GrokTimeouts::default();
        assert_eq!(timeouts.connect_ms, 10_000);
        assert_eq!(timeouts.header_ms, 60_000);
        assert_eq!(timeouts.first_byte_ms, 60_000);
        assert_eq!(timeouts.body_idle_ms, 300_000);
    }

    #[test]
    fn auth_error_mapping_preserves_failure_category() {
        let credentials = auth_error(GrokAuthError::CredentialsInvalid {
            message: "invalid grant".into(),
        });
        assert_eq!(credentials.status, StatusCode::UNAUTHORIZED);
        assert_eq!(credentials.origin, GrokErrorOrigin::Auth);
        assert!(!credentials.is_retryable());

        let temporary = auth_error(GrokAuthError::Temporary {
            message: "discovery unavailable".into(),
        });
        assert_eq!(temporary.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(temporary.origin, GrokErrorOrigin::Auth);
        assert!(temporary.is_retryable());

        let limited = auth_error(GrokAuthError::RateLimited {
            message: "slow down".into(),
            retry_after: Some("11".into()),
        });
        assert_eq!(limited.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limited.retry_after.as_deref(), Some("11"));
        assert_eq!(limited.origin, GrokErrorOrigin::Auth);
        assert!(limited.is_retryable());
    }

    #[tokio::test]
    async fn auth_refresh_connect_failure_is_retryable_503_without_login_advice() {
        let unused_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", unused_listener.local_addr().unwrap());
        drop(unused_listener);
        let (client, _temp) = test_client_with_auth_issuer(
            "http://127.0.0.1:1/v1",
            issuer,
            0,
            GrokTimeouts {
                connect_ms: 100,
                header_ms: 100,
                first_byte_ms: 100,
                body_idle_ms: 100,
            },
        )
        .await;

        let error = match client.post(&sample_body(), None).await {
            Ok(_) => panic!("authentication refresh should fail before the Grok request"),
            Err(error) => error,
        };

        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.origin, GrokErrorOrigin::Auth);
        assert!(error.is_retryable());
        assert!(error.message.contains("temporarily unavailable"));
        assert!(!error.message.contains("Re-authenticate"));
    }

    #[tokio::test]
    async fn forced_refresh_rate_limit_is_not_flattened_to_401() {
        let auth_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let auth_addr = auth_listener.local_addr().unwrap();
        let issuer = format!("http://{auth_addr}");
        let auth_server_issuer = issuer.clone();
        let auth_server = tokio::spawn(async move {
            let (mut discovery, _) = auth_listener.accept().await.unwrap();
            let _ = read_http_request(&mut discovery).await;
            let body = serde_json::json!({
                "issuer": auth_server_issuer.clone(),
                "token_endpoint": format!("{auth_server_issuer}/oauth/token")
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            discovery.write_all(response.as_bytes()).await.unwrap();

            let (mut token, _) = auth_listener.accept().await.unwrap();
            let _ = read_http_request(&mut token).await;
            let body = br#"{"error":"slow_down"}"#;
            let response = format!(
                "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncontent-length: {}\r\nretry-after: 13\r\nconnection: close\r\n\r\n",
                body.len()
            );
            token.write_all(response.as_bytes()).await.unwrap();
            token.write_all(body).await.unwrap();
        });

        let model_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model_addr = model_listener.local_addr().unwrap();
        let model_server = tokio::spawn(async move {
            let (mut stream, _) = model_listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });

        let (client, _temp) = test_client_with_auth_issuer(
            &format!("http://{model_addr}/v1"),
            issuer,
            now_ms().saturating_add(3_600_000),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let error = match client.post(&sample_body(), None).await {
            Ok(_) => panic!("the forced refresh should be rate limited"),
            Err(error) => error,
        };
        model_server.await.unwrap();
        auth_server.await.unwrap();

        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error.retry_after.as_deref(), Some("13"));
        assert_eq!(error.origin, GrokErrorOrigin::Auth);
        assert!(error.is_retryable());
        assert!(!error.message.contains("Re-authenticate"));
    }

    #[tokio::test]
    async fn fourth_wire_attempt_401_is_terminal_without_oauth_refresh() {
        let auth_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", auth_listener.local_addr().unwrap());
        let auth_hits = Arc::new(AtomicUsize::new(0));
        let auth_hits_server = auth_hits.clone();
        let auth_server = tokio::spawn(async move {
            if let Ok(Ok((stream, _))) =
                tokio::time::timeout(Duration::from_millis(500), auth_listener.accept()).await
            {
                auth_hits_server.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });

        let model_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model_addr = model_listener.local_addr().unwrap();
        let model_hits = Arc::new(AtomicUsize::new(0));
        let model_hits_server = model_hits.clone();
        let model_server = tokio::spawn(async move {
            for attempt in 0..MAX_WIRE_ATTEMPTS {
                let (mut stream, _) = model_listener.accept().await.unwrap();
                let _ = read_http_request(&mut stream).await;
                model_hits_server.fetch_add(1, Ordering::SeqCst);
                let (status, body): (&str, &[u8]) = if attempt + 1 == MAX_WIRE_ATTEMPTS {
                    ("401 Unauthorized", b"")
                } else {
                    ("503 Service Unavailable", b"busy")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let (client, _temp) = test_client_with_auth_issuer(
            &format!("http://{model_addr}/v1"),
            issuer,
            now_ms().saturating_add(3_600_000),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let retry = Arc::new(Mutex::new(GrokRetryState::new()));
        let error = match client
            .post_with_retry(&sample_body(), None, retry.clone())
            .await
        {
            Ok(_) => panic!("the fourth attempt should terminate on 401"),
            Err(error) => error,
        };
        model_server.await.unwrap();
        auth_server.await.unwrap();

        assert_eq!(
            model_hits.load(Ordering::SeqCst),
            MAX_WIRE_ATTEMPTS as usize
        );
        assert_eq!(auth_hits.load(Ordering::SeqCst), 0);
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        assert!(!error.is_retryable());
        assert!(error.message.contains("no model retry attempts remain"));
        let state = retry.lock().await;
        assert_eq!(state.wire_attempt, MAX_WIRE_ATTEMPTS);
        assert!(!state.auth_refresh_succeeded);
        assert!(state.is_terminal());
    }

    #[tokio::test]
    async fn concurrent_reentry_waits_for_refresh_and_uses_rotated_token() {
        let auth_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let auth_addr = auth_listener.local_addr().unwrap();
        let issuer = format!("http://{auth_addr}");
        let auth_server_issuer = issuer.clone();
        let token_refreshes = Arc::new(AtomicUsize::new(0));
        let token_refreshes_server = token_refreshes.clone();
        let auth_server = tokio::spawn(async move {
            let (mut discovery, _) = auth_listener.accept().await.unwrap();
            let _ = read_http_request(&mut discovery).await;
            let body = serde_json::json!({
                "issuer": auth_server_issuer.clone(),
                "token_endpoint": format!("{auth_server_issuer}/oauth/token")
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            discovery.write_all(response.as_bytes()).await.unwrap();

            let (mut token, _) = auth_listener.accept().await.unwrap();
            let _ = read_http_request(&mut token).await;
            token_refreshes_server.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            let body = br#"{"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":3600}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            token.write_all(response.as_bytes()).await.unwrap();
            token.write_all(body).await.unwrap();
        });

        let model_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model_addr = model_listener.local_addr().unwrap();
        let model_server = tokio::spawn(async move {
            for attempt in 0..3 {
                let (mut stream, _) = model_listener.accept().await.unwrap();
                let request = read_http_request(&mut stream).await;
                let request = String::from_utf8_lossy(&request);
                if attempt == 0 {
                    assert!(request.contains("Bearer expired-access"));
                    stream
                        .write_all(
                            b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                } else {
                    assert!(request.contains("Bearer rotated-access"));
                    let body = b"data: ok\n\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.write_all(body).await.unwrap();
                }
            }
        });

        let (client, _temp) = test_client_with_auth_issuer(
            &format!("http://{model_addr}/v1"),
            issuer,
            now_ms().saturating_add(3_600_000),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let client = Arc::new(client);
        let retry = Arc::new(Mutex::new(GrokRetryState::new()));
        let first_body = sample_body();
        let second_body = sample_body();
        let (first, second) = tokio::join!(
            client.post_with_retry(&first_body, None, retry.clone()),
            client.post_with_retry(&second_body, None, retry.clone()),
        );
        let first = match first {
            Ok(response) => response,
            Err(error) => panic!("first request failed: {}", error.message),
        };
        let second = match second {
            Ok(response) => response,
            Err(error) => panic!("second request failed: {}", error.message),
        };
        assert_eq!(first.into_bytes().await.unwrap(), b"data: ok\n\n");
        assert_eq!(second.into_bytes().await.unwrap(), b"data: ok\n\n");
        model_server.await.unwrap();
        auth_server.await.unwrap();

        assert_eq!(token_refreshes.load(Ordering::SeqCst), 1);
        let state = retry.lock().await;
        assert_eq!(state.wire_attempt, 3);
        assert!(state.auth_refresh_succeeded);
        assert!(!state.is_terminal());
    }

    #[tokio::test]
    async fn retries_503_then_succeeds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = read_http_request(&mut stream).await;
                hits_server.fetch_add(1, Ordering::SeqCst);
                let (status, body): (&str, &[u8]) = if attempt == 0 {
                    ("503 Service Unavailable", b"retry")
                } else {
                    (
                        "200 OK",
                        b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n",
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let response = client.post(&sample_body(), None).await.unwrap();
        let body = response.into_bytes().await.unwrap();
        server.await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert!(String::from_utf8_lossy(&body).contains("response.completed"));
    }

    #[tokio::test]
    async fn retries_429_with_retry_after() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = read_http_request(&mut stream).await;
                hits_server.fetch_add(1, Ordering::SeqCst);
                let (status, body): (&str, &[u8]) = if attempt == 0 {
                    ("429 Too Many Requests", b"slow down")
                } else {
                    ("200 OK", b"data: ok\n\n")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let response = client.post(&sample_body(), None).await.unwrap();
        let _ = response.into_bytes().await.unwrap();
        server.await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn transport_reset_retries_within_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            // First connection is reset before headers.
            let (stream, _) = listener.accept().await.unwrap();
            hits_server.fetch_add(1, Ordering::SeqCst);
            drop(stream);

            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            let body = b"data: ok\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let response = client.post(&sample_body(), None).await.unwrap();
        let body = response.into_bytes().await.unwrap();
        server.await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(body, b"data: ok\n\n");
    }

    #[tokio::test]
    async fn shared_retry_budget_is_not_nested() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            // Exhaust the shared budget: 1 initial + 3 retries = 4 total.
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = read_http_request(&mut stream).await;
                hits_server.fetch_add(1, Ordering::SeqCst);
                let body = b"busy";
                let response = format!(
                    "HTTP/1.1 503 Service Unavailable\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let retry = Arc::new(Mutex::new(GrokRetryState::new()));
        let err = match client
            .post_with_retry(&sample_body(), None, retry.clone())
            .await
        {
            Ok(_) => panic!("budget exhaustion should fail"),
            Err(error) => error,
        };
        server.await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 4);
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!err.is_retryable());

        let wire_attempts = retry.lock().await.wire_attempt;
        let reentered = match client
            .post_with_retry(&sample_body(), None, retry.clone())
            .await
        {
            Ok(_) => panic!("an exhausted state must reject re-entry without another request"),
            Err(error) => error,
        };
        assert!(!reentered.is_retryable());
        assert_eq!(retry.lock().await.wire_attempt, wire_attempts);
    }

    #[tokio::test]
    async fn concurrent_reentry_atomically_reserves_at_most_four_wire_attempts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            loop {
                let accepted =
                    tokio::time::timeout(Duration::from_millis(500), listener.accept()).await;
                let Ok(Ok((mut stream, _))) = accepted else {
                    break;
                };
                let _ = read_http_request(&mut stream).await;
                hits_server.fetch_add(1, Ordering::SeqCst);
                let body = b"busy";
                let response = format!(
                    "HTTP/1.1 503 Service Unavailable\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let client = Arc::new(client);
        let retry = Arc::new(Mutex::new(GrokRetryState::new()));
        let first_body = sample_body();
        let second_body = sample_body();
        let (first, second) = tokio::join!(
            client.post_with_retry(&first_body, None, retry.clone()),
            client.post_with_retry(&second_body, None, retry.clone()),
        );
        server.await.unwrap();

        let first = match first {
            Ok(_) => panic!("the first concurrent caller should exhaust the budget"),
            Err(error) => error,
        };
        let second = match second {
            Ok(_) => panic!("the second concurrent caller should exhaust the budget"),
            Err(error) => error,
        };
        assert!(!first.is_retryable());
        assert!(!second.is_retryable());
        assert_eq!(hits.load(Ordering::SeqCst), MAX_WIRE_ATTEMPTS as usize);
        let state = retry.lock().await;
        assert_eq!(state.wire_attempt, MAX_WIRE_ATTEMPTS);
        assert!(state.is_terminal());
    }

    #[tokio::test]
    async fn active_long_stream_is_not_limited_by_whole_request_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            for chunk in [b"a".as_slice(), b"b", b"c"] {
                stream.write_all(b"1\r\n").await.unwrap();
                stream.write_all(chunk).await.unwrap();
                stream.write_all(b"\r\n").await.unwrap();
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
            stream.write_all(b"0\r\n\r\n").await.unwrap();
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 100,
                first_byte_ms: 200,
                body_idle_ms: 200,
            },
        )
        .await;
        let response = client.post(&sample_body(), None).await.unwrap();
        let body = response.into_bytes().await.unwrap();
        server.await.unwrap();
        assert_eq!(body, b"abc");
    }

    #[tokio::test]
    async fn first_byte_timeout_is_retryable_stream_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 50,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let response = client.post(&sample_body(), None).await.unwrap();
        let err = response.into_bytes().await.unwrap_err();
        let _ = server.await;
        assert_eq!(err.origin, GrokErrorOrigin::StreamTransport);
        assert_eq!(err.stage, GrokErrorStage::Body);
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn header_timeout_exhaustion_is_terminal_request_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // Keep accepting and holding connections so every retry hits the
            // header timeout rather than a later connection-refused path.
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let _ = read_http_request(&mut stream).await;
                // Never send response headers.
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 50,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        // Single attempt path: exhaust budget is slow due to backoff; assert the
        // classification from a direct timed-out attempt by using a shared retry
        // state already at the limit after one failure via post() retries.
        // Force one-shot classification by using post_with_retry with a pre-exhausted
        // budget after capturing the first error through a custom loop is heavy;
        // instead assert the final exhausted error still retains request transport shape.
        let err = match client.post(&sample_body(), None).await {
            Ok(_) => panic!("header timeout should fail"),
            Err(error) => error,
        };
        server.abort();
        assert_eq!(err.origin, GrokErrorOrigin::RequestTransport);
        assert_eq!(err.stage, GrokErrorStage::Header);
        assert!(!err.is_retryable());
        assert!(err.message.contains("headers"));
    }

    #[tokio::test]
    async fn body_idle_timeout_is_retryable_stream_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            stream.write_all(b"1\r\na\r\n").await.unwrap();
            // Stall after first body byte so idle timeout fires.
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 50,
            },
        )
        .await;
        let response = client.post(&sample_body(), None).await.unwrap();
        let err = response.into_bytes().await.unwrap_err();
        let _ = server.await;
        assert_eq!(err.origin, GrokErrorOrigin::StreamTransport);
        assert_eq!(err.stage, GrokErrorStage::Stream);
        assert!(err.is_retryable());
        assert!(err.message.contains("next Grok response body chunk"));
    }

    #[tokio::test]
    async fn rejected_body_drain_respects_timeouts_and_allows_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            // First attempt: 503 headers, then hang the error body.
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\ntransfer-encoding: chunked\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            drop(stream);

            // Second attempt succeeds.
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            let body = b"data: ok\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 50,
                body_idle_ms: 50,
            },
        )
        .await;
        let response = client.post(&sample_body(), None).await.unwrap();
        let body = response.into_bytes().await.unwrap();
        server.await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(body, b"data: ok\n\n");
    }

    #[tokio::test]
    async fn response_size_limit_is_not_retryable() {
        let err = GrokError::response_limit(
            GrokErrorStage::Body,
            "Grok upstream response exceeds the size limit",
        );
        assert_eq!(err.origin, GrokErrorOrigin::ResponseLimit);
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn non_retryable_4xx_fails_immediately() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            let body = b"bad";
            let response = format!(
                "HTTP/1.1 400 Bad Request\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let err = match client.post(&sample_body(), None).await {
            Ok(_) => panic!("non-retryable 4xx should fail immediately"),
            Err(error) => error,
        };
        server.await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn traffic_capture_records_retry_attempts_without_terminal_error_on_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = read_http_request(&mut stream).await;
                let (status, body): (&str, &[u8]) = if attempt == 0 {
                    ("503 Service Unavailable", b"retry")
                } else {
                    ("200 OK", b"data: done\n\n")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let temp = TempDir::new().unwrap();
        let traffic = Arc::new(test_capture(temp.path().join("traffic")));
        let (client, _auth_temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let response = client
            .post(&sample_body(), Some(traffic.clone()))
            .await
            .unwrap();
        let _ = response.into_bytes().await.unwrap();
        server.await.unwrap();

        let mut captured = String::new();
        let mut attempt_artifacts = 0;
        for entry in std::fs::read_dir(temp.path().join("traffic")).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                if path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().contains("022-upstream-attempt"))
                {
                    attempt_artifacts += 1;
                }
                captured.push_str(&std::fs::read_to_string(path).unwrap());
            }
        }
        assert_eq!(attempt_artifacts, 2);
        assert!(captured.contains("\"wait_ms\""));
        assert!(captured.contains("\"attempt\""));
        assert!(
            captured.contains("023-upstream-retry") || captured.contains("\"transient_failures\"")
        );
        assert!(!captured.contains("060-grok-stream-error"));
        assert!(!captured.contains("test-access"));
    }

    #[tokio::test]
    async fn traffic_capture_records_retry_exhausted() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = read_http_request(&mut stream).await;
                let body = b"busy";
                let response = format!(
                    "HTTP/1.1 503 Service Unavailable\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let temp = TempDir::new().unwrap();
        let traffic = Arc::new(test_capture(temp.path().join("traffic")));
        let (client, _auth_temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 1_000,
                first_byte_ms: 1_000,
                body_idle_ms: 1_000,
            },
        )
        .await;
        let err = match client.post(&sample_body(), Some(traffic.clone())).await {
            Ok(_) => panic!("retry budget should exhaust"),
            Err(error) => error,
        };
        server.await.unwrap();
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);

        let mut captured = String::new();
        for entry in std::fs::read_dir(temp.path().join("traffic")).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                captured.push_str(&std::fs::read_to_string(path).unwrap());
            }
        }
        assert!(captured.contains("retry_exhausted"));
        assert!(captured.contains("060-grok-stream-error") || captured.contains("\"kind\""));
        assert!(!captured.contains("test-access"));
    }
}
