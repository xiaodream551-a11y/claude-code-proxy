use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::anthropic::sse::parse_sse_events;
use crate::config;
use crate::logging::create_logger;
use crate::provider::RequestContext;
use crate::retry::{compute_backoff_delay, should_retry_status, sleep};
use crate::traffic::TrafficCapture;

use super::auth::constants::{CODEX_API_ENDPOINT, ORIGINATOR, RESPONSES_LITE_ORIGINATOR};
use super::auth::manager::CodexAuthManager;
use super::auth::token_store::{DefaultCodexAuthStore, StoredAuth, file_store};
use super::translate::request::ResponsesRequest;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct CodexError {
    pub status: u16,
    pub message: String,
    pub detail: Option<String>,
    pub retry_after: Option<String>,
    pub origin: CodexErrorOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexErrorOrigin {
    Http,
    WebSocket,
    WebSocketHandshake,
    Auth,
    BufferedHttp,
    BufferedWebSocket,
}

impl CodexError {
    pub fn new(status: u16, message: String) -> Self {
        Self {
            status,
            message,
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::Http,
        }
    }
}

impl std::fmt::Display for CodexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Codex error {}: {}", self.status, self.message)
    }
}

#[derive(Debug)]
pub struct CodexHeaderTimeoutError {
    pub timeout_ms: u64,
}

impl std::fmt::Display for CodexHeaderTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Timed out waiting {}ms for Codex response headers",
            self.timeout_ms
        )
    }
}

#[derive(Debug)]
pub struct CodexTransportError {
    pub message: String,
}

impl std::fmt::Display for CodexTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Codex transport error: {}", self.message)
    }
}

// ---------------------------------------------------------------------------
// Header builder
// ---------------------------------------------------------------------------

fn default_user_agent(use_responses_lite: bool) -> String {
    if use_responses_lite {
        RESPONSES_LITE_ORIGINATOR.to_string()
    } else {
        format!("claude-code-proxy/{}", env!("CARGO_PKG_VERSION"))
    }
}

pub fn build_codex_headers(
    auth: &StoredAuth,
    ctx: &RequestContext,
    use_responses_lite: bool,
) -> Result<http::HeaderMap, CodexError> {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        header_value("content-type", "application/json")?,
    );
    headers.insert(
        http::header::ACCEPT,
        header_value("accept", "text/event-stream")?,
    );
    let bearer = format!("Bearer {}", auth.access);
    headers.insert(
        http::header::AUTHORIZATION,
        header_value("authorization", &bearer)?,
    );
    let originator = if use_responses_lite {
        RESPONSES_LITE_ORIGINATOR.to_string()
    } else {
        config::codex_originator(ORIGINATOR)
    };
    headers.insert("originator", header_value("originator", &originator)?);
    headers.insert(
        "openai-beta",
        header_value("openai-beta", "responses=experimental")?,
    );
    if use_responses_lite {
        headers.insert(
            "x-openai-internal-codex-responses-lite",
            header_value("x-openai-internal-codex-responses-lite", "true")?,
        );
    }
    if let Some(ref account_id) = auth.account_id {
        headers.insert(
            "ChatGPT-Account-Id",
            header_value("ChatGPT-Account-Id", account_id)?,
        );
    }
    if let Some(ref session_id) = ctx.session_id {
        headers.insert("session_id", header_value("session_id", session_id)?);
        headers.insert(
            "x-client-request-id",
            header_value("x-client-request-id", session_id)?,
        );
        let window_id = format!("{session_id}:0");
        headers.insert(
            "x-codex-window-id",
            header_value("x-codex-window-id", &window_id)?,
        );
    }
    let user_agent = config::codex_user_agent(&default_user_agent(use_responses_lite));
    if !user_agent.is_empty() {
        headers.insert(
            http::header::USER_AGENT,
            header_value("user-agent", &user_agent)?,
        );
    }
    Ok(headers)
}

fn header_value(name: &str, value: &str) -> Result<http::HeaderValue, CodexError> {
    http::HeaderValue::from_str(value).map_err(|e| CodexError {
        status: 500,
        message: format!("Failed to parse {name} header"),
        detail: Some(e.to_string()),
        retry_after: None,
        origin: CodexErrorOrigin::Http,
    })
}

// ---------------------------------------------------------------------------
// WebSocket request shaping
// ---------------------------------------------------------------------------

pub fn build_websocket_request(
    body: &ResponsesRequest,
    continuation: Option<&super::continuation::ContinuationCandidate>,
) -> serde_json::Value {
    let mut payload = serde_json::to_value(body).unwrap_or_default();
    let obj = payload.as_object_mut().expect("request must be an object");

    // Omit the stream field for WebSocket transport
    obj.remove("stream");
    obj.insert("type".to_string(), serde_json::json!("response.create"));

    // Apply continuation if available
    if let Some(candidate) = continuation {
        if let Some(ref prev_id) = candidate.previous_response_id {
            obj.insert(
                "previous_response_id".to_string(),
                serde_json::json!(prev_id),
            );
        }
        if let Some(ref delta) = candidate.input_delta {
            obj.insert(
                "input".to_string(),
                serde_json::to_value(delta).unwrap_or_default(),
            );
        }
    }

    payload
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

pub struct CodexResponse {
    pub body: Vec<u8>,
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

const MAX_BUFFERED_TRANSPORT_RETRIES: u32 = 3;
const MAX_BUFFERED_TRANSPORT_ATTEMPTS: u32 = MAX_BUFFERED_TRANSPORT_RETRIES + 1;
const HTTP_RESPONSE_BODY_IDLE_TIMEOUT_MS: u64 = 300_000;
pub(super) const DEFAULT_CODEX_TOTAL_TIMEOUT_MS: u64 = 540_000;
pub(super) const CODEX_TOTAL_TIMEOUT_DETAIL: &str = "codex_total_timeout";
const MAX_CODEX_TOTAL_TIMEOUT_MS: u64 = 10 * 365 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy)]
pub(super) struct CodexRequestDeadline {
    at: tokio::time::Instant,
    timeout_ms: u64,
}

impl CodexRequestDeadline {
    pub(super) fn configured_from(started_at: Instant) -> Self {
        Self::from_started_at(
            started_at,
            config::codex_total_timeout_ms(DEFAULT_CODEX_TOTAL_TIMEOUT_MS),
        )
    }

    pub(super) fn from_timeout_ms(timeout_ms: u64) -> Self {
        let timeout_ms = timeout_ms.min(MAX_CODEX_TOTAL_TIMEOUT_MS);
        let now = tokio::time::Instant::now();
        Self {
            at: now
                .checked_add(Duration::from_millis(timeout_ms))
                .unwrap_or(now),
            timeout_ms,
        }
    }

    fn from_started_at(started_at: Instant, timeout_ms: u64) -> Self {
        let timeout_ms = timeout_ms.min(MAX_CODEX_TOTAL_TIMEOUT_MS);
        let remaining = Duration::from_millis(timeout_ms).saturating_sub(started_at.elapsed());
        let now = tokio::time::Instant::now();
        Self {
            at: now.checked_add(remaining).unwrap_or(now),
            timeout_ms,
        }
    }

    pub(super) fn at(self) -> tokio::time::Instant {
        self.at
    }

    pub(super) fn timeout_ms(self) -> u64 {
        self.timeout_ms
    }
}

struct CodexRequestGuard {
    session_id: Option<String>,
    turn_id: Option<u64>,
    pool_key: Option<String>,
    armed: bool,
}

impl CodexRequestGuard {
    fn new(
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
        transport: crate::config::CodexTransport,
    ) -> Self {
        Self {
            session_id: ctx.session_id.clone(),
            turn_id: continuation.and_then(|candidate| candidate.turn_id),
            pool_key: if matches!(transport, crate::config::CodexTransport::Http) {
                None
            } else {
                websocket_pool_key(ctx, continuation).map(str::to_string)
            },
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CodexRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Invalidation is turn-gated, so it must run while the continuation
        // still owns the turn. Aborting first would make this a no-op and could
        // leave a partially consumed pooled socket available for reuse.
        if let Some(key) = self.pool_key.as_deref() {
            super::websocket::invalidate_codex_websocket_pool_turn(key, self.turn_id);
        }
        super::continuation::abort_continuation(self.session_id.as_deref(), self.turn_id);
    }
}

pub struct CodexHttpClient {
    client: reqwest::Client,
    auth_manager: CodexAuthManager<DefaultCodexAuthStore>,
    base_url: String,
    header_timeout_ms: u64,
    body_idle_timeout_ms: u64,
    total_timeout_ms: u64,
    #[allow(dead_code)]
    header_timeout_retries: u32,
}

impl Default for CodexHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexHttpClient {
    pub fn new() -> Self {
        let timeout_ms = 60_000;
        Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(15))
                .build()
                .expect("failed to create HTTP client"),
            auth_manager: CodexAuthManager::new(file_store()),
            base_url: config::codex_base_url(CODEX_API_ENDPOINT),
            header_timeout_ms: timeout_ms,
            body_idle_timeout_ms: HTTP_RESPONSE_BODY_IDLE_TIMEOUT_MS,
            total_timeout_ms: config::codex_total_timeout_ms(DEFAULT_CODEX_TOTAL_TIMEOUT_MS),
            header_timeout_retries: 1,
        }
    }

    pub fn new_with_client(
        client: reqwest::Client,
        auth_manager: CodexAuthManager<DefaultCodexAuthStore>,
        base_url: String,
    ) -> Self {
        Self {
            client,
            auth_manager,
            base_url,
            header_timeout_ms: 60_000,
            body_idle_timeout_ms: HTTP_RESPONSE_BODY_IDLE_TIMEOUT_MS,
            total_timeout_ms: config::codex_total_timeout_ms(DEFAULT_CODEX_TOTAL_TIMEOUT_MS),
            header_timeout_retries: 1,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(
        client: reqwest::Client,
        base_url: String,
        header_timeout_ms: u64,
        body_idle_timeout_ms: u64,
        total_timeout_ms: u64,
        header_timeout_retries: u32,
    ) -> Self {
        Self {
            client,
            auth_manager: CodexAuthManager::new(file_store()),
            base_url,
            header_timeout_ms,
            body_idle_timeout_ms,
            total_timeout_ms,
            header_timeout_retries,
        }
    }

    pub fn auth_manager(&self) -> &CodexAuthManager<DefaultCodexAuthStore> {
        &self.auth_manager
    }

    pub async fn post_codex(
        &self,
        body: &ResponsesRequest,
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
    ) -> Result<CodexResponse, CodexError> {
        self.post_codex_with_transport(body, ctx, continuation, crate::config::codex_transport())
            .await
    }

    pub(super) async fn post_codex_before(
        &self,
        body: &ResponsesRequest,
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
        deadline: CodexRequestDeadline,
    ) -> Result<CodexResponse, CodexError> {
        self.post_codex_with_transport_before(
            body,
            ctx,
            continuation,
            crate::config::codex_transport(),
            deadline,
        )
        .await
    }

    pub(super) async fn post_codex_http_before(
        &self,
        body: &ResponsesRequest,
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
        deadline: CodexRequestDeadline,
    ) -> Result<CodexResponse, CodexError> {
        self.post_codex_with_transport_before(
            body,
            ctx,
            continuation,
            crate::config::CodexTransport::Http,
            deadline,
        )
        .await
    }

    async fn post_codex_with_transport(
        &self,
        body: &ResponsesRequest,
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
        transport: crate::config::CodexTransport,
    ) -> Result<CodexResponse, CodexError> {
        self.post_codex_with_transport_before(
            body,
            ctx,
            continuation,
            transport,
            CodexRequestDeadline::from_timeout_ms(self.total_timeout_ms),
        )
        .await
    }

    async fn post_codex_with_transport_before(
        &self,
        body: &ResponsesRequest,
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
        transport: crate::config::CodexTransport,
        deadline: CodexRequestDeadline,
    ) -> Result<CodexResponse, CodexError> {
        let mut guard = CodexRequestGuard::new(ctx, continuation, transport);
        let result = match tokio::time::timeout_at(
            deadline.at(),
            self.post_codex_with_transport_inner(body, ctx, continuation, transport),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(codex_total_timeout_error(transport, deadline.timeout_ms())),
        };
        if result.is_ok() {
            guard.disarm();
        }
        result
    }

    async fn post_codex_with_transport_inner(
        &self,
        body: &ResponsesRequest,
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
        transport: crate::config::CodexTransport,
    ) -> Result<CodexResponse, CodexError> {
        use crate::config::CodexTransport;

        let mut auth = self.auth_manager.get_auth().await.map_err(|e| CodexError {
            status: 401,
            message: "Auth error".to_string(),
            detail: Some(e.to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::Auth,
        })?;

        let initial_pool_key = websocket_pool_key(ctx, continuation);
        if should_reset_websocket_pool(continuation)
            && let Some(key) = initial_pool_key
        {
            super::websocket::invalidate_codex_websocket_pool_turn(
                key,
                continuation.and_then(|candidate| candidate.turn_id),
            );
        }

        let turn_id = continuation.and_then(|candidate| candidate.turn_id);
        let mut active_continuation = continuation.cloned();
        // Auto may degrade to HTTP, but never switches back within this logical request.
        let mut active_transport = transport;
        let mut auth_refresh_attempted = false;
        // Count every actual Codex request, including an immediate Auto fallback.
        let mut physical_attempts = 0u32;
        let websocket_timeouts = super::websocket::CodexWebSocketTimeouts::configured();
        loop {
            let pool_key = websocket_pool_key(ctx, active_continuation.as_ref());
            let (result, result_transport) = match active_transport {
                CodexTransport::Http => {
                    let body_json = serialize_request(body)?;
                    physical_attempts += 1;
                    (
                        self.attempt_post_http(
                            &auth,
                            &body_json,
                            ctx,
                            body.client_metadata.is_some(),
                        )
                        .await,
                        CodexTransport::Http,
                    )
                }
                CodexTransport::WebSocket => {
                    let ws_headers =
                        build_codex_headers(&auth, ctx, body.client_metadata.is_some())?;
                    let ws_headers = super::websocket::codex_websocket_headers(&ws_headers);
                    let ws_body = build_websocket_request(body, active_continuation.as_ref());

                    physical_attempts += 1;
                    (
                        super::websocket::codex_websocket_request(
                            &self.base_url,
                            &ws_headers,
                            &ws_body,
                            ctx,
                            ctx.traffic.as_deref(),
                            pool_key,
                            websocket_timeouts,
                            active_continuation.as_ref(),
                        )
                        .await,
                        CodexTransport::WebSocket,
                    )
                }
                CodexTransport::Auto => {
                    let circuit_key = ctx.session_id.as_deref();
                    if circuit_key.is_some_and(super::websocket::codex_websocket_circuit_open) {
                        log_websocket_circuit_fallback(ctx);
                        active_transport = CodexTransport::Http;
                        let body_json = serialize_request(body)?;
                        physical_attempts += 1;
                        (
                            self.attempt_post_http(
                                &auth,
                                &body_json,
                                ctx,
                                body.client_metadata.is_some(),
                            )
                            .await,
                            CodexTransport::Http,
                        )
                    } else {
                        let ws_headers =
                            build_codex_headers(&auth, ctx, body.client_metadata.is_some())?;
                        let ws_headers = super::websocket::codex_websocket_headers(&ws_headers);
                        let ws_body = build_websocket_request(body, active_continuation.as_ref());

                        // Try WebSocket first
                        physical_attempts += 1;
                        let ws_result = super::websocket::codex_websocket_request(
                            &self.base_url,
                            &ws_headers,
                            &ws_body,
                            ctx,
                            ctx.traffic.as_deref(),
                            pool_key,
                            websocket_timeouts,
                            active_continuation.as_ref(),
                        )
                        .await;

                        match ws_result {
                            Ok(response) => {
                                if let Some(key) = circuit_key {
                                    super::websocket::record_codex_websocket_success(key);
                                }
                                (Ok(response), CodexTransport::WebSocket)
                            }
                            Err(err) if should_fallback_to_http(&err) => {
                                record_auto_websocket_failure(ctx, &err);
                                if physical_attempts >= MAX_BUFFERED_TRANSPORT_ATTEMPTS {
                                    (Err(err), CodexTransport::WebSocket)
                                } else {
                                    log_auto_http_fallback(ctx, &err);
                                    if let Some(key) = pool_key {
                                        super::websocket::invalidate_codex_websocket_pool_turn(
                                            key, turn_id,
                                        );
                                    }
                                    active_continuation =
                                        full_context_continuation(active_continuation.as_ref());
                                    active_transport = CodexTransport::Http;
                                    // Immediate buffered HTTP for retryable pre-output WS transport faults.
                                    let body_json = serialize_request(body)?;
                                    physical_attempts += 1;
                                    (
                                        self.attempt_post_http(
                                            &auth,
                                            &body_json,
                                            ctx,
                                            body.client_metadata.is_some(),
                                        )
                                        .await,
                                        CodexTransport::Http,
                                    )
                                }
                            }
                            Err(err) => {
                                record_auto_websocket_failure(ctx, &err);
                                (Err(err), CodexTransport::WebSocket)
                            }
                        }
                    }
                }
            };

            if physical_attempts < MAX_BUFFERED_TRANSPORT_ATTEMPTS
                && should_refresh_after_unauthorized(&result, auth_refresh_attempted)
            {
                auth_refresh_attempted = true;
                match self.auth_manager.force_refresh(&auth.access).await {
                    Ok(new_auth) => {
                        auth = new_auth;
                        if let Some(key) = pool_key {
                            super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                        }
                        active_continuation =
                            full_context_continuation(active_continuation.as_ref());
                        continue;
                    }
                    Err(e) => {
                        return Err(CodexError {
                            status: 401,
                            message: "Unauthorized".to_string(),
                            detail: Some(e.to_string()),
                            retry_after: None,
                            origin: CodexErrorOrigin::Http,
                        });
                    }
                }
            }

            if let Ok(response) = &result
                && (200..300).contains(&response.status)
                && let Some(failure) = super::events::first_retryable_failure(&response.body)
            {
                if physical_attempts < MAX_BUFFERED_TRANSPORT_ATTEMPTS {
                    let delay = compute_backoff_delay(
                        physical_attempts.saturating_sub(1),
                        failure.retry_after.as_deref(),
                    );
                    if delay.exceeds_budget {
                        return Err(CodexError {
                            status: failure.status,
                            message: failure.message.clone(),
                            detail: Some(failure.message),
                            retry_after: failure.retry_after,
                            origin: buffered_origin(result_transport),
                        });
                    }
                    log_buffered_retry(
                        ctx,
                        result_transport,
                        physical_attempts,
                        delay.wait_ms,
                        failure.status,
                        "upstream_event",
                        &failure.message,
                    );
                    active_continuation = full_context_continuation(active_continuation.as_ref());
                    sleep(delay.wait_ms).await;
                    continue;
                }

                log_buffered_retry_exhausted(
                    ctx,
                    result_transport,
                    failure.status,
                    "upstream_event",
                    &failure.message,
                );
                return Err(CodexError {
                    status: failure.status,
                    message: failure.message.clone(),
                    detail: Some(failure.message),
                    retry_after: failure.retry_after,
                    origin: buffered_origin(result_transport),
                });
            }

            match result {
                Ok(response) if response.status == 401 => {
                    let detail = String::from_utf8_lossy(&response.body).to_string();
                    return Err(CodexError {
                        status: 401,
                        message: "Unauthorized".to_string(),
                        detail: Some(detail),
                        retry_after: None,
                        origin: CodexErrorOrigin::Http,
                    });
                }
                Ok(response) if response.status == 403 => {
                    let detail = String::from_utf8_lossy(&response.body).to_string();
                    return Err(CodexError {
                        status: 403,
                        message: "Forbidden".to_string(),
                        detail: Some(detail),
                        retry_after: None,
                        origin: CodexErrorOrigin::Http,
                    });
                }
                Ok(response) if response.status == 429 => {
                    let retry_after = response
                        .headers
                        .iter()
                        .find(|(k, _)| k.to_lowercase() == "retry-after")
                        .map(|(_, v)| v.clone());
                    if physical_attempts < MAX_BUFFERED_TRANSPORT_ATTEMPTS {
                        let delay = compute_backoff_delay(
                            physical_attempts.saturating_sub(1),
                            retry_after.as_deref(),
                        );
                        if delay.exceeds_budget {
                            let detail = String::from_utf8_lossy(&response.body).to_string();
                            return Err(CodexError {
                                status: 429,
                                message: "Rate limited".to_string(),
                                detail: Some(detail),
                                retry_after,
                                origin: CodexErrorOrigin::Http,
                            });
                        }
                        log_buffered_retry(
                            ctx,
                            result_transport,
                            physical_attempts,
                            delay.wait_ms,
                            response.status,
                            "upstream",
                            "rate limited",
                        );
                        sleep(delay.wait_ms).await;
                        continue;
                    }
                    let detail = String::from_utf8_lossy(&response.body).to_string();
                    log_buffered_retry_exhausted(
                        ctx,
                        result_transport,
                        response.status,
                        "upstream",
                        "rate limited",
                    );
                    return Err(CodexError {
                        status: 429,
                        message: "Rate limited".to_string(),
                        detail: Some(detail),
                        retry_after,
                        origin: CodexErrorOrigin::Http,
                    });
                }
                Ok(response) if should_retry_codex_status(response.status) => {
                    if physical_attempts < MAX_BUFFERED_TRANSPORT_ATTEMPTS {
                        let retry_after = response
                            .headers
                            .iter()
                            .find(|(key, _)| key.eq_ignore_ascii_case("retry-after"))
                            .map(|(_, value)| value.as_str());
                        let delay =
                            compute_backoff_delay(physical_attempts.saturating_sub(1), retry_after);
                        if delay.exceeds_budget {
                            return Err(codex_status_error(response, result_transport));
                        }
                        log_buffered_retry(
                            ctx,
                            result_transport,
                            physical_attempts,
                            delay.wait_ms,
                            response.status,
                            "upstream",
                            "retryable upstream status",
                        );
                        sleep(delay.wait_ms).await;
                        continue;
                    }
                    log_buffered_retry_exhausted(
                        ctx,
                        result_transport,
                        response.status,
                        "upstream",
                        "retryable upstream status",
                    );
                    return Err(codex_status_error(response, result_transport));
                }
                Ok(response) if !(200..300).contains(&response.status) => {
                    return Err(codex_status_error(response, result_transport));
                }
                Ok(response) => return Ok(response),
                Err(err)
                    if physical_attempts < MAX_BUFFERED_TRANSPORT_ATTEMPTS
                        && should_retry_without_continuation(
                            &err,
                            active_continuation.as_ref(),
                        ) =>
                {
                    if let Some(key) = pool_key {
                        super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                    }
                    active_continuation = full_context_continuation(active_continuation.as_ref());
                    continue;
                }
                Err(err) => {
                    // Determine if retryable
                    let retryable = is_retryable_transport_error(&err);
                    if retryable && physical_attempts < MAX_BUFFERED_TRANSPORT_ATTEMPTS {
                        let delay = compute_backoff_delay(
                            physical_attempts.saturating_sub(1),
                            err.retry_after.as_deref(),
                        );
                        if delay.exceeds_budget {
                            return Err(err);
                        }
                        log_buffered_retry(
                            ctx,
                            result_transport,
                            physical_attempts,
                            delay.wait_ms,
                            err.status,
                            codex_error_origin_name(err.origin),
                            &err.message,
                        );
                        sleep(delay.wait_ms).await;
                        continue;
                    }
                    if retryable {
                        log_buffered_retry_exhausted(
                            ctx,
                            result_transport,
                            err.status,
                            codex_error_origin_name(err.origin),
                            &err.message,
                        );
                    }
                    return Err(err);
                }
            }
        }
    }

    pub async fn stream_codex_websocket_events(
        self: &Arc<Self>,
        body: &ResponsesRequest,
        ctx: &RequestContext,
        continuation: Option<&super::continuation::ContinuationCandidate>,
    ) -> Result<super::websocket::CodexWebSocketEventReceiver, CodexError> {
        let auth = self.auth_manager.get_auth().await.map_err(|e| CodexError {
            status: 401,
            message: "Auth error".to_string(),
            detail: Some(e.to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::Auth,
        })?;

        let turn_id = continuation.and_then(|candidate| candidate.turn_id);
        let pool_key = websocket_pool_key(ctx, continuation).map(str::to_string);
        if should_reset_websocket_pool(continuation)
            && let Some(key) = pool_key.as_deref()
        {
            super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
        }

        let client = self.clone();
        let body = body.clone();
        let ctx = ctx.clone();
        let continuation = continuation.cloned();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            client
                .coordinate_live_websocket_events(body, ctx, continuation, auth, pool_key, tx)
                .await;
        });

        Ok(rx)
    }

    async fn coordinate_live_websocket_events(
        &self,
        body: ResponsesRequest,
        ctx: RequestContext,
        mut continuation: Option<super::continuation::ContinuationCandidate>,
        mut auth: StoredAuth,
        pool_key: Option<String>,
        tx: tokio::sync::mpsc::Sender<Result<serde_json::Value, CodexError>>,
    ) {
        let turn_id = continuation
            .as_ref()
            .and_then(|candidate| candidate.turn_id);
        let mut auth_refresh_attempted = false;
        let mut continuation_retry_available = continuation
            .as_ref()
            .and_then(|candidate| candidate.previous_response_id.as_deref())
            .is_some();
        let mut forwarded_any = false;
        let websocket_timeouts = super::websocket::CodexWebSocketTimeouts::configured();

        'attempt: loop {
            let ws_headers = match build_codex_headers(&auth, &ctx, body.client_metadata.is_some())
            {
                Ok(headers) => super::websocket::codex_websocket_headers(&headers),
                Err(err) => {
                    let _ = tx.send(Err(err)).await;
                    return;
                }
            };
            let ws_body = build_websocket_request(&body, continuation.as_ref());
            let start = super::websocket::codex_websocket_event_stream(
                &self.base_url,
                &ws_headers,
                &ws_body,
                &ctx,
                ctx.traffic.clone(),
                pool_key.as_deref(),
                websocket_timeouts,
                continuation.as_ref(),
            );
            let mut stream = tokio::select! {
                _ = tx.closed() => {
                    if let Some(key) = pool_key.as_deref() {
                        super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                    }
                    super::continuation::abort_continuation(ctx.session_id.as_deref(), turn_id);
                    return;
                }
                result = start => match result {
                    Ok(stream) => stream,
                    Err(err) if err.status == 401 && !auth_refresh_attempted && !forwarded_any => {
                        auth_refresh_attempted = true;
                        if let Some(key) = pool_key.as_deref() {
                            super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                        }
                        let refresh = self.auth_manager.force_refresh(&auth.access);
                        auth = tokio::select! {
                            _ = tx.closed() => {
                                super::continuation::abort_continuation(
                                    ctx.session_id.as_deref(),
                                    turn_id,
                                );
                                return;
                            }
                            result = refresh => match result {
                                Ok(auth) => auth,
                                Err(refresh_err) => {
                                    let _ = tx.send(Err(auth_refresh_error(refresh_err))).await;
                                    return;
                                }
                            }
                        };
                        continuation = full_context_continuation(continuation.as_ref());
                        continue 'attempt;
                    }
                    Err(err) if continuation_retry_available && is_continuation_retry_error(&err) => {
                        continuation_retry_available = false;
                        if let Some(key) = pool_key.as_deref() {
                            super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                        }
                        continuation = full_context_continuation(continuation.as_ref());
                        continue 'attempt;
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                }
            };

            loop {
                let item = tokio::select! {
                    _ = tx.closed() => {
                        if let Some(key) = pool_key.as_deref() {
                            super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                        }
                        super::continuation::abort_continuation(
                            ctx.session_id.as_deref(),
                            turn_id,
                        );
                        return;
                    }
                    item = stream.recv() => item,
                };
                let Some(item) = item else {
                    return;
                };

                let unauthorized = match &item {
                    Err(err) => err.status == 401,
                    Ok(payload) => super::websocket::event_error_status(payload) == Some(401),
                };
                if unauthorized && !auth_refresh_attempted && !forwarded_any {
                    auth_refresh_attempted = true;
                    if let Some(key) = pool_key.as_deref() {
                        super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                    }
                    let refresh = self.auth_manager.force_refresh(&auth.access);
                    auth = tokio::select! {
                        _ = tx.closed() => {
                            super::continuation::abort_continuation(
                                ctx.session_id.as_deref(),
                                turn_id,
                            );
                            return;
                        }
                        result = refresh => match result {
                            Ok(auth) => auth,
                            Err(refresh_err) => {
                                let _ = tx.send(Err(auth_refresh_error(refresh_err))).await;
                                return;
                            }
                        }
                    };
                    continue 'attempt;
                }

                if let Err(err) = &item
                    && continuation_retry_available
                    && is_continuation_retry_error(err)
                    && !forwarded_any
                {
                    continuation_retry_available = false;
                    if let Some(key) = pool_key.as_deref() {
                        super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                    }
                    continuation = full_context_continuation(continuation.as_ref());
                    continue 'attempt;
                }

                if item.as_ref().is_ok_and(event_closes_live_retry_window) {
                    forwarded_any = true;
                }
                if tx.send(item).await.is_err() {
                    if let Some(key) = pool_key.as_deref() {
                        super::websocket::invalidate_codex_websocket_pool_turn(key, turn_id);
                    }
                    super::continuation::abort_continuation(ctx.session_id.as_deref(), turn_id);
                    return;
                }
            }
        }
    }

    async fn attempt_post_http(
        &self,
        auth: &StoredAuth,
        body_json: &str,
        ctx: &RequestContext,
        use_responses_lite: bool,
    ) -> Result<CodexResponse, CodexError> {
        let url = &self.base_url;
        let headers = build_codex_headers(auth, ctx, use_responses_lite)?;

        if let Some(traffic) = ctx.traffic.as_deref() {
            write_codex_http_request_capture(traffic, url, &headers, body_json);
        }

        // Build headers
        let mut req_builder = self.client.post(url);
        for (key, value) in headers.iter() {
            req_builder = req_builder.header(key.as_str(), value.as_bytes());
        }

        // Apply header timeout
        let started_at = Instant::now();
        let send_fut = req_builder.body(body_json.to_string()).send();
        let header_timeout_dur = Duration::from_millis(self.header_timeout_ms);

        let mut resp = tokio::time::timeout(header_timeout_dur, send_fut)
            .await
            .map_err(|_| CodexError {
                status: 0,
                message: format!(
                    "Timed out waiting {}ms for Codex response headers",
                    self.header_timeout_ms
                ),
                detail: None,
                retry_after: None,
                origin: CodexErrorOrigin::Http,
            })?
            .map_err(|e| {
                if is_retryable_reqwest_error(&e) {
                    CodexError {
                        status: 0,
                        message: format!("Transport error: {e}"),
                        detail: None,
                        retry_after: None,
                        origin: CodexErrorOrigin::Http,
                    }
                } else {
                    CodexError {
                        status: 0,
                        message: format!("Network error: {e}"),
                        detail: None,
                        retry_after: None,
                        origin: CodexErrorOrigin::Http,
                    }
                }
            })?;

        let status = resp.status().as_u16();
        let headers: Vec<(String, String)> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let mut body_bytes = Vec::new();
        let mut response_started = false;
        loop {
            let chunk = tokio::time::timeout(
                Duration::from_millis(self.body_idle_timeout_ms),
                resp.chunk(),
            )
            .await
            .map_err(|_| CodexError {
                status: 0,
                message: format!(
                    "Timed out waiting {}ms for the next Codex response body chunk",
                    self.body_idle_timeout_ms
                ),
                detail: Some("http_response_body".to_string()),
                retry_after: None,
                origin: CodexErrorOrigin::Http,
            })?
            .map_err(|e| CodexError {
                status: 0,
                message: format!("Transport error reading Codex response body: {e}"),
                detail: Some("http_response_body".to_string()),
                retry_after: None,
                origin: CodexErrorOrigin::Http,
            })?;

            let Some(chunk) = chunk else {
                break;
            };
            if !response_started {
                if let Some(monitor) = ctx.monitor.as_ref() {
                    monitor.generation_started(&ctx.req_id);
                }
                response_started = true;
            }
            body_bytes.extend_from_slice(&chunk);
        }

        if let Some(traffic) = ctx.traffic.as_deref() {
            write_upstream_response_capture(
                traffic,
                status,
                started_at.elapsed(),
                &headers,
                &body_bytes,
            );
        }

        Ok(CodexResponse {
            body: body_bytes,
            status,
            headers,
        })
    }
}

fn write_codex_http_request_capture(
    traffic: &TrafficCapture,
    url: &str,
    headers: &http::HeaderMap,
    body_json: &str,
) {
    let body = serde_json::from_str(body_json).unwrap_or_else(|_| {
        serde_json::json!({
            "unparseable": true,
            "bytes": body_json.len(),
        })
    });
    traffic.write_json("020-upstream-request", &body);
    traffic.write_json(
        "021-upstream-request-metadata",
        &serde_json::json!({
            "provider": "codex",
            "transport": "http",
            "url": url,
            "method": "POST",
            "headers": headers_to_json(headers),
            "size": summarize_json_request_size(&body, body_json),
        }),
    );
}

fn write_upstream_response_capture(
    traffic: &TrafficCapture,
    status: u16,
    elapsed: Duration,
    headers: &[(String, String)],
    body: &[u8],
) {
    traffic.write_json(
        "030-upstream-response-headers",
        &serde_json::json!({
            "status": status,
            "elapsedMs": elapsed.as_millis(),
            "headers": headers_to_json_from_pairs(headers),
        }),
    );
    if status >= 400 {
        traffic.write_text("031-upstream-error-body", &String::from_utf8_lossy(body));
    } else {
        traffic.write_bytes("032-upstream-response-body.sse", body);
        write_codex_sse_event_capture(traffic, body);
    }
}

fn write_codex_sse_event_capture(traffic: &TrafficCapture, body: &[u8]) {
    for event in parse_sse_events(body) {
        if event.data == "[DONE]" {
            traffic.write_json_event(
                "040-upstream-event",
                &serde_json::json!({
                    "event": event.event,
                    "data": "[DONE]",
                }),
            );
            continue;
        }

        match serde_json::from_str::<serde_json::Value>(&event.data) {
            Ok(mut value) => {
                if let Some(name) = event.event
                    && let Some(obj) = value.as_object_mut()
                {
                    obj.entry("_sse_event").or_insert(serde_json::json!(name));
                }
                traffic.write_json_event("040-upstream-event", &value);
            }
            Err(_) => {
                traffic.write_json_event(
                    "040-upstream-event",
                    &serde_json::json!({
                        "event": event.event,
                        "unparseable": true,
                        "data": event.data,
                    }),
                );
            }
        }
    }
}

fn headers_to_json(headers: &http::HeaderMap) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (key, value) in headers.iter() {
        out.insert(
            key.to_string(),
            serde_json::Value::String(value.to_str().unwrap_or("").to_string()),
        );
    }
    serde_json::Value::Object(out)
}

fn headers_to_json_from_pairs(headers: &[(String, String)]) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (key, value) in headers {
        out.insert(key.clone(), serde_json::Value::String(value.clone()));
    }
    serde_json::Value::Object(out)
}

fn summarize_json_request_size(body: &serde_json::Value, body_json: &str) -> serde_json::Value {
    serde_json::json!({
        "bytes": body_json.len(),
        "inputCount": body
            .get("input")
            .and_then(|v| v.as_array())
            .map(|items| items.len()),
        "toolCount": body
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|items| items.len()),
    })
}

fn auth_refresh_error(err: anyhow::Error) -> CodexError {
    CodexError {
        status: 401,
        message: "Unauthorized".to_string(),
        detail: Some(err.to_string()),
        retry_after: None,
        origin: CodexErrorOrigin::Auth,
    }
}

pub(super) fn codex_total_timeout_error(
    transport: crate::config::CodexTransport,
    timeout_ms: u64,
) -> CodexError {
    CodexError {
        status: 504,
        message: format!("Codex request exceeded the total wall-clock budget of {timeout_ms}ms"),
        detail: Some(CODEX_TOTAL_TIMEOUT_DETAIL.to_string()),
        retry_after: None,
        origin: buffered_origin(transport),
    }
}

fn codex_status_error(
    response: CodexResponse,
    transport: crate::config::CodexTransport,
) -> CodexError {
    let retry_after = response
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("retry-after"))
        .map(|(_, value)| value.clone());
    let message = codex_status_error_message(&response.body).unwrap_or_else(|| {
        format!(
            "Upstream Codex request failed with status {}",
            response.status
        )
    });
    CodexError {
        status: response.status,
        message: message.clone(),
        detail: Some(message),
        retry_after,
        origin: buffered_origin(transport),
    }
}

fn codex_status_error_message(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .or_else(|| value.get("detail"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            parse_sse_events(body).into_iter().find_map(|event| {
                let payload = serde_json::from_str::<serde_json::Value>(&event.data).ok()?;
                super::events::classify_event_failure(&payload).map(|failure| failure.message)
            })
        })
}

fn buffered_origin(transport: crate::config::CodexTransport) -> CodexErrorOrigin {
    match transport {
        crate::config::CodexTransport::Http => CodexErrorOrigin::BufferedHttp,
        crate::config::CodexTransport::WebSocket | crate::config::CodexTransport::Auto => {
            CodexErrorOrigin::BufferedWebSocket
        }
    }
}

fn serialize_request(body: &ResponsesRequest) -> Result<String, CodexError> {
    serde_json::to_string(body).map_err(|error| CodexError {
        status: 500,
        message: "Failed to serialize request".to_string(),
        detail: Some(error.to_string()),
        retry_after: None,
        origin: CodexErrorOrigin::Http,
    })
}

fn should_retry_codex_status(status: u16) -> bool {
    should_retry_status(status) || status == 529
}

fn codex_error_origin_name(origin: CodexErrorOrigin) -> &'static str {
    match origin {
        CodexErrorOrigin::Http => "http",
        CodexErrorOrigin::WebSocket => "websocket",
        CodexErrorOrigin::WebSocketHandshake => "websocket_handshake",
        CodexErrorOrigin::Auth => "auth",
        CodexErrorOrigin::BufferedHttp => "buffered_http",
        CodexErrorOrigin::BufferedWebSocket => "buffered_websocket",
    }
}

fn log_buffered_retry(
    ctx: &RequestContext,
    transport: crate::config::CodexTransport,
    failed_attempt: u32,
    delay_ms: u64,
    status: u16,
    origin: &str,
    reason: &str,
) {
    let mut fields = serde_json::Map::new();
    fields.insert("reqId".into(), serde_json::json!(ctx.req_id));
    fields.insert("transport".into(), serde_json::json!(transport.as_str()));
    fields.insert("failedAttempt".into(), serde_json::json!(failed_attempt));
    fields.insert("nextAttempt".into(), serde_json::json!(failed_attempt + 1));
    fields.insert(
        "maxAttempts".into(),
        serde_json::json!(MAX_BUFFERED_TRANSPORT_ATTEMPTS),
    );
    fields.insert("delayMs".into(), serde_json::json!(delay_ms));
    fields.insert("status".into(), serde_json::json!(status));
    fields.insert("origin".into(), serde_json::json!(origin));
    fields.insert("reason".into(), serde_json::json!(reason));
    create_logger("codex").warn("buffered_transport_retry", Some(fields));
}

fn log_buffered_retry_exhausted(
    ctx: &RequestContext,
    transport: crate::config::CodexTransport,
    status: u16,
    origin: &str,
    reason: &str,
) {
    let mut fields = serde_json::Map::new();
    fields.insert("reqId".into(), serde_json::json!(ctx.req_id));
    fields.insert("transport".into(), serde_json::json!(transport.as_str()));
    fields.insert(
        "attempts".into(),
        serde_json::json!(MAX_BUFFERED_TRANSPORT_ATTEMPTS),
    );
    fields.insert("status".into(), serde_json::json!(status));
    fields.insert("origin".into(), serde_json::json!(origin));
    fields.insert("reason".into(), serde_json::json!(reason));
    create_logger("codex").warn("buffered_transport_retry_exhausted", Some(fields));
}

pub(super) fn log_live_transport_retry(
    ctx: &RequestContext,
    transport: crate::config::CodexTransport,
    failed_attempt: u32,
    max_attempts: u32,
    delay_ms: u64,
    err: &CodexError,
) {
    create_logger("codex").warn(
        "live_transport_retry",
        Some(serde_json::Map::from_iter([
            ("reqId".into(), serde_json::json!(ctx.req_id)),
            ("transport".into(), serde_json::json!(transport.as_str())),
            ("failedAttempt".into(), serde_json::json!(failed_attempt)),
            ("nextAttempt".into(), serde_json::json!(failed_attempt + 1)),
            ("maxAttempts".into(), serde_json::json!(max_attempts)),
            ("delayMs".into(), serde_json::json!(delay_ms)),
            ("status".into(), serde_json::json!(err.status)),
            (
                "origin".into(),
                serde_json::json!(codex_error_origin_name(err.origin)),
            ),
            ("reason".into(), serde_json::json!(err.message)),
        ])),
    );
}

pub(super) fn log_websocket_circuit_fallback(ctx: &RequestContext) {
    create_logger("codex").warn(
        "websocket_circuit_fallback",
        Some(serde_json::Map::from_iter([
            ("reqId".into(), serde_json::json!(ctx.req_id)),
            (
                "cooldownMs".into(),
                serde_json::json!(super::websocket::WEBSOCKET_CIRCUIT_COOLDOWN_MS),
            ),
            ("fallbackTransport".into(), serde_json::json!("http")),
        ])),
    );
}

pub(super) fn record_auto_websocket_failure(ctx: &RequestContext, err: &CodexError) -> bool {
    let Some(key) = ctx.session_id.as_deref() else {
        return false;
    };
    if !should_fallback_to_http(err) {
        // A non-transport response proves the WebSocket path is reachable and
        // breaks a run of consecutive transport failures.
        super::websocket::record_codex_websocket_success(key);
        return false;
    }

    let opened = super::websocket::record_codex_websocket_failure(key);
    if opened {
        create_logger("codex").warn(
            "websocket_circuit_opened",
            Some(serde_json::Map::from_iter([
                ("reqId".into(), serde_json::json!(ctx.req_id)),
                (
                    "failures".into(),
                    serde_json::json!(super::websocket::WEBSOCKET_CIRCUIT_FAILURE_THRESHOLD),
                ),
                (
                    "cooldownMs".into(),
                    serde_json::json!(super::websocket::WEBSOCKET_CIRCUIT_COOLDOWN_MS),
                ),
            ])),
        );
    }
    opened
}

pub(super) fn is_retryable_transport_error(err: &CodexError) -> bool {
    if err.origin == CodexErrorOrigin::WebSocketHandshake {
        return err.status == 0 || should_retry_codex_status(err.status);
    }
    if super::websocket::is_retryable_transport_detail(err.detail.as_deref()) {
        return true;
    }
    if err.detail.as_deref() == Some("websocket_pre_request") {
        return err.status == 0 || should_retry_codex_status(err.status);
    }
    if err.status != 0 {
        return false;
    }

    let message = err.message.to_ascii_lowercase();
    message.contains("timed out waiting")
        || message.contains("transport error")
        || message.contains("connection reset")
        || message.contains("connection closed")
        || message.contains("timed out")
        || message.contains("econnreset")
        || message.contains("etimedout")
}

fn is_retryable_reqwest_error(err: &reqwest::Error) -> bool {
    if err.is_timeout() || err.is_connect() {
        return true;
    }
    let msg = err.to_string().to_lowercase();
    msg.contains("connection reset")
        || msg.contains("connection closed")
        || msg.contains("econnreset")
        || msg.contains("etimedout")
        || msg.contains("epipe")
}

fn should_refresh_after_unauthorized(
    result: &Result<CodexResponse, CodexError>,
    auth_refresh_attempted: bool,
) -> bool {
    if auth_refresh_attempted {
        return false;
    }
    match result {
        Ok(response) => response.status == 401,
        Err(err) => err.status == 401,
    }
}

/// Immediate Auto-mode fallback from WebSocket to buffered HTTP.
///
/// Only pre-output, retryable WebSocket transport failures qualify. Business
/// 4xx, continuation misses, and service 429/5xx event errors stay on the
/// original error path and must not be treated as transport fallback.
pub(super) fn should_fallback_to_http(err: &CodexError) -> bool {
    matches!(
        err.origin,
        CodexErrorOrigin::WebSocket | CodexErrorOrigin::WebSocketHandshake
    ) && err.detail.as_deref() != Some("previous_response_not_found")
        && is_retryable_transport_error(err)
}

pub(super) fn log_auto_http_fallback(ctx: &RequestContext, err: &CodexError) {
    let mut fields = serde_json::Map::from_iter([
        ("reqId".into(), serde_json::json!(ctx.req_id)),
        ("action".into(), serde_json::json!("fallback_http")),
        (
            "origin".into(),
            serde_json::json!(codex_error_origin_name(err.origin)),
        ),
        ("status".into(), serde_json::json!(err.status)),
        ("reason".into(), serde_json::json!(err.message)),
    ]);
    if let Some(detail) = err.detail.as_deref() {
        fields.insert("detail".into(), serde_json::json!(detail));
    }
    create_logger("codex").warn("auto_transport_fallback", Some(fields));
}

fn should_retry_without_continuation(
    err: &CodexError,
    continuation: Option<&super::continuation::ContinuationCandidate>,
) -> bool {
    if continuation
        .and_then(|c| c.previous_response_id.as_deref())
        .is_none()
    {
        return false;
    }

    is_continuation_retry_error(err)
}

fn full_context_continuation(
    continuation: Option<&super::continuation::ContinuationCandidate>,
) -> Option<super::continuation::ContinuationCandidate> {
    continuation.map(|candidate| super::continuation::ContinuationCandidate {
        turn_id: candidate.turn_id,
        previous_response_id: None,
        input_delta: None,
        input_delta_count: candidate.input_delta_count,
        disabled_reason: Some("full_context_retry".to_string()),
    })
}

fn event_closes_live_retry_window(payload: &serde_json::Value) -> bool {
    !matches!(
        payload.get("type").and_then(|value| value.as_str()),
        Some("codex.rate_limits" | "keepalive")
    )
}

fn is_continuation_retry_error(err: &CodexError) -> bool {
    // store:false continuation state is tied to its WebSocket. Once that
    // connection is lost or detached, the retry must send full context.
    err.detail.as_deref() == Some("previous_response_not_found")
        || super::websocket::is_retryable_transport_detail(err.detail.as_deref())
        || (err.origin == CodexErrorOrigin::WebSocketHandshake
            && (err.status == 0 || should_retry_codex_status(err.status)))
}

fn websocket_pool_key<'a>(
    ctx: &'a RequestContext,
    continuation: Option<&super::continuation::ContinuationCandidate>,
) -> Option<&'a str> {
    let session_id = ctx.session_id.as_deref()?;
    let continuation = continuation?;
    if continuation.disabled_reason.as_deref() == Some("disabled") {
        return None;
    }
    Some(session_id)
}

fn should_reset_websocket_pool(
    continuation: Option<&super::continuation::ContinuationCandidate>,
) -> bool {
    let Some(reason) = continuation.and_then(|c| c.disabled_reason.as_deref()) else {
        return false;
    };
    reason != "disabled"
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn http_test_auth() -> StoredAuth {
        StoredAuth {
            access: "test".into(),
            refresh: String::new(),
            account_id: Some("acct".into()),
            expires: u64::MAX,
        }
    }

    fn http_test_context() -> RequestContext {
        RequestContext {
            req_id: "http-body-test".into(),
            session_id: None,
            session_seq: None,
            provider: "codex".into(),
            traffic: None,
            monitor: None,
        }
    }

    fn http_test_client(base_url: String, body_idle_timeout_ms: u64) -> CodexHttpClient {
        CodexHttpClient::new_for_test(
            reqwest::Client::new(),
            base_url,
            100,
            body_idle_timeout_ms,
            10_000,
            0,
        )
    }

    fn buffered_test_request() -> ResponsesRequest {
        ResponsesRequest {
            model: "gpt-5.6-sol".into(),
            instructions: None,
            input: vec![],
            tools: None,
            tool_choice: None,
            store: false,
            stream: true,
            parallel_tool_calls: true,
            include: None,
            client_metadata: None,
            service_tier: None,
            prompt_cache_key: None,
            text: super::super::translate::request::ResponsesText {
                verbosity: None,
                format: None,
            },
            reasoning: None,
        }
    }

    fn authenticated_http_test_client(base_url: String) -> CodexHttpClient {
        let client = http_test_client(base_url, 100);
        client.auth_manager().set_test_auth(http_test_auth());
        client
    }

    #[tokio::test]
    async fn buffered_http_retries_retryable_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 16 * 1024];
                assert!(stream.read(&mut request).await.unwrap() > 0);
                let (status, body): (&str, &[u8]) = if attempt == 0 {
                    ("503 Service Unavailable", b"retry")
                } else {
                    ("200 OK", b"data: keep\n\n")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });

        let response = authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &buffered_test_request(),
                &http_test_context(),
                None,
                crate::config::CodexTransport::Http,
            )
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"data: keep\n\n");
    }

    #[tokio::test]
    async fn auto_falls_back_to_http_after_statusful_websocket_handshake_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut websocket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 16 * 1024];
            let read = websocket.read(&mut request).await.unwrap();
            assert!(read > 0);
            assert!(String::from_utf8_lossy(&request[..read]).contains("Upgrade: websocket"));
            websocket
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\nretry-after: 0\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            drop(websocket);

            let (mut http, _) = listener.accept().await.unwrap();
            let read = http.read(&mut request).await.unwrap();
            assert!(read > 0);
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("POST "));
            let body = b"data: keep\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            http.write_all(response.as_bytes()).await.unwrap();
            http.write_all(body).await.unwrap();
        });

        let response = authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &buffered_test_request(),
                &http_test_context(),
                None,
                crate::config::CodexTransport::Auto,
            )
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"data: keep\n\n");
    }

    #[tokio::test]
    async fn auto_falls_back_to_http_after_websocket_closes_before_terminal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = futures_util::StreamExt::next(&mut websocket).await;
            drop(websocket);

            let (mut http, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 16 * 1024];
            let read = http.read(&mut request).await.unwrap();
            assert!(read > 0);
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("POST "));
            let body = b"data: keep\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            http.write_all(response.as_bytes()).await.unwrap();
            http.write_all(body).await.unwrap();
        });

        let response = authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &buffered_test_request(),
                &http_test_context(),
                None,
                crate::config::CodexTransport::Auto,
            )
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"data: keep\n\n");
    }

    #[tokio::test]
    async fn auto_http_fallback_stays_on_http_and_uses_full_context() {
        use super::super::translate::request::{ResponsesContentPart, ResponsesInputItem};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let first = futures_util::StreamExt::next(&mut websocket)
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap();
            assert!(first.contains("\"previous_response_id\":\"resp_prev\""));
            assert!(first.contains("delta only"));
            drop(websocket);

            let (mut http, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 16 * 1024];
            assert!(http.read(&mut request).await.unwrap() > 0);
            http.write_all(
                b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 5\r\nretry-after: 0\r\nconnection: close\r\n\r\nretry",
            )
            .await
            .unwrap();
            drop(http);

            let (mut retried_http, _) = listener.accept().await.unwrap();
            let mut retried_request = [0_u8; 16 * 1024];
            let read = retried_http.read(&mut retried_request).await.unwrap();
            let retried_request = String::from_utf8_lossy(&retried_request[..read]);
            assert!(retried_request.starts_with("POST "));
            assert!(!retried_request.contains("previous_response_id"));
            assert!(retried_request.contains("full context"));
            assert!(!retried_request.contains("delta only"));
            let body = b"data: keep\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            retried_http.write_all(response.as_bytes()).await.unwrap();
            retried_http.write_all(body).await.unwrap();
        });

        let full_input = ResponsesInputItem::Message {
            role: "user".into(),
            content: vec![ResponsesContentPart::InputText {
                text: "full context".into(),
            }],
        };
        let delta_input = ResponsesInputItem::Message {
            role: "user".into(),
            content: vec![ResponsesContentPart::InputText {
                text: "delta only".into(),
            }],
        };
        let mut request = buffered_test_request();
        request.input = vec![full_input];
        let continuation = super::super::continuation::ContinuationCandidate {
            turn_id: Some(7),
            previous_response_id: Some("resp_prev".into()),
            input_delta: Some(vec![delta_input]),
            input_delta_count: 1,
            disabled_reason: None,
        };

        let response = authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &request,
                &http_test_context(),
                Some(&continuation),
                crate::config::CodexTransport::Auto,
            )
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"data: keep\n\n");
    }

    #[tokio::test]
    async fn auto_fallback_counts_websocket_and_http_against_one_attempt_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = futures_util::StreamExt::next(&mut websocket).await;
            drop(websocket);

            for _ in 0..3 {
                let (mut http, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 16 * 1024];
                let read = http.read(&mut request).await.unwrap();
                assert!(read > 0);
                assert!(String::from_utf8_lossy(&request[..read]).starts_with("POST "));
                http.write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 5\r\nretry-after: 0\r\nconnection: close\r\n\r\nretry",
                )
                .await
                .unwrap();
            }
        });

        let result = authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &buffered_test_request(),
                &http_test_context(),
                None,
                crate::config::CodexTransport::Auto,
            )
            .await;
        server.await.unwrap();
        let error = match result {
            Ok(_) => panic!("the shared physical-attempt budget should be exhausted"),
            Err(error) => error,
        };

        assert_eq!(error.status, 503);
        assert_eq!(error.origin, CodexErrorOrigin::BufferedHttp);
    }

    #[tokio::test]
    async fn auto_routes_directly_to_http_while_websocket_circuit_is_open() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 16 * 1024];
            let read = stream.read(&mut request).await.unwrap();
            assert!(read > 0);
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("POST "));
            let body = b"data: keep\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let mut ctx = http_test_context();
        let circuit_key = "auto-circuit-http-test";
        ctx.session_id = Some(circuit_key.to_string());
        for _ in 0..super::super::websocket::WEBSOCKET_CIRCUIT_FAILURE_THRESHOLD {
            super::super::websocket::record_codex_websocket_failure(circuit_key);
        }

        let response = authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &buffered_test_request(),
                &ctx,
                None,
                crate::config::CodexTransport::Auto,
            )
            .await
            .unwrap();
        server.await.unwrap();
        super::super::websocket::record_codex_websocket_success(circuit_key);

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"data: keep\n\n");
    }

    #[tokio::test]
    async fn over_budget_retry_after_stops_without_replay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 16 * 1024];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 4\r\nretry-after: 120\r\nconnection: close\r\n\r\nstop",
                )
                .await
                .unwrap();
        });

        let error = match authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &buffered_test_request(),
                &http_test_context(),
                None,
                crate::config::CodexTransport::Http,
            )
            .await
        {
            Ok(_) => panic!("over-budget Retry-After should propagate"),
            Err(error) => error,
        };
        server.await.unwrap();
        assert_eq!(error.status, 503);
        assert_eq!(error.retry_after.as_deref(), Some("120"));
    }

    #[tokio::test]
    async fn buffered_http_rejects_non_retryable_error_status_before_sse_parsing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 16 * 1024];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            let body = br#"{"error":{"message":"Model not found gpt-test"}}"#;
            let response = format!(
                "HTTP/1.1 404 Not Found\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        let result = authenticated_http_test_client(format!("http://{addr}/responses"))
            .post_codex_with_transport(
                &buffered_test_request(),
                &http_test_context(),
                None,
                crate::config::CodexTransport::Http,
            )
            .await;
        server.await.unwrap();
        let error = match result {
            Ok(_) => panic!("non-success HTTP status must not reach the SSE reducer"),
            Err(error) => error,
        };

        assert_eq!(error.status, 404);
        assert_eq!(error.detail.as_deref(), Some("Model not found gpt-test"));
        assert_eq!(error.origin, CodexErrorOrigin::BufferedHttp);
    }

    #[test]
    fn status_error_preserves_buffered_websocket_event_message() {
        let error = codex_status_error(
            CodexResponse {
                body: b"data: {\"type\":\"error\",\"error\":{\"status\":400,\"message\":\"bad request\"}}\n\n"
                    .to_vec(),
                status: 400,
                headers: Vec::new(),
            },
            crate::config::CodexTransport::WebSocket,
        );

        assert_eq!(error.status, 400);
        assert_eq!(error.detail.as_deref(), Some("bad request"));
        assert_eq!(error.origin, CodexErrorOrigin::BufferedWebSocket);
    }

    #[test]
    fn total_deadline_clamps_extreme_timeout_without_overflowing() {
        let deadline = CodexRequestDeadline::from_started_at(Instant::now(), u64::MAX);

        assert_eq!(deadline.timeout_ms(), MAX_CODEX_TOTAL_TIMEOUT_MS);
        assert!(deadline.at() > tokio::time::Instant::now());
    }

    #[tokio::test]
    async fn total_timeout_preempts_stalled_http_headers_with_504() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            futures_util::future::pending::<()>().await;
        });
        let client = CodexHttpClient::new_for_test(
            reqwest::Client::new(),
            format!("http://{addr}/responses"),
            5_000,
            5_000,
            100,
            0,
        );
        client.auth_manager().set_test_auth(http_test_auth());

        let started_at = Instant::now();
        let result = client
            .post_codex_with_transport(
                &buffered_test_request(),
                &http_test_context(),
                None,
                crate::config::CodexTransport::Http,
            )
            .await;
        server.abort();
        let error = match result {
            Ok(_) => panic!("the total wall-clock budget must preempt the header timeout"),
            Err(error) => error,
        };

        assert_eq!(error.status, 504);
        assert_eq!(error.detail.as_deref(), Some(CODEX_TOTAL_TIMEOUT_DETAIL));
        assert!(started_at.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn pooled_websocket_total_timeout_invalidates_before_aborting_turn() {
        use super::super::translate::request::{ResponsesContentPart, ResponsesInputItem};
        use tokio_tungstenite::tungstenite::Message;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (second_request_tx, second_request_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut request_count = 0_u8;
            let mut second_request_tx = Some(second_request_tx);
            while let Some(frame) = futures_util::StreamExt::next(&mut websocket).await {
                match frame.unwrap() {
                    Message::Ping(payload) => {
                        futures_util::SinkExt::send(&mut websocket, Message::Pong(payload))
                            .await
                            .unwrap();
                    }
                    Message::Text(_) => {
                        request_count += 1;
                        if request_count == 1 {
                            futures_util::SinkExt::send(
                                &mut websocket,
                                Message::Text(
                                    r#"{"type":"response.completed","response":{"id":"resp_1","usage":{}}}"#
                                        .into(),
                                ),
                            )
                            .await
                            .unwrap();
                        } else {
                            let _ = second_request_tx.take().unwrap().send(());
                            futures_util::future::pending::<()>().await;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });

        let session_id = "pooled-websocket-deadline-cleanup";
        let input = |text: &str| ResponsesInputItem::Message {
            role: "user".into(),
            content: vec![ResponsesContentPart::InputText { text: text.into() }],
        };
        let mut first_request = buffered_test_request();
        first_request.input.push(input("first"));
        let first_continuation = super::super::continuation::continuation_candidate(
            Some(session_id),
            &first_request,
            true,
        );
        let mut ctx = http_test_context();
        ctx.session_id = Some(session_id.to_string());
        let client = CodexHttpClient::new_for_test(
            reqwest::Client::new(),
            format!("http://{addr}/responses"),
            2_000,
            2_000,
            2_000,
            0,
        );
        client.auth_manager().set_test_auth(http_test_auth());

        let first_response = client
            .post_codex_with_transport_before(
                &first_request,
                &ctx,
                Some(&first_continuation),
                crate::config::CodexTransport::WebSocket,
                CodexRequestDeadline::from_timeout_ms(2_000),
            )
            .await
            .unwrap();
        assert_eq!(first_response.status, 200);
        assert!(super::super::websocket::codex_websocket_pool_contains_for_tests(session_id));

        super::super::continuation::record_continuation(
            Some(session_id),
            first_continuation.turn_id,
            &first_request,
            Some("resp_1"),
            &[],
        );
        let mut second_request = first_request.clone();
        second_request.input.push(input("second"));
        let second_continuation = super::super::continuation::continuation_candidate(
            Some(session_id),
            &second_request,
            true,
        );
        assert_eq!(
            second_continuation.previous_response_id.as_deref(),
            Some("resp_1")
        );
        assert!(second_continuation.disabled_reason.is_none());

        let result = client
            .post_codex_with_transport_before(
                &second_request,
                &ctx,
                Some(&second_continuation),
                crate::config::CodexTransport::WebSocket,
                CodexRequestDeadline::from_timeout_ms(250),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("the stalled pooled request should hit its total deadline"),
            Err(error) => error,
        };
        assert_eq!(error.detail.as_deref(), Some(CODEX_TOTAL_TIMEOUT_DETAIL));
        tokio::time::timeout(Duration::from_millis(250), second_request_rx)
            .await
            .expect("the timed-out request should have reused the pooled connection")
            .unwrap();
        assert!(
            !super::super::websocket::codex_websocket_pool_contains_for_tests(session_id),
            "a timed-out pooled socket must not remain reusable"
        );
        assert!(!super::super::continuation::is_current_turn(
            Some(session_id),
            second_continuation.turn_id
        ));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn dropping_buffered_request_closes_http_and_aborts_continuation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            let _ = request_seen_tx.send(());
            let mut byte = [0_u8; 1];
            stream.read(&mut byte).await
        });

        let session_id = "buffered-cancel-cleans-continuation";
        let request = buffered_test_request();
        let continuation =
            super::super::continuation::continuation_candidate(Some(session_id), &request, true);
        let mut ctx = http_test_context();
        ctx.session_id = Some(session_id.to_string());
        let client = authenticated_http_test_client(format!("http://{addr}/responses"));
        let task = tokio::spawn(async move {
            client
                .post_codex_with_transport(
                    &request,
                    &ctx,
                    Some(&continuation),
                    crate::config::CodexTransport::Http,
                )
                .await
        });

        tokio::time::timeout(Duration::from_millis(500), request_seen_rx)
            .await
            .expect("request should reach the upstream")
            .unwrap();
        task.abort();
        let _ = task.await;
        let closed = tokio::time::timeout(Duration::from_millis(500), server)
            .await
            .expect("cancelling the request should close the upstream socket")
            .unwrap();
        assert!(matches!(closed, Ok(0) | Err(_)));

        let next = super::super::continuation::continuation_candidate(
            Some(session_id),
            &buffered_test_request(),
            true,
        );
        assert_eq!(next.disabled_reason.as_deref(), Some("missing_state"));
        super::super::continuation::abort_continuation(Some(session_id), next.turn_id);
    }

    #[tokio::test]
    async fn active_http_body_can_exceed_header_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
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
                tokio::time::sleep(Duration::from_millis(45)).await;
            }
            stream.write_all(b"0\r\n\r\n").await.unwrap();
        });

        let response = http_test_client(format!("http://{addr}/responses"), 80)
            .attempt_post_http(&http_test_auth(), "{}", &http_test_context(), false)
            .await
            .expect("active body should not hit a whole-request timeout");
        server.await.unwrap();

        assert_eq!(response.body, b"abc");
    }

    #[tokio::test]
    async fn stalled_http_body_hits_idle_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let result = http_test_client(format!("http://{addr}/responses"), 30)
            .attempt_post_http(&http_test_auth(), "{}", &http_test_context(), false)
            .await;
        server.await.unwrap();
        let error = result.err().expect("stalled body should time out");

        assert!(error.message.contains("next Codex response body chunk"));
        assert_eq!(error.detail.as_deref(), Some("http_response_body"));
    }

    #[tokio::test]
    async fn reset_http_body_returns_transport_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\npartial")
                .await
                .unwrap();
        });

        let result = http_test_client(format!("http://{addr}/responses"), 100)
            .attempt_post_http(&http_test_auth(), "{}", &http_test_context(), false)
            .await;
        server.await.unwrap();
        let error = result.err().expect("truncated body should fail");

        assert!(
            error
                .message
                .contains("Transport error reading Codex response body")
        );
        assert_eq!(error.detail.as_deref(), Some("http_response_body"));
    }

    #[test]
    fn codex_error_display() {
        let err = CodexError {
            status: 429,
            message: "Rate limited".to_string(),
            detail: Some("body".to_string()),
            retry_after: Some("5".to_string()),
            origin: CodexErrorOrigin::Http,
        };
        let display = format!("{err}");
        assert!(display.contains("429"));
        assert!(display.contains("Rate limited"));
    }

    #[test]
    fn websocket_pre_request_502_is_retryable() {
        let err = CodexError {
            status: 502,
            message: "WebSocket connect error".to_string(),
            detail: Some("websocket_pre_request".to_string()),
            retry_after: Some("3".to_string()),
            origin: CodexErrorOrigin::WebSocket,
        };

        assert!(is_retryable_transport_error(&err));
    }

    #[test]
    fn websocket_pre_request_statusless_error_is_retryable() {
        let err = CodexError {
            status: 0,
            message: "WebSocket connect timeout after 15000ms".to_string(),
            detail: Some("websocket_pre_request".to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };

        assert!(is_retryable_transport_error(&err));
    }

    #[test]
    fn websocket_pre_request_400_is_not_retryable() {
        let err = CodexError {
            status: 400,
            message: "WebSocket connect error".to_string(),
            detail: Some("websocket_pre_request".to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };

        assert!(!is_retryable_transport_error(&err));
    }

    #[test]
    fn statusless_transport_error_matching_is_case_insensitive() {
        let err = CodexError {
            status: 0,
            message: "WebSocket protocol error: Connection reset without closing handshake"
                .to_string(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };

        assert!(is_retryable_transport_error(&err));
    }

    #[test]
    fn structured_websocket_watchdog_errors_are_retryable() {
        for detail in [
            super::super::websocket::WEBSOCKET_RESPONSE_START_TIMEOUT_DETAIL,
            super::super::websocket::WEBSOCKET_IDLE_TIMEOUT_DETAIL,
            super::super::websocket::WEBSOCKET_HEARTBEAT_TIMEOUT_DETAIL,
            super::super::websocket::WEBSOCKET_POOL_HEALTHCHECK_DETAIL,
            super::super::websocket::WEBSOCKET_POOL_BUSY_DETAIL,
            super::super::websocket::WEBSOCKET_CONNECTION_ERROR_DETAIL,
            super::super::websocket::WEBSOCKET_MISSING_TERMINAL_DETAIL,
        ] {
            let err = CodexError {
                status: 0,
                message: "localized or implementation-specific message".to_string(),
                detail: Some(detail.to_string()),
                retry_after: None,
                origin: CodexErrorOrigin::WebSocket,
            };
            assert!(is_retryable_transport_error(&err), "detail={detail}");
        }
    }

    #[test]
    fn codex_headers_include_session_and_beta() {
        let auth = StoredAuth {
            access: "tok".into(),
            refresh: String::new(),
            account_id: Some("acct".into()),
            expires: u64::MAX,
        };
        let ctx = RequestContext {
            req_id: "r".into(),
            session_id: Some("s".into()),
            session_seq: None,
            provider: "codex".into(),
            traffic: None,
            monitor: None,
        };
        let headers = build_codex_headers(&auth, &ctx, false).unwrap();
        assert_eq!(
            headers.get("openai-beta").unwrap(),
            "responses=experimental"
        );
        assert_eq!(headers.get("session_id").unwrap(), "s");
    }

    #[test]
    fn codex_headers_include_responses_lite_when_requested() {
        let auth = StoredAuth {
            access: "tok".into(),
            refresh: String::new(),
            account_id: None,
            expires: u64::MAX,
        };
        let ctx = RequestContext {
            req_id: "r".into(),
            session_id: None,
            session_seq: None,
            provider: "codex".into(),
            traffic: None,
            monitor: None,
        };
        let headers = build_codex_headers(&auth, &ctx, true).unwrap();
        assert_eq!(
            headers
                .get("x-openai-internal-codex-responses-lite")
                .unwrap(),
            "true"
        );
        assert_eq!(headers.get("originator").unwrap(), "codex_cli_rs");
        assert_eq!(default_user_agent(true), "codex_cli_rs");
    }

    #[test]
    fn codex_headers_omit_session_when_missing() {
        let auth = StoredAuth {
            access: "tok".into(),
            refresh: String::new(),
            account_id: None,
            expires: u64::MAX,
        };
        let ctx = RequestContext {
            req_id: "r".into(),
            session_id: None,
            session_seq: None,
            provider: "codex".into(),
            traffic: None,
            monitor: None,
        };
        let headers = build_codex_headers(&auth, &ctx, false).unwrap();
        assert!(headers.get("session_id").is_none());
        assert!(headers.get("x-client-request-id").is_none());
    }

    #[test]
    fn codex_headers_return_error_for_invalid_session_header() {
        let auth = StoredAuth {
            access: "tok".into(),
            refresh: String::new(),
            account_id: None,
            expires: u64::MAX,
        };
        let ctx = RequestContext {
            req_id: "r".into(),
            session_id: Some("bad\nsession".into()),
            session_seq: None,
            provider: "codex".into(),
            traffic: None,
            monitor: None,
        };
        let err = build_codex_headers(&auth, &ctx, false).unwrap_err();
        assert_eq!(err.status, 500);
        assert!(err.message.contains("session_id"));
    }

    #[test]
    fn build_websocket_request_removes_stream() {
        let input = vec![
            super::super::translate::request::ResponsesInputItem::Message {
                role: "user".to_string(),
                content: vec![
                    super::super::translate::request::ResponsesContentPart::InputText {
                        text: "hello".to_string(),
                    },
                ],
            },
        ];
        let req = ResponsesRequest {
            model: "gpt-5.5".to_string(),
            instructions: None,
            input,
            tools: None,
            tool_choice: None,
            store: false,
            stream: true,
            parallel_tool_calls: true,
            include: None,
            client_metadata: None,
            service_tier: None,
            prompt_cache_key: None,
            text: super::super::translate::request::ResponsesText {
                verbosity: Some("low".to_string()),
                format: None,
            },
            reasoning: None,
        };
        let payload = build_websocket_request(&req, None);
        assert_eq!(
            payload.get("type").and_then(|v| v.as_str()),
            Some("response.create")
        );
        assert!(payload.get("stream").is_none());
        assert!(payload.get("previous_response_id").is_none());
    }

    #[test]
    fn websocket_pool_key_tracks_continuation_opt_in() {
        let ctx = RequestContext {
            req_id: "r".into(),
            session_id: Some("session".into()),
            session_seq: None,
            provider: "codex".into(),
            traffic: None,
            monitor: None,
        };
        let disabled = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: None,
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: Some("disabled".into()),
        };
        let first_enabled = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: None,
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: Some("missing_state".into()),
        };
        let append = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: Some("resp_1".into()),
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: None,
        };

        assert_eq!(websocket_pool_key(&ctx, Some(&disabled)), None);
        assert_eq!(
            websocket_pool_key(&ctx, Some(&first_enabled)),
            Some("session")
        );
        assert_eq!(websocket_pool_key(&ctx, Some(&append)), Some("session"));
    }

    #[test]
    fn websocket_pool_reset_clears_initial_stale_state() {
        let missing_state = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: None,
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: Some("missing_state".into()),
        };
        let disabled = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: None,
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: Some("disabled".into()),
        };
        let prompt_changed = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: None,
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: Some("prompt_changed".into()),
        };

        assert!(should_reset_websocket_pool(Some(&missing_state)));
        assert!(!should_reset_websocket_pool(Some(&disabled)));
        assert!(should_reset_websocket_pool(Some(&prompt_changed)));
    }

    #[test]
    fn build_codex_headers_error_on_empty_access() {
        let auth = StoredAuth {
            access: "".into(),
            refresh: String::new(),
            account_id: None,
            expires: u64::MAX,
        };
        let ctx = RequestContext {
            req_id: "r".into(),
            session_id: None,
            session_seq: None,
            provider: "codex".into(),
            traffic: None,
            monitor: None,
        };
        let result = build_codex_headers(&auth, &ctx, false);
        assert!(
            result.is_ok(),
            "empty access should still produce valid Bearer header"
        );
    }

    #[test]
    fn codex_header_timeout_error_display() {
        let err = CodexHeaderTimeoutError { timeout_ms: 60000 };
        let display = format!("{err}");
        assert!(display.contains("60000"));
    }

    #[test]
    fn codex_transport_error_display() {
        let err = CodexTransportError {
            message: "connection reset".to_string(),
        };
        let display = format!("{err}");
        assert!(display.contains("connection reset"));
    }

    #[test]
    fn unauthorized_retry_refreshes_every_transport_once() {
        let http_unauthorized = Ok(CodexResponse {
            body: Vec::new(),
            status: 401,
            headers: Vec::new(),
        });
        let websocket_unauthorized = Err(CodexError {
            status: 401,
            message: "WebSocket connect error".to_string(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        });
        let forbidden = Err(CodexError {
            status: 403,
            message: "Forbidden".to_string(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        });
        let rejected_handshake = Err(CodexError {
            status: 401,
            message: "WebSocket connect error".to_string(),
            detail: Some("policy denied".to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocketHandshake,
        });
        let rejected_handshake_err = match &rejected_handshake {
            Err(error) => error,
            Ok(_) => panic!("expected rejected handshake"),
        };
        let stale_pool = CodexError {
            status: 0,
            message: "stale pooled connection".to_string(),
            detail: Some(super::super::websocket::WEBSOCKET_POOL_HEALTHCHECK_DETAIL.to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };

        assert!(should_refresh_after_unauthorized(&http_unauthorized, false));
        assert!(should_refresh_after_unauthorized(
            &websocket_unauthorized,
            false
        ));
        assert!(!should_refresh_after_unauthorized(&forbidden, false));
        assert!(should_refresh_after_unauthorized(
            &rejected_handshake,
            false
        ));
        assert!(!should_refresh_after_unauthorized(&http_unauthorized, true));
        // Permanent 4xx handshake rejections are not transport fallback candidates.
        assert!(!should_fallback_to_http(rejected_handshake_err));
        // Structured pre-output WebSocket transport failures fall back in auto mode.
        assert!(should_fallback_to_http(&stale_pool));
    }

    #[test]
    fn should_fallback_to_http_only_for_retryable_websocket_transport_errors() {
        let statusless_ws = CodexError {
            status: 0,
            message: "WebSocket protocol error: Connection reset without closing handshake"
                .to_string(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };
        let retryable_handshake = CodexError {
            status: 503,
            message: "Service unavailable".into(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocketHandshake,
        };
        let statusless_handshake = CodexError {
            status: 0,
            message: "WebSocket connect timeout after 15000ms".into(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocketHandshake,
        };
        let http_reset = CodexError {
            status: 0,
            message: "Connection reset".into(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::Http,
        };
        let business_400 = CodexError {
            status: 400,
            message: "Bad request".into(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocketHandshake,
        };
        let business_401 = CodexError {
            status: 401,
            message: "Unauthorized".into(),
            detail: Some("policy denied".into()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocketHandshake,
        };
        let service_429 = CodexError {
            status: 429,
            message: "Rate limited".into(),
            detail: Some("rate limit".into()),
            retry_after: Some("1".into()),
            origin: CodexErrorOrigin::WebSocket,
        };
        let service_503 = CodexError {
            status: 503,
            message: "Overloaded".into(),
            detail: Some("service unavailable".into()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };
        let missing_continuation = CodexError {
            status: 404,
            message: "Previous response not found".into(),
            detail: Some("previous_response_not_found".into()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };

        assert!(should_fallback_to_http(&statusless_ws));
        assert!(should_fallback_to_http(&retryable_handshake));
        assert!(should_fallback_to_http(&statusless_handshake));
        assert!(!should_fallback_to_http(&http_reset));
        assert!(!should_fallback_to_http(&business_400));
        assert!(!should_fallback_to_http(&business_401));
        assert!(!should_fallback_to_http(&service_429));
        assert!(!should_fallback_to_http(&service_503));
        assert!(!should_fallback_to_http(&missing_continuation));
    }

    #[test]
    fn auto_circuit_counts_only_retryable_websocket_transport_failures() {
        let response_timeout = CodexError {
            status: 0,
            message: "Response start timeout".into(),
            detail: Some(super::super::websocket::WEBSOCKET_RESPONSE_START_TIMEOUT_DETAIL.into()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };
        let bad_request = CodexError {
            status: 400,
            message: "Bad request".into(),
            detail: None,
            retry_after: None,
            origin: CodexErrorOrigin::WebSocketHandshake,
        };
        let mut ctx = http_test_context();
        let circuit_key = "auto-circuit-reset-test";
        ctx.session_id = Some(circuit_key.into());
        assert!(!record_auto_websocket_failure(&ctx, &response_timeout));
        assert!(!record_auto_websocket_failure(&ctx, &response_timeout));
        assert!(!record_auto_websocket_failure(&ctx, &bad_request));
        assert!(!record_auto_websocket_failure(&ctx, &response_timeout));
        super::super::websocket::record_codex_websocket_success(circuit_key);
    }

    #[test]
    fn informational_events_keep_live_continuation_retry_available() {
        assert!(!event_closes_live_retry_window(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": false}
        })));
        assert!(!event_closes_live_retry_window(&serde_json::json!({
            "type": "keepalive"
        })));
        assert!(event_closes_live_retry_window(&serde_json::json!({
            "type": "response.created"
        })));
    }

    #[test]
    fn continuation_retry_requires_previous_response_id() {
        let append = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: Some("resp_1".into()),
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: None,
        };
        let initial = super::super::continuation::ContinuationCandidate {
            turn_id: None,
            previous_response_id: None,
            input_delta: None,
            input_delta_count: 1,
            disabled_reason: Some("missing_state".into()),
        };
        let timeout = CodexError {
            status: 0,
            message: "WebSocket response start timeout after 60000ms".to_string(),
            detail: Some(
                super::super::websocket::WEBSOCKET_RESPONSE_START_TIMEOUT_DETAIL.to_string(),
            ),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };
        let missing = CodexError {
            status: 0,
            message: "Previous response not found".to_string(),
            detail: Some("previous_response_not_found".to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };
        let idle = CodexError {
            status: 0,
            message: "WebSocket idle timeout after 60000ms".to_string(),
            detail: Some(super::super::websocket::WEBSOCKET_IDLE_TIMEOUT_DETAIL.to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };
        let pool_healthcheck = CodexError {
            status: 0,
            message: "WebSocket pooled connection health check failed".to_string(),
            detail: Some(super::super::websocket::WEBSOCKET_POOL_HEALTHCHECK_DETAIL.to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };
        let pool_busy = CodexError {
            status: 0,
            message: "WebSocket pooled connection remained busy".to_string(),
            detail: Some(super::super::websocket::WEBSOCKET_POOL_BUSY_DETAIL.to_string()),
            retry_after: None,
            origin: CodexErrorOrigin::WebSocket,
        };

        assert!(should_retry_without_continuation(&timeout, Some(&append)));
        assert!(should_retry_without_continuation(&missing, Some(&append)));
        assert!(should_retry_without_continuation(&idle, Some(&append)));
        assert!(should_retry_without_continuation(
            &pool_healthcheck,
            Some(&append)
        ));
        assert!(should_retry_without_continuation(&pool_busy, Some(&append)));
        assert!(!should_retry_without_continuation(&timeout, Some(&initial)));
        assert!(!should_retry_without_continuation(&timeout, None));
    }
}
