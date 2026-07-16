pub mod auth;
pub mod client;
pub mod continuation;
pub mod count_tokens;
pub(crate) mod events;
pub mod request_summary;
pub mod translate;
pub mod websocket;

use async_trait::async_trait;
use axum::Json;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http::StatusCode;
use std::sync::Arc;
use std::time::Instant;

use crate::anthropic::error::json_error;
use crate::anthropic::schema::{CountTokensResponse, MessagesRequest};
use crate::config;
use crate::monitor::usage_from_anthropic_sse;
use crate::provider::{CliHandlers, Provider, RequestContext};
use crate::registry;
use crate::retry::{compute_backoff_delay, sleep};

use self::auth::browser_login::run_browser_login;
use self::auth::device::DeviceAuthClient;
use self::auth::manager::CodexAuthManager;
use self::auth::token_store::file_store;
use self::client::CodexHttpClient;
use self::continuation::{
    ContinuationCandidate, abort_continuation, continuation_candidate, record_continuation,
};
use self::count_tokens::count_translated_tokens;
use self::translate::accumulate::accumulate_response_with_traffic;
use self::translate::live_stream::LiveStreamTranslator;
use self::translate::model_allowlist::{
    assert_allowed_model, full_lane_web_search_model, resolve_model_request, uses_responses_lite,
};
use self::translate::reducer::finish_metadata_from_upstream;
use self::translate::request::{
    ServiceTier, TranslateOptions, has_hosted_web_search, translate_request,
};

const MAX_RETRYABLE_LIVE_STREAM_RETRIES: u32 = 10;
const MAX_LIVE_TRANSPORT_RETRIES: u32 = 2;
use self::translate::stream::translate_stream_bytes_with_traffic;

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

pub struct CodexProvider {
    client: Arc<CodexHttpClient>,
}

impl Default for CodexProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexProvider {
    pub fn new() -> Self {
        Self {
            client: Arc::new(CodexHttpClient::new()),
        }
    }
}

#[async_trait]
impl Provider for CodexProvider {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn supported_models(&self) -> Vec<String> {
        let mut models: Vec<String> = registry::CODEX_MODELS
            .iter()
            .map(|m| m.to_string())
            .collect();
        for m in registry::CODEX_MODELS {
            models.push(format!("{m}-fast"));
        }
        models.sort_unstable();
        models.dedup();
        models
    }

    fn cli(&self) -> &'static dyn CliHandlers {
        &CODEX_CLI
    }

    async fn handle_messages(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let provider_started_at = Instant::now();
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
        let want_stream = body.stream;
        let model = body.model.as_deref().unwrap_or("gpt-5.6-sol");

        let mut resolved = resolve_model_request(model);
        if let Err(e) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Model \"{model}\" resolves to unsupported model \"{}\"",
                    e.model
                ),
            );
        }
        let native_web_search = has_hosted_web_search(&body);
        let use_responses_lite = apply_model_lane_for_request(&mut resolved.model, &body);
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.model_resolved(&ctx.req_id, &resolved.model);
        }

        let translated = match translate_request(
            &body,
            TranslateOptions {
                session_id: ctx.session_id.clone(),
                service_tier: resolved.service_tier.clone(),
                model: resolved.model.clone(),
                use_responses_lite,
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    e.to_string(),
                );
            }
        };
        log_codex_request_configuration(&ctx, &translated, use_responses_lite, native_web_search);

        // Check continuation
        let previous_response_id_enabled = config::codex_previous_response_id();
        let continuation = continuation_candidate(
            ctx.session_id.as_deref(),
            &translated,
            previous_response_id_enabled,
        );
        let turn_id = continuation.turn_id;

        // Post to upstream with continuation
        let client = self.client.clone();
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.upstream_started(&ctx.req_id);
        }
        let transport = config::codex_transport();
        if want_stream && !matches!(transport, config::CodexTransport::Http) {
            let stream_request = translated.clone();
            return live_stream_response(
                client,
                message_id,
                model,
                ctx,
                stream_request,
                continuation,
                provider_started_at,
            )
            .await;
        }

        let upstream = match client
            .post_codex(&translated, &ctx, Some(&continuation))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                abort_continuation(ctx.session_id.as_deref(), turn_id);
                return map_codex_error_to_response(&e);
            }
        };

        if want_stream {
            buffered_stream_response(upstream, &message_id, model, &ctx, turn_id, &translated)
        } else {
            match accumulate_response_with_traffic(
                &upstream.body,
                &message_id,
                model,
                ctx.traffic.as_deref(),
            ) {
                Ok(json) => {
                    if let Some(monitor) = ctx.monitor.as_ref() {
                        monitor.usage_updated(
                            &ctx.req_id,
                            json.pointer("/usage/input_tokens").and_then(|v| v.as_u64()),
                            json.pointer("/usage/output_tokens")
                                .and_then(|v| v.as_u64()),
                        );
                    }
                    update_continuation_from_upstream(
                        ctx.session_id.as_deref(),
                        turn_id,
                        &translated,
                        &upstream.body,
                    );
                    (StatusCode::OK, Json(json)).into_response()
                }
                Err(e) => {
                    abort_continuation(ctx.session_id.as_deref(), turn_id);
                    map_codex_failure_to_response(&format!("Accumulation error: {e}"))
                }
            }
        }
    }

    async fn handle_count_tokens(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let model = body.model.as_deref().unwrap_or("gpt-5.6-sol");
        let mut resolved = resolve_model_request(model);
        if let Err(e) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Model \"{model}\" resolves to unsupported model \"{}\"",
                    e.model
                ),
            );
        }
        let use_responses_lite = apply_model_lane_for_request(&mut resolved.model, &body);
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.model_resolved(&ctx.req_id, &resolved.model);
        }

        let translated = match translate_request(
            &body,
            TranslateOptions {
                session_id: None,
                service_tier: resolved.service_tier.clone(),
                model: resolved.model.clone(),
                use_responses_lite,
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    e.to_string(),
                );
            }
        };

        let tokens = count_translated_tokens(&translated);
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.usage_updated(&ctx.req_id, Some(tokens), None);
        }
        (
            StatusCode::OK,
            Json(CountTokensResponse {
                input_tokens: tokens,
            }),
        )
            .into_response()
    }
}

/// Picks the upstream model and lane for a request. Hosted web_search always
/// uses the full Responses API and upgrades Luna for account compatibility.
/// Other GPT-5.6 requests use Lite by default but may opt into the full shape.
fn apply_model_lane_for_request(model: &mut String, body: &MessagesRequest) -> bool {
    apply_model_lane_for_request_with_lite(model, body, config::codex_responses_lite())
}

fn apply_model_lane_for_request_with_lite(
    model: &mut String,
    body: &MessagesRequest,
    responses_lite: bool,
) -> bool {
    if has_hosted_web_search(body) {
        *model = full_lane_web_search_model(model).to_string();
        return false;
    }
    responses_lite && uses_responses_lite(model)
}

fn count_sse_events(bytes: &[u8]) -> u64 {
    String::from_utf8_lossy(bytes).matches("event:").count() as u64
}

fn buffered_stream_response(
    upstream: client::CodexResponse,
    message_id: &str,
    model: &str,
    ctx: &RequestContext,
    turn_id: Option<u64>,
    request_body: &translate::request::ResponsesRequest,
) -> Response {
    let sse_bytes = match translate_stream_bytes_with_traffic(
        &upstream.body,
        message_id,
        model,
        ctx.traffic.as_deref(),
    ) {
        Ok(bytes) => bytes,
        Err(error) => {
            abort_continuation(ctx.session_id.as_deref(), turn_id);
            return map_codex_failure_to_response(&format!("Stream translation error: {error}"));
        }
    };
    if let Some(monitor) = ctx.monitor.as_ref() {
        let (input_tokens, output_tokens) = usage_from_anthropic_sse(&sse_bytes);
        monitor.stream_progress(
            &ctx.req_id,
            sse_bytes.len() as u64,
            count_sse_events(&sse_bytes),
            input_tokens,
            output_tokens,
        );
    }
    update_continuation_from_upstream(
        ctx.session_id.as_deref(),
        turn_id,
        request_body,
        &upstream.body,
    );

    let headers = [
        (http::header::CONTENT_TYPE, "text/event-stream"),
        (http::header::CACHE_CONTROL, "no-cache"),
        (http::header::CONNECTION, "keep-alive"),
    ];
    (headers, sse_bytes).into_response()
}

async fn buffered_http_stream_fallback(
    client: &Arc<CodexHttpClient>,
    message_id: &str,
    model: &str,
    ctx: &RequestContext,
    turn_id: Option<u64>,
    request_body: &translate::request::ResponsesRequest,
    continuation: Option<&ContinuationCandidate>,
) -> Response {
    let upstream = match client
        .post_codex_http(request_body, ctx, continuation)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            abort_continuation(ctx.session_id.as_deref(), turn_id);
            return map_codex_error_to_response(&error);
        }
    };
    buffered_stream_response(upstream, message_id, model, ctx, turn_id, request_body)
}

enum LiveStreamStart {
    Response(Response),
    Retry { error: client::CodexError },
}

async fn live_stream_response(
    client: Arc<CodexHttpClient>,
    message_id: String,
    model: &str,
    ctx: RequestContext,
    request_body: translate::request::ResponsesRequest,
    continuation: ContinuationCandidate,
    provider_started_at: Instant,
) -> Response {
    let model = model.to_string();
    let turn_id = continuation.turn_id;
    let mut attempt = 0_u32;
    let mut continuation = Some(continuation);
    let circuit_key = ctx.session_id.as_deref();
    let transport = config::codex_transport();

    loop {
        if transport == config::CodexTransport::Auto
            && circuit_key.is_some_and(websocket::codex_websocket_circuit_open)
        {
            client::log_websocket_circuit_fallback(&ctx);
            return buffered_http_stream_fallback(
                &client,
                &message_id,
                &model,
                &ctx,
                turn_id,
                &request_body,
                continuation.as_ref(),
            )
            .await;
        }

        let upstream_events = match client
            .stream_codex_websocket_events(&request_body, &ctx, continuation.as_ref())
            .await
        {
            Ok(events) => events,
            Err(err) if retryable_live_start_codex_error(&err) => {
                let dropped = drop_live_continuation_for_retry(&mut continuation);
                if dropped && is_missing_previous_response_error(&err) {
                    attempt += 1;
                    continue;
                }
                if transport == config::CodexTransport::Auto
                    && client::record_auto_websocket_failure(&ctx, &err)
                {
                    continue;
                }
                let max_retries = max_live_retries(&err);
                if attempt >= max_retries {
                    abort_continuation(ctx.session_id.as_deref(), turn_id);
                    return map_codex_error_to_response(&err);
                }
                let delay = compute_backoff_delay(attempt, err.retry_after.as_deref());
                if delay.exceeds_budget {
                    abort_continuation(ctx.session_id.as_deref(), turn_id);
                    return map_codex_error_to_response(&err);
                }
                client::log_live_transport_retry(
                    &ctx,
                    transport,
                    attempt + 1,
                    max_retries + 1,
                    delay.wait_ms,
                    &err,
                );
                attempt += 1;
                sleep(delay.wait_ms).await;
                continue;
            }
            Err(err) => {
                abort_continuation(ctx.session_id.as_deref(), turn_id);
                return map_codex_error_to_response(&err);
            }
        };

        match live_stream_response_once(
            upstream_events,
            message_id.clone(),
            &model,
            ctx.clone(),
            turn_id,
            request_body.clone(),
            provider_started_at,
        )
        .await
        {
            LiveStreamStart::Response(response) => {
                if transport == config::CodexTransport::Auto
                    && let Some(key) = circuit_key
                {
                    websocket::record_codex_websocket_success(key);
                }
                return response;
            }
            LiveStreamStart::Retry { error } => {
                if transport == config::CodexTransport::Auto
                    && client::should_fallback_to_http(&error)
                {
                    client::record_auto_websocket_failure(&ctx, &error);
                    return buffered_http_stream_fallback(
                        &client,
                        &message_id,
                        &model,
                        &ctx,
                        turn_id,
                        &request_body,
                        continuation.as_ref(),
                    )
                    .await;
                }
                if !retryable_live_start_codex_error(&error) {
                    abort_continuation(ctx.session_id.as_deref(), turn_id);
                    return map_codex_error_to_response(&error);
                }
                let dropped = drop_live_continuation_for_retry(&mut continuation);
                if dropped && is_missing_previous_response_error(&error) {
                    attempt += 1;
                    continue;
                }
                if transport == config::CodexTransport::Auto
                    && client::record_auto_websocket_failure(&ctx, &error)
                {
                    continue;
                }
                let max_retries = max_live_retries(&error);
                if attempt >= max_retries {
                    abort_continuation(ctx.session_id.as_deref(), turn_id);
                    return map_codex_error_to_response(&error);
                }
                let delay = compute_backoff_delay(attempt, error.retry_after.as_deref());
                if delay.exceeds_budget {
                    abort_continuation(ctx.session_id.as_deref(), turn_id);
                    return map_codex_error_to_response(&error);
                }
                client::log_live_transport_retry(
                    &ctx,
                    transport,
                    attempt + 1,
                    max_retries + 1,
                    delay.wait_ms,
                    &error,
                );
                attempt += 1;
                sleep(delay.wait_ms).await;
            }
        }
    }
}

async fn live_stream_response_once(
    mut upstream_events: websocket::CodexWebSocketEventReceiver,
    message_id: String,
    model: &str,
    ctx: RequestContext,
    turn_id: Option<u64>,
    request_body: translate::request::ResponsesRequest,
    provider_started_at: Instant,
) -> LiveStreamStart {
    let mut translator = LiveStreamTranslator::new(message_id, model.to_string());
    let mut upstream_sse_body = Vec::new();
    let mut generation_started = false;

    while let Some(item) = upstream_events.recv().await {
        let payload = match item {
            Ok(payload) => payload,
            Err(err) => {
                if retryable_live_start_codex_error(&err)
                    || err.origin == client::CodexErrorOrigin::WebSocketHandshake
                {
                    return LiveStreamStart::Retry { error: err };
                }
                abort_continuation(ctx.session_id.as_deref(), turn_id);
                return LiveStreamStart::Response(map_codex_error_to_response(&err));
            }
        };
        log_native_web_search_phase(&ctx, &payload, provider_started_at);
        if !generation_started && codex_generation_event(&payload) {
            log_codex_first_event(&ctx, &payload, provider_started_at);
            if let Some(monitor) = ctx.monitor.as_ref() {
                monitor.generation_started(&ctx.req_id);
            }
            generation_started = true;
        }
        append_upstream_sse_payload(&mut upstream_sse_body, &payload);
        let (chunk, terminal) = match translate_live_stream_payload(&mut translator, &payload, &ctx)
        {
            Ok(result) => result,
            Err(message) => {
                if retryable_live_start_payload(&payload, &message) {
                    let lower_message = message.to_ascii_lowercase();
                    let status = websocket::event_error_status(&payload).unwrap_or_else(|| {
                        let error = payload.get("error").or_else(|| {
                            payload.get("response").and_then(|value| value.get("error"))
                        });
                        let overloaded = error.is_some_and(|error| {
                            error.get("code").and_then(|value| value.as_str())
                                == Some("overloaded_error")
                                || error.get("type").and_then(|value| value.as_str())
                                    == Some("overloaded_error")
                        });
                        if payload.get("type").and_then(|value| value.as_str())
                            == Some("codex.rate_limits")
                            || lower_message.contains("rate limit")
                        {
                            429
                        } else if overloaded || lower_message.contains("overloaded") {
                            529
                        } else {
                            503
                        }
                    });
                    return LiveStreamStart::Retry {
                        error: client::CodexError {
                            status,
                            message: message.clone(),
                            detail: Some(message),
                            retry_after: retry_after_from_live_payload(&payload),
                            origin: client::CodexErrorOrigin::WebSocket,
                        },
                    };
                }
                abort_continuation(ctx.session_id.as_deref(), turn_id);
                return LiveStreamStart::Response(map_codex_failure_to_response(&message));
            }
        };
        if !chunk.is_empty() {
            record_live_stream_progress(&ctx, &chunk);
            if terminal {
                update_continuation_from_upstream(
                    ctx.session_id.as_deref(),
                    turn_id,
                    &request_body,
                    &upstream_sse_body,
                );
                return LiveStreamStart::Response(single_live_stream_response(chunk));
            }
            return LiveStreamStart::Response(remaining_live_stream_response(
                upstream_events,
                translator,
                chunk,
                ctx,
                turn_id,
                request_body,
                upstream_sse_body,
                provider_started_at,
            ));
        }
        if terminal {
            update_continuation_from_upstream(
                ctx.session_id.as_deref(),
                turn_id,
                &request_body,
                &upstream_sse_body,
            );
            return LiveStreamStart::Response(empty_live_stream_response());
        }
    }

    LiveStreamStart::Retry {
        error: client::CodexError {
            status: 0,
            message: "WebSocket connection closed before terminal Codex response event".to_string(),
            detail: Some(websocket::WEBSOCKET_MISSING_TERMINAL_DETAIL.to_string()),
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        },
    }
}

fn codex_generation_event(payload: &serde_json::Value) -> bool {
    !matches!(
        payload.get("type").and_then(|value| value.as_str()),
        Some("codex.rate_limits" | "keepalive") | None
    )
}

fn log_codex_request_configuration(
    ctx: &RequestContext,
    request: &translate::request::ResponsesRequest,
    use_responses_lite: bool,
    native_web_search: bool,
) {
    let service_tier = request.service_tier.as_ref().map(|tier| match tier {
        ServiceTier::Priority => "priority",
        ServiceTier::Flex => "flex",
    });
    let reasoning_effort = request
        .reasoning
        .as_ref()
        .and_then(|reasoning| reasoning.effort.as_ref())
        .map(ToString::to_string);
    crate::logging::create_logger("codex").info(
        "request_configuration",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), serde_json::json!(ctx.req_id)),
            ("model".to_string(), serde_json::json!(request.model)),
            ("serviceTier".to_string(), serde_json::json!(service_tier)),
            (
                "reasoningEffort".to_string(),
                serde_json::json!(reasoning_effort),
            ),
            (
                "transport".to_string(),
                serde_json::json!(config::codex_transport().as_str()),
            ),
            (
                "responsesLite".to_string(),
                serde_json::json!(use_responses_lite),
            ),
            (
                "nativeWebSearch".to_string(),
                serde_json::json!(native_web_search),
            ),
            (
                "parallelToolCalls".to_string(),
                serde_json::json!(request.parallel_tool_calls),
            ),
        ])),
    );
}

fn native_web_search_phase(payload: &serde_json::Value) -> Option<&'static str> {
    match payload.get("type").and_then(|value| value.as_str()) {
        Some("response.web_search_call.in_progress") => Some("in_progress"),
        Some("response.web_search_call.searching") => Some("searching"),
        Some("response.web_search_call.completed") => Some("completed"),
        Some("response.output_item.added")
            if payload
                .pointer("/item/type")
                .and_then(|value| value.as_str())
                == Some("web_search_call") =>
        {
            Some("added")
        }
        Some("response.output_item.done")
            if payload
                .pointer("/item/type")
                .and_then(|value| value.as_str())
                == Some("web_search_call") =>
        {
            Some("done")
        }
        _ => None,
    }
}

fn log_native_web_search_phase(
    ctx: &RequestContext,
    payload: &serde_json::Value,
    provider_started_at: Instant,
) {
    let Some(phase) = native_web_search_phase(payload) else {
        return;
    };
    crate::logging::create_logger("codex").info(
        "native_web_search_phase",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), serde_json::json!(ctx.req_id)),
            ("phase".to_string(), serde_json::json!(phase)),
            (
                "elapsedMs".to_string(),
                serde_json::json!(provider_started_at.elapsed().as_millis()),
            ),
        ])),
    );
}

fn log_codex_first_event(
    ctx: &RequestContext,
    payload: &serde_json::Value,
    provider_started_at: Instant,
) {
    crate::logging::create_logger("codex").info(
        "upstream_first_event",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), serde_json::json!(ctx.req_id)),
            (
                "event".to_string(),
                serde_json::json!(payload.get("type").and_then(|value| value.as_str())),
            ),
            (
                "elapsedMs".to_string(),
                serde_json::json!(provider_started_at.elapsed().as_millis()),
            ),
        ])),
    );
}

fn translate_live_stream_payload(
    translator: &mut LiveStreamTranslator,
    payload: &serde_json::Value,
    ctx: &RequestContext,
) -> Result<(Vec<u8>, bool), String> {
    let chunk = translator.accept(payload, ctx.traffic.as_deref())?;
    let terminal = is_codex_terminal_event(payload) || translator.is_finished();
    Ok((chunk, terminal))
}

fn record_live_stream_progress(ctx: &RequestContext, chunk: &[u8]) {
    if let Some(monitor) = ctx.monitor.as_ref() {
        let (input_tokens, output_tokens) = usage_from_anthropic_sse(chunk);
        monitor.stream_progress(
            &ctx.req_id,
            chunk.len() as u64,
            count_sse_events(chunk),
            input_tokens,
            output_tokens,
        );
    }
}

fn single_live_stream_response(chunk: Vec<u8>) -> Response {
    event_stream_response(futures_util::stream::once(async move {
        Ok::<Bytes, std::io::Error>(Bytes::from(chunk))
    }))
}

fn empty_live_stream_response() -> Response {
    event_stream_response(futures_util::stream::empty::<Result<Bytes, std::io::Error>>())
}

fn remaining_live_stream_response(
    mut upstream_events: websocket::CodexWebSocketEventReceiver,
    mut translator: LiveStreamTranslator,
    first_chunk: Vec<u8>,
    ctx: RequestContext,
    turn_id: Option<u64>,
    request_body: translate::request::ResponsesRequest,
    mut upstream_sse_body: Vec<u8>,
    provider_started_at: Instant,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    tokio::spawn(async move {
        if tx.send(Ok(Bytes::from(first_chunk))).await.is_err() {
            abort_continuation(ctx.session_id.as_deref(), turn_id);
            return;
        }
        while let Some(item) = upstream_events.recv().await {
            match item {
                Ok(payload) => {
                    log_native_web_search_phase(&ctx, &payload, provider_started_at);
                    append_upstream_sse_payload(&mut upstream_sse_body, &payload);
                    let (chunk, terminal) =
                        match translate_live_stream_payload(&mut translator, &payload, &ctx) {
                            Ok(result) => result,
                            Err(message) => {
                                abort_continuation(ctx.session_id.as_deref(), turn_id);
                                let chunk = translator.error_chunk(
                                    &message,
                                    "api_error",
                                    ctx.traffic.as_deref(),
                                );
                                if !chunk.is_empty() {
                                    record_live_stream_progress(&ctx, &chunk);
                                    let _ = tx.send(Ok(Bytes::from(chunk))).await;
                                }
                                return;
                            }
                        };
                    if !chunk.is_empty() {
                        record_live_stream_progress(&ctx, &chunk);
                        if tx.send(Ok(Bytes::from(chunk))).await.is_err() {
                            abort_continuation(ctx.session_id.as_deref(), turn_id);
                            return;
                        }
                    }
                    if terminal {
                        update_continuation_from_upstream(
                            ctx.session_id.as_deref(),
                            turn_id,
                            &request_body,
                            &upstream_sse_body,
                        );
                        return;
                    }
                }
                Err(err) => {
                    abort_continuation(ctx.session_id.as_deref(), turn_id);
                    let chunk =
                        translator.finish_after_closed_completed_tool_call(ctx.traffic.as_deref());
                    if !chunk.is_empty() {
                        record_live_stream_progress(&ctx, &chunk);
                        let _ = tx.send(Ok(Bytes::from(chunk))).await;
                        return;
                    }
                    let error_type = codex_stream_error_type(&err);
                    let chunk = translator.error_chunk(
                        codex_error_message(&err),
                        error_type,
                        ctx.traffic.as_deref(),
                    );
                    if !chunk.is_empty() {
                        record_live_stream_progress(&ctx, &chunk);
                        let _ = tx.send(Ok(Bytes::from(chunk))).await;
                    }
                    return;
                }
            }
        }

        abort_continuation(ctx.session_id.as_deref(), turn_id);
        let chunk = translator.finish_after_closed_completed_tool_call(ctx.traffic.as_deref());
        if !chunk.is_empty() {
            record_live_stream_progress(&ctx, &chunk);
            let _ = tx.send(Ok(Bytes::from(chunk))).await;
            return;
        }
        let chunk = translator.error_chunk(
            "WebSocket connection closed before terminal Codex response event",
            "api_error",
            ctx.traffic.as_deref(),
        );
        if !chunk.is_empty() {
            record_live_stream_progress(&ctx, &chunk);
            let _ = tx.send(Ok(Bytes::from(chunk))).await;
        }
    });

    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    event_stream_response(stream)
}

fn append_upstream_sse_payload(buffer: &mut Vec<u8>, payload: &serde_json::Value) {
    let text = payload.to_string();
    for line in text.lines() {
        buffer.extend_from_slice(b"data: ");
        buffer.extend_from_slice(line.as_bytes());
        buffer.push(b'\n');
    }
    buffer.push(b'\n');
}

fn event_stream_response<S>(stream: S) -> Response
where
    S: futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    let headers = [
        (http::header::CONTENT_TYPE, "text/event-stream"),
        (http::header::CACHE_CONTROL, "no-cache"),
        (http::header::CONNECTION, "keep-alive"),
    ];
    (headers, Body::from_stream(stream)).into_response()
}

fn is_codex_terminal_event(payload: &serde_json::Value) -> bool {
    matches!(
        payload.get("type").and_then(|v| v.as_str()),
        Some("response.completed")
            | Some("response.incomplete")
            | Some("response.done")
            | Some("response.failed")
            | Some("response.error")
            | Some("error")
    )
}

fn retryable_live_start_codex_error(err: &client::CodexError) -> bool {
    if err.origin == client::CodexErrorOrigin::WebSocketHandshake {
        return err.status == 0 || matches!(err.status, 429 | 500 | 502 | 503 | 504 | 529);
    }
    if websocket::is_retryable_transport_detail(err.detail.as_deref()) {
        return true;
    }
    matches!(err.status, 429 | 500 | 502 | 503 | 504 | 529)
        || (err.status == 0 && retryable_live_message(codex_error_message(err)))
}

fn max_live_retries(err: &client::CodexError) -> u32 {
    if err.status == 0
        && matches!(
            err.origin,
            client::CodexErrorOrigin::WebSocket | client::CodexErrorOrigin::WebSocketHandshake
        )
    {
        MAX_LIVE_TRANSPORT_RETRIES
    } else {
        MAX_RETRYABLE_LIVE_STREAM_RETRIES
    }
}

fn is_missing_previous_response_error(err: &client::CodexError) -> bool {
    err.detail.as_deref() == Some("previous_response_not_found")
}

fn drop_live_continuation_for_retry(continuation: &mut Option<ContinuationCandidate>) -> bool {
    if continuation
        .as_ref()
        .and_then(|candidate| candidate.previous_response_id.as_deref())
        .is_none()
    {
        return false;
    }

    if let Some(candidate) = continuation.as_mut() {
        candidate.previous_response_id = None;
        candidate.input_delta = None;
        candidate.disabled_reason = Some("full_context_retry".to_string());
    }
    true
}

fn retryable_live_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    [
        "overloaded",
        "rate limit",
        "you can retry your request",
        "temporarily unavailable",
        "timed out",
        "connection closed",
        "connection reset",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn retryable_live_start_payload(payload: &serde_json::Value, _message: &str) -> bool {
    events::classify_event_failure(payload).is_some_and(|failure| failure.retryable())
}

fn retry_after_from_live_payload(payload: &serde_json::Value) -> Option<String> {
    events::classify_event_failure(payload).and_then(|failure| failure.retry_after)
}

fn codex_stream_error_type(err: &client::CodexError) -> &'static str {
    match err.status {
        429 => "rate_limit_error",
        529 => "overloaded_error",
        _ if codex_error_message(err)
            .to_lowercase()
            .contains("overloaded") =>
        {
            "overloaded_error"
        }
        _ => "api_error",
    }
}

fn update_continuation_from_upstream(
    session_id: Option<&str>,
    turn_id: Option<u64>,
    request_body: &translate::request::ResponsesRequest,
    upstream_body: &[u8],
) {
    match finish_metadata_from_upstream(upstream_body) {
        Ok(Some(finish)) if finish.continuation_eligible => {
            record_continuation(
                session_id,
                turn_id,
                request_body,
                finish.response_id.as_deref(),
                &finish.output_items,
            );
        }
        _ => abort_continuation(session_id, turn_id),
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn map_codex_error_to_response(err: &client::CodexError) -> Response {
    let message = codex_error_message(err);
    if is_context_window_overflow(message) {
        return map_codex_failure_to_response(message);
    }

    match err.status {
        401 | 403 => json_error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            err.detail.as_deref().unwrap_or("Authentication failed"),
        ),
        429 => {
            let retry_after = err.retry_after.as_deref().unwrap_or("5");
            let resp = json_error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                &err.message,
            );
            let headers = [(http::header::RETRY_AFTER, retry_after)];
            (headers, resp).into_response()
        }
        status @ (500 | 502 | 503 | 504 | 529)
            if matches!(
                err.origin,
                client::CodexErrorOrigin::BufferedHttp
                    | client::CodexErrorOrigin::BufferedWebSocket
            ) =>
        {
            let response = json_error(
                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                if status == 529 {
                    "overloaded_error"
                } else {
                    "api_error"
                },
                codex_error_message(err),
            );
            if let Some(retry_after) = err.retry_after.as_deref() {
                ([(http::header::RETRY_AFTER, retry_after)], response).into_response()
            } else {
                response
            }
        }
        _ => json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            codex_error_message(err),
        ),
    }
}

fn map_codex_failure_to_response(message: &str) -> Response {
    if is_context_window_overflow(message) {
        json_error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large", message)
    } else {
        json_error(StatusCode::BAD_GATEWAY, "api_error", message)
    }
}

fn is_context_window_overflow(message: &str) -> bool {
    message.to_ascii_lowercase().contains("context window")
}

fn codex_error_message(err: &client::CodexError) -> &str {
    if websocket::is_retryable_transport_detail(err.detail.as_deref()) {
        return err.message.as_str();
    }
    err.detail.as_deref().unwrap_or({
        if err.status == 0 {
            err.message.as_str()
        } else {
            "Upstream error"
        }
    })
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

pub(crate) struct CodexCli;

impl CliHandlers for CodexCli {
    fn login(&self) -> Result<(), anyhow::Error> {
        let tokens = run_browser_login()?;
        let store = file_store();
        let manager = CodexAuthManager::new(store);
        let saved = manager.persist_initial_tokens(&tokens)?;
        print!(
            "{}",
            format_auth_saved_output(&manager.store.auth_path(), saved.account_id.as_deref())
        );
        Ok(())
    }

    fn device(&self) -> Result<(), anyhow::Error> {
        let tokens = DeviceAuthClient::new().run()?;
        let store = file_store();
        let manager = CodexAuthManager::new(store);
        let saved = manager.persist_initial_tokens(&tokens)?;
        print!(
            "{}",
            format_auth_saved_output(&manager.store.auth_path(), saved.account_id.as_deref())
        );
        Ok(())
    }

    fn status(&self) -> Result<(), anyhow::Error> {
        let store = file_store();
        let stored = store.load_auth()?;
        match stored {
            Some(auth) => {
                println!(
                    "Account: {}",
                    auth.account_id.as_deref().unwrap_or("(none)")
                );
                println!("{}", format_expiry(auth.expires, now_ms()));
                println!("Storage: {}", store.auth_path());
                Ok(())
            }
            None => {
                anyhow::bail!("Not authenticated");
            }
        }
    }

    fn logout(&self) -> Result<(), anyhow::Error> {
        let store = file_store();
        store.clear_auth()?;
        println!("Logged out");
        Ok(())
    }
}

pub(crate) static CODEX_CLI: CodexCli = CodexCli;

// ---------------------------------------------------------------------------
// CLI helpers
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn format_expiry(expires: u64, now: u64) -> String {
    let remaining = (i128::from(expires) - i128::from(now)).div_euclid(1000);
    let iso = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(expires) * 1_000_000)
        .ok()
        .and_then(|dt| {
            let fmt = time::format_description::parse_borrowed::<2>(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
            )
            .ok()?;
            dt.format(&fmt).ok()
        })
        .unwrap_or_else(|| "invalid".to_string());
    format!("Expires: {iso} (in {remaining}s)")
}

fn format_auth_saved_output(auth_path: &str, account_id: Option<&str>) -> String {
    let mut out = format!("Auth saved in {auth_path}\n");
    if let Some(account_id) = account_id {
        out.push_str(&format!("Account: {account_id}\n"));
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn live_test_context() -> RequestContext {
        RequestContext {
            req_id: "live-retry-test".to_string(),
            session_id: None,
            session_seq: None,
            provider: "codex".to_string(),
            traffic: None,
            monitor: None,
        }
    }

    fn live_test_request() -> translate::request::ResponsesRequest {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gpt-5.6-sol",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true
        }))
        .unwrap();
        translate_request(
            &body,
            TranslateOptions {
                session_id: None,
                service_tier: None,
                model: "gpt-5.6-sol".to_string(),
                use_responses_lite: true,
            },
        )
        .unwrap()
    }

    fn heartbeat_test_error() -> client::CodexError {
        client::CodexError {
            status: 0,
            message: "WebSocket heartbeat timed out after 10000ms without a Pong".to_string(),
            detail: Some(websocket::WEBSOCKET_HEARTBEAT_TIMEOUT_DETAIL.to_string()),
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        }
    }

    fn request_with_tools(tools: serde_json::Value) -> MessagesRequest {
        serde_json::from_value(serde_json::json!({
            "model": "gpt-5.6-luna",
            "messages": [{"role":"user", "content":"find it"}],
            "tools": tools
        }))
        .unwrap()
    }

    #[test]
    fn web_search_requests_leave_lite_lane_and_upgrade_luna() {
        let body = request_with_tools(serde_json::json!([
            {"type":"web_search_20250305", "name":"web_search"}
        ]));
        for (resolved, expected) in [
            ("gpt-5.6-luna", "gpt-5.6-sol"),
            ("gpt-5.6-sol", "gpt-5.6-sol"),
            ("gpt-5.6-terra", "gpt-5.6-terra"),
            ("gpt-5.4", "gpt-5.4"),
        ] {
            let mut model = resolved.to_string();
            let lite = apply_model_lane_for_request_with_lite(&mut model, &body, true);
            assert!(!lite, "{resolved} with web_search must use the full lane");
            assert_eq!(model, expected);
        }
    }

    #[test]
    fn requests_without_web_search_keep_model_and_lite_lane() {
        let body = request_with_tools(serde_json::json!([
            {"name":"Bash", "input_schema":{}}
        ]));
        for (resolved, lite_expected) in [
            ("gpt-5.6-luna", true),
            ("gpt-5.6-sol", true),
            ("gpt-5.4", false),
        ] {
            let mut model = resolved.to_string();
            let lite = apply_model_lane_for_request_with_lite(&mut model, &body, true);
            assert_eq!(model, resolved, "model must not change without web_search");
            assert_eq!(lite, lite_expected);
        }
    }

    #[test]
    fn responses_lite_can_be_disabled_without_changing_the_model() {
        let body = request_with_tools(serde_json::json!([
            {"name":"Bash", "input_schema":{}}
        ]));
        let mut model = "gpt-5.6-luna".to_string();

        let lite = apply_model_lane_for_request_with_lite(&mut model, &body, false);

        assert!(!lite);
        assert_eq!(model, "gpt-5.6-luna");
    }

    #[test]
    fn generation_timing_ignores_control_events() {
        assert!(!codex_generation_event(&serde_json::json!({
            "type": "codex.rate_limits"
        })));
        assert!(!codex_generation_event(&serde_json::json!({
            "type": "keepalive"
        })));
        assert!(codex_generation_event(&serde_json::json!({
            "type": "response.created"
        })));
    }

    #[test]
    fn classifies_native_web_search_phases() {
        for (event, expected) in [
            ("response.web_search_call.in_progress", "in_progress"),
            ("response.web_search_call.searching", "searching"),
            ("response.web_search_call.completed", "completed"),
        ] {
            assert_eq!(
                native_web_search_phase(&serde_json::json!({"type": event})),
                Some(expected)
            );
        }
        assert_eq!(
            native_web_search_phase(&serde_json::json!({
                "type": "response.output_item.added",
                "item": {"type": "web_search_call"}
            })),
            Some("added")
        );
        assert_eq!(
            native_web_search_phase(&serde_json::json!({
                "type": "response.output_item.done",
                "item": {"type": "web_search_call"}
            })),
            Some("done")
        );
        assert_eq!(
            native_web_search_phase(&serde_json::json!({
                "type": "response.output_item.added",
                "item": {"type": "message"}
            })),
            None
        );
    }

    #[test]
    fn live_stream_progress_records_terminal_usage() {
        let monitor = crate::monitor::MonitorHandle::new(10);
        monitor.request_started(
            "request",
            None,
            None,
            crate::monitor::EndpointKind::Messages,
        );
        let ctx = RequestContext {
            req_id: "request".to_string(),
            session_id: None,
            session_seq: None,
            provider: "codex".to_string(),
            traffic: None,
            monitor: Some(monitor.clone()),
        };
        let chunk = b"event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":12,\"output_tokens\":48}}\n\n";

        record_live_stream_progress(&ctx, chunk);

        let state = monitor.snapshot();
        assert_eq!(state.active[0].input_tokens, Some(12));
        assert_eq!(state.active[0].output_tokens, Some(48));
    }

    #[tokio::test]
    async fn live_transport_retry_stops_after_anthropic_output_begins() {
        let request = live_test_request();
        let ctx = live_test_context();

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(Err(heartbeat_test_error())).await.unwrap();
        drop(tx);
        assert!(matches!(
            live_stream_response_once(
                rx,
                "msg_before".to_string(),
                "gpt-5.6-sol",
                ctx.clone(),
                None,
                request.clone(),
                Instant::now(),
            )
            .await,
            LiveStreamStart::Retry { .. }
        ));

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(Ok(serde_json::json!({
            "type": "response.output_text.delta",
            "output_index": 0,
            "delta": "hello"
        })))
        .await
        .unwrap();
        tx.send(Err(heartbeat_test_error())).await.unwrap();
        drop(tx);
        let response = match live_stream_response_once(
            rx,
            "msg_after".to_string(),
            "gpt-5.6-sol",
            ctx,
            None,
            request,
            Instant::now(),
        )
        .await
        {
            LiveStreamStart::Response(response) => response,
            LiveStreamStart::Retry { .. } => panic!("must not replay after streaming output"),
        };
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("hello"));
        assert!(body.contains("heartbeat timed out"));
    }

    #[test]
    fn supported_models_includes_fast_variants() {
        let provider = CodexProvider::new();
        let models = provider.supported_models();
        assert!(models.contains(&"gpt-5.6-sol".to_string()));
        assert!(models.contains(&"gpt-5.6-sol-fast".to_string()));
        assert!(models.contains(&"gpt-5.6-terra".to_string()));
        assert!(models.contains(&"gpt-5.6-luna".to_string()));
        assert!(models.contains(&"gpt-5.4".to_string()));
        assert!(models.contains(&"gpt-5.4-mini".to_string()));
    }

    #[test]
    fn format_auth_saved_output_with_account() {
        assert_eq!(
            format_auth_saved_output("/tmp/auth.json", Some("acct_1")),
            "Auth saved in /tmp/auth.json\nAccount: acct_1\n"
        );
    }

    #[test]
    fn format_auth_saved_output_without_account() {
        assert_eq!(
            format_auth_saved_output("/tmp/auth.json", None),
            "Auth saved in /tmp/auth.json\n"
        );
    }

    #[test]
    fn format_expiry_with_future_expiry() {
        // 2100-01-01T00:00:00Z in ms
        let expires = 4102444800000;
        let now = 4102444790000; // 10s before
        let output = format_expiry(expires, now);
        assert!(output.starts_with("Expires: 2100-01-01T00:00:00.000Z (in "));
        assert!(output.ends_with("s)"));
    }

    #[test]
    fn format_expiry_with_past_expiry() {
        // 2000-01-01T00:00:00Z in ms
        let expires = 946684800000;
        let now = 946684810000; // 10s after
        let output = format_expiry(expires, now);
        assert!(output.starts_with("Expires: 2000-01-01T00:00:00.000Z (in -"));
    }

    #[tokio::test]
    async fn statusless_codex_error_returns_source_message() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket connect error: HTTP error: 502 Bad Gateway".to_string(),
            detail: None,
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        };

        let response = map_codex_error_to_response(&err);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body.pointer("/error/message").and_then(|v| v.as_str()),
            Some("WebSocket connect error: HTTP error: 502 Bad Gateway")
        );
    }

    #[tokio::test]
    async fn websocket_watchdog_error_returns_descriptive_message() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket heartbeat timed out after 10000ms without a Pong".to_string(),
            detail: Some(websocket::WEBSOCKET_HEARTBEAT_TIMEOUT_DETAIL.to_string()),
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        };

        let response = map_codex_error_to_response(&err);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body.pointer("/error/message")
                .and_then(|value| value.as_str()),
            Some("WebSocket heartbeat timed out after 10000ms without a Pong")
        );
    }

    #[test]
    fn live_start_statusless_websocket_handshake_error_is_retryable() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket connect timeout after 15000ms".to_string(),
            detail: None,
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocketHandshake,
        };

        assert!(retryable_live_start_codex_error(&err));
    }

    #[test]
    fn live_start_heartbeat_error_is_retryable_with_short_network_budget() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket heartbeat timed out".to_string(),
            detail: Some(websocket::WEBSOCKET_HEARTBEAT_TIMEOUT_DETAIL.to_string()),
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        };

        assert!(retryable_live_start_codex_error(&err));
        assert_eq!(max_live_retries(&err), MAX_LIVE_TRANSPORT_RETRIES);
    }

    #[test]
    fn live_upstream_overload_keeps_service_retry_budget() {
        let err = client::CodexError {
            status: 529,
            message: "Overloaded".to_string(),
            detail: None,
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        };

        assert!(retryable_live_start_codex_error(&err));
        assert_eq!(max_live_retries(&err), MAX_RETRYABLE_LIVE_STREAM_RETRIES);
    }

    #[test]
    fn live_start_payload_retry_detection_covers_rate_limit_and_overload() {
        assert!(retryable_live_start_payload(
            &serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": true}
            }),
            "rate limit reached",
        ));
        assert!(retryable_live_start_payload(
            &serde_json::json!({
                "type": "response.failed",
                "response": {"error": {"type": "overloaded_error", "message": "overloaded"}}
            }),
            "overloaded",
        ));
        assert!(!retryable_live_start_payload(
            &serde_json::json!({
                "type": "response.failed",
                "response": {"error": {"message": "bad request"}}
            }),
            "bad request",
        ));
    }
}
