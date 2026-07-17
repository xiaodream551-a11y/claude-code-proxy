pub mod auth;
pub mod client;
pub mod count_tokens;
pub mod translate;

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::{
    Json,
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::anthropic::{
    error::json_error,
    schema::{CountTokensResponse, MessagesRequest},
};
use crate::monitor::MonitorHandle;
use crate::provider::{CliHandlers, Provider, RequestContext};
use crate::retry::{compute_backoff_delay, sleep};
use crate::{registry::GROK_MODELS, traffic::StreamTrafficCapture};

use self::auth::token_store::file_store;
use self::client::{GrokByteStream, GrokError, GrokRequestDeadline, GrokRetryState};
use self::translate::{
    accumulate::accumulate_response_with_traffic,
    model_allowlist::{assert_allowed_model, resolve_model_request},
    request::{GrokReasoning, GrokResponsesRequest, translate_request},
    stream::{SseDecoder, StreamTranslator, stream_error, stream_error_with_message, stream_ping},
};

const GROK_DOWNSTREAM_CHANNEL_CAPACITY: usize = 3;
const GROK_DOWNSTREAM_QUEUE_BYTES: usize = 2 * 1024 * 1024;
const GROK_DOWNSTREAM_STALL_TIMEOUT: Duration = Duration::from_secs(60);
const GROK_MAX_CUMULATIVE_STREAM_BYTES: u64 = 8 * 1024 * 1024;

const MIN_STREAM_HEARTBEAT: Duration = Duration::from_secs(1);
const MAX_STREAM_HEARTBEAT: Duration = Duration::from_secs(60);

pub struct GrokProvider {
    client: Arc<client::GrokClient>,
}
impl GrokProvider {
    pub fn new() -> Self {
        Self {
            client: Arc::new(
                client::GrokClient::new(
                    crate::config::grok_base_url(),
                    crate::config::grok_client_version(),
                )
                .expect("Grok transport is unavailable"),
            ),
        }
    }

    pub fn with_client(client: client::GrokClient) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}
impl Default for GrokProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Provider for GrokProvider {
    fn name(&self) -> &'static str {
        "grok"
    }
    fn supported_models(&self) -> Vec<String> {
        GROK_MODELS
            .iter()
            .map(|model| (*model).to_string())
            .collect()
    }
    fn cli(&self) -> &'static dyn CliHandlers {
        &GROK_CLI
    }
    async fn handle_messages(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let deadline = GrokRequestDeadline::configured();
        let requested = body.model.clone().unwrap_or_else(|| "grok-4.5".into());
        let resolved = resolve_model_request(&requested);
        if let Err(error) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                error.to_string(),
            );
        }
        let mut translated = match translate_request(&body, resolved.model.clone()) {
            Ok(value) => value,
            Err(error) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    error.to_string(),
                );
            }
        };
        if let Some(effort) = resolved.reasoning_effort {
            translated.reasoning = Some(GrokReasoning {
                effort: effort.into(),
            });
        }
        crate::logging::create_logger("grok").info(
            "request_configuration",
            Some(serde_json::Map::from_iter([
                ("reqId".into(), serde_json::json!(ctx.req_id)),
                ("model".into(), serde_json::json!(resolved.model)),
                (
                    "reasoningEffort".into(),
                    serde_json::json!(
                        translated
                            .reasoning
                            .as_ref()
                            .map(|reasoning| reasoning.effort.as_str())
                    ),
                ),
                ("transport".into(), serde_json::json!("http")),
            ])),
        );
        if let Some(monitor) = &ctx.monitor {
            monitor.model_resolved(&ctx.req_id, &resolved.model);
            monitor.upstream_started(&ctx.req_id);
        }

        let retry = Arc::new(Mutex::new(GrokRetryState::with_deadline(deadline)));
        let upstream = match self
            .client
            .post_with_retry(&translated, ctx.traffic.clone(), retry.clone())
            .await
        {
            Ok(response) => response,
            Err(error) => return map_error(error),
        };

        if body.stream {
            let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
            let reconnect = Some(GrokReconnectContext {
                client: self.client.clone(),
                request: Arc::new(translated),
                traffic: ctx.traffic.clone(),
                retry,
            });
            stream_body_with_policy(
                upstream.into_stream(),
                message_id,
                requested,
                ctx.monitor.clone(),
                ctx.req_id.clone(),
                ctx.traffic.clone(),
                reconnect,
                deadline,
                configured_stream_heartbeat(),
            )
        } else {
            match read_non_streaming_body(
                self.client.clone(),
                translated,
                upstream,
                ctx.traffic.clone(),
                retry,
                deadline,
            )
            .await
            {
                Ok(upstream_bytes) => {
                    match accumulate_response_with_traffic(
                        &upstream_bytes,
                        &format!("msg_{}", uuid::Uuid::new_v4().simple()),
                        &requested,
                        ctx.traffic.as_deref(),
                    ) {
                        Ok(value) => {
                            if let Some(traffic) = ctx.traffic.as_ref() {
                                traffic.write_json("051-downstream-response", &value);
                            }
                            if let Some(monitor) = ctx.monitor.as_ref() {
                                monitor.usage_updated(
                                    &ctx.req_id,
                                    value
                                        .pointer("/usage/input_tokens")
                                        .and_then(|v| v.as_u64()),
                                    value
                                        .pointer("/usage/output_tokens")
                                        .and_then(|v| v.as_u64()),
                                );
                            }
                            (StatusCode::OK, Json(value)).into_response()
                        }
                        Err(_) => {
                            write_error(ctx.traffic.as_deref(), "accumulate", "invalid_response");
                            json_error(
                                StatusCode::BAD_GATEWAY,
                                "api_error",
                                "Grok response is invalid",
                            )
                        }
                    }
                }
                Err(error) => {
                    write_error(ctx.traffic.as_deref(), "body_read", "transport");
                    map_error(error)
                }
            }
        }
    }
    async fn handle_count_tokens(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let requested = body.model.clone().unwrap_or_else(|| "grok-4.5".into());
        let resolved = resolve_model_request(&requested);
        if let Err(error) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                error.to_string(),
            );
        }
        let translated = match translate_request(&body, resolved.model) {
            Ok(value) => value,
            Err(error) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    error.to_string(),
                );
            }
        };
        let tokens = count_tokens::count_tokens(&translated);
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

async fn read_non_streaming_body(
    client: Arc<client::GrokClient>,
    request: GrokResponsesRequest,
    mut response: client::GrokResponse,
    traffic: Option<Arc<crate::traffic::TrafficCapture>>,
    retry: Arc<Mutex<GrokRetryState>>,
    deadline: GrokRequestDeadline,
) -> Result<Vec<u8>, GrokError> {
    loop {
        match response.into_bytes().await {
            Ok(bytes) => return Ok(bytes),
            Err(error) if error.is_retryable() => {
                let delay = {
                    let mut state = retry.lock().await;
                    if !state.can_retry_transient() {
                        state.mark_terminal();
                        return Err(error.into_terminal("retry exhausted"));
                    }
                    let delay = compute_backoff_delay(
                        state.transient_failures(),
                        error.retry_after.as_deref(),
                    );
                    if delay.exceeds_budget {
                        state.mark_terminal();
                        return Err(error.into_terminal("retry delay exceeds budget"));
                    }
                    state.note_transient_failure();
                    delay
                };
                client::run_before_deadline(
                    deadline,
                    retry.clone(),
                    error.stage,
                    sleep(delay.wait_ms),
                )
                .await?;
                response = client
                    .post_with_retry(&request, traffic.clone(), retry.clone())
                    .await?;
            }
            Err(error) => return Err(error),
        }
    }
}

struct GrokReconnectContext {
    client: Arc<client::GrokClient>,
    request: Arc<GrokResponsesRequest>,
    traffic: Option<Arc<crate::traffic::TrafficCapture>>,
    retry: Arc<Mutex<GrokRetryState>>,
}

#[cfg(test)]
fn stream_body<S>(
    upstream: S,
    message_id: String,
    model: String,
    monitor: Option<MonitorHandle>,
    req_id: String,
    traffic: Option<Arc<crate::traffic::TrafficCapture>>,
    reconnect: Option<GrokReconnectContext>,
) -> Response
where
    S: Stream<Item = Result<Bytes, client::GrokError>> + Unpin + Send + 'static,
{
    stream_body_with_policy(
        upstream,
        message_id,
        model,
        monitor,
        req_id,
        traffic,
        reconnect,
        GrokRequestDeadline::after(Duration::from_millis(client::DEFAULT_TOTAL_TIMEOUT_MS)),
        Duration::from_millis(client::DEFAULT_STREAM_HEARTBEAT_MS),
    )
}

fn configured_stream_heartbeat() -> Duration {
    Duration::from_millis(crate::config::grok_stream_heartbeat_ms(
        client::DEFAULT_STREAM_HEARTBEAT_MS,
    ))
    .clamp(MIN_STREAM_HEARTBEAT, MAX_STREAM_HEARTBEAT)
}

#[allow(clippy::too_many_arguments)]
fn stream_body_with_policy<S>(
    upstream: S,
    message_id: String,
    model: String,
    monitor: Option<MonitorHandle>,
    req_id: String,
    traffic: Option<Arc<crate::traffic::TrafficCapture>>,
    reconnect: Option<GrokReconnectContext>,
    deadline: GrokRequestDeadline,
    heartbeat_interval: Duration,
) -> Response
where
    S: Stream<Item = Result<Bytes, client::GrokError>> + Unpin + Send + 'static,
{
    let next_heartbeat = tokio::time::Instant::now()
        .checked_add(heartbeat_interval)
        .unwrap_or_else(|| deadline.at())
        .min(deadline.at());
    let state = GrokStreamState {
        upstream: Box::pin(upstream),
        decoder: SseDecoder::default(),
        reducer: translate::reducer::Reducer::default(),
        translator: StreamTranslator::new(message_id.clone(), model.clone()),
        message_id,
        model,
        terminal: false,
        error_sent: false,
        downstream_emitted: false,
        attempt_bytes: 0,
        monitor,
        req_id,
        bytes: 0,
        chunks: 0,
        stream_capture: traffic.as_ref().map(|traffic| traffic.stream_capture()),
        traffic,
        reconnect,
        rebuild: None,
        deadline,
        heartbeat_interval,
        next_heartbeat,
    };
    let (tx, rx) = mpsc::channel(GROK_DOWNSTREAM_CHANNEL_CAPACITY);
    tokio::spawn(run_grok_stream_producer(state, tx));
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|chunk: GrokBudgetedChunk| (Ok::<Bytes, Infallible>(chunk.bytes), rx))
    });
    (
        [
            (http::header::CONTENT_TYPE, "text/event-stream"),
            (http::header::CACHE_CONTROL, "no-cache"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

struct GrokBudgetedChunk {
    bytes: Bytes,
    _permit: Option<OwnedSemaphorePermit>,
}

enum GrokChunkSendOutcome {
    Sent,
    Closed,
    Deadline,
    Stalled,
    TooLarge,
}

async fn send_grok_chunk(
    tx: &mpsc::Sender<GrokBudgetedChunk>,
    budget: &Arc<Semaphore>,
    bytes: Vec<u8>,
    deadline: tokio::time::Instant,
) -> GrokChunkSendOutcome {
    if bytes.len() > GROK_DOWNSTREAM_QUEUE_BYTES {
        return GrokChunkSendOutcome::TooLarge;
    }
    let stall_deadline = tokio::time::Instant::now() + GROK_DOWNSTREAM_STALL_TIMEOUT;
    let permit = tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => return GrokChunkSendOutcome::Deadline,
        _ = tokio::time::sleep_until(stall_deadline) => return GrokChunkSendOutcome::Stalled,
        _ = tx.closed() => return GrokChunkSendOutcome::Closed,
        permit = budget.clone().acquire_many_owned(bytes.len() as u32) => match permit {
            Ok(permit) => permit,
            Err(_) => return GrokChunkSendOutcome::Closed,
        },
    };
    tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => GrokChunkSendOutcome::Deadline,
        _ = tokio::time::sleep_until(stall_deadline) => GrokChunkSendOutcome::Stalled,
        _ = tx.closed() => GrokChunkSendOutcome::Closed,
        result = tx.send(GrokBudgetedChunk {
            bytes: Bytes::from(bytes),
            _permit: Some(permit),
        }) => if result.is_ok() {
            GrokChunkSendOutcome::Sent
        } else {
            GrokChunkSendOutcome::Closed
        },
    }
}

async fn run_grok_stream_producer(mut state: GrokStreamState, tx: mpsc::Sender<GrokBudgetedChunk>) {
    let mut terminal_permit = match tx.clone().try_reserve_owned() {
        Ok(permit) => Some(permit),
        Err(_) => return,
    };
    let budget = Arc::new(Semaphore::new(GROK_DOWNSTREAM_QUEUE_BYTES));
    loop {
        let output = tokio::select! {
            biased;
            _ = tx.closed() => return,
            output = state.next_output() => output,
        };
        let Some(bytes) = output else {
            return;
        };
        match send_grok_chunk(&tx, &budget, bytes, state.deadline.at()).await {
            GrokChunkSendOutcome::Sent => {}
            GrokChunkSendOutcome::Closed => return,
            GrokChunkSendOutcome::Deadline => {
                if let Some(retry) = state.retry_state() {
                    retry.lock().await.mark_terminal();
                }
                let error = GrokError::deadline_exceeded(client::GrokErrorStage::Stream);
                let bytes = state.fail_mapped(error, "deadline", "total_timeout");
                send_reserved_grok_terminal(&mut terminal_permit, bytes);
                return;
            }
            GrokChunkSendOutcome::Stalled => {
                if let Some(retry) = state.retry_state() {
                    retry.lock().await.mark_terminal();
                }
                let bytes = state.fail_at("downstream", "consumer_stalled");
                send_reserved_grok_terminal(&mut terminal_permit, bytes);
                return;
            }
            GrokChunkSendOutcome::TooLarge => {
                let bytes = state.fail_at("downstream", "event_size_limit");
                send_reserved_grok_terminal(&mut terminal_permit, bytes);
                return;
            }
        }
    }
}

fn send_reserved_grok_terminal(
    terminal_permit: &mut Option<mpsc::OwnedPermit<GrokBudgetedChunk>>,
    bytes: Vec<u8>,
) {
    if let Some(permit) = terminal_permit.take() {
        let _ = permit.send(GrokBudgetedChunk {
            bytes: Bytes::from(bytes),
            _permit: None,
        });
    }
}

struct GrokStreamState {
    upstream: GrokByteStream,
    decoder: SseDecoder,
    reducer: translate::reducer::Reducer,
    translator: StreamTranslator,
    message_id: String,
    model: String,
    terminal: bool,
    error_sent: bool,
    downstream_emitted: bool,
    attempt_bytes: u64,
    monitor: Option<MonitorHandle>,
    req_id: String,
    bytes: u64,
    chunks: u64,
    stream_capture: Option<StreamTrafficCapture>,
    traffic: Option<Arc<crate::traffic::TrafficCapture>>,
    reconnect: Option<GrokReconnectContext>,
    rebuild: Option<GrokPendingRebuild>,
    deadline: GrokRequestDeadline,
    heartbeat_interval: Duration,
    next_heartbeat: tokio::time::Instant,
}

impl GrokStreamState {
    fn retry_state(&self) -> Option<Arc<Mutex<GrokRetryState>>> {
        self.reconnect
            .as_ref()
            .map(|reconnect| reconnect.retry.clone())
    }

    async fn next_output(&mut self) -> Option<Vec<u8>> {
        if self.terminal {
            return None;
        }
        if self.error_sent {
            self.terminal = true;
            return None;
        }
        loop {
            if self.rebuild.is_some() {
                let (fail_stage, fail_kind) = {
                    let rebuild = self.rebuild.as_ref().expect("rebuild must exist");
                    (rebuild.fail_stage, rebuild.fail_kind)
                };
                match self.next_rebuild_poll().await {
                    GrokRebuildPoll::Rebuilt(upstream) => {
                        self.reset_attempt(upstream);
                        continue;
                    }
                    GrokRebuildPoll::Heartbeat => {
                        let ping = stream_ping();
                        self.capture_downstream(&ping);
                        return Some(ping);
                    }
                    GrokRebuildPoll::Failed(error) => {
                        return Some(self.fail_mapped(error, fail_stage, fail_kind));
                    }
                }
            }
            let chunk = match self.next_upstream_chunk().await {
                Ok(GrokStreamPoll::Chunk(chunk)) => chunk,
                Ok(GrokStreamPoll::Heartbeat) => {
                    let ping = stream_ping();
                    self.capture_downstream(&ping);
                    return Some(ping);
                }
                Ok(GrokStreamPoll::End) => {
                    if self.decoder.finish().is_err() || !self.reducer.finished() {
                        match self.begin_rebuild(
                            client::GrokError::stream_transport(
                                client::GrokErrorStage::Stream,
                                "Grok stream closed before a terminal event",
                            ),
                            "decoder",
                            "incomplete_stream",
                        ) {
                            Ok(()) => continue,
                            Err(bytes) => return Some(bytes),
                        }
                    }
                    self.terminal = true;
                    self.finish_capture(true);
                    return None;
                }
                Err(error) => match self.begin_rebuild(error, "transport", "upstream_stream") {
                    Ok(()) => continue,
                    Err(bytes) => return Some(bytes),
                },
            };

            if cumulative_stream_size_exceeded(self.bytes, chunk.len()) {
                return Some(self.fail_at("transport", "response_size_limit"));
            }
            if self.bytes == 0
                && let Some(monitor) = self.monitor.as_ref()
            {
                monitor.generation_started(&self.req_id);
            }
            self.bytes = self.bytes.saturating_add(chunk.len() as u64);
            self.attempt_bytes = self.attempt_bytes.saturating_add(chunk.len() as u64);
            self.chunks = self.chunks.saturating_add(1);
            if let Some(monitor) = self.monitor.as_ref() {
                monitor.stream_progress(&self.req_id, chunk.len() as u64, 1, None, None);
            }
            let events = match self.decoder.push(&chunk) {
                Ok(events) => events,
                Err(_) => return Some(self.fail_at("decoder", "malformed_sse")),
            };
            let mut out = Vec::new();
            let mut semantic_output = false;
            for event in events {
                let value: serde_json::Value = match serde_json::from_str(&event.data) {
                    Ok(value) => value,
                    Err(_) => {
                        if let Some(capture) = self.stream_capture.as_mut() {
                            capture.malformed("json", "malformed_event");
                        }
                        return Some(self.fail_at("json", "malformed_event"));
                    }
                };
                if let Some(capture) = self.stream_capture.as_mut() {
                    capture.upstream_event(event.event.as_deref(), &value);
                }
                let reduced = match self.reducer.push(value) {
                    Ok(events) => events,
                    Err(_) => return Some(self.fail_at("reducer", "invalid_event")),
                };
                let usage = reduced.iter().find_map(|event| match event {
                    translate::reducer::ReducerEvent::Finish {
                        input_tokens,
                        output_tokens,
                        ..
                    } => Some((*input_tokens, *output_tokens)),
                    _ => None,
                });
                semantic_output |= reduced.iter().any(|event| event.is_semantic());
                match self.translator.render(reduced) {
                    Ok(bytes) => out.extend(bytes),
                    Err(_) => return Some(self.fail_at("render", "invalid_event")),
                }
                if let Some((input_tokens, output_tokens)) = usage
                    && let Some(monitor) = self.monitor.as_ref()
                {
                    monitor.usage_updated(&self.req_id, Some(input_tokens), Some(output_tokens));
                }
                if self.reducer.finished() {
                    self.terminal = true;
                    if semantic_output {
                        self.downstream_emitted = true;
                    }
                    self.capture_downstream(&out);
                    self.finish_capture(true);
                    return if out.is_empty() { None } else { Some(out) };
                }
            }
            if !out.is_empty() {
                if semantic_output {
                    self.downstream_emitted = true;
                }
                self.capture_downstream(&out);
                self.schedule_next_heartbeat();
                return Some(out);
            }
        }
    }

    async fn next_upstream_chunk(&mut self) -> Result<GrokStreamPoll, GrokError> {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(self.deadline.at()) => {
                if let Some(reconnect) = self.reconnect.as_ref() {
                    reconnect.retry.lock().await.mark_terminal();
                }
                Err(GrokError::deadline_exceeded(client::GrokErrorStage::Stream))
            }
            _ = tokio::time::sleep_until(self.next_heartbeat) => {
                self.schedule_next_heartbeat();
                Ok(GrokStreamPoll::Heartbeat)
            },
            item = self.upstream.next() => match item {
                Some(Ok(chunk)) => Ok(GrokStreamPoll::Chunk(chunk)),
                Some(Err(error)) => Err(error),
                None => Ok(GrokStreamPoll::End),
            },
        }
    }

    fn schedule_next_heartbeat(&mut self) {
        self.next_heartbeat = tokio::time::Instant::now()
            .checked_add(self.heartbeat_interval)
            .unwrap_or_else(|| self.deadline.at())
            .min(self.deadline.at());
    }

    fn begin_rebuild(
        &mut self,
        error: GrokError,
        fail_stage: &'static str,
        fail_kind: &'static str,
    ) -> Result<(), Vec<u8>> {
        if self.downstream_emitted || !error.is_retryable() {
            return Err(self.fail_mapped(error, fail_stage, fail_kind));
        }
        let Some(reconnect) = self.reconnect.as_ref() else {
            return Err(self.fail_at(fail_stage, fail_kind));
        };
        let client = reconnect.client.clone();
        let request = reconnect.request.clone();
        let traffic = reconnect.traffic.clone();
        let retry = reconnect.retry.clone();
        let deadline = self.deadline;
        let attempt_bytes = self.attempt_bytes;
        let future = Box::pin(async move {
            let delay = {
                let mut state = retry.lock().await;
                if !state.can_retry_transient() {
                    state.mark_terminal();
                    return Err(error.into_terminal("retry exhausted"));
                }
                let delay =
                    compute_backoff_delay(state.transient_failures(), error.retry_after.as_deref());
                if delay.exceeds_budget {
                    state.mark_terminal();
                    return Err(error.into_terminal("retry delay exceeds budget"));
                }
                state.note_transient_failure();
                if let Some(traffic) = traffic.as_ref() {
                    traffic.write_json(
                        "024-upstream-stream-rebuild",
                        &serde_json::json!({
                            "wait_ms": delay.wait_ms,
                            "origin": client::origin_name(error.origin),
                            "error_stage": client::stage_name(error.stage),
                            "status": error.status.as_u16(),
                            "message": error.message,
                            "stage": fail_stage,
                            "kind": fail_kind,
                            "attempt_bytes": attempt_bytes,
                        }),
                    );
                }
                delay
            };
            client::run_before_deadline(deadline, retry.clone(), error.stage, sleep(delay.wait_ms))
                .await?;
            client
                .post_with_retry(&request, traffic, retry)
                .await
                .map(client::GrokResponse::into_stream)
        });
        self.rebuild = Some(GrokPendingRebuild {
            future,
            fail_stage,
            fail_kind,
        });
        Ok(())
    }

    async fn next_rebuild_poll(&mut self) -> GrokRebuildPoll {
        enum WaitResult {
            Deadline,
            Heartbeat,
            Finished(Result<GrokByteStream, GrokError>),
        }

        let wait = {
            let rebuild = self.rebuild.as_mut().expect("rebuild must exist");
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(self.deadline.at()) => WaitResult::Deadline,
                _ = tokio::time::sleep_until(self.next_heartbeat) => WaitResult::Heartbeat,
                result = &mut rebuild.future => WaitResult::Finished(result),
            }
        };
        match wait {
            WaitResult::Deadline => {
                // Drop the in-flight auth/header/body request before taking the retry lock. This
                // makes downstream cancellation and the wall-clock deadline cancel the socket,
                // rather than leaving a detached reconnect task behind.
                self.rebuild.take();
                if let Some(reconnect) = self.reconnect.as_ref() {
                    reconnect.retry.lock().await.mark_terminal();
                }
                GrokRebuildPoll::Failed(GrokError::deadline_exceeded(
                    client::GrokErrorStage::Stream,
                ))
            }
            WaitResult::Heartbeat => {
                self.schedule_next_heartbeat();
                GrokRebuildPoll::Heartbeat
            }
            WaitResult::Finished(result) => {
                self.rebuild.take();
                match result {
                    Ok(upstream) => GrokRebuildPoll::Rebuilt(upstream),
                    Err(error) => GrokRebuildPoll::Failed(error),
                }
            }
        }
    }

    fn reset_attempt(&mut self, upstream: GrokByteStream) {
        self.upstream = upstream;
        self.decoder = SseDecoder::default();
        self.reducer = translate::reducer::Reducer::default();
        self.translator = StreamTranslator::new(self.message_id.clone(), self.model.clone());
        self.attempt_bytes = 0;
        self.error_sent = false;
        self.terminal = false;
    }

    fn fail_mapped(&mut self, error: GrokError, stage: &str, kind: &str) -> Vec<u8> {
        self.fail(stage, kind, Some(&error))
    }

    fn fail_at(&mut self, stage: &str, kind: &str) -> Vec<u8> {
        self.fail(stage, kind, None)
    }

    fn fail(&mut self, stage: &str, kind: &str, error: Option<&GrokError>) -> Vec<u8> {
        self.error_sent = true;
        let stream_message = match error {
            Some(error) if error.origin == client::GrokErrorOrigin::Deadline => {
                error.message.as_str()
            }
            _ => "Grok stream is invalid",
        };
        if let Some(capture) = self.stream_capture.as_mut() {
            capture.malformed(stage, kind);
            capture.downstream_event(
                "error",
                serde_json::json!({
                    "type":"error",
                    "error":{"type":"api_error","message":stream_message}
                }),
            );
        }
        if let Some(traffic) = self.traffic.as_ref() {
            let mut fields = serde_json::Map::from_iter([
                ("stage".into(), serde_json::json!(stage)),
                ("kind".into(), serde_json::json!(kind)),
                ("bytes".into(), serde_json::json!(self.bytes)),
                ("chunks".into(), serde_json::json!(self.chunks)),
                (
                    "downstream_emitted".into(),
                    serde_json::json!(self.downstream_emitted),
                ),
            ]);
            if let Some(error) = error {
                fields.insert("status".into(), serde_json::json!(error.status.as_u16()));
                fields.insert(
                    "origin".into(),
                    serde_json::json!(client::origin_name(error.origin)),
                );
                fields.insert(
                    "error_stage".into(),
                    serde_json::json!(client::stage_name(error.stage)),
                );
                fields.insert("message".into(), serde_json::json!(error.message));
            }
            traffic.write_json("060-grok-stream-error", &serde_json::Value::Object(fields));
        }
        if let Some(error) = error {
            crate::logging::create_logger("grok").info(
                "stream_error",
                Some(serde_json::Map::from_iter([
                    ("stage".into(), serde_json::json!(stage)),
                    ("kind".into(), serde_json::json!(kind)),
                    ("status".into(), serde_json::json!(error.status.as_u16())),
                    (
                        "origin".into(),
                        serde_json::json!(client::origin_name(error.origin)),
                    ),
                    (
                        "errorStage".into(),
                        serde_json::json!(client::stage_name(error.stage)),
                    ),
                    ("message".into(), serde_json::json!(error.message)),
                    ("reqId".into(), serde_json::json!(self.req_id)),
                ])),
            );
        }
        self.finish_capture(false);
        if stream_message == "Grok stream is invalid" {
            stream_error()
        } else {
            stream_error_with_message(stream_message)
        }
    }

    fn capture_downstream(&mut self, bytes: &[u8]) {
        let Some(capture) = self.stream_capture.as_mut() else {
            return;
        };
        let mut decoder = SseDecoder::default();
        if let Ok(events) = decoder.push(bytes) {
            for event in events {
                if let Ok(data) = serde_json::from_str(&event.data) {
                    capture.downstream_event(event.event.as_deref().unwrap_or("message"), data);
                }
            }
        }
    }

    fn finish_capture(&mut self, completed: bool) {
        if let (Some(capture), Some(traffic)) = (self.stream_capture.take(), self.traffic.as_ref())
        {
            capture.finish(
                traffic,
                serde_json::json!({
                    "kind": if completed { "stream_completion" } else { "stream_error" },
                    "bytes": self.bytes,
                    "chunks": self.chunks,
                }),
            );
        }
    }
}

fn cumulative_stream_size_exceeded(current: u64, next: usize) -> bool {
    current.saturating_add(next as u64) > GROK_MAX_CUMULATIVE_STREAM_BYTES
}

type GrokRebuildFuture =
    Pin<Box<dyn Future<Output = Result<GrokByteStream, GrokError>> + Send + 'static>>;

struct GrokPendingRebuild {
    future: GrokRebuildFuture,
    fail_stage: &'static str,
    fail_kind: &'static str,
}

enum GrokRebuildPoll {
    Rebuilt(GrokByteStream),
    Heartbeat,
    Failed(GrokError),
}

enum GrokStreamPoll {
    Chunk(Bytes),
    Heartbeat,
    End,
}

impl Drop for GrokStreamState {
    fn drop(&mut self) {
        if self.terminal || self.stream_capture.is_none() {
            return;
        }
        if let Some(traffic) = self.traffic.as_ref() {
            traffic.write_json(
                "060-grok-stream-abandoned",
                &serde_json::json!({
                    "stage": "downstream",
                    "kind": "client_disconnect",
                    "reason": "downstream_body_dropped",
                    "bytes": self.bytes,
                    "chunks": self.chunks,
                }),
            );
        }
        if let (Some(capture), Some(traffic)) = (self.stream_capture.take(), self.traffic.as_ref())
        {
            capture.finish(
                traffic,
                serde_json::json!({
                    "kind": "stream_abandoned",
                    "reason": "downstream_body_dropped",
                    "bytes": self.bytes,
                    "chunks": self.chunks,
                }),
            );
        }
    }
}

fn write_error(traffic: Option<&crate::traffic::TrafficCapture>, stage: &str, kind: &str) {
    if let Some(traffic) = traffic {
        traffic.write_json(
            "060-grok-stream-error",
            &serde_json::json!({"stage":stage,"kind":kind}),
        );
    }
}

fn map_error(error: client::GrokError) -> Response {
    match error.status {
        StatusCode::UNAUTHORIZED => json_error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            error.message,
        ),
        StatusCode::TOO_MANY_REQUESTS => {
            let response = json_error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                error.message,
            );
            if let Some(retry_after) = error.retry_after {
                ([(http::header::RETRY_AFTER, retry_after)], response).into_response()
            } else {
                response
            }
        }
        StatusCode::PAYMENT_REQUIRED | StatusCode::FORBIDDEN => {
            json_error(error.status, "permission_error", error.message)
        }
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => json_error(error.status, "api_error", error.message),
        _ => json_error(StatusCode::BAD_GATEWAY, "api_error", error.message),
    }
}

pub struct GrokCli;
pub static GROK_CLI: GrokCli = GrokCli;
impl CliHandlers for GrokCli {
    fn login(&self) -> anyhow::Result<()> {
        let store = file_store();
        auth::login::login(&store)?;
        println!("Grok authentication saved in {}", store.auth_path());
        Ok(())
    }
    fn device(&self) -> anyhow::Result<()> {
        let store = file_store();
        auth::device::device_login(&store)?;
        println!("Grok authentication saved in {}", store.auth_path());
        Ok(())
    }
    fn status(&self) -> anyhow::Result<()> {
        let store = file_store();
        match store.load_auth()? {
            Some(auth) => {
                println!("Auth path: {}", store.auth_path());
                println!("Authenticated: true");
                println!(
                    "Expires in {}s",
                    auth.expires_at_ms.saturating_sub(now_ms()) / 1000
                );
                Ok(())
            }
            None => anyhow::bail!("Not authenticated"),
        }
    }
    fn logout(&self) -> anyhow::Result<()> {
        let store = file_store();
        store.clear_auth_exclusive()?;
        println!("Grok proxy credentials removed");
        Ok(())
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use crate::monitor::{EndpointKind, MonitorHandle};
    use crate::traffic::test_capture;
    use http_body_util::BodyExt;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::client::{GrokErrorStage, GrokTimeouts};
    use super::*;

    #[test]
    fn cumulative_stream_byte_limit_accepts_boundary_and_rejects_next_byte() {
        assert!(!cumulative_stream_size_exceeded(
            GROK_MAX_CUMULATIVE_STREAM_BYTES - 1,
            1
        ));
        assert!(cumulative_stream_size_exceeded(
            GROK_MAX_CUMULATIVE_STREAM_BYTES,
            1
        ));
    }

    struct ChannelStream(mpsc::Receiver<Result<Bytes, client::GrokError>>);

    impl Stream for ChannelStream {
        type Item = Result<Bytes, client::GrokError>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }

    #[tokio::test]
    async fn completely_silent_stream_emits_pings_then_explicit_total_timeout() {
        let (tx, rx) = mpsc::channel(1);
        let response = stream_body_with_policy(
            ChannelStream(rx),
            "msg_silent".into(),
            "grok-4.5".into(),
            None,
            "req_silent".into(),
            None,
            None,
            GrokRequestDeadline::after(Duration::from_millis(90)),
            Duration::from_millis(20),
        );

        let body = tokio::time::timeout(Duration::from_millis(500), response.into_body().collect())
            .await
            .expect("the total deadline should end a silent stream")
            .unwrap()
            .to_bytes();
        drop(tx);
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.matches("event: ping").count() >= 2, "{body}");
        assert!(body.contains(r#"{"type":"ping"}"#), "{body}");
        assert!(body.contains("total wall-clock timeout"), "{body}");
        assert!(!body.contains("Grok stream is invalid"), "{body}");
    }

    #[tokio::test]
    async fn continuous_reasoning_cannot_starve_ping_or_extend_total_deadline() {
        let (tx, rx) = mpsc::channel(8);
        let producer = tokio::spawn(async move {
            let chunk = Bytes::from_static(
                b"data: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"draft \\ud83d\\ude0a\"}\n\n",
            );
            loop {
                if tx.send(Ok(chunk.clone())).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        });
        let response = stream_body_with_policy(
            ChannelStream(rx),
            "msg_reasoning".into(),
            "grok-4.5".into(),
            None,
            "req_reasoning".into(),
            None,
            None,
            GrokRequestDeadline::after(Duration::from_millis(90)),
            Duration::from_millis(20),
        );

        let body = tokio::time::timeout(Duration::from_millis(500), response.into_body().collect())
            .await
            .expect("reasoning traffic must not extend the total deadline")
            .unwrap()
            .to_bytes();
        producer.await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let pings = body.matches("event: ping").count();

        assert!((2..=6).contains(&pings), "pings={pings}, body={body}");
        assert!(!body.contains("draft"), "{body}");
        assert!(!body.contains('😊'), "{body}");
        assert!(body.contains("total wall-clock timeout"), "{body}");
    }

    #[tokio::test]
    async fn continuous_reasoning_emits_pings_before_one_final_answer() {
        let (tx, rx) = mpsc::channel(8);
        let producer = tokio::spawn(async move {
            let reasoning = Bytes::from_static(
                b"data: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"draft\"}\n\n",
            );
            for _ in 0..24 {
                tx.send(Ok(reasoning.clone())).await.unwrap();
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            tx.send(Ok(Bytes::from_static(
                b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"FINAL_ONCE\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n",
            )))
            .await
            .unwrap();
        });
        let response = stream_body_with_policy(
            ChannelStream(rx),
            "msg_reasoning_final".into(),
            "grok-4.5".into(),
            None,
            "req_reasoning_final".into(),
            None,
            None,
            GrokRequestDeadline::after(Duration::from_millis(500)),
            Duration::from_millis(20),
        );

        let body = tokio::time::timeout(Duration::from_secs(1), response.into_body().collect())
            .await
            .expect("the final answer should complete before the total deadline")
            .unwrap()
            .to_bytes();
        producer.await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.matches("event: ping").count() >= 2, "{body}");
        assert_eq!(body.matches("FINAL_ONCE").count(), 1, "{body}");
        assert!(!body.contains("draft"), "{body}");
        assert!(!body.contains("total wall-clock timeout"), "{body}");
    }

    #[tokio::test]
    async fn dropping_downstream_body_immediately_drops_upstream_stream() {
        let (tx, rx) = mpsc::channel(1);
        let response = stream_body_with_policy(
            ChannelStream(rx),
            "msg_drop".into(),
            "grok-4.5".into(),
            None,
            "req_drop".into(),
            None,
            None,
            GrokRequestDeadline::after(Duration::from_secs(5)),
            Duration::from_millis(20),
        );
        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_millis(200), body.frame())
            .await
            .expect("heartbeat should make the body observable")
            .expect("stream should yield a heartbeat")
            .unwrap();
        assert!(String::from_utf8_lossy(frame.data_ref().unwrap()).contains("event: ping"));

        drop(body);

        tokio::time::timeout(Duration::from_millis(100), tx.closed())
            .await
            .expect("dropping the downstream body must cancel the upstream poller");
    }

    #[tokio::test]
    async fn stalled_stream_rebuild_emits_multiple_pings_then_honors_total_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_complete_http_request(&mut stream).await;
            let mut byte = [0_u8; 1];
            tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
                .await
                .expect("the total deadline must cancel the stalled rebuild socket")
                .unwrap()
        });
        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 5_000,
                first_byte_ms: 5_000,
                body_idle_ms: 5_000,
            },
        )
        .await;
        let deadline = GrokRequestDeadline::after(Duration::from_millis(180));
        let retry = Arc::new(Mutex::new(GrokRetryState::with_deadline(deadline)));
        let reconnect = Some(GrokReconnectContext {
            client: Arc::new(client),
            request: Arc::new(sample_request()),
            traffic: None,
            retry: retry.clone(),
        });
        let mut reset =
            client::GrokError::stream_transport(GrokErrorStage::Stream, "synthetic stream reset");
        reset.retry_after = Some("0".into());

        let response = stream_body_with_policy(
            futures_util::stream::iter(vec![Err(reset)]),
            "msg_stalled_rebuild".into(),
            "grok-4.5".into(),
            None,
            "req_stalled_rebuild".into(),
            None,
            reconnect,
            deadline,
            Duration::from_millis(25),
        );
        let body = tokio::time::timeout(Duration::from_secs(1), response.into_body().collect())
            .await
            .expect("the shared total deadline must terminate the rebuild")
            .unwrap()
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.matches("event: ping").count() >= 3, "{body}");
        assert!(body.contains("total wall-clock timeout"), "{body}");
        assert!(retry.lock().await.is_terminal());
        assert_eq!(server.await.unwrap(), 0);
    }

    #[tokio::test]
    async fn dropping_downstream_during_stalled_rebuild_cancels_socket_immediately() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_complete_http_request(&mut stream).await;
            let _ = request_seen_tx.send(());
            let mut byte = [0_u8; 1];
            tokio::time::timeout(Duration::from_millis(500), stream.read(&mut byte))
                .await
                .expect("dropping downstream must promptly cancel the rebuild socket")
                .unwrap()
        });
        let (client, _temp) = test_client(
            &format!("http://{addr}/v1"),
            GrokTimeouts {
                connect_ms: 1_000,
                header_ms: 5_000,
                first_byte_ms: 5_000,
                body_idle_ms: 5_000,
            },
        )
        .await;
        let deadline = GrokRequestDeadline::after(Duration::from_secs(5));
        let retry = Arc::new(Mutex::new(GrokRetryState::with_deadline(deadline)));
        let reconnect = Some(GrokReconnectContext {
            client: Arc::new(client),
            request: Arc::new(sample_request()),
            traffic: None,
            retry,
        });
        let mut reset =
            client::GrokError::stream_transport(GrokErrorStage::Stream, "synthetic stream reset");
        reset.retry_after = Some("0".into());
        let response = stream_body_with_policy(
            futures_util::stream::iter(vec![Err(reset)]),
            "msg_cancel_rebuild".into(),
            "grok-4.5".into(),
            None,
            "req_cancel_rebuild".into(),
            None,
            reconnect,
            deadline,
            Duration::from_millis(20),
        );
        let mut body = response.into_body();
        let first = tokio::time::timeout(Duration::from_millis(200), body.frame())
            .await
            .expect("rebuild wait must emit its first ping")
            .expect("stream must remain open")
            .unwrap();
        assert!(String::from_utf8_lossy(first.data_ref().unwrap()).contains("event: ping"));
        tokio::time::timeout(Duration::from_millis(200), request_seen_rx)
            .await
            .expect("the stalled rebuild request must start")
            .expect("the rebuild server must observe the request");
        let second = tokio::time::timeout(Duration::from_millis(200), body.frame())
            .await
            .expect("rebuild wait must emit another ping")
            .expect("stream must remain open")
            .unwrap();
        assert!(String::from_utf8_lossy(second.data_ref().unwrap()).contains("event: ping"));

        drop(body);

        assert_eq!(server.await.unwrap(), 0);
    }

    fn sample_request() -> GrokResponsesRequest {
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
            text: None,
        }
    }

    async fn test_client(base_url: &str, timeouts: GrokTimeouts) -> (client::GrokClient, TempDir) {
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
        let store = auth::token_store::GrokTokenStore::new(crate::auth::FileAuthStore::new(
            primary, legacy,
        ));
        store
            .save_auth(auth::token_store::StoredAuth {
                access: "test-access".into(),
                refresh: "test-refresh".into(),
                expires_at_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + 3_600_000,
                issuer: auth::login::CANONICAL_ISSUER.into(),
                client_id: auth::login::CLIENT_ID.into(),
            })
            .unwrap();
        let auth_manager = auth::manager::GrokAuthManager::new(store).unwrap();
        let client = client::GrokClient::new_for_test(
            base_url.to_string(),
            "test-version".into(),
            timeouts,
            auth_manager,
        )
        .unwrap();
        (client, temp)
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut buf = vec![0_u8; 32 * 1024];
        let mut total = 0usize;
        loop {
            let n = stream.read(&mut buf[total..]).await.unwrap();
            assert!(n > 0);
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

    async fn read_complete_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = read_http_request(stream).await;
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .expect("HTTP request must contain complete headers");
        let content_length = String::from_utf8_lossy(&request[..header_end])
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        let complete_len = header_end.saturating_add(content_length);
        if request.len() < complete_len {
            let previous_len = request.len();
            request.resize(complete_len, 0);
            stream
                .read_exact(&mut request[previous_len..])
                .await
                .expect("HTTP request body must arrive");
        }
        request
    }

    #[tokio::test]
    async fn map_error_preserves_transient_gateway_statuses() {
        for status in [
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            let response = map_error(client::GrokError {
                status,
                retry_after: None,
                message: "temporary authentication failure".into(),
                origin: client::GrokErrorOrigin::Auth,
                stage: client::GrokErrorStage::Auth,
                retryable: true,
            });
            assert_eq!(response.status(), status);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["error"]["type"], "api_error");
            assert_eq!(body["error"]["message"], "temporary authentication failure");
        }
    }

    #[tokio::test]
    async fn streaming_usage_updates_completed_monitor_request() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started(
            "req_1",
            Some("session_1".into()),
            Some(1),
            EndpointKind::Messages,
        );
        monitor.provider_selected("req_1", "grok", "grok-4.5", None);
        monitor.request_completed("req_1", 200, None, None);

        let upstream = futures_util::stream::iter(vec![Ok(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}}\n\n",
        ))]);
        let response = stream_body(
            upstream,
            "msg_1".into(),
            "grok-4.5".into(),
            Some(monitor.clone()),
            "req_1".into(),
            None,
            None,
        );
        let _ = response.into_body().collect().await.unwrap();

        let snapshot = monitor.snapshot();
        let request = snapshot
            .recent
            .iter()
            .find(|request| request.request_id == "req_1")
            .unwrap();
        assert_eq!(request.input_tokens, Some(12));
        assert_eq!(request.output_tokens, Some(3));
        assert!(request.streamed_bytes > 0);
        assert!(request.stream_chunks > 0);
        let session = snapshot
            .sessions
            .iter()
            .find(|session| session.session_id.as_deref() == Some("session_1"))
            .unwrap();
        assert_eq!(session.input_tokens, 12);
        assert_eq!(session.output_tokens, 3);
    }

    #[tokio::test]
    async fn downstream_event_arrives_before_upstream_completion() {
        let (tx, rx) = mpsc::channel(2);
        let response = stream_body(
            ChannelStream(rx),
            "msg_1".into(),
            "grok-4.5".into(),
            None,
            "req_1".into(),
            None,
            None,
        );
        let mut body = response.into_body();

        tx.send(Ok(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"first\"}\n\n",
        )))
        .await
        .unwrap();

        let first = tokio::time::timeout(Duration::from_millis(250), body.frame())
            .await
            .expect("downstream body waited for upstream completion")
            .expect("downstream body ended before its first event")
            .expect("downstream body frame failed")
            .into_data()
            .expect("first downstream frame was not data");
        let first = String::from_utf8(first.to_vec()).unwrap();
        assert!(first.contains("event: message_start"));
        assert!(first.contains("first"));

        tx.send(Ok(Bytes::from_static(
            b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n",
        )))
        .await
        .unwrap();
        let terminal = tokio::time::timeout(Duration::from_millis(250), body.frame())
            .await
            .expect("downstream completion timed out")
            .expect("downstream completion was missing")
            .expect("downstream completion frame failed")
            .into_data()
            .expect("downstream completion frame was not data");
        assert!(
            String::from_utf8(terminal.to_vec())
                .unwrap()
                .contains("event: message_stop")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(250), body.frame())
                .await
                .expect("downstream EOF waited for upstream EOF")
                .is_none()
        );
    }

    #[tokio::test]
    async fn dropped_downstream_body_finalizes_partial_capture() {
        let temp = TempDir::new().unwrap();
        let traffic = Arc::new(test_capture(temp.path().join("traffic")));
        let (tx, rx) = mpsc::channel(2);
        let response = stream_body(
            ChannelStream(rx),
            "msg_1".into(),
            "grok-4.5".into(),
            None,
            "req_1".into(),
            Some(traffic),
            None,
        );
        let mut body = response.into_body();

        tx.send(Ok(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
        )))
        .await
        .unwrap();
        let _ = body.frame().await.unwrap().unwrap();
        drop(body);

        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                let ready = std::fs::read_dir(temp.path().join("traffic"))
                    .ok()
                    .is_some_and(|entries| {
                        entries.filter_map(Result::ok).any(|entry| {
                            entry
                                .path()
                                .to_string_lossy()
                                .contains("061-grok-stream-summary")
                        })
                    });
                if ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the body should finalize capture promptly");

        let entries: Vec<_> = std::fs::read_dir(temp.path().join("traffic"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        let abandoned = entries
            .iter()
            .find(|path| path.to_string_lossy().contains("060-grok-stream-abandoned"))
            .unwrap();
        let summary = entries
            .iter()
            .find(|path| path.to_string_lossy().contains("061-grok-stream-summary"))
            .unwrap();
        let abandoned: serde_json::Value =
            serde_json::from_slice(&std::fs::read(abandoned).unwrap()).unwrap();
        let summary: serde_json::Value =
            serde_json::from_slice(&std::fs::read(summary).unwrap()).unwrap();
        assert_eq!(abandoned["kind"], "client_disconnect");
        assert_eq!(summary["completion"]["kind"], "stream_abandoned");
        assert_eq!(summary["completion"]["reason"], "downstream_body_dropped");
        assert_eq!(summary["completion"]["chunks"], 1);
        assert!(summary["upstream_events"]["captured"].as_u64().unwrap() > 0);
        assert!(summary["downstream_events"]["captured"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn streaming_capture_writes_redacted_complete_artifacts() {
        let temp = TempDir::new().unwrap();
        let traffic = Arc::new(test_capture(temp.path().join("traffic")));
        let upstream = futures_util::stream::iter(vec![Ok(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"first\"}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"call_1\",\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n",
        ))]);
        let response = stream_body(
            upstream,
            "msg_1".into(),
            "grok-4.5".into(),
            None,
            "req_1".into(),
            Some(traffic),
            None,
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("tool_use"));
        let names: Vec<_> = std::fs::read_dir(temp.path().join("traffic"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            names
                .iter()
                .any(|name| name.contains("032-upstream-response-body.sse"))
        );
        assert!(
            names
                .iter()
                .any(|name| name.contains("061-grok-stream-summary"))
        );
    }

    #[tokio::test]
    async fn streaming_capture_records_fragmented_search_and_tool_events() {
        let temp = TempDir::new().unwrap();
        let traffic = Arc::new(test_capture(temp.path().join("traffic")));
        let upstream = futures_util::stream::iter(vec![
            Ok(Bytes::from_static(
                b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"fir",
            )),
            Ok(Bytes::from_static(
                b"st\"}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"custom_tool_call\",\"name\":\"x_search\",\"id\":\"search_1\"}}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"custom_tool_call\",\"name\":\"x_search\",\"id\":\"search_1\"}}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"call_1\",\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n",
            )),
        ]);
        let response = stream_body(
            upstream,
            "msg_1".into(),
            "grok-4.5".into(),
            None,
            "req_1".into(),
            Some(traffic),
            None,
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("tool_use"));
        let captured = capture_contents(temp.path().join("traffic"));
        assert!(captured.contains("x_search"));
        assert!(captured.contains("function_call"));
        assert!(captured.contains("stream_completion"));
    }

    #[tokio::test]
    async fn streaming_capture_records_malformed_and_failed_streams() {
        for (payload, stage) in [
            (b"data: {bad json}\n\n".as_slice(), "json"),
            (
                b"data: {\"type\":\"response.failed\",\"response\":{}}\n\n".as_slice(),
                "reducer",
            ),
        ] {
            let temp = TempDir::new().unwrap();
            let traffic = Arc::new(test_capture(temp.path().join("traffic")));
            let response = stream_body(
                futures_util::stream::iter(vec![Ok(Bytes::copy_from_slice(payload))]),
                "msg_1".into(),
                "grok-4.5".into(),
                None,
                "req_1".into(),
                Some(traffic),
                None,
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(String::from_utf8_lossy(&body).contains("event: error"));
            let captured = capture_contents(temp.path().join("traffic"));
            assert!(captured.contains(&format!("\"stage\": \"{stage}\"")));
            assert!(captured.contains("stream_error"));
        }
    }

    #[test]
    fn non_streaming_malformed_capture_keeps_diagnostics() {
        let temp = TempDir::new().unwrap();
        let traffic = test_capture(temp.path().join("traffic"));
        assert!(
            accumulate_response_with_traffic(
                b"data: {bad json}\n\n",
                "msg_1",
                "grok-4.5",
                Some(&traffic),
            )
            .is_err()
        );
        let captured = capture_contents(temp.path().join("traffic"));
        assert!(captured.contains("malformed_event"));
        assert!(captured.contains("\"outcome\": \"error\""));
    }

    #[test]
    fn transport_failure_capture_contains_no_credentials() {
        let temp = TempDir::new().unwrap();
        let traffic = test_capture(temp.path().join("traffic"));
        client::capture_terminal_failure(
            Some(&traffic),
            "transport",
            "transport",
            1,
            Some(502),
            Some("Grok upstream request failed"),
        );
        let captured = capture_contents(temp.path().join("traffic"));
        assert!(captured.contains("transport"));
        assert!(captured.contains("060-grok-stream-error") || captured.contains("\"kind\""));
        for secret in [
            "Bearer token",
            "refresh-secret",
            "oauth-code",
            "person@example.com",
            "test-access",
        ] {
            assert!(!captured.contains(secret));
        }
    }

    fn capture_contents(root: std::path::PathBuf) -> String {
        let mut captured = String::new();
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    captured.push_str(&std::fs::read_to_string(path).unwrap());
                }
            }
        }
        captured
    }

    fn capture_contents_matching(root: std::path::PathBuf, name_fragment: &str) -> String {
        let mut captured = String::new();
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(name_fragment))
                {
                    captured.push_str(&std::fs::read_to_string(path).unwrap());
                }
            }
        }
        captured
    }

    #[tokio::test]
    async fn stream_capture_keeps_reasoning_only_upstream() {
        let temp = TempDir::new().unwrap();
        let traffic = Arc::new(test_capture(temp.path().join("traffic")));
        let (tx, rx) = mpsc::channel(1);
        let upstream = "data: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"draft only 😊\"}\n\ndata: {\"type\":\"response.reasoning_text.done\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"final only 🚀\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        tx.send(Ok(Bytes::copy_from_slice(upstream.as_bytes())))
            .await
            .unwrap();
        drop(tx);

        let body = stream_body(
            ChannelStream(rx),
            "msg_capture".into(),
            "grok-4.5".into(),
            None,
            "req_capture".into(),
            Some(traffic),
            None,
        )
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains("draft only"));
        assert!(!body.contains('😊'));
        assert_eq!(body.matches("final only 🚀").count(), 1);

        let root = temp.path().join("traffic");
        let downstream = capture_contents_matching(root.clone(), "050-downstream-event");
        assert!(!downstream.contains("draft only"));
        assert!(!downstream.contains('😊'));
        assert_eq!(downstream.matches("final only 🚀").count(), 1);

        let upstream_capture = capture_contents_matching(root, "040-upstream-event");
        assert!(upstream_capture.contains("draft only"));
        assert!(upstream_capture.contains('😊'));
    }

    #[test]
    fn non_streaming_capture_writes_response_and_redacts_secrets() {
        let temp = TempDir::new().unwrap();
        let traffic = test_capture(temp.path().join("traffic"));
        let upstream = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\",\"access_token\":\"secret\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{}}\n\n";
        let value = accumulate_response_with_traffic(upstream, "msg_1", "grok-4.5", Some(&traffic))
            .unwrap();
        traffic.write_json("051-downstream-response", &value);
        let mut captured = String::new();
        for entry in std::fs::read_dir(temp.path().join("traffic")).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                captured.push_str(&std::fs::read_to_string(path).unwrap());
            }
        }
        assert!(captured.contains("[redacted len=6]"));
        assert!(!captured.contains("secret"));
    }

    #[tokio::test]
    async fn pre_emit_stream_reset_rebuilds_with_shared_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            // First attempt: 200 headers then reset before body.
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(stream);

            // Second attempt succeeds.
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            let body = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
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
        let client = Arc::new(client);
        let deadline = GrokRequestDeadline::after(Duration::from_secs(5));
        let retry = Arc::new(Mutex::new(GrokRetryState::with_deadline(deadline)));
        let response = client
            .post_with_retry(&sample_request(), None, retry.clone())
            .await
            .unwrap();
        let reconnect = Some(GrokReconnectContext {
            client: client.clone(),
            request: Arc::new(sample_request()),
            traffic: None,
            retry,
        });
        let body = tokio::time::timeout(
            Duration::from_secs(3),
            stream_body_with_policy(
                response.into_stream(),
                "msg_rebuild".into(),
                "grok-4.5".into(),
                None,
                "req_rebuild".into(),
                None,
                reconnect,
                deadline,
                Duration::from_millis(10),
            )
            .into_body()
            .collect(),
        )
        .await
        .expect("rebuild must remain bounded by the request deadline")
        .unwrap()
        .to_bytes();
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .expect("rebuild should issue the second request")
            .unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("message_start"));
        assert!(body.contains("ok"));
        assert!(body.contains("event: ping"));
        assert!(body.contains("message_stop"));
    }

    #[tokio::test]
    async fn reasoning_only_reset_rebuilds_without_replaying_visible_content() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let chunk = b"data: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"draft \\ud83d\\ude0a\"}\n\ndata: {\"type\":\"response.reasoning_text.done\"}\n\n";
            let header = format!("{:x}\r\n", chunk.len());
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(chunk).await.unwrap();
            stream.write_all(b"\r\n").await.unwrap();
            drop(stream);

            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            let body = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"final once\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
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
        let client = Arc::new(client);
        let retry = Arc::new(Mutex::new(GrokRetryState::new()));
        let response = client
            .post_with_retry(&sample_request(), None, retry.clone())
            .await
            .unwrap();
        let reconnect = Some(GrokReconnectContext {
            client,
            request: Arc::new(sample_request()),
            traffic: None,
            retry,
        });
        let body = stream_body(
            response.into_stream(),
            "msg_reasoning_rebuild".into(),
            "grok-4.5".into(),
            None,
            "req_reasoning_rebuild".into(),
            None,
            reconnect,
        )
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
        server.await.unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 2);
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains("draft"));
        assert!(!body.contains('😊'));
        assert_eq!(body.matches("final once").count(), 1);
        assert!(!body.contains("event: error"));
    }

    #[tokio::test]
    async fn post_emit_stream_reset_does_not_replay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            // First body chunk is a complete Anthropic-producing event.
            let chunk = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"first\"}\n\n";
            let header = format!("{:x}\r\n", chunk.len());
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(chunk).await.unwrap();
            stream.write_all(b"\r\n").await.unwrap();
            // Then reset.
            drop(stream);
            // If a second request arrives, count it (should not happen).
            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_millis(150), listener.accept()).await
            {
                let _ = read_http_request(&mut stream).await;
                hits_server.fetch_add(1, Ordering::SeqCst);
            }
        });

        let temp = TempDir::new().unwrap();
        let traffic = Arc::new(test_capture(temp.path().join("traffic")));
        let (client, _auth) = test_client(
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
        let response = client
            .post_with_retry(&sample_request(), Some(traffic.clone()), retry.clone())
            .await
            .unwrap();
        let reconnect = Some(GrokReconnectContext {
            client,
            request: Arc::new(sample_request()),
            traffic: Some(traffic.clone()),
            retry,
        });
        let body = stream_body(
            response.into_stream(),
            "msg_post".into(),
            "grok-4.5".into(),
            None,
            "req_post".into(),
            Some(traffic),
            reconnect,
        )
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
        let _ = server.await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("first"));
        assert!(body.contains("event: error"));
        let captured = capture_contents(temp.path().join("traffic"));
        assert!(captured.contains("stream_error") || captured.contains("downstream_emitted"));
    }

    #[tokio::test]
    async fn incomplete_socket_close_without_terminal_is_not_success() {
        let (tx, rx) = mpsc::channel(1);
        let response = stream_body(
            ChannelStream(rx),
            "msg_1".into(),
            "grok-4.5".into(),
            None,
            "req_1".into(),
            None,
            None,
        );
        let mut body = response.into_body();
        // No terminal event; just close.
        drop(tx);
        let frame = tokio::time::timeout(Duration::from_millis(250), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert!(String::from_utf8_lossy(&frame).contains("event: error"));
    }

    #[tokio::test]
    async fn malformed_json_is_not_retried_even_with_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_server = hits.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut stream).await;
            hits_server.fetch_add(1, Ordering::SeqCst);
            let body = b"data: {bad json}\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_millis(100), listener.accept()).await
            {
                let _ = read_http_request(&mut stream).await;
                hits_server.fetch_add(1, Ordering::SeqCst);
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
        let response = client
            .post_with_retry(&sample_request(), None, retry.clone())
            .await
            .unwrap();
        let reconnect = Some(GrokReconnectContext {
            client,
            request: Arc::new(sample_request()),
            traffic: None,
            retry,
        });
        let body = stream_body(
            response.into_stream(),
            "msg_bad".into(),
            "grok-4.5".into(),
            None,
            "req_bad".into(),
            None,
            reconnect,
        )
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
        let _ = server.await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8_lossy(&body).contains("event: error"));
    }

    #[test]
    fn stream_error_classifier_marks_transport_retryable() {
        let err = client::GrokError::stream_transport(GrokErrorStage::Stream, "reset");
        assert!(err.is_retryable());
        assert_eq!(err.origin, client::GrokErrorOrigin::StreamTransport);
        let bad = client::GrokError::http(StatusCode::BAD_REQUEST, None, "nope");
        assert!(!bad.is_retryable());
        let limit = client::GrokError::response_limit(GrokErrorStage::Body, "too large");
        assert!(!limit.is_retryable());
        assert_eq!(limit.origin, client::GrokErrorOrigin::ResponseLimit);
    }
}
