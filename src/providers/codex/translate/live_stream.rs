use std::collections::{HashMap, HashSet};

use crate::anthropic::sse::encode_sse_event;
use crate::traffic::TrafficCapture;

use super::read_rewrite::sanitize_read_args;
use super::reasoning_signature::{PendingReasoning, encode_reasoning_signature};
use super::reducer::{
    CodexUsage, STOP_END_TURN, STOP_TOOL_USE, map_codex_usage_to_anthropic,
    stop_reason_for_incomplete_response, validate_tool_arguments,
};

const BUFFERED_READ_REPAIR_TRAILING_WHITESPACE_BYTES: usize = 1_024;
const BUFFERED_TOOL_MAX_ARGS_BYTES: usize = 5_000_000;
const MAX_LIVE_TRANSLATOR_INPUT_BYTES: usize = 8 * 1024 * 1024;

enum LiveBlock {
    Text {
        index: usize,
        text: String,
        deferred: bool,
    },
    Tool {
        index: usize,
        call_id: String,
        name: String,
        args_accum: String,
        done_snapshot: Option<String>,
        emitted_args: bool,
    },
}

struct LiveWebSearch {
    output_index: usize,
    index: usize,
    result_index: usize,
    id: String,
    query: String,
    results: Vec<LiveWebSearchResult>,
}

#[derive(Clone)]
struct LiveWebSearchResult {
    title: String,
    url: String,
}

#[derive(Clone, Copy)]
struct LiveThinking {
    output_index: usize,
    anthropic_index: usize,
}

struct PreparedToolArguments {
    output_index: usize,
    index: usize,
    arguments: String,
    emit: bool,
}

pub struct LiveStreamTranslator {
    message_id: String,
    model: String,
    message_started: bool,
    blocks_by_output_index: HashMap<usize, LiveBlock>,
    item_id_to_output_index: HashMap<String, usize>,
    anthropic_index: usize,
    thinking: Option<LiveThinking>,
    reasoning_by_output_index: HashMap<usize, PendingReasoning>,
    saw_tool_use: bool,
    web_search_requests: usize,
    web_search_output_indices: HashSet<usize>,
    web_searches: Vec<LiveWebSearch>,
    web_search_results: Vec<LiveWebSearchResult>,
    deferred_text: Vec<(usize, String)>,
    upstream_input_bytes: usize,
    finished: bool,
}

impl LiveStreamTranslator {
    pub fn new(message_id: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            model: model.into(),
            message_started: false,
            blocks_by_output_index: HashMap::new(),
            item_id_to_output_index: HashMap::new(),
            anthropic_index: 0,
            thinking: None,
            reasoning_by_output_index: HashMap::new(),
            saw_tool_use: false,
            web_search_requests: 0,
            web_search_output_indices: HashSet::new(),
            web_searches: Vec::new(),
            web_search_results: Vec::new(),
            deferred_text: Vec::new(),
            upstream_input_bytes: 0,
            finished: false,
        }
    }

    pub fn accept(
        &mut self,
        payload: &serde_json::Value,
        traffic: Option<&TrafficCapture>,
    ) -> Result<Vec<u8>, String> {
        if self.finished {
            return Ok(Vec::new());
        }
        let input_bytes = serde_json::to_vec(payload)
            .map_err(|error| format!("Codex stream event could not be encoded: {error}"))?
            .len();
        if self.upstream_input_bytes.saturating_add(input_bytes) > MAX_LIVE_TRANSLATOR_INPUT_BYTES {
            return Err("Codex live response exceeded the cumulative size limit".to_string());
        }
        self.upstream_input_bytes += input_bytes;

        let kind = payload.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let mut out = Vec::new();

        match kind {
            "codex.rate_limits" => {
                if payload
                    .get("rate_limits")
                    .and_then(|r| r.get("limit_reached"))
                    .and_then(|v| v.as_bool())
                    == Some(true)
                {
                    return Err("rate limit reached".to_string());
                }
            }
            "keepalive" => {}
            "response.failed" | "response.error" | "error" => {
                return Err(error_message(payload));
            }
            "response.web_search_call.in_progress"
            | "response.web_search_call.searching"
            | "response.web_search_call.completed" => {}
            "response.output_item.added" => {
                self.output_item_added(payload, traffic, &mut out);
            }
            "response.reasoning_summary_part.added" => {
                let output_index = output_index(payload);
                if let Some(thinking) = self
                    .thinking
                    .filter(|thinking| thinking.output_index == output_index)
                {
                    self.emit(
                        traffic,
                        &mut out,
                        "content_block_delta",
                        &serde_json::json!({
                            "type": "content_block_delta",
                            "index": thinking.anthropic_index,
                            "delta": {"type": "thinking_delta", "thinking": "\n\n"}
                        }),
                    );
                }
            }
            "response.reasoning_summary_text.delta" => {
                self.reasoning_delta(payload, traffic, &mut out);
            }
            "response.output_text.delta" => {
                self.text_delta(payload, traffic, &mut out);
            }
            "response.output_text.annotation.added" => {
                self.web_search_annotation(payload);
            }
            "response.function_call_arguments.delta" => {
                self.tool_delta(payload, traffic, &mut out)?;
            }
            "response.function_call_arguments.done" => {
                self.tool_arguments_done(payload)?;
            }
            "response.output_item.done" => {
                self.output_item_done(payload, traffic, &mut out)?;
            }
            "response.completed" | "response.incomplete" | "response.done" => {
                self.finish(payload, traffic, &mut out)?;
            }
            _ => {}
        }

        Ok(out)
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn finish_after_closed_completed_tool_call(
        &mut self,
        traffic: Option<&TrafficCapture>,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        if self.finished || !self.saw_tool_use {
            return out;
        }

        // A flaky transport can close after the model has sent a complete JSON
        // function call but before `response.output_item.done` or the terminal
        // response event. The tool block has already crossed the downstream
        // replay boundary, so retrying the whole request could duplicate a tool
        // invocation. Validate every remaining block transactionally and, when
        // they are all complete tools, finish the Anthropic stream instead.
        let mut prepared = Vec::with_capacity(self.blocks_by_output_index.len());
        for output_index in self.blocks_by_output_index.keys().copied() {
            let Ok(Some(arguments)) = self.prepare_tool_arguments(output_index, None) else {
                return out;
            };
            prepared.push(arguments);
        }
        prepared.sort_by_key(|arguments| arguments.index);

        self.close_thinking(traffic, &mut out);
        self.ensure_message_start(traffic, &mut out);
        for arguments in prepared {
            let output_index = arguments.output_index;
            if let Some((index, partial_json)) = self.commit_tool_arguments(arguments) {
                self.emit(
                    traffic,
                    &mut out,
                    "content_block_delta",
                    &serde_json::json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": partial_json
                        }
                    }),
                );
            }
            let Some(LiveBlock::Tool { index, .. }) =
                self.blocks_by_output_index.remove(&output_index)
            else {
                return Vec::new();
            };
            self.emit(
                traffic,
                &mut out,
                "content_block_stop",
                &serde_json::json!({
                    "type": "content_block_stop",
                    "index": index,
                }),
            );
        }
        self.emit_finish(STOP_TOOL_USE, None, traffic, &mut out);
        out
    }

    pub fn error_chunk(
        &mut self,
        message: &str,
        error_type: &str,
        traffic: Option<&TrafficCapture>,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        if self.finished {
            return out;
        }
        self.close_open_blocks(traffic, &mut out);
        self.ensure_message_start(traffic, &mut out);
        self.emit(
            traffic,
            &mut out,
            "error",
            &serde_json::json!({
                "type": "error",
                "error": {
                    "type": error_type,
                    "message": message,
                }
            }),
        );
        self.finished = true;
        out
    }

    fn ensure_message_start(&mut self, traffic: Option<&TrafficCapture>, out: &mut Vec<u8>) {
        if self.message_started {
            return;
        }
        self.message_started = true;
        self.emit(
            traffic,
            out,
            "message_start",
            &serde_json::json!({
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": 0,
                        "output_tokens": 0
                    }
                }
            }),
        );
    }

    fn emit(
        &self,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
        event: &str,
        data: &serde_json::Value,
    ) {
        if let Some(traffic) = traffic {
            traffic.write_json_event(
                "050-downstream-event",
                &serde_json::json!({
                    "event": event,
                    "data": data,
                }),
            );
        }
        out.extend_from_slice(&encode_sse_event(Some(event), &data.to_string()));
    }

    fn output_item_added(
        &mut self,
        payload: &serde_json::Value,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) {
        let Some(item) = payload.get("item") else {
            return;
        };
        let output_index = output_index(payload);
        let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match item_type {
            "reasoning" => {
                self.reasoning_by_output_index
                    .entry(output_index)
                    .or_default()
                    .capture(item);
            }
            "message" => {
                self.close_thinking(traffic, out);
                let index = self.anthropic_index;
                self.anthropic_index += 1;
                if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                    self.item_id_to_output_index
                        .insert(id.to_string(), output_index);
                }
                let deferred = !self.web_searches.is_empty();
                self.blocks_by_output_index.insert(
                    output_index,
                    LiveBlock::Text {
                        index,
                        text: String::new(),
                        deferred,
                    },
                );
                if !deferred {
                    self.ensure_message_start(traffic, out);
                    self.emit(
                        traffic,
                        out,
                        "content_block_start",
                        &serde_json::json!({
                            "type": "content_block_start",
                            "index": index,
                            "content_block": {"type": "text", "text": "", "citations": null}
                        }),
                    );
                }
            }
            "function_call" => {
                self.close_thinking(traffic, out);
                self.saw_tool_use = true;
                let index = self.anthropic_index;
                self.anthropic_index += 1;
                let call_id = item
                    .get("call_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = item
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Some(id) = item.get("id").and_then(|value| value.as_str()) {
                    self.item_id_to_output_index
                        .insert(id.to_string(), output_index);
                }
                self.blocks_by_output_index.insert(
                    output_index,
                    LiveBlock::Tool {
                        index,
                        call_id: call_id.clone(),
                        name: name.clone(),
                        args_accum: String::new(),
                        done_snapshot: None,
                        emitted_args: false,
                    },
                );
                self.ensure_message_start(traffic, out);
                self.emit(
                    traffic,
                    out,
                    "content_block_start",
                    &serde_json::json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {
                            "type": "tool_use",
                            "id": call_id,
                            "name": name,
                            "input": {},
                            "caller": {"type": "direct"}
                        }
                    }),
                );
            }
            "web_search_call" => {
                if let Some(id) = item.get("id").and_then(|value| value.as_str()) {
                    self.item_id_to_output_index
                        .insert(id.to_string(), output_index);
                }
                if self.web_search_output_indices.insert(output_index) {
                    self.web_search_requests += 1;
                }
            }
            _ => {}
        }
    }

    fn reasoning_delta(
        &mut self,
        payload: &serde_json::Value,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) {
        let output_index = output_index(payload);
        let delta = payload.get("delta").and_then(|v| v.as_str()).unwrap_or("");
        if delta.is_empty() {
            return;
        }
        if self.thinking.map(|thinking| thinking.output_index) != Some(output_index) {
            self.close_thinking(traffic, out);
            let index = self.anthropic_index;
            self.anthropic_index += 1;
            self.thinking = Some(LiveThinking {
                output_index,
                anthropic_index: index,
            });
            self.ensure_message_start(traffic, out);
            self.emit(
                traffic,
                out,
                "content_block_start",
                &serde_json::json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "thinking", "thinking": "", "signature": ""}
                }),
            );
        }
        let index = self
            .thinking
            .expect("thinking block was started")
            .anthropic_index;
        self.emit(
            traffic,
            out,
            "content_block_delta",
            &serde_json::json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {"type": "thinking_delta", "thinking": delta}
            }),
        );
    }

    fn text_delta(
        &mut self,
        payload: &serde_json::Value,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) {
        self.close_thinking(traffic, out);
        let delta = payload.get("delta").and_then(|v| v.as_str()).unwrap_or("");
        if delta.is_empty() {
            return;
        }

        let output_index = payload
            .get("output_index")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .or_else(|| {
                payload
                    .get("item_id")
                    .and_then(|v| v.as_str())
                    .and_then(|id| self.item_id_to_output_index.get(id).copied())
            })
            .unwrap_or(0);

        if !self.blocks_by_output_index.contains_key(&output_index) {
            let index = self.anthropic_index;
            self.anthropic_index += 1;
            let deferred = !self.web_searches.is_empty();
            self.blocks_by_output_index.insert(
                output_index,
                LiveBlock::Text {
                    index,
                    text: String::new(),
                    deferred,
                },
            );
            if !deferred {
                self.ensure_message_start(traffic, out);
                self.emit(
                    traffic,
                    out,
                    "content_block_start",
                    &serde_json::json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {"type": "text", "text": "", "citations": null}
                    }),
                );
            }
        }

        let Some(LiveBlock::Text {
            index,
            text,
            deferred,
        }) = self.blocks_by_output_index.get_mut(&output_index)
        else {
            return;
        };
        text.push_str(delta);
        if *deferred {
            return;
        }
        let index = *index;
        self.emit(
            traffic,
            out,
            "content_block_delta",
            &serde_json::json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {"type": "text_delta", "text": delta}
            }),
        );
    }

    fn tool_delta(
        &mut self,
        payload: &serde_json::Value,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        let Some(output_index) = self.resolve_output_index(payload) else {
            return Ok(());
        };
        let delta = payload.get("delta").and_then(|v| v.as_str()).unwrap_or("");
        if delta.is_empty() {
            return Ok(());
        }
        let mut repaired_read: Option<(usize, String)> = None;
        let Some(LiveBlock::Tool {
            index,
            call_id,
            name,
            args_accum,
            done_snapshot,
            ..
        }) = self.blocks_by_output_index.get_mut(&output_index)
        else {
            return Ok(());
        };

        // `response.function_call_arguments.done` is the authoritative snapshot.
        // A late/replayed delta must never mutate arguments after that boundary.
        if done_snapshot.is_some() {
            return Ok(());
        }
        args_accum.push_str(delta);
        if args_accum.len() > BUFFERED_TOOL_MAX_ARGS_BYTES {
            return Err(format!(
                "Buffered {name} tool arguments exceeded safe limits"
            ));
        }
        if let Some(repaired) =
            repair_whitespace_stalled_read_args(name, args_accum, Some(call_id.as_str()))
        {
            repaired_read = Some((*index, repaired));
        }

        if let Some((index, repaired)) = repaired_read {
            self.blocks_by_output_index.remove(&output_index);
            self.emit(
                traffic,
                out,
                "content_block_delta",
                &serde_json::json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": repaired
                    }
                }),
            );
            self.emit(
                traffic,
                out,
                "content_block_stop",
                &serde_json::json!({
                    "type": "content_block_stop",
                    "index": index,
                }),
            );
        }
        Ok(())
    }

    fn tool_arguments_done(&mut self, payload: &serde_json::Value) -> Result<(), String> {
        let Some(output_index) = self.resolve_output_index(payload) else {
            return Ok(());
        };
        let Some(args) = payload.get("arguments").and_then(|v| v.as_str()) else {
            return Ok(());
        };
        let Some(LiveBlock::Tool {
            name,
            done_snapshot,
            ..
        }) = self.blocks_by_output_index.get_mut(&output_index)
        else {
            return Ok(());
        };
        if args.len() > BUFFERED_TOOL_MAX_ARGS_BYTES {
            return Err(format!(
                "Buffered {name} tool arguments exceeded safe limits"
            ));
        }
        *done_snapshot = Some(args.to_string());
        Ok(())
    }

    fn output_item_done(
        &mut self,
        payload: &serde_json::Value,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        let Some(output_index) = self.resolve_output_index(payload) else {
            return Ok(());
        };
        if let Some(item) = payload
            .get("item")
            .and_then(|item| item.get("type"))
            .and_then(|v| v.as_str())
            .filter(|item_type| *item_type == "reasoning")
            .and_then(|_| payload.get("item"))
        {
            self.reasoning_by_output_index
                .entry(output_index)
                .or_default()
                .capture(item);
            let had_active_summary = self
                .thinking
                .is_some_and(|thinking| thinking.output_index == output_index);
            self.close_thinking(traffic, out);
            if !had_active_summary {
                self.emit_signature_only_reasoning(output_index, traffic, out);
            }
            return Ok(());
        }

        if payload
            .get("item")
            .and_then(|item| item.get("type"))
            .and_then(|v| v.as_str())
            == Some("web_search_call")
        {
            self.close_thinking(traffic, out);
            let item = &payload["item"];
            let raw_id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
            if self.web_search_output_indices.insert(output_index) {
                self.web_search_requests += 1;
            }
            if self
                .web_searches
                .iter()
                .any(|search| search.output_index == output_index)
            {
                return Ok(());
            }
            let index = self.anthropic_index;
            self.anthropic_index += 1;
            let result_index = self.anthropic_index;
            self.anthropic_index += 1;
            self.web_searches.push(LiveWebSearch {
                output_index,
                index,
                result_index,
                id: super::web_search_compat::server_tool_use_id_from_codex_web_search_id(raw_id),
                query: web_search_query(item),
                results: web_search_sources(item),
            });
            return Ok(());
        }

        let final_args = payload
            .get("item")
            .and_then(|item| item.get("arguments"))
            .and_then(|value| value.as_str());
        let prepared = self.prepare_tool_arguments(output_index, final_args)?;
        if let Some((index, arguments)) =
            prepared.and_then(|prepared| self.commit_tool_arguments(prepared))
        {
            self.emit(
                traffic,
                out,
                "content_block_delta",
                &serde_json::json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": arguments
                    }
                }),
            );
        }

        let Some(mut state) = self.blocks_by_output_index.remove(&output_index) else {
            return Ok(());
        };

        match &mut state {
            LiveBlock::Text {
                index,
                text,
                deferred,
            } => {
                if *deferred {
                    self.deferred_text.push((*index, std::mem::take(text)));
                } else {
                    self.emit(
                        traffic,
                        out,
                        "content_block_stop",
                        &serde_json::json!({
                            "type": "content_block_stop",
                            "index": index,
                        }),
                    );
                }
            }
            LiveBlock::Tool { index, .. } => {
                self.emit(
                    traffic,
                    out,
                    "content_block_stop",
                    &serde_json::json!({
                        "type": "content_block_stop",
                        "index": index,
                    }),
                );
            }
        }
        Ok(())
    }

    fn prepare_tool_arguments(
        &self,
        output_index: usize,
        final_args: Option<&str>,
    ) -> Result<Option<PreparedToolArguments>, String> {
        let Some(LiveBlock::Tool {
            index,
            name,
            call_id,
            args_accum,
            done_snapshot,
            emitted_args,
            ..
        }) = self.blocks_by_output_index.get(&output_index)
        else {
            return Ok(None);
        };

        // Prefer the explicit arguments-done snapshot, then the completed item
        // snapshot, and only fall back to accumulated deltas for truncated streams.
        let mut arguments = done_snapshot
            .as_deref()
            .or(final_args)
            .unwrap_or(args_accum)
            .to_string();
        if !arguments.is_empty() {
            arguments = sanitize_read_args(name, &arguments, Some(call_id.as_str()));
        }
        validate_tool_arguments(name, call_id, &arguments)?;

        Ok(Some(PreparedToolArguments {
            output_index,
            index: *index,
            arguments,
            emit: !*emitted_args,
        }))
    }

    fn resolve_output_index(&self, payload: &serde_json::Value) -> Option<usize> {
        payload
            .get("output_index")
            .and_then(|value| value.as_u64())
            .map(|value| value as usize)
            .or_else(|| {
                payload
                    .get("item_id")
                    .and_then(|value| value.as_str())
                    .or_else(|| {
                        payload
                            .get("item")
                            .and_then(|item| item.get("id"))
                            .and_then(|value| value.as_str())
                    })
                    .and_then(|id| self.item_id_to_output_index.get(id).copied())
            })
            .or_else(|| {
                let call_id = payload
                    .get("call_id")
                    .and_then(|value| value.as_str())
                    .or_else(|| {
                        payload
                            .get("item")
                            .and_then(|item| item.get("call_id"))
                            .and_then(|value| value.as_str())
                    })?;
                self.blocks_by_output_index
                    .iter()
                    .find_map(|(output_index, block)| match block {
                        LiveBlock::Tool {
                            call_id: candidate, ..
                        } if candidate == call_id => Some(*output_index),
                        _ => None,
                    })
            })
    }

    fn commit_tool_arguments(
        &mut self,
        prepared: PreparedToolArguments,
    ) -> Option<(usize, String)> {
        let LiveBlock::Tool {
            args_accum,
            emitted_args,
            ..
        } = self
            .blocks_by_output_index
            .get_mut(&prepared.output_index)?
        else {
            return None;
        };
        *args_accum = prepared.arguments.clone();
        if prepared.emit {
            *emitted_args = true;
            Some((prepared.index, prepared.arguments))
        } else {
            None
        }
    }

    fn web_search_annotation(&mut self, payload: &serde_json::Value) {
        let Some(annotation) = payload.get("annotation") else {
            return;
        };
        if annotation.get("type").and_then(|v| v.as_str()) != Some("url_citation") {
            return;
        }
        let Some(url) = annotation.get("url").and_then(|v| v.as_str()) else {
            return;
        };
        if self
            .web_search_results
            .iter()
            .any(|result| result.url == url)
        {
            return;
        }
        let title = annotation
            .get("title")
            .and_then(|v| v.as_str())
            .filter(|title| !title.is_empty())
            .unwrap_or(url);
        self.web_search_results.push(LiveWebSearchResult {
            title: title.to_string(),
            url: url.to_string(),
        });
    }

    fn emit_web_searches(&mut self, traffic: Option<&TrafficCapture>, out: &mut Vec<u8>) {
        let searches = std::mem::take(&mut self.web_searches);
        let allow_global_annotation_fallback = searches.len() == 1;
        for search in searches {
            self.ensure_message_start(traffic, out);
            self.emit(
                traffic,
                out,
                "content_block_start",
                &serde_json::json!({
                    "type": "content_block_start",
                    "index": search.index,
                    "content_block": {
                        "type": "server_tool_use",
                        "id": search.id,
                        "name": "web_search",
                        "input": {},
                        "caller": {"type":"direct"}
                    }
                }),
            );
            self.emit(
                traffic,
                out,
                "content_block_delta",
                &serde_json::json!({
                    "type": "content_block_delta",
                    "index": search.index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": serde_json::to_string(&serde_json::json!({"query": search.query})).unwrap_or_default()
                    }
                }),
            );
            self.emit(
                traffic,
                out,
                "content_block_stop",
                &serde_json::json!({"type": "content_block_stop", "index": search.index}),
            );
            let search_results = if search.results.is_empty() && allow_global_annotation_fallback {
                &self.web_search_results
            } else {
                &search.results
            };
            let results: Vec<_> = search_results
                .iter()
                .map(|result| {
                    serde_json::json!({
                        "type": "web_search_result",
                        "title": result.title,
                        "url": result.url,
                        "encrypted_content": "",
                        "page_age": null,
                    })
                })
                .collect();
            self.emit(
                traffic,
                out,
                "content_block_start",
                &serde_json::json!({
                    "type": "content_block_start",
                    "index": search.result_index,
                    "content_block": {
                        "type": "web_search_tool_result",
                        "tool_use_id": search.id,
                        "content": results,
                        "caller": {"type":"direct"}
                    }
                }),
            );
            self.emit(
                traffic,
                out,
                "content_block_stop",
                &serde_json::json!({"type": "content_block_stop", "index": search.result_index}),
            );
        }

        for (index, text) in std::mem::take(&mut self.deferred_text) {
            self.emit(
                traffic,
                out,
                "content_block_start",
                &serde_json::json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "text", "text": "", "citations": null}
                }),
            );
            if !text.is_empty() {
                self.emit(
                    traffic,
                    out,
                    "content_block_delta",
                    &serde_json::json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {"type": "text_delta", "text": text}
                    }),
                );
            }
            self.emit(
                traffic,
                out,
                "content_block_stop",
                &serde_json::json!({"type": "content_block_stop", "index": index}),
            );
        }
    }

    fn finish(
        &mut self,
        payload: &serde_json::Value,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        let mut output_indices: Vec<_> = self.blocks_by_output_index.keys().copied().collect();
        output_indices.sort_unstable();
        let mut prepared_tools = Vec::new();
        for output_index in output_indices {
            if let Some(prepared) = self.prepare_tool_arguments(output_index, None)? {
                prepared_tools.push(prepared);
            }
        }
        prepared_tools.sort_by_key(|prepared| prepared.index);
        for prepared in prepared_tools {
            if let Some((index, arguments)) = self.commit_tool_arguments(prepared) {
                self.emit(
                    traffic,
                    out,
                    "content_block_delta",
                    &serde_json::json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": arguments
                        }
                    }),
                );
            }
        }
        self.close_thinking(traffic, out);
        self.close_open_blocks(traffic, out);
        self.emit_web_searches(traffic, out);
        self.ensure_message_start(traffic, out);
        let usage = payload.get("response").map(parse_codex_usage);
        let event_type = payload
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let stop_reason =
            if let Some(reason) = stop_reason_for_incomplete_response(payload, event_type) {
                reason
            } else if self.saw_tool_use {
                STOP_TOOL_USE
            } else {
                STOP_END_TURN
            };
        self.emit_finish(stop_reason, usage, traffic, out);
        Ok(())
    }

    fn emit_finish(
        &mut self,
        stop_reason: &str,
        usage: Option<CodexUsage>,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) {
        let mapped = map_codex_usage_to_anthropic(&usage, Some(self.web_search_requests));
        self.emit(
            traffic,
            out,
            "message_delta",
            &serde_json::json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": stop_reason,
                    "stop_sequence": null
                },
                "usage": mapped,
            }),
        );
        self.emit(
            traffic,
            out,
            "message_stop",
            &serde_json::json!({"type": "message_stop"}),
        );
        self.finished = true;
    }

    fn close_open_blocks(&mut self, traffic: Option<&TrafficCapture>, out: &mut Vec<u8>) {
        self.close_thinking(traffic, out);
        let mut open: Vec<_> = self
            .blocks_by_output_index
            .iter()
            .map(|(output_index, state)| {
                let index = match state {
                    LiveBlock::Text { index, .. } | LiveBlock::Tool { index, .. } => *index,
                };
                (*output_index, index)
            })
            .collect();
        open.sort_by_key(|(_, index)| *index);
        for (output_index, _) in open {
            let Some(state) = self.blocks_by_output_index.remove(&output_index) else {
                continue;
            };
            let index = match state {
                LiveBlock::Text {
                    index,
                    text,
                    deferred: true,
                } => {
                    self.deferred_text.push((index, text));
                    continue;
                }
                LiveBlock::Text { index, .. } => index,
                LiveBlock::Tool { index, .. } => index,
            };
            self.emit(
                traffic,
                out,
                "content_block_stop",
                &serde_json::json!({
                    "type": "content_block_stop",
                    "index": index,
                }),
            );
        }
    }

    fn emit_signature_only_reasoning(
        &mut self,
        output_index: usize,
        traffic: Option<&TrafficCapture>,
        out: &mut Vec<u8>,
    ) {
        let Some(replay) = self
            .reasoning_by_output_index
            .remove(&output_index)
            .and_then(|pending| pending.replay())
        else {
            return;
        };
        let Some(signature) = encode_reasoning_signature(&replay) else {
            return;
        };
        let index = self.anthropic_index;
        self.anthropic_index += 1;
        self.ensure_message_start(traffic, out);
        self.emit(
            traffic,
            out,
            "content_block_start",
            &serde_json::json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {"type": "thinking", "thinking": "", "signature": ""}
            }),
        );
        self.emit(
            traffic,
            out,
            "content_block_delta",
            &serde_json::json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {"type": "signature_delta", "signature": signature}
            }),
        );
        self.emit(
            traffic,
            out,
            "content_block_stop",
            &serde_json::json!({
                "type": "content_block_stop",
                "index": index,
            }),
        );
    }

    fn close_thinking(&mut self, traffic: Option<&TrafficCapture>, out: &mut Vec<u8>) {
        let Some(thinking) = self.thinking.take() else {
            return;
        };
        if let Some(signature) = self
            .reasoning_by_output_index
            .remove(&thinking.output_index)
            .and_then(|pending| pending.replay())
            .and_then(|replay| encode_reasoning_signature(&replay))
        {
            self.emit(
                traffic,
                out,
                "content_block_delta",
                &serde_json::json!({
                    "type": "content_block_delta",
                    "index": thinking.anthropic_index,
                    "delta": {"type": "signature_delta", "signature": signature}
                }),
            );
        }
        self.emit(
            traffic,
            out,
            "content_block_stop",
            &serde_json::json!({
                "type": "content_block_stop",
                "index": thinking.anthropic_index,
            }),
        );
    }
}

fn web_search_query(item: &serde_json::Value) -> String {
    let Some(action) = item.get("action") else {
        return String::new();
    };
    if let Some(query) = action.get("query").and_then(|v| v.as_str()) {
        return query.to_string();
    }
    action
        .get("queries")
        .and_then(|v| v.as_array())
        .map(|queries| {
            queries
                .iter()
                .filter_map(|query| query.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn web_search_sources(item: &serde_json::Value) -> Vec<LiveWebSearchResult> {
    let Some(sources) = item
        .get("action")
        .and_then(|action| action.get("sources"))
        .and_then(|sources| sources.as_array())
    else {
        return Vec::new();
    };

    let mut results = Vec::new();
    for source in sources {
        let Some(url) = source.get("url").and_then(|value| value.as_str()) else {
            continue;
        };
        if results
            .iter()
            .any(|result: &LiveWebSearchResult| result.url == url)
        {
            continue;
        }
        let title = source
            .get("title")
            .and_then(|value| value.as_str())
            .filter(|title| !title.is_empty())
            .unwrap_or(url);
        results.push(LiveWebSearchResult {
            title: title.to_string(),
            url: url.to_string(),
        });
    }
    results
}

fn output_index(payload: &serde_json::Value) -> usize {
    payload
        .get("output_index")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize
}

fn parse_codex_usage(response: &serde_json::Value) -> CodexUsage {
    let usage = match response.get("usage") {
        Some(u) => u,
        None => return CodexUsage::default(),
    };
    CodexUsage {
        input_tokens: usage.get("input_tokens").and_then(|v| v.as_u64()),
        output_tokens: usage.get("output_tokens").and_then(|v| v.as_u64()),
        input_tokens_details_cached: usage
            .get("input_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(|v| v.as_u64()),
        output_tokens_details_reasoning: usage
            .get("output_tokens_details")
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(|v| v.as_u64()),
    }
}

fn repair_whitespace_stalled_read_args(
    name: &str,
    args: &str,
    call_id: Option<&str>,
) -> Option<String> {
    if name != "Read" {
        return None;
    }
    let trimmed = args.trim_end();
    let trailing_whitespace = args.len().saturating_sub(trimmed.len());
    if trailing_whitespace < BUFFERED_READ_REPAIR_TRAILING_WHITESPACE_BYTES {
        return None;
    }
    parse_read_args_candidate(trimmed, call_id).or_else(|| {
        let with_brace = format!("{trimmed}}}");
        parse_read_args_candidate(&with_brace, call_id)
    })
}

fn parse_read_args_candidate(args: &str, call_id: Option<&str>) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(args).ok()?;
    if !is_valid_read_args(&parsed) {
        return None;
    }
    Some(sanitize_read_args(
        "Read",
        &serde_json::to_string(&parsed).ok()?,
        call_id,
    ))
}

fn is_valid_read_args(value: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    for key in obj.keys() {
        if !matches!(key.as_str(), "file_path" | "offset" | "limit" | "pages") {
            return false;
        }
    }
    let Some(file_path) = obj.get("file_path").and_then(|v| v.as_str()) else {
        return false;
    };
    if file_path.is_empty() {
        return false;
    }
    if let Some(offset) = obj.get("offset").and_then(|v| v.as_i64())
        && offset < 0
    {
        return false;
    }
    if let Some(limit) = obj.get("limit").and_then(|v| v.as_i64())
        && limit <= 0
    {
        return false;
    }
    if obj.get("offset").is_some_and(|v| !v.is_i64()) {
        return false;
    }
    if obj.get("limit").is_some_and(|v| !v.is_i64()) {
        return false;
    }
    if obj.get("pages").is_some_and(|v| !v.is_string()) {
        return false;
    }
    true
}

fn error_message(payload: &serde_json::Value) -> String {
    payload
        .get("response")
        .and_then(|r| r.get("error"))
        .and_then(|e| e.get("message"))
        .and_then(|v| v.as_str())
        .or_else(|| {
            payload
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("Upstream error")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn render(events: Vec<serde_json::Value>) -> String {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.5");
        let mut out = Vec::new();
        for event in events {
            out.extend(translator.accept(&event, None).unwrap());
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn emits_text_delta_before_terminal_event() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.5");
        let out = translator
            .accept(
                &json!({
                    "type": "response.output_text.delta",
                    "output_index": 0,
                    "delta": "hello"
                }),
                None,
            )
            .unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("message_start"));
        assert!(out.contains("content_block_start"));
        assert!(out.contains("text_delta"));
        assert!(out.contains("hello"));
        assert!(!out.contains("message_stop"));
    }

    #[test]
    fn finishes_text_stream() {
        let out = render(vec![
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "message", "id": "msg_up"}
            }),
            json!({
                "type": "response.output_text.delta",
                "output_index": 0,
                "delta": "hello"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "message"}
            }),
            json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "status": "completed", "incomplete_details": null, "usage": {"input_tokens": 2, "output_tokens": 1}}
            }),
        ]);
        assert!(out.contains("content_block_stop"));
        assert!(out.contains("message_delta"));
        assert!(out.contains(r#""stop_reason":"end_turn""#));
        assert!(out.contains("message_stop"));
    }

    #[test]
    fn completed_response_with_null_incomplete_details_is_end_turn() {
        let out = render(vec![json!({
            "type": "response.completed",
            "response": {"id": "resp_1", "status": "completed", "incomplete_details": null, "usage": {}}
        })]);
        assert!(out.contains(r#""stop_reason":"end_turn""#));
        assert!(!out.contains(r#""stop_reason":"max_tokens""#));
    }

    #[test]
    fn content_filter_incomplete_is_refusal() {
        let out = render(vec![json!({
            "type": "response.incomplete",
            "response": {
                "id": "resp_1",
                "status": "incomplete",
                "incomplete_details": {"reason": "content_filter"},
                "usage": {}
            }
        })]);
        assert!(out.contains(r#""stop_reason":"refusal""#));
        assert!(!out.contains(r#""stop_reason":"max_tokens""#));
    }

    #[test]
    fn buffers_read_tool_args_until_done() {
        let out = render(vec![
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Read"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": "{\"file_path\":\"/tmp/a\",\"pages\":\"\"}"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "Read",
                    "arguments": "{\"file_path\":\"/tmp/a\",\"pages\":\"\"}"
                }
            }),
            json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ]);
        assert!(out.contains("tool_use"));
        assert!(out.contains("input_json_delta"));
        assert!(out.contains("/tmp/a"));
        assert!(!out.contains("pages"));
    }

    #[test]
    fn done_snapshot_overrides_deltas_item_snapshot_and_late_delta() {
        let out = render(vec![
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "id":"item_1", "call_id": "call_1", "name": "Bash"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "item_1",
                "delta": "{\"command\":\"from delta\"}"
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "call_id": "call_1",
                "arguments": "{\"command\":\"from done\"}"
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "item_1",
                "delta": "{late garbage"
            }),
            json!({
                "type": "response.output_item.done",
                "item": {
                    "type": "function_call",
                    "id": "item_1",
                    "call_id": "call_1",
                    "name": "Bash",
                    "arguments": "{\"command\":\"from item\"}"
                }
            }),
            json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ]);

        assert!(out.contains(r#""partial_json":"{\"command\":\"from done\"}""#));
        assert!(!out.contains("from delta"));
        assert!(!out.contains("from item"));
        assert!(!out.contains("late garbage"));
        assert_eq!(out.matches(r#""type":"input_json_delta""#).count(), 1);
    }

    #[test]
    fn malformed_done_snapshot_is_rejected_before_arguments_are_emitted() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        let mut rendered = String::new();
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Bash"}
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 0,
                "arguments": "{\"command\":"
            }),
        ] {
            rendered
                .push_str(&String::from_utf8(translator.accept(&event, None).unwrap()).unwrap());
        }

        let error = translator
            .accept(
                &json!({
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "Bash",
                        "arguments": "{\"command\":\"valid but non-authoritative\"}"
                    }
                }),
                None,
            )
            .unwrap_err();

        assert!(error.contains("invalid JSON arguments"));
        assert!(!rendered.contains("input_json_delta"));
        assert!(!translator.is_finished());
    }

    #[test]
    fn terminal_event_rejects_tool_call_without_json_arguments() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        translator
            .accept(
                &json!({
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {"type": "function_call", "call_id": "call_1", "name": "Read"}
                }),
                None,
            )
            .unwrap();

        let error = translator
            .accept(
                &json!({
                    "type": "response.completed",
                    "response": {"id": "resp_1", "usage": {}}
                }),
                None,
            )
            .unwrap_err();

        assert!(error.contains("completed without JSON arguments"));
        assert!(!translator.is_finished());
        let terminal =
            String::from_utf8(translator.error_chunk(&error, "api_error", None)).unwrap();
        assert!(terminal.contains("event: error"));
        assert!(!terminal.contains("message_stop"));
    }

    #[test]
    fn output_item_done_rejects_malformed_tool_json() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Bash"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": "{\"command\":"
            }),
        ] {
            translator.accept(&event, None).unwrap();
        }

        let error = translator
            .accept(
                &json!({
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "Bash",
                        "arguments": "{\"command\":"
                    }
                }),
                None,
            )
            .unwrap_err();

        assert!(error.contains("invalid JSON arguments"));
        assert!(!translator.is_finished());
    }

    #[test]
    fn terminal_event_emits_valid_buffered_tool_arguments_before_success() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        let mut out = Vec::new();
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Read"}
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 0,
                "arguments": "{\"file_path\":\"/tmp/a\"}"
            }),
            json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ] {
            out.extend(translator.accept(&event, None).unwrap());
        }

        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains(r#""partial_json":"{\"file_path\":\"/tmp/a\"}""#));
        assert!(rendered.contains(r#""stop_reason":"tool_use""#));
        assert!(rendered.contains("message_stop"));
        assert!(translator.is_finished());
    }

    #[test]
    fn terminal_tool_argument_completion_commits_all_tools_transactionally() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "call_a", "name": "Read"}
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 0,
                "arguments": "{\"file_path\":\"/tmp/a\"}"
            }),
            json!({
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {"type": "function_call", "call_id": "call_b", "name": "Read"}
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 1,
                "arguments": "{\"file_path\":"
            }),
        ] {
            translator.accept(&event, None).unwrap();
        }

        let error = translator
            .accept(
                &json!({
                    "type": "response.completed",
                    "response": {"id": "resp_bad", "usage": {}}
                }),
                None,
            )
            .unwrap_err();
        assert!(error.contains("invalid JSON arguments"));

        for output_index in [0, 1] {
            let Some(LiveBlock::Tool { emitted_args, .. }) =
                translator.blocks_by_output_index.get(&output_index)
            else {
                panic!("tool {output_index} should remain pending after a failed transaction");
            };
            assert!(
                !emitted_args,
                "no tool may be marked emitted when the terminal transaction fails"
            );
        }

        translator
            .accept(
                &json!({
                    "type": "response.function_call_arguments.done",
                    "output_index": 1,
                    "arguments": "{\"file_path\":\"/tmp/b\"}"
                }),
                None,
            )
            .unwrap();
        let repaired_b = translator
            .accept(
                &json!({
                    "type": "response.output_item.done",
                    "output_index": 1,
                    "item": {
                        "type": "function_call",
                        "call_id": "call_b",
                        "name": "Read",
                        "arguments": "{\"file_path\":\"/tmp/b\"}"
                    }
                }),
                None,
            )
            .unwrap();
        assert!(String::from_utf8(repaired_b).unwrap().contains("/tmp/b"));

        let completed = translator
            .accept(
                &json!({
                    "type": "response.completed",
                    "response": {"id": "resp_ok", "usage": {}}
                }),
                None,
            )
            .unwrap();
        let completed = String::from_utf8(completed).unwrap();
        assert!(
            completed.contains("/tmp/a"),
            "the valid first tool must remain available after the failed transaction"
        );
        assert!(translator.is_finished());
    }

    #[test]
    fn preserves_two_interleaved_parallel_tool_calls_with_out_of_order_completion() {
        let out = render(vec![
            json!({
                "type": "response.output_item.added",
                "output_index": 3,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Lookup"}
            }),
            json!({
                "type": "response.output_item.added",
                "output_index": 7,
                "item": {"type": "function_call", "call_id": "call_2", "name": "Lookup"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 3,
                "delta": "{\"q\":\"a"
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 7,
                "delta": "{\"q\":\"b\"}"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 7,
                "item": {"type": "function_call", "call_id": "call_2", "name": "Lookup", "arguments": "{\"q\":\"b\"}"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 3,
                "delta": "\"}"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 3,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Lookup", "arguments": "{\"q\":\"a\"}"}
            }),
            json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ]);

        let values: Vec<serde_json::Value> =
            crate::anthropic::sse::try_parse_sse_events(out.as_bytes())
                .unwrap()
                .into_iter()
                .map(|event| serde_json::from_str(&event.data).unwrap())
                .collect();
        let starts: Vec<_> = values
            .iter()
            .filter(|value| value["type"] == "content_block_start")
            .collect();
        assert_eq!(starts.len(), 2);
        assert_eq!(starts[0]["index"], 0);
        assert_eq!(starts[0]["content_block"]["id"], "call_1");
        assert_eq!(starts[0]["content_block"]["name"], "Lookup");
        assert_eq!(starts[0]["content_block"]["caller"]["type"], "direct");
        assert_eq!(starts[1]["index"], 1);
        assert_eq!(starts[1]["content_block"]["id"], "call_2");
        assert_eq!(starts[1]["content_block"]["name"], "Lookup");

        let mut arguments = [String::new(), String::new()];
        let mut stop_order = Vec::new();
        for value in &values {
            if value["type"] == "content_block_delta"
                && value["delta"]["type"] == "input_json_delta"
            {
                let index = value["index"].as_u64().unwrap() as usize;
                arguments[index].push_str(value["delta"]["partial_json"].as_str().unwrap());
            }
            if value["type"] == "content_block_stop" {
                stop_order.push(value["index"].as_u64().unwrap());
            }
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&arguments[0]).unwrap(),
            json!({"q":"a"})
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&arguments[1]).unwrap(),
            json!({"q":"b"})
        );
        assert_eq!(stop_order, vec![1, 0]);
        assert!(out.contains(r#""stop_reason":"tool_use""#));
        assert_eq!(out.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn read_stall_closes_only_read_and_preserves_parallel_peer() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        let mut out = Vec::new();
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type":"function_call","call_id":"read_1","name":"Read"}
            }),
            json!({
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {"type":"function_call","call_id":"bash_1","name":"Bash"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 1,
                "delta": "{\"command\":\"echo peer\"}"
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": format!("{{\"file_path\":\"/tmp/a\",\"pages\":\"\"{}", " ".repeat(1024))
            }),
        ] {
            out.extend(translator.accept(&event, None).unwrap());
        }

        let before_peer_done = String::from_utf8(out.clone()).unwrap();
        assert!(before_peer_done.contains("/tmp/a"));
        assert!(!before_peer_done.contains("echo peer"));
        assert!(!before_peer_done.contains("message_stop"));
        assert!(!translator.is_finished());

        for event in [
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 1,
                "arguments": "{\"command\":\"echo peer\"}"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": {
                    "type":"function_call",
                    "call_id":"bash_1",
                    "name":"Bash",
                    "arguments":"{\"command\":\"echo peer\"}"
                }
            }),
            json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ] {
            out.extend(translator.accept(&event, None).unwrap());
        }

        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains("echo peer"));
        assert_eq!(rendered.matches("event: content_block_stop").count(), 2);
        assert_eq!(rendered.matches("event: message_stop").count(), 1);
        assert!(translator.is_finished());
    }

    #[test]
    fn preserves_three_tools_completed_in_c_a_b_order() {
        let mut events = Vec::new();
        for (output_index, call_id) in [(0, "call_a"), (1, "call_b"), (2, "call_c")] {
            events.push(json!({
                "type": "response.output_item.added",
                "output_index": output_index,
                "item": {"type":"function_call","call_id":call_id,"name":"Lookup"}
            }));
            events.push(json!({
                "type": "response.function_call_arguments.delta",
                "output_index": output_index,
                "delta": format!("{{\"q\":\"{call_id}\"}}")
            }));
        }
        for output_index in [2, 0, 1] {
            let call_id = ["call_a", "call_b", "call_c"][output_index];
            events.push(json!({
                "type": "response.function_call_arguments.done",
                "output_index": output_index,
                "arguments": format!("{{\"q\":\"{call_id}\"}}")
            }));
            events.push(json!({
                "type": "response.output_item.done",
                "output_index": output_index,
                "item": {
                    "type":"function_call",
                    "call_id":call_id,
                    "name":"Lookup",
                    "arguments":format!("{{\"q\":\"{call_id}\"}}")
                }
            }));
        }
        events.push(json!({
            "type": "response.completed",
            "response": {"id": "resp_1", "usage": {}}
        }));

        let out = render(events);
        let values: Vec<serde_json::Value> =
            crate::anthropic::sse::try_parse_sse_events(out.as_bytes())
                .unwrap()
                .into_iter()
                .map(|event| serde_json::from_str(&event.data).unwrap())
                .collect();
        let stop_order: Vec<_> = values
            .iter()
            .filter(|value| value["type"] == "content_block_stop")
            .filter_map(|value| value["index"].as_u64())
            .collect();
        assert_eq!(stop_order, vec![2, 0, 1]);
        for call_id in ["call_a", "call_b", "call_c"] {
            assert!(out.contains(call_id));
        }
        assert_eq!(out.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn terminal_snapshot_closes_parallel_tools_in_content_block_order() {
        let out = render(vec![
            json!({
                "type":"response.output_item.added",
                "output_index":20,
                "item":{"type":"function_call","call_id":"call_a","name":"Lookup"}
            }),
            json!({
                "type":"response.output_item.added",
                "output_index":5,
                "item":{"type":"function_call","call_id":"call_b","name":"Lookup"}
            }),
            json!({
                "type":"response.output_item.added",
                "output_index":10,
                "item":{"type":"function_call","call_id":"call_c","name":"Lookup"}
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "output_index":10,
                "arguments":"{\"q\":\"c\"}"
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "output_index":20,
                "arguments":"{\"q\":\"a\"}"
            }),
            json!({
                "type":"response.function_call_arguments.done",
                "output_index":5,
                "arguments":"{\"q\":\"b\"}"
            }),
            json!({
                "type":"response.completed",
                "response":{"id":"resp_1","usage":{}}
            }),
        ]);
        let values: Vec<serde_json::Value> =
            crate::anthropic::sse::try_parse_sse_events(out.as_bytes())
                .unwrap()
                .into_iter()
                .map(|event| serde_json::from_str(&event.data).unwrap())
                .collect();
        let argument_order: Vec<_> = values
            .iter()
            .filter(|value| value["delta"]["type"] == "input_json_delta")
            .filter_map(|value| value["index"].as_u64())
            .collect();
        let stop_order: Vec<_> = values
            .iter()
            .filter(|value| value["type"] == "content_block_stop")
            .filter_map(|value| value["index"].as_u64())
            .collect();
        assert_eq!(argument_order, vec![0, 1, 2]);
        assert_eq!(stop_order, vec![0, 1, 2]);
        assert_eq!(out.matches("event: message_stop").count(), 1);
    }

    #[test]
    fn repairs_whitespace_stalled_read_args_without_finishing_message() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.5");
        let mut out = Vec::new();
        out.extend(
            translator
                .accept(
                    &json!({
                        "type": "response.output_item.added",
                        "output_index": 0,
                        "item": {"type":"function_call","call_id":"call_1","name":"Read"}
                    }),
                    None,
                )
                .unwrap(),
        );
        out.extend(
            translator
                .accept(
                    &json!({
                        "type": "response.function_call_arguments.delta",
                        "output_index": 0,
                        "delta": format!("{{\"file_path\":\"/tmp/a\",\"pages\":\"\"{}", " ".repeat(1024))
                    }),
                    None,
                )
                .unwrap(),
        );
        let rendered = String::from_utf8(out.clone()).unwrap();
        assert!(rendered.contains(r#""partial_json":"{\"file_path\":\"/tmp/a\"}""#));
        assert!(!rendered.contains(r#""stop_reason":"tool_use""#));
        assert!(!rendered.contains("message_stop"));
        assert!(!translator.is_finished());

        out.extend(translator.finish_after_closed_completed_tool_call(None));
        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains(r#""stop_reason":"tool_use""#));
        assert!(rendered.contains("message_stop"));
        assert!(translator.is_finished());
    }

    #[test]
    fn finishes_after_closed_completed_tool_call() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.5");
        let mut out = Vec::new();
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type":"function_call","call_id":"call_1","name":"WebSearch"}
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "output_index": 0,
                "arguments": "{\"query\":\"claude-code-proxy github\"}"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type":"function_call",
                    "call_id":"call_1",
                    "name":"WebSearch",
                    "arguments":"{\"query\":\"claude-code-proxy github\"}"
                }
            }),
        ] {
            out.extend(translator.accept(&event, None).unwrap());
        }
        out.extend(translator.finish_after_closed_completed_tool_call(None));
        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains("content_block_start"));
        assert!(rendered.contains("input_json_delta"));
        assert!(rendered.contains(r#""stop_reason":"tool_use""#));
        assert!(rendered.contains("message_stop"));
        assert!(!rendered.contains("event: error"));
    }

    #[test]
    fn finishes_complete_tool_arguments_when_stream_closes_before_item_done() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        let mut out = Vec::new();
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type":"function_call","call_id":"call_1","name":"Workflow"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": "{\"script\":\"const result = await agent('inspect');\"}"
            }),
        ] {
            out.extend(translator.accept(&event, None).unwrap());
        }

        out.extend(translator.finish_after_closed_completed_tool_call(None));
        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains("content_block_start"));
        assert!(rendered.contains("input_json_delta"));
        assert!(rendered.contains(r#""stop_reason":"tool_use""#));
        assert!(rendered.contains("message_stop"));
        assert!(!rendered.contains("event: error"));
        assert!(translator.is_finished());
    }

    #[test]
    fn finishes_buffered_read_with_short_trailing_whitespace_on_close() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        let mut out = Vec::new();
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type":"function_call","call_id":"call_1","name":"Read"}
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": "{\"file_path\":\"/tmp/a\"}   "
            }),
        ] {
            out.extend(translator.accept(&event, None).unwrap());
        }

        out.extend(translator.finish_after_closed_completed_tool_call(None));
        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains("input_json_delta"));
        assert!(rendered.contains("/tmp/a"));
        assert!(rendered.contains(r#""stop_reason":"tool_use""#));
        assert!(rendered.contains("message_stop"));
        assert!(translator.is_finished());
    }

    #[test]
    fn does_not_finish_partial_tool_arguments_after_close() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.6-sol");
        translator
            .accept(
                &json!({
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {"type":"function_call","call_id":"call_1","name":"Workflow"}
                }),
                None,
            )
            .unwrap();
        translator
            .accept(
                &json!({
                    "type": "response.function_call_arguments.delta",
                    "output_index": 0,
                    "delta": "{\"script\":\"unfinished"
                }),
                None,
            )
            .unwrap();

        assert!(
            translator
                .finish_after_closed_completed_tool_call(None)
                .is_empty()
        );
        assert!(!translator.is_finished());
    }

    #[test]
    fn emits_web_search_results_from_citations_before_deferred_text() {
        let out = render(vec![
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "web_search_call", "id": "ws_1"}
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "web_search_call",
                    "id": "ws_1",
                    "action": {"query": "grok reasoning effort"}
                }
            }),
            json!({
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {"type": "message", "id": "msg_up"}
            }),
            json!({
                "type": "response.output_text.delta",
                "output_index": 1,
                "delta": "See the official docs."
            }),
            json!({
                "type": "response.output_text.annotation.added",
                "annotation": {
                    "type": "url_citation",
                    "title": "Reasoning",
                    "url": "https://docs.x.ai/docs/guides/reasoning"
                }
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": {"type": "message"}
            }),
            json!({
                "type": "response.completed",
                "response": {"status": "completed", "usage": {}}
            }),
        ]);

        let tool = out.find("server_tool_use").unwrap();
        let result = out.find("web_search_tool_result").unwrap();
        let text = out.find("See the official docs.").unwrap();
        assert!(tool < result && result < text);
        assert!(out.contains("https://docs.x.ai/docs/guides/reasoning"));
        assert!(out.contains(r#""caller":{"type":"direct"}"#));
        assert!(out.contains(r#""encrypted_content":"""#));
        assert!(out.contains(r#""page_age":null"#));
        assert!(out.contains(r#""web_search_requests":1"#));
    }

    #[test]
    fn keeps_web_search_queries_and_sources_associated_per_call() {
        let out = render(vec![
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "web_search_call", "id": "ws_1"}
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "web_search_call",
                    "id": "ws_1",
                    "action": {
                        "queries": ["alpha", "beta"],
                        "sources": [{"type":"url","title":"Source A","url":"https://a.example"}]
                    }
                }
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": {
                    "type": "web_search_call",
                    "id": "ws_2",
                    "action": {
                        "query": "gamma",
                        "sources": [{"type":"url","title":"Source B","url":"https://b.example"}]
                    }
                }
            }),
            json!({
                "type": "response.completed",
                "response": {"status": "completed", "usage": {}}
            }),
        ]);

        let values: Vec<serde_json::Value> =
            crate::anthropic::sse::try_parse_sse_events(out.as_bytes())
                .unwrap()
                .into_iter()
                .map(|event| serde_json::from_str(&event.data).unwrap())
                .collect();
        let tool_inputs: Vec<_> = values
            .iter()
            .filter(|value| value["delta"]["type"] == "input_json_delta")
            .filter_map(|value| value["delta"]["partial_json"].as_str())
            .map(|input| serde_json::from_str::<serde_json::Value>(input).unwrap())
            .collect();
        assert_eq!(
            tool_inputs,
            vec![json!({"query":"alpha\nbeta"}), json!({"query":"gamma"})]
        );

        let results: Vec<_> = values
            .iter()
            .filter_map(|value| {
                let block = value.get("content_block")?;
                (block.get("type")?.as_str()? == "web_search_tool_result").then_some(block)
            })
            .collect();
        let server_uses: Vec<_> = values
            .iter()
            .filter_map(|value| {
                let block = value.get("content_block")?;
                (block.get("type")?.as_str()? == "server_tool_use").then_some(block)
            })
            .collect();
        assert_eq!(server_uses.len(), 2);
        assert!(
            server_uses
                .iter()
                .all(|block| block["caller"]["type"] == "direct")
        );
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["tool_use_id"], "srvtoolu_ws_1");
        assert_eq!(results[0]["content"][0]["url"], "https://a.example");
        assert_eq!(results[0]["content"].as_array().unwrap().len(), 1);
        assert_eq!(results[0]["caller"]["type"], "direct");
        assert_eq!(results[0]["content"][0]["encrypted_content"], "");
        assert!(results[0]["content"][0]["page_age"].is_null());
        assert_eq!(results[1]["tool_use_id"], "srvtoolu_ws_2");
        assert_eq!(results[1]["content"][0]["url"], "https://b.example");
        assert_eq!(results[1]["content"].as_array().unwrap().len(), 1);
        assert_eq!(results[1]["caller"]["type"], "direct");
        assert!(out.contains(r#""web_search_requests":2"#));
    }

    #[test]
    fn rate_limit_event_returns_error() {
        let mut translator = LiveStreamTranslator::new("msg_1", "gpt-5.5");
        let err = translator
            .accept(
                &json!({
                    "type": "codex.rate_limits",
                    "rate_limits": {"limit_reached": true}
                }),
                None,
            )
            .unwrap_err();
        assert_eq!(err, "rate limit reached");
    }

    #[test]
    fn cumulative_live_input_is_bounded_across_many_valid_deltas() {
        let mut translator = LiveStreamTranslator::new("msg_limit", "gpt-5.5");
        let event = json!({
            "type": "response.output_text.delta",
            "output_index": 0,
            "delta": "x".repeat(512 * 1024),
        });
        let event_bytes = serde_json::to_vec(&event).unwrap().len();
        let accepted = MAX_LIVE_TRANSLATOR_INPUT_BYTES / event_bytes;
        for _ in 0..accepted {
            translator.accept(&event, None).unwrap();
        }
        let error = translator.accept(&event, None).unwrap_err();
        assert!(error.contains("cumulative size limit"));
        assert!(translator.upstream_input_bytes <= MAX_LIVE_TRANSLATOR_INPUT_BYTES);
    }

    #[test]
    fn live_stream_emits_signature_delta_before_thinking_stop() {
        let out = render(vec![
            json!({
                "type":"response.output_item.added",
                "output_index":0,
                "item":{"type":"reasoning","id":"rs_1","encrypted_content":"opaque"}
            }),
            json!({
                "type":"response.reasoning_summary_text.delta",
                "output_index":0,
                "delta":"plan"
            }),
            json!({
                "type":"response.output_item.done",
                "output_index":0,
                "item":{"type":"reasoning","id":"rs_1"}
            }),
            json!({
                "type":"response.completed",
                "response":{"id":"resp_1","usage":{}}
            }),
        ]);
        let thinking_delta = out.find(r#""type":"thinking_delta""#).unwrap();
        let signature_delta = out.find(r#""type":"signature_delta""#).unwrap();
        let thinking_stop = out[signature_delta..]
            .find("event: content_block_stop")
            .map(|offset| signature_delta + offset)
            .unwrap();
        assert!(thinking_delta < signature_delta);
        assert!(signature_delta < thinking_stop);
        assert!(out.contains("ccp:codex:v1:"));
    }
}
