use std::collections::BTreeSet;

use super::reducer::{GrokUsage, Reducer, ReducerEvent};
use super::tool_policy::ToolCallPolicy;
use crate::anthropic::sse::{SseEvent, encode_sse_event};

pub const MAX_SSE_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Default)]
pub struct SseDecoder {
    frame: Vec<u8>,
    line_start: usize,
    skip_lf: bool,
}

impl SseDecoder {
    pub fn push(&mut self, input: &[u8]) -> anyhow::Result<Vec<SseEvent>> {
        let mut events = Vec::new();
        for &byte in input {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\n' => self.end_line(&mut events)?,
                b'\r' => {
                    self.end_line(&mut events)?;
                    self.skip_lf = true;
                }
                _ => self.push_byte(byte)?,
            }
        }
        Ok(events)
    }

    pub fn finish(&mut self) -> anyhow::Result<()> {
        if self.frame.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("Grok SSE stream ended with an incomplete frame")
        }
    }

    fn push_byte(&mut self, byte: u8) -> anyhow::Result<()> {
        if self.frame.len() >= MAX_SSE_FRAME_BYTES {
            anyhow::bail!("Grok SSE frame exceeds the size limit");
        }
        self.frame.push(byte);
        Ok(())
    }

    fn end_line(&mut self, events: &mut Vec<SseEvent>) -> anyhow::Result<()> {
        if self.frame.len() == self.line_start {
            if !self.frame.is_empty()
                && let Some(event) = parse_frame(&self.frame)?
            {
                events.push(event);
            }
            self.frame.clear();
            self.line_start = 0;
            return Ok(());
        }
        self.push_byte(b'\n')?;
        self.line_start = self.frame.len();
        Ok(())
    }
}

fn parse_frame(frame: &[u8]) -> anyhow::Result<Option<SseEvent>> {
    let frame = std::str::from_utf8(frame)
        .map_err(|_| anyhow::anyhow!("Grok SSE frame contains invalid UTF-8"))?;
    let mut event = None;
    let mut data = Vec::new();
    for line in frame.lines() {
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => event = Some(value.to_owned()),
            "data" => data.push(value),
            _ => {}
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(SseEvent {
        event,
        data: data.join("\n"),
    }))
}

#[derive(Clone)]
pub struct StreamTranslator {
    message_id: String,
    model: String,
    started: bool,
    finished: bool,
    open_content: BTreeSet<usize>,
    usage: GrokUsage,
}

pub struct LiveStreamTranslator {
    decoder: SseDecoder,
    reducer: Reducer,
    renderer: StreamTranslator,
}

impl LiveStreamTranslator {
    pub fn new(message_id: String, model: String) -> Self {
        Self::with_tool_policy(message_id, model, ToolCallPolicy::permissive())
    }

    pub(crate) fn with_tool_policy(
        message_id: String,
        model: String,
        tool_policy: ToolCallPolicy,
    ) -> Self {
        Self {
            decoder: SseDecoder::default(),
            reducer: Reducer::with_tool_policy(tool_policy),
            renderer: StreamTranslator::new(message_id, model),
        }
    }

    pub fn push(&mut self, chunk: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut decoder = self.decoder.clone();
        let values = decoder
            .push(chunk)?
            .into_iter()
            .map(|event| {
                serde_json::from_str(&event.data)
                    .map_err(|_| anyhow::anyhow!("malformed Grok SSE event"))
            })
            .collect::<anyhow::Result<Vec<serde_json::Value>>>()?;
        let (reducer, reduced) = self.reducer.stage_batch(values)?;
        let mut renderer = self.renderer.clone();
        let out = renderer.render(reduced)?;
        self.decoder = decoder;
        self.reducer = reducer;
        self.renderer = renderer;
        Ok(out)
    }

    pub fn finish(mut self) -> anyhow::Result<()> {
        self.decoder.finish()?;
        if !self.reducer.finished() {
            anyhow::bail!("Grok stream ended without completion");
        }
        Ok(())
    }
}

impl StreamTranslator {
    pub fn new(message_id: String, model: String) -> Self {
        Self {
            message_id,
            model,
            started: false,
            finished: false,
            open_content: BTreeSet::new(),
            usage: GrokUsage::default(),
        }
    }

    pub fn render(&mut self, events: Vec<ReducerEvent>) -> anyhow::Result<Vec<u8>> {
        if self.finished && !events.is_empty() {
            anyhow::bail!("event after terminal completion");
        }
        let mut out = Vec::new();
        for event in events {
            if let ReducerEvent::Usage(usage) = &event {
                self.usage = usage.clone();
                continue;
            }
            if !self.started
                && matches!(
                    event,
                    ReducerEvent::TextStart(_)
                        | ReducerEvent::ToolStart(_, _, _)
                        | ReducerEvent::HostedSearch { .. }
                        | ReducerEvent::Finish { .. }
                )
            {
                self.started = true;
                emit(
                    &mut out,
                    "message_start",
                    serde_json::json!({"type":"message_start","message":{"id":self.message_id,"type":"message","role":"assistant","model":self.model,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}),
                );
            }
            match &event {
                ReducerEvent::TextStart(index) | ReducerEvent::ToolStart(index, _, _) => {
                    self.open_content.insert(*index);
                }
                ReducerEvent::TextStop(index) | ReducerEvent::ToolStop(index) => {
                    self.open_content.remove(index);
                }
                ReducerEvent::Finish { .. } => self.close_open_content(&mut out),
                _ => {}
            }
            if matches!(event, ReducerEvent::Finish { .. }) {
                self.finished = true;
            }
            render(&mut out, event, &self.usage);
        }
        Ok(out)
    }

    /// Finish an in-band failed stream without leaving a started Anthropic content block open.
    pub fn render_error(&mut self, message: &str) -> Vec<u8> {
        self.render_typed_error("api_error", message)
    }

    /// Finish an in-band failed stream while preserving the upstream error category.
    pub fn render_typed_error(&mut self, error_type: &str, message: &str) -> Vec<u8> {
        if self.finished {
            return Vec::new();
        }
        let mut out = Vec::new();
        self.close_open_content(&mut out);
        out.extend(stream_error_with_type(error_type, message));
        self.finished = true;
        out
    }

    fn close_open_content(&mut self, out: &mut Vec<u8>) {
        for index in std::mem::take(&mut self.open_content) {
            emit(
                out,
                "content_block_stop",
                serde_json::json!({"type":"content_block_stop","index":index}),
            );
        }
    }
}

pub fn translate_stream_bytes(
    upstream: &[u8],
    message_id: &str,
    model: &str,
) -> anyhow::Result<Vec<u8>> {
    translate_stream_bytes_with_tool_policy(
        upstream,
        message_id,
        model,
        &ToolCallPolicy::permissive(),
    )
}

pub(crate) fn translate_stream_bytes_with_tool_policy(
    upstream: &[u8],
    message_id: &str,
    model: &str,
    tool_policy: &ToolCallPolicy,
) -> anyhow::Result<Vec<u8>> {
    let mut decoder = SseDecoder::default();
    let mut reducer = Reducer::with_tool_policy(tool_policy.clone());
    let mut translator = StreamTranslator::new(message_id.into(), model.into());
    let values = decoder
        .push(upstream)?
        .into_iter()
        .map(|event| {
            serde_json::from_str(&event.data)
                .map_err(|_| anyhow::anyhow!("malformed Grok SSE event"))
        })
        .collect::<anyhow::Result<Vec<serde_json::Value>>>()?;
    let out = translator.render(reducer.push_batch(values)?)?;
    decoder.finish()?;
    if !reducer.finished() {
        anyhow::bail!("Grok stream ended without completion");
    }
    Ok(out)
}

pub fn stream_error() -> Vec<u8> {
    stream_error_with_message("Grok stream is invalid")
}

pub fn stream_error_with_message(message: &str) -> Vec<u8> {
    stream_error_with_type("api_error", message)
}

pub fn stream_error_with_type(error_type: &str, message: &str) -> Vec<u8> {
    let data = serde_json::json!({"type":"error","error":{"type":error_type,"message":message}});
    encode_sse_event(Some("error"), &data.to_string())
}

pub fn stream_ping() -> Vec<u8> {
    encode_sse_event(Some("ping"), r#"{"type":"ping"}"#)
}

fn emit(out: &mut Vec<u8>, event: &str, data: serde_json::Value) {
    out.extend(encode_sse_event(Some(event), &data.to_string()));
}

fn render(out: &mut Vec<u8>, event: ReducerEvent, usage: &GrokUsage) {
    match event {
        // The Grok CLI endpoint exposes a plaintext reasoning summary. It is not a signed
        // Anthropic thinking block and often contains draft answers or model chatter. The Grok
        // coordinator emits protocol-level pings independently, so suppress these events here.
        ReducerEvent::ThinkingStart(_)
        | ReducerEvent::ThinkingDelta(_, _)
        | ReducerEvent::ThinkingStop(_) => {}
        ReducerEvent::TextStop(i) | ReducerEvent::ToolStop(i) => emit(
            out,
            "content_block_stop",
            serde_json::json!({"type":"content_block_stop","index":i}),
        ),
        ReducerEvent::TextStart(i) => emit(
            out,
            "content_block_start",
            serde_json::json!({"type":"content_block_start","index":i,"content_block":{"type":"text","text":"","citations":null}}),
        ),
        ReducerEvent::TextDelta(i, t) => emit(
            out,
            "content_block_delta",
            serde_json::json!({"type":"content_block_delta","index":i,"delta":{"type":"text_delta","text":t}}),
        ),
        ReducerEvent::ToolStart(i, id, name) => emit(
            out,
            "content_block_start",
            serde_json::json!({"type":"content_block_start","index":i,"content_block":{"type":"tool_use","id":id,"name":name,"input":{},"caller":{"type":"direct"}}}),
        ),
        ReducerEvent::ToolDelta(i, t) => emit(
            out,
            "content_block_delta",
            serde_json::json!({"type":"content_block_delta","index":i,"delta":{"type":"input_json_delta","partial_json":t}}),
        ),
        ReducerEvent::HostedSearch {
            index,
            result_index,
            id,
            name,
            query,
        } => {
            // Only web_search belongs to Anthropic's server-tool ContentBlock union. Reducer-side
            // x_search degradation should make this branch unreachable for x_search, but keep the
            // renderer fail-closed against synthetic internal events as well.
            if name != "web_search" {
                return;
            }
            emit(
                out,
                "content_block_start",
                serde_json::json!({"type":"content_block_start","index":index,"content_block":{"type":"server_tool_use","id":id,"name":name,"input":{},"caller":{"type":"direct"}}}),
            );
            emit(
                out,
                "content_block_delta",
                serde_json::json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":serde_json::json!({"query":query}).to_string()}}),
            );
            emit(
                out,
                "content_block_stop",
                serde_json::json!({"type":"content_block_stop","index":index}),
            );
            emit(
                out,
                "content_block_start",
                serde_json::json!({"type":"content_block_start","index":result_index,"content_block":{"type":"web_search_tool_result","tool_use_id":id,"content":[],"caller":{"type":"direct"}}}),
            );
            emit(
                out,
                "content_block_stop",
                serde_json::json!({"type":"content_block_stop","index":result_index}),
            );
        }
        ReducerEvent::Citation(i, annotation) => {
            let citation = serde_json::json!({
                "type":"web_search_result_location",
                "url":annotation.get("url").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "title":annotation.get("title").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "cited_text":annotation.get("text").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "encrypted_index":""
            });
            emit(
                out,
                "content_block_delta",
                serde_json::json!({"type":"content_block_delta","index":i,"delta":{"type":"citations_delta","citation":citation}}),
            );
        }
        ReducerEvent::Terminal(_) | ReducerEvent::Usage(_) => {}
        ReducerEvent::Finish {
            stop_reason,
            input_tokens,
            output_tokens,
            web_search_requests,
            x_search_requests: _,
        } => {
            emit(
                out,
                "message_delta",
                serde_json::json!({"type":"message_delta","delta":{"stop_reason":stop_reason,"stop_sequence":null},"usage":anthropic_usage(usage, input_tokens, output_tokens, web_search_requests)}),
            );
            emit(
                out,
                "message_stop",
                serde_json::json!({"type":"message_stop"}),
            );
        }
    }
}

pub(super) fn anthropic_usage(
    usage: &GrokUsage,
    fallback_input_tokens: u64,
    fallback_output_tokens: u64,
    hosted_search_requests: u64,
) -> serde_json::Value {
    let mapped_input = usage.mapped_input_tokens();
    let mapped_cache_read = usage.mapped_cache_read_input_tokens();
    let output = usage.output_tokens;
    let value = serde_json::Map::from_iter([
        (
            "input_tokens".into(),
            serde_json::json!(mapped_input.unwrap_or(fallback_input_tokens)),
        ),
        (
            "output_tokens".into(),
            serde_json::json!(output.unwrap_or(fallback_output_tokens)),
        ),
        ("cache_creation_input_tokens".into(), serde_json::json!(0)),
        (
            "cache_read_input_tokens".into(),
            serde_json::json!(mapped_cache_read.unwrap_or(0)),
        ),
        (
            "server_tool_use".into(),
            serde_json::json!({"web_search_requests":hosted_search_requests}),
        ),
    ]);
    serde_json::Value::Object(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response_tool_policy(
        tools: serde_json::Value,
        tool_choice: serde_json::Value,
    ) -> ToolCallPolicy {
        let request: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model":"grok-4.5",
                "messages":[{"role":"user","content":"use tools"}],
                "tools":tools,
                "tool_choice":tool_choice
            }))
            .unwrap();
        let translated = super::super::request::translate_request(&request, "grok-4.5".into())
            .expect("test request must translate");
        ToolCallPolicy::from_request(&translated)
    }

    #[test]
    fn live_stream_rejects_a_tool_forbidden_by_the_request_policy_before_rendering() {
        let policy = response_tool_policy(
            serde_json::json!([{"name":"Read","input_schema":{"type":"object"}}]),
            serde_json::json!({"type":"none"}),
        );
        let mut live =
            LiveStreamTranslator::with_tool_policy("msg_1".into(), "grok-4.5".into(), policy);
        let error = live
            .push(b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"c1\",\"name\":\"Read\"}}\n\n")
            .unwrap_err();
        assert!(error.to_string().contains("tool_choice forbids"), "{error}");
    }

    #[test]
    fn buffered_stream_rejects_success_without_a_required_tool() {
        let policy = response_tool_policy(
            serde_json::json!([{"name":"Read","input_schema":{"type":"object"}}]),
            serde_json::json!({"type":"any"}),
        );
        let error = translate_stream_bytes_with_tool_policy(
            b"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{}}}\n\n",
            "msg_1",
            "grok-4.5",
            &policy,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("required by tool_choice"),
            "{error}"
        );
    }

    #[test]
    fn decoder_accepts_every_boundary_and_line_ending() {
        let input = b": note\r\nevent: ignored\r\ndata: first\r\ndata: second\r\n\r\n";
        let expected = vec![SseEvent {
            event: Some("ignored".into()),
            data: "first\nsecond".into(),
        }];
        for split in 0..=input.len() {
            let mut decoder = SseDecoder::default();
            let mut events = decoder.push(&input[..split]).unwrap();
            events.extend(decoder.push(&input[split..]).unwrap());
            decoder.finish().unwrap();
            assert_eq!(events, expected);
        }
    }

    #[test]
    fn decoder_ignores_comment_only_keepalives() {
        let mut decoder = SseDecoder::default();
        let events = decoder
            .push(b": keep-alive\n\nevent: message_start\ndata: {\"type\":\"message_start\"}\n\n")
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.as_deref(), Some("message_start"));
        assert_eq!(events[0].data, "{\"type\":\"message_start\"}");
    }

    #[test]
    fn decoder_requires_terminated_valid_frames_and_bounds_them() {
        assert!(SseDecoder::default().push(b"data: \xff\n\n").is_err());
        let mut decoder = SseDecoder::default();
        decoder.push(b"data: incomplete").unwrap();
        assert!(decoder.finish().is_err());
        let mut decoder = SseDecoder::default();
        let exact = vec![b'x'; MAX_SSE_FRAME_BYTES - b"data: \n".len()];
        assert!(decoder.push(b"data: ").is_ok());
        assert!(decoder.push(&exact).is_ok());
        let events = decoder.push(b"\n\n").unwrap();
        assert_eq!(events[0].data.len(), exact.len());
        decoder.finish().unwrap();
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"data: ").is_ok());
        assert!(decoder.push(&vec![b'x'; exact.len() + 1]).is_ok());
        assert!(decoder.push(b"\n").is_err());
    }

    #[test]
    fn stream_translates_hosted_web_search_and_citations() {
        let input = b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"web_search_call\",\"id\":\"ws_1\"}}\n\ndata: {\"type\":\"response.web_search_call.in_progress\",\"item_id\":\"ws_1\"}\n\ndata: {\"type\":\"response.web_search_call.searching\",\"item_id\":\"ws_1\"}\n\ndata: {\"type\":\"response.web_search_call.completed\",\"item_id\":\"ws_1\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"web_search_call\",\"id\":\"ws_1\",\"action\":{\"query\":\"rust news\"}}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Result\"}\n\ndata: {\"type\":\"response.output_text.annotation.added\",\"annotation\":{\"type\":\"url_citation\",\"url\":\"https://example.com\",\"title\":\"Example\"}}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":2}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();
        assert!(output.contains("server_tool_use"));
        assert!(output.contains("web_search_tool_result"));
        assert_eq!(
            output.matches("\"caller\":{\"type\":\"direct\"}").count(),
            2
        );
        assert!(output.contains("citations_delta"));
        assert!(output.contains("https://example.com"));
        assert!(output.contains("\"encrypted_index\":\"\""));
        assert!(output.contains("\"citations\":null"));
        assert!(output.contains("\"web_search_requests\":1"));
    }

    #[test]
    fn stream_marks_function_tools_as_direct_calls() {
        let input = b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"call_1\",\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();
        assert!(output.contains("\"caller\":{\"type\":\"direct\"}"));
    }

    #[test]
    fn stream_degrades_x_search_to_schema_valid_text_and_citations() {
        let input = b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"custom_tool_call\",\"name\":\"x_search\",\"id\":\"xs_1\"}}\n\ndata: {\"type\":\"response.custom_tool_call_input.delta\",\"item_id\":\"xs_1\",\"delta\":\"{\\\"query\\\":\\\"claude-code-proxy\\\"}\"}\n\ndata: {\"type\":\"response.custom_tool_call_input.done\",\"item_id\":\"xs_1\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"custom_tool_call\",\"name\":\"x_search\",\"id\":\"xs_1\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Recent post\"}\n\ndata: {\"type\":\"response.output_text.annotation.added\",\"annotation\":{\"type\":\"url_citation\",\"url\":\"https://x.com/example/status/1\",\"title\":\"Example post\"}}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":4,\"output_tokens\":3}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();
        assert!(!output.contains("\"type\":\"server_tool_use\""));
        assert!(!output.contains("x_search_tool_result"));
        assert!(!output.contains("\"name\":\"x_search\""));
        assert!(output.contains("Recent post"));
        assert!(output.contains("https://x.com/example/status/1"));
        assert!(output.contains("\"encrypted_index\":\"\""));
        assert!(output.contains("\"citations\":null"));
        assert!(output.contains("\"web_search_requests\":0"));
        assert!(output.contains("\"stop_reason\":\"end_turn\""));
        assert!(!output.contains("x_search_requests"));

        let mut decoder = SseDecoder::default();
        let starts: Vec<serde_json::Value> = decoder
            .push(output.as_bytes())
            .unwrap()
            .into_iter()
            .filter_map(|event| serde_json::from_str(&event.data).ok())
            .filter(|event: &serde_json::Value| {
                event.get("type").and_then(serde_json::Value::as_str) == Some("content_block_start")
            })
            .collect();
        decoder.finish().unwrap();
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0]["content_block"]["type"], "text");
        assert!(starts[0]["content_block"]["citations"].is_null());
    }

    #[test]
    fn stream_incomplete_after_completed_tool_uses_terminal_stop_reason() {
        let input = b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"arguments\":\"{}\"}}\n\ndata: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();
        assert!(output.contains("\"type\":\"tool_use\""));
        assert!(output.contains("\"stop_reason\":\"max_tokens\""));
        assert!(!output.contains("\"stop_reason\":\"tool_use\""));
    }

    #[test]
    fn live_translator_emits_first_event_before_upstream_completion() {
        let mut translator = LiveStreamTranslator::new("msg_1".into(), "grok-4.5".into());
        let output = translator
            .push(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"first\"}\n\n")
            .unwrap();
        assert!(String::from_utf8(output).unwrap().contains("first"));
    }

    #[test]
    fn stream_hides_plaintext_reasoning_without_shifting_content_indexes() {
        let input = b"data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"draft answer \\ud83d\\ude0a\"}\n\ndata: {\"type\":\"response.reasoning_summary_text.done\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"final answer\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();

        assert!(!output.contains("draft answer"));
        assert!(!output.contains('😊'));
        assert_eq!(output.matches("final answer").count(), 1);
        assert!(output.contains("\"index\":0"));
        assert!(!output.contains("\"type\":\"thinking\""));
    }

    #[test]
    fn stream_emits_output_delta_once_and_ignores_full_snapshots() {
        let input = b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\"}}\n\ndata: {\"type\":\"response.content_part.added\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"answer \\ud83d\\ude0a\"}\n\ndata: {\"type\":\"response.output_text.done\",\"text\":\"answer \\ud83d\\ude0a\"}\n\ndata: {\"type\":\"response.content_part.done\",\"part\":{\"type\":\"output_text\",\"text\":\"answer \\ud83d\\ude0a\"}}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer \\ud83d\\ude0a\"}]}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer \\ud83d\\ude0a\"}]}],\"usage\":{}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();

        assert_eq!(output.matches("answer 😊").count(), 1);
    }

    #[test]
    fn live_stream_preserves_final_utf8_across_every_chunk_boundary() {
        let input = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"answer 😊\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n".as_bytes();
        for split in 0..=input.len() {
            let mut translator = LiveStreamTranslator::new("msg_1".into(), "grok-4.5".into());
            let mut output = translator.push(&input[..split]).unwrap();
            output.extend(translator.push(&input[split..]).unwrap());
            translator.finish().unwrap();
            let output = String::from_utf8(output).unwrap();
            assert_eq!(output.matches("answer 😊").count(), 1, "split={split}");
        }
    }

    #[test]
    fn completed_with_trailing_rate_limit_telemetry_succeeds_in_both_stream_paths() {
        let input = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\ndata: {\"type\":\"rate_limits.updated\",\"remaining\":42}\n\n";

        let buffered =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();
        let mut live = LiveStreamTranslator::new("msg_1".into(), "grok-4.5".into());
        let live_output = String::from_utf8(live.push(input).unwrap()).unwrap();
        live.finish().unwrap();

        for output in [buffered, live_output] {
            assert_eq!(output.matches("answer").count(), 1, "{output}");
            assert_eq!(output.matches("event: message_stop").count(), 1, "{output}");
            assert!(!output.contains("rate_limits.updated"), "{output}");
        }
    }

    #[test]
    fn terminal_with_trailing_text_fails_and_rolls_back_live_same_chunk() {
        let input = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"STAGED_ONLY\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"UNSAFE_TAIL\"}\n\n";

        let buffered_error = translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap_err();
        assert!(
            buffered_error.to_string().contains("event after terminal"),
            "{buffered_error:#}"
        );

        let mut live = LiveStreamTranslator::new("msg_1".into(), "grok-4.5".into());
        let live_error = live.push(input).unwrap_err();
        assert!(
            live_error.to_string().contains("event after terminal"),
            "{live_error:#}"
        );
        assert!(!live.reducer.finished());
        assert!(!live.renderer.started);
        assert!(!live.renderer.finished);
    }

    #[test]
    fn incomplete_stream_closes_content_and_forwards_detailed_usage() {
        let input = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\ndata: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":3},\"output_tokens\":7,\"output_tokens_details\":{\"reasoning_tokens\":5},\"total_tokens\":19}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();

        let stop = output.find("event: content_block_stop").unwrap();
        let finish = output.find("event: message_delta").unwrap();
        assert!(stop < finish, "{output}");
        assert!(
            output.contains("\"stop_reason\":\"max_tokens\""),
            "{output}"
        );
        assert!(output.contains("\"input_tokens\":9"), "{output}");
        assert!(
            output.contains("\"cache_creation_input_tokens\":0"),
            "{output}"
        );
        assert!(output.contains("\"cache_read_input_tokens\":3"), "{output}");
        assert!(output.contains("\"output_tokens\":7"), "{output}");
        assert!(!output.contains("reasoning_tokens"), "{output}");
        assert!(!output.contains("total_tokens"), "{output}");
    }

    #[test]
    fn missing_usage_uses_only_protocol_placeholders_downstream() {
        let input = b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        let output =
            String::from_utf8(translate_stream_bytes(input, "msg_1", "grok-4.5").unwrap()).unwrap();

        assert!(!output.contains("ccproxy_usage"), "{output}");
        assert!(!output.contains("reasoning_tokens"), "{output}");
        assert!(!output.contains("total_tokens"), "{output}");
        for field in [
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ] {
            assert_eq!(
                output.matches(&format!("\"{field}\":")).count(),
                2,
                "field={field}, output={output}"
            );
        }
    }

    #[test]
    fn in_band_error_closes_every_open_content_block_first() {
        let mut translator = StreamTranslator::new("msg_1".into(), "grok-4.5".into());
        let mut output = translator
            .render(vec![
                ReducerEvent::TextStart(0),
                ReducerEvent::TextDelta(0, "partial".into()),
                ReducerEvent::ToolStart(1, "call_1".into(), "Read".into()),
            ])
            .unwrap();
        output.extend(translator.render_error("Grok stream is invalid"));
        let output = String::from_utf8(output).unwrap();

        assert_eq!(
            output.matches("event: content_block_stop").count(),
            2,
            "{output}"
        );
        let last_stop = output.rfind("event: content_block_stop").unwrap();
        let error = output.find("event: error").unwrap();
        assert!(last_stop < error, "{output}");
    }
}
