use serde_json::Value;

const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexTerminalKind {
    Completed,
    Done,
    Incomplete,
    Failed,
    ResponseError,
    Cancelled,
    Error,
}

impl CodexTerminalKind {
    pub(crate) fn from_payload(payload: &Value) -> Option<Self> {
        match payload.get("type").and_then(Value::as_str) {
            Some("response.completed") => Some(Self::Completed),
            Some("response.done") => Some(Self::Done),
            Some("response.incomplete") => Some(Self::Incomplete),
            Some("response.failed") => Some(Self::Failed),
            Some("response.error") => Some(Self::ResponseError),
            Some("response.cancelled") => Some(Self::Cancelled),
            Some("error") => Some(Self::Error),
            _ => None,
        }
    }

    pub(crate) fn is_failure(self) -> bool {
        matches!(
            self,
            Self::Failed | Self::ResponseError | Self::Cancelled | Self::Error
        )
    }

    pub(crate) fn is_reusable(self) -> bool {
        matches!(self, Self::Completed | Self::Done)
    }
}

/// Validate an optional terminal response snapshot status against the event that carries it.
///
/// Codex has emitted both full Responses events and the Lite `response.done` success alias.
/// Omitted and null statuses remain compatible with both shapes, but a provided status must never
/// contradict the terminal event and turn an upstream failure into a successful Anthropic
/// response. This is deliberately the Codex Responses contract, not Realtime's multi-outcome
/// `response.done` event.
pub(crate) fn validate_terminal_snapshot_status(payload: &Value) -> Result<(), String> {
    let Some(kind) = CodexTerminalKind::from_payload(payload) else {
        return Ok(());
    };
    let event_type = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let expected = match kind {
        CodexTerminalKind::Completed | CodexTerminalKind::Done => "completed",
        CodexTerminalKind::Incomplete => "incomplete",
        CodexTerminalKind::Failed => "failed",
        // `error` is an event name, not a valid Responses snapshot status. A
        // response snapshot carried by either error event describes a failed
        // response.
        CodexTerminalKind::ResponseError | CodexTerminalKind::Error => "failed",
        CodexTerminalKind::Cancelled => "cancelled",
    };
    match payload.pointer("/response/status") {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(status)) if status == expected => Ok(()),
        Some(Value::String(status)) => Err(format!(
            "{event_type} response.status must be {expected:?} when provided, got {status:?}"
        )),
        Some(_) => Err(format!(
            "{event_type} response.status must be {expected:?} or null when provided"
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexFailureKind {
    RateLimit,
    Overloaded,
    Transient,
    Permanent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexEventFailure {
    pub kind: CodexFailureKind,
    pub explicit_status: Option<u16>,
    pub status: u16,
    pub message: String,
    pub retry_after: Option<String>,
}

impl CodexEventFailure {
    pub fn retryable(&self) -> bool {
        !matches!(self.kind, CodexFailureKind::Permanent)
    }
}

pub(crate) fn classify_event_failure(payload: &Value) -> Option<CodexEventFailure> {
    let event_type = payload.get("type").and_then(Value::as_str)?;
    if event_type == "codex.rate_limits" {
        if payload
            .pointer("/rate_limits/limit_reached")
            .and_then(Value::as_bool)
            != Some(true)
        {
            return None;
        }
        return Some(CodexEventFailure {
            kind: CodexFailureKind::RateLimit,
            explicit_status: Some(429),
            status: 429,
            message: "rate limit reached".to_string(),
            retry_after: scalar_string(payload.pointer("/rate_limits/primary/reset_after_seconds")),
        });
    }
    if !CodexTerminalKind::from_payload(payload).is_some_and(CodexTerminalKind::is_failure) {
        return None;
    }
    if validate_terminal_snapshot_status(payload).is_err() {
        // A contradictory terminal snapshot is protocol corruption. It must be
        // surfaced by the reducers, never promoted into a replayable service
        // failure by the retry classifier.
        return None;
    }

    let error = payload
        .get("error")
        .or_else(|| payload.pointer("/response/error"));
    let explicit_status = numeric_status(payload)
        .or_else(|| {
            error
                .and_then(|value| value.get("status"))
                .and_then(Value::as_u64)
        })
        .and_then(|status| u16::try_from(status).ok());
    let message = error
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("Upstream error")
        .to_string();
    let code = error
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str);
    let error_type = error
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str);
    let lower = message.to_ascii_lowercase();

    let kind = if explicit_status == Some(429) || lower.contains("rate limit") {
        CodexFailureKind::RateLimit
    } else if explicit_status == Some(529)
        || code == Some("overloaded_error")
        || error_type == Some("overloaded_error")
        || lower.contains("overloaded")
    {
        CodexFailureKind::Overloaded
    } else if explicit_status.is_some_and(|status| matches!(status, 500 | 502 | 503 | 504))
        || matches!(
            code,
            Some("server_error" | "internal_server_error" | "internal_error")
        )
        || matches!(
            error_type,
            Some("server_error" | "internal_server_error" | "internal_error")
        )
        || retryable_message(&lower)
    {
        CodexFailureKind::Transient
    } else {
        CodexFailureKind::Permanent
    };
    let status = explicit_status.unwrap_or(match kind {
        CodexFailureKind::RateLimit => 429,
        CodexFailureKind::Overloaded => 529,
        CodexFailureKind::Transient => 503,
        CodexFailureKind::Permanent => 500,
    });
    let retry_after = error
        .and_then(|value| value.get("retry_after"))
        .and_then(scalar_string_value)
        .or_else(|| {
            error
                .and_then(|value| value.get("retry_after_seconds"))
                .and_then(scalar_string_value)
        })
        .or_else(|| scalar_string(payload.get("retry_after_seconds")))
        .or_else(|| scalar_string(payload.pointer("/headers/retry-after")))
        .or_else(|| scalar_string(payload.pointer("/headers/Retry-After")));

    Some(CodexEventFailure {
        kind,
        explicit_status,
        status,
        message,
        retry_after,
    })
}

pub(crate) fn first_retryable_failure(body: &[u8]) -> Option<CodexEventFailure> {
    let mut first_failure = None;
    let mut hosted_side_effect_started = false;
    // Malformed protocol bytes must never become the reason to replay a model
    // request. The reducer will surface the decode failure to the caller.
    for event in parse_codex_sse_events(body).ok()? {
        if event.data == "[DONE]" {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        if starts_hosted_side_effect(&payload) {
            hosted_side_effect_started = true;
            first_failure = None;
        }
        if CodexTerminalKind::from_payload(&payload).is_some_and(|kind| {
            matches!(
                kind,
                CodexTerminalKind::Completed
                    | CodexTerminalKind::Done
                    | CodexTerminalKind::Incomplete
            )
        }) {
            // A non-error response terminal is authoritative. Providers may append
            // quota telemetry after it; that tail must not turn either a completed
            // response or a valid truncated response into a whole-request replay.
            return None;
        }
        if let Some(failure) = classify_event_failure(&payload)
            && failure.retryable()
            && !hosted_side_effect_started
            && first_failure.is_none()
        {
            first_failure = Some(failure);
        }
    }
    (!hosted_side_effect_started)
        .then_some(first_failure)
        .flatten()
}

/// Once Codex exposes a hosted search lifecycle event, replaying the logical
/// request may repeat an external search even when no Anthropic bytes were
/// emitted yet. Keep this separate from downstream stream commitment.
pub(crate) fn starts_hosted_side_effect(payload: &Value) -> bool {
    match payload.get("type").and_then(Value::as_str) {
        Some(
            "response.web_search_call.in_progress"
            | "response.web_search_call.searching"
            | "response.web_search_call.completed",
        ) => true,
        Some("response.output_item.added" | "response.output_item.done") => {
            payload
                .get("item")
                .and_then(|item| item.get("type"))
                .and_then(Value::as_str)
                == Some("web_search_call")
        }
        _ => false,
    }
}

/// Strict Codex SSE parsing with the protocol's one allowed stream-start BOM.
/// A later BOM could prefix `data:` and otherwise be treated as an unknown SSE
/// field, silently dropping a model event.
pub(crate) fn parse_codex_sse_events(
    body: &[u8],
) -> Result<Vec<crate::anthropic::sse::SseEvent>, String> {
    let body = body.strip_prefix(UTF8_BOM).unwrap_or(body);
    if body
        .windows(UTF8_BOM.len())
        .any(|window| window == UTF8_BOM)
    {
        return Err("Codex SSE response contained a UTF-8 BOM after stream start".to_string());
    }
    crate::anthropic::sse::try_parse_sse_events(body).map_err(|error| {
        format!(
            "Codex SSE response contained invalid UTF-8 at byte {}",
            error.valid_up_to()
        )
    })
}

pub(crate) fn numeric_status(payload: &Value) -> Option<u64> {
    payload
        .get("status")
        .and_then(Value::as_u64)
        .or_else(|| payload.get("status_code").and_then(Value::as_u64))
}

fn scalar_string(value: Option<&Value>) -> Option<String> {
    value.and_then(scalar_string_value)
}

fn scalar_string_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn retryable_message(message: &str) -> bool {
    [
        "server error",
        "internal server error",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "temporarily unavailable",
        "you can retry your request",
        "socket connection was closed unexpectedly",
        "connection closed unexpectedly",
        "operation timed out",
        "connection reset",
        "connection closed",
        "timed out",
        "timeout",
        "econnreset",
        "epipe",
        "etimedout",
        "und_err_socket",
        "fetch failed",
        "unexpected eof",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_kinds_are_classified_consistently() {
        let cases = [
            (
                "response.completed",
                CodexTerminalKind::Completed,
                true,
                false,
            ),
            ("response.done", CodexTerminalKind::Done, true, false),
            (
                "response.incomplete",
                CodexTerminalKind::Incomplete,
                false,
                false,
            ),
            ("response.failed", CodexTerminalKind::Failed, false, true),
            (
                "response.error",
                CodexTerminalKind::ResponseError,
                false,
                true,
            ),
            (
                "response.cancelled",
                CodexTerminalKind::Cancelled,
                false,
                true,
            ),
            ("error", CodexTerminalKind::Error, false, true),
        ];

        for (event_type, expected, reusable, failure) in cases {
            let payload = serde_json::json!({"type": event_type});
            let actual = CodexTerminalKind::from_payload(&payload);
            assert_eq!(actual, Some(expected), "event type {event_type}");
            assert_eq!(expected.is_reusable(), reusable, "event type {event_type}");
            assert_eq!(expected.is_failure(), failure, "event type {event_type}");
        }

        assert_eq!(
            CodexTerminalKind::from_payload(
                &serde_json::json!({"type": "response.output_text.delta"})
            ),
            None
        );
    }

    #[test]
    fn terminal_snapshot_status_must_match_its_event_when_provided() {
        for (event_type, accepted, rejected) in [
            ("response.completed", "completed", "failed"),
            ("response.done", "completed", "incomplete"),
            ("response.incomplete", "incomplete", "completed"),
            ("response.failed", "failed", "completed"),
            ("response.error", "failed", "completed"),
            ("response.cancelled", "cancelled", "failed"),
            ("error", "failed", "completed"),
        ] {
            for status in [
                None,
                Some(Value::Null),
                Some(Value::String(accepted.into())),
            ] {
                let mut response = serde_json::json!({});
                if let Some(status) = status {
                    response["status"] = status;
                }
                let payload = serde_json::json!({"type":event_type, "response":response});
                validate_terminal_snapshot_status(&payload)
                    .unwrap_or_else(|error| panic!("{event_type}: {error}"));
            }

            let mismatch = serde_json::json!({
                "type":event_type,
                "response":{"status":rejected}
            });
            let error = validate_terminal_snapshot_status(&mismatch).unwrap_err();
            assert!(error.contains("response.status"), "{event_type}: {error}");

            let wrong_type = serde_json::json!({
                "type":event_type,
                "response":{"status":42}
            });
            assert!(validate_terminal_snapshot_status(&wrong_type).is_err());
        }
    }

    #[test]
    fn classifies_retryable_failure_kinds() {
        let rate = classify_event_failure(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true, "primary": {"reset_after_seconds": 1.5}}
        }))
        .unwrap();
        assert_eq!(rate.kind, CodexFailureKind::RateLimit);
        assert_eq!(rate.retry_after.as_deref(), Some("1.5"));

        let overload = classify_event_failure(&serde_json::json!({
            "type": "response.failed",
            "response": {"error": {"type": "overloaded_error", "message": "busy"}}
        }))
        .unwrap();
        assert_eq!(overload.status, 529);
        assert!(overload.retryable());
    }

    #[test]
    fn ignores_informational_and_permanent_events() {
        assert!(
            classify_event_failure(&serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": false}
            }))
            .is_none()
        );
        let failure = classify_event_failure(&serde_json::json!({
            "type": "error",
            "error": {"status": 400, "message": "bad request"}
        }))
        .unwrap();
        assert!(!failure.retryable());
    }

    #[test]
    fn completed_response_wins_over_trailing_rate_limit_telemetry() {
        let body = b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{}}}\n\ndata: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"limit_reached\":true,\"primary\":{\"reset_after_seconds\":0}}}\n\n";

        assert!(first_retryable_failure(body).is_none());
    }

    #[test]
    fn incomplete_response_wins_over_trailing_rate_limit_telemetry() {
        let body = b"data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{}}}\n\ndata: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"limit_reached\":true,\"primary\":{\"reset_after_seconds\":0}}}\n\n";

        assert!(first_retryable_failure(body).is_none());
    }

    #[test]
    fn rate_limit_without_completed_response_remains_retryable() {
        let body = b"data: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"limit_reached\":true,\"primary\":{\"reset_after_seconds\":0}}}\n\n";

        assert_eq!(
            first_retryable_failure(body).map(|failure| failure.status),
            Some(429)
        );
    }

    #[test]
    fn hosted_side_effect_blocks_buffered_model_replay() {
        for hosted in [
            r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"web_search_call","id":"ws_1"}}"#,
            r#"{"type":"response.web_search_call.searching","output_index":0}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"web_search_call","id":"ws_1"}}"#,
        ] {
            let body = format!(
                "data: {hosted}\n\ndata: {{\"type\":\"response.failed\",\"response\":{{\"status\":\"failed\",\"error\":{{\"status\":503,\"message\":\"busy\"}}}}}}\n\n"
            );
            assert!(first_retryable_failure(body.as_bytes()).is_none());
        }
    }

    #[test]
    fn malformed_protocol_bytes_never_trigger_retry_prescan() {
        let invalid_utf8 = b"data: {\"type\":\"response.failed\",\"error\":{\"status\":503,\"message\":\"busy\xff\"}}\n\n";
        assert!(first_retryable_failure(invalid_utf8).is_none());

        let later_bom = b"data: {\"type\":\"response.created\"}\n\n\xef\xbb\xbfdata: {\"type\":\"response.failed\",\"error\":{\"status\":503,\"message\":\"busy\"}}\n\n";
        assert!(first_retryable_failure(later_bom).is_none());
    }
}
