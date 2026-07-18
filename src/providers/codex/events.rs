use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexTerminalKind {
    Completed,
    Done,
    Incomplete,
    Failed,
    ResponseError,
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
            Some("error") => Some(Self::Error),
            _ => None,
        }
    }

    pub(crate) fn is_failure(self) -> bool {
        matches!(self, Self::Failed | Self::ResponseError | Self::Error)
    }

    pub(crate) fn is_reusable(self) -> bool {
        matches!(self, Self::Completed | Self::Done)
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
    for event in crate::anthropic::sse::parse_sse_events(body) {
        if event.data == "[DONE]" {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
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
            && first_failure.is_none()
        {
            first_failure = Some(failure);
        }
    }
    first_failure
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
}
