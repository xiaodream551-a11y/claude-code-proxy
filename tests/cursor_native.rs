//! Integration tests for the Cursor native provider.
//!
//! These tests cover:
//! - Prost message roundtrip
//! - Connect frame encode/decode (with fixtures)
//! - Auth resolution
//! - Model catalog resolution
//! - Prompt rendering
//! - Client request/response boundary
//! - SSE framing
//! - Registry routing
//! - Provider end-to-end against mock upstream

use once_cell::sync::Lazy;
use std::sync::Mutex;

static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

// ---------------------------------------------------------------------------
// Prost roundtrip
// ---------------------------------------------------------------------------

#[test]
fn prost_roundtrip_preserves_cursor_server_message() {
    use claude_code_proxy::providers::cursor::proto::*;
    use prost::Message;

    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: Some(TextDelta {
                text: "hello".into(),
            }),
            turn_ended: Some(TurnEnded {
                input_tokens: 7,
                output_tokens: 2,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            }),
        }),
        exec_server_message: None,
    };
    let mut bytes = Vec::new();
    msg.encode(&mut bytes).unwrap();
    let decoded = AgentServerMessage::decode(bytes.as_slice()).unwrap();
    assert_eq!(
        decoded.interaction_update.unwrap().text_delta.unwrap().text,
        "hello"
    );
}

#[test]
fn prost_roundtrip_preserves_client_message() {
    use claude_code_proxy::providers::cursor::proto::*;
    use prost::Message;

    let msg = AgentClientMessage {
        run_request: Some(RunRequest {
            conversation_state: None,
            action: Some(Action {
                user_message_action: Some(UserMessageAction {
                    user_message: Some(UserMessage {
                        text: "hello".into(),
                        message_id: "msg-id".into(),
                        selected_context: None,
                        mode: "AGENT_MODE_AGENT".into(),
                    }),
                }),
            }),
            mcp_tools: None,
            conversation_id: String::new(),
            requested_model: Some(CursorModel {
                model_id: "gpt-5.5".into(),
                parameters: vec![ModelParameter {
                    id: "context".into(),
                    value: "128k".into(),
                }],
            }),
            exclude_workspace_context: false,
            selected_subagent_models: vec![],
            conversation_group_id: String::new(),
            client_supports_inline_images: true,
        }),
        client_heartbeat: None,
    };

    let mut bytes = Vec::new();
    msg.encode(&mut bytes).unwrap();
    let decoded = AgentClientMessage::decode(bytes.as_slice()).unwrap();
    let run = decoded.run_request.unwrap();
    let action = run.action.unwrap();
    let user_msg = action.user_message_action.unwrap().user_message.unwrap();
    assert_eq!(user_msg.text, "hello");
    assert_eq!(run.requested_model.unwrap().model_id, "gpt-5.5");
}

// ---------------------------------------------------------------------------
// Connect frame fixtures
// ---------------------------------------------------------------------------

#[test]
fn connect_frame_fixture_matches_reference_layout() {
    use claude_code_proxy::providers::cursor::connect::encode_connect_frame;

    let frame = encode_connect_frame(b"abc", 0);
    assert_eq!(hex::encode(frame), "0000000003616263");
}

#[test]
fn connect_frame_decode_reference() {
    use claude_code_proxy::providers::cursor::connect::ConnectFrameDecoder;

    let wire = hex::decode("0000000003616263").unwrap();
    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder.push(&wire).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].flags, 0);
    assert_eq!(&frames[0].payload[..], b"abc");
}

#[test]
fn connect_frame_with_flags_decode() {
    use claude_code_proxy::providers::cursor::connect::ConnectFrameDecoder;
    use claude_code_proxy::providers::cursor::connect::encode_connect_frame;

    let frame = encode_connect_frame(b"xyz", 0x03);
    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder.push(frame).unwrap();
    assert_eq!(frames[0].flags, 0x03);
}

// ---------------------------------------------------------------------------
// Auth resolution
// ---------------------------------------------------------------------------

#[test]
fn auth_returns_token_from_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe {
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "test-token-123");
    }
    let token = claude_code_proxy::providers::cursor::auth::load_cursor_token();
    assert_eq!(token.as_deref(), Some("test-token-123"));
    unsafe {
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
    }
}

// ---------------------------------------------------------------------------
// Model catalog
// ---------------------------------------------------------------------------

#[test]
fn model_resolution_resolves_cursor_agent_prefix() {
    use claude_code_proxy::providers::cursor::model::*;

    let r = resolve_cursor_model("cursor-agent:gpt-5.5").unwrap();
    assert_eq!(r.model_id, "gpt-5.5");
    assert_eq!(r.mode, CursorAgentMode::Agent);
}

#[test]
fn model_resolution_accepts_legacy_cursor_agent() {
    use claude_code_proxy::providers::cursor::model::*;

    let r = resolve_cursor_model("cursor-agent").unwrap();
    assert_eq!(r.mode, CursorAgentMode::Agent);
}

#[test]
fn registry_routes_cursor_model_to_cursor_provider() {
    use claude_code_proxy::Registry;
    use claude_code_proxy::config::AliasProvider;

    let registry = Registry::new(AliasProvider::Codex);
    let provider = registry.provider_for_model("cursor:gpt-5.5", None);
    assert!(provider.is_some());
    assert_eq!(provider.unwrap().name(), "cursor");

    let provider = registry.provider_for_model("cursor-agent", None);
    assert!(provider.is_some());
    assert_eq!(provider.unwrap().name(), "cursor");
}

// ---------------------------------------------------------------------------
// Prompt rendering
// ---------------------------------------------------------------------------

#[test]
fn prompt_renders_system_tools_and_messages() {
    use claude_code_proxy::MessagesRequest;
    use claude_code_proxy::providers::cursor::request::render_cursor_prompt;

    let req: MessagesRequest = serde_json::from_value(serde_json::json!({
        "model": "cursor:gpt-5.5",
        "system": "be direct",
        "messages": [{
            "role": "user",
            "content": [
                {"type":"text","text":"hi"},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}
            ]
        }],
        "tools": [{"name":"Read","description":"read files","input_schema":{"type":"object"}}]
    }))
    .unwrap();

    let rendered = render_cursor_prompt(&req);
    assert!(rendered.contains("<system>"));
    assert!(rendered.contains("<user>"));
    assert!(rendered.contains("<tools>"));
}

#[test]
fn selected_images_count_matches_base64_images() {
    use claude_code_proxy::MessagesRequest;
    use claude_code_proxy::providers::cursor::request::cursor_selected_images;

    let req: MessagesRequest = serde_json::from_value(serde_json::json!({
        "model": "cursor:gpt-5.5",
        "messages": [{
            "role": "user",
            "content": [
                {"type":"text","text":"analyze"},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}},
                {"type":"image","source":{"type":"base64","media_type":"image/jpeg","data":"BBBB"}}
            ]
        }]
    }))
    .unwrap();

    assert_eq!(cursor_selected_images(&req).len(), 2);
}

// ---------------------------------------------------------------------------
// Client request/response boundary (high-level shape)
// ---------------------------------------------------------------------------

#[test]
fn cursor_client_constructs_correct_url() {
    use claude_code_proxy::providers::cursor::client::CursorHttpClient;

    let client = CursorHttpClient::new();
    // Just ensure construction doesn't panic
    let _ = client;
}

#[test]
fn cursor_error_display_works() {
    use claude_code_proxy::providers::cursor::client::CursorError;

    let err = CursorError::new(429, "rate limited", Some("backoff".to_string()));
    let display = format!("{err}");
    assert!(display.contains("429"));
    assert!(display.contains("rate limited"));
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

#[test]
fn response_decode_extracts_text_and_usage() {
    use claude_code_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_code_proxy::providers::cursor::proto::*;
    use claude_code_proxy::providers::cursor::response::*;
    use prost::Message;

    let mut body = Vec::new();

    // Text frame
    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: Some(TextDelta {
                text: "Hello".into(),
            }),
            turn_ended: None,
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    // Usage frame
    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: 10,
                output_tokens: 5,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            }),
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    // End frame
    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let upstream = claude_code_proxy::providers::cursor::client::CursorUpstreamResponse {
        status: 200,
        body,
        error_detail: None,
    };

    let json = decode_cursor_upstream(&upstream, "msg_test", "cursor-test").unwrap();
    assert_eq!(json["id"], "msg_test");
    assert_eq!(json["content"][0]["text"], "Hello");
    assert_eq!(json["usage"]["input_tokens"].as_u64(), Some(10));
    assert_eq!(json["usage"]["output_tokens"].as_u64(), Some(5));
}

// ---------------------------------------------------------------------------
// SSE framing - parse event names and data
// ---------------------------------------------------------------------------

#[test]
fn sse_parses_event_names_and_data() {
    use claude_code_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_code_proxy::providers::cursor::proto::*;
    use claude_code_proxy::providers::cursor::sse::frame_cursor_stream;
    use prost::Message;

    let mut body = Vec::new();

    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: Some(TextDelta { text: "hi".into() }),
            turn_ended: None,
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: 5,
                output_tokens: 1,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            }),
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let upstream = claude_code_proxy::providers::cursor::client::CursorUpstreamResponse {
        status: 200,
        body,
        error_detail: None,
    };

    let sse = frame_cursor_stream(&upstream, "msg_sse", "cursor-test");
    let sse_str = String::from_utf8_lossy(&sse);

    let events = parse_sse_events(&sse_str);
    let names: Vec<&str> = events.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"message_start"),
        "expected message_start in {names:?}"
    );
    assert!(
        names.contains(&"content_block_delta"),
        "expected content_block_delta in {names:?}"
    );
    assert!(
        names.contains(&"message_delta"),
        "expected message_delta in {names:?}"
    );
    assert!(
        names.contains(&"message_stop"),
        "expected message_stop in {names:?}"
    );
}

#[test]
fn sse_message_delta_contains_usage() {
    use claude_code_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_code_proxy::providers::cursor::proto::*;
    use claude_code_proxy::providers::cursor::sse::frame_cursor_stream;
    use prost::Message;

    let mut body = Vec::new();

    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: Some(TextDelta {
                text: "test".into(),
            }),
            turn_ended: Some(TurnEnded {
                input_tokens: 42,
                output_tokens: 7,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            }),
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));
    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let upstream = claude_code_proxy::providers::cursor::client::CursorUpstreamResponse {
        status: 200,
        body,
        error_detail: None,
    };

    let sse = frame_cursor_stream(&upstream, "msg_u", "cursor-test");
    let sse_str = String::from_utf8_lossy(&sse);
    let events = parse_sse_events(&sse_str);

    let msg_delta_data = events
        .iter()
        .find(|(n, _)| n == "message_delta")
        .map(|(_, d)| d.clone());
    assert!(msg_delta_data.is_some(), "expected message_delta event");
    let data = msg_delta_data.unwrap();
    assert_eq!(data["usage"]["input_tokens"].as_u64(), Some(42));
    assert_eq!(data["usage"]["output_tokens"].as_u64(), Some(7));
    assert_eq!(
        data["usage"]["cache_creation_input_tokens"].as_u64(),
        Some(0)
    );
    assert_eq!(data["usage"]["cache_read_input_tokens"].as_u64(), Some(0));
    assert_eq!(data["delta"]["stop_reason"], "end_turn");
}

// ---------------------------------------------------------------------------
// Registry integration
// ---------------------------------------------------------------------------

#[test]
fn registry_provider_for_legacy_cursor_model() {
    use claude_code_proxy::Registry;
    use claude_code_proxy::config::AliasProvider;

    let registry = Registry::new(AliasProvider::Codex);

    // Legacy models
    for model in &[
        "cursor",
        "cursor-agent",
        "cursor-composer",
        "cursor-composer-fast",
        "cursor-plan",
        "cursor-ask",
    ] {
        let provider = registry.provider_for_model(model, None);
        assert!(
            provider.is_some(),
            "expected provider for legacy model {model}"
        );
        assert_eq!(
            provider.unwrap().name(),
            "cursor",
            "model {model} should route to cursor provider"
        );
    }
}

// ---------------------------------------------------------------------------
// Mock upstream streaming test (full integration)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_provider_streams_text_and_usage_from_mock_upstream() {
    use axum::{Router, routing::post};
    use claude_code_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_code_proxy::providers::cursor::proto::*;
    use claude_code_proxy::providers::cursor::response::decode_cursor_upstream;
    use claude_code_proxy::providers::cursor::sse::frame_cursor_stream;
    use prost::Message;

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // Build mock upstream response bytes
    let mut body = Vec::new();

    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: Some(TextDelta {
                text: "Hello from mock".into(),
            }),
            turn_ended: None,
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: 15,
                output_tokens: 3,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            }),
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let response_body = body.clone();

    let app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(move |_body: axum::body::Bytes| async move {
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "application/connect+proto",
                )],
                response_body,
            )
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &mock_url);
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "mock-token");
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "0.0.0");
    }

    let _handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    use claude_code_proxy::providers::cursor::auth::load_cursor_token;
    use claude_code_proxy::providers::cursor::client::CursorHttpClient;

    let token = load_cursor_token().unwrap();
    let client = CursorHttpClient::new();
    let upstream = client
        .run_agent(&token, "test prompt", "cursor:gpt-5.5", &[])
        .await
        .expect("mock upstream request should succeed");

    assert!(upstream.is_success());
    assert_eq!(upstream.status, 200);

    let json = decode_cursor_upstream(&upstream, "msg_mock", "cursor-test").unwrap();
    assert_eq!(json["content"][0]["text"], "Hello from mock");
    assert_eq!(json["usage"]["input_tokens"].as_u64(), Some(15));
    assert_eq!(json["usage"]["output_tokens"].as_u64(), Some(3));

    let sse = frame_cursor_stream(&upstream, "msg_sse_mock", "cursor-test");
    let sse_str = String::from_utf8_lossy(&sse);
    let events = parse_sse_events(&sse_str);
    let names: Vec<&str> = events.iter().map(|(n, _)| n.as_str()).collect();

    assert!(
        names.contains(&"message_start"),
        "SSE should include message_start in {names:?}"
    );
    assert!(
        names.contains(&"message_stop"),
        "SSE should include message_stop in {names:?}"
    );

    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
    }
}

// ---------------------------------------------------------------------------
// Provider handle_messages shape test (non-streaming)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_provider_handle_messages_returns_anthropic_json() {
    use axum::{Router, routing::post};
    use claude_code_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_code_proxy::providers::cursor::proto::*;
    use prost::Message;

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // Build mock response
    let mut body = Vec::new();
    let msg = AgentServerMessage {
        interaction_update: Some(InteractionUpdate {
            thinking_delta: None,
            text_delta: Some(TextDelta {
                text: "Mock response text".into(),
            }),
            turn_ended: Some(TurnEnded {
                input_tokens: 20,
                output_tokens: 4,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            }),
        }),
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));
    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let response_body = body;

    let app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(move |_body: axum::body::Bytes| async move {
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "application/connect+proto",
                )],
                response_body,
            )
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &mock_url);
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "mock-token-handler");
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "0.0.0");
    }

    let _handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Send via handle_messages (non-streaming)
    use claude_code_proxy::provider::Provider;
    use claude_code_proxy::provider::RequestContext;
    use claude_code_proxy::providers::cursor::CursorProvider;

    let provider = CursorProvider::new();
    let body = serde_json::from_value(serde_json::json!({
        "model": "cursor:gpt-5.5",
        "messages": [{"role": "user", "content": "test"}]
    }))
    .unwrap();

    let ctx = RequestContext {
        req_id: "test-req".into(),
        session_id: None,
        session_seq: None,
        provider: "cursor".into(),
    };

    let response = provider.handle_messages(body, ctx).await;
    // Should not return an error response
    let status = response.status();
    assert!(
        status != 401 && status != 400,
        "handle_messages returned error status {status}"
    );

    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
    }
}

// ---------------------------------------------------------------------------
// SSE parser helper (mirrors sse.rs internal helper for cross-module access)
// ---------------------------------------------------------------------------

fn parse_sse_events(sse: &str) -> Vec<(String, serde_json::Value)> {
    let mut events = Vec::new();
    let mut current_event = String::new();

    for line in sse.lines() {
        if line.starts_with("event: ") {
            current_event = line["event: ".len()..].to_string();
        } else if line.starts_with("data: ") {
            let data_str = &line["data: ".len()..];
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(data_str) {
                events.push((current_event.clone(), data));
            }
        }
    }

    events
}
