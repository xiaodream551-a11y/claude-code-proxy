use std::collections::HashSet;

use serde::Serialize;
use serde_json::Value;

use crate::anthropic::schema::{Message, MessagesRequest};
use crate::providers::translate_shared::{
    ImageSource, flatten_system_text, image_source_to_url, is_claude_code_compaction_request,
    normalize_strict_json_schema, read_effort,
};

const MAX_GROK_IMAGE_BYTES: u64 = 20 * 1024 * 1024;

#[derive(Debug, Clone, Serialize)]
pub struct GrokResponsesRequest {
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    pub input: Vec<GrokInputItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<GrokTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<GrokToolChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    pub store: bool,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<GrokReasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<GrokText>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GrokReasoning {
    pub effort: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GrokText {
    pub format: GrokTextFormat,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GrokTextFormat {
    JsonSchema {
        name: String,
        schema: Value,
        strict: bool,
    },
    JsonObject,
    Text,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum GrokInputItem {
    #[serde(rename = "message")]
    Message {
        role: String,
        content: Vec<GrokContentPart>,
    },
    #[serde(rename = "function_call")]
    FunctionCall {
        call_id: String,
        name: String,
        arguments: String,
    },
    #[serde(rename = "function_call_output")]
    FunctionCallOutput { call_id: String, output: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum GrokContentPart {
    #[serde(rename = "input_text")]
    InputText { text: String },
    #[serde(rename = "input_image")]
    InputImage { image_url: String },
    #[serde(rename = "output_text")]
    OutputText { text: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct GrokTool {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_x_handles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excluded_x_handles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_date: Option<String>,
}

impl GrokTool {
    fn hosted(kind: &str) -> Self {
        Self {
            kind: kind.into(),
            name: None,
            description: None,
            parameters: None,
            allowed_x_handles: None,
            excluded_x_handles: None,
            from_date: None,
            to_date: None,
        }
    }

    fn function(name: &str, description: Option<String>, parameters: Value) -> Self {
        Self {
            kind: "function".into(),
            name: Some(name.into()),
            description,
            parameters: Some(parameters),
            allowed_x_handles: None,
            excluded_x_handles: None,
            from_date: None,
            to_date: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum GrokToolChoice {
    Auto(String),
    Required(String),
    None(String),
    Function { r#type: String, name: String },
}

pub fn translate_request(
    req: &MessagesRequest,
    model: String,
) -> anyhow::Result<GrokResponsesRequest> {
    reject_unknown_top_level(req)?;
    let reasoning_effort = read_effort(req)?;
    let compaction = is_claude_code_compaction_request(req);
    let text = read_output_format(req).map(|format| GrokText { format });
    let internal_text_request = compaction || text.is_some();
    let mut instructions = parse_system(req.extra.get("system"))?;
    let mut tools = if compaction {
        None
    } else {
        parse_tools(req.extra.get("tools"))?
    };
    let hosted_web_search = tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|tool| tool.kind == "web_search"));
    let dedicated_x_search = tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(|tool| tool.kind == "x_search"));
    let x_search_intent = !internal_text_request && requests_x_search(req);
    let force_x_search = dedicated_x_search || x_search_intent;
    let force_web_search =
        !force_x_search && !internal_text_request && hosted_web_search && requests_web_search(req);
    if force_x_search {
        tools = Some(vec![GrokTool::hosted("x_search")]);
    } else if force_web_search {
        tools = Some(vec![GrokTool::hosted("web_search")]);
    } else if !internal_text_request {
        let tools = tools.get_or_insert_default();
        if !tools.iter().any(|tool| tool.kind == "x_search") {
            tools.push(GrokTool::hosted("x_search"));
        }
    }
    if !internal_text_request && hosted_web_search {
        append_guidance(
            &mut instructions,
            "For general web searches, use the hosted web_search tool. Do not use shell commands, HTTP clients, or local tools to search the web.",
        );
    }
    if !internal_text_request {
        append_guidance(
            &mut instructions,
            "For requests to search X or Twitter, use the hosted x_search tool. XSearch accepts a query and supports allowed_x_handles, excluded_x_handles, from_date, and to_date filters. Do not use Bash, curl, HTTP clients, or general web_search for X searches.",
        );
    }
    let tool_choice = if compaction {
        None
    } else if force_x_search || force_web_search {
        Some(GrokToolChoice::Required("required".into()))
    } else {
        parse_tool_choice(req.extra.get("tool_choice"), tools.as_ref())?
    };
    let parallel_tool_calls = if compaction {
        None
    } else {
        req.extra
            .get("tool_choice")
            .and_then(Value::as_object)
            .and_then(|choice| choice.get("disable_parallel_tool_use"))
            .and_then(Value::as_bool)
            .map(|disabled| !disabled)
    };
    let mut call_ids = HashSet::new();
    let mut input = Vec::new();
    for message in &req.messages {
        parse_message(message, &mut input, &mut call_ids)?;
    }
    let reasoning = if model == "grok-4.5" {
        reasoning_effort.map(|effort| GrokReasoning {
            effort: match effort {
                "max" | "xhigh" => "high",
                value => value,
            }
            .to_string(),
        })
    } else {
        None
    };
    Ok(GrokResponsesRequest {
        model,
        instructions,
        input,
        tools,
        tool_choice,
        parallel_tool_calls,
        store: false,
        stream: true,
        max_output_tokens: req.max_tokens,
        reasoning,
        text,
    })
}

fn read_output_format(req: &MessagesRequest) -> Option<GrokTextFormat> {
    let output_config = req.extra.get("output_config")?.as_object()?;
    let format = output_config.get("format")?.as_object()?;
    match format.get("type")?.as_str()? {
        "json_schema" => Some(GrokTextFormat::JsonSchema {
            name: format
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("response")
                .to_string(),
            schema: normalize_strict_json_schema(format.get("schema")?),
            strict: true,
        }),
        "json_object" => Some(GrokTextFormat::JsonObject),
        _ => Some(GrokTextFormat::Text),
    }
}

fn append_guidance(instructions: &mut Option<String>, guidance: &str) {
    *instructions = Some(match instructions.take() {
        Some(existing) if !existing.is_empty() => format!("{existing}\n\n{guidance}"),
        _ => guidance.into(),
    });
}

fn latest_user_text(req: &MessagesRequest) -> Option<String> {
    let message = req
        .messages
        .iter()
        .rev()
        .find(|message| message.role == "user")?;
    match &message.content {
        Value::String(text) => Some(text.to_ascii_lowercase()),
        Value::Array(blocks) => Some(
            blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase(),
        ),
        _ => None,
    }
}

fn requests_x_search(req: &MessagesRequest) -> bool {
    let Some(text) = latest_user_text(req) else {
        return false;
    };
    [
        "search x for",
        "search on x",
        "search twitter",
        "search tweets",
        "x search",
        "posts on x",
        "posts from x",
        "tweets about",
        "twitter posts",
    ]
    .iter()
    .any(|phrase| text.contains(phrase))
}

fn requests_web_search(req: &MessagesRequest) -> bool {
    let Some(text) = latest_user_text(req) else {
        return false;
    };
    [
        "search online",
        "search the web",
        "web search",
        "look up online",
        "look up on the web",
    ]
    .iter()
    .any(|phrase| text.contains(phrase))
}

fn reject_unknown_top_level(req: &MessagesRequest) -> anyhow::Result<()> {
    for key in req.extra.keys() {
        if ![
            "system",
            "tools",
            "tool_choice",
            "context_management",
            "metadata",
            "output_config",
            "thinking",
            "temperature",
            "top_p",
            "top_k",
            "stop_sequences",
            "service_tier",
        ]
        .contains(&key.as_str())
        {
            anyhow::bail!("unsupported Grok request field: {key}");
        }
    }
    Ok(())
}

fn parse_system(value: Option<&Value>) -> anyhow::Result<Option<String>> {
    let Some(value) = value else { return Ok(None) };
    match value {
        Value::String(_) => Ok(flatten_system_text(Some(value))),
        Value::Array(blocks) => {
            for block in blocks {
                let object = block
                    .as_object()
                    .ok_or_else(|| anyhow::anyhow!("system content must contain text blocks"))?;
                if object
                    .keys()
                    .any(|key| !["type", "text", "cache_control"].contains(&key.as_str()))
                    || object.get("type").and_then(Value::as_str) != Some("text")
                    || !valid_cache_control(object.get("cache_control"))
                {
                    anyhow::bail!("unsupported system block");
                }
                if object.get("text").and_then(Value::as_str).is_none() {
                    anyhow::bail!("system text is invalid");
                }
            }
            Ok(flatten_system_text(Some(value)))
        }
        _ => anyhow::bail!("system must be text"),
    }
}

fn parse_tools(value: Option<&Value>) -> anyhow::Result<Option<Vec<GrokTool>>> {
    let Some(value) = value else { return Ok(None) };
    let tools = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("tools must be an array"))?;
    let mut names = HashSet::new();
    let mut out = Vec::new();
    for tool in tools {
        let obj = tool
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("tool must be an object"))?;
        for key in obj.keys() {
            if ![
                "type",
                "name",
                "description",
                "input_schema",
                "cache_control",
            ]
            .contains(&key.as_str())
            {
                anyhow::bail!("unsupported tool field: {key}");
            }
        }
        let declared_type = obj.get("type").map(|kind| {
            kind.as_str()
                .filter(|kind| !kind.is_empty())
                .ok_or_else(|| anyhow::anyhow!("tool type is invalid"))
        });
        let declared_type = declared_type.transpose()?;
        if !valid_cache_control(obj.get("cache_control")) {
            anyhow::bail!("unsupported tool cache_control");
        }
        let name = obj
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("tool name is invalid"))?;
        if !names.insert(name.to_string()) {
            anyhow::bail!("duplicate tool name");
        }
        if name == "WebSearch" || declared_type.is_some_and(|kind| kind.starts_with("web_search_"))
        {
            out.push(GrokTool::hosted("web_search"));
            continue;
        }
        if name == "XSearch" {
            out.push(GrokTool::hosted("x_search"));
            continue;
        }
        let parameters = obj
            .get("input_schema")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("tool input_schema must be an object"))?;
        out.push(GrokTool::function(
            name,
            obj.get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
            parameters,
        ));
    }
    Ok(Some(out))
}

fn parse_tool_choice(
    value: Option<&Value>,
    tools: Option<&Vec<GrokTool>>,
) -> anyhow::Result<Option<GrokToolChoice>> {
    let Some(value) = value else { return Ok(None) };
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("tool_choice must be an object"))?;
    let kind = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("tool_choice type is invalid"))?;
    if let Some(disable_parallel) = obj.get("disable_parallel_tool_use")
        && !disable_parallel.is_boolean()
    {
        anyhow::bail!("tool_choice disable_parallel_tool_use must be boolean");
    }
    match kind {
        "auto" | "any" | "none"
            if obj
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "disable_parallel_tool_use")) =>
        {
            Ok(Some(match kind {
                "auto" => GrokToolChoice::Auto("auto".into()),
                "any" => GrokToolChoice::Required("required".into()),
                "none" => GrokToolChoice::None("none".into()),
                _ => unreachable!(),
            }))
        }
        "tool"
            if obj.keys().all(|key| {
                matches!(key.as_str(), "type" | "name" | "disable_parallel_tool_use")
            }) =>
        {
            let name = obj
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("tool_choice name is invalid"))?;
            if !tools
                .is_some_and(|items| items.iter().any(|tool| tool.name.as_deref() == Some(name)))
            {
                anyhow::bail!("tool_choice references an unknown tool");
            }
            Ok(Some(GrokToolChoice::Function {
                r#type: "function".into(),
                name: name.into(),
            }))
        }
        _ => anyhow::bail!("unsupported tool_choice"),
    }
}

fn parse_message(
    message: &Message,
    out: &mut Vec<GrokInputItem>,
    calls: &mut HashSet<String>,
) -> anyhow::Result<()> {
    if !["system", "user", "assistant"].contains(&message.role.as_str()) {
        anyhow::bail!("unsupported message role");
    }
    let blocks: Vec<Value> = match &message.content {
        Value::String(text) => vec![serde_json::json!({"type":"text", "text":text})],
        Value::Array(items) => items.clone(),
        _ => anyhow::bail!("message content must be text or blocks"),
    };
    let mut content = Vec::new();
    for block in blocks {
        let object = block
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("content block must be an object"))?;
        let typ = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("content block type is invalid"))?;
        match (message.role.as_str(), typ) {
            (_, "thinking") | (_, "redacted_thinking") => {}
            (_, "text") => {
                if object
                    .keys()
                    .any(|key| !["type", "text", "cache_control"].contains(&key.as_str()))
                    || !valid_cache_control(object.get("cache_control"))
                {
                    anyhow::bail!("unsupported text block field");
                }
                let text = object
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("text block is invalid"))?;
                content.push(if message.role == "assistant" {
                    GrokContentPart::OutputText { text: text.into() }
                } else {
                    GrokContentPart::InputText { text: text.into() }
                });
            }
            ("user", "image") => {
                if object
                    .keys()
                    .any(|key| !["type", "source", "cache_control"].contains(&key.as_str()))
                    || !valid_cache_control(object.get("cache_control"))
                {
                    anyhow::bail!("unsupported image block field");
                }
                let source = parse_image_source(object.get("source"))?;
                content.push(GrokContentPart::InputImage {
                    image_url: image_source_to_url(&source),
                });
            }
            ("assistant", "server_tool_use") => {
                let name = object.get("name").and_then(Value::as_str);
                if !matches!(name, Some("web_search" | "x_search")) {
                    anyhow::bail!("unsupported server tool use");
                }
            }
            ("assistant", "web_search_tool_result" | "x_search_tool_result")
            | ("user", "web_search_tool_result" | "x_search_tool_result") => {}
            ("assistant", "tool_use") => {
                if object.keys().any(|key| {
                    !["type", "id", "name", "input", "cache_control"].contains(&key.as_str())
                }) || !valid_cache_control(object.get("cache_control"))
                {
                    anyhow::bail!("unsupported tool_use field");
                }
                flush_message(&message.role, &mut content, out);
                let id = object
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| anyhow::anyhow!("tool call id is invalid"))?;
                let name = object
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| anyhow::anyhow!("tool call name is invalid"))?;
                let input = object
                    .get("input")
                    .filter(|value| value.is_object())
                    .ok_or_else(|| anyhow::anyhow!("tool call input must be an object"))?;
                if !calls.insert(id.into()) {
                    anyhow::bail!("duplicate tool call id");
                }
                out.push(GrokInputItem::FunctionCall {
                    call_id: id.into(),
                    name: name.into(),
                    arguments: serde_json::to_string(input)?,
                });
            }
            ("user", "tool_result") => {
                if object.keys().any(|key| {
                    ![
                        "type",
                        "tool_use_id",
                        "content",
                        "is_error",
                        "cache_control",
                    ]
                    .contains(&key.as_str())
                }) {
                    anyhow::bail!("unsupported tool_result field");
                }
                if let Some(is_error) = object.get("is_error")
                    && !is_error.is_boolean()
                {
                    anyhow::bail!("tool result is_error must be boolean");
                }
                flush_message(&message.role, &mut content, out);
                let id = object
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| anyhow::anyhow!("tool result id is invalid"))?;
                if !calls.remove(id) {
                    anyhow::bail!("tool result references an unknown or resolved tool call");
                }
                let value = object
                    .get("content")
                    .ok_or_else(|| anyhow::anyhow!("tool result content is required"))?;
                let output = match value {
                    Value::String(text) => text.clone(),
                    Value::Array(parts) => parts
                        .iter()
                        .map(|part| {
                            let part = part.as_object().ok_or_else(|| {
                                anyhow::anyhow!("tool result child must be an object")
                            })?;
                            if part.get("type").and_then(Value::as_str) != Some("text")
                                || part.keys().any(|key| {
                                    !["type", "text", "cache_control"].contains(&key.as_str())
                                })
                                || !valid_cache_control(part.get("cache_control"))
                            {
                                anyhow::bail!("tool result supports text children only");
                            }
                            part.get("text")
                                .and_then(Value::as_str)
                                .ok_or_else(|| anyhow::anyhow!("tool result text is invalid"))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?
                        .join(""),
                    _ => anyhow::bail!("tool result supports text only"),
                };
                let output = if object
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    if output.is_empty() {
                        "[tool execution error]".to_string()
                    } else {
                        format!("[tool execution error]\n{output}")
                    }
                } else {
                    output
                };
                out.push(GrokInputItem::FunctionCallOutput {
                    call_id: id.into(),
                    output,
                });
            }
            _ => anyhow::bail!("unsupported content block: {typ}"),
        }
    }
    flush_message(&message.role, &mut content, out);
    Ok(())
}

fn parse_image_source(value: Option<&Value>) -> anyhow::Result<ImageSource> {
    let source = value
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("image source must be an object"))?;
    let source_type = source
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("image source type is invalid"))?;
    match source_type {
        "base64" => {
            if !source
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "media_type" | "data"))
            {
                anyhow::bail!("unsupported base64 image source field");
            }
            let media_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .filter(|value| matches!(*value, "image/jpeg" | "image/png"))
                .ok_or_else(|| anyhow::anyhow!("unsupported image media type"))?;
            let data = source
                .get("data")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow::anyhow!("image base64 data is empty"))?;
            let mut decoder = base64::read::DecoderReader::new(
                data.as_bytes(),
                &base64::engine::general_purpose::STANDARD,
            );
            let decoded_bytes = std::io::copy(&mut decoder, &mut std::io::sink())
                .map_err(|_| anyhow::anyhow!("image base64 data is invalid"))?;
            if decoded_bytes > MAX_GROK_IMAGE_BYTES {
                anyhow::bail!("image exceeds the 20 MiB size limit");
            }
            Ok(ImageSource {
                media_type: media_type.to_string(),
                data: data.to_string(),
                source_type: source_type.to_string(),
            })
        }
        "url" => {
            if !source
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "url"))
            {
                anyhow::bail!("unsupported URL image source field");
            }
            let raw = source
                .get("url")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow::anyhow!("image URL is empty"))?;
            let parsed =
                url::Url::parse(raw).map_err(|_| anyhow::anyhow!("image URL is invalid"))?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
            {
                anyhow::bail!("image URL must be an HTTP(S) URL without credentials");
            }
            Ok(ImageSource {
                media_type: String::new(),
                data: raw.to_string(),
                source_type: source_type.to_string(),
            })
        }
        _ => anyhow::bail!("unsupported image source type"),
    }
}

fn valid_cache_control(value: Option<&Value>) -> bool {
    let Some(value) = value else { return true };
    let Some(object) = value.as_object() else {
        return false;
    };
    object.keys().all(|key| key == "type" || key == "ttl")
        && object.get("type").and_then(Value::as_str) == Some("ephemeral")
        && object
            .get("ttl")
            .is_none_or(|ttl| matches!(ttl.as_str(), Some("5m") | Some("1h")))
}

fn flush_message(role: &str, content: &mut Vec<GrokContentPart>, out: &mut Vec<GrokInputItem>) {
    if !content.is_empty() {
        out.push(GrokInputItem::Message {
            role: role.into(),
            content: std::mem::take(content),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grok_translation_filters_billing_header_and_separates_system_blocks() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "system":[
                {"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.212"},
                {"type":"text","text":"Follow repository rules."},
                {"type":"text","text":"Keep answers concise.","cache_control":{"type":"ephemeral"}}
            ],
            "messages":[{"role":"user","content":"hello"}]
        }))
        .unwrap();

        let translated = translate_request(&request, "grok-4.5".into()).unwrap();
        let instructions = translated.instructions.unwrap();
        assert!(instructions.starts_with("Follow repository rules.\n\nKeep answers concise."));
        assert!(!instructions.contains("x-anthropic-billing-header:"));
    }

    #[test]
    fn grok_translation_accepts_tool_type_and_disable_parallel_tool_use() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "tools":[{
                "type":"custom",
                "name":"lookup",
                "description":"Look things up",
                "input_schema":{"type":"object"}
            }],
            "tool_choice":{"type":"auto","disable_parallel_tool_use":true},
            "messages":[{"role":"user","content":"hello"}]
        }))
        .unwrap();

        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert!(
            translated["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| { tool["type"] == "function" && tool["name"] == "lookup" })
        );
        assert_eq!(translated["tool_choice"], "auto");
        assert_eq!(translated["parallel_tool_calls"], false);
    }

    #[test]
    fn grok_translation_maps_typed_anthropic_web_search() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "tools":[{
                "type":"web_search_20250305",
                "name":"web_search"
            }],
            "messages":[{"role":"user","content":"search online for the project"}]
        }))
        .unwrap();

        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(
            translated["tools"],
            serde_json::json!([{"type":"web_search"}])
        );
        assert_eq!(translated["tool_choice"], "required");
    }

    #[test]
    fn grok_translation_rejects_invalid_tool_type_and_parallel_flag() {
        for request in [
            serde_json::json!({
                "model":"grok-4.5",
                "tools":[{"type":1,"name":"lookup","input_schema":{"type":"object"}}],
                "messages":[{"role":"user","content":"hello"}]
            }),
            serde_json::json!({
                "model":"grok-4.5",
                "tools":[{"name":"lookup","input_schema":{"type":"object"}}],
                "tool_choice":{"type":"auto","disable_parallel_tool_use":"yes"},
                "messages":[{"role":"user","content":"hello"}]
            }),
        ] {
            let request: MessagesRequest = serde_json::from_value(request).unwrap();
            assert!(translate_request(&request, "grok-4.5".into()).is_err());
        }
    }

    #[test]
    fn grok_translation_preserves_tool_result_error_semantics() {
        let request = request_with_blocks(serde_json::json!([{
            "type":"tool_result",
            "tool_use_id":"call_1",
            "content":"permission denied",
            "is_error":true
        }]));

        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(
            translated["input"][1]["output"],
            "[tool execution error]\npermission denied"
        );
    }

    #[test]
    fn grok_translation_maps_base64_and_url_images() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[{"role":"user","content":[
                {"type":"text","text":"compare these"},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGVsbG8="},"cache_control":{"type":"ephemeral"}},
                {"type":"image","source":{"type":"url","url":"https://example.com/image.png?size=large"}}
            ]}]
        }))
        .unwrap();

        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        let content = translated["input"][0]["content"].as_array().unwrap();
        assert_eq!(content[1]["type"], "input_image");
        assert_eq!(content[1]["image_url"], "data:image/png;base64,aGVsbG8=");
        assert_eq!(content[2]["type"], "input_image");
        assert_eq!(
            content[2]["image_url"],
            "https://example.com/image.png?size=large"
        );
        assert!(!translated.to_string().contains("cache_control"));
    }

    #[test]
    fn grok_translation_rejects_unsafe_or_malformed_images() {
        for source in [
            serde_json::json!({"type":"base64","media_type":"image/svg+xml","data":"aGVsbG8="}),
            serde_json::json!({"type":"base64","media_type":"image/gif","data":"R0lGODlhAQABAAAAACw="}),
            serde_json::json!({"type":"base64","media_type":"image/webp","data":"UklGRgAAAABXRUJQ"}),
            serde_json::json!({"type":"base64","media_type":"image/png","data":"not base64"}),
            serde_json::json!({"type":"url","url":"file:///tmp/image.png"}),
            serde_json::json!({"type":"url","url":"https://user:pass@example.com/image.png"}),
            serde_json::json!({"type":"file","file_id":"file_1"}),
        ] {
            let request: MessagesRequest = serde_json::from_value(serde_json::json!({
                "model":"grok-4.5",
                "messages":[{"role":"user","content":[{"type":"image","source":source}]}]
            }))
            .unwrap();
            assert!(translate_request(&request, "grok-4.5".into()).is_err());
        }
    }

    #[test]
    fn grok_translation_replays_hosted_search_history() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[
                {"role":"user","content":"search X for the project"},
                {"role":"assistant","content":[
                    {"type":"server_tool_use","id":"srvtoolu_1","name":"x_search","input":{"query":"project"}},
                    {"type":"x_search_tool_result","tool_use_id":"srvtoolu_1","content":[]},
                    {"type":"text","text":"Found it"}
                ]},
                {"role":"user","content":"summarize it"}
            ]
        }))
        .unwrap();
        let translated = translate_request(&request, "grok-4.5".into()).unwrap();
        let value = serde_json::to_value(translated).unwrap();
        assert!(value["input"].as_array().unwrap().iter().any(|item| {
            item["role"] == "assistant" && item["content"][0]["text"] == "Found it"
        }));
        assert!(!value.to_string().contains("srvtoolu_1"));
    }

    #[test]
    fn grok_translation_maps_text_and_function_round_trip() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5", "max_tokens":12, "system":"rules",
            "tools":[{"name":"lookup","input_schema":{"type":"object"}}],
            "tool_choice":{"type":"tool","name":"lookup"},
            "messages":[
              {"role":"user","content":"hello"},
              {"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"lookup","input":{"q":"a"}}]},
              {"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"result"}]}
            ]
        })).unwrap();
        let value =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert!(value["instructions"].as_str().unwrap().starts_with("rules"));
        assert_eq!(value["input"][1]["type"], "function_call");
        assert_eq!(value["input"][2]["type"], "function_call_output");
        assert_eq!(value["tool_choice"]["type"], "function");
    }

    #[test]
    fn grok_translation_forwards_supported_reasoning_effort() {
        for (requested, expected) in [
            ("low", "low"),
            ("medium", "medium"),
            ("high", "high"),
            ("xhigh", "high"),
            ("max", "high"),
        ] {
            let request: MessagesRequest = serde_json::from_value(serde_json::json!({
                "model":"grok-4.5",
                "messages":[{"role":"user","content":"hello"}],
                "output_config":{"effort":requested}
            }))
            .unwrap();
            let translated =
                serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap())
                    .unwrap();
            assert_eq!(translated["reasoning"]["effort"], expected);
        }
    }

    #[test]
    fn grok_translation_forwards_title_schema_without_search_tools() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[{"role":"user","content":"Generate a concise session title"}],
            "output_config": {"format": {
                "type": "json_schema",
                "name": "session_title",
                "schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "short": {"type": "boolean"}
                    },
                    "required": ["title"]
                }
            }}
        }))
        .unwrap();

        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(translated["text"]["format"]["type"], "json_schema");
        assert_eq!(translated["text"]["format"]["name"], "session_title");
        assert_eq!(translated["text"]["format"]["strict"], true);
        let required = translated["text"]["format"]["schema"]["required"]
            .as_array()
            .unwrap();
        assert!(required.iter().any(|value| value == "title"));
        assert!(required.iter().any(|value| value == "short"));
        assert!(translated.get("tools").is_none());
        assert!(!translated.to_string().contains("x_search"));
    }

    #[test]
    fn grok_compaction_disables_all_tools_and_keeps_high_effort() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5-high",
            "messages":[{"role":"user","content":"CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\nYour entire response must be plain text: an <analysis> block followed by a <summary> block.\nYour task is to create a detailed summary of the conversation so far."}],
            "tools":[{"name":"Bash","input_schema":{"type":"object"}}],
            "output_config":{"effort":"high"}
        }))
        .unwrap();

        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(translated["reasoning"]["effort"], "high");
        assert!(translated.get("tools").is_none());
        assert!(translated.get("tool_choice").is_none());
        assert!(!translated.to_string().contains("x_search"));
    }

    #[test]
    fn grok_translation_omits_reasoning_for_non_reasoning_model() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-composer-2.5-fast",
            "messages":[{"role":"user","content":"hello"}],
            "output_config":{"effort":"high"}
        }))
        .unwrap();
        let translated = serde_json::to_value(
            translate_request(&request, "grok-composer-2.5-fast".into()).unwrap(),
        )
        .unwrap();
        assert!(translated.get("reasoning").is_none());
    }
    #[test]
    fn grok_translation_maps_claude_web_search_to_hosted_web_search() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[{"role":"user","content":"search online for the project"}],
            "tools":[{
                "name":"WebSearch",
                "description":"Search the web",
                "input_schema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}
            }]
        }))
        .unwrap();
        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(
            translated["tools"],
            serde_json::json!([{"type":"web_search"}])
        );
        assert!(
            translated["instructions"]
                .as_str()
                .unwrap()
                .contains("use the hosted web_search tool")
        );
        assert_eq!(translated["tool_choice"], "required");
    }

    #[test]
    fn grok_translation_maps_x_intent_to_required_hosted_x_search() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[{"role":"user","content":"Search X for recent posts mentioning claude-code-proxy"}],
            "tools":[
                {"name":"Bash","description":"Run a command","input_schema":{"type":"object"}},
                {"name":"WebSearch","description":"Search the web","input_schema":{"type":"object","properties":{"query":{"type":"string"}}}}
            ]
        }))
        .unwrap();
        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(
            translated["tools"],
            serde_json::json!([{"type":"x_search"}])
        );
        assert_eq!(translated["tool_choice"], "required");
        assert!(!translated.to_string().contains("\"name\":\"Bash\""));
    }

    #[test]
    fn grok_translation_maps_dedicated_xsearch_with_domain_schema() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[{"role":"user","content":"find relevant posts"}],
            "tools":[{
                "name":"XSearch",
                "description":"Search X posts",
                "input_schema":{
                    "type":"object",
                    "properties":{
                        "query":{"type":"string"},
                        "allowed_x_handles":{"type":"array","items":{"type":"string"}},
                        "excluded_x_handles":{"type":"array","items":{"type":"string"}},
                        "from_date":{"type":"string","format":"date"},
                        "to_date":{"type":"string","format":"date"}
                    },
                    "required":["query"]
                }
            }]
        }))
        .unwrap();
        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(
            translated["tools"],
            serde_json::json!([{"type":"x_search"}])
        );
        assert_eq!(translated["tool_choice"], "required");
    }

    #[test]
    fn grok_translation_accepts_claude_code_context_management() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-composer-2.5-fast",
            "messages":[{"role":"user","content":"hello"}],
            "context_management":{"edits":[{"type":"clear_tool_uses_20250919","trigger":{"type":"input_tokens","value":100000}}]}
        }))
        .unwrap();
        let translated = translate_request(&request, "grok-composer-2.5-fast".into()).unwrap();
        assert_eq!(translated.input.len(), 1);
    }

    #[test]
    fn grok_translation_rejects_unknown_fields() {
        let request: MessagesRequest = serde_json::from_value(
            serde_json::json!({"model":"grok-4.5","messages":[],"unknown_field":true}),
        )
        .unwrap();
        assert!(translate_request(&request, "grok-4.5".into()).is_err());
    }

    #[test]
    fn grok_translation_accepts_verified_cache_control_without_forwarding_it() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "system":[{"type":"text","text":"rules","cache_control":{"type":"ephemeral"}}],
            "messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral","ttl":"5m"}}]}]
        })).unwrap();
        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert!(
            translated["instructions"]
                .as_str()
                .unwrap()
                .starts_with("rules")
        );
        assert_eq!(translated["input"][0]["content"][0]["text"], "hello");
        assert!(!translated.to_string().contains("cache_control"));
    }

    #[test]
    fn grok_translation_rejects_invalid_cache_control() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5", "messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"persistent"}}]}]
        })).unwrap();
        assert!(translate_request(&request, "grok-4.5".into()).is_err());
    }

    fn request_with_blocks(blocks: Value) -> MessagesRequest {
        serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[
                {"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"lookup","input":{}}]},
                {"role":"user","content":blocks}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn grok_translation_rejects_unknown_tool_block_fields() {
        let mut request = request_with_blocks(serde_json::json!([
            {"type":"tool_result","tool_use_id":"call_1","content":"ok"}
        ]));
        request.messages[0].content[0]["unknown"] = Value::Bool(true);
        assert!(translate_request(&request, "grok-4.5".into()).is_err());

        let request = request_with_blocks(serde_json::json!([
            {"type":"tool_result","tool_use_id":"call_1","content":"ok","unknown":true}
        ]));
        assert!(translate_request(&request, "grok-4.5".into()).is_err());
    }

    #[test]
    fn grok_translation_rejects_malformed_tool_result_children() {
        for child in [
            serde_json::json!("text"),
            serde_json::json!({"text":"ok"}),
            serde_json::json!({"type":"image","text":"ok"}),
            serde_json::json!({"type":"text","text":1}),
            serde_json::json!({"type":"text","text":"ok","unknown":true}),
        ] {
            let request = request_with_blocks(serde_json::json!([
                {"type":"tool_result","tool_use_id":"call_1","content":[child]}
            ]));
            assert!(translate_request(&request, "grok-4.5".into()).is_err());
        }
    }

    #[test]
    fn grok_translation_rejects_duplicate_tool_results() {
        let request = request_with_blocks(serde_json::json!([
            {"type":"tool_result","tool_use_id":"call_1","content":"first"},
            {"type":"tool_result","tool_use_id":"call_1","content":"second"}
        ]));
        assert!(translate_request(&request, "grok-4.5".into()).is_err());
    }

    #[test]
    fn grok_translation_accepts_tool_cache_control_without_forwarding_it() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[{"role":"user","content":"hello"}],
            "tools":[{
                "name":"lookup",
                "description":"Look things up",
                "input_schema":{"type":"object"},
                "cache_control":{"type":"ephemeral"}
            }]
        }))
        .unwrap();
        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert!(
            translated["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| { tool["type"] == "function" && tool["name"] == "lookup" })
        );
        assert!(!translated.to_string().contains("cache_control"));
    }

    #[test]
    fn grok_translation_rejects_invalid_tool_cache_control() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[{"role":"user","content":"hello"}],
            "tools":[{
                "name":"lookup",
                "input_schema":{"type":"object"},
                "cache_control":{"type":"persistent"}
            }]
        }))
        .unwrap();
        assert!(translate_request(&request, "grok-4.5".into()).is_err());
    }

    #[test]
    fn grok_translation_accepts_cache_control_on_tool_use_and_tool_result() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model":"grok-4.5",
            "messages":[
                {"role":"assistant","content":[
                    {"type":"tool_use","id":"call_1","name":"lookup","input":{"q":"a"},"cache_control":{"type":"ephemeral","ttl":"1h"}}
                ]},
                {"role":"user","content":[
                    {"type":"tool_result","tool_use_id":"call_1","content":[
                        {"type":"text","text":"result","cache_control":{"type":"ephemeral"}}
                    ]}
                ]}
            ]
        }))
        .unwrap();
        let translated =
            serde_json::to_value(translate_request(&request, "grok-4.5".into()).unwrap()).unwrap();
        assert_eq!(translated["input"][0]["type"], "function_call");
        assert_eq!(translated["input"][1]["type"], "function_call_output");
        assert_eq!(translated["input"][1]["output"], "result");
        assert!(!translated.to_string().contains("cache_control"));
    }
}
