use serde_json::Value;

use crate::anthropic::schema::MessagesRequest;

#[derive(Debug)]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Image {
        source: ImageSource,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: Value,
        is_error: Option<bool>,
    },
    Thinking {
        thinking: String,
        signature: Option<String>,
    },
}

#[derive(Debug)]
pub struct ImageSource {
    pub media_type: String,
    pub data: String,
    pub source_type: String,
}

pub fn flatten_system_text(system_val: Option<&Value>) -> Option<String> {
    let system = system_val?;
    let texts: Vec<String> = match system {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|b| {
                let text = b.get("text").and_then(|v| v.as_str())?;
                if text.starts_with("x-anthropic-billing-header:") {
                    None
                } else {
                    Some(text.to_string())
                }
            })
            .collect(),
        _ => return None,
    };
    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n\n"))
    }
}

pub fn read_effort(req: &MessagesRequest) -> Result<Option<&str>, anyhow::Error> {
    let output_config = match req.extra.get("output_config") {
        Some(Value::Object(m)) => m,
        _ => return Ok(None),
    };
    match output_config.get("effort") {
        Some(Value::String(s)) => {
            let valid = ["low", "medium", "high", "xhigh", "max", "ultra"];
            if valid.contains(&s.as_str()) {
                Ok(Some(s.as_str()))
            } else {
                anyhow::bail!("Invalid output_config.effort: {s}")
            }
        }
        _ => Ok(None),
    }
}

#[derive(Debug)]
pub enum JsonObjectError {
    Empty,
    Invalid(serde_json::Error),
    NotObject,
}

/// Parse a completed Responses API tool argument payload using one provider-independent rule.
pub fn parse_json_object(value: &str) -> Result<Value, JsonObjectError> {
    if value.is_empty() {
        return Err(JsonObjectError::Empty);
    }
    let parsed = serde_json::from_str::<Value>(value).map_err(JsonObjectError::Invalid)?;
    if !parsed.is_object() {
        return Err(JsonObjectError::NotObject);
    }
    Ok(parsed)
}

fn content_contains_text(content: &Value, needle: &str) -> bool {
    match content {
        Value::String(text) => text.contains(needle),
        Value::Array(blocks) => blocks.iter().any(|block| {
            block
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.contains(needle))
        }),
        _ => false,
    }
}

/// Detect Claude Code's automatic and manual compaction summary prompt.
///
/// Claude Code currently sends compaction with the selected main model, so
/// providers and the server share this classifier when applying internal
/// request policy.
pub fn is_claude_code_compaction_request(req: &MessagesRequest) -> bool {
    let Some(content) = req
        .messages
        .iter()
        .rev()
        .find(|message| message.role == "user")
        .map(|message| &message.content)
    else {
        return false;
    };

    [
        "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.",
        "Your entire response must be plain text: an <analysis> block followed by a <summary> block.",
        "Your task is to create a detailed summary",
    ]
    .iter()
    .all(|marker| content_contains_text(content, marker))
}

/// Normalize Anthropic JSON schemas for strict Responses API validation.
pub fn normalize_strict_json_schema(schema: &Value) -> Value {
    match schema {
        Value::Array(items) => {
            Value::Array(items.iter().map(normalize_strict_json_schema).collect())
        }
        Value::Object(map) => {
            let originally_required: std::collections::HashSet<&str> = map
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            let mut out: serde_json::Map<String, Value> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_strict_json_schema(value)))
                .collect();
            let is_bare_object = !out.contains_key("properties")
                && out.get("type").and_then(Value::as_str) == Some("object");
            let property_names = match out.get_mut("properties") {
                Some(Value::Object(properties)) => {
                    for (name, property_schema) in properties.iter_mut() {
                        if !originally_required.contains(name.as_str()) {
                            *property_schema = nullable_schema(std::mem::take(property_schema));
                        }
                    }
                    Some(properties.keys().cloned().collect())
                }
                None if is_bare_object => {
                    out.insert("properties".into(), Value::Object(serde_json::Map::new()));
                    Some(Vec::new())
                }
                _ => None,
            };
            if let Some(property_names) = property_names {
                out.insert(
                    "required".into(),
                    Value::Array(property_names.into_iter().map(Value::String).collect()),
                );
                out.insert("additionalProperties".into(), Value::Bool(false));
            }
            Value::Object(out)
        }
        _ => schema.clone(),
    }
}

fn nullable_schema(schema: Value) -> Value {
    if schema_accepts_null(&schema) {
        schema
    } else {
        serde_json::json!({"anyOf": [schema, {"type": "null"}]})
    }
}

fn schema_accepts_null(schema: &Value) -> bool {
    let Some(schema) = schema.as_object() else {
        return false;
    };
    if schema.get("const").is_some_and(Value::is_null)
        || schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(Value::is_null))
    {
        return true;
    }
    match schema.get("type") {
        Some(Value::String(kind)) if kind == "null" => return true,
        Some(Value::Array(kinds)) if kinds.iter().any(|kind| kind.as_str() == Some("null")) => {
            return true;
        }
        _ => {}
    }
    ["anyOf", "oneOf"]
        .into_iter()
        .filter_map(|key| schema.get(key).and_then(Value::as_array))
        .flatten()
        .any(schema_accepts_null)
}

pub fn normalize_content(content: &Value, missing_tool_input: Value) -> Vec<ContentBlock> {
    match content {
        Value::String(s) => {
            vec![ContentBlock::Text { text: s.clone() }]
        }
        Value::Array(arr) => {
            let mut blocks = Vec::new();
            for item in arr {
                if let Some(block) = parse_content_block(item, missing_tool_input.clone()) {
                    blocks.push(block);
                }
            }
            blocks
        }
        _ => Vec::new(),
    }
}

pub fn image_source_to_url(source: &ImageSource) -> String {
    if source.source_type == "url" {
        source.data.clone()
    } else {
        format!("data:{};base64,{}", source.media_type, source.data)
    }
}

pub fn image_block_to_url(block: &Value) -> String {
    let source_type = block
        .get("source")
        .and_then(|s| s.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("base64");
    if source_type == "url" {
        block
            .get("source")
            .and_then(|s| s.get("url"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    } else {
        let media_type = block
            .get("source")
            .and_then(|s| s.get("media_type"))
            .and_then(|v| v.as_str())
            .unwrap_or("image/png");
        let data = block
            .get("source")
            .and_then(|s| s.get("data"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        format!("data:{media_type};base64,{data}")
    }
}

fn parse_content_block(value: &Value, missing_tool_input: Value) -> Option<ContentBlock> {
    let kind = value.get("type").and_then(|v| v.as_str())?;
    match kind {
        "text" => {
            let text = value
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Some(ContentBlock::Text { text })
        }
        "image" => {
            let source = value.get("source")?;
            let media_type = source
                .get("media_type")
                .and_then(|v| v.as_str())
                .unwrap_or("image/png")
                .to_string();
            let source_type = source
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("base64")
                .to_string();
            let data = if source_type == "url" {
                source.get("url").and_then(|v| v.as_str()).unwrap_or("")
            } else {
                source.get("data").and_then(|v| v.as_str()).unwrap_or("")
            }
            .to_string();
            Some(ContentBlock::Image {
                source: ImageSource {
                    media_type,
                    data,
                    source_type,
                },
            })
        }
        "tool_use" => {
            let id = value
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = value
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let input = value.get("input").cloned().unwrap_or(missing_tool_input);
            Some(ContentBlock::ToolUse { id, name, input })
        }
        "tool_result" => {
            let tool_use_id = value
                .get("tool_use_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let content = value
                .get("content")
                .cloned()
                .unwrap_or(Value::String(String::new()));
            let is_error = value.get("is_error").and_then(|v| v.as_bool());
            Some(ContentBlock::ToolResult {
                tool_use_id,
                content,
                is_error,
            })
        }
        "thinking" => {
            let thinking = value
                .get("thinking")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let signature = value
                .get("signature")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            Some(ContentBlock::Thinking {
                thinking,
                signature,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_detector_requires_all_markers_in_latest_user_message() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [
                {"role": "user", "content": "Earlier task"},
                {"role": "assistant", "content": "Earlier response"},
                {"role": "user", "content": [{
                    "type": "text",
                    "text": "CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\nYour entire response must be plain text: an <analysis> block followed by a <summary> block.\nYour task is to create a detailed summary of the conversation so far."
                }]}
            ]
        }))
        .unwrap();

        assert!(is_claude_code_compaction_request(&request));

        let ordinary: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "Summarize this conversation"}]
        }))
        .unwrap();
        assert!(!is_claude_code_compaction_request(&ordinary));
    }

    #[test]
    fn effort_reader_accepts_ultra() {
        let request: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gpt-5.6-sol",
            "messages": [{"role": "user", "content": "hello"}],
            "output_config": {"effort": "ultra"}
        }))
        .unwrap();

        assert_eq!(read_effort(&request).unwrap(), Some("ultra"));
    }

    #[test]
    fn strict_schema_requires_every_declared_property_recursively() {
        let normalized = normalize_strict_json_schema(&serde_json::json!({
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {"short": {"type": "boolean"}}
                }
            },
            "required": ["title"]
        }));

        assert_eq!(
            normalized["required"],
            serde_json::json!(["metadata", "title"])
        );
        assert_eq!(
            normalized["properties"]["metadata"]["required"],
            Value::Null
        );
        assert_eq!(normalized["additionalProperties"], false);
        assert_eq!(
            normalized["properties"]["metadata"]["anyOf"][0]["required"],
            serde_json::json!(["short"])
        );
        assert_eq!(
            normalized["properties"]["metadata"]["anyOf"][0]["additionalProperties"],
            false
        );
        assert_eq!(
            normalized["properties"]["metadata"]["anyOf"][1],
            serde_json::json!({"type":"null"})
        );
    }

    #[test]
    fn strict_empty_object_schema_gets_explicit_closed_shape() {
        let normalized = normalize_strict_json_schema(&serde_json::json!({"type": "object"}));

        assert_eq!(normalized["properties"], serde_json::json!({}));
        assert_eq!(normalized["required"], serde_json::json!([]));
        assert_eq!(normalized["additionalProperties"], false);
    }

    #[test]
    fn completed_tool_arguments_must_be_a_json_object() {
        assert!(matches!(parse_json_object(""), Err(JsonObjectError::Empty)));
        assert!(matches!(
            parse_json_object("{\"path\":"),
            Err(JsonObjectError::Invalid(_))
        ));
        assert!(matches!(
            parse_json_object("[]"),
            Err(JsonObjectError::NotObject)
        ));
        assert_eq!(
            parse_json_object("{\"path\":\"/tmp/a\"}").unwrap(),
            serde_json::json!({"path":"/tmp/a"})
        );
    }
}
