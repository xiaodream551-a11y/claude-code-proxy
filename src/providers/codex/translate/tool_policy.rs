use std::collections::HashSet;

use super::request::{
    ResponsesInputItem, ResponsesRequest, ResponsesTool, ResponsesToolChoice,
    ResponsesToolChoiceMode,
};

#[derive(Clone, Debug)]
pub(crate) struct ToolCallPolicy {
    function_rule: FunctionRule,
    hosted_rule: HostedRule,
    terminal_requirement: TerminalRequirement,
    max_tool_calls: Option<usize>,
    max_hosted_calls: Option<u64>,
}

#[derive(Clone, Debug)]
enum FunctionRule {
    Permissive,
    Forbidden,
    Declared(HashSet<String>),
    Named { name: String, declared: bool },
}

#[derive(Clone, Copy, Debug)]
enum HostedRule {
    Permissive,
    Forbidden,
    Declared(bool),
}

#[derive(Clone, Debug)]
enum TerminalRequirement {
    None,
    AnyTool,
    NamedFunction(String),
    HostedWebSearch,
}

impl ToolCallPolicy {
    /// Compatibility policy for lower-level translation helpers that are not
    /// associated with a request. Production request paths must use
    /// `from_request` so upstream output cannot widen request permissions.
    pub(crate) fn permissive() -> Self {
        Self {
            function_rule: FunctionRule::Permissive,
            hosted_rule: HostedRule::Permissive,
            terminal_requirement: TerminalRequirement::None,
            max_tool_calls: None,
            max_hosted_calls: None,
        }
    }

    /// Derive the response-side policy from the final Codex wire request. This
    /// intentionally observes only tools that survived direct-caller filtering
    /// and deferred-tool loading.
    pub(crate) fn from_request(request: &ResponsesRequest) -> Self {
        let (declared, hosted_declared) = declared_tools(request);
        let mut policy = match request.tool_choice.as_ref() {
            Some(ResponsesToolChoice::Mode(ResponsesToolChoiceMode::None)) => Self {
                function_rule: FunctionRule::Forbidden,
                hosted_rule: HostedRule::Forbidden,
                terminal_requirement: TerminalRequirement::None,
                max_tool_calls: None,
                max_hosted_calls: None,
            },
            Some(ResponsesToolChoice::Mode(ResponsesToolChoiceMode::Required)) => Self {
                function_rule: FunctionRule::Declared(declared),
                hosted_rule: HostedRule::Declared(hosted_declared),
                terminal_requirement: TerminalRequirement::AnyTool,
                max_tool_calls: None,
                max_hosted_calls: None,
            },
            Some(ResponsesToolChoice::Function { name, .. }) => Self {
                function_rule: FunctionRule::Named {
                    name: name.clone(),
                    declared: declared.contains(name),
                },
                hosted_rule: HostedRule::Forbidden,
                terminal_requirement: TerminalRequirement::NamedFunction(name.clone()),
                max_tool_calls: None,
                max_hosted_calls: None,
            },
            Some(ResponsesToolChoice::WebSearch { .. }) => Self {
                function_rule: FunctionRule::Forbidden,
                hosted_rule: HostedRule::Declared(hosted_declared),
                terminal_requirement: TerminalRequirement::HostedWebSearch,
                max_tool_calls: None,
                max_hosted_calls: None,
            },
            Some(ResponsesToolChoice::AllowedTools { mode, tools, .. }) => {
                let allowed_functions: HashSet<String> = tools
                    .iter()
                    .filter(|tool| {
                        tool.get("type").and_then(serde_json::Value::as_str) == Some("function")
                    })
                    .filter_map(|tool| tool.get("name").and_then(serde_json::Value::as_str))
                    .filter(|name| declared.contains(*name))
                    .map(str::to_string)
                    .collect();
                let allows_hosted = tools.iter().any(|tool| {
                    tool.get("type").and_then(serde_json::Value::as_str) == Some("web_search")
                });
                let function_rule = if allowed_functions.is_empty() {
                    FunctionRule::Forbidden
                } else {
                    FunctionRule::Declared(allowed_functions)
                };
                let terminal_requirement = if mode == "required" {
                    if allows_hosted && matches!(&function_rule, FunctionRule::Forbidden) {
                        TerminalRequirement::HostedWebSearch
                    } else {
                        TerminalRequirement::AnyTool
                    }
                } else {
                    TerminalRequirement::None
                };
                Self {
                    function_rule,
                    hosted_rule: if allows_hosted {
                        HostedRule::Declared(hosted_declared)
                    } else {
                        HostedRule::Forbidden
                    },
                    terminal_requirement,
                    max_tool_calls: None,
                    max_hosted_calls: None,
                }
            }
            Some(ResponsesToolChoice::Mode(ResponsesToolChoiceMode::Auto)) | None => Self {
                function_rule: FunctionRule::Declared(declared),
                hosted_rule: HostedRule::Declared(hosted_declared),
                terminal_requirement: TerminalRequirement::None,
                max_tool_calls: None,
                max_hosted_calls: None,
            },
        };
        policy.max_tool_calls = (!request.parallel_tool_calls).then_some(1);
        policy.max_hosted_calls = request.hosted_web_search_max_uses;
        policy
    }

    pub(crate) fn validate_next_tool_call(
        &self,
        registered_tool_calls: usize,
    ) -> Result<(), String> {
        if self
            .max_tool_calls
            .is_some_and(|maximum| registered_tool_calls >= maximum)
        {
            return Err(
                "Codex returned multiple tool calls while parallel_tool_calls is false".to_string(),
            );
        }
        Ok(())
    }

    pub(crate) fn validate_hosted_call(
        &self,
        registered_hosted_calls: usize,
    ) -> Result<(), String> {
        match self.hosted_rule {
            HostedRule::Permissive | HostedRule::Declared(true) => {}
            HostedRule::Forbidden => {
                return Err(
                    "Codex returned hosted web search while tool_choice forbids it".to_string(),
                );
            }
            HostedRule::Declared(false) => {
                return Err("Codex returned undeclared hosted web search".to_string());
            }
        }
        if self
            .max_hosted_calls
            .is_some_and(|maximum| registered_hosted_calls as u64 >= maximum)
        {
            return Err("Codex exceeded the hosted web search max_uses limit".to_string());
        }
        Ok(())
    }

    pub(crate) fn validate_function_name(&self, name: &str) -> Result<(), String> {
        match &self.function_rule {
            FunctionRule::Permissive => Ok(()),
            FunctionRule::Forbidden => Err(format!(
                "Codex returned function tool {name:?} while tool_choice forbids function calls"
            )),
            FunctionRule::Declared(names) if names.contains(name) => Ok(()),
            FunctionRule::Declared(_) => {
                Err(format!("Codex returned undeclared function tool {name:?}"))
            }
            FunctionRule::Named {
                name: required,
                declared: true,
            } if name == required => Ok(()),
            FunctionRule::Named { name: required, .. } if name != required => Err(format!(
                "Codex returned function tool {name:?} while tool_choice requires {required:?}"
            )),
            FunctionRule::Named { .. } => {
                Err(format!("Codex returned undeclared function tool {name:?}"))
            }
        }
    }

    pub(crate) fn validate_success_terminal(
        &self,
        completed_function_calls: usize,
        completed_hosted_calls: usize,
    ) -> Result<(), String> {
        match &self.terminal_requirement {
            TerminalRequirement::None => Ok(()),
            TerminalRequirement::AnyTool
                if completed_function_calls > 0 || completed_hosted_calls > 0 =>
            {
                Ok(())
            }
            TerminalRequirement::AnyTool => {
                Err("Codex completed without the tool call required by tool_choice".to_string())
            }
            TerminalRequirement::NamedFunction(_) if completed_function_calls > 0 => Ok(()),
            TerminalRequirement::NamedFunction(name) => Err(format!(
                "Codex completed without required function tool {name:?}"
            )),
            TerminalRequirement::HostedWebSearch if completed_hosted_calls > 0 => Ok(()),
            TerminalRequirement::HostedWebSearch => Err(
                "Codex completed without the hosted web search required by tool_choice".to_string(),
            ),
        }
    }
}

fn declared_tools(request: &ResponsesRequest) -> (HashSet<String>, bool) {
    let mut functions = HashSet::new();
    let mut hosted = false;

    for tool in request.tools.iter().flatten() {
        match tool {
            ResponsesTool::Function(function) => {
                functions.insert(function.name.clone());
            }
            ResponsesTool::WebSearch(_) => hosted = true,
        }
    }

    for item in &request.input {
        let ResponsesInputItem::AdditionalTools { tools, .. } = item else {
            continue;
        };
        for tool in tools {
            match tool.get("type").and_then(serde_json::Value::as_str) {
                Some("function") => {
                    if let Some(name) = tool.get("name").and_then(serde_json::Value::as_str) {
                        functions.insert(name.to_string());
                    }
                }
                Some("web_search") => hosted = true,
                _ => {}
            }
        }
    }

    (functions, hosted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(tool_choice: serde_json::Value) -> ResponsesRequest {
        serde_json::from_value(json!({
            "model": "gpt-5.6-sol",
            "input": [{
                "type": "additional_tools",
                "role": "developer",
                "tools": [{
                    "type": "function",
                    "name": "DeferredRead",
                    "parameters": {"type": "object"},
                    "strict": false
                }]
            }],
            "tools": [
                {
                    "type": "function",
                    "name": "Read",
                    "parameters": {"type": "object"},
                    "strict": false
                },
                {
                    "type": "web_search",
                    "external_web_access": true,
                    "search_content_types": ["text"]
                }
            ],
            "tool_choice": tool_choice,
            "store": false,
            "stream": true,
            "parallel_tool_calls": true,
            "text": {}
        }))
        .unwrap()
    }

    #[test]
    fn policy_observes_top_level_and_loaded_deferred_functions() {
        let policy = ToolCallPolicy::from_request(&request(json!("auto")));
        policy.validate_function_name("Read").unwrap();
        policy.validate_function_name("DeferredRead").unwrap();
        assert!(
            policy
                .validate_function_name("Bash")
                .unwrap_err()
                .contains("undeclared")
        );
    }

    #[test]
    fn forced_hosted_tool_requires_hosted_terminal() {
        let mut request = request(json!("auto"));
        request.tool_choice = Some(ResponsesToolChoice::AllowedTools {
            r#type: "allowed_tools".to_string(),
            mode: "required".to_string(),
            tools: vec![json!({"type": "web_search"})],
        });
        let policy = ToolCallPolicy::from_request(&request);
        assert!(policy.validate_function_name("Read").is_err());
        assert!(policy.validate_success_terminal(0, 0).is_err());
        policy.validate_success_terminal(0, 1).unwrap();
    }

    #[test]
    fn hosted_max_uses_is_enforced_before_accepting_the_next_call() {
        let mut request = request(json!("auto"));
        request.hosted_web_search_max_uses = Some(1);
        let policy = ToolCallPolicy::from_request(&request);

        policy.validate_hosted_call(0).unwrap();
        assert!(
            policy
                .validate_hosted_call(1)
                .unwrap_err()
                .contains("max_uses")
        );
    }
}
