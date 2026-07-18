use std::collections::{HashMap, VecDeque};

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::stream::SseDecoder;
use crate::providers::translate_shared::{JsonObjectError, parse_json_object};

const MAX_TOOL_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_INCOMPLETE_TOOL_CALLS: usize = 128;
const MAX_SEQUENCE_HISTORY: usize = 1024;
const MAX_INCOMPLETE_REASON_BYTES: usize = 256;
const MAX_UPSTREAM_FAILURE_MESSAGE_BYTES: usize = 2 * 1024;

/// Return whether an event is known to be non-semantic telemetry after a response terminal.
///
/// Usage belongs to the terminal response payload, so it is intentionally absent. Extend this
/// exact allowlist only after a real upstream capture proves another event safe to discard.
fn is_ignorable_post_terminal_event(event_type: &str) -> bool {
    matches!(event_type, "rate_limits.updated")
}

#[derive(Clone, Default)]
enum SequenceState {
    #[default]
    Undecided,
    Legacy,
    Sequenced {
        last: u64,
        digests: HashMap<u64, [u8; 32]>,
        order: VecDeque<u64>,
    },
}

impl SequenceState {
    fn accept(&mut self, value: &Value) -> anyhow::Result<bool> {
        let sequence = value.get("sequence_number");
        match (&mut *self, sequence) {
            (Self::Undecided, None) => {
                *self = Self::Legacy;
                Ok(true)
            }
            (Self::Legacy, None) => Ok(true),
            (Self::Legacy, Some(_)) => {
                anyhow::bail!("Grok stream started sequence numbering after legacy events")
            }
            (Self::Sequenced { .. }, None) => {
                anyhow::bail!("Grok sequenced stream event omitted sequence_number")
            }
            (Self::Undecided, Some(sequence)) => {
                let sequence = parse_sequence(sequence)?;
                let digest = event_digest(value)?;
                *self = Self::Sequenced {
                    last: sequence,
                    digests: HashMap::from([(sequence, digest)]),
                    order: VecDeque::from([sequence]),
                };
                Ok(true)
            }
            (
                Self::Sequenced {
                    last,
                    digests,
                    order,
                },
                Some(sequence),
            ) => {
                let sequence = parse_sequence(sequence)?;
                let next_digest = event_digest(value)?;
                if let Some(known_digest) = digests.get(&sequence) {
                    if next_digest == *known_digest {
                        return Ok(false);
                    }
                    anyhow::bail!("Grok stream repeated sequence_number with different payload")
                }
                if sequence < *last {
                    anyhow::bail!("Grok stream sequence_number moved backwards")
                }
                if sequence != last.saturating_add(1) {
                    anyhow::bail!("Grok stream sequence_number has a gap")
                }
                *last = sequence;
                digests.insert(sequence, next_digest);
                order.push_back(sequence);
                while order.len() > MAX_SEQUENCE_HISTORY {
                    if let Some(evicted) = order.pop_front() {
                        digests.remove(&evicted);
                    }
                }
                Ok(true)
            }
        }
    }
}

fn parse_sequence(value: &Value) -> anyhow::Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("Grok sequence_number is not an unsigned integer"))
}

fn event_digest(value: &Value) -> anyhow::Result<[u8; 32]> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| anyhow::anyhow!("failed to canonicalize Grok event: {error}"))?;
    Ok(Sha256::digest(encoded).into())
}

#[derive(Debug, Clone)]
pub enum ReducerEvent {
    ThinkingStart(usize),
    ThinkingDelta(usize, String),
    ThinkingStop(usize),
    TextStart(usize),
    TextDelta(usize, String),
    TextStop(usize),
    ToolStart(usize, String, String),
    ToolDelta(usize, String),
    ToolStop(usize),
    HostedSearch {
        index: usize,
        result_index: usize,
        id: String,
        name: String,
        query: String,
    },
    Citation(usize, Value),
    Terminal(GrokTerminal),
    Usage(GrokUsage),
    Finish {
        stop_reason: String,
        input_tokens: u64,
        output_tokens: u64,
        web_search_requests: u64,
        x_search_requests: u64,
    },
}

impl ReducerEvent {
    pub fn is_semantic(&self) -> bool {
        !matches!(
            self,
            Self::ThinkingStart(_)
                | Self::ThinkingDelta(_, _)
                | Self::ThinkingStop(_)
                | Self::Terminal(_)
                | Self::Usage(_)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrokTerminal {
    Completed,
    IncompleteMaxOutputTokens,
    IncompleteContentFilter,
    IncompleteMissingReason,
    IncompleteOther,
}

impl GrokTerminal {
    pub fn outcome(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::IncompleteMaxOutputTokens
            | Self::IncompleteContentFilter
            | Self::IncompleteMissingReason
            | Self::IncompleteOther => "incomplete",
        }
    }

    pub fn incomplete_reason(self) -> Option<&'static str> {
        match self {
            Self::Completed => None,
            Self::IncompleteMaxOutputTokens => Some("max_output_tokens"),
            Self::IncompleteContentFilter => Some("content_filter"),
            Self::IncompleteMissingReason => Some("missing"),
            Self::IncompleteOther => Some("other"),
        }
    }

    fn stop_reason(self) -> &'static str {
        match self {
            Self::Completed => "end_turn",
            Self::IncompleteMaxOutputTokens => "max_tokens",
            Self::IncompleteContentFilter
            | Self::IncompleteMissingReason
            | Self::IncompleteOther => "refusal",
        }
    }
}

/// Token counters reported by the upstream Responses API.
///
/// Every field remains optional so an omitted counter is distinguishable from a real zero. The
/// upstream input counter includes cached tokens; [`GrokUsage::mapped_input_tokens`] splits them
/// out for the Anthropic usage shape when the cached-token detail is internally consistent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrokUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

impl GrokUsage {
    pub fn availability_state(&self) -> &'static str {
        let any = self.input_tokens.is_some()
            || self.output_tokens.is_some()
            || self.cached_input_tokens.is_some()
            || self.reasoning_tokens.is_some()
            || self.total_tokens.is_some();
        if !any {
            "unavailable"
        } else if self.input_tokens.is_some() && self.output_tokens.is_some() {
            "reported"
        } else {
            "partial"
        }
    }

    pub fn mapped_input_tokens(&self) -> Option<u64> {
        match (self.input_tokens, self.cached_input_tokens) {
            (Some(total), Some(cached)) if cached <= total => Some(total - cached),
            (Some(total), _) => Some(total),
            (None, _) => None,
        }
    }

    pub fn mapped_cache_read_input_tokens(&self) -> Option<u64> {
        match (self.input_tokens, self.cached_input_tokens) {
            (Some(total), Some(cached)) if cached <= total => Some(cached),
            _ => None,
        }
    }

    pub fn cache_breakdown_is_inconsistent(&self) -> bool {
        matches!(
            (self.input_tokens, self.cached_input_tokens),
            (Some(total), Some(cached)) if cached > total
        )
    }
}

/// A non-streaming reduction failure together with any usage reported by the failing response.
///
/// `response.failed` is still a hard protocol error. Keeping its usage beside the error lets the
/// provider update diagnostics without turning the failed response into a successful message.
#[derive(Debug)]
pub struct GrokReductionError {
    source: anyhow::Error,
    usage: Option<GrokUsage>,
}

impl GrokReductionError {
    fn new(source: anyhow::Error, usage: Option<GrokUsage>) -> Self {
        Self { source, usage }
    }

    pub fn usage(&self) -> Option<&GrokUsage> {
        self.usage.as_ref()
    }

    pub fn upstream_failure(&self) -> Option<&GrokUpstreamFailure> {
        self.source.downcast_ref()
    }
}

impl std::fmt::Display for GrokReductionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for GrokReductionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// A structured failure delivered inside an otherwise successful Grok SSE response.
///
/// The Responses API can report model, capacity, or request failures after the HTTP 200 headers.
/// Keep the transport semantics intact so a caller can retry a known transient failure before it
/// commits output, or render the matching Anthropic in-band error after output has started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokUpstreamFailure {
    pub event_type: String,
    pub status: u16,
    pub retry_after: Option<String>,
    pub message: String,
    pub retryable: bool,
}

impl std::fmt::Display for GrokUpstreamFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.event_type, self.message)
    }
}

impl std::error::Error for GrokUpstreamFailure {}

#[derive(Clone, Default)]
pub struct Reducer {
    sequence: SequenceState,
    next_index: usize,
    active: Option<(String, usize)>,
    active_text: String,
    saw_text_output: bool,
    calls: HashMap<String, (usize, String)>,
    item_calls: HashMap<String, String>,
    tool_args: HashMap<String, String>,
    completed_arguments: HashMap<String, bool>,
    hosted_calls: HashMap<String, (String, String)>,
    web_search_requests: u64,
    x_search_requests: u64,
    saw_tool: bool,
    completed: bool,
    incomplete_reason: Option<String>,
    usage: Option<GrokUsage>,
}

impl Reducer {
    pub fn push(&mut self, value: Value) -> anyhow::Result<Vec<ReducerEvent>> {
        let typ = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("event lacks type"))?;
        // Rate-limit telemetry is transport metadata rather than part of the sequenced response
        // event stream. Once the response is terminal, ignore it before sequence validation so a
        // numbered response can safely be followed by unnumbered or repeated telemetry.
        if self.completed && is_ignorable_post_terminal_event(typ) {
            return Ok(vec![]);
        }
        if !self.sequence.accept(&value)? {
            return Ok(vec![]);
        }
        if self.completed {
            // Text, tools, and unknown future events fail closed until a real capture proves that
            // they are safe to ignore.
            anyhow::bail!("event after terminal completion: {typ}");
        }
        match typ {
            "response.created" | "response.in_progress" => Ok(vec![]),
            "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.content_part.added" => Ok(vec![]),
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => self
                .delta(
                    "thinking",
                    value
                        .get("delta")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow::anyhow!("reasoning delta is invalid"))?,
                ),
            "response.custom_tool_call_input.delta" => {
                let id = value
                    .get("item_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("custom tool delta lacks item id"))?;
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("custom tool delta is invalid"))?;
                let (_, input) = self
                    .hosted_calls
                    .get_mut(id)
                    .ok_or_else(|| anyhow::anyhow!("custom tool delta is out of order"))?;
                if input.len().saturating_add(delta.len()) > MAX_TOOL_ARGUMENT_BYTES {
                    anyhow::bail!("custom tool input exceeds the size limit");
                }
                input.push_str(delta);
                Ok(vec![])
            }
            "response.custom_tool_call_input.done"
            | "response.web_search_call.in_progress"
            | "response.web_search_call.searching"
            | "response.web_search_call.completed" => Ok(vec![]),
            "response.output_text.annotation.added" => {
                let Some((kind, index)) = self.active.as_ref() else {
                    return Ok(vec![]);
                };
                let Some(annotation) = value.get("annotation") else {
                    return Ok(vec![]);
                };
                if kind == "text"
                    && annotation.get("type").and_then(Value::as_str) == Some("url_citation")
                {
                    Ok(vec![ReducerEvent::Citation(*index, annotation.clone())])
                } else {
                    Ok(vec![])
                }
            }
            "response.output_text.delta" => self.delta(
                "text",
                value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("text delta is invalid"))?,
            ),
            "response.output_item.added" => {
                let item = value
                    .get("item")
                    .and_then(Value::as_object)
                    .ok_or_else(|| anyhow::anyhow!("output item is invalid"))?;
                if item.get("type").and_then(Value::as_str) == Some("custom_tool_call") {
                    let id = item
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("custom tool call id is invalid"))?;
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.is_empty())
                        .unwrap_or("x_search");
                    let name = if name.starts_with("x_") {
                        "x_search"
                    } else {
                        name
                    };
                    self.hosted_calls
                        .insert(id.into(), (name.into(), String::new()));
                    Ok(vec![])
                } else if item.get("type").and_then(Value::as_str) == Some("function_call") {
                    let id = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("function call id is invalid"))?;
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("function call name is invalid"))?;
                    if self.calls.contains_key(id) {
                        anyhow::bail!("duplicate function call id");
                    }
                    if self.calls.len() >= MAX_INCOMPLETE_TOOL_CALLS {
                        anyhow::bail!("too many incomplete function calls");
                    }
                    let mut out = self.close_active()?;
                    let index = self.next_index;
                    self.next_index += 1;
                    self.calls.insert(id.into(), (index, name.into()));
                    if let Some(item_id) = item.get("id").and_then(Value::as_str) {
                        self.item_calls.insert(item_id.into(), id.into());
                    }
                    self.tool_args.insert(id.into(), String::new());
                    self.completed_arguments.insert(id.into(), false);
                    self.saw_tool = true;
                    out.push(ReducerEvent::ToolStart(index, id.into(), name.into()));
                    Ok(out)
                } else {
                    Ok(vec![])
                }
            }
            "response.function_call_arguments.delta" => {
                let id = value
                    .get("call_id")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        value
                            .get("item_id")
                            .and_then(Value::as_str)
                            .and_then(|item_id| self.item_calls.get(item_id).map(String::as_str))
                    })
                    .ok_or_else(|| anyhow::anyhow!("function delta lacks call id"))?;
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("function delta is invalid"))?;
                let (index, _) = self
                    .calls
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("function delta is out of order"))?
                    .clone();
                let args = self
                    .tool_args
                    .get_mut(id)
                    .ok_or_else(|| anyhow::anyhow!("function delta is out of order"))?;
                if args.len().saturating_add(delta.len()) > MAX_TOOL_ARGUMENT_BYTES {
                    anyhow::bail!("function arguments exceed the size limit");
                }
                args.push_str(delta);
                Ok(vec![ReducerEvent::ToolDelta(index, delta.into())])
            }
            "response.function_call_arguments.done" => {
                let id = value
                    .get("call_id")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        value
                            .get("item_id")
                            .and_then(Value::as_str)
                            .and_then(|item_id| self.item_calls.get(item_id).map(String::as_str))
                    })
                    .ok_or_else(|| anyhow::anyhow!("function completion lacks call id"))?;
                let args = value.get("arguments").and_then(Value::as_str);
                let accumulated = self
                    .tool_args
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("function completion is out of order"))?;
                let index = self
                    .calls
                    .get(id)
                    .map(|(index, _)| *index)
                    .ok_or_else(|| anyhow::anyhow!("function completion is out of order"))?;
                let output = match args {
                    Some(args) if accumulated.is_empty() && !args.is_empty() => {
                        self.tool_args.get_mut(id).unwrap().push_str(args);
                        vec![ReducerEvent::ToolDelta(index, args.into())]
                    }
                    Some(args) if args != accumulated => {
                        anyhow::bail!("function completion arguments disagree with deltas")
                    }
                    _ => vec![],
                };
                self.completed_arguments.insert(id.into(), true);
                Ok(output)
            }
            "response.output_text.done" => {
                let mut out = value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(|text| self.complete_text_snapshot(text))
                    .transpose()?
                    .unwrap_or_default();
                out.extend(self.close_kind("text")?);
                Ok(out)
            }
            "response.reasoning_summary_text.done" | "response.reasoning_text.done" => {
                self.close_kind("thinking")
            }
            "response.content_part.done" => Ok(vec![]),
            "response.output_item.done" => {
                let item = value
                    .get("item")
                    .and_then(Value::as_object)
                    .ok_or_else(|| anyhow::anyhow!("completed output item is invalid"))?;
                match item.get("type").and_then(Value::as_str) {
                    Some("web_search_call") => {
                        let id = item
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .ok_or_else(|| anyhow::anyhow!("completed web search lacks id"))?;
                        let query = item
                            .get("action")
                            .and_then(|action| action.get("query"))
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let mut out = self.close_active()?;
                        let index = self.next_index;
                        let result_index = index + 1;
                        self.next_index += 2;
                        self.web_search_requests += 1;
                        out.push(ReducerEvent::HostedSearch {
                            index,
                            result_index,
                            id: format!("srvtoolu_{id}"),
                            name: "web_search".into(),
                            query: query.into(),
                        });
                        Ok(out)
                    }
                    Some("custom_tool_call") => {
                        let id = item
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .ok_or_else(|| anyhow::anyhow!("completed custom tool lacks id"))?;
                        let (name, input) = self.hosted_calls.remove(id).unwrap_or_else(|| {
                            (
                                item.get("name")
                                    .and_then(Value::as_str)
                                    .unwrap_or("x_search")
                                    .into(),
                                String::new(),
                            )
                        });
                        if name != "x_search" {
                            return Ok(vec![]);
                        }
                        let query = serde_json::from_str::<Value>(&input)
                            .ok()
                            .and_then(|input| {
                                input
                                    .get("query")
                                    .and_then(Value::as_str)
                                    .map(str::to_string)
                            })
                            .unwrap_or_default();
                        let mut out = self.close_active()?;
                        let index = self.next_index;
                        let result_index = index + 1;
                        self.next_index += 2;
                        self.x_search_requests += 1;
                        out.push(ReducerEvent::HostedSearch {
                            index,
                            result_index,
                            id: format!("srvtoolu_{id}"),
                            name,
                            query,
                        });
                        Ok(out)
                    }
                    Some("function_call") => {
                        let id = item.get("call_id").and_then(Value::as_str).ok_or_else(|| {
                            anyhow::anyhow!("completed function call lacks call id")
                        })?;
                        let (index, _) = self
                            .calls
                            .remove(id)
                            .ok_or_else(|| anyhow::anyhow!("completed function call is unknown"))?;
                        let args = self.tool_args.remove(id).unwrap_or_default();
                        self.completed_arguments.remove(id);
                        parse_function_arguments(&args)?;
                        Ok(vec![ReducerEvent::ToolStop(index)])
                    }
                    _ => Ok(vec![]),
                }
            }
            "response.completed" | "response.incomplete" => {
                let response = value.get("response").unwrap_or(&value);
                let usage = grok_usage(response);
                self.usage = Some(usage.clone());
                if !self.calls.is_empty() {
                    anyhow::bail!("function call is incomplete");
                }
                let fallback_text = if self.saw_text_output {
                    Vec::new()
                } else {
                    response_output_text(response)
                };
                let mut out = self.close_active()?;
                for text in fallback_text {
                    out.extend(self.delta("text", &text)?);
                    out.extend(self.close_kind("text")?);
                }
                // Keep the legacy Finish field as the upstream total for the non-streaming
                // accumulator. The streaming renderer uses GrokUsage to split cached input into
                // Anthropic's input/cache-read counters without double counting.
                let input = usage.input_tokens.unwrap_or(0);
                let output = usage.output_tokens.unwrap_or(0);
                let incomplete_reason = response
                    .get("incomplete_details")
                    .and_then(|details| details.get("reason"))
                    .and_then(Value::as_str);
                let response_is_incomplete = typ == "response.incomplete"
                    || response.get("status").and_then(Value::as_str) == Some("incomplete")
                    || incomplete_reason.is_some();
                let terminal = if response_is_incomplete {
                    match incomplete_reason {
                        Some("content_filter") => GrokTerminal::IncompleteContentFilter,
                        Some("max_output_tokens" | "max_tokens" | "length") => {
                            GrokTerminal::IncompleteMaxOutputTokens
                        }
                        None => GrokTerminal::IncompleteMissingReason,
                        Some(_) => GrokTerminal::IncompleteOther,
                    }
                } else {
                    GrokTerminal::Completed
                };
                self.incomplete_reason = response_is_incomplete
                    .then(|| incomplete_reason.map(bounded_incomplete_reason))
                    .flatten();
                let stop = if self.saw_tool {
                    "tool_use"
                } else {
                    terminal.stop_reason()
                };
                self.completed = true;
                out.push(ReducerEvent::Terminal(terminal));
                out.push(ReducerEvent::Usage(usage));
                out.push(ReducerEvent::Finish {
                    stop_reason: stop.into(),
                    input_tokens: input,
                    output_tokens: output,
                    web_search_requests: self.web_search_requests,
                    x_search_requests: self.x_search_requests,
                });
                Ok(out)
            }
            "error" | "response.failed" | "response.error" => {
                let response = value.get("response").unwrap_or(&value);
                self.usage = Some(grok_usage(response));
                Err(anyhow::Error::new(parse_upstream_failure(&value, typ)))
            }
            _ => anyhow::bail!("unsupported Grok stream event: {typ}"),
        }
    }

    /// Apply every event decoded from one upstream byte chunk atomically.
    ///
    /// A terminal event followed by an invalid semantic tail must not commit the terminal or any
    /// preceding text/tool state from that same chunk. Callers can render the returned events only
    /// after this method succeeds.
    pub fn push_batch(
        &mut self,
        values: impl IntoIterator<Item = Value>,
    ) -> anyhow::Result<Vec<ReducerEvent>> {
        let mut staged = self.clone();
        let mut out = Vec::new();
        for value in values {
            let failed_usage = failed_event_usage(&value);
            match staged.push(value) {
                Ok(events) => out.extend(events),
                Err(error) => {
                    // Usage in an explicit upstream failure is diagnostic telemetry, not semantic
                    // output. Preserve it while rolling back text/tool/terminal state.
                    if let Some(usage) = failed_usage {
                        self.usage = Some(usage);
                    }
                    return Err(error);
                }
            }
        }
        *self = staged;
        Ok(out)
    }

    /// Apply a decoded chunk without cloning all accumulated text/tool state.
    ///
    /// Callers must terminate the stream if this returns an error. Events remain staged in the
    /// returned vector until the entire chunk succeeds, so an invalid tail cannot leak earlier
    /// deltas downstream; retaining the partial reducer state also preserves failed-event usage.
    pub fn push_batch_in_place(
        &mut self,
        values: impl IntoIterator<Item = Value>,
    ) -> anyhow::Result<Vec<ReducerEvent>> {
        let mut out = Vec::new();
        for value in values {
            out.extend(self.push(value)?);
        }
        Ok(out)
    }

    pub fn stage_batch(
        &self,
        values: impl IntoIterator<Item = Value>,
    ) -> anyhow::Result<(Self, Vec<ReducerEvent>)> {
        let mut staged = self.clone();
        let events = staged.push_batch_in_place(values)?;
        Ok((staged, events))
    }
    fn delta(&mut self, kind: &str, delta: &str) -> anyhow::Result<Vec<ReducerEvent>> {
        let mut out = Vec::new();
        if self
            .active
            .as_ref()
            .is_none_or(|(active, _)| active != kind)
        {
            out.extend(self.close_active()?);
            let index = self.next_index;
            // Grok reasoning is progress, not an Anthropic content block. Reserving an index for
            // it would leave a hole once the plaintext reasoning is hidden downstream.
            if kind != "thinking" {
                self.next_index += 1;
            }
            self.active = Some((kind.into(), index));
            if kind == "text" {
                self.active_text.clear();
            }
            out.push(if kind == "thinking" {
                ReducerEvent::ThinkingStart(index)
            } else {
                ReducerEvent::TextStart(index)
            });
        }
        let index = self.active.as_ref().unwrap().1;
        if kind == "text" {
            self.active_text.push_str(delta);
            self.saw_text_output |= !delta.is_empty();
        }
        out.push(if kind == "thinking" {
            ReducerEvent::ThinkingDelta(index, delta.into())
        } else {
            ReducerEvent::TextDelta(index, delta.into())
        });
        Ok(out)
    }
    fn close_active(&mut self) -> anyhow::Result<Vec<ReducerEvent>> {
        Ok(match self.active.take() {
            Some((kind, index)) if kind == "thinking" => vec![ReducerEvent::ThinkingStop(index)],
            Some((_, index)) => {
                self.active_text.clear();
                vec![ReducerEvent::TextStop(index)]
            }
            None => vec![],
        })
    }
    fn complete_text_snapshot(&mut self, snapshot: &str) -> anyhow::Result<Vec<ReducerEvent>> {
        if snapshot.is_empty() {
            return Ok(Vec::new());
        }
        let suffix = match self.active.as_ref() {
            Some((kind, _)) if kind == "text" => snapshot.strip_prefix(&self.active_text),
            None if !self.saw_text_output => Some(snapshot),
            _ => None,
        };
        match suffix {
            Some(suffix) if !suffix.is_empty() => self.delta("text", suffix),
            _ => Ok(Vec::new()),
        }
    }
    fn close_kind(&mut self, kind: &str) -> anyhow::Result<Vec<ReducerEvent>> {
        if self
            .active
            .as_ref()
            .is_some_and(|(active, _)| active == kind)
        {
            self.close_active()
        } else {
            Ok(vec![])
        }
    }
    pub fn finished(&self) -> bool {
        self.completed
    }

    pub fn usage(&self) -> Option<&GrokUsage> {
        self.usage.as_ref()
    }

    pub fn incomplete_reason(&self) -> Option<&str> {
        self.incomplete_reason.as_deref()
    }
}

fn bounded_incomplete_reason(reason: &str) -> String {
    super::super::text::truncate_utf8(reason, MAX_INCOMPLETE_REASON_BYTES)
        .0
        .to_owned()
}

fn grok_usage(response: &Value) -> GrokUsage {
    let Some(usage) = response.get("usage") else {
        return GrokUsage::default();
    };
    GrokUsage {
        input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
        output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
        cached_input_tokens: usage
            .get("input_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64),
        reasoning_tokens: usage
            .get("output_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64),
        total_tokens: usage.get("total_tokens").and_then(Value::as_u64),
    }
}

fn failed_event_usage(value: &Value) -> Option<GrokUsage> {
    let event_type = value.get("type").and_then(Value::as_str)?;
    if !matches!(event_type, "error" | "response.failed" | "response.error") {
        return None;
    }
    let response = value.get("response").unwrap_or(value);
    response.get("usage")?;
    Some(grok_usage(response))
}

fn parse_upstream_failure(value: &Value, event_type: &str) -> GrokUpstreamFailure {
    let response = value.get("response").unwrap_or(value);
    let error = response
        .get("error")
        .or_else(|| value.get("error"))
        .unwrap_or(response);
    let message = failure_message(value, response, error);
    let explicit_status = [error, response, value]
        .into_iter()
        .find_map(failure_status);
    let classification = failure_code(error, response, value)
        .and_then(classify_failure_code)
        .or_else(|| classify_failure_message(&message));
    let (status, classified) = explicit_status
        .map(|status| (status, true))
        .or_else(|| classification.map(|status| (status, true)))
        .unwrap_or((502, false));
    let retry_after = [error, response, value]
        .into_iter()
        .find_map(|object| object.get("retry_after").and_then(stringify_retry_after));

    GrokUpstreamFailure {
        event_type: event_type.to_owned(),
        status,
        retry_after,
        message,
        retryable: classified && matches!(status, 429 | 500 | 502 | 503 | 504 | 529),
    }
}

fn failure_message(value: &Value, response: &Value, error: &Value) -> String {
    let message = [error, response, value]
        .into_iter()
        .find_map(|object| {
            object
                .get("message")
                .and_then(Value::as_str)
                .filter(|message| !message.trim().is_empty())
        })
        .or_else(|| error.as_str().filter(|message| !message.trim().is_empty()))
        .unwrap_or("Grok upstream stream failed");
    super::super::text::truncate_utf8(message, MAX_UPSTREAM_FAILURE_MESSAGE_BYTES)
        .0
        .to_owned()
}

fn failure_status(value: &Value) -> Option<u16> {
    ["status_code", "http_status", "status"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(parse_status))
}

fn parse_status(value: &Value) -> Option<u16> {
    let status = value
        .as_u64()
        .and_then(|status| u16::try_from(status).ok())
        .or_else(|| value.as_str()?.parse::<u16>().ok())?;
    (400..=599).contains(&status).then_some(status)
}

fn failure_code<'a>(error: &'a Value, response: &'a Value, value: &'a Value) -> Option<&'a str> {
    ["code", "error_type", "type"]
        .into_iter()
        .find_map(|field| error.get(field).and_then(Value::as_str))
        .or_else(|| {
            ["code", "error_type"]
                .into_iter()
                .find_map(|field| response.get(field).and_then(Value::as_str))
        })
        .or_else(|| {
            ["code", "error_type"]
                .into_iter()
                .find_map(|field| value.get(field).and_then(Value::as_str))
        })
}

fn classify_failure_code(code: &str) -> Option<u16> {
    let code = code.trim().to_ascii_lowercase();
    if let Ok(status) = code.parse::<u16>()
        && (400..=599).contains(&status)
    {
        return Some(status);
    }
    match code.as_str() {
        "rate_limit_error" | "rate_limit_exceeded" | "too_many_requests" => Some(429),
        "overloaded_error" | "overloaded" | "server_overloaded" => Some(529),
        "authentication_error" | "unauthorized" | "invalid_api_key" => Some(401),
        "permission_error" | "forbidden" => Some(403),
        "not_found_error" | "not_found" => Some(404),
        "conflict_error" | "conflict" => Some(409),
        "unprocessable_entity" => Some(422),
        "invalid_request_error" | "bad_request" | "context_length_exceeded" => Some(400),
        "gateway_timeout" | "timeout" => Some(504),
        "service_unavailable" | "temporarily_unavailable" => Some(503),
        "server_error" | "internal_server_error" | "internal_error" | "api_error" => Some(500),
        _ => None,
    }
}

fn classify_failure_message(message: &str) -> Option<u16> {
    let message = message.to_ascii_lowercase();
    if message.contains("rate limit") || message.contains("too many requests") {
        Some(429)
    } else if message.contains("overload") {
        Some(529)
    } else if message.contains("service unavailable")
        || message.contains("temporarily unavailable")
    {
        Some(503)
    } else if message.contains("gateway timeout")
        || message.contains("timed out")
        || message.contains("timeout")
    {
        Some(504)
    } else if message.contains("internal server") || message.contains("server error") {
        Some(500)
    } else if message.contains("context window")
        || message.contains("context length")
        || message.contains("invalid request")
    {
        Some(400)
    } else if message.contains("unauthorized") || message.contains("authentication") {
        Some(401)
    } else if message.contains("forbidden") || message.contains("permission denied") {
        Some(403)
    } else {
        None
    }
}

fn stringify_retry_after(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(super) fn parse_function_arguments(arguments: &str) -> anyhow::Result<Value> {
    match parse_json_object(arguments) {
        Ok(value) => Ok(value),
        Err(JsonObjectError::Empty | JsonObjectError::Invalid(_)) => {
            anyhow::bail!("function arguments are incomplete")
        }
        Err(JsonObjectError::NotObject) => {
            anyhow::bail!("function arguments must be a JSON object")
        }
    }
}

fn response_output_text(response: &Value) -> Vec<String> {
    let mut text = Vec::new();
    let Some(output) = response.get("output").and_then(Value::as_array) else {
        return text;
    };
    for item in output {
        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };
        for part in content {
            if part.get("type").and_then(Value::as_str) == Some("output_text")
                && let Some(value) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
            {
                text.push(value.to_string());
            }
        }
    }
    text
}

pub fn reduce_upstream_bytes(bytes: &[u8]) -> Result<Vec<ReducerEvent>, GrokReductionError> {
    let mut reducer = Reducer::default();
    let mut decoder = SseDecoder::default();
    let values = decoder
        .push(bytes)
        .map_err(|error| GrokReductionError::new(error, None))?
        .into_iter()
        .map(|event| {
            serde_json::from_str(&event.data)
                .map_err(|_| anyhow::anyhow!("malformed Grok SSE event"))
        })
        .collect::<anyhow::Result<Vec<Value>>>()
        .map_err(|error| GrokReductionError::new(error, None))?;
    let out = reducer
        .push_batch(values)
        .map_err(|error| GrokReductionError::new(error, reducer.usage().cloned()))?;
    decoder
        .finish()
        .map_err(|error| GrokReductionError::new(error, reducer.usage().cloned()))?;
    if !reducer.finished() {
        return Err(GrokReductionError::new(
            anyhow::anyhow!("Grok stream ended without completion"),
            reducer.usage().cloned(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_validator_ignores_only_the_same_number_and_payload() {
        let mut reducer = Reducer::default();
        let first = serde_json::from_str(
            r#"{"type":"response.output_text.delta","delta":"yes 😊","sequence_number":7}"#,
        )
        .unwrap();
        let reordered = serde_json::from_str(
            r#"{"sequence_number":7,"delta":"yes 😊","type":"response.output_text.delta"}"#,
        )
        .unwrap();

        assert_eq!(reducer.push(first).unwrap().len(), 2);
        assert!(reducer.push(reordered).unwrap().is_empty());

        let repeated_text_new_sequence = serde_json::json!({
            "sequence_number": 8,
            "type": "response.output_text.delta",
            "delta": "yes 😊"
        });
        let events = reducer.push(repeated_text_new_sequence).unwrap();
        assert!(matches!(
            events.as_slice(),
            [ReducerEvent::TextDelta(_, text)] if text == "yes 😊"
        ));
        let old_duplicate = serde_json::json!({
            "type": "response.output_text.delta",
            "delta": "yes 😊",
            "sequence_number": 7
        });
        assert!(reducer.push(old_duplicate).unwrap().is_empty());

        let same_text_again = serde_json::json!({
            "sequence_number": 9,
            "type": "response.output_text.delta",
            "delta": "yes 😊"
        });
        assert!(matches!(
            reducer.push(same_text_again).unwrap().as_slice(),
            [ReducerEvent::TextDelta(_, text)] if text == "yes 😊"
        ));
    }

    #[test]
    fn sequence_validator_rejects_conflicts_backwards_and_mixed_streams() {
        let mut conflict = Reducer::default();
        conflict
            .push(serde_json::json!({
                "sequence_number": 1,
                "type": "response.output_text.delta",
                "delta": "a"
            }))
            .unwrap();
        let error = conflict
            .push(serde_json::json!({
                "sequence_number": 1,
                "type": "response.output_text.delta",
                "delta": "b"
            }))
            .unwrap_err();
        assert!(error.to_string().contains("different payload"));

        let mut backwards = Reducer::default();
        backwards
            .push(serde_json::json!({
                "sequence_number": 5,
                "type": "response.created"
            }))
            .unwrap();
        assert!(
            backwards
                .push(serde_json::json!({
                    "sequence_number": 4,
                    "type": "response.in_progress"
                }))
                .unwrap_err()
                .to_string()
                .contains("backwards")
        );

        let mut mixed = Reducer::default();
        mixed
            .push(serde_json::json!({"type": "response.created"}))
            .unwrap();
        assert!(
            mixed
                .push(serde_json::json!({
                    "sequence_number": 1,
                    "type": "response.in_progress"
                }))
                .is_err()
        );
    }

    #[test]
    fn sequence_validator_deduplicates_tool_deltas_before_state_mutation() {
        let mut reducer = Reducer::default();
        reducer
            .push(serde_json::json!({
                "sequence_number": 0,
                "type": "response.output_item.added",
                "item": {"type":"function_call","call_id":"call_1","name":"lookup"}
            }))
            .unwrap();
        let delta = serde_json::json!({
            "sequence_number": 1,
            "type": "response.function_call_arguments.delta",
            "call_id": "call_1",
            "delta": "{}"
        });
        assert_eq!(reducer.push(delta.clone()).unwrap().len(), 1);
        assert!(reducer.push(delta).unwrap().is_empty());
    }

    #[test]
    fn sequence_validator_rejects_gaps_and_ignores_repeated_terminal_event() {
        let mut gap = Reducer::default();
        gap.push(serde_json::json!({
            "sequence_number": 3,
            "type": "response.created"
        }))
        .unwrap();
        let error = gap
            .push(serde_json::json!({
                "sequence_number": 5,
                "type": "response.output_text.delta",
                "delta": "unsafe gap"
            }))
            .unwrap_err();
        assert!(error.to_string().contains("gap"));

        let mut terminal = Reducer::default();
        let completed = serde_json::json!({
            "sequence_number": 12,
            "type": "response.completed",
            "response": {"usage":{"input_tokens":1,"output_tokens":1}}
        });
        assert!(!terminal.push(completed.clone()).unwrap().is_empty());
        assert!(terminal.push(completed).unwrap().is_empty());
    }

    #[test]
    fn terminal_tail_policy_allows_only_rate_limit_telemetry() {
        let completed = serde_json::json!({
            "type": "response.completed",
            "response": {"usage": {}}
        });
        let mut allowed = Reducer::default();
        let events = allowed
            .push_batch([
                completed.clone(),
                serde_json::json!({"type":"rate_limits.updated","remaining":42}),
            ])
            .unwrap();
        assert!(allowed.finished());
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ReducerEvent::Finish { .. }))
        );

        for tail in [
            serde_json::json!({"type":"response.output_text.delta","delta":"unsafe"}),
            serde_json::json!({"type":"response.output_item.added","item":{"type":"message"}}),
            serde_json::json!({"type":"response.usage","usage":{}}),
            serde_json::json!({"type":"future.telemetry"}),
        ] {
            let mut reducer = Reducer::default();
            let error = reducer.push_batch([completed.clone(), tail]).unwrap_err();
            assert!(
                error.to_string().contains("event after terminal"),
                "{error:#}"
            );
            assert!(
                !reducer.finished(),
                "a rejected same-batch tail must roll back the terminal"
            );
        }
    }

    #[test]
    fn failed_event_usage_survives_transaction_rollback() {
        let mut reducer = Reducer::default();
        let error = reducer
            .push_batch([
                serde_json::json!({"type":"response.output_text.delta","delta":"discarded"}),
                serde_json::json!({
                    "type":"response.failed",
                    "response":{"usage":{
                        "input_tokens":12,
                        "input_tokens_details":{"cached_tokens":3},
                        "output_tokens":2
                    }}
                }),
            ])
            .unwrap_err();

        assert!(error.to_string().contains("failed"));
        let usage = reducer.usage().expect("failed response usage must survive");
        assert_eq!(usage.mapped_input_tokens(), Some(9));
        assert_eq!(usage.mapped_cache_read_input_tokens(), Some(3));
        assert_eq!(usage.output_tokens, Some(2));
        assert!(!reducer.finished());
    }

    #[test]
    fn numbered_terminal_ignores_unsequenced_and_duplicate_rate_limit_telemetry() {
        let completed = serde_json::json!({
            "sequence_number": 41,
            "type": "response.completed",
            "response": {"usage": {}}
        });
        let telemetry = serde_json::json!({
            "type": "rate_limits.updated",
            "remaining": 42
        });
        let mut reducer = Reducer::default();
        let events = reducer
            .push_batch([completed, telemetry.clone(), telemetry])
            .unwrap();

        assert!(reducer.finished());
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ReducerEvent::Finish { .. }))
                .count(),
            1
        );

        let unsequenced_text = reducer
            .push(serde_json::json!({
                "type":"response.output_text.delta",
                "delta":"unsafe"
            }))
            .unwrap_err();
        assert!(
            unsequenced_text
                .to_string()
                .contains("omitted sequence_number"),
            "{unsequenced_text:#}"
        );

        let sequenced_text = reducer
            .push(serde_json::json!({
                "sequence_number":42,
                "type":"response.output_text.delta",
                "delta":"unsafe"
            }))
            .unwrap_err();
        assert!(
            sequenced_text.to_string().contains("event after terminal"),
            "{sequenced_text:#}"
        );
    }

    #[test]
    fn incomplete_reason_mapping_is_explicit_and_conservative() {
        let cases = [
            (
                Some("max_output_tokens"),
                GrokTerminal::IncompleteMaxOutputTokens,
                "max_tokens",
            ),
            (
                Some("max_tokens"),
                GrokTerminal::IncompleteMaxOutputTokens,
                "max_tokens",
            ),
            (
                Some("length"),
                GrokTerminal::IncompleteMaxOutputTokens,
                "max_tokens",
            ),
            (
                Some("content_filter"),
                GrokTerminal::IncompleteContentFilter,
                "refusal",
            ),
            (
                Some("upstream_capacity"),
                GrokTerminal::IncompleteOther,
                "refusal",
            ),
            (None, GrokTerminal::IncompleteMissingReason, "refusal"),
        ];

        for (reason, expected_terminal, expected_stop) in cases {
            let mut response = serde_json::json!({"status":"incomplete","usage":{}});
            if let Some(reason) = reason {
                response["incomplete_details"] = serde_json::json!({"reason":reason});
            }
            let mut reducer = Reducer::default();
            let events = reducer
                .push(serde_json::json!({
                    "type":"response.incomplete",
                    "response":response
                }))
                .unwrap();
            assert!(events.iter().any(
                |event| matches!(event, ReducerEvent::Terminal(status) if *status == expected_terminal)
            ));
            assert!(events.iter().any(|event| matches!(
                event,
                ReducerEvent::Finish { stop_reason, .. } if stop_reason == expected_stop
            )));
            assert_eq!(reducer.incomplete_reason(), reason);
        }
    }

    #[test]
    fn unknown_incomplete_reason_is_utf8_safely_bounded_for_logs() {
        let reason = format!("{}😊tail", "x".repeat(255));
        let mut reducer = Reducer::default();
        reducer
            .push(serde_json::json!({
                "type":"response.incomplete",
                "response":{
                    "status":"incomplete",
                    "incomplete_details":{"reason":reason},
                    "usage":{}
                }
            }))
            .unwrap();

        let captured = reducer.incomplete_reason().unwrap();
        assert!(captured.len() <= MAX_INCOMPLETE_REASON_BYTES);
        assert_eq!(captured, "x".repeat(255));
    }

    #[test]
    fn grok_reducer_handles_text_tool_and_completion() {
        let input = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"call_1\",\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":2}}}\n\n";
        let events = reduce_upstream_bytes(input).unwrap();
        assert!(
            matches!(events.last(), Some(ReducerEvent::Finish { stop_reason, .. }) if stop_reason == "tool_use")
        );
    }

    #[test]
    fn grok_reducer_maps_item_id_argument_events() {
        let input = b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"id\":\"item_1\",\"call_id\":\"call_1\",\"name\":\"Bash\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\",\"arguments\":\"{}\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        let events = reduce_upstream_bytes(input).unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ReducerEvent::ToolDelta(_, delta) if delta == "{}"))
        );
        assert!(matches!(events.last(), Some(ReducerEvent::Finish { .. })));
    }

    #[test]
    fn grok_reducer_accepts_live_reasoning_text_events() {
        let input = b"data: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"think\"}\n\ndata: {\"type\":\"response.reasoning_text.done\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":4,\"output_tokens\":2}}}\n\n";
        let events = reduce_upstream_bytes(input).unwrap();
        assert!(matches!(events[0], ReducerEvent::ThinkingStart(0)));
        assert!(matches!(
            &events[1],
            ReducerEvent::ThinkingDelta(0, delta) if delta == "think"
        ));
        assert!(matches!(events.last(), Some(ReducerEvent::Finish { .. })));
    }

    #[test]
    fn grok_reducer_accepts_hosted_search_lifecycle() {
        let input = b"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"custom_tool_call\",\"name\":\"x_keyword_search\",\"id\":\"search_1\"}}\n\ndata: {\"type\":\"response.custom_tool_call_input.delta\",\"item_id\":\"search_1\",\"delta\":\"{\\\"query\\\":\\\"test\\\"}\"}\n\ndata: {\"type\":\"response.custom_tool_call_input.done\",\"item_id\":\"search_1\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"custom_tool_call\",\"name\":\"x_keyword_search\",\"id\":\"search_1\"}}\n\ndata: {\"type\":\"response.output_text.annotation.added\",\"annotation\":{\"type\":\"url_citation\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"result\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        let events = reduce_upstream_bytes(input).unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            ReducerEvent::HostedSearch { name, query, .. }
                if name == "x_search" && query == "test"
        )));
        assert!(matches!(
            events.last(),
            Some(ReducerEvent::Finish {
                x_search_requests: 1,
                ..
            })
        ));
    }

    #[test]
    fn grok_reducer_accepts_reasoning_summary_part_completion() {
        let input = b"data: {\"type\":\"response.reasoning_summary_part.added\"}\n\ndata: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"think\"}\n\ndata: {\"type\":\"response.reasoning_summary_text.done\"}\n\ndata: {\"type\":\"response.reasoning_summary_part.done\",\"part\":{\"type\":\"summary_text\",\"text\":\"think\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        let events = reduce_upstream_bytes(input).unwrap();
        assert!(matches!(events.last(), Some(ReducerEvent::Finish { .. })));
    }

    #[test]
    fn grok_reducer_accepts_complete_observed_lifecycle() {
        let input = b"data: {\"type\":\"response.created\"}\n\ndata: {\"type\":\"response.in_progress\"}\n\ndata: {\"type\":\"response.reasoning_summary_part.added\"}\n\ndata: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"think\"}\n\ndata: {\"type\":\"response.reasoning_summary_text.done\"}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\"}}\n\ndata: {\"type\":\"response.content_part.added\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\ndata: {\"type\":\"response.output_text.done\"}\n\ndata: {\"type\":\"response.content_part.done\"}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"call_1\",\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.function_call_arguments.done\",\"call_id\":\"call_1\",\"arguments\":\"{}\"}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":4,\"output_tokens\":2}}}\n\n";
        let events = reduce_upstream_bytes(input).unwrap();
        assert!(matches!(
            events.last(),
            Some(ReducerEvent::Finish {
                input_tokens: 4,
                output_tokens: 2,
                ..
            })
        ));
    }

    #[test]
    fn grok_reducer_uses_done_snapshot_without_replaying_deltas() {
        let input = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"answer \"}\n\ndata: {\"type\":\"response.output_text.done\",\"text\":\"answer 😊\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer 😊\"}]}]}}\n\n";
        let events = reduce_upstream_bytes(input.as_bytes()).unwrap();
        let text = events
            .iter()
            .filter_map(|event| match event {
                ReducerEvent::TextDelta(_, text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();

        assert_eq!(text, "answer 😊");
    }

    #[test]
    fn grok_reducer_recovers_snapshot_only_response() {
        let input = "data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"snapshot only 😊\"}]}],\"usage\":{}}}\n\n";
        let events = reduce_upstream_bytes(input.as_bytes()).unwrap();

        assert!(events.iter().any(
            |event| matches!(event, ReducerEvent::TextDelta(0, text) if text == "snapshot only 😊")
        ));
    }

    #[test]
    fn incomplete_response_closes_text_and_preserves_reported_usage_details() {
        let input = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\ndata: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":3},\"output_tokens\":7,\"output_tokens_details\":{\"reasoning_tokens\":5},\"total_tokens\":19}}}\n\n";
        let events = reduce_upstream_bytes(input).unwrap();

        assert!(
            events
                .iter()
                .any(|event| matches!(event, ReducerEvent::TextStop(0)))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ReducerEvent::Terminal(GrokTerminal::IncompleteMaxOutputTokens)
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ReducerEvent::Usage(GrokUsage {
                input_tokens: Some(12),
                output_tokens: Some(7),
                cached_input_tokens: Some(3),
                reasoning_tokens: Some(5),
                total_tokens: Some(19),
            })
        )));
        assert!(matches!(
            events.last(),
            Some(ReducerEvent::Finish {
                stop_reason,
                input_tokens: 12,
                output_tokens: 7,
                ..
            }) if stop_reason == "max_tokens"
        ));
    }

    #[test]
    fn missing_usage_is_distinct_from_reported_zero() {
        let missing = reduce_upstream_bytes(
            b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n",
        )
        .unwrap();
        let zero = reduce_upstream_bytes(
            b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\n",
        )
        .unwrap();

        assert!(missing.iter().any(|event| matches!(
            event,
            ReducerEvent::Usage(usage) if usage.availability_state() == "unavailable"
        )));
        assert!(zero.iter().any(|event| matches!(
            event,
            ReducerEvent::Usage(usage) if usage.availability_state() == "reported"
        )));
    }

    #[test]
    fn completed_function_call_rejects_non_object_arguments() {
        for arguments in ["null", "[]", "\"text\""] {
            let input = format!(
                "data: {{\"type\":\"response.output_item.added\",\"item\":{{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}}}\n\ndata: {{\"type\":\"response.function_call_arguments.done\",\"call_id\":\"call_1\",\"arguments\":{arguments:?}}}\n\ndata: {{\"type\":\"response.output_item.done\",\"item\":{{\"type\":\"function_call\",\"call_id\":\"call_1\"}}}}\n\n"
            );
            let error = reduce_upstream_bytes(input.as_bytes()).unwrap_err();
            assert!(error.to_string().contains("JSON object"), "{error:#}");
        }
    }

    #[test]
    fn upstream_failure_events_preserve_retry_semantics() {
        let cases = [
            (
                concat!(r#"data: {"type":"response.failed","response":{"error":{"type":"overloaded_error","message":"capacity exhausted"},"retry_after":0.25,"usage":{"input_tokens":7}}}"#, "\n\n"),
                "response.failed",
                529,
                true,
                Some("0.25"),
                "capacity exhausted",
            ),
            (
                concat!(r#"data: {"type":"error","code":"rate_limit_exceeded","message":"slow down","retry_after":"1"}"#, "\n\n"),
                "error",
                429,
                true,
                Some("1"),
                "slow down",
            ),
            (
                concat!(r#"data: {"type":"response.error","response":{"error":{"status_code":400,"message":"invalid tool schema"}}}"#, "\n\n"),
                "response.error",
                400,
                false,
                None,
                "invalid tool schema",
            ),
            (
                concat!(r#"data: {"type":"response.failed","response":{"error":{"message":"model failed"}}}"#, "\n\n"),
                "response.failed",
                502,
                false,
                None,
                "model failed",
            ),
        ];

        for (wire, event_type, status, retryable, retry_after, message) in cases {
            let error = reduce_upstream_bytes(wire.as_bytes()).unwrap_err();
            let failure = error
                .upstream_failure()
                .expect("failure event must remain structured");
            assert_eq!(failure.event_type, event_type);
            assert_eq!(failure.status, status);
            assert_eq!(failure.retryable, retryable);
            assert_eq!(failure.retry_after.as_deref(), retry_after);
            assert_eq!(failure.message, message);
        }
    }

    #[test]
    fn response_error_preserves_failure_usage() {
        let error = reduce_upstream_bytes(
            b"data: {\"type\":\"response.error\",\"response\":{\"error\":{\"code\":\"service_unavailable\",\"message\":\"try later\"},\"usage\":{\"input_tokens\":9,\"output_tokens\":1}}}\n\n",
        )
        .unwrap_err();

        let usage = error.usage().expect("failure usage must survive reduction");
        assert_eq!(usage.input_tokens, Some(9));
        assert_eq!(usage.output_tokens, Some(1));
        assert_eq!(error.upstream_failure().unwrap().status, 503);
    }
}
