use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::mem::size_of;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::ser::SerializeSeq;
use sha2::{Digest, Sha256};

use crate::provider::RequestLaneKey;
use crate::timeutil::now_ms;

use super::translate::request::{
    DeferredToolHydrationProvenance, DeferredToolHydrationResult, ResponsesFunctionCallOutput,
    ResponsesInputItem, ResponsesRequest, UNAVAILABLE_TOOL_REFERENCES_OUTPUT,
};

const TTL_MS: u64 = 30 * 60 * 1000;
const MAX_STATES: usize = 10_000;
const MAX_CONTINUATION_CANDIDATES_PER_LANE: usize = 2;
const MAX_PARALLEL_BATCHES_PER_SESSION: usize = 1_024;
const MAX_RESPONSE_ID_BYTES: usize = 512;
pub(super) const MAX_SESSION_TRANSCRIPT_BYTES: u64 = 2_000_000;
pub(super) const MAX_TOTAL_TRANSCRIPT_BYTES: u64 = 20_000_000;

const PROMPT_CHANGE_FIELDS: [&str; 12] = [
    "model",
    "instructions",
    "store",
    "stream",
    "parallel_tool_calls",
    "include",
    "client_metadata",
    "service_tier",
    "prompt_cache_key",
    "text_format",
    "reasoning",
    "unattributed",
];
const PROMPT_SIGNATURE_FIELD_COUNT: usize = PROMPT_CHANGE_FIELDS.len() - 1;
const PROMPT_CHANGE_UNATTRIBUTED_BIT: u16 = 1 << PROMPT_SIGNATURE_FIELD_COUNT;

#[derive(Clone, Debug, PartialEq, Eq)]
struct PromptSignature {
    safety_digest: [u8; 32],
    diagnostic_fields: [u64; PROMPT_SIGNATURE_FIELD_COUNT],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PromptChangeMask(u16);

impl PromptChangeMask {
    fn between(previous: &PromptSignature, current: &PromptSignature) -> Self {
        let mut mask = 0_u16;
        for (index, (previous, current)) in previous
            .diagnostic_fields
            .iter()
            .zip(&current.diagnostic_fields)
            .enumerate()
        {
            if previous != current {
                mask |= 1 << index;
            }
        }
        if previous.safety_digest != current.safety_digest && mask == 0 {
            mask |= PROMPT_CHANGE_UNATTRIBUTED_BIT;
        }
        Self(mask)
    }

    fn names(self) -> Vec<&'static str> {
        PROMPT_CHANGE_FIELDS
            .iter()
            .enumerate()
            .filter_map(|(index, name)| (self.0 & (1 << index) != 0).then_some(*name))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParallelBatchRange {
    start: usize,
    end: usize,
}

#[derive(Clone)]
struct ContinuationState {
    response_id: String,
    owner_turn_id: u64,
    prompt_signature: PromptSignature,
    transcript: Vec<ResponsesInputItem>,
    request_input_len: usize,
    parallel_batches: Vec<ParallelBatchRange>,
    deferred_tool_hydration: DeferredToolHydrationProvenance,
    transcript_bytes: u64,
    retained_bytes: u64,
    updated_at: u64,
}

struct SessionState {
    current_turn: u64,
    candidates: Vec<ContinuationState>,
    pending_fallback: Option<ContinuationState>,
    pending_parallel_batches: Vec<ParallelBatchRange>,
    pending: bool,
    updated_at: u64,
}

#[derive(Default)]
struct ContinuationRegistry {
    lanes: HashMap<RequestLaneKey, SessionState>,
    total_retained_bytes: u64,
}

fn session_retained_bytes(session: &SessionState) -> u64 {
    session
        .candidates
        .iter()
        .map(|state| state.retained_bytes)
        .chain(
            session
                .pending_fallback
                .iter()
                .map(|state| state.retained_bytes),
        )
        .fold(0, u64::saturating_add)
}

fn subtract_session_retained_bytes(registry: &mut ContinuationRegistry, session: &SessionState) {
    registry.total_retained_bytes = registry
        .total_retained_bytes
        .saturating_sub(session_retained_bytes(session));
}

static REGISTRY: Mutex<Option<ContinuationRegistry>> = Mutex::new(None);
static NEXT_TURN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct ContinuationCandidate {
    pub turn_id: Option<u64>,
    pub previous_response_id: Option<String>,
    /// Producing turn of `previous_response_id`. Together these fields form the exact
    /// `store:false` WebSocket affinity; response ids alone are not sufficient under delayed
    /// eviction or synthetic/id-reuse tests.
    pub previous_response_owner_turn_id: Option<u64>,
    pub matched_candidate_rank: Option<u8>,
    pub candidate_count: u8,
    /// This turn belongs to a strictly proven deferred-tool hydration fork
    /// whose alternate response-affine branch must remain reachable. A turn
    /// without `previous_response_id` opens the fork with full context; a
    /// primary exact hit carries the established fork forward.
    pub response_affine_fork: bool,
    pub input_delta: Option<Vec<ResponsesInputItem>>,
    pub input_delta_count: usize,
    pub disabled_reason: Option<String>,
}

struct CandidateEvaluation {
    candidate: ContinuationCandidate,
    parallel_batches: Vec<ParallelBatchRange>,
    prompt_change_mask: PromptChangeMask,
}

struct PrefixMatch {
    delta: Vec<ResponsesInputItem>,
    parallel_batches: Vec<ParallelBatchRange>,
}

pub struct ContinuationDecision {
    pub candidate: ContinuationCandidate,
    pub prompt_changed_fields: Vec<&'static str>,
}

pub fn continuation_candidate(
    lane_key: Option<&RequestLaneKey>,
    body: &ResponsesRequest,
    enabled: bool,
) -> ContinuationCandidate {
    continuation_candidate_with_diagnostics(lane_key, body, enabled).candidate
}

pub fn continuation_candidate_with_diagnostics(
    lane_key: Option<&RequestLaneKey>,
    body: &ResponsesRequest,
    enabled: bool,
) -> ContinuationDecision {
    if !enabled {
        clear_continuation(lane_key);
        return ContinuationDecision {
            candidate: ContinuationCandidate {
                turn_id: None,
                previous_response_id: None,
                previous_response_owner_turn_id: None,
                matched_candidate_rank: None,
                candidate_count: 0,
                response_affine_fork: false,
                input_delta: None,
                input_delta_count: body.input.len(),
                disabled_reason: Some("disabled".to_string()),
            },
            prompt_changed_fields: Vec::new(),
        };
    }

    let Some(lane_key) = lane_key else {
        return ContinuationDecision {
            candidate: ContinuationCandidate {
                turn_id: None,
                previous_response_id: None,
                previous_response_owner_turn_id: None,
                matched_candidate_rank: None,
                candidate_count: 0,
                response_affine_fork: false,
                input_delta: None,
                input_delta_count: body.input.len(),
                disabled_reason: Some("missing_session".to_string()),
            },
            prompt_changed_fields: Vec::new(),
        };
    };

    let turn_id = NEXT_TURN_ID.fetch_add(1, Ordering::Relaxed);
    let now = now_ms();
    let (mut states, superseded_turn) = {
        let mut guard = REGISTRY.lock().unwrap();
        let registry = guard.get_or_insert_with(ContinuationRegistry::default);
        let existing = registry.lanes.remove(lane_key);
        let superseded_turn = existing.as_ref().is_some_and(|session| session.pending);
        let states = match existing {
            Some(session) => {
                subtract_session_retained_bytes(registry, &session);
                if session.pending {
                    Vec::new()
                } else {
                    session.candidates
                }
            }
            None => Vec::new(),
        };
        registry.lanes.insert(
            *lane_key,
            SessionState {
                current_turn: turn_id,
                candidates: Vec::new(),
                pending_fallback: None,
                pending_parallel_batches: Vec::new(),
                pending: true,
                updated_at: now,
            },
        );
        evict_oldest(registry);
        (states, superseded_turn)
    };

    states.retain(|state| now.saturating_sub(state.updated_at) <= TTL_MS);
    states.truncate(MAX_CONTINUATION_CANDIDATES_PER_LANE);
    let candidate_count = u8::try_from(states.len()).unwrap_or(u8::MAX);

    let mut matched_index = None;
    let mut matched_evaluation = None;
    let mut first_evaluation = states.first().map(|state| {
        continuation_candidate_from_state(
            turn_id,
            body,
            Some(state),
            Some(0),
            candidate_count,
            false,
            now,
        )
    });
    let hydration_drift = first_evaluation.as_ref().is_some_and(|evaluation| {
        evaluation.candidate.disabled_reason.as_deref() == Some("not_append_only")
    }) && states
        .first()
        .is_some_and(|state| strict_deferred_tool_hydration_drift(body, state));
    if first_evaluation
        .as_ref()
        .is_some_and(|evaluation| evaluation.candidate.previous_response_id.is_some())
    {
        matched_index = Some(0);
        matched_evaluation = first_evaluation.take();
    } else if hydration_drift {
        for (index, state) in states.iter().enumerate().skip(1) {
            let evaluation = continuation_candidate_from_state(
                turn_id,
                body,
                Some(state),
                u8::try_from(index).ok(),
                candidate_count,
                false,
                now,
            );
            if evaluation.candidate.previous_response_id.is_some() {
                matched_index = Some(index);
                matched_evaluation = Some(evaluation);
                break;
            }
        }
    }

    let mut evaluation = matched_evaluation.or(first_evaluation).unwrap_or_else(|| {
        continuation_candidate_from_state(
            turn_id,
            body,
            None,
            None,
            candidate_count,
            superseded_turn,
            now,
        )
    });
    evaluation.candidate.response_affine_fork = match matched_index {
        Some(0) => states.len() > 1,
        None => hydration_drift,
        Some(_) => false,
    };
    let pending_fallback = match matched_index {
        // A delayed hydration transition can take multiple unavailable-tool turns. Keep the
        // loaded-history branch hidden while the newest exact branch is in flight, then publish
        // it again beside the new state only after this turn records successfully.
        Some(0) if states.len() > 1 => Some(states.swap_remove(1)),
        // Matching the older branch completes hydration. Do not retain either obsolete branch.
        Some(_) => None,
        // The proven hydration mismatch must use full context once. Retain only the newest exact
        // state so a later hydration restoration can recover without weakening item equality.
        None if evaluation.candidate.response_affine_fork => states.into_iter().next(),
        None => None,
    };

    let mut guard = REGISTRY.lock().unwrap();
    if let Some(registry) = guard.as_mut()
        && registry
            .lanes
            .get(lane_key)
            .is_some_and(|session| session.current_turn == turn_id && session.pending)
    {
        let fallback_bytes = pending_fallback
            .as_ref()
            .map_or(0, |state| state.retained_bytes);
        if let Some(session) = registry.lanes.get_mut(lane_key) {
            session.pending_parallel_batches = evaluation.parallel_batches;
            session.pending_fallback = pending_fallback;
        }
        registry.total_retained_bytes =
            registry.total_retained_bytes.saturating_add(fallback_bytes);
        evict_oldest(registry);
    }
    ContinuationDecision {
        candidate: evaluation.candidate,
        prompt_changed_fields: evaluation.prompt_change_mask.names(),
    }
}

fn continuation_candidate_from_state(
    turn_id: u64,
    body: &ResponsesRequest,
    state: Option<&ContinuationState>,
    candidate_rank: Option<u8>,
    candidate_count: u8,
    superseded_turn: bool,
    now: u64,
) -> CandidateEvaluation {
    let state = match state {
        Some(state) if now.saturating_sub(state.updated_at) <= TTL_MS => state,
        Some(_) | None => {
            return CandidateEvaluation::without_batches(ContinuationCandidate {
                turn_id: Some(turn_id),
                previous_response_id: None,
                previous_response_owner_turn_id: None,
                matched_candidate_rank: None,
                candidate_count,
                response_affine_fork: false,
                input_delta: None,
                input_delta_count: body.input.len(),
                disabled_reason: Some(if superseded_turn {
                    "superseded_turn".to_string()
                } else {
                    "missing_state".to_string()
                }),
            });
        }
    };

    let Some(signature) = prompt_signature(body) else {
        return CandidateEvaluation::without_batches(ContinuationCandidate {
            turn_id: Some(turn_id),
            previous_response_id: None,
            previous_response_owner_turn_id: None,
            matched_candidate_rank: None,
            candidate_count,
            response_affine_fork: false,
            input_delta: None,
            input_delta_count: body.input.len(),
            disabled_reason: Some("prompt_signature_error".to_string()),
        });
    };
    if signature != state.prompt_signature {
        let prompt_change_mask = PromptChangeMask::between(&state.prompt_signature, &signature);
        return CandidateEvaluation::with_prompt_changes(
            ContinuationCandidate {
                turn_id: Some(turn_id),
                previous_response_id: None,
                previous_response_owner_turn_id: None,
                matched_candidate_rank: None,
                candidate_count,
                response_affine_fork: false,
                input_delta: None,
                input_delta_count: body.input.len(),
                disabled_reason: Some("prompt_changed".to_string()),
            },
            prompt_change_mask,
        );
    }

    let Some(prefix_match) = input_suffix_after_prefix(
        &body.input,
        &state.transcript,
        state.request_input_len,
        &state.parallel_batches,
        body.parallel_tool_calls,
    ) else {
        return CandidateEvaluation::without_batches(ContinuationCandidate {
            turn_id: Some(turn_id),
            previous_response_id: None,
            previous_response_owner_turn_id: None,
            matched_candidate_rank: None,
            candidate_count,
            response_affine_fork: false,
            input_delta: None,
            input_delta_count: body.input.len(),
            disabled_reason: Some("not_append_only".to_string()),
        });
    };

    if prefix_match.delta.is_empty() {
        return CandidateEvaluation::without_batches(ContinuationCandidate {
            turn_id: Some(turn_id),
            previous_response_id: None,
            previous_response_owner_turn_id: None,
            matched_candidate_rank: None,
            candidate_count,
            response_affine_fork: false,
            input_delta: None,
            input_delta_count: 0,
            disabled_reason: Some("empty_delta".to_string()),
        });
    }

    CandidateEvaluation {
        candidate: ContinuationCandidate {
            turn_id: Some(turn_id),
            previous_response_id: Some(state.response_id.clone()),
            previous_response_owner_turn_id: Some(state.owner_turn_id),
            matched_candidate_rank: candidate_rank,
            candidate_count,
            response_affine_fork: false,
            input_delta_count: prefix_match.delta.len(),
            input_delta: Some(prefix_match.delta),
            disabled_reason: None,
        },
        parallel_batches: prefix_match.parallel_batches,
        prompt_change_mask: PromptChangeMask::default(),
    }
}

impl CandidateEvaluation {
    fn without_batches(candidate: ContinuationCandidate) -> Self {
        Self {
            candidate,
            parallel_batches: Vec::new(),
            prompt_change_mask: PromptChangeMask::default(),
        }
    }

    fn with_prompt_changes(
        candidate: ContinuationCandidate,
        prompt_change_mask: PromptChangeMask,
    ) -> Self {
        Self {
            candidate,
            parallel_batches: Vec::new(),
            prompt_change_mask,
        }
    }
}

struct TranscriptParts<'a> {
    input: &'a [ResponsesInputItem],
    output: &'a [ResponsesInputItem],
}

impl Serialize for TranscriptParts<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let item_count = self.input.len().checked_add(self.output.len());
        let mut sequence = serializer.serialize_seq(item_count)?;
        for item in self.input.iter().chain(self.output) {
            sequence.serialize_element(item)?;
        }
        sequence.end()
    }
}

struct BoundedCountingWriter {
    bytes: u64,
    limit: u64,
}

impl Write for BoundedCountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let buffer_len = u64::try_from(buffer.len())
            .map_err(|_| io::Error::other("serialized continuation size overflow"))?;
        let next = self
            .bytes
            .checked_add(buffer_len)
            .ok_or_else(|| io::Error::other("serialized continuation size overflow"))?;
        if next > self.limit {
            return Err(io::Error::other(
                "serialized continuation exceeded the retained-byte limit",
            ));
        }
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_transcript_bytes(
    input: &[ResponsesInputItem],
    output: &[ResponsesInputItem],
    limit: u64,
) -> Option<u64> {
    let mut writer = BoundedCountingWriter { bytes: 0, limit };
    serde_json::to_writer(&mut writer, &TranscriptParts { input, output }).ok()?;
    Some(writer.bytes)
}

fn continuation_retained_bytes(state: &ContinuationState) -> Option<u64> {
    let mut bytes = state.transcript_bytes;
    for value in [
        state.response_id.len(),
        size_of::<RequestLaneKey>(),
        size_of::<SessionState>(),
        size_of::<PromptSignature>(),
    ] {
        bytes = bytes.checked_add(u64::try_from(value).ok()?)?;
    }
    bytes = bytes.checked_add(
        u64::try_from(
            state
                .transcript
                .len()
                .checked_mul(size_of::<ResponsesInputItem>())?,
        )
        .ok()?,
    )?;
    bytes = bytes.checked_add(
        u64::try_from(
            state
                .parallel_batches
                .len()
                .checked_mul(size_of::<ParallelBatchRange>())?,
        )
        .ok()?,
    )?;
    bytes = bytes.checked_add(u64::try_from(size_of::<DeferredToolHydrationProvenance>()).ok()?)?;
    bytes = bytes.checked_add(
        u64::try_from(
            state
                .deferred_tool_hydration
                .loaded_groups
                .len()
                .checked_mul(size_of::<
                    super::translate::request::DeferredToolHydrationGroup,
                >())?,
        )
        .ok()?,
    )?;
    let result_count = state
        .deferred_tool_hydration
        .loaded_groups
        .iter()
        .try_fold(
            state.deferred_tool_hydration.unavailable_results.len(),
            |total, group| total.checked_add(group.results.len()),
        )?;
    bytes.checked_add(
        u64::try_from(result_count.checked_mul(size_of::<DeferredToolHydrationResult>())?).ok()?,
    )
}

pub fn record_continuation(
    lane_key: Option<&RequestLaneKey>,
    turn_id: Option<u64>,
    request_body: &ResponsesRequest,
    response_id: Option<&str>,
    output_items: &[ResponsesInputItem],
) {
    let (lane_key, turn_id) = match (lane_key, turn_id) {
        (Some(lane_key), Some(turn_id)) => (lane_key, turn_id),
        _ => return,
    };

    let response_id = match response_id {
        Some(id) if !id.is_empty() && id.len() <= MAX_RESPONSE_ID_BYTES => id.to_string(),
        Some(_) | None => {
            abort_continuation(Some(lane_key), Some(turn_id));
            return;
        }
    };

    let Some(prompt_signature) = prompt_signature(request_body) else {
        abort_continuation(Some(lane_key), Some(turn_id));
        return;
    };
    let Some(transcript_bytes) = serialized_transcript_bytes(
        &request_body.input,
        output_items,
        MAX_SESSION_TRANSCRIPT_BYTES,
    ) else {
        abort_continuation(Some(lane_key), Some(turn_id));
        return;
    };

    let mut transcript: Vec<ResponsesInputItem> = request_body.input.clone();
    transcript.extend_from_slice(output_items);

    let recorded_at = now_ms();
    let mut state = ContinuationState {
        response_id,
        owner_turn_id: turn_id,
        prompt_signature,
        transcript,
        request_input_len: request_body.input.len(),
        parallel_batches: Vec::new(),
        deferred_tool_hydration: request_body.deferred_tool_hydration.clone(),
        transcript_bytes,
        retained_bytes: 0,
        updated_at: recorded_at,
    };

    let mut guard = REGISTRY.lock().unwrap();
    let Some(registry) = guard.as_mut() else {
        return;
    };
    if !registry
        .lanes
        .get(lane_key)
        .is_some_and(|session| session.current_turn == turn_id && session.pending)
    {
        return;
    }
    let mut session = registry
        .lanes
        .remove(lane_key)
        .expect("current continuation turn disappeared while registry was locked");
    subtract_session_retained_bytes(registry, &session);
    if !session.pending {
        return;
    }
    if session.pending_parallel_batches.len() > MAX_PARALLEL_BATCHES_PER_SESSION
        || !parallel_batch_ranges_are_valid(
            &session.pending_parallel_batches,
            request_body.input.len(),
        )
    {
        return;
    }
    state.parallel_batches = std::mem::take(&mut session.pending_parallel_batches);
    let Some(retained_bytes) = continuation_retained_bytes(&state) else {
        return;
    };
    state.retained_bytes = retained_bytes;

    let mut candidates = Vec::with_capacity(MAX_CONTINUATION_CANDIDATES_PER_LANE);
    candidates.push(state);
    if let Some(fallback) = session.pending_fallback.take()
        && recorded_at.saturating_sub(fallback.updated_at) <= TTL_MS
    {
        candidates.push(fallback);
    }
    debug_assert!(candidates.len() <= MAX_CONTINUATION_CANDIDATES_PER_LANE);
    let published_bytes = candidates
        .iter()
        .map(|candidate| candidate.retained_bytes)
        .fold(0, u64::saturating_add);
    registry.total_retained_bytes = registry
        .total_retained_bytes
        .saturating_add(published_bytes);
    registry.lanes.insert(
        *lane_key,
        SessionState {
            current_turn: turn_id,
            candidates,
            pending_fallback: None,
            pending_parallel_batches: Vec::new(),
            pending: false,
            updated_at: recorded_at,
        },
    );
    evict_oldest(registry);
}

pub fn abort_continuation(lane_key: Option<&RequestLaneKey>, turn_id: Option<u64>) {
    let (Some(lane_key), Some(turn_id)) = (lane_key, turn_id) else {
        return;
    };
    let mut guard = REGISTRY.lock().unwrap();
    let Some(registry) = guard.as_mut() else {
        return;
    };
    if registry
        .lanes
        .get(lane_key)
        .is_some_and(|session| session.current_turn == turn_id)
        && let Some(session) = registry.lanes.remove(lane_key)
    {
        subtract_session_retained_bytes(registry, &session);
    }
}

/// Stop a pending response publication while keeping its independently reachable exact fallback.
///
/// This is narrower than [`abort_continuation`]: a `store:false` response can lose its newly
/// bound socket between the terminal event and `record_continuation`, while an older hydration
/// branch still owns a healthy socket. Publishing the new response would be unsafe, but deleting
/// the fallback would throw away a valid continuation. Resetting `current_turn` to the fallback's
/// producing turn also makes late abort/record callbacks for the abandoned turn harmless.
pub fn abandon_pending_response_preserving_fallback(
    lane_key: Option<&RequestLaneKey>,
    turn_id: Option<u64>,
) {
    let (Some(lane_key), Some(turn_id)) = (lane_key, turn_id) else {
        return;
    };
    let mut guard = REGISTRY.lock().unwrap();
    let Some(registry) = guard.as_mut() else {
        return;
    };
    let is_pending_owner = registry
        .lanes
        .get(lane_key)
        .is_some_and(|session| session.pending && session.current_turn == turn_id);
    if !is_pending_owner {
        return;
    }

    let mut session = registry
        .lanes
        .remove(lane_key)
        .expect("pending continuation disappeared while registry was locked");
    subtract_session_retained_bytes(registry, &session);
    let now = now_ms();
    let Some(fallback) = session
        .pending_fallback
        .take()
        .filter(|fallback| now.saturating_sub(fallback.updated_at) <= TTL_MS)
    else {
        return;
    };

    session.current_turn = fallback.owner_turn_id;
    session.candidates = vec![fallback];
    session.pending_parallel_batches.clear();
    session.pending = false;
    session.updated_at = now;
    registry.total_retained_bytes = registry
        .total_retained_bytes
        .saturating_add(session_retained_bytes(&session));
    registry.lanes.insert(*lane_key, session);
    evict_oldest(registry);
}

/// Drop a hidden exact fallback when this turn stops using its selected response id and retries
/// with full context. The turn itself remains current so a successful retry can publish a fresh
/// continuation state.
pub fn discard_pending_fallback(lane_key: Option<&RequestLaneKey>, turn_id: Option<u64>) {
    let (Some(lane_key), Some(turn_id)) = (lane_key, turn_id) else {
        return;
    };
    let mut guard = REGISTRY.lock().unwrap();
    let Some(registry) = guard.as_mut() else {
        return;
    };
    let removed_bytes = registry
        .lanes
        .get_mut(lane_key)
        .filter(|session| session.current_turn == turn_id && session.pending)
        .and_then(|session| session.pending_fallback.take())
        .map_or(0, |state| state.retained_bytes);
    registry.total_retained_bytes = registry.total_retained_bytes.saturating_sub(removed_bytes);
}

/// Remove continuation states whose `store:false` response is no longer reachable on an idle
/// WebSocket. Callers must not hold the WebSocket-pool lock: continuation turn checks acquire the
/// registry before pool operations, so reversing that order would deadlock.
pub fn invalidate_response_affinity(lane_key: &RequestLaneKey, response_id: &str) {
    invalidate_response_affinity_inner(lane_key, response_id, None);
}

/// Remove a response-affine continuation after its owning WebSocket is evicted.
///
/// An idle socket can be evicted after its terminal response has been bound to the socket but
/// before `record_continuation` publishes that response id. In that window there is no id in the
/// registry to remove, so affinity invalidation must reject that pending publication while
/// restoring an independently reachable fallback. Once the turn has recorded (or a newer turn
/// owns the lane), normal composite-affinity removal preserves every unrelated branch.
pub fn invalidate_response_affinity_for_turn(
    lane_key: &RequestLaneKey,
    response_id: &str,
    owner_turn_id: u64,
) {
    invalidate_response_affinity_inner(lane_key, response_id, Some(owner_turn_id));
}

fn invalidate_response_affinity_inner(
    lane_key: &RequestLaneKey,
    response_id: &str,
    owner_turn_id: Option<u64>,
) {
    if response_id.is_empty() || response_id.len() > MAX_RESPONSE_ID_BYTES {
        return;
    }

    let mut guard = REGISTRY.lock().unwrap();
    let Some(registry) = guard.as_mut() else {
        return;
    };
    let Some(mut session) = registry.lanes.remove(lane_key) else {
        return;
    };
    subtract_session_retained_bytes(registry, &session);

    if owner_turn_id
        .is_some_and(|owner_turn_id| session.pending && session.current_turn == owner_turn_id)
    {
        // The socket disappeared before this turn could publish its response id. Reject a late
        // record, but restore an independently reachable exact fallback when one exists.
        let now = now_ms();
        if let Some(fallback) = session
            .pending_fallback
            .take()
            .filter(|fallback| now.saturating_sub(fallback.updated_at) <= TTL_MS)
        {
            session.current_turn = fallback.owner_turn_id;
            session.candidates = vec![fallback];
            session.pending_parallel_batches.clear();
            session.pending = false;
            session.updated_at = now;
            registry.total_retained_bytes = registry
                .total_retained_bytes
                .saturating_add(session_retained_bytes(&session));
            registry.lanes.insert(*lane_key, session);
            evict_oldest(registry);
        }
        return;
    }

    let affinity_matches = |state: &ContinuationState| {
        state.response_id == response_id
            && owner_turn_id.is_none_or(|owner_turn_id| state.owner_turn_id == owner_turn_id)
    };
    session.candidates.retain(|state| !affinity_matches(state));
    if session
        .pending_fallback
        .as_ref()
        .is_some_and(affinity_matches)
    {
        session.pending_fallback = None;
    }

    if session.pending || !session.candidates.is_empty() || session.pending_fallback.is_some() {
        registry.total_retained_bytes = registry
            .total_retained_bytes
            .saturating_add(session_retained_bytes(&session));
        registry.lanes.insert(*lane_key, session);
    }
}

pub fn if_current_turn<T>(
    lane_key: Option<&RequestLaneKey>,
    turn_id: Option<u64>,
    action: impl FnOnce() -> T,
) -> Option<T> {
    let (Some(lane_key), Some(turn_id)) = (lane_key, turn_id) else {
        return Some(action());
    };
    let guard = REGISTRY.lock().unwrap();
    let current = guard
        .as_ref()
        .and_then(|registry| registry.lanes.get(lane_key))
        .is_some_and(|session| session.current_turn == turn_id);
    current.then(action)
}

pub fn with_current_turn(
    lane_key: Option<&RequestLaneKey>,
    turn_id: Option<u64>,
    action: impl FnOnce(),
) -> bool {
    if_current_turn(lane_key, turn_id, action).is_some()
}

pub fn is_current_turn(lane_key: Option<&RequestLaneKey>, turn_id: Option<u64>) -> bool {
    let (Some(lane_key), Some(turn_id)) = (lane_key, turn_id) else {
        return false;
    };
    let guard = REGISTRY.lock().unwrap();
    guard
        .as_ref()
        .and_then(|registry| registry.lanes.get(lane_key))
        .is_some_and(|session| session.current_turn == turn_id)
}

pub fn clear_continuation(lane_key: Option<&RequestLaneKey>) {
    let Some(lane_key) = lane_key else {
        return;
    };
    let mut guard = REGISTRY.lock().unwrap();
    let Some(registry) = guard.as_mut() else {
        return;
    };
    if let Some(session) = registry.lanes.remove(lane_key) {
        subtract_session_retained_bytes(registry, &session);
    }
}

pub fn has_continuation_for_tests(lane_key: &RequestLaneKey) -> bool {
    let guard = REGISTRY.lock().unwrap();
    guard
        .as_ref()
        .and_then(|registry| registry.lanes.get(lane_key))
        .is_some_and(|session| !session.pending && !session.candidates.is_empty())
}

pub fn clear_all_continuations_for_tests() {
    let mut guard = REGISTRY.lock().unwrap();
    *guard = None;
}

struct ContractedHydrationTranscript {
    input: Vec<ResponsesInputItem>,
    request_input_len: usize,
    parallel_batches: Vec<ParallelBatchRange>,
}

/// Recognize only the Claude Code resume transition for which a fresh process
/// replaces historical ToolSearch results with its exact unavailable marker
/// and omits the translator-emitted AdditionalTools control item. This is an
/// optimization proof, not semantic equality: the mismatching turn still
/// sends full context, while the exact older response remains hidden.
fn strict_deferred_tool_hydration_drift(
    body: &ResponsesRequest,
    state: &ContinuationState,
) -> bool {
    let current = &body.deferred_tool_hydration;
    let stored = &state.deferred_tool_hydration;

    if hydration_sides_match(
        &state.transcript,
        state.request_input_len,
        stored,
        &body.input,
        body.input.len(),
        current,
    ) {
        let Some(contracted) = contract_loaded_hydration(
            &state.transcript,
            state.request_input_len,
            &state.parallel_batches,
            stored,
        ) else {
            return false;
        };
        return input_suffix_after_prefix(
            &body.input,
            &contracted.input,
            contracted.request_input_len,
            &contracted.parallel_batches,
            body.parallel_tool_calls,
        )
        .is_some();
    }

    if hydration_sides_match(
        &body.input,
        body.input.len(),
        current,
        &state.transcript,
        state.request_input_len,
        stored,
    ) {
        let Some(contracted) =
            contract_loaded_hydration(&body.input, body.input.len(), &[], current)
        else {
            return false;
        };
        return input_suffix_after_prefix(
            &contracted.input,
            &state.transcript,
            state.request_input_len,
            &state.parallel_batches,
            body.parallel_tool_calls,
        )
        .is_some();
    }

    false
}

fn hydration_sides_match(
    loaded_input: &[ResponsesInputItem],
    loaded_limit: usize,
    loaded: &DeferredToolHydrationProvenance,
    unavailable_input: &[ResponsesInputItem],
    unavailable_limit: usize,
    unavailable: &DeferredToolHydrationProvenance,
) -> bool {
    let Some(mut loaded_ids) = validated_loaded_tool_search_ids(loaded_input, loaded_limit, loaded)
    else {
        return false;
    };
    let Some(mut unavailable_ids) =
        validated_unavailable_tool_search_ids(unavailable_input, unavailable_limit, unavailable)
    else {
        return false;
    };
    loaded_ids.sort_unstable();
    unavailable_ids.sort_unstable();
    loaded_ids == unavailable_ids
}

fn validated_loaded_tool_search_ids<'a>(
    input: &'a [ResponsesInputItem],
    limit: usize,
    provenance: &DeferredToolHydrationProvenance,
) -> Option<Vec<&'a str>> {
    if provenance.ambiguous
        || provenance.loaded_groups.is_empty()
        || !provenance.unavailable_results.is_empty()
        || limit > input.len()
    {
        return None;
    }

    let mut ids = Vec::new();
    let mut indices = std::collections::HashSet::new();
    let mut seen_ids = std::collections::HashSet::new();
    for group in &provenance.loaded_groups {
        if group.results.is_empty()
            || group.additional_tools_index >= limit
            || !indices.insert(group.additional_tools_index)
            || !matches!(
                input.get(group.additional_tools_index),
                Some(ResponsesInputItem::AdditionalTools { id: None, role, tools })
                    if role == "developer" && !tools.is_empty()
            )
        {
            return None;
        }
        for result in &group.results {
            let call_id = validated_tool_search_result(input, limit, *result, false)?;
            if !indices.insert(result.tool_search_call_index)
                || !indices.insert(result.output_index)
                || !seen_ids.insert(call_id)
            {
                return None;
            }
            ids.push(call_id);
        }
    }
    Some(ids)
}

fn validated_unavailable_tool_search_ids<'a>(
    input: &'a [ResponsesInputItem],
    limit: usize,
    provenance: &DeferredToolHydrationProvenance,
) -> Option<Vec<&'a str>> {
    if provenance.ambiguous
        || !provenance.loaded_groups.is_empty()
        || provenance.unavailable_results.is_empty()
        || limit > input.len()
    {
        return None;
    }

    let mut ids = Vec::new();
    let mut indices = std::collections::HashSet::new();
    let mut seen_ids = std::collections::HashSet::new();
    for result in &provenance.unavailable_results {
        let call_id = validated_tool_search_result(input, limit, *result, true)?;
        if !indices.insert(result.tool_search_call_index)
            || !indices.insert(result.output_index)
            || !seen_ids.insert(call_id)
        {
            return None;
        }
        ids.push(call_id);
    }
    Some(ids)
}

fn validated_tool_search_result(
    input: &[ResponsesInputItem],
    limit: usize,
    result: DeferredToolHydrationResult,
    unavailable: bool,
) -> Option<&str> {
    if result.tool_search_call_index >= limit
        || result.output_index >= limit
        || result.tool_search_call_index >= result.output_index
    {
        return None;
    }
    let ResponsesInputItem::FunctionCall {
        call_id,
        name,
        arguments: _,
    } = input.get(result.tool_search_call_index)?
    else {
        return None;
    };
    let ResponsesInputItem::FunctionCallOutput {
        call_id: output_call_id,
        output,
    } = input.get(result.output_index)?
    else {
        return None;
    };
    if call_id.is_empty() || name != "ToolSearch" || output_call_id != call_id {
        return None;
    }
    let is_unavailable = matches!(
        output,
        ResponsesFunctionCallOutput::Text(text)
            if text == UNAVAILABLE_TOOL_REFERENCES_OUTPUT
    );
    (is_unavailable == unavailable).then_some(call_id.as_str())
}

fn contract_loaded_hydration(
    input: &[ResponsesInputItem],
    request_input_len: usize,
    parallel_batches: &[ParallelBatchRange],
    provenance: &DeferredToolHydrationProvenance,
) -> Option<ContractedHydrationTranscript> {
    validated_loaded_tool_search_ids(input, request_input_len, provenance)?;

    let mut remove = vec![false; input.len()];
    let mut replace = vec![false; input.len()];
    for group in &provenance.loaded_groups {
        remove[group.additional_tools_index] = true;
        for result in &group.results {
            replace[result.output_index] = true;
        }
    }
    let mut removed_before = Vec::with_capacity(input.len().saturating_add(1));
    removed_before.push(0_usize);
    for removed in &remove {
        let next = removed_before
            .last()
            .copied()?
            .checked_add(usize::from(*removed))?;
        removed_before.push(next);
    }

    let mut contracted = Vec::with_capacity(
        input
            .len()
            .saturating_sub(removed_before.get(input.len()).copied().unwrap_or_default()),
    );
    for (index, item) in input.iter().enumerate() {
        if remove[index] {
            continue;
        }
        if replace[index] {
            let ResponsesInputItem::FunctionCallOutput { call_id, .. } = item else {
                return None;
            };
            contracted.push(ResponsesInputItem::FunctionCallOutput {
                call_id: call_id.clone(),
                output: ResponsesFunctionCallOutput::Text(
                    UNAVAILABLE_TOOL_REFERENCES_OUTPUT.to_string(),
                ),
            });
        } else {
            contracted.push(item.clone());
        }
    }

    let request_input_len =
        request_input_len.checked_sub(*removed_before.get(request_input_len)?)?;
    let mut adjusted_batches = Vec::with_capacity(parallel_batches.len());
    for range in parallel_batches {
        let removed_inside = removed_before
            .get(range.end)?
            .checked_sub(*removed_before.get(range.start)?)?;
        if removed_inside != 0 {
            return None;
        }
        adjusted_batches.push(ParallelBatchRange {
            start: range.start.checked_sub(*removed_before.get(range.start)?)?,
            end: range.end.checked_sub(*removed_before.get(range.end)?)?,
        });
    }

    Some(ContractedHydrationTranscript {
        input: contracted,
        request_input_len,
        parallel_batches: adjusted_batches,
    })
}

fn input_suffix_after_prefix(
    input: &[ResponsesInputItem],
    prefix: &[ResponsesInputItem],
    request_input_len: usize,
    recorded_batches: &[ParallelBatchRange],
    allow_parallel_reorder: bool,
) -> Option<PrefixMatch> {
    if request_input_len > prefix.len() {
        return None;
    }
    if !parallel_batch_ranges_are_valid(recorded_batches, request_input_len)
        || (!allow_parallel_reorder && !recorded_batches.is_empty())
    {
        return None;
    }

    let mut call_group_start = prefix.len();
    while call_group_start > request_input_len
        && matches!(
            prefix.get(call_group_start - 1),
            Some(ResponsesInputItem::FunctionCall { .. })
        )
    {
        call_group_start -= 1;
    }

    // Historical input remains strictly ordered except for completed parallel batches whose
    // provenance was established while matching the immediately preceding upstream response.
    // Never infer reorderable regions by scanning arbitrary client-supplied history.
    if !prefix_segment_matches(
        input,
        prefix,
        call_group_start,
        recorded_batches,
        allow_parallel_reorder,
    ) {
        return None;
    }

    let mut tracked_call_ids = parallel_batch_call_ids(prefix, recorded_batches)?;
    let mut parallel_batches = recorded_batches.to_vec();

    if call_group_start == prefix.len() {
        if prefix.len() > input.len()
            || suffix_reuses_parallel_batch(&input[prefix.len()..], &tracked_call_ids)
        {
            return None;
        }
        return Some(PrefixMatch {
            delta: input[prefix.len()..].to_vec(),
            parallel_batches,
        });
    }

    let expected_calls = &prefix[call_group_start..];
    let mut call_positions = HashMap::with_capacity(expected_calls.len());
    for (index, call) in expected_calls.iter().enumerate() {
        let ResponsesInputItem::FunctionCall { call_id, .. } = call else {
            return None;
        };
        if call_id.is_empty() || call_positions.insert(call_id.as_str(), index).is_some() {
            return None;
        }
        if !tracked_call_ids.insert(call_id.as_str()) {
            return None;
        }
    }

    let mut seen_calls = std::collections::HashSet::with_capacity(expected_calls.len());
    let mut seen_outputs = std::collections::HashSet::with_capacity(expected_calls.len());
    let mut delta = Vec::new();
    let mut input_index = call_group_start;
    while seen_calls.len() < expected_calls.len() || seen_outputs.len() < expected_calls.len() {
        let item = input.get(input_index)?;
        match item {
            ResponsesInputItem::FunctionCall { call_id, .. } => {
                let call_position = *call_positions.get(call_id.as_str())?;
                if (!allow_parallel_reorder && call_position != seen_calls.len())
                    || !input_items_semantically_equal(item, expected_calls.get(call_position)?)
                    || !seen_calls.insert(call_id.as_str())
                {
                    return None;
                }
            }
            ResponsesInputItem::FunctionCallOutput { call_id, .. } => {
                if !call_positions.contains_key(call_id.as_str())
                    || !seen_calls.contains(call_id.as_str())
                    || !seen_outputs.insert(call_id.as_str())
                {
                    return None;
                }
                delta.push(item.clone());
            }
            _ => return None,
        }
        input_index += 1;
    }

    if allow_parallel_reorder && expected_calls.len() > 1 {
        if parallel_batches.len() >= MAX_PARALLEL_BATCHES_PER_SESSION
            || parallel_batches
                .last()
                .is_some_and(|range| range.end > call_group_start)
        {
            return None;
        }
        let range = ParallelBatchRange {
            start: call_group_start,
            end: input_index,
        };
        complete_parallel_batch(&input[range.start..range.end])?;
        parallel_batches.push(range);
    }

    // Preserve all later appended input verbatim, but do not allow a duplicate member of the
    // normalized call/result groups to hide in the suffix.
    if suffix_reuses_parallel_batch(&input[input_index..], &tracked_call_ids) {
        return None;
    }
    delta.extend_from_slice(&input[input_index..]);
    Some(PrefixMatch {
        delta,
        parallel_batches,
    })
}

fn parallel_batch_ranges_are_valid(ranges: &[ParallelBatchRange], limit: usize) -> bool {
    if ranges.len() > MAX_PARALLEL_BATCHES_PER_SESSION {
        return false;
    }
    let mut previous_end = 0;
    for range in ranges {
        if range.start < previous_end || range.start >= range.end || range.end > limit {
            return false;
        }
        previous_end = range.end;
    }
    true
}

fn prefix_segment_matches(
    input: &[ResponsesInputItem],
    prefix: &[ResponsesInputItem],
    end: usize,
    recorded_batches: &[ParallelBatchRange],
    allow_parallel_reorder: bool,
) -> bool {
    if end > prefix.len() {
        return false;
    }

    let mut cursor = 0;
    for range in recorded_batches {
        if range.end > end
            || !strict_input_range_matches(input, prefix, cursor, range.start)
            || !allow_parallel_reorder
            || !parallel_batches_semantically_equal(
                input.get(range.start..range.end),
                prefix.get(range.start..range.end),
            )
        {
            return false;
        }
        cursor = range.end;
    }
    strict_input_range_matches(input, prefix, cursor, end)
}

fn strict_input_range_matches(
    input: &[ResponsesInputItem],
    prefix: &[ResponsesInputItem],
    start: usize,
    end: usize,
) -> bool {
    if start > end || end > prefix.len() || end > input.len() {
        return false;
    }
    input[start..end]
        .iter()
        .zip(&prefix[start..end])
        .all(|(actual, expected)| input_items_semantically_equal(actual, expected))
}

fn parallel_batches_semantically_equal(
    input: Option<&[ResponsesInputItem]>,
    prefix: Option<&[ResponsesInputItem]>,
) -> bool {
    let (Some(input), Some(prefix)) = (input, prefix) else {
        return false;
    };
    if input.len() != prefix.len() {
        return false;
    }
    let Some((input_calls, input_outputs)) = complete_parallel_batch(input) else {
        return false;
    };
    let Some((prefix_calls, prefix_outputs)) = complete_parallel_batch(prefix) else {
        return false;
    };
    if input_calls.len() != prefix_calls.len() {
        return false;
    }

    prefix_calls.iter().all(|(call_id, expected_call)| {
        input_calls
            .get(call_id)
            .is_some_and(|actual_call| input_items_semantically_equal(actual_call, expected_call))
            && input_outputs.get(call_id).is_some_and(|actual_output| {
                prefix_outputs.get(call_id).is_some_and(|expected_output| {
                    input_items_semantically_equal(actual_output, expected_output)
                })
            })
    })
}

type ParallelBatchItems<'a> = (
    HashMap<&'a str, &'a ResponsesInputItem>,
    HashMap<&'a str, &'a ResponsesInputItem>,
);

fn complete_parallel_batch(items: &[ResponsesInputItem]) -> Option<ParallelBatchItems<'_>> {
    let mut calls = HashMap::new();
    let mut outputs = HashMap::new();
    for item in items {
        match item {
            ResponsesInputItem::FunctionCall { call_id, .. } => {
                if call_id.is_empty() || calls.insert(call_id.as_str(), item).is_some() {
                    return None;
                }
            }
            ResponsesInputItem::FunctionCallOutput { call_id, .. } => {
                if call_id.is_empty()
                    || !calls.contains_key(call_id.as_str())
                    || outputs.insert(call_id.as_str(), item).is_some()
                {
                    return None;
                }
            }
            _ => return None,
        }
    }
    (calls.len() >= 2 && calls.len() == outputs.len()).then_some((calls, outputs))
}

fn parallel_batch_call_ids<'a>(
    prefix: &'a [ResponsesInputItem],
    ranges: &[ParallelBatchRange],
) -> Option<std::collections::HashSet<&'a str>> {
    let mut call_ids = std::collections::HashSet::new();
    for range in ranges {
        let (calls, _) = complete_parallel_batch(prefix.get(range.start..range.end)?)?;
        for call_id in calls.into_keys() {
            if !call_ids.insert(call_id) {
                return None;
            }
        }
    }
    Some(call_ids)
}

fn suffix_reuses_parallel_batch(
    suffix: &[ResponsesInputItem],
    tracked_call_ids: &std::collections::HashSet<&str>,
) -> bool {
    suffix.iter().any(|item| {
        let call_id = match item {
            ResponsesInputItem::FunctionCall { call_id, .. }
            | ResponsesInputItem::FunctionCallOutput { call_id, .. } => Some(call_id.as_str()),
            _ => None,
        };
        call_id.is_some_and(|call_id| tracked_call_ids.contains(call_id))
    })
}

fn input_items_semantically_equal(left: &ResponsesInputItem, right: &ResponsesInputItem) -> bool {
    match (left, right) {
        (
            ResponsesInputItem::FunctionCall {
                call_id: left_call_id,
                name: left_name,
                arguments: left_arguments,
            },
            ResponsesInputItem::FunctionCall {
                call_id: right_call_id,
                name: right_name,
                arguments: right_arguments,
            },
        ) => {
            left_call_id == right_call_id
                && left_name == right_name
                && function_arguments_semantically_equal(left_arguments, right_arguments)
        }
        _ => left == right,
    }
}

fn function_arguments_semantically_equal(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let (Some(CanonicalJsonValue::Object(left)), Some(CanonicalJsonValue::Object(right))) = (
        parse_canonical_json_value(left),
        parse_canonical_json_value(right),
    ) else {
        return false;
    };
    left == right
}

#[derive(Debug, PartialEq, Eq)]
enum CanonicalJsonValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<CanonicalJsonValue>),
    Object(BTreeMap<String, CanonicalJsonValue>),
}

struct CanonicalJsonParser<'a> {
    input: &'a str,
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> CanonicalJsonParser<'a> {
    const MAX_DEPTH: usize = 128;

    fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            cursor: 0,
        }
    }

    fn parse(mut self) -> Option<CanonicalJsonValue> {
        self.skip_whitespace();
        let value = self.parse_value(0)?;
        self.skip_whitespace();
        (self.cursor == self.bytes.len()).then_some(value)
    }

    fn parse_value(&mut self, depth: usize) -> Option<CanonicalJsonValue> {
        if depth > Self::MAX_DEPTH {
            return None;
        }
        match self.bytes.get(self.cursor).copied()? {
            b'n' => {
                self.consume_literal(b"null")?;
                Some(CanonicalJsonValue::Null)
            }
            b't' => {
                self.consume_literal(b"true")?;
                Some(CanonicalJsonValue::Bool(true))
            }
            b'f' => {
                self.consume_literal(b"false")?;
                Some(CanonicalJsonValue::Bool(false))
            }
            b'"' => self.parse_string().map(CanonicalJsonValue::String),
            b'[' => self.parse_array(depth + 1),
            b'{' => self.parse_object(depth + 1),
            b'-' | b'0'..=b'9' => self.parse_number().map(CanonicalJsonValue::Number),
            _ => None,
        }
    }

    fn consume_literal(&mut self, literal: &[u8]) -> Option<()> {
        self.bytes
            .get(self.cursor..self.cursor.checked_add(literal.len())?)
            .filter(|candidate| *candidate == literal)?;
        self.cursor += literal.len();
        Some(())
    }

    fn parse_string(&mut self) -> Option<String> {
        let start = self.cursor;
        self.cursor += 1;
        while let Some(byte) = self.bytes.get(self.cursor).copied() {
            match byte {
                b'"' => {
                    self.cursor += 1;
                    return serde_json::from_str(&self.input[start..self.cursor]).ok();
                }
                b'\\' => {
                    self.cursor += 1;
                    match self.bytes.get(self.cursor).copied()? {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                            self.cursor += 1;
                        }
                        b'u' => {
                            let digits = self
                                .bytes
                                .get(self.cursor + 1..self.cursor.checked_add(5)?)?;
                            if !digits.iter().all(u8::is_ascii_hexdigit) {
                                return None;
                            }
                            self.cursor += 5;
                        }
                        _ => return None,
                    }
                }
                0x00..=0x1f => return None,
                _ => self.cursor += 1,
            }
        }
        None
    }

    fn parse_number(&mut self) -> Option<String> {
        let start = self.cursor;
        if self.bytes.get(self.cursor) == Some(&b'-') {
            self.cursor += 1;
        }
        match self.bytes.get(self.cursor).copied()? {
            b'0' => self.cursor += 1,
            b'1'..=b'9' => {
                self.cursor += 1;
                while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                    self.cursor += 1;
                }
            }
            _ => return None,
        }
        if self.bytes.get(self.cursor) == Some(&b'.') {
            self.cursor += 1;
            let fraction_start = self.cursor;
            while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                self.cursor += 1;
            }
            if self.cursor == fraction_start {
                return None;
            }
        }
        if matches!(self.bytes.get(self.cursor), Some(b'e' | b'E')) {
            self.cursor += 1;
            if matches!(self.bytes.get(self.cursor), Some(b'+' | b'-')) {
                self.cursor += 1;
            }
            let exponent_start = self.cursor;
            while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                self.cursor += 1;
            }
            if self.cursor == exponent_start {
                return None;
            }
        }
        Some(self.input[start..self.cursor].to_string())
    }

    fn parse_array(&mut self, depth: usize) -> Option<CanonicalJsonValue> {
        self.cursor += 1;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.bytes.get(self.cursor) == Some(&b']') {
            self.cursor += 1;
            return Some(CanonicalJsonValue::Array(values));
        }
        loop {
            values.push(self.parse_value(depth)?);
            self.skip_whitespace();
            match self.bytes.get(self.cursor).copied()? {
                b',' => {
                    self.cursor += 1;
                    self.skip_whitespace();
                }
                b']' => {
                    self.cursor += 1;
                    return Some(CanonicalJsonValue::Array(values));
                }
                _ => return None,
            }
        }
    }

    fn parse_object(&mut self, depth: usize) -> Option<CanonicalJsonValue> {
        self.cursor += 1;
        self.skip_whitespace();
        let mut values = BTreeMap::new();
        if self.bytes.get(self.cursor) == Some(&b'}') {
            self.cursor += 1;
            return Some(CanonicalJsonValue::Object(values));
        }
        loop {
            let key = self.parse_string()?;
            self.skip_whitespace();
            if self.bytes.get(self.cursor) != Some(&b':') {
                return None;
            }
            self.cursor += 1;
            self.skip_whitespace();
            let value = self.parse_value(depth)?;
            if values.insert(key, value).is_some() {
                return None;
            }
            self.skip_whitespace();
            match self.bytes.get(self.cursor).copied()? {
                b',' => {
                    self.cursor += 1;
                    self.skip_whitespace();
                }
                b'}' => {
                    self.cursor += 1;
                    return Some(CanonicalJsonValue::Object(values));
                }
                _ => return None,
            }
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(
            self.bytes.get(self.cursor),
            Some(b' ' | b'\n' | b'\r' | b'\t')
        ) {
            self.cursor += 1;
        }
    }
}

fn parse_canonical_json_value(input: &str) -> Option<CanonicalJsonValue> {
    CanonicalJsonParser::new(input).parse()
}

struct DigestWriter<'a>(&'a mut Sha256);

impl Write for DigestWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn prompt_field_digest<T: Serialize + ?Sized>(name: &str, value: &T) -> Option<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(b"ccproxy-codex-prompt-signature-field-v3\0");
    digest.update(name.as_bytes());
    digest.update([0]);
    serde_json::to_writer(&mut DigestWriter(&mut digest), value).ok()?;
    Some(digest.finalize().into())
}

fn prompt_signature(body: &ResponsesRequest) -> Option<PromptSignature> {
    // The input transcript is intentionally absent: continuation validates it independently as
    // an append-only prefix. Tools and tool_choice are also intentionally absent: Claude Code
    // expands and contracts its deferred tool set between otherwise compatible turns, and the
    // WebSocket request is built from the complete current body before only its input is replaced
    // with the delta. The current turn's tools and choice therefore still go upstream. The
    // remaining borrowed fields are hashed in a fixed tagged schema. serde_json::Value maps are
    // key-sorted in this build, and the only HashMap field is normalized explicitly below. Each
    // field is serialized once: the complete field digests form the safety gate, while truncated
    // in-memory fingerprints only identify which fixed fields changed for bounded diagnostics.
    // Neither digest nor field content is ever logged.
    let client_metadata = body.client_metadata.as_ref().map(|metadata| {
        metadata
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<BTreeMap<_, _>>()
    });
    let field_hashes = [
        prompt_field_digest(PROMPT_CHANGE_FIELDS[0], body.model.as_str())?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[1], &body.instructions)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[2], &body.store)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[3], &body.stream)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[4], &body.parallel_tool_calls)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[5], &body.include)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[6], &client_metadata)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[7], &body.service_tier)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[8], &body.prompt_cache_key)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[9], &body.text)?,
        prompt_field_digest(PROMPT_CHANGE_FIELDS[10], &body.reasoning)?,
    ];
    let diagnostic_fields = field_hashes.map(|hash| {
        u64::from_be_bytes(
            hash[..size_of::<u64>()]
                .try_into()
                .expect("SHA-256 digest contains a u64 prefix"),
        )
    });
    let mut safety_digest = Sha256::new();
    safety_digest.update(b"ccproxy-codex-prompt-signature-v3\0");
    for (name, hash) in PROMPT_CHANGE_FIELDS
        .iter()
        .take(PROMPT_SIGNATURE_FIELD_COUNT)
        .zip(field_hashes)
    {
        safety_digest.update(name.as_bytes());
        safety_digest.update([0]);
        safety_digest.update(hash);
    }
    Some(PromptSignature {
        safety_digest: safety_digest.finalize().into(),
        diagnostic_fields,
    })
}

fn evict_oldest(registry: &mut ContinuationRegistry) {
    while registry.lanes.len() > MAX_STATES
        || registry.total_retained_bytes > MAX_TOTAL_TRANSCRIPT_BYTES
    {
        let key = registry
            .lanes
            .iter()
            .min_by_key(|(_, session)| session.updated_at)
            .map(|(key, _)| *key);
        let Some(key) = key else {
            break;
        };
        if let Some(session) = registry.lanes.remove(&key) {
            subtract_session_retained_bytes(registry, &session);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_lane_key(label: &str) -> RequestLaneKey {
        let mut digest = Sha256::new();
        digest.update(b"ccproxy-continuation-test-lane\0");
        digest.update(label.as_bytes());
        RequestLaneKey::from_digest(digest.finalize().into())
    }

    fn continuation_candidate(
        lane: Option<&str>,
        body: &ResponsesRequest,
        enabled: bool,
    ) -> ContinuationCandidate {
        let lane = lane.map(test_lane_key);
        super::continuation_candidate(lane.as_ref(), body, enabled)
    }

    fn record_continuation(
        lane: Option<&str>,
        turn_id: Option<u64>,
        request_body: &ResponsesRequest,
        response_id: Option<&str>,
        output_items: &[ResponsesInputItem],
    ) {
        let lane = lane.map(test_lane_key);
        super::record_continuation(
            lane.as_ref(),
            turn_id,
            request_body,
            response_id,
            output_items,
        );
    }

    fn abort_continuation(lane: Option<&str>, turn_id: Option<u64>) {
        let lane = lane.map(test_lane_key);
        super::abort_continuation(lane.as_ref(), turn_id);
    }

    fn has_continuation_for_tests(lane: &str) -> bool {
        super::has_continuation_for_tests(&test_lane_key(lane))
    }

    fn ready_response_ids(lane: &str) -> Vec<String> {
        let guard = REGISTRY.lock().unwrap();
        guard
            .as_ref()
            .and_then(|registry| registry.lanes.get(&test_lane_key(lane)))
            .map(|session| {
                session
                    .candidates
                    .iter()
                    .map(|state| state.response_id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn pending_fallback_response_id(lane: &str) -> Option<String> {
        let guard = REGISTRY.lock().unwrap();
        guard
            .as_ref()
            .and_then(|registry| registry.lanes.get(&test_lane_key(lane)))
            .and_then(|session| session.pending_fallback.as_ref())
            .map(|state| state.response_id.clone())
    }

    fn total_retained_bytes() -> u64 {
        REGISTRY
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |registry| registry.total_retained_bytes)
    }

    fn request_with_input(
        input: Vec<ResponsesInputItem>,
        extra: Option<serde_json::Value>,
    ) -> ResponsesRequest {
        let mut fields = serde_json::Map::new();
        fields.insert("model".into(), json!("gpt-5.5"));
        fields.insert("input".into(), json!(input));
        fields.insert("store".into(), json!(false));
        fields.insert("stream".into(), json!(true));
        fields.insert("text".into(), json!({"verbosity": "low"}));
        fields.insert("parallel_tool_calls".into(), json!(true));
        if let Some(extras) = extra
            && let Some(obj) = extras.as_object()
        {
            for (k, v) in obj {
                fields.insert(k.clone(), v.clone());
            }
        }
        serde_json::from_value(serde_json::Value::Object(fields)).unwrap()
    }

    fn start_and_record(session_id: &str, request: &ResponsesRequest, response_id: Option<&str>) {
        let candidate = continuation_candidate(Some(session_id), request, true);
        record_continuation(
            Some(session_id),
            candidate.turn_id,
            request,
            response_id,
            &[],
        );
    }

    fn start_pending_not_append(session: &str) -> (ResponsesRequest, ContinuationCandidate) {
        let base = user_message("original history");
        let search_call = named_function_call("search_1", "ToolSearch", r#"{"query":"one"}"#);
        let loaded_output = function_output("search_1", "[tool reference: DeferredTool]");
        let additions = additional_tools(&["DeferredTool"]);
        let mut original_request = request_with_input(
            vec![base.clone(), search_call.clone(), loaded_output, additions],
            None,
        );
        mark_loaded_hydration(&mut original_request, 1, 2, 3);
        let original = continuation_candidate(Some(session), &original_request, true);
        let original_answer = assistant_message("original answer");
        record_continuation(
            Some(session),
            original.turn_id,
            &original_request,
            Some("resp_original"),
            std::slice::from_ref(&original_answer),
        );

        let mut rewritten_request = request_with_input(
            vec![
                base,
                search_call,
                function_output("search_1", UNAVAILABLE_TOOL_REFERENCES_OUTPUT),
                original_answer,
                user_message("resume"),
            ],
            None,
        );
        mark_unavailable_hydration(&mut rewritten_request, 1, 2);
        let candidate = continuation_candidate(Some(session), &rewritten_request, true);
        assert_eq!(candidate.previous_response_id, None);
        assert_eq!(
            candidate.disabled_reason.as_deref(),
            Some("not_append_only")
        );
        assert_eq!(
            pending_fallback_response_id(session).as_deref(),
            Some("resp_original")
        );
        assert!(candidate.response_affine_fork);
        (rewritten_request, candidate)
    }

    fn mark_loaded_hydration(
        request: &mut ResponsesRequest,
        call_index: usize,
        output_index: usize,
        additional_tools_index: usize,
    ) {
        request.deferred_tool_hydration = DeferredToolHydrationProvenance {
            loaded_groups: vec![
                super::super::translate::request::DeferredToolHydrationGroup {
                    results: vec![DeferredToolHydrationResult {
                        tool_search_call_index: call_index,
                        output_index,
                    }],
                    additional_tools_index,
                },
            ],
            unavailable_results: Vec::new(),
            ambiguous: false,
        };
    }

    fn mark_unavailable_hydration(
        request: &mut ResponsesRequest,
        call_index: usize,
        output_index: usize,
    ) {
        request.deferred_tool_hydration = DeferredToolHydrationProvenance {
            loaded_groups: Vec::new(),
            unavailable_results: vec![DeferredToolHydrationResult {
                tool_search_call_index: call_index,
                output_index,
            }],
            ambiguous: false,
        };
    }

    fn function_call(call_id: &str, arguments: &str) -> ResponsesInputItem {
        named_function_call(call_id, "Read", arguments)
    }

    fn named_function_call(call_id: &str, name: &str, arguments: &str) -> ResponsesInputItem {
        ResponsesInputItem::FunctionCall {
            call_id: call_id.to_string(),
            name: name.to_string(),
            arguments: arguments.to_string(),
        }
    }

    fn function_output(call_id: &str, output: &str) -> ResponsesInputItem {
        ResponsesInputItem::FunctionCallOutput {
            call_id: call_id.to_string(),
            output: super::super::translate::request::ResponsesFunctionCallOutput::Text(
                output.to_string(),
            ),
        }
    }

    fn additional_tools(names: &[&str]) -> ResponsesInputItem {
        ResponsesInputItem::AdditionalTools {
            id: None,
            role: "developer".to_string(),
            tools: names
                .iter()
                .map(|name| {
                    json!({
                        "type": "function",
                        "name": name,
                        "description": format!("deferred {name}"),
                        "parameters": {"type": "object", "properties": {}}
                    })
                })
                .collect(),
        }
    }

    fn user_message(text: &str) -> ResponsesInputItem {
        message("user", text)
    }

    fn assistant_message(text: &str) -> ResponsesInputItem {
        message("assistant", text)
    }

    fn message(role: &str, text: &str) -> ResponsesInputItem {
        ResponsesInputItem::Message {
            role: role.to_string(),
            content: vec![
                super::super::translate::request::ResponsesContentPart::InputText {
                    text: text.to_string(),
                },
            ],
        }
    }

    #[test]
    fn function_argument_object_key_order_is_semantically_equal() {
        for (recorded, rebuilt) in [
            (
                r#"{"path":"src","pattern":"TODO","output_mode":"content"}"#,
                r#"{"output_mode":"content","path":"src","pattern":"TODO"}"#,
            ),
            (
                r#"{"file_path":"src/lib.rs","offset":10,"limit":20}"#,
                r#"{"file_path":"src/lib.rs","limit":20,"offset":10}"#,
            ),
            (
                r#" { "file_path" : "src/lib.rs", "offset" : 10 } "#,
                r#"{"offset":10,"file_path":"src/lib.rs"}"#,
            ),
            (
                r#"{"outer":{"z":1,"a":2},"items":[{"b":1,"a":2}]}"#,
                r#"{"items":[{"a":2,"b":1}],"outer":{"a":2,"z":1}}"#,
            ),
        ] {
            assert!(input_items_semantically_equal(
                &function_call("call_1", recorded),
                &function_call("call_1", rebuilt),
            ));
        }
    }

    #[test]
    fn function_argument_numbers_preserve_exact_json_lexemes() {
        let unequal_numbers = [
            ("18446744073709551616", "18446744073709551617"),
            (
                "0.123456789012345678901234567890",
                "0.123456789012345678901234567891",
            ),
            ("1e100000", "10e99999"),
            ("1", "1.0"),
            ("1e0", "1E0"),
            ("-0", "0"),
        ];
        for (recorded_number, rebuilt_number) in unequal_numbers {
            let recorded = format!(r#"{{"number":{recorded_number},"stable":true}}"#);
            let rebuilt = format!(r#"{{"stable":true,"number":{rebuilt_number}}}"#);
            assert!(
                !function_arguments_semantically_equal(&recorded, &rebuilt),
                "distinct number lexemes collided: {recorded_number} and {rebuilt_number}"
            );
        }

        assert!(function_arguments_semantically_equal(
            r#"{"number":18446744073709551617,"stable":true}"#,
            r#"{"stable":true,"number":18446744073709551617}"#,
        ));
        assert!(function_arguments_semantically_equal(
            r#"{"number":0.123456789012345678901234567891,"stable":true}"#,
            r#"{"stable":true,"number":0.123456789012345678901234567891}"#,
        ));
    }

    #[test]
    fn function_argument_canonicalizer_rejects_duplicates_and_invalid_numbers() {
        for invalid in [
            r#"{"a":1,"\u0061":1}"#,
            r#"{"nested":{"x":1,"x":1}}"#,
            r#"{"number":01}"#,
            r#"{"number":1.}"#,
            r#"{"number":1e}"#,
            r#"{"number":+1}"#,
        ] {
            assert!(
                !function_arguments_semantically_equal(invalid, r#"{"a":1}"#),
                "invalid JSON was accepted by the canonicalizer: {invalid}"
            );
        }
    }

    #[test]
    fn prompt_signature_is_fixed_size_deterministic_and_excludes_input() {
        let first = request_with_input(
            vec![user_message("first")],
            Some(json!({
                "tools": [{
                    "type": "function",
                    "name": "Read",
                    "parameters": {"type": "object"}
                }],
                "tool_choice": "auto"
            })),
        );
        let mut second = request_with_input(
            vec![user_message(&"large-history".repeat(10_000))],
            Some(json!({
                "tools": [
                    {
                        "type": "function",
                        "name": "Read",
                        "parameters": {"type": "object"}
                    },
                    {
                        "type": "function",
                        "name": "Grep",
                        "description": "A newly loaded deferred tool",
                        "parameters": {"type": "object"}
                    }
                ],
                "tool_choice": "required"
            })),
        );
        first
            .client_metadata
            .as_ref()
            .inspect(|_| panic!("fixture unexpectedly contains client metadata"));

        let mut first_metadata = HashMap::new();
        first_metadata.insert("z".to_string(), "last".to_string());
        first_metadata.insert("a".to_string(), "first".to_string());
        let mut second_metadata = HashMap::new();
        second_metadata.insert("a".to_string(), "first".to_string());
        second_metadata.insert("z".to_string(), "last".to_string());

        let mut first = first;
        first.client_metadata = Some(first_metadata);
        second.client_metadata = Some(second_metadata);
        let first_signature = prompt_signature(&first).unwrap();
        let second_signature = prompt_signature(&second).unwrap();

        assert_eq!(first_signature.safety_digest.len(), 32);
        assert_eq!(first_signature, second_signature);

        second.parallel_tool_calls = false;
        assert_ne!(first_signature, prompt_signature(&second).unwrap());

        second.parallel_tool_calls = true;
        second.instructions = Some("changed instructions".to_string());
        assert_ne!(first_signature, prompt_signature(&second).unwrap());
    }

    #[test]
    fn prompt_signature_diagnostics_report_only_fixed_changed_fields() {
        fn changed_fields(
            base: &ResponsesRequest,
            mutate: impl FnOnce(&mut ResponsesRequest),
        ) -> Vec<&'static str> {
            let previous = prompt_signature(base).unwrap();
            let mut current = base.clone();
            mutate(&mut current);
            let current = prompt_signature(&current).unwrap();
            assert_ne!(previous.safety_digest, current.safety_digest);
            PromptChangeMask::between(&previous, &current).names()
        }

        let base = request_with_input(vec![user_message("history")], None);
        assert_eq!(
            changed_fields(&base, |request| request.model = "gpt-5.6-sol".into()),
            ["model"]
        );
        assert_eq!(
            changed_fields(&base, |request| request.instructions = Some("new".into())),
            ["instructions"]
        );
        assert_eq!(
            changed_fields(&base, |request| request.store = !request.store),
            ["store"]
        );
        assert_eq!(
            changed_fields(&base, |request| request.stream = !request.stream),
            ["stream"]
        );
        assert_eq!(
            changed_fields(&base, |request| {
                request.parallel_tool_calls = !request.parallel_tool_calls
            }),
            ["parallel_tool_calls"]
        );
        assert_eq!(
            changed_fields(&base, |request| request.include =
                Some(vec!["usage".into()])),
            ["include"]
        );
        assert_eq!(
            changed_fields(&base, |request| {
                request.client_metadata = Some(HashMap::from([("kind".into(), "resume".into())]))
            }),
            ["client_metadata"]
        );
        assert_eq!(
            changed_fields(&base, |request| {
                request.service_tier = Some(super::super::translate::request::ServiceTier::Priority)
            }),
            ["service_tier"]
        );
        assert_eq!(
            changed_fields(&base, |request| {
                request.prompt_cache_key = Some("cache-lane".into())
            }),
            ["prompt_cache_key"]
        );
        assert_eq!(
            changed_fields(&base, |request| request.text.verbosity =
                Some("high".into())),
            ["text_format"]
        );
        assert_eq!(
            changed_fields(&base, |request| {
                request.reasoning = Some(super::super::translate::request::ResponsesReasoning {
                    effort: None,
                    summary: Some("auto".into()),
                    context: None,
                })
            }),
            ["reasoning"]
        );
        assert_eq!(
            changed_fields(&base, |request| {
                request.instructions = Some("new".into());
                request.text.verbosity = Some("high".into());
                request.reasoning = Some(super::super::translate::request::ResponsesReasoning {
                    effort: None,
                    summary: Some("auto".into()),
                    context: None,
                });
            }),
            ["instructions", "text_format", "reasoning"]
        );

        let previous = prompt_signature(&base).unwrap();
        let mut unattributed = previous.clone();
        unattributed.safety_digest[0] ^= 1;
        assert_eq!(
            PromptChangeMask::between(&previous, &unattributed).names(),
            ["unattributed"]
        );
    }

    #[test]
    fn continuation_survives_dynamic_tool_expansion_choice_change_and_resume_contraction() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let compact_tools = json!([
            {"type": "function", "name": "Read", "parameters": {"type": "object"}},
            {"type": "function", "name": "Grep", "parameters": {"type": "object"}},
            {"type": "function", "name": "Glob", "parameters": {"type": "object"}}
        ]);
        let expanded_tools = serde_json::Value::Array(
            (0..15)
                .map(|index| {
                    json!({
                        "type": "function",
                        "name": format!("DeferredTool{index}"),
                        "description": format!("Dynamically loaded tool {index}"),
                        "parameters": {"type": "object", "properties": {}}
                    })
                })
                .collect(),
        );

        let base = user_message("solve a repository problem");
        let first_text = assistant_message("first result");
        let first_request = request_with_input(
            vec![base.clone()],
            Some(json!({"tools": compact_tools, "tool_choice": "auto"})),
        );
        let first = continuation_candidate(Some("dynamic-tools"), &first_request, true);
        record_continuation(
            Some("dynamic-tools"),
            first.turn_id,
            &first_request,
            Some("resp_compact"),
            std::slice::from_ref(&first_text),
        );

        let second_user = user_message("continue after loading deferred tools");
        let second_request = request_with_input(
            vec![base.clone(), first_text.clone(), second_user.clone()],
            Some(json!({"tools": expanded_tools, "tool_choice": "required"})),
        );
        let expanded = continuation_candidate(Some("dynamic-tools"), &second_request, true);
        assert_eq!(
            expanded.previous_response_id.as_deref(),
            Some("resp_compact")
        );
        assert_eq!(expanded.disabled_reason, None);
        assert_eq!(
            serde_json::to_value(&expanded.input_delta).unwrap(),
            json!([second_user])
        );

        let second_text = assistant_message("second result");
        record_continuation(
            Some("dynamic-tools"),
            expanded.turn_id,
            &second_request,
            Some("resp_expanded"),
            std::slice::from_ref(&second_text),
        );

        let resumed_user = user_message("continue after a fresh CLI process");
        let resumed_request = request_with_input(
            vec![
                base,
                first_text,
                user_message("continue after loading deferred tools"),
                second_text,
                resumed_user.clone(),
            ],
            Some(json!({"tools": compact_tools, "tool_choice": "none"})),
        );
        let contracted = continuation_candidate(Some("dynamic-tools"), &resumed_request, true);
        assert_eq!(
            contracted.previous_response_id.as_deref(),
            Some("resp_expanded")
        );
        assert_eq!(contracted.disabled_reason, None);
        assert_eq!(
            serde_json::to_value(contracted.input_delta).unwrap(),
            json!([resumed_user])
        );
    }

    #[test]
    fn transcript_counter_matches_serialization_and_enforces_limit_without_a_string() {
        let input = vec![user_message("hello"), function_call("call_1", r#"{"x":1}"#)];
        let output = vec![function_output("call_1", "world")];
        let mut combined = input.clone();
        combined.extend_from_slice(&output);
        let expected = serde_json::to_vec(&combined).unwrap().len() as u64;

        assert_eq!(
            serialized_transcript_bytes(&input, &output, expected),
            Some(expected)
        );
        assert_eq!(
            serialized_transcript_bytes(&input, &output, expected - 1),
            None
        );
    }

    #[test]
    fn function_argument_value_changes_and_non_objects_remain_strict() {
        for (recorded, rebuilt) in [
            (
                r#"{"path":"src","limit":20}"#,
                r#"{"limit":21,"path":"src"}"#,
            ),
            (r#"{"path":"src"}"#, r#"{"path":"other"}"#),
            (r#"{"path":"src""#, r#"{"path":"src"}"#),
            (r#"{"path":"src""#, r#"{"path":"src" "#),
            (r#"[1,2]"#, r#"[2,1]"#),
            (r#"[1,2]"#, r#"[ 1, 2 ]"#),
            ("null", " null "),
            (r#"{"path":"old","path":"src"}"#, r#"{"path":"src"}"#),
        ] {
            assert!(
                !input_items_semantically_equal(
                    &function_call("call_1", recorded),
                    &function_call("call_1", rebuilt),
                ),
                "unsafe argument rewrite was accepted: {recorded} -> {rebuilt}"
            );
        }

        assert!(!input_items_semantically_equal(
            &function_call("call_1", r#"{"path":"src"}"#),
            &function_call("call_2", r#"{"path":"src"}"#),
        ));
        assert!(!input_items_semantically_equal(
            &named_function_call("call_1", "Read", r#"{"path":"src"}"#),
            &named_function_call("call_1", "Grep", r#"{"path":"src"}"#),
        ));
    }

    #[test]
    fn non_function_input_items_compare_directly_without_json_materialization() {
        let message = user_message("same");
        assert!(input_items_semantically_equal(&message, &message.clone()));
        assert!(!input_items_semantically_equal(
            &message,
            &user_message("changed")
        ));

        let output = function_output("call_1", "same");
        assert!(input_items_semantically_equal(&output, &output.clone()));
        assert!(!input_items_semantically_equal(
            &output,
            &function_output("call_1", "changed")
        ));

        let reasoning = ResponsesInputItem::Reasoning {
            id: "reasoning_1".to_string(),
            summary: vec![json!({"type":"summary_text","text":"same"})],
            encrypted_content: "opaque".to_string(),
        };
        assert!(input_items_semantically_equal(
            &reasoning,
            &reasoning.clone()
        ));
    }

    #[test]
    fn continuation_accepts_early_outputs_between_ordered_parallel_calls() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let base = user_message("inspect the repository");
        let request = request_with_input(vec![base.clone()], None);
        let turn = continuation_candidate(Some("interleaved-session"), &request, true);
        let recorded_a = named_function_call(
            "call_a",
            "Grep",
            r#"{"path":"src","pattern":"TODO","output_mode":"content"}"#,
        );
        let recorded_b = function_call(
            "call_b",
            r#"{"file_path":"src/lib.rs","offset":10,"limit":20}"#,
        );
        let recorded_c = function_call("call_c", r#"{"file_path":"src/main.rs"}"#);
        record_continuation(
            Some("interleaved-session"),
            turn.turn_id,
            &request,
            Some("resp_parallel"),
            &[recorded_a, recorded_b, recorded_c.clone()],
        );

        let rebuilt_a = named_function_call(
            "call_a",
            "Grep",
            r#"{"output_mode":"content","path":"src","pattern":"TODO"}"#,
        );
        let rebuilt_b = function_call(
            "call_b",
            r#"{"file_path":"src/lib.rs","limit":20,"offset":10}"#,
        );
        let output_a = function_output("call_a", "grep result");
        let output_b = function_output("call_b", "lib result");
        let output_c = function_output("call_c", "main result");
        let appended = user_message("continue");
        let next = request_with_input(
            vec![
                base,
                rebuilt_a,
                rebuilt_b,
                output_a.clone(),
                output_b.clone(),
                recorded_c,
                output_c.clone(),
                appended.clone(),
            ],
            None,
        );

        let candidate = continuation_candidate(Some("interleaved-session"), &next, true);
        assert_eq!(
            candidate.previous_response_id.as_deref(),
            Some("resp_parallel")
        );
        assert_eq!(candidate.disabled_reason, None);
        assert_eq!(candidate.input_delta_count, 4);
        assert_eq!(
            serde_json::to_value(candidate.input_delta).unwrap(),
            json!([output_a, output_b, output_c, appended])
        );
    }

    #[test]
    fn independent_lanes_with_the_same_prompt_signature_never_supersede_each_other() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let main_lane = test_lane_key("shared-session-main");
        let agent_lane = test_lane_key("shared-session-agent-a");
        let main_base = user_message("main branch");
        let agent_base = user_message("agent branch");
        let main_first_request = request_with_input(vec![main_base.clone()], None);
        let agent_first_request = request_with_input(vec![agent_base.clone()], None);

        let main_first = super::continuation_candidate(Some(&main_lane), &main_first_request, true);
        let agent_first =
            super::continuation_candidate(Some(&agent_lane), &agent_first_request, true);
        super::record_continuation(
            Some(&main_lane),
            main_first.turn_id,
            &main_first_request,
            Some("resp_main_1"),
            &[],
        );
        super::record_continuation(
            Some(&agent_lane),
            agent_first.turn_id,
            &agent_first_request,
            Some("resp_agent_1"),
            &[],
        );

        let main_second_request =
            request_with_input(vec![main_base.clone(), user_message("main next")], None);
        let agent_second_request =
            request_with_input(vec![agent_base.clone(), user_message("agent next")], None);
        let main_second =
            super::continuation_candidate(Some(&main_lane), &main_second_request, true);
        let agent_second =
            super::continuation_candidate(Some(&agent_lane), &agent_second_request, true);
        assert_eq!(
            main_second.previous_response_id.as_deref(),
            Some("resp_main_1")
        );
        assert_eq!(
            agent_second.previous_response_id.as_deref(),
            Some("resp_agent_1")
        );
        assert_eq!(main_second.input_delta_count, 1);
        assert_eq!(agent_second.input_delta_count, 1);

        super::record_continuation(
            Some(&main_lane),
            main_second.turn_id,
            &main_second_request,
            Some("resp_main_2"),
            &[],
        );
        super::record_continuation(
            Some(&agent_lane),
            agent_second.turn_id,
            &agent_second_request,
            Some("resp_agent_2"),
            &[],
        );
        assert_eq!(REGISTRY.lock().unwrap().as_ref().unwrap().lanes.len(), 2);

        // Cancelling a new child-agent turn removes only that child lane. The parent retains its
        // own previous response and still produces a one-item delta.
        let agent_cancel = super::continuation_candidate(
            Some(&agent_lane),
            &request_with_input(
                vec![
                    agent_base,
                    user_message("agent next"),
                    user_message("cancel me"),
                ],
                None,
            ),
            true,
        );
        super::abort_continuation(Some(&agent_lane), agent_cancel.turn_id);

        let main_third = super::continuation_candidate(
            Some(&main_lane),
            &request_with_input(
                vec![
                    main_base,
                    user_message("main next"),
                    user_message("parent resumes"),
                ],
                None,
            ),
            true,
        );
        assert_eq!(
            main_third.previous_response_id.as_deref(),
            Some("resp_main_2")
        );
        assert_eq!(main_third.input_delta_count, 1);
        assert!(!super::has_continuation_for_tests(&agent_lane));
    }

    #[test]
    fn parallel_resume_accepts_live_call_reordering_only_when_enabled() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let base = user_message("inspect the repository");
        let recorded_grep = named_function_call(
            "fvY",
            "Grep",
            r#"{"path":"src","pattern":"TODO","output_mode":"content"}"#,
        );
        let recorded_read_dcy = function_call(
            "DCY",
            r#"{"file_path":"src/lib.rs","offset":10,"limit":20}"#,
        );
        let recorded_read_lmuk = function_call(
            "lMUK",
            r#"{"file_path":"src/main.rs","offset":30,"limit":40}"#,
        );
        let request = request_with_input(vec![base.clone()], None);
        let turn = continuation_candidate(Some("reordered-live-session"), &request, true);
        record_continuation(
            Some("reordered-live-session"),
            turn.turn_id,
            &request,
            Some("resp_reordered_live"),
            &[
                recorded_grep.clone(),
                recorded_read_dcy.clone(),
                recorded_read_lmuk.clone(),
            ],
        );
        let prefix = vec![
            base.clone(),
            recorded_grep,
            recorded_read_dcy.clone(),
            recorded_read_lmuk.clone(),
        ];

        let output_grep = function_output("fvY", "grep result");
        let output_dcy = function_output("DCY", "lib result");
        let output_lmuk = function_output("lMUK", "main result");
        let appended = user_message("continue");
        let input = vec![
            base,
            named_function_call(
                "fvY",
                "Grep",
                r#"{"output_mode":"content","pattern":"TODO","path":"src"}"#,
            ),
            function_call(
                "lMUK",
                r#"{"limit":40,"file_path":"src/main.rs","offset":30}"#,
            ),
            function_call(
                "DCY",
                r#"{"offset":10,"limit":20,"file_path":"src/lib.rs"}"#,
            ),
            output_grep.clone(),
            output_dcy.clone(),
            output_lmuk.clone(),
            appended.clone(),
        ];

        let next = request_with_input(input.clone(), None);
        let candidate = continuation_candidate(Some("reordered-live-session"), &next, true);
        assert_eq!(
            candidate.previous_response_id.as_deref(),
            Some("resp_reordered_live")
        );
        assert_eq!(candidate.disabled_reason, None);
        assert_eq!(
            serde_json::to_value(candidate.input_delta).unwrap(),
            json!([output_grep, output_dcy, output_lmuk, appended])
        );
        assert!(input_suffix_after_prefix(&input, &prefix, 1, &[], false).is_none());
    }

    #[test]
    fn completed_parallel_batch_provenance_survives_final_text_and_resume_rebuild() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "cross-process-resume";
        let base = user_message("inspect the repository");
        let call_a = named_function_call(
            "fvY",
            "Grep",
            r#"{"path":"src","pattern":"TODO","output_mode":"content"}"#,
        );
        let call_b = function_call(
            "DCY",
            r#"{"file_path":"src/lib.rs","offset":10,"limit":20}"#,
        );
        let call_c = function_call(
            "lMUK",
            r#"{"file_path":"src/main.rs","offset":30,"limit":40}"#,
        );
        let output_a = function_output("fvY", "grep result");
        let output_b = function_output("DCY", "lib result");
        let output_c = function_output("lMUK", "main result");

        let first_request = request_with_input(vec![base.clone()], None);
        let first = continuation_candidate(Some(session), &first_request, true);
        record_continuation(
            Some(session),
            first.turn_id,
            &first_request,
            Some("resp_calls"),
            &[call_a.clone(), call_b.clone(), call_c.clone()],
        );

        // The same live process observes the authoritative call order, with results completing in
        // a different order. Matching this turn establishes provenance for the completed batch.
        let second_request = request_with_input(
            vec![
                base.clone(),
                call_a.clone(),
                call_b.clone(),
                call_c.clone(),
                output_b.clone(),
                output_a.clone(),
                output_c.clone(),
            ],
            None,
        );
        let second = continuation_candidate(Some(session), &second_request, true);
        assert_eq!(second.previous_response_id.as_deref(), Some("resp_calls"));
        let final_text = assistant_message("audit complete");
        record_continuation(
            Some(session),
            second.turn_id,
            &second_request,
            Some("resp_final"),
            std::slice::from_ref(&final_text),
        );

        // A fresh Claude process rebuilds the persisted parallel batch in a different call/result
        // order. The exact graph remains equivalent, so only the newly appended user item is sent.
        let appended = user_message("continue after resume");
        let rebuilt_request = request_with_input(
            vec![
                base,
                call_a,
                call_c,
                call_b,
                output_a.clone(),
                output_b.clone(),
                output_c,
                final_text,
                appended.clone(),
            ],
            None,
        );
        let resumed = continuation_candidate(Some(session), &rebuilt_request, true);
        assert_eq!(resumed.previous_response_id.as_deref(), Some("resp_final"));
        assert_eq!(resumed.disabled_reason, None);
        assert_eq!(
            serde_json::to_value(resumed.input_delta).unwrap(),
            json!([appended])
        );

        let recorded_batch = vec![
            function_call("a", r#"{"path":"a"}"#),
            function_call("b", r#"{"path":"b"}"#),
            function_output("a", "same"),
            function_output("b", "same"),
        ];
        let changed_result = vec![
            function_call("b", r#"{"path":"b"}"#),
            function_call("a", r#"{"path":"a"}"#),
            function_output("a", "changed"),
            function_output("b", "same"),
        ];
        assert!(!parallel_batches_semantically_equal(
            Some(&changed_result),
            Some(&recorded_batch),
        ));
    }

    #[test]
    fn deferred_tools_after_parallel_results_preserve_delta_and_resume_reordering() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "parallel-deferred-tools";
        let base = user_message("load several deferred tools");
        let call_a = named_function_call("search_a", "ToolSearch", r#"{"query":"one"}"#);
        let call_b = named_function_call("search_b", "ToolSearch", r#"{"query":"two"}"#);
        let call_c = named_function_call("search_c", "ToolSearch", r#"{"query":"three"}"#);
        let output_a = function_output("search_a", "tool_reference: DeferredOne");
        let output_b = function_output("search_b", "tool_reference: DeferredTwo");
        let output_c = function_output("search_c", "no match");
        let additions = additional_tools(&["DeferredOne", "DeferredTwo"]);

        let first_request = request_with_input(vec![base.clone()], None);
        let first = continuation_candidate(Some(session), &first_request, true);
        record_continuation(
            Some(session),
            first.turn_id,
            &first_request,
            Some("resp_search_calls"),
            &[call_a.clone(), call_b.clone(), call_c.clone()],
        );

        // Translation normalizes all results before the single AdditionalTools control item.
        // The matcher must retain that control item in the real upstream delta while recording
        // only the complete call/result graph as reorderable provenance.
        let second_request = request_with_input(
            vec![
                base.clone(),
                call_a.clone(),
                call_b.clone(),
                call_c.clone(),
                output_b.clone(),
                output_a.clone(),
                output_c.clone(),
                additions.clone(),
            ],
            None,
        );
        let second = continuation_candidate(Some(session), &second_request, true);
        assert_eq!(
            second.previous_response_id.as_deref(),
            Some("resp_search_calls")
        );
        assert_eq!(second.disabled_reason, None);
        assert_eq!(
            second.input_delta,
            Some(vec![
                output_b.clone(),
                output_a.clone(),
                output_c.clone(),
                additions.clone(),
            ])
        );

        let final_text = assistant_message("tools loaded");
        record_continuation(
            Some(session),
            second.turn_id,
            &second_request,
            Some("resp_tools_loaded"),
            std::slice::from_ref(&final_text),
        );

        // A resumed Claude process may rebuild the completed parallel batch in a different
        // call/result order. The exact AdditionalTools contract remains strictly ordered and
        // equal, so only the newly appended user message is sent upstream.
        let appended = user_message("continue after loading");
        let resumed_request = request_with_input(
            vec![
                base,
                call_c,
                call_a,
                call_b,
                output_c,
                output_a,
                output_b,
                additions,
                final_text,
                appended.clone(),
            ],
            None,
        );
        let resumed = continuation_candidate(Some(session), &resumed_request, true);
        assert_eq!(
            resumed.previous_response_id.as_deref(),
            Some("resp_tools_loaded")
        );
        assert_eq!(resumed.disabled_reason, None);
        assert_eq!(resumed.input_delta, Some(vec![appended]));
    }

    #[test]
    fn unavailable_tool_resume_recovers_loaded_exact_branch_after_delayed_hydration() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "delayed-tool-hydration";
        let base = user_message("load the repository search tool");
        let search_call =
            named_function_call("search_1", "ToolSearch", r#"{"query":"select:Grep"}"#);
        let loaded_output = function_output("search_1", "tool_reference: Grep");
        let unavailable_output = function_output(
            "search_1",
            "[Tool references removed - tools no longer available]",
        );
        let additions = additional_tools(&["Grep"]);
        let loaded_final = assistant_message("Grep is loaded");

        let first_request = request_with_input(vec![base.clone()], None);
        let first = continuation_candidate(Some(session), &first_request, true);
        record_continuation(
            Some(session),
            first.turn_id,
            &first_request,
            Some("resp_search"),
            std::slice::from_ref(&search_call),
        );

        let mut loaded_request = request_with_input(
            vec![
                base.clone(),
                search_call.clone(),
                loaded_output.clone(),
                additions.clone(),
            ],
            None,
        );
        mark_loaded_hydration(&mut loaded_request, 1, 2, 3);
        let loaded = continuation_candidate(Some(session), &loaded_request, true);
        assert_eq!(loaded.previous_response_id.as_deref(), Some("resp_search"));
        record_continuation(
            Some(session),
            loaded.turn_id,
            &loaded_request,
            Some("resp_loaded"),
            std::slice::from_ref(&loaded_final),
        );

        // A fresh CLI initially replaces the unavailable historical tool reference and omits the
        // AdditionalTools control item. Equality stays strict, so this first resume request must
        // use full context while retaining the exact loaded branch out of band.
        let resume_user_one = user_message("inspect the continuation implementation");
        let mut unavailable_request_one = request_with_input(
            vec![
                base.clone(),
                search_call.clone(),
                unavailable_output.clone(),
                loaded_final.clone(),
                resume_user_one.clone(),
            ],
            None,
        );
        mark_unavailable_hydration(&mut unavailable_request_one, 1, 2);
        let unavailable_one = continuation_candidate(Some(session), &unavailable_request_one, true);
        assert_eq!(unavailable_one.previous_response_id, None);
        assert_eq!(unavailable_one.matched_candidate_rank, None);
        assert_eq!(unavailable_one.candidate_count, 1);
        assert_eq!(
            unavailable_one.disabled_reason.as_deref(),
            Some("not_append_only")
        );
        assert_eq!(
            pending_fallback_response_id(session).as_deref(),
            Some("resp_loaded")
        );
        assert!(unavailable_one.response_affine_fork);

        let unavailable_answer_one = assistant_message("first resumed answer");
        record_continuation(
            Some(session),
            unavailable_one.turn_id,
            &unavailable_request_one,
            Some("resp_unavailable_one"),
            std::slice::from_ref(&unavailable_answer_one),
        );
        assert_eq!(
            ready_response_ids(session),
            ["resp_unavailable_one", "resp_loaded"]
        );

        // Tool hydration can lag for more than one request. A strict hit on the newest unavailable
        // branch must carry the older loaded branch through the in-flight turn.
        let resume_user_two = user_message("now inspect its registry accounting");
        let mut unavailable_request_two = request_with_input(
            vec![
                base.clone(),
                search_call.clone(),
                unavailable_output,
                loaded_final.clone(),
                resume_user_one.clone(),
                unavailable_answer_one.clone(),
                resume_user_two.clone(),
            ],
            None,
        );
        mark_unavailable_hydration(&mut unavailable_request_two, 1, 2);
        let unavailable_two = continuation_candidate(Some(session), &unavailable_request_two, true);
        assert_eq!(
            unavailable_two.previous_response_id.as_deref(),
            Some("resp_unavailable_one")
        );
        assert_eq!(unavailable_two.matched_candidate_rank, Some(0));
        assert_eq!(unavailable_two.candidate_count, 2);
        assert_eq!(
            unavailable_two.input_delta,
            Some(vec![resume_user_two.clone()])
        );
        assert_eq!(
            pending_fallback_response_id(session).as_deref(),
            Some("resp_loaded")
        );
        assert!(unavailable_two.response_affine_fork);

        let unavailable_answer_two = assistant_message("second resumed answer");
        record_continuation(
            Some(session),
            unavailable_two.turn_id,
            &unavailable_request_two,
            Some("resp_unavailable_two"),
            std::slice::from_ref(&unavailable_answer_two),
        );
        assert_eq!(
            ready_response_ids(session),
            ["resp_unavailable_two", "resp_loaded"]
        );

        // Once MCP hydration restores the exact historical output/control item, the newest branch
        // fails strictly and the older exact branch supplies its response id. The delta includes
        // every complete intervening unavailable turn, preserving the conversation once.
        let resume_user_three = user_message("finish with a concurrency audit");
        let mut hydrated_request = request_with_input(
            vec![
                base,
                search_call,
                loaded_output,
                additions,
                loaded_final,
                resume_user_one.clone(),
                unavailable_answer_one.clone(),
                resume_user_two.clone(),
                unavailable_answer_two.clone(),
                resume_user_three.clone(),
            ],
            None,
        );
        mark_loaded_hydration(&mut hydrated_request, 1, 2, 3);
        let hydrated = continuation_candidate(Some(session), &hydrated_request, true);
        assert_eq!(
            hydrated.previous_response_id.as_deref(),
            Some("resp_loaded")
        );
        assert_eq!(hydrated.matched_candidate_rank, Some(1));
        assert_eq!(hydrated.candidate_count, 2);
        assert_eq!(
            hydrated.input_delta,
            Some(vec![
                resume_user_one,
                unavailable_answer_one,
                resume_user_two,
                unavailable_answer_two,
                resume_user_three,
            ])
        );
        assert_eq!(pending_fallback_response_id(session), None);
        assert!(!hydrated.response_affine_fork);

        record_continuation(
            Some(session),
            hydrated.turn_id,
            &hydrated_request,
            Some("resp_hydrated"),
            std::slice::from_ref(&assistant_message("hydrated answer")),
        );
        assert_eq!(ready_response_ids(session), ["resp_hydrated"]);
    }

    #[test]
    fn repeated_deferred_tool_reference_recovers_the_loaded_exact_branch() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "repeated-deferred-tool-hydration";
        let base = user_message("load the same deferred tool twice");
        let search_one = named_function_call("search_1", "ToolSearch", r#"{"query":"first"}"#);
        let search_two = named_function_call("search_2", "ToolSearch", r#"{"query":"second"}"#);
        let loaded_one = function_output("search_1", "tool_reference: DeferredOne");
        let loaded_two = function_output("search_2", "tool_reference: DeferredOne");
        let unavailable_one = function_output("search_1", UNAVAILABLE_TOOL_REFERENCES_OUTPUT);
        let unavailable_two = function_output("search_2", UNAVAILABLE_TOOL_REFERENCES_OUTPUT);
        let additions = additional_tools(&["DeferredOne"]);
        let loaded_answer = assistant_message("the deferred tool is loaded");
        let loaded_provenance = DeferredToolHydrationProvenance {
            loaded_groups: vec![
                super::super::translate::request::DeferredToolHydrationGroup {
                    results: vec![
                        DeferredToolHydrationResult {
                            tool_search_call_index: 1,
                            output_index: 2,
                        },
                        DeferredToolHydrationResult {
                            tool_search_call_index: 4,
                            output_index: 5,
                        },
                    ],
                    additional_tools_index: 3,
                },
            ],
            unavailable_results: Vec::new(),
            ambiguous: false,
        };

        let mut loaded_request = request_with_input(
            vec![
                base.clone(),
                search_one.clone(),
                loaded_one.clone(),
                additions.clone(),
                search_two.clone(),
                loaded_two.clone(),
            ],
            None,
        );
        loaded_request.deferred_tool_hydration = loaded_provenance.clone();
        let loaded = continuation_candidate(Some(session), &loaded_request, true);
        record_continuation(
            Some(session),
            loaded.turn_id,
            &loaded_request,
            Some("resp_loaded_repeated"),
            std::slice::from_ref(&loaded_answer),
        );

        let resume_one = user_message("continue while hydration is delayed");
        let mut unavailable_request = request_with_input(
            vec![
                base.clone(),
                search_one.clone(),
                unavailable_one,
                search_two.clone(),
                unavailable_two,
                loaded_answer.clone(),
                resume_one.clone(),
            ],
            None,
        );
        unavailable_request.deferred_tool_hydration = DeferredToolHydrationProvenance {
            loaded_groups: Vec::new(),
            unavailable_results: vec![
                DeferredToolHydrationResult {
                    tool_search_call_index: 1,
                    output_index: 2,
                },
                DeferredToolHydrationResult {
                    tool_search_call_index: 3,
                    output_index: 4,
                },
            ],
            ambiguous: false,
        };
        let unavailable = continuation_candidate(Some(session), &unavailable_request, true);
        assert!(unavailable.previous_response_id.is_none());
        assert!(unavailable.response_affine_fork);
        let unavailable_answer = assistant_message("delayed hydration answer");
        record_continuation(
            Some(session),
            unavailable.turn_id,
            &unavailable_request,
            Some("resp_unavailable_repeated"),
            std::slice::from_ref(&unavailable_answer),
        );

        let resume_two = user_message("finish after hydration recovers");
        let mut hydrated_request = request_with_input(
            vec![
                base,
                search_one,
                loaded_one,
                additions,
                search_two,
                loaded_two,
                loaded_answer,
                resume_one.clone(),
                unavailable_answer.clone(),
                resume_two.clone(),
            ],
            None,
        );
        hydrated_request.deferred_tool_hydration = loaded_provenance;
        let hydrated = continuation_candidate(Some(session), &hydrated_request, true);
        assert_eq!(
            hydrated.previous_response_id.as_deref(),
            Some("resp_loaded_repeated")
        );
        assert_eq!(hydrated.matched_candidate_rank, Some(1));
        assert_eq!(hydrated.candidate_count, 2);
        assert_eq!(
            hydrated.input_delta,
            Some(vec![resume_one, unavailable_answer, resume_two])
        );
        assert!(!hydrated.response_affine_fork);
        abort_continuation(Some(session), hydrated.turn_id);
    }

    #[test]
    fn ordinary_history_edit_never_retains_an_exact_fallback() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "ordinary-history-edit";
        let original = request_with_input(vec![user_message("original")], None);
        start_and_record(session, &original, Some("resp_original"));
        let rewritten = request_with_input(
            vec![user_message("rewritten"), user_message("continue")],
            None,
        );
        let candidate = continuation_candidate(Some(session), &rewritten, true);
        assert_eq!(
            candidate.disabled_reason.as_deref(),
            Some("not_append_only")
        );
        assert!(!candidate.response_affine_fork);
        assert_eq!(pending_fallback_response_id(session), None);
        assert_eq!(total_retained_bytes(), 0);

        record_continuation(
            Some(session),
            candidate.turn_id,
            &rewritten,
            Some("resp_rewritten"),
            &[],
        );
        assert_eq!(ready_response_ids(session), ["resp_rewritten"]);
    }

    #[test]
    fn hydration_sidecar_does_not_excuse_any_other_history_change() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "hydration-plus-history-edit";
        let call = named_function_call("search_1", "ToolSearch", r#"{"query":"one"}"#);
        let mut loaded = request_with_input(
            vec![
                user_message("stable history"),
                call.clone(),
                function_output("search_1", "[tool reference: DeferredTool]"),
                additional_tools(&["DeferredTool"]),
            ],
            None,
        );
        mark_loaded_hydration(&mut loaded, 1, 2, 3);
        let initial = continuation_candidate(Some(session), &loaded, true);
        record_continuation(
            Some(session),
            initial.turn_id,
            &loaded,
            Some("resp_loaded"),
            &[assistant_message("answer")],
        );

        let mut unavailable = request_with_input(
            vec![
                user_message("changed history"),
                call,
                function_output("search_1", UNAVAILABLE_TOOL_REFERENCES_OUTPUT),
                assistant_message("answer"),
                user_message("continue"),
            ],
            None,
        );
        mark_unavailable_hydration(&mut unavailable, 1, 2);
        let candidate = continuation_candidate(Some(session), &unavailable, true);
        assert_eq!(
            candidate.disabled_reason.as_deref(),
            Some("not_append_only")
        );
        assert!(!candidate.response_affine_fork);
        assert_eq!(pending_fallback_response_id(session), None);
    }

    #[test]
    fn interleaved_output_matching_rejects_reordered_or_incomplete_topology() {
        let base = user_message("original history");
        let call_a = function_call("call_a", r#"{"path":"a"}"#);
        let call_b = function_call("call_b", r#"{"path":"b"}"#);
        let call_c = function_call("call_c", r#"{"path":"c"}"#);
        let output_a = function_output("call_a", "a-result");
        let output_b = function_output("call_b", "b-result");
        let output_c = function_output("call_c", "c-result");
        let prefix = vec![base.clone(), call_a.clone(), call_b.clone(), call_c.clone()];

        let reordered = vec![
            base.clone(),
            call_a.clone(),
            call_c.clone(),
            output_a.clone(),
            call_b.clone(),
            output_b.clone(),
            output_c.clone(),
        ];
        assert!(input_suffix_after_prefix(&reordered, &prefix, 1, &[], false).is_none());

        let invalid_inputs = vec![
            // A result cannot precede its own call.
            vec![
                base.clone(),
                call_a.clone(),
                output_b.clone(),
                call_b.clone(),
                output_a.clone(),
                call_c.clone(),
                output_c.clone(),
            ],
            // Missing and duplicate results both fail closed.
            vec![
                base.clone(),
                call_a.clone(),
                output_a.clone(),
                call_b.clone(),
                output_b.clone(),
                call_c.clone(),
            ],
            vec![
                base.clone(),
                call_a.clone(),
                output_a.clone(),
                output_a.clone(),
                call_b.clone(),
                output_b.clone(),
                call_c.clone(),
                output_c.clone(),
            ],
            // Unknown/cross-batch output and non-tool items cannot interrupt an unresolved group.
            vec![
                base.clone(),
                call_a.clone(),
                function_output("call_unknown", "unknown"),
                call_b.clone(),
                output_a.clone(),
                output_b.clone(),
                call_c.clone(),
                output_c.clone(),
            ],
            vec![
                base.clone(),
                call_a.clone(),
                user_message("unexpected"),
                call_b.clone(),
                output_a.clone(),
                output_b.clone(),
                call_c.clone(),
                output_c.clone(),
            ],
            // Matching IDs do not excuse a changed argument value.
            vec![
                base.clone(),
                function_call("call_a", r#"{"path":"changed"}"#),
                output_a.clone(),
                call_b.clone(),
                output_b.clone(),
                call_c.clone(),
                output_c.clone(),
            ],
            // The strict historical request prefix cannot be rewritten.
            vec![
                user_message("changed history"),
                call_a.clone(),
                output_a.clone(),
                call_b.clone(),
                output_b.clone(),
                call_c.clone(),
                output_c.clone(),
            ],
            // A duplicate group member cannot hide after an otherwise complete group.
            vec![
                base.clone(),
                call_a.clone(),
                output_a.clone(),
                call_b.clone(),
                output_b.clone(),
                call_c.clone(),
                output_c.clone(),
                output_a,
            ],
        ];

        for input in invalid_inputs {
            for allow_parallel_reorder in [false, true] {
                assert!(
                    input_suffix_after_prefix(&input, &prefix, 1, &[], allow_parallel_reorder,)
                        .is_none(),
                    "unsafe continuation topology was accepted: {}",
                    serde_json::to_string(&input).unwrap()
                );
            }
        }
    }

    #[test]
    fn pending_exact_fallback_is_never_eligible_after_concurrent_supersede() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "pending-fallback-supersede";
        let (rewritten_request, first) = start_pending_not_append(session);
        assert!(total_retained_bytes() > 0);

        // The hidden state belongs only to the in-flight turn. A concurrent request cannot select
        // it and atomically discards its retained-byte accounting when superseding that turn.
        let mut concurrent_request = rewritten_request.clone();
        concurrent_request
            .input
            .push(user_message("concurrent request"));
        let concurrent = continuation_candidate(Some(session), &concurrent_request, true);
        assert_eq!(concurrent.previous_response_id, None);
        assert_eq!(
            concurrent.disabled_reason.as_deref(),
            Some("superseded_turn")
        );
        assert_eq!(pending_fallback_response_id(session), None);
        assert_eq!(total_retained_bytes(), 0);

        record_continuation(
            Some(session),
            first.turn_id,
            &rewritten_request,
            Some("resp_stale"),
            &[],
        );
        assert!(!has_continuation_for_tests(session));
        record_continuation(
            Some(session),
            concurrent.turn_id,
            &concurrent_request,
            Some("resp_concurrent"),
            &[],
        );
        assert_eq!(ready_response_ids(session), ["resp_concurrent"]);
    }

    #[test]
    fn empty_primary_delta_never_falls_through_to_a_shorter_exact_candidate() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "empty-primary-no-fallthrough";
        let first_item = user_message("already consumed");
        let second_item = assistant_message("most recent response");
        let request = request_with_input(vec![first_item.clone(), second_item.clone()], None);
        let initial = continuation_candidate(Some(session), &request, true);
        record_continuation(
            Some(session),
            initial.turn_id,
            &request,
            Some("resp_primary"),
            &[],
        );

        // Seed a valid shorter exact branch. It would see `second_item` as a non-empty delta, but
        // the newest branch already consumed the complete request and therefore must win with
        // empty_delta instead of replaying that item against an older response.
        {
            let mut guard = REGISTRY.lock().unwrap();
            let registry = guard.as_mut().unwrap();
            let fallback_bytes = {
                let session_state = registry.lanes.get_mut(&test_lane_key(session)).unwrap();
                let mut fallback = session_state.candidates[0].clone();
                fallback.response_id = "resp_shorter".to_string();
                fallback.transcript = vec![first_item.clone()];
                fallback.request_input_len = 1;
                fallback.parallel_batches.clear();
                fallback.transcript_bytes = serialized_transcript_bytes(
                    &fallback.transcript,
                    &[],
                    MAX_SESSION_TRANSCRIPT_BYTES,
                )
                .unwrap();
                fallback.retained_bytes = continuation_retained_bytes(&fallback).unwrap();
                let fallback_bytes = fallback.retained_bytes;
                session_state.candidates.push(fallback);
                fallback_bytes
            };
            registry.total_retained_bytes =
                registry.total_retained_bytes.saturating_add(fallback_bytes);
        }

        let candidate = continuation_candidate(Some(session), &request, true);
        assert_eq!(candidate.previous_response_id, None);
        assert_eq!(candidate.matched_candidate_rank, None);
        assert_eq!(candidate.candidate_count, 2);
        assert_eq!(candidate.disabled_reason.as_deref(), Some("empty_delta"));
        assert_eq!(candidate.input_delta_count, 0);
        assert_eq!(pending_fallback_response_id(session), None);
        abort_continuation(Some(session), candidate.turn_id);
        assert_eq!(total_retained_bytes(), 0);

        // Keep the fixture honest: the older branch really would have accepted this suffix if the
        // matcher had fallen through after empty_delta.
        let shorter = input_suffix_after_prefix(&request.input, &[first_item], 1, &[], true)
            .expect("the shorter exact candidate should see one appended item");
        assert_eq!(shorter.delta, [second_item]);
    }

    #[test]
    fn fallback_ttl_is_checked_independently_from_the_newest_candidate() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "per-branch-ttl";
        let (unavailable_request, unavailable) = start_pending_not_append(session);
        let unavailable_answer = assistant_message("unavailable answer");
        record_continuation(
            Some(session),
            unavailable.turn_id,
            &unavailable_request,
            Some("resp_unavailable"),
            std::slice::from_ref(&unavailable_answer),
        );
        assert_eq!(
            ready_response_ids(session),
            ["resp_unavailable", "resp_original"]
        );

        {
            let mut guard = REGISTRY.lock().unwrap();
            let session = guard
                .as_mut()
                .unwrap()
                .lanes
                .get_mut(&test_lane_key(session))
                .unwrap();
            session.candidates[1].updated_at = now_ms().saturating_sub(TTL_MS + 1);
        }

        let mut hydrated_request = request_with_input(
            vec![
                user_message("original history"),
                named_function_call("search_1", "ToolSearch", r#"{"query":"one"}"#),
                function_output("search_1", "[tool reference: DeferredTool]"),
                additional_tools(&["DeferredTool"]),
                assistant_message("original answer"),
                user_message("resume"),
                unavailable_answer,
                user_message("hydrated turn"),
            ],
            None,
        );
        mark_loaded_hydration(&mut hydrated_request, 1, 2, 3);
        let hydrated = continuation_candidate(Some(session), &hydrated_request, true);
        assert_eq!(hydrated.previous_response_id, None);
        assert_eq!(hydrated.disabled_reason.as_deref(), Some("not_append_only"));
        assert_eq!(
            pending_fallback_response_id(session).as_deref(),
            Some("resp_unavailable")
        );
        abort_continuation(Some(session), hydrated.turn_id);
        assert_eq!(total_retained_bytes(), 0);
    }

    #[test]
    fn failed_pending_publication_clears_fallback_and_retained_byte_accounting() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();

        clear_all_continuations_for_tests();
        let (request, candidate) = start_pending_not_append("invalid-fallback-response-id");
        assert!(total_retained_bytes() > 0);
        record_continuation(
            Some("invalid-fallback-response-id"),
            candidate.turn_id,
            &request,
            Some(""),
            &[],
        );
        assert_eq!(total_retained_bytes(), 0);
        assert!(!has_continuation_for_tests("invalid-fallback-response-id"));

        clear_all_continuations_for_tests();
        let (request, candidate) = start_pending_not_append("oversized-fallback-transcript");
        assert!(total_retained_bytes() > 0);
        let oversized = assistant_message(&"x".repeat(MAX_SESSION_TRANSCRIPT_BYTES as usize));
        record_continuation(
            Some("oversized-fallback-transcript"),
            candidate.turn_id,
            &request,
            Some("resp_too_large"),
            std::slice::from_ref(&oversized),
        );
        assert_eq!(total_retained_bytes(), 0);
        assert!(!has_continuation_for_tests("oversized-fallback-transcript"));

        clear_all_continuations_for_tests();
        let (request, candidate) = start_pending_not_append("invalid-fallback-batches");
        assert!(total_retained_bytes() > 0);
        {
            let mut guard = REGISTRY.lock().unwrap();
            guard
                .as_mut()
                .unwrap()
                .lanes
                .get_mut(&test_lane_key("invalid-fallback-batches"))
                .unwrap()
                .pending_parallel_batches = vec![ParallelBatchRange { start: 1, end: 1 }];
        }
        record_continuation(
            Some("invalid-fallback-batches"),
            candidate.turn_id,
            &request,
            Some("resp_invalid_batches"),
            &[],
        );
        assert_eq!(total_retained_bytes(), 0);
        assert!(!has_continuation_for_tests("invalid-fallback-batches"));
    }

    #[test]
    fn full_context_retry_discards_only_the_current_pending_fallback() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "discard-pending-fallback";
        let (request, candidate) = start_pending_not_append(session);
        let turn_id = candidate.turn_id;
        assert!(total_retained_bytes() > 0);

        let stale_turn = turn_id.map(|turn| turn.saturating_sub(1));
        discard_pending_fallback(Some(&test_lane_key(session)), stale_turn);
        assert_eq!(
            pending_fallback_response_id(session).as_deref(),
            Some("resp_original")
        );

        discard_pending_fallback(Some(&test_lane_key(session)), turn_id);
        assert_eq!(pending_fallback_response_id(session), None);
        assert_eq!(total_retained_bytes(), 0);
        record_continuation(
            Some(session),
            turn_id,
            &request,
            Some("resp_full_context_retry"),
            &[],
        );
        assert_eq!(ready_response_ids(session), ["resp_full_context_retry"]);
    }

    #[test]
    fn websocket_affinity_eviction_removes_only_the_bound_exact_state() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "socket-affinity-eviction";
        let request = request_with_input(vec![user_message("original history")], None);
        start_and_record(session, &request, Some("resp_bound"));
        let retained = total_retained_bytes();
        assert!(retained > 0);

        invalidate_response_affinity(&test_lane_key(session), "resp_other");
        assert_eq!(ready_response_ids(session), ["resp_bound"]);
        assert_eq!(total_retained_bytes(), retained);

        invalidate_response_affinity(&test_lane_key(session), "resp_bound");
        assert!(!has_continuation_for_tests(session));
        assert_eq!(total_retained_bytes(), 0);

        let (rewritten_request, pending) = start_pending_not_append(session);
        assert_eq!(
            pending_fallback_response_id(session).as_deref(),
            Some("resp_original")
        );
        invalidate_response_affinity(&test_lane_key(session), "resp_original");
        assert_eq!(pending_fallback_response_id(session), None);
        assert_eq!(total_retained_bytes(), 0);

        record_continuation(
            Some(session),
            pending.turn_id,
            &rewritten_request,
            Some("resp_replacement"),
            &[],
        );
        assert_eq!(ready_response_ids(session), ["resp_replacement"]);
    }

    #[test]
    fn websocket_affinity_eviction_rejects_late_record_and_restores_live_fallback() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "pending-socket-affinity-eviction";
        let (request, pending) = start_pending_not_append(session);
        let turn_id = pending.turn_id.expect("continuation turn");
        assert!(total_retained_bytes() > 0);

        invalidate_response_affinity_for_turn(
            &test_lane_key(session),
            "resp_not_yet_published",
            turn_id.saturating_sub(1),
        );
        assert!(is_current_turn(
            Some(&test_lane_key(session)),
            Some(turn_id)
        ));
        assert_eq!(
            pending_fallback_response_id(session).as_deref(),
            Some("resp_original")
        );

        invalidate_response_affinity_for_turn(
            &test_lane_key(session),
            "resp_not_yet_published",
            turn_id,
        );
        assert!(!is_current_turn(
            Some(&test_lane_key(session)),
            Some(turn_id)
        ));
        assert_eq!(ready_response_ids(session), ["resp_original"]);
        assert_eq!(pending_fallback_response_id(session), None);
        assert!(total_retained_bytes() > 0);

        record_continuation(
            Some(session),
            Some(turn_id),
            &request,
            Some("resp_dead_socket"),
            &[],
        );
        assert_eq!(ready_response_ids(session), ["resp_original"]);
    }

    #[test]
    fn unreachable_success_without_affinity_restores_fallback_and_blocks_late_record() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "pending-success-without-affinity";
        let (request, pending) = start_pending_not_append(session);
        let turn_id = pending.turn_id.expect("continuation turn");
        abandon_pending_response_preserving_fallback(Some(&test_lane_key(session)), Some(turn_id));

        assert_eq!(ready_response_ids(session), ["resp_original"]);
        assert_eq!(pending_fallback_response_id(session), None);
        record_continuation(
            Some(session),
            Some(turn_id),
            &request,
            Some("resp_late_unbound"),
            &[],
        );
        assert_eq!(ready_response_ids(session), ["resp_original"]);
    }

    #[test]
    fn stale_affinity_eviction_cannot_delete_a_reused_id_from_a_newer_turn() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "socket-affinity-aba";
        let first_request = request_with_input(vec![user_message("first")], None);
        let first = continuation_candidate(Some(session), &first_request, true);
        let first_turn = first.turn_id.expect("first continuation turn");
        record_continuation(
            Some(session),
            Some(first_turn),
            &first_request,
            Some("resp_reused"),
            &[],
        );

        let second_request =
            request_with_input(vec![user_message("first"), user_message("second")], None);
        let second = continuation_candidate(Some(session), &second_request, true);
        assert_eq!(second.previous_response_id.as_deref(), Some("resp_reused"));
        assert_eq!(
            second.previous_response_owner_turn_id,
            Some(first_turn),
            "a continuation candidate must carry the producing turn, not only the response id"
        );
        let second_turn = second.turn_id.expect("second continuation turn");
        record_continuation(
            Some(session),
            Some(second_turn),
            &second_request,
            Some("resp_reused"),
            &[],
        );

        invalidate_response_affinity_for_turn(&test_lane_key(session), "resp_reused", first_turn);
        assert_eq!(ready_response_ids(session), ["resp_reused"]);

        invalidate_response_affinity_for_turn(&test_lane_key(session), "resp_reused", second_turn);
        assert!(!has_continuation_for_tests(session));
    }

    #[test]
    fn record_rejects_empty_or_oversized_response_ids() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        let request = request_with_input(vec![user_message("hello")], None);
        for response_id in [String::new(), "r".repeat(MAX_RESPONSE_ID_BYTES + 1)] {
            clear_all_continuations_for_tests();
            let candidate = continuation_candidate(Some("bounded-response-id"), &request, true);
            record_continuation(
                Some("bounded-response-id"),
                candidate.turn_id,
                &request,
                Some(&response_id),
                &[],
            );
            assert!(!has_continuation_for_tests("bounded-response-id"));
        }
    }

    #[test]
    fn record_publication_refreshes_session_eviction_timestamp() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();
        let request = request_with_input(vec![user_message("hello")], None);
        let candidate = continuation_candidate(Some("fresh-publication"), &request, true);
        {
            let mut guard = REGISTRY.lock().unwrap();
            guard
                .as_mut()
                .unwrap()
                .lanes
                .get_mut(&test_lane_key("fresh-publication"))
                .unwrap()
                .updated_at = 1;
        }

        record_continuation(
            Some("fresh-publication"),
            candidate.turn_id,
            &request,
            Some("resp_fresh"),
            &[],
        );

        let guard = REGISTRY.lock().unwrap();
        let session = guard
            .as_ref()
            .unwrap()
            .lanes
            .get(&test_lane_key("fresh-publication"))
            .unwrap();
        let continuation = session.candidates.first().unwrap();
        assert_eq!(session.updated_at, continuation.updated_at);
        assert!(session.updated_at > 1);
    }

    #[test]
    fn disabling_continuation_clears_ready_and_pending_lane_state() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        clear_all_continuations_for_tests();

        let session = "toggle-continuation";
        let initial_request = request_with_input(vec![user_message("first")], None);
        start_and_record(session, &initial_request, Some("resp_before_disable"));
        assert!(has_continuation_for_tests(session));
        assert!(total_retained_bytes() > 0);

        let disabled_request = request_with_input(
            vec![
                user_message("first"),
                user_message("full request while disabled"),
            ],
            None,
        );
        let disabled = continuation_candidate(Some(session), &disabled_request, false);
        assert_eq!(disabled.turn_id, None);
        assert_eq!(disabled.disabled_reason.as_deref(), Some("disabled"));
        assert_eq!(disabled.candidate_count, 0);
        assert!(!has_continuation_for_tests(session));
        assert_eq!(total_retained_bytes(), 0);

        let reenabled = continuation_candidate(Some(session), &disabled_request, true);
        assert_eq!(reenabled.previous_response_id, None);
        assert_eq!(reenabled.matched_candidate_rank, None);
        assert_eq!(reenabled.candidate_count, 0);
        assert_eq!(reenabled.disabled_reason.as_deref(), Some("missing_state"));
        abort_continuation(Some(session), reenabled.turn_id);

        // A disabled call also clears an in-flight hidden fallback and its accounting.
        let (_rewritten, pending) = start_pending_not_append(session);
        assert!(pending_fallback_response_id(session).is_some());
        continuation_candidate(Some(session), &initial_request, false);
        assert_eq!(pending_fallback_response_id(session), None);
        assert_eq!(total_retained_bytes(), 0);
        abort_continuation(Some(session), pending.turn_id);
    }

    #[test]
    fn continuation_behaviors() {
        let _state_guard = super::super::CODEX_STATE_TEST_LOCK.blocking_lock();
        // All tests run in sequence to avoid global state interference

        // disabled_when_not_enabled
        clear_all_continuations_for_tests();
        let input = vec![ResponsesInputItem::Message {
            role: "user".to_string(),
            content: vec![
                super::super::translate::request::ResponsesContentPart::InputText {
                    text: "one".to_string(),
                },
            ],
        }];
        let req = request_with_input(input, None);
        let result = continuation_candidate(Some("s1"), &req, false);
        assert_eq!(result.disabled_reason, Some("disabled".to_string()));
        assert_eq!(result.input_delta_count, 1);

        // missing_session
        clear_all_continuations_for_tests();
        let input = vec![ResponsesInputItem::Message {
            role: "user".to_string(),
            content: vec![
                super::super::translate::request::ResponsesContentPart::InputText {
                    text: "one".to_string(),
                },
            ],
        }];
        let req = request_with_input(input, None);
        let result = continuation_candidate(None, &req, true);
        assert_eq!(result.disabled_reason, Some("missing_session".to_string()));

        // uses_previous_response_id_for_append_only
        clear_all_continuations_for_tests();
        let input = vec![ResponsesInputItem::Message {
            role: "user".to_string(),
            content: vec![
                super::super::translate::request::ResponsesContentPart::InputText {
                    text: "one".to_string(),
                },
            ],
        }];
        let req = request_with_input(input, None);
        start_and_record("s1", &req, Some("resp_1"));

        let input2 = vec![
            ResponsesInputItem::Message {
                role: "user".to_string(),
                content: vec![
                    super::super::translate::request::ResponsesContentPart::InputText {
                        text: "one".to_string(),
                    },
                ],
            },
            ResponsesInputItem::Message {
                role: "user".to_string(),
                content: vec![
                    super::super::translate::request::ResponsesContentPart::InputText {
                        text: "two".to_string(),
                    },
                ],
            },
        ];
        let req2 = request_with_input(input2, None);
        let result = continuation_candidate(Some("s1"), &req2, true);
        assert_eq!(result.previous_response_id, Some("resp_1".to_string()));
        assert_eq!(result.input_delta_count, 1);

        // clears_state_when_prompt_signature_changes
        clear_all_continuations_for_tests();
        let input = vec![ResponsesInputItem::Message {
            role: "user".to_string(),
            content: vec![
                super::super::translate::request::ResponsesContentPart::InputText {
                    text: "one".to_string(),
                },
            ],
        }];
        let req = request_with_input(input.clone(), None);
        start_and_record("s1", &req, Some("resp_1"));

        let req2 = request_with_input(input, Some(json!({"service_tier": "flex"})));
        let lane_key = test_lane_key("s1");
        let decision = continuation_candidate_with_diagnostics(Some(&lane_key), &req2, true);
        assert_eq!(decision.prompt_changed_fields, ["service_tier"]);
        let result = decision.candidate;
        assert_eq!(result.disabled_reason, Some("prompt_changed".to_string()));
        assert!(!has_continuation_for_tests("s1"));

        // clears_state_when_missing_response_id
        clear_all_continuations_for_tests();
        let input = vec![ResponsesInputItem::Message {
            role: "user".to_string(),
            content: vec![
                super::super::translate::request::ResponsesContentPart::InputText {
                    text: "one".to_string(),
                },
            ],
        }];
        let req = request_with_input(input.clone(), None);
        start_and_record("s1", &req, Some("resp_1"));
        assert!(has_continuation_for_tests("s1"));

        let candidate = continuation_candidate(Some("s1"), &req, true);
        record_continuation(Some("s1"), candidate.turn_id, &req, None, &[]);
        assert!(!has_continuation_for_tests("s1"));

        // stale turns cannot publish or clear a newer turn
        clear_all_continuations_for_tests();
        let first = continuation_candidate(Some("s1"), &req, true);
        record_continuation(Some("s1"), first.turn_id, &req, Some("resp_1"), &[]);
        record_continuation(Some("s1"), first.turn_id, &req, Some("resp_duplicate"), &[]);
        assert_eq!(ready_response_ids("s1"), ["resp_1"]);
        let second = continuation_candidate(Some("s1"), &req, true);
        let third = continuation_candidate(Some("s1"), &req, true);
        assert_eq!(third.disabled_reason.as_deref(), Some("superseded_turn"));

        record_continuation(Some("s1"), second.turn_id, &req, Some("resp_2"), &[]);
        assert!(!has_continuation_for_tests("s1"));
        record_continuation(Some("s1"), third.turn_id, &req, Some("resp_3"), &[]);
        assert!(has_continuation_for_tests("s1"));
        abort_continuation(Some("s1"), second.turn_id);
        assert!(has_continuation_for_tests("s1"));
    }
}
