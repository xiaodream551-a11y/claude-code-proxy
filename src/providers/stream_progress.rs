use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

pub(crate) const STREAM_PROGRESS_INTERVAL: Duration = Duration::from_secs(60);
const MAX_STREAM_PROGRESS_RECORDS: u8 = 64;
const MAX_PROGRESS_COUNT: u64 = 1_000_000_000;
const MAX_PROGRESS_BYTES: u64 = 1_u64 << 40;
const MAX_PROGRESS_ELAPSED_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamProgressPhase {
    Upstream,
    Rebuild,
}

impl StreamProgressPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Upstream => "upstream",
            Self::Rebuild => "rebuild",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamActivity {
    Generation,
    Control,
    None,
}

impl UpstreamActivity {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Generation => "generation",
            Self::Control => "control",
            Self::None => "none",
        }
    }
}

pub(crate) struct StreamProgress {
    started_at: Instant,
    upstream_events: u64,
    generation_events: u64,
    downstream_chunks: u64,
    downstream_bytes: u64,
    local_heartbeats: u64,
    rebuilds: u64,
    last_logged_upstream_events: u64,
    last_logged_generation_events: u64,
    records: u8,
}

impl StreamProgress {
    pub(crate) fn new(started_at: Instant) -> Self {
        Self {
            started_at,
            upstream_events: 0,
            generation_events: 0,
            downstream_chunks: 0,
            downstream_bytes: 0,
            local_heartbeats: 0,
            rebuilds: 0,
            last_logged_upstream_events: 0,
            last_logged_generation_events: 0,
            records: 0,
        }
    }

    pub(crate) fn timer(&self) -> tokio::time::Interval {
        let first_delay = STREAM_PROGRESS_INTERVAL.saturating_sub(self.started_at.elapsed());
        let first_at = tokio::time::Instant::now()
            .checked_add(first_delay)
            .unwrap_or_else(tokio::time::Instant::now);
        let mut interval = tokio::time::interval_at(first_at, STREAM_PROGRESS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    }

    pub(crate) fn observe_upstream_event(&mut self, generation: bool) {
        self.observe_upstream_events(1, usize::from(generation));
    }

    pub(crate) fn observe_upstream_events(&mut self, total: usize, generation: usize) {
        let total = u64::try_from(total).unwrap_or(u64::MAX);
        let generation = u64::try_from(generation).unwrap_or(u64::MAX).min(total);
        self.upstream_events = bounded_add(self.upstream_events, total, MAX_PROGRESS_COUNT);
        self.generation_events =
            bounded_add(self.generation_events, generation, MAX_PROGRESS_COUNT);
    }

    pub(crate) fn observe_downstream_chunk(&mut self, bytes: usize) {
        self.downstream_chunks = bounded_add(self.downstream_chunks, 1, MAX_PROGRESS_COUNT);
        self.downstream_bytes = bounded_add(
            self.downstream_bytes,
            u64::try_from(bytes).unwrap_or(u64::MAX),
            MAX_PROGRESS_BYTES,
        );
    }

    pub(crate) fn observe_local_heartbeat(&mut self) {
        self.local_heartbeats = bounded_add(self.local_heartbeats, 1, MAX_PROGRESS_COUNT);
    }

    pub(crate) fn observe_rebuild(&mut self) {
        self.rebuilds = bounded_add(self.rebuilds, 1, MAX_PROGRESS_COUNT);
    }

    pub(crate) fn log(
        &mut self,
        provider: &'static str,
        req_id: &str,
        generation_started: bool,
        phase: StreamProgressPhase,
    ) {
        let Some(fields) = self.next_fields(req_id, generation_started, phase) else {
            return;
        };
        crate::logging::create_logger(provider).info("stream_progress", Some(fields));
    }

    fn next_fields(
        &mut self,
        req_id: &str,
        generation_started: bool,
        phase: StreamProgressPhase,
    ) -> Option<Map<String, Value>> {
        if self.records >= MAX_STREAM_PROGRESS_RECORDS {
            return None;
        }
        let activity = if self.generation_events > self.last_logged_generation_events {
            UpstreamActivity::Generation
        } else if self.upstream_events > self.last_logged_upstream_events {
            UpstreamActivity::Control
        } else {
            UpstreamActivity::None
        };
        self.last_logged_upstream_events = self.upstream_events;
        self.last_logged_generation_events = self.generation_events;
        self.records = self.records.saturating_add(1);

        let elapsed_ms = u64::try_from(self.started_at.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .min(MAX_PROGRESS_ELAPSED_MS);
        Some(Map::from_iter([
            ("reqId".into(), json!(req_id)),
            ("elapsedMs".into(), json!(elapsed_ms)),
            ("streamPhase".into(), json!(phase.as_str())),
            ("upstreamActivity".into(), json!(activity.as_str())),
            ("generationStarted".into(), json!(generation_started)),
            ("upstreamEvents".into(), json!(self.upstream_events)),
            ("generationEvents".into(), json!(self.generation_events)),
            ("downstreamChunks".into(), json!(self.downstream_chunks)),
            ("downstreamBytes".into(), json!(self.downstream_bytes)),
            ("localHeartbeats".into(), json!(self.local_heartbeats)),
            ("rebuilds".into(), json!(self.rebuilds)),
        ]))
    }
}

fn bounded_add(current: u64, delta: u64, maximum: u64) -> u64 {
    current.saturating_add(delta).min(maximum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_classifies_only_upstream_events_as_upstream_activity() {
        let mut progress = StreamProgress::new(Instant::now());
        progress.observe_local_heartbeat();
        let local = progress
            .next_fields("request", false, StreamProgressPhase::Upstream)
            .unwrap();
        assert_eq!(local["upstreamActivity"], "none");
        assert_eq!(local["localHeartbeats"], 1);

        progress.observe_upstream_event(false);
        let control = progress
            .next_fields("request", false, StreamProgressPhase::Upstream)
            .unwrap();
        assert_eq!(control["upstreamActivity"], "control");

        progress.observe_upstream_event(true);
        let generation = progress
            .next_fields("request", true, StreamProgressPhase::Upstream)
            .unwrap();
        assert_eq!(generation["upstreamActivity"], "generation");
    }

    #[test]
    fn progress_counts_saturate_and_records_are_bounded() {
        let mut progress = StreamProgress::new(Instant::now());
        progress.upstream_events = MAX_PROGRESS_COUNT;
        progress.generation_events = MAX_PROGRESS_COUNT;
        progress.downstream_chunks = MAX_PROGRESS_COUNT;
        progress.downstream_bytes = MAX_PROGRESS_BYTES;
        progress.local_heartbeats = MAX_PROGRESS_COUNT;
        progress.rebuilds = MAX_PROGRESS_COUNT;
        progress.observe_upstream_event(true);
        progress.observe_downstream_chunk(usize::MAX);
        progress.observe_local_heartbeat();
        progress.observe_rebuild();
        assert_eq!(progress.upstream_events, MAX_PROGRESS_COUNT);
        assert_eq!(progress.generation_events, MAX_PROGRESS_COUNT);
        assert_eq!(progress.downstream_chunks, MAX_PROGRESS_COUNT);
        assert_eq!(progress.downstream_bytes, MAX_PROGRESS_BYTES);
        assert_eq!(progress.local_heartbeats, MAX_PROGRESS_COUNT);
        assert_eq!(progress.rebuilds, MAX_PROGRESS_COUNT);

        for _ in 0..MAX_STREAM_PROGRESS_RECORDS {
            assert!(
                progress
                    .next_fields("request", true, StreamProgressPhase::Rebuild)
                    .is_some()
            );
        }
        assert!(
            progress
                .next_fields("request", true, StreamProgressPhase::Rebuild)
                .is_none()
        );
    }
}
