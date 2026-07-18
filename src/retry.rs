use rand::Rng;
use std::time::{Duration, SystemTime};

pub const RETRY_INITIAL_DELAY_MS: u64 = 2000;
pub const RETRY_MAX_DELAY_MS: u64 = 30_000;
pub const RETRY_BACKOFF_FACTOR: u64 = 2;
pub const MAX_RATE_LIMIT_RETRIES: u32 = 3;
const RETRY_AFTER_MIN_DELAY_MS: u64 = 100;
const RETRY_JITTER_MIN_PERCENT: u64 = 80;
const RETRY_JITTER_MAX_PERCENT: u64 = 120;
const FAST_PRE_DISPATCH_MIN_DELAY_MS: u64 = 100;
const FAST_PRE_DISPATCH_MAX_DELAY_MS: u64 = 300;

/// Whether replaying a model request can be justified from the observed
/// transport outcome.
///
/// This is deliberately separate from an error being transient. A header or
/// response-body reset can be transient while still leaving the result of the
/// original POST unknown, in which case replaying it may duplicate generation
/// or hosted-tool work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaySafety {
    /// The model request was not dispatched to the upstream application.
    DefinitelyNotDispatched,
    /// The upstream explicitly returned a response that permits retry.
    ExplicitlyRetryableResponse,
    /// The request may have been accepted or partially processed upstream.
    OutcomeUnknown,
}

impl ReplaySafety {
    pub fn permits_model_replay(self) -> bool {
        !matches!(self, Self::OutcomeUnknown)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DefinitelyNotDispatched => "definitely_not_dispatched",
            Self::ExplicitlyRetryableResponse => "explicitly_retryable_response",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

/// Backoff state for physical attempts of one logical model request.
///
/// One proven pre-dispatch transport failure may recover quickly. All other
/// replay-safe failures retain the existing service-overload backoff, while an
/// unknown POST outcome never receives a replay delay at all.
#[derive(Debug, Default)]
pub struct ModelRetryBackoff {
    saw_definitely_not_dispatched: bool,
    standard_attempt: u32,
}

impl ModelRetryBackoff {
    pub fn next_delay(
        &mut self,
        replay_safety: ReplaySafety,
        retry_after: Option<&str>,
    ) -> Option<BackoffOutcome> {
        match replay_safety {
            ReplaySafety::OutcomeUnknown => None,
            ReplaySafety::DefinitelyNotDispatched => {
                let first_predispatch = !self.saw_definitely_not_dispatched;
                self.saw_definitely_not_dispatched = true;
                if first_predispatch && retry_after.is_none() {
                    return Some(BackoffOutcome {
                        wait_ms: rand::thread_rng().gen_range(
                            FAST_PRE_DISPATCH_MIN_DELAY_MS..=FAST_PRE_DISPATCH_MAX_DELAY_MS,
                        ),
                        exceeds_budget: false,
                    });
                }
                Some(self.next_standard_delay(retry_after))
            }
            ReplaySafety::ExplicitlyRetryableResponse => {
                Some(self.next_standard_delay(retry_after))
            }
        }
    }

    fn next_standard_delay(&mut self, retry_after: Option<&str>) -> BackoffOutcome {
        let delay = compute_backoff_delay(self.standard_attempt, retry_after);
        self.standard_attempt = self.standard_attempt.saturating_add(1);
        delay
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BackoffOutcome {
    pub wait_ms: u64,
    pub exceeds_budget: bool,
}

pub fn should_retry_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

pub fn compute_backoff_delay(attempt: u32, retry_after: Option<&str>) -> BackoffOutcome {
    let jitter_percent =
        rand::thread_rng().gen_range(RETRY_JITTER_MIN_PERCENT..=RETRY_JITTER_MAX_PERCENT);
    compute_backoff_delay_at(attempt, retry_after, SystemTime::now(), jitter_percent)
}

fn compute_backoff_delay_at(
    attempt: u32,
    retry_after: Option<&str>,
    now: SystemTime,
    jitter_percent: u64,
) -> BackoffOutcome {
    if let Some(target_ms) = retry_after.and_then(|raw| retry_after_ms(raw, now)) {
        return BackoffOutcome {
            wait_ms: target_ms.clamp(RETRY_AFTER_MIN_DELAY_MS, RETRY_MAX_DELAY_MS),
            exceeds_budget: target_ms > RETRY_MAX_DELAY_MS,
        };
    }

    let mut exp =
        RETRY_INITIAL_DELAY_MS.saturating_mul(RETRY_BACKOFF_FACTOR.saturating_pow(attempt));
    if exp > RETRY_MAX_DELAY_MS {
        exp = RETRY_MAX_DELAY_MS;
    }
    let wait_ms = exp
        .saturating_mul(jitter_percent)
        .div_ceil(100)
        .min(RETRY_MAX_DELAY_MS);
    BackoffOutcome {
        wait_ms,
        exceeds_budget: false,
    }
}

fn retry_after_ms(raw: &str, now: SystemTime) -> Option<u64> {
    if let Ok(seconds) = raw.parse::<f64>()
        && seconds.is_finite()
        && seconds >= 0.0
    {
        return Some((seconds * 1000.0).ceil() as u64);
    }

    let target = httpdate::parse_http_date(raw).ok()?;
    Some(
        target
            .duration_since(now)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

pub async fn sleep(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[cfg(test)]
pub async fn retry_on_statuses<T, E, F>(mut next: F) -> Result<T, E>
where
    E: std::fmt::Debug,
    F: FnMut(u32) -> Result<T, E>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        if attempt > MAX_RATE_LIMIT_RETRIES + 1 {
            break;
        }
        match next(attempt) {
            Ok(value) => return Ok(value),
            Err(err) if attempt <= MAX_RATE_LIMIT_RETRIES + 1 => {
                if attempt > MAX_RATE_LIMIT_RETRIES {
                    return Err(err);
                }
                sleep(compute_backoff_delay(attempt, None).wait_ms).await;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_applies_bounded_jitter() {
        let now = SystemTime::UNIX_EPOCH;
        let low = compute_backoff_delay_at(0, None, now, RETRY_JITTER_MIN_PERCENT);
        let high = compute_backoff_delay_at(0, None, now, RETRY_JITTER_MAX_PERCENT);

        assert_eq!(low.wait_ms, 1_600);
        assert_eq!(high.wait_ms, 2_400);
        assert!(!low.exceeds_budget);
        assert!(!high.exceeds_budget);
    }

    #[test]
    fn retry_after_accepts_http_date_and_applies_a_minimum_delay() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let future = httpdate::fmt_http_date(now + Duration::from_secs(5));
        let past = httpdate::fmt_http_date(now - Duration::from_secs(5));

        assert_eq!(
            compute_backoff_delay_at(0, Some(&future), now, 100).wait_ms,
            5_000
        );
        assert_eq!(
            compute_backoff_delay_at(0, Some(&past), now, 100).wait_ms,
            RETRY_AFTER_MIN_DELAY_MS
        );
        assert_eq!(
            compute_backoff_delay_at(0, Some("0"), now, 100).wait_ms,
            RETRY_AFTER_MIN_DELAY_MS
        );
    }

    #[test]
    fn retry_after_rejects_non_finite_and_over_budget_values() {
        let now = SystemTime::UNIX_EPOCH;
        let invalid = compute_backoff_delay_at(0, Some("NaN"), now, 100);
        let too_long = compute_backoff_delay_at(0, Some("120"), now, 100);

        assert_eq!(invalid.wait_ms, RETRY_INITIAL_DELAY_MS);
        assert!(too_long.exceeds_budget);
        assert_eq!(too_long.wait_ms, RETRY_MAX_DELAY_MS);
    }

    #[test]
    fn first_proven_pre_dispatch_failure_uses_one_fast_jitter_window() {
        let mut backoff = ModelRetryBackoff::default();
        let fast = backoff
            .next_delay(ReplaySafety::DefinitelyNotDispatched, None)
            .unwrap();
        assert!(
            (FAST_PRE_DISPATCH_MIN_DELAY_MS..=FAST_PRE_DISPATCH_MAX_DELAY_MS)
                .contains(&fast.wait_ms)
        );
        assert!(!fast.exceeds_budget);

        let standard = backoff
            .next_delay(ReplaySafety::DefinitelyNotDispatched, None)
            .unwrap();
        assert!((1_600..=2_400).contains(&standard.wait_ms));
    }

    #[test]
    fn explicit_responses_keep_standard_backoff_and_retry_after() {
        let mut backoff = ModelRetryBackoff::default();
        let overload = backoff
            .next_delay(ReplaySafety::ExplicitlyRetryableResponse, None)
            .unwrap();
        assert!((1_600..=2_400).contains(&overload.wait_ms));

        let retry_after = backoff
            .next_delay(ReplaySafety::ExplicitlyRetryableResponse, Some("0.25"))
            .unwrap();
        assert_eq!(retry_after.wait_ms, 250);
    }

    #[test]
    fn retry_after_supersedes_fast_predispatch_delay_and_unknown_never_replays() {
        let mut backoff = ModelRetryBackoff::default();
        let retry_after = backoff
            .next_delay(ReplaySafety::DefinitelyNotDispatched, Some("0.4"))
            .unwrap();
        assert_eq!(retry_after.wait_ms, 400);

        let next = backoff
            .next_delay(ReplaySafety::DefinitelyNotDispatched, None)
            .unwrap();
        assert!((3_200..=4_800).contains(&next.wait_ms));
        assert!(
            backoff
                .next_delay(ReplaySafety::OutcomeUnknown, None)
                .is_none()
        );
    }
}
