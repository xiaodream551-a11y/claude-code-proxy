use std::time::Duration;

const MODEL_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MODEL_POOL_MAX_IDLE_PER_HOST: usize = 8;
const MODEL_TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// Apply one transport policy to long-lived model clients.
///
/// Provider instances already share their Reqwest client across requests. A
/// longer idle pool and TCP keepalives preserve that benefit across tool calls
/// and interactive pauses, while adaptive HTTP/2 windows avoid making a large
/// Claude transcript advance through the default fixed receive window. Do not
/// set an HTTP/2 PING acknowledgement timeout here: a short transport timeout
/// would override the providers' deliberately longer body-idle tolerance during
/// a temporary network outage.
pub(crate) fn model_client_builder(connect_timeout: Duration) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .connect_timeout(connect_timeout)
        .pool_idle_timeout(MODEL_POOL_IDLE_TIMEOUT)
        .pool_max_idle_per_host(MODEL_POOL_MAX_IDLE_PER_HOST)
        .tcp_keepalive(MODEL_TCP_KEEPALIVE)
        .http2_adaptive_window(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_client_policy_builds_with_reusable_connections() {
        assert_eq!(MODEL_POOL_IDLE_TIMEOUT, Duration::from_secs(300));
        assert_eq!(MODEL_POOL_MAX_IDLE_PER_HOST, 8);
        model_client_builder(Duration::from_secs(1))
            .no_proxy()
            .build()
            .expect("model client policy should build");
    }
}
