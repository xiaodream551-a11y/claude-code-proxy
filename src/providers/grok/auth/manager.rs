use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use tokio::sync::Mutex;
use url::Url;

use super::login::{CANONICAL_ISSUER, CLIENT_ID};
use super::token_store::{GrokTokenStore, StoredAuth};
use crate::auth::AuthStorage;

const REFRESH_SKEW_MS: u64 = 5 * 60 * 1000;

#[derive(Deserialize)]
struct Discovery {
    issuer: String,
    token_endpoint: String,
}

#[derive(Deserialize)]
struct RefreshResponse {
    access_token: String,
    expires_in: u64,
    #[serde(default)]
    refresh_token: Option<String>,
}

#[derive(Deserialize)]
struct OAuthErrorResponse {
    error: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrokAuthErrorKind {
    CredentialsInvalid,
    Temporary,
    RateLimited,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrokAuthError {
    #[error("Grok credentials are invalid: {message}")]
    CredentialsInvalid { message: String },
    #[error("Grok authentication is temporarily unavailable: {message}")]
    Temporary {
        message: String,
        safe_to_retry: bool,
    },
    #[error("Grok authentication is rate limited: {message}")]
    RateLimited {
        message: String,
        retry_after: Option<String>,
        safe_to_retry: bool,
    },
}

impl GrokAuthError {
    pub fn credentials_invalid(message: impl Into<String>) -> Self {
        Self::CredentialsInvalid {
            message: message.into(),
        }
    }

    fn temporary(message: impl Into<String>) -> Self {
        Self::Temporary {
            message: message.into(),
            safe_to_retry: false,
        }
    }

    fn retryable_temporary(message: impl Into<String>) -> Self {
        Self::Temporary {
            message: message.into(),
            safe_to_retry: true,
        }
    }

    fn rate_limited(message: impl Into<String>, retry_after: Option<String>) -> Self {
        Self::RateLimited {
            message: message.into(),
            retry_after,
            safe_to_retry: true,
        }
    }

    pub fn kind(&self) -> GrokAuthErrorKind {
        match self {
            Self::CredentialsInvalid { .. } => GrokAuthErrorKind::CredentialsInvalid,
            Self::Temporary { .. } => GrokAuthErrorKind::Temporary,
            Self::RateLimited { .. } => GrokAuthErrorKind::RateLimited,
        }
    }

    pub fn retry_after(&self) -> Option<&str> {
        match self {
            Self::RateLimited { retry_after, .. } => retry_after.as_deref(),
            _ => None,
        }
    }

    pub fn safe_to_retry(&self) -> bool {
        match self {
            Self::CredentialsInvalid { .. } => false,
            Self::Temporary { safe_to_retry, .. } | Self::RateLimited { safe_to_retry, .. } => {
                *safe_to_retry
            }
        }
    }
}

pub struct GrokAuthManager<S: AuthStorage<StoredAuth>> {
    store: GrokTokenStore<S>,
    client: reqwest::Client,
    refresh_lock: Arc<Mutex<()>>,
    issuer: String,
    require_https: bool,
}

impl<S: AuthStorage<StoredAuth>> GrokAuthManager<S> {
    pub fn new(store: GrokTokenStore<S>) -> anyhow::Result<Self> {
        Self::with_issuer(store, CANONICAL_ISSUER.into(), true)
    }

    fn with_issuer(
        store: GrokTokenStore<S>,
        issuer: String,
        require_https: bool,
    ) -> anyhow::Result<Self> {
        let issuer = issuer.trim_end_matches('/').to_string();
        let issuer_url = Url::parse(&issuer)?;
        if require_https && issuer_url.scheme() != "https" {
            anyhow::bail!("Grok OAuth issuer must use HTTPS");
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(20))
            .build()?;
        Ok(Self {
            store,
            client,
            refresh_lock: Arc::new(Mutex::new(())),
            issuer,
            require_https,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(store: GrokTokenStore<S>, issuer: String) -> anyhow::Result<Self> {
        Self::with_issuer(store, issuer, false)
    }

    pub fn store(&self) -> &GrokTokenStore<S> {
        &self.store
    }

    pub async fn get_auth(&self) -> Result<StoredAuth, GrokAuthError> {
        let auth = self
            .store
            .load_auth()
            .map_err(|error| {
                GrokAuthError::temporary(format!("failed to load stored credentials: {error}"))
            })?
            .ok_or_else(|| GrokAuthError::credentials_invalid("not authenticated"))?;
        if auth.expires_at_ms > now_ms().saturating_add(REFRESH_SKEW_MS) {
            return Ok(auth);
        }
        self.refresh(false, None).await
    }

    pub async fn force_refresh(&self, rejected_access: &str) -> Result<StoredAuth, GrokAuthError> {
        self.refresh(true, Some(rejected_access)).await
    }

    async fn refresh(
        &self,
        force: bool,
        rejected_access: Option<&str>,
    ) -> Result<StoredAuth, GrokAuthError> {
        let _guard = self.refresh_lock.lock().await;
        let auth = self
            .store
            .load_auth()
            .map_err(|error| {
                GrokAuthError::temporary(format!("failed to load stored credentials: {error}"))
            })?
            .ok_or_else(|| GrokAuthError::credentials_invalid("not authenticated"))?;
        if (!force && auth.expires_at_ms > now_ms().saturating_add(REFRESH_SKEW_MS))
            || rejected_access.is_some_and(|access| auth.access != access)
        {
            return Ok(auth);
        }
        if auth.issuer.trim_end_matches('/') != self.issuer || auth.client_id != CLIENT_ID {
            return Err(GrokAuthError::credentials_invalid(
                "unsupported OAuth session; re-authenticate",
            ));
        }
        if auth.refresh.is_empty() {
            return Err(GrokAuthError::credentials_invalid(
                "no refresh token is stored; re-authenticate",
            ));
        }
        let issuer = Url::parse(&self.issuer).map_err(|error| {
            GrokAuthError::temporary(format!("configured OAuth issuer is invalid: {error}"))
        })?;
        let discovery_url = issuer
            .join("/.well-known/openid-configuration")
            .map_err(|error| {
                GrokAuthError::temporary(format!("failed to construct discovery URL: {error}"))
            })?;
        let discovery_response = self
            .client
            .get(discovery_url)
            .send()
            .await
            .map_err(|error| {
                GrokAuthError::retryable_temporary(format!(
                    "OIDC discovery request failed: {error}"
                ))
            })?;
        let discovery_status = discovery_response.status();
        if discovery_status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(GrokAuthError::rate_limited(
                "OIDC discovery was rate limited",
                retry_after(discovery_response.headers()),
            ));
        }
        if !discovery_status.is_success() {
            let message = format!(
                "OIDC discovery failed with status {}",
                discovery_status.as_u16()
            );
            return Err(
                if matches!(discovery_status.as_u16(), 500 | 502 | 503 | 504) {
                    GrokAuthError::retryable_temporary(message)
                } else {
                    GrokAuthError::temporary(message)
                },
            );
        }
        let discovery: Discovery = discovery_response.json().await.map_err(|error| {
            GrokAuthError::temporary(format!("OIDC discovery response is invalid: {error}"))
        })?;
        if discovery.issuer.trim_end_matches('/') != self.issuer {
            return Err(GrokAuthError::temporary("OIDC discovery issuer mismatch"));
        }
        let endpoint = Url::parse(&discovery.token_endpoint).map_err(|error| {
            GrokAuthError::temporary(format!("OIDC token endpoint is invalid: {error}"))
        })?;
        if (self.require_https && endpoint.scheme() != "https")
            || endpoint.origin() != issuer.origin()
        {
            return Err(GrokAuthError::temporary(
                "OIDC token endpoint is outside the canonical issuer",
            ));
        }
        let refresh_response = self
            .client
            .post(endpoint)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", auth.refresh.as_str()),
                ("client_id", auth.client_id.as_str()),
            ])
            .send()
            .await
            .map_err(|error| {
                let message = format!("token refresh request failed: {error}");
                if error.is_connect() {
                    GrokAuthError::retryable_temporary(message)
                } else {
                    GrokAuthError::temporary(message)
                }
            })?;
        let refresh_status = refresh_response.status();
        if refresh_status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(GrokAuthError::rate_limited(
                "token refresh was rate limited",
                retry_after(refresh_response.headers()),
            ));
        }
        if !refresh_status.is_success() {
            let oauth_error = if refresh_status == reqwest::StatusCode::BAD_REQUEST {
                refresh_response
                    .json::<OAuthErrorResponse>()
                    .await
                    .ok()
                    .map(|response| response.error)
            } else {
                None
            };
            let credentials_invalid = matches!(refresh_status.as_u16(), 401 | 403)
                || (refresh_status == reqwest::StatusCode::BAD_REQUEST
                    && oauth_error.as_deref().is_some_and(is_credential_error));
            if credentials_invalid {
                return Err(GrokAuthError::credentials_invalid(format!(
                    "token refresh was rejected with status {}",
                    refresh_status.as_u16()
                )));
            }
            return Err(GrokAuthError::temporary(format!(
                "token refresh failed with status {}",
                refresh_status.as_u16()
            )));
        }
        let refreshed: RefreshResponse = refresh_response.json().await.map_err(|error| {
            GrokAuthError::temporary(format!("token refresh response is invalid: {error}"))
        })?;
        if refreshed.access_token.is_empty() || refreshed.expires_in == 0 {
            return Err(GrokAuthError::temporary(
                "token refresh response omitted required fields",
            ));
        }
        let updated = StoredAuth {
            access: refreshed.access_token,
            refresh: refreshed
                .refresh_token
                .filter(|token| !token.is_empty())
                .unwrap_or(auth.refresh),
            expires_at_ms: now_ms().saturating_add(refreshed.expires_in.saturating_mul(1000)),
            issuer: auth.issuer,
            client_id: auth.client_id,
        };
        self.store.save_auth(updated.clone()).map_err(|error| {
            GrokAuthError::temporary(format!("failed to save refreshed credentials: {error}"))
        })?;
        Ok(updated)
    }
}

fn is_credential_error(error: &str) -> bool {
    matches!(
        error,
        "invalid_grant" | "invalid_client" | "unauthorized_client" | "invalid_token"
    )
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::InMemoryAuthStore;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    fn auth(access: &str) -> StoredAuth {
        StoredAuth {
            access: access.into(),
            refresh: "synthetic-refresh".into(),
            expires_at_ms: now_ms().saturating_add(3_600_000),
            issuer: CANONICAL_ISSUER.into(),
            client_id: "synthetic-client".into(),
        }
    }

    fn read_request(stream: &mut TcpStream) {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "expected an HTTP request");
            request.extend_from_slice(&buffer[..read]);
            if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        while request.len().saturating_sub(header_end) < content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "expected the complete HTTP request body");
            request.extend_from_slice(&buffer[..read]);
        }
    }

    fn spawn_refresh_server(
        token_status: u16,
        retry_after: Option<&str>,
        oauth_error: &str,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let server_issuer = issuer.clone();
        let retry_after = retry_after.map(str::to_string);
        let oauth_error = oauth_error.to_string();
        let server = thread::spawn(move || {
            let (mut discovery, _) = listener.accept().unwrap();
            read_request(&mut discovery);
            let body = serde_json::json!({
                "issuer": server_issuer.clone(),
                "token_endpoint": format!("{server_issuer}/oauth/token")
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            discovery.write_all(response.as_bytes()).unwrap();

            let (mut token, _) = listener.accept().unwrap();
            read_request(&mut token);
            let reason = match token_status {
                400 => "Bad Request",
                429 => "Too Many Requests",
                503 => "Service Unavailable",
                _ => "Test Status",
            };
            let body = serde_json::json!({"error": oauth_error}).to_string();
            let retry_header = retry_after
                .as_deref()
                .map(|value| format!("retry-after: {value}\r\n"))
                .unwrap_or_default();
            let response = format!(
                "HTTP/1.1 {token_status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{retry_header}connection: close\r\n\r\n{body}",
                body.len()
            );
            token.write_all(response.as_bytes()).unwrap();
        });
        (issuer, server)
    }

    async fn refresh_error(
        token_status: u16,
        retry_after: Option<&str>,
        oauth_error: &str,
    ) -> GrokAuthError {
        let (issuer, server) = spawn_refresh_server(token_status, retry_after, oauth_error);
        let store = GrokTokenStore::new(InMemoryAuthStore::new());
        store
            .save_auth(StoredAuth {
                access: "expired-access".into(),
                refresh: "expired-refresh".into(),
                expires_at_ms: 0,
                issuer: issuer.clone(),
                client_id: CLIENT_ID.into(),
            })
            .unwrap();
        let manager = GrokAuthManager::new_for_test(store, issuer).unwrap();
        let error = manager.get_auth().await.unwrap_err();
        server.join().unwrap();
        error
    }

    #[test]
    fn discovery_accepts_standard_metadata_fields() {
        let discovery: Discovery = serde_json::from_value(serde_json::json!({
            "issuer": CANONICAL_ISSUER,
            "token_endpoint": "https://auth.x.ai/oauth/token",
            "authorization_endpoint": "https://auth.x.ai/oauth/authorize",
            "jwks_uri": "https://auth.x.ai/.well-known/jwks.json"
        }))
        .unwrap();

        assert_eq!(discovery.issuer, CANONICAL_ISSUER);
    }

    #[tokio::test]
    async fn concurrent_stale_401_refreshes_reuse_the_rotated_access_token() {
        let store = GrokTokenStore::new(InMemoryAuthStore::new());
        store.save_auth(auth("rotated-access")).unwrap();
        let manager = Arc::new(GrokAuthManager::new(store).unwrap());
        let (first, second) = tokio::join!(
            manager.force_refresh("rejected-access"),
            manager.force_refresh("rejected-access"),
        );
        assert_eq!(first.unwrap().access, "rotated-access");
        assert_eq!(second.unwrap().access, "rotated-access");
    }

    #[tokio::test]
    async fn rejected_refresh_is_classified_as_invalid_credentials() {
        let error = refresh_error(400, None, "invalid_grant").await;

        assert_eq!(error.kind(), GrokAuthErrorKind::CredentialsInvalid);
        assert_eq!(error.retry_after(), None);
    }

    #[tokio::test]
    async fn unavailable_refresh_service_is_classified_as_temporary() {
        let error = refresh_error(503, None, "temporarily_unavailable").await;

        assert_eq!(error.kind(), GrokAuthErrorKind::Temporary);
        assert_eq!(error.retry_after(), None);
    }

    #[tokio::test]
    async fn refresh_rate_limit_preserves_retry_after() {
        let error = refresh_error(429, Some("17"), "slow_down").await;

        assert_eq!(error.kind(), GrokAuthErrorKind::RateLimited);
        assert_eq!(error.retry_after(), Some("17"));
    }

    #[tokio::test]
    async fn unrelated_bad_request_is_not_misclassified_as_invalid_credentials() {
        let error = refresh_error(400, None, "invalid_request").await;

        assert_eq!(error.kind(), GrokAuthErrorKind::Temporary);
        assert_eq!(error.retry_after(), None);
    }
}
