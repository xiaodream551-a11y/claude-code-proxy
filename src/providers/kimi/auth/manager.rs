use std::sync::{Arc, Mutex, MutexGuard};

use super::constants::{CLIENT_ID, REFRESH_MARGIN_MS, oauth_host};
use super::headers::{common_headers, header_map};
use super::jwt::extract_user_id;
use super::login::TokenResponse;
use super::token_store::{KimiTokenStore, StoredAuth};
use crate::auth::AuthStorage;
use crate::timeutil::now_ms;

const MAX_REFRESH_ATTEMPTS: u32 = 3;
const RETRYABLE_STATUSES: &[u16] = &[429, 500, 502, 503, 504];

pub struct KimiAuthManager<S: AuthStorage<StoredAuth>> {
    pub store: KimiTokenStore<S>,
    cached: Arc<Mutex<Option<StoredAuth>>>,
}

impl<S: AuthStorage<StoredAuth>> KimiAuthManager<S> {
    pub fn new(store: KimiTokenStore<S>) -> Self {
        Self {
            store,
            cached: Arc::new(Mutex::new(None)),
        }
    }

    fn lock_cache(&self) -> Result<MutexGuard<'_, Option<StoredAuth>>, anyhow::Error> {
        self.cached.lock().map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub fn get_auth(&self) -> Result<StoredAuth, anyhow::Error> {
        let cached = self.lock_cache()?.clone();
        let stored = match cached {
            Some(auth) => auth,
            None => {
                let loaded = self.store.load_auth()?;
                match loaded {
                    Some(auth) => {
                        *self.lock_cache()? = Some(auth.clone());
                        auth
                    }
                    None => {
                        anyhow::bail!("Not authenticated. Run: claude-code-proxy kimi auth login");
                    }
                }
            }
        };

        if stored.expires > now_ms() + REFRESH_MARGIN_MS {
            return Ok(stored);
        }

        self.refresh_now(&stored)
    }

    pub fn force_refresh(&self) -> Result<StoredAuth, anyhow::Error> {
        let stored = match self.lock_cache()?.clone() {
            Some(auth) => auth,
            None => {
                let loaded = self.store.load_auth()?;
                loaded.ok_or_else(|| anyhow::anyhow!("Not authenticated"))?
            }
        };
        self.refresh_now(&stored)
    }

    fn refresh_now(&self, current: &StoredAuth) -> Result<StoredAuth, anyhow::Error> {
        if current.refresh.is_empty() {
            anyhow::bail!("No refresh token stored; re-authenticate");
        }

        let headers = common_headers()?;
        let client = reqwest::blocking::Client::new();

        for attempt in 0..MAX_REFRESH_ATTEMPTS {
            let form = [
                ("client_id", CLIENT_ID.to_string()),
                ("grant_type", "refresh_token".to_string()),
                ("refresh_token", current.refresh.clone()),
            ];

            let resp = match client
                .post(format!("{}/api/oauth/token", oauth_host()))
                .headers(header_map(&headers))
                .form(&form)
                .send()
            {
                Ok(r) => r,
                Err(err) => {
                    if attempt < MAX_REFRESH_ATTEMPTS - 1 {
                        let ms = 2u64.pow(attempt) * 1000;
                        std::thread::sleep(std::time::Duration::from_millis(ms));
                        continue;
                    }
                    anyhow::bail!("refresh network error: {err}");
                }
            };

            if resp.status().as_u16() == 200 {
                let tokens: TokenResponse = resp.json()?;
                let expires = now_ms() + (tokens.expires_in.unwrap_or(900) as u64 * 1000);
                let next = StoredAuth {
                    access: tokens.access_token.clone(),
                    refresh: tokens
                        .refresh_token
                        .unwrap_or_else(|| current.refresh.clone()),
                    expires,
                    scope: tokens.scope.clone(),
                    user_id: extract_user_id(&tokens.access_token)
                        .or_else(|| current.user_id.clone()),
                };
                self.store.save_auth(next.clone())?;
                *self.lock_cache()? = Some(next.clone());
                return Ok(next);
            }

            let status = resp.status().as_u16();
            if status == 401 || status == 403 {
                *self.lock_cache()? = None;
                let _ = self.store.clear_auth();
                let err_msg = resp
                    .text()
                    .unwrap_or_else(|_| "Token refresh unauthorized".to_string());
                anyhow::bail!("{err_msg}");
            }

            if !RETRYABLE_STATUSES.contains(&status) {
                anyhow::bail!("Token refresh failed: {status}");
            }

            if attempt < MAX_REFRESH_ATTEMPTS - 1 {
                let ms = 2u64.pow(attempt) * 1000;
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
        }

        anyhow::bail!("Token refresh failed after {MAX_REFRESH_ATTEMPTS} attempts");
    }

    pub fn persist_initial_tokens(
        &self,
        tokens: &TokenResponse,
    ) -> Result<StoredAuth, anyhow::Error> {
        let expires = now_ms() + (tokens.expires_in.unwrap_or(900) * 1000);
        let auth = StoredAuth {
            access: tokens.access_token.clone(),
            refresh: tokens.refresh_token.clone().unwrap_or_default(),
            expires,
            scope: tokens.scope.clone(),
            user_id: extract_user_id(&tokens.access_token),
        };
        self.store.save_auth(auth.clone())?;
        *self.lock_cache()? = Some(auth.clone());
        Ok(auth)
    }

    pub fn reset_cache(&self) {
        if let Ok(mut guard) = self.cached.lock() {
            *guard = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::InMemoryAuthStore;

    fn test_store() -> KimiTokenStore<InMemoryAuthStore<StoredAuth>> {
        KimiTokenStore::new(InMemoryAuthStore::new())
    }

    #[test]
    fn get_auth_returns_stored() {
        let store = test_store();
        let auth = StoredAuth {
            access: "test_access".into(),
            refresh: "test_refresh".into(),
            expires: 9999999999999,
            scope: Some("openid".into()),
            user_id: Some("user1".into()),
        };
        store.save_auth(auth.clone()).unwrap();
        let manager = KimiAuthManager::new(store);
        let result = manager.get_auth().unwrap();
        assert_eq!(result.access, "test_access");
        assert_eq!(result.user_id.as_deref(), Some("user1"));
    }

    #[test]
    fn get_auth_fails_when_no_auth() {
        let store = test_store();
        let manager = KimiAuthManager::new(store);
        assert!(manager.get_auth().is_err());
    }
}
