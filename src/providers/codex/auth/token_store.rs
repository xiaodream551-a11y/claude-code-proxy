use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::auth::{AuthStorage, KeychainFileAuthStore, SystemKeychain};
use crate::oauth_rotation::{AuthMutationLock, clear_refresh_pending};
use crate::paths;

pub const KEYCHAIN_SERVICE: &str = "claude-code-proxy.codex";
pub const KEYCHAIN_ACCOUNT: &str = "auth";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredAuth {
    pub access: String,
    pub refresh: String,
    pub expires: u64,
    #[serde(
        default,
        rename = "accountId",
        alias = "account_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub account_id: Option<String>,
}

pub struct CodexTokenStore<S: AuthStorage<StoredAuth>> {
    store: S,
}

impl<S: AuthStorage<StoredAuth>> CodexTokenStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn load_auth(&self) -> Result<Option<StoredAuth>, anyhow::Error> {
        self.store.load()
    }

    pub fn save_auth(&self, value: StoredAuth) -> Result<(), anyhow::Error> {
        self.store.save(value)
    }

    pub fn save_auth_exclusive(&self, value: StoredAuth) -> Result<(), anyhow::Error> {
        let coordination_path = self.coordination_path();
        let _lock = AuthMutationLock::acquire(coordination_path.as_deref())?;
        self.save_auth(value)?;
        clear_refresh_pending(coordination_path.as_deref())
    }

    pub fn clear_auth(&self) -> Result<(), anyhow::Error> {
        self.store.clear()
    }

    pub fn clear_auth_exclusive(&self) -> Result<(), anyhow::Error> {
        let coordination_path = self.coordination_path();
        let _lock = AuthMutationLock::acquire(coordination_path.as_deref())?;
        self.clear_auth()?;
        clear_refresh_pending(coordination_path.as_deref())
    }

    pub fn auth_path(&self) -> String {
        self.store.path()
    }

    pub fn coordination_path(&self) -> Option<PathBuf> {
        self.store.coordination_path()
    }
}

pub type DefaultCodexAuthStore = KeychainFileAuthStore<StoredAuth, SystemKeychain>;

pub fn file_store() -> CodexTokenStore<DefaultCodexAuthStore> {
    let primary = paths::provider_auth_file("codex");
    let legacy = paths::provider_legacy_auth_file("codex");
    let store = KeychainFileAuthStore::new(
        primary.to_string_lossy().to_string(),
        legacy.to_string_lossy().to_string(),
        KEYCHAIN_SERVICE,
        KEYCHAIN_ACCOUNT,
        use_macos_keychain(),
        SystemKeychain,
    );
    CodexTokenStore::new(store)
}

fn use_macos_keychain() -> bool {
    cfg!(target_os = "macos") && std::env::var_os("CCP_CONFIG_DIR").is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{FileAuthStore, InMemoryAuthStore};
    use serde_json::json;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn stored_auth_reads_account_id_alias() {
        let auth: StoredAuth = serde_json::from_value(json!({
            "access": "a",
            "refresh": "r",
            "expires": 123,
            "accountId": "acct"
        }))
        .unwrap();
        assert_eq!(auth.account_id.as_deref(), Some("acct"));
    }

    #[test]
    fn stored_auth_writes_account_id_key() {
        let auth = StoredAuth {
            access: "a".into(),
            refresh: "r".into(),
            expires: 4102444800000,
            account_id: Some("acct_1".into()),
        };
        let value = serde_json::to_value(auth).unwrap();
        assert_eq!(value["accountId"], "acct_1");
        assert!(value.get("account_id").is_none());
    }

    #[test]
    fn stored_auth_roundtrip() {
        let store = CodexTokenStore::new(InMemoryAuthStore::new());
        let auth = StoredAuth {
            access: "token".into(),
            refresh: "refresh".into(),
            expires: 9999999999999,
            account_id: Some("acct_1".into()),
        };
        store.save_auth(auth.clone()).unwrap();
        let loaded = store.load_auth().unwrap().unwrap();
        assert_eq!(loaded.access, "token");
        assert_eq!(loaded.account_id.as_deref(), Some("acct_1"));
    }

    #[test]
    fn explicit_auth_mutation_waits_for_rotation_lock() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("codex/auth.json");
        let path_string = path.to_string_lossy().into_owned();
        let store =
            CodexTokenStore::new(FileAuthStore::new(path_string.clone(), path_string.clone()));
        let held = AuthMutationLock::acquire(Some(&path)).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let writer_path = path_string.clone();
        let writer = std::thread::spawn(move || {
            let store = CodexTokenStore::new(FileAuthStore::new(writer_path.clone(), writer_path));
            started_tx.send(()).unwrap();
            let result = store.save_auth_exclusive(StoredAuth {
                access: "new".into(),
                refresh: "new-refresh".into(),
                expires: u64::MAX,
                account_id: None,
            });
            done_tx.send(result).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(held);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        writer.join().unwrap();
        assert_eq!(store.load_auth().unwrap().unwrap().access, "new");
    }

    #[test]
    fn exclusive_logout_clears_auth_and_pending_marker() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("codex/auth.json");
        let path_string = path.to_string_lossy().into_owned();
        let store = CodexTokenStore::new(FileAuthStore::new(path_string.clone(), path_string));
        let auth = StoredAuth {
            access: "old".into(),
            refresh: "old-refresh".into(),
            expires: 0,
            account_id: None,
        };
        store.save_auth(auth.clone()).unwrap();
        crate::oauth_rotation::write_refresh_pending(
            store.coordination_path().as_deref(),
            crate::oauth_rotation::generation_fingerprint(&auth).unwrap(),
        )
        .unwrap();

        store.clear_auth_exclusive().unwrap();

        assert!(store.load_auth().unwrap().is_none());
        assert!(
            crate::oauth_rotation::read_refresh_pending(store.coordination_path().as_deref())
                .unwrap()
                .is_none()
        );
    }
}
