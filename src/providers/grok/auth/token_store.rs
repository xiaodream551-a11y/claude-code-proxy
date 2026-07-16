use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::auth::{AuthStorage, FileAuthStore};
use crate::paths;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredAuth {
    pub access: String,
    pub refresh: String,
    pub expires_at_ms: u64,
    pub issuer: String,
    pub client_id: String,
}

pub struct GrokTokenStore<S: AuthStorage<StoredAuth>> {
    store: S,
}

pub(crate) struct AuthMutationLock {
    _file: Option<File>,
}

impl AuthMutationLock {
    fn open(auth_path: &str) -> io::Result<Option<File>> {
        let Some(path) = mutation_lock_path(auth_path) else {
            return Ok(None);
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map(Some)
    }

    pub(crate) fn try_acquire(auth_path: &str) -> io::Result<Self> {
        let file = Self::open(auth_path)?;
        if let Some(file) = file.as_ref() {
            file.try_lock_exclusive()?;
        }
        Ok(Self { _file: file })
    }

    fn acquire(auth_path: &str) -> io::Result<Self> {
        let file = Self::open(auth_path)?;
        if let Some(file) = file.as_ref() {
            file.lock_exclusive()?;
        }
        Ok(Self { _file: file })
    }
}

fn filesystem_auth_path(auth_path: &str) -> Option<&Path> {
    let path = Path::new(auth_path);
    path.is_absolute().then_some(path)
}

fn mutation_lock_path(auth_path: &str) -> Option<PathBuf> {
    filesystem_auth_path(auth_path)
        .map(|path| PathBuf::from(format!("{}.refresh.lock", path.display())))
}

fn refresh_pending_path(auth_path: &str) -> Option<PathBuf> {
    filesystem_auth_path(auth_path)
        .map(|path| PathBuf::from(format!("{}.refresh-pending.json", path.display())))
}

fn clear_refresh_pending(auth_path: &str) -> io::Result<()> {
    let Some(path) = refresh_pending_path(auth_path) else {
        return Ok(());
    };
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

impl<S: AuthStorage<StoredAuth>> GrokTokenStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
    pub fn load_auth(&self) -> anyhow::Result<Option<StoredAuth>> {
        self.store.load()
    }
    pub fn save_auth(&self, auth: StoredAuth) -> anyhow::Result<()> {
        self.store.save(auth)
    }
    pub fn save_auth_exclusive(&self, auth: StoredAuth) -> anyhow::Result<()> {
        let _lock = AuthMutationLock::acquire(&self.auth_path())?;
        self.save_auth(auth)?;
        clear_refresh_pending(&self.auth_path())?;
        Ok(())
    }
    pub fn clear_auth(&self) -> anyhow::Result<()> {
        self.store.clear()
    }
    pub fn clear_auth_exclusive(&self) -> anyhow::Result<()> {
        let _lock = AuthMutationLock::acquire(&self.auth_path())?;
        self.clear_auth()?;
        clear_refresh_pending(&self.auth_path())?;
        Ok(())
    }
    pub fn auth_path(&self) -> String {
        self.store.path()
    }
}

pub fn file_store() -> GrokTokenStore<FileAuthStore<StoredAuth>> {
    let primary = paths::provider_auth_file("grok");
    let legacy = paths::provider_legacy_auth_file("grok");
    GrokTokenStore::new(FileAuthStore::new(
        primary.to_string_lossy().into_owned(),
        legacy.to_string_lossy().into_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn auth(access: &str) -> StoredAuth {
        StoredAuth {
            access: access.to_string(),
            refresh: format!("{access}-refresh"),
            expires_at_ms: 1,
            issuer: "https://auth.x.ai".to_string(),
            client_id: "client".to_string(),
        }
    }

    #[test]
    fn explicit_auth_mutation_waits_for_the_refresh_file_lock() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("grok/auth.json");
        let path = path.to_string_lossy().into_owned();
        let store = GrokTokenStore::new(FileAuthStore::new(path.clone(), path.clone()));
        store.save_auth(auth("old")).unwrap();
        let held = AuthMutationLock::acquire(&path).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let thread_path = path.clone();
        let writer = std::thread::spawn(move || {
            let store =
                GrokTokenStore::new(FileAuthStore::new(thread_path.clone(), thread_path.clone()));
            started_tx.send(()).unwrap();
            let result = store.save_auth_exclusive(auth("new"));
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
}
