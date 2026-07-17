use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Cross-process guard for OAuth credential mutations.
///
/// The lock file is deliberately separate from the credential backend. That
/// lets Keychain-backed stores coordinate through a small file without
/// exposing credentials on disk.
pub struct AuthMutationLock {
    _file: Option<File>,
}

impl AuthMutationLock {
    fn open(coordination_path: Option<&Path>) -> io::Result<Option<File>> {
        let Some(coordination_path) = coordination_path else {
            return Ok(None);
        };
        let path = mutation_lock_path(coordination_path);
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

    pub fn acquire(coordination_path: Option<&Path>) -> io::Result<Self> {
        let file = Self::open(coordination_path)?;
        if let Some(file) = file.as_ref() {
            file.lock_exclusive()?;
        }
        Ok(Self { _file: file })
    }

    pub async fn acquire_async(coordination_path: Option<&Path>) -> io::Result<Self> {
        loop {
            let file = Self::open(coordination_path)?;
            match file.as_ref().map(FileExt::try_lock_exclusive) {
                Some(Err(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    drop(file);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                }
                Some(Err(error)) => return Err(error),
                Some(Ok(())) | None => return Ok(Self { _file: file }),
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RefreshPendingMarker {
    auth_generation_sha256: String,
}

pub fn generation_fingerprint<T: Serialize>(value: &T) -> anyhow::Result<[u8; 32]> {
    let encoded = serde_json::to_vec(value)?;
    Ok(Sha256::digest(encoded).into())
}

pub fn write_refresh_pending(
    coordination_path: Option<&Path>,
    generation: [u8; 32],
) -> anyhow::Result<()> {
    let Some(path) = coordination_path.map(refresh_pending_path) else {
        return Ok(());
    };
    crate::auth::write_atomically(
        path.to_string_lossy().as_ref(),
        &RefreshPendingMarker {
            auth_generation_sha256: hex::encode(generation),
        },
    )
}

pub fn read_refresh_pending(coordination_path: Option<&Path>) -> anyhow::Result<Option<[u8; 32]>> {
    let Some(path) = coordination_path.map(refresh_pending_path) else {
        return Ok(None);
    };
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let marker: RefreshPendingMarker = serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("OAuth refresh pending marker is invalid: {error}"))?;
    let decoded = hex::decode(marker.auth_generation_sha256)
        .map_err(|error| anyhow::anyhow!("OAuth refresh pending marker is invalid: {error}"))?;
    decoded
        .try_into()
        .map(Some)
        .map_err(|_| anyhow::anyhow!("OAuth refresh pending marker has an invalid fingerprint"))
}

pub fn clear_refresh_pending(coordination_path: Option<&Path>) -> anyhow::Result<()> {
    let Some(path) = coordination_path.map(refresh_pending_path) else {
        return Ok(());
    };
    match std::fs::remove_file(&path) {
        Ok(()) => {
            #[cfg(unix)]
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn mutation_lock_path(coordination_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.refresh.lock", coordination_path.display()))
}

pub fn refresh_pending_path(coordination_path: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}.refresh-pending.json",
        coordination_path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_marker_roundtrips_and_clears() {
        let temp = tempfile::TempDir::new().unwrap();
        let auth_path = temp.path().join("codex/auth.json");
        let generation = [7_u8; 32];

        write_refresh_pending(Some(&auth_path), generation).unwrap();
        assert_eq!(
            read_refresh_pending(Some(&auth_path)).unwrap(),
            Some(generation)
        );
        clear_refresh_pending(Some(&auth_path)).unwrap();
        assert_eq!(read_refresh_pending(Some(&auth_path)).unwrap(), None);
    }
}
