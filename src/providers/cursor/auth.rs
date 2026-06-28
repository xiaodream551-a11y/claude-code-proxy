use std::collections::HashMap;

/// Load the Cursor auth token from environment variables or auth file.
///
/// Resolution order:
/// 1. `CCP_CURSOR_AUTH_TOKEN` env var
/// 2. `CURSOR_AUTH_TOKEN` env var
/// 3. Proxy auth file at `<config_dir>/cursor/auth.json` (accessToken field)
/// 4. Legacy auth file at `<legacy_config_dir>/cursor/auth.json`
pub fn load_cursor_token() -> Option<String> {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(token) = env.get("CCP_CURSOR_AUTH_TOKEN") {
        if !token.is_empty() {
            return Some(token.clone());
        }
    }
    if let Some(token) = env.get("CURSOR_AUTH_TOKEN") {
        if !token.is_empty() {
            return Some(token.clone());
        }
    }

    let path = crate::paths::provider_auth_file("cursor");
    let legacy = crate::paths::provider_legacy_auth_file("cursor");

    if path == legacy {
        return load_token_from_file(&path);
    }
    load_token_from_file(&path).or_else(|| load_token_from_file(&legacy))
}

fn load_token_from_file(path: &std::path::Path) -> Option<String> {
    let value: serde_json::Value = crate::auth::load_auth_file_value(path)?;
    // Try accessToken first, then token
    if let Some(token) = value.get("accessToken").and_then(|v| v.as_str()) {
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    if let Some(token) = value.get("token").and_then(|v| v.as_str()) {
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    // If the file contains a plain string token
    if let Some(token) = value.as_str() {
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::sync::Mutex;

    static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    #[test]
    fn auth_uses_cursor_auth_token_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("CURSOR_AUTH_TOKEN", "tok_from_cursor");
            std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        }
        assert_eq!(load_cursor_token().as_deref(), Some("tok_from_cursor"));
        unsafe {
            std::env::remove_var("CURSOR_AUTH_TOKEN");
        }
    }

    #[test]
    fn auth_prioritizes_ccp_env_over_cursor_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "tok_ccp");
            std::env::set_var("CURSOR_AUTH_TOKEN", "tok_cursor");
        }
        assert_eq!(load_cursor_token().as_deref(), Some("tok_ccp"));
        unsafe {
            std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
            std::env::remove_var("CURSOR_AUTH_TOKEN");
        }
    }

    #[test]
    fn auth_returns_none_when_not_set() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
            std::env::remove_var("CURSOR_AUTH_TOKEN");
        }
        assert!(load_cursor_token().is_none());
    }
}
