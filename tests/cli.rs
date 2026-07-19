use assert_cmd::Command;
use predicates::str::contains;
use std::env;
use tempfile::TempDir;

#[test]
fn version_aliases_print_expected_version() -> Result<(), Box<dyn std::error::Error>> {
    let expected = format!("claude-code-proxy {}", env!("CARGO_PKG_VERSION"));

    for arg in ["--version", "-v", "version"] {
        let mut cmd = Command::cargo_bin("claude-code-proxy")?;
        cmd.arg(arg)
            .assert()
            .success()
            .stdout(contains(expected.clone()));
    }
    Ok(())
}

#[test]
fn version_json_is_machine_readable() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    let output = cmd.args(["version", "--json"]).output()?;
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
    assert!(value["binarySha256"].as_str().is_some());
    Ok(())
}

#[cfg(unix)]
#[test]
fn version_json_ignores_unrelated_non_utf8_environment_values()
-> Result<(), Box<dyn std::error::Error>> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    let output = cmd
        .args(["version", "--json"])
        .env("CCP_NON_UTF8_TEST", OsString::from_vec(vec![0xff, 0xfe]))
        .output()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
    Ok(())
}

#[test]
fn models_prints_all_providers() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    cmd.arg("models");
    let out = String::from_utf8(cmd.output()?.stdout)?;
    assert!(out.contains("codex:"));
    assert!(out.contains("kimi:"));
    assert!(out.contains("cursor:"));

    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    cmd.args(["models", "--full"]);
    cmd.output()?;
    Ok(())
}

#[test]
fn help_discovers_serverless_tui_demo() -> Result<(), Box<dyn std::error::Error>> {
    Command::cargo_bin("claude-code-proxy")?
        .arg("--help")
        .assert()
        .success()
        .stdout(contains("demo"))
        .stdout(contains("mock data and no proxy server"));
    Ok(())
}

#[test]
fn invalid_command_exits_two() -> Result<(), Box<dyn std::error::Error>> {
    Command::cargo_bin("claude-code-proxy")?
        .arg("definitely-not-a-command")
        .assert()
        .failure()
        .code(2);
    Ok(())
}

#[test]
fn unsupported_provider_auth_command_exits_two() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    cmd.args(["cursor", "auth", "device"]);
    let output = cmd.output()?;
    assert_eq!(output.status.code(), Some(2));
    let out = String::from_utf8(output.stderr)?;
    assert!(out.contains("not yet implemented") || out.contains("unsupported"));
    Ok(())
}

#[test]
fn provider_logout_without_auth_is_success() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    cmd.args(["kimi", "auth", "logout"]);
    cmd.env("CCP_CONFIG_DIR", temp.path());
    cmd.assert().success();
    Ok(())
}

#[test]
fn models_output_is_stable_order() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    cmd.args(["models", "--full"]);
    let output = cmd.output()?;
    let out = String::from_utf8(output.stdout)?;
    let codex_pos = out.find("codex:").unwrap_or(0);
    let kimi_pos = out.find("kimi:").unwrap_or(0);
    let cursor_pos = out.find("cursor:").unwrap_or(0);
    assert!(codex_pos < kimi_pos);
    assert!(kimi_pos < cursor_pos);
    Ok(())
}

#[test]
fn kimi_auth_status_reads_stored_auth() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("kimi");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("auth.json"),
        r#"{"access":"a","refresh":"r","expires":4102444800000,"scope":"openid","userId":"u"}"#,
    )?;
    let mut cmd = Command::cargo_bin("claude-code-proxy")?;
    cmd.args(["kimi", "auth", "status"]);
    cmd.env("CCP_CONFIG_DIR", temp.path());
    cmd.assert().success().stdout(contains("User: u"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn co_execs_claude_with_gpt_profile_and_forwards_arguments()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = ClaudeLauncherFixture::new("co")?;
    let mut cmd = Command::new(&fixture.shortcut);
    cmd.args(["--effort", "max", "hello world"])
        .env("PATH", fixture.path_env())
        .env("PORT", "19876")
        .env("CCP_BIND_ADDRESS", "0.0.0.0");

    cmd.assert()
        .success()
        .stdout(contains("base=http://127.0.0.1:19876"))
        .stdout(contains("main=gpt-5.6-sol"))
        .stdout(contains("fable=gpt-5.6-sol"))
        .stdout(contains("sonnet=gpt-5.6-terra"))
        .stdout(contains("haiku=gpt-5.6-luna"))
        .stdout(contains("small=gpt-5.6-luna"))
        .stdout(contains("max_context=272000"))
        .stdout(contains("compact_window=272000"))
        .stdout(contains("compact_pct=90"))
        .stdout(contains("disable_1m=1"))
        .stdout(contains("max_retries=1"))
        .stdout(contains("tool_concurrency=10"))
        .stdout(contains("tool_search=true"))
        .stdout(contains("arg=<--settings>"))
        .stdout(contains("arg=<--agents>"))
        .stdout(contains("\"Explore\":{\"model\":\"gpt-5.6-luna\"}"))
        .stdout(contains("\"effortLevel\":\"xhigh\""))
        .stdout(contains("\"model\":\"gpt-5.6-sol\""))
        .stdout(contains("\"ultracode\":true"))
        .stdout(contains("arg=<--effort>"))
        .stdout(contains("arg=<max>"))
        .stdout(contains("arg=<hello world>"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn cg_execs_claude_with_grok_profile_and_preserves_exit_code()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = ClaudeLauncherFixture::new("cg")?;
    let mut cmd = Command::new(&fixture.shortcut);
    cmd.args(["--continue"])
        .env("PATH", fixture.path_env())
        .env("PORT", "19877")
        .env("FAKE_CLAUDE_EXIT_CODE", "37");

    cmd.assert()
        .failure()
        .code(37)
        .stdout(contains("base=http://127.0.0.1:19877"))
        .stdout(contains("main=grok-4.5-high"))
        .stdout(contains("fable=grok-4.5-high"))
        .stdout(contains("opus=grok-4.5-high"))
        .stdout(contains("sonnet=grok-4.5-high"))
        .stdout(contains("haiku=grok-4.5-medium"))
        .stdout(contains("small=grok-4.5-medium"))
        .stdout(contains("max_context=500000"))
        .stdout(contains("compact_window=500000"))
        .stdout(contains("compact_pct=90"))
        .stdout(contains("disable_1m=1"))
        .stdout(contains("max_retries=1"))
        .stdout(contains("tool_concurrency=10"))
        .stdout(contains("tool_search=true"))
        .stdout(contains("arg=<--settings>"))
        .stdout(contains("arg=<--agents>"))
        .stdout(contains("\"Explore\":{\"model\":\"grok-4.5-medium\"}"))
        .stdout(contains("\"effortLevel\":\"high\""))
        .stdout(contains("\"model\":\"grok-4.5-high\""))
        .stdout(contains("\"ultracode\":false"))
        .stdout(contains("arg=<--continue>"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn profile_shortcuts_reject_cross_family_models_and_settings_overrides()
-> Result<(), Box<dyn std::error::Error>> {
    let grok = ClaudeLauncherFixture::new("cg")?;
    Command::new(&grok.shortcut)
        .args(["--model", "gpt-5.6-sol"])
        .env("PATH", grok.path_env())
        .assert()
        .failure()
        .stderr(contains("outside the Grok launch profile"));

    let gpt = ClaudeLauncherFixture::new("co")?;
    Command::new(&gpt.shortcut)
        .args(["--settings", "{}"])
        .env("PATH", gpt.path_env())
        .assert()
        .failure()
        .stderr(contains("--settings is disabled"));
    Ok(())
}

#[cfg(unix)]
struct ClaudeLauncherFixture {
    _temp: TempDir,
    shortcut: std::path::PathBuf,
    bin_dir: std::path::PathBuf,
}

#[cfg(unix)]
impl ClaudeLauncherFixture {
    fn new(shortcut_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let temp = TempDir::new()?;
        let bin_dir = temp.path().to_path_buf();
        let fake_claude = bin_dir.join("claude");
        std::fs::write(
            &fake_claude,
            r#"#!/bin/sh
printf 'base=%s\n' "$ANTHROPIC_BASE_URL"
printf 'main=%s\n' "$ANTHROPIC_MODEL"
printf 'fable=%s\n' "$ANTHROPIC_DEFAULT_FABLE_MODEL"
printf 'opus=%s\n' "$ANTHROPIC_DEFAULT_OPUS_MODEL"
printf 'sonnet=%s\n' "$ANTHROPIC_DEFAULT_SONNET_MODEL"
printf 'haiku=%s\n' "$ANTHROPIC_DEFAULT_HAIKU_MODEL"
printf 'small=%s\n' "$ANTHROPIC_SMALL_FAST_MODEL"
printf 'max_context=%s\n' "$CLAUDE_CODE_MAX_CONTEXT_TOKENS"
printf 'compact_window=%s\n' "$CLAUDE_CODE_AUTO_COMPACT_WINDOW"
printf 'compact_pct=%s\n' "$CLAUDE_AUTOCOMPACT_PCT_OVERRIDE"
printf 'disable_1m=%s\n' "$CLAUDE_CODE_DISABLE_1M_CONTEXT"
printf 'max_retries=%s\n' "$CLAUDE_CODE_MAX_RETRIES"
printf 'tool_concurrency=%s\n' "$CLAUDE_CODE_MAX_TOOL_USE_CONCURRENCY"
printf 'tool_search=%s\n' "$ENABLE_TOOL_SEARCH"
for arg in "$@"; do
  printf 'arg=<%s>\n' "$arg"
done
exit "${FAKE_CLAUDE_EXIT_CODE:-0}"
"#,
        )?;
        let mut permissions = std::fs::metadata(&fake_claude)?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_claude, permissions)?;

        let shortcut = bin_dir.join(shortcut_name);
        symlink(
            assert_cmd::cargo::cargo_bin!("claude-code-proxy"),
            &shortcut,
        )?;
        Ok(Self {
            _temp: temp,
            shortcut,
            bin_dir,
        })
    }

    fn path_env(&self) -> std::ffi::OsString {
        let mut paths = vec![self.bin_dir.clone()];
        if let Some(existing) = env::var_os("PATH") {
            paths.extend(env::split_paths(&existing));
        }
        env::join_paths(paths).expect("test PATH must be valid")
    }
}
