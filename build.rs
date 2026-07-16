use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src");
    if let Some(head_path) = git(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");
    }
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"])
        && let Some(reference_path) = git(&["rev-parse", "--git-path", &reference])
    {
        println!("cargo:rerun-if-changed={reference_path}");
    }

    let sha = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let dirty = git(&["status", "--porcelain", "--untracked-files=normal"])
        .is_some_and(|status| !status.is_empty());
    let built_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    println!("cargo:rustc-env=CCPROXY_GIT_SHA={sha}");
    println!("cargo:rustc-env=CCPROXY_GIT_DIRTY={dirty}");
    println!("cargo:rustc-env=CCPROXY_BUILD_UNIX_EPOCH={built_at}");
}
