use anyhow::{Context, Result};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use claude_code_proxy::{
    config, logging,
    monitor::MonitorHandle,
    paths,
    registry::{ANTHROPIC_STYLE_ALIASES, Registry},
    server::{self, ServerConfig},
    tui::{self, MonitorExit, MonitorUiConfig},
};
use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const CODEX_XHIGH_AS_MAX_HEADER_NAME: &str = "x-ccproxy-codex-xhigh-as-max";
const CODEX_XHIGH_AS_MAX_HEADER: &str = "x-ccproxy-codex-xhigh-as-max: 1";
const EXPLORE_AGENT_DESCRIPTION: &str = "Fast, focused, read-only codebase exploration and search. Use proactively to locate files, trace code paths, understand architecture, and gather evidence before implementation.";
const EXPLORE_AGENT_PROMPT: &str = "You are a focused codebase exploration agent. Investigate the requested scope without modifying files. Prefer targeted searches and exact paths, trace the relevant control flow, and return concise findings with file and line references. Clearly separate confirmed evidence from inference.";
const GENERAL_PURPOSE_AGENT_DESCRIPTION: &str = "General-purpose agent for complex, multi-step work that may require investigation, reasoning, implementation, and verification. Use proactively for substantial tasks that do not fit a narrower specialist.";
const GENERAL_PURPOSE_AGENT_PROMPT: &str = "You are a capable general-purpose engineering agent. Complete the delegated task end to end: inspect the relevant context, make focused changes when authorized, verify the result in proportion to risk, and return a concise evidence-backed summary. Preserve unrelated user changes and follow all applicable instructions.";
const PLAN_AGENT_DESCRIPTION: &str = "Read-only planning and research agent. Use proactively to investigate architecture, constraints, risks, and verification needs before implementation.";
const PLAN_AGENT_PROMPT: &str = "You are a read-only planning and research agent. Investigate the requested scope without modifying files, identify the relevant architecture and constraints, surface risks and edge cases, and produce a decision-complete implementation and verification plan grounded in file and line evidence.";

#[derive(Debug, Parser)]
#[command(
    name = "claude-code-proxy",
    version = VERSION,
    about = "Anthropic-compatible proxy for Claude Code provider backends",
    disable_version_flag = true
)]
struct Cli {
    #[arg(long = "version", short = 'v', action = ArgAction::SetTrue)]
    version_flag: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Version {
        #[arg(long, action = ArgAction::SetTrue)]
        json: bool,
    },
    Serve {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long = "no-monitor", action = ArgAction::SetTrue)]
        no_monitor: bool,
    },
    /// Open the monitor TUI with mock data and no proxy server
    Demo,
    Models {
        #[arg(long)]
        full: bool,
    },
    Codex {
        #[command(subcommand)]
        command: ProviderGroup,
    },
    Kimi {
        #[command(subcommand)]
        command: ProviderGroup,
    },
    Cursor {
        #[command(subcommand)]
        command: ProviderGroup,
    },
    Grok {
        #[command(subcommand)]
        command: ProviderGroup,
    },
    /// Launch Claude Code with a model-specific proxy profile
    Claude {
        #[arg(value_enum)]
        profile: ClaudeProfile,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<OsString>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ClaudeProfile {
    Gpt,
    Grok,
}

#[derive(Debug, Clone, Copy)]
struct ClaudeProfileConfig {
    name: &'static str,
    main_model: &'static str,
    fable_model: &'static str,
    opus_model: &'static str,
    sonnet_model: &'static str,
    haiku_model: &'static str,
    context_tokens: &'static str,
    effort_level: &'static str,
    ultracode: bool,
    explore_effort: &'static str,
    general_purpose_effort: &'static str,
    plan_effort: &'static str,
    promote_codex_xhigh_to_max: bool,
    available_models: &'static [&'static str],
}

impl ClaudeProfile {
    fn config(self) -> ClaudeProfileConfig {
        match self {
            Self::Gpt => ClaudeProfileConfig {
                name: "GPT",
                main_model: "gpt-5.6-sol",
                fable_model: "gpt-5.6-sol",
                opus_model: "gpt-5.6-sol",
                sonnet_model: "gpt-5.6-terra",
                haiku_model: "gpt-5.6-luna",
                context_tokens: "272000",
                effort_level: "xhigh",
                ultracode: true,
                explore_effort: "medium",
                general_purpose_effort: "high",
                plan_effort: "high",
                promote_codex_xhigh_to_max: true,
                available_models: &[
                    "gpt-5.6-sol",
                    "gpt-5.6-sol-fast",
                    "gpt-5.6-terra",
                    "gpt-5.6-terra-fast",
                    "gpt-5.6-luna",
                    "gpt-5.6-luna-fast",
                ],
            },
            Self::Grok => ClaudeProfileConfig {
                name: "Grok",
                main_model: "grok-4.5-high",
                fable_model: "grok-4.5-high",
                opus_model: "grok-4.5-high",
                sonnet_model: "grok-4.5-high",
                haiku_model: "grok-4.5-medium",
                context_tokens: "500000",
                effort_level: "high",
                ultracode: false,
                explore_effort: "medium",
                general_purpose_effort: "high",
                plan_effort: "high",
                promote_codex_xhigh_to_max: false,
                available_models: &["grok-4.5", "grok-4.5-high", "grok-4.5-medium"],
            },
        }
    }
}

#[derive(Debug, Subcommand)]
enum ProviderGroup {
    Auth {
        #[command(subcommand)]
        command: claude_code_proxy::provider::AuthCommand,
    },
}

fn main() -> Result<()> {
    let result = run();
    if !logging::flush(Duration::from_secs(2)) {
        eprintln!("warning: proxy logs could not be flushed completely");
    }
    result
}

fn run() -> Result<()> {
    if let Some((profile, args)) = claude_profile_from_argv(std::env::args_os()) {
        return launch_claude(profile, &args);
    }

    let cli = Cli::parse();

    if cli.version_flag {
        println!("claude-code-proxy {}", VERSION);
        return Ok(());
    }

    let commands = cli.command.unwrap_or(Commands::Serve {
        port: None,
        no_monitor: false,
    });

    match commands {
        Commands::Version { json } => {
            if json {
                println!("{}", serde_json::to_string(&server::version_info())?);
            } else {
                println!("claude-code-proxy {}", VERSION);
            }
            Ok(())
        }
        Commands::Serve { port, no_monitor } => {
            // Cache the running inode before a deployment can atomically replace
            // the Cellar path while this process is still serving.
            server::initialize_process_identity();
            let bind_address = config::bind_address();
            let effective_port = port.unwrap_or_else(config::port);
            let registry = Registry::with_default_alias();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            match select_serve_mode(std::io::stdout().is_terminal(), no_monitor) {
                ServeMode::Plain => {
                    print_server_banner(&bind_address, effective_port, &registry);
                    runtime
                        .block_on(server::serve_with_shutdown(
                            ServerConfig {
                                bind_address,
                                port: effective_port,
                                monitor: None,
                            },
                            shutdown_signal(),
                        ))
                        .map_err(|err| anyhow::anyhow!(err))
                }
                ServeMode::Monitor => {
                    let _stderr_guard = logging::suppress_stderr();
                    let monitor = MonitorHandle::default();
                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                    let (shutdown_complete_tx, shutdown_complete_rx) = std::sync::mpsc::channel();
                    let listener = runtime
                        .block_on(server::bind_proxy_listener(&bind_address, effective_port))?;
                    let local_addr = listener.local_addr()?;
                    let monitor_listen_url =
                        listen_url(&local_addr.ip().to_string(), local_addr.port());
                    let server_monitor = monitor.clone();
                    let server_task = runtime.spawn(async move {
                        let result =
                            server::serve_listener(listener, Some(server_monitor), async move {
                                tokio::select! {
                                    _ = async {
                                        let _ = shutdown_rx.await;
                                    } => {}
                                    _ = shutdown_signal() => {}
                                }
                            })
                            .await;
                        let _ = shutdown_complete_tx.send(());
                        result
                    });
                    let ui_result = tui::run_monitor(
                        monitor,
                        MonitorUiConfig {
                            listen_url: monitor_listen_url,
                            port: effective_port,
                            registry: &registry,
                            shutdown: Some(shutdown_tx),
                            shutdown_complete: Some(shutdown_complete_rx),
                        },
                    );
                    if matches!(&ui_result, Ok(MonitorExit::ForceQuit)) {
                        server_task.abort();
                        let _ = runtime.block_on(server_task);
                        exit_with_logs(130);
                    }
                    let server_result = runtime.block_on(server_task)?;
                    ui_result?;
                    server_result.map_err(|err| anyhow::anyhow!(err))
                }
            }
        }
        Commands::Demo => {
            let registry = Registry::with_default_alias();
            tui::run_mock_monitor(config::port(), &registry)
        }
        Commands::Models { full } => {
            print_models(&Registry::with_default_alias(), full);
            Ok(())
        }
        Commands::Codex { command } => run_provider_cli("codex", command),
        Commands::Kimi { command } => run_provider_cli("kimi", command),
        Commands::Cursor { command } => run_provider_cli("cursor", command),
        Commands::Grok { command } => run_provider_cli("grok", command),
        Commands::Claude { profile, args } => launch_claude(profile, &args),
    }
}

fn claude_profile_from_argv(
    args: impl IntoIterator<Item = OsString>,
) -> Option<(ClaudeProfile, Vec<OsString>)> {
    let mut args = args.into_iter();
    let executable = args.next()?;
    let name = Path::new(&executable).file_name()?.to_str()?;
    let profile = match name {
        "co" | "co.exe" => ClaudeProfile::Gpt,
        "cg" | "cg.exe" => ClaudeProfile::Grok,
        _ => return None,
    };
    Some((profile, args.collect()))
}

fn launch_claude(profile: ClaudeProfile, args: &[OsString]) -> Result<()> {
    let loaded = config::load_config();
    let base_url = proxy_client_url(&loaded.bind_address, loaded.port);
    let mut command = build_claude_command(profile, args, &base_url)?;

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;

        let error = command.exec();
        Err(error).context("failed to exec local Claude Code")
    }

    #[cfg(not(unix))]
    {
        let status = command
            .status()
            .context("failed to launch local Claude Code")?;
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn build_claude_command(
    profile: ClaudeProfile,
    args: &[OsString],
    base_url: &str,
) -> Result<Command> {
    let profile = profile.config();
    validate_claude_profile_args(profile, args)?;
    let environment = claude_profile_environment(profile, base_url);
    let settings_environment = environment
        .iter()
        .map(|(name, value)| (name.to_string(), serde_json::Value::String(value.clone())))
        .collect::<serde_json::Map<_, _>>();
    let inline_settings = serde_json::json!({
        "env": settings_environment,
        "model": profile.main_model,
        "effortLevel": profile.effort_level,
        "ultracode": profile.ultracode,
        "availableModels": profile.available_models,
        "enforceAvailableModels": true,
        "fallbackModel": [],
    })
    .to_string();
    // CLI agent definitions are replacements, not partial overrides: Claude
    // Code requires both description and prompt. Keep its private built-in
    // `claude` agent untouched because public definitions cannot preserve that
    // agent's appendSystemPrompt/FleetView completion protocol.
    let inline_agents = serde_json::json!({
        "Explore": {
            "description": EXPLORE_AGENT_DESCRIPTION,
            "prompt": EXPLORE_AGENT_PROMPT,
            "tools": ["Read", "Glob", "Grep"],
            "permissionMode": "plan",
            "model": profile.haiku_model,
            "effort": profile.explore_effort,
        },
        "general-purpose": {
            "description": GENERAL_PURPOSE_AGENT_DESCRIPTION,
            "prompt": GENERAL_PURPOSE_AGENT_PROMPT,
            "model": profile.opus_model,
            "effort": profile.general_purpose_effort,
        },
        "Plan": {
            "description": PLAN_AGENT_DESCRIPTION,
            "prompt": PLAN_AGENT_PROMPT,
            "tools": ["Read", "Glob", "Grep"],
            "permissionMode": "plan",
            "model": profile.sonnet_model,
            "effort": profile.plan_effort,
        },
    })
    .to_string();

    let mut command = Command::new("claude");
    command
        .arg("--settings")
        .arg(inline_settings)
        .arg("--agents")
        .arg(inline_agents)
        .args(args)
        .envs(environment);
    Ok(command)
}

fn validate_claude_profile_args(profile: ClaudeProfileConfig, args: &[OsString]) -> Result<()> {
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let Some(argument) = argument.to_str() else {
            continue;
        };
        if argument == "--" {
            break;
        }
        if let Some(option) = blocked_claude_profile_option(argument) {
            anyhow::bail!(
                "{option} is disabled for the {} launch profile because it can override its model, agent, or context isolation; run `claude` directly for custom settings",
                profile.name
            );
        }

        if argument == "--model" || argument == "-m" {
            let value = args
                .next()
                .context("--model/-m requires a model name after it")?;
            let model = value
                .to_str()
                .context("--model/-m must be valid UTF-8 in a co/cg launch profile")?;
            validate_profile_model(profile, model, "--model")?;
            continue;
        }
        if let Some(model) = argument.strip_prefix("--model=") {
            validate_profile_model(profile, model, "--model")?;
            continue;
        }
        if let Some(model) = argument.strip_prefix("-m=") {
            validate_profile_model(profile, model, "--model")?;
            continue;
        }
        if let Some(model) = argument.strip_prefix("-m") {
            validate_profile_model(profile, model, "--model")?;
            continue;
        }

        let fallback_models = if argument == "--fallback-model" {
            let value = args
                .next()
                .context("--fallback-model requires a comma-separated model list after it")?;
            Some(
                value
                    .to_str()
                    .context("--fallback-model must be valid UTF-8 in a co/cg launch profile")?,
            )
        } else {
            argument.strip_prefix("--fallback-model=")
        };
        if let Some(fallback_models) = fallback_models {
            for model in fallback_models.split(',').map(str::trim) {
                validate_profile_model(profile, model, "--fallback-model")?;
            }
        }
    }
    Ok(())
}

fn blocked_claude_profile_option(argument: &str) -> Option<&'static str> {
    [
        "--settings",
        "--managed-settings",
        "--agents",
        "--autocompact",
        "--advisor",
    ]
    .into_iter()
    .find(|option| argument == *option || argument.starts_with(&format!("{option}=")))
}

fn validate_profile_model(profile: ClaudeProfileConfig, model: &str, option: &str) -> Result<()> {
    if model.is_empty() || !profile.available_models.contains(&model) {
        anyhow::bail!(
            "{option} model `{model}` is outside the {} launch profile; use `co` for GPT models and `cg` for Grok models",
            profile.name
        );
    }
    Ok(())
}

fn claude_profile_environment(
    profile: ClaudeProfileConfig,
    base_url: &str,
) -> Vec<(&'static str, String)> {
    let mut environment = vec![
        ("ANTHROPIC_BASE_URL", base_url.to_string()),
        ("ANTHROPIC_AUTH_TOKEN", "unused".to_string()),
        ("ANTHROPIC_MODEL", profile.main_model.to_string()),
        (
            "ANTHROPIC_DEFAULT_FABLE_MODEL",
            profile.fable_model.to_string(),
        ),
        (
            "ANTHROPIC_DEFAULT_FABLE_MODEL_NAME",
            profile.fable_model.to_string(),
        ),
        (
            "ANTHROPIC_DEFAULT_OPUS_MODEL",
            profile.opus_model.to_string(),
        ),
        (
            "ANTHROPIC_DEFAULT_OPUS_MODEL_NAME",
            profile.opus_model.to_string(),
        ),
        (
            "ANTHROPIC_DEFAULT_SONNET_MODEL",
            profile.sonnet_model.to_string(),
        ),
        (
            "ANTHROPIC_DEFAULT_SONNET_MODEL_NAME",
            profile.sonnet_model.to_string(),
        ),
        (
            "ANTHROPIC_DEFAULT_HAIKU_MODEL",
            profile.haiku_model.to_string(),
        ),
        (
            "ANTHROPIC_DEFAULT_HAIKU_MODEL_NAME",
            profile.haiku_model.to_string(),
        ),
        (
            "ANTHROPIC_SMALL_FAST_MODEL",
            profile.haiku_model.to_string(),
        ),
        (
            "CLAUDE_CODE_MAX_CONTEXT_TOKENS",
            profile.context_tokens.to_string(),
        ),
        (
            "CLAUDE_CODE_AUTO_COMPACT_WINDOW",
            profile.context_tokens.to_string(),
        ),
        ("CLAUDE_AUTOCOMPACT_PCT_OVERRIDE", "90".to_string()),
        ("CLAUDE_CODE_DISABLE_1M_CONTEXT", "1".to_string()),
        ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1".to_string()),
        ("CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK", "1".to_string()),
        ("CLAUDE_CODE_MAX_RETRIES", "1".to_string()),
        ("CLAUDE_CODE_MAX_TOOL_USE_CONCURRENCY", "10".to_string()),
        ("ENABLE_TOOL_SEARCH", "true".to_string()),
    ];
    if profile.promote_codex_xhigh_to_max {
        let existing = std::env::var("ANTHROPIC_CUSTOM_HEADERS").ok();
        environment.push((
            "ANTHROPIC_CUSTOM_HEADERS",
            merged_anthropic_custom_headers(existing.as_deref()),
        ));
    }
    environment
}

fn merged_anthropic_custom_headers(existing: Option<&str>) -> String {
    let mut headers = existing
        .unwrap_or_default()
        .lines()
        .filter(|line| {
            let name = line.split_once(':').map_or(*line, |(name, _)| name);
            !name
                .trim()
                .eq_ignore_ascii_case(CODEX_XHIGH_AS_MAX_HEADER_NAME)
        })
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    headers.push(CODEX_XHIGH_AS_MAX_HEADER);
    headers.join("\n")
}

fn proxy_client_url(bind_address: &str, port: u16) -> String {
    let client_address = match bind_address.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(address)) if address.is_unspecified() => "127.0.0.1".to_string(),
        Ok(std::net::IpAddr::V6(address)) if address.is_unspecified() => "::1".to_string(),
        _ => bind_address.to_string(),
    };
    listen_url(&client_address, port)
}

fn exit_with_logs(code: i32) -> ! {
    if !logging::flush(Duration::from_secs(2)) {
        eprintln!("warning: proxy logs could not be flushed completely");
    }
    std::process::exit(code)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServeMode {
    Monitor,
    Plain,
}

fn select_serve_mode(stdout_is_tty: bool, no_monitor: bool) -> ServeMode {
    if stdout_is_tty && !no_monitor {
        ServeMode::Monitor
    } else {
        ServeMode::Plain
    }
}

fn run_provider_cli(name: &str, command: ProviderGroup) -> Result<()> {
    let registry = Registry::with_default_alias();
    let provider = registry
        .provider(name)
        .ok_or_else(|| anyhow::anyhow!("unknown provider: {name}"))?;
    let handlers = provider.cli();
    match command {
        ProviderGroup::Auth { command } => match command {
            claude_code_proxy::provider::AuthCommand::Login => {
                if let Err(err) = handlers.login() {
                    eprintln!("{err}");
                    exit_with_logs(2);
                }
                Ok(())
            }
            claude_code_proxy::provider::AuthCommand::Device => {
                if let Err(err) = handlers.device() {
                    eprintln!("{err}");
                    exit_with_logs(2);
                }
                Ok(())
            }
            claude_code_proxy::provider::AuthCommand::Status => {
                if let Err(err) = handlers.status() {
                    println!("{err}");
                    if err.to_string() == "Not authenticated" {
                        exit_with_logs(1);
                    }
                    exit_with_logs(2);
                }
                Ok(())
            }
            claude_code_proxy::provider::AuthCommand::Logout => {
                handlers.logout()?;
                Ok(())
            }
        },
    }
}

fn print_models(registry: &Registry, full: bool) {
    let grouped = registry.grouped_models();
    for provider in ["codex", "kimi", "grok", "cursor"] {
        let Some(models) = grouped.get(provider) else {
            continue;
        };
        if full || provider != "cursor" {
            println!("{provider}: {}", models.join(", "));
        } else {
            println!("{provider}: {}", compact_cursor_list(models));
        }
    }
}

fn compact_cursor_list(models: &[String]) -> String {
    let mut legacy = Vec::new();
    let mut dynamic = Vec::new();
    for model in models {
        if !model.contains(':') {
            legacy.push(model.clone());
        } else {
            dynamic.push(model.clone());
        }
    }
    let mut out = String::new();
    if !legacy.is_empty() {
        out.push_str(&legacy.join(", "));
        out.push_str("; ");
    }
    out.push_str(&format!("{} cursor model aliases", dynamic.len()));
    if !dynamic.is_empty() {
        out.push_str(", example: cursor:gpt-5.5");
    }
    out.push_str(" run `claude-code-proxy models --full` for all aliases");
    out
}

fn listen_url(bind_address: &str, port: u16) -> String {
    match bind_address.parse::<std::net::IpAddr>() {
        Ok(ip) => format!("http://{}", std::net::SocketAddr::new(ip, port)),
        Err(_) => format!("http://{bind_address}:{port}"),
    }
}

fn print_server_banner(bind_address: &str, port: u16, registry: &Registry) {
    println!("Proxy listening on {}", listen_url(bind_address, port));
    println!("Logs: {}", paths::log_file().display());
    let cfg = paths::config_dir();
    if cfg.exists() {
        println!("Config: {}", cfg.display());
    }
    print_models(registry, false);
    println!();
    println!("Configure Claude Code (pick a model from above):");
    println!("  export ANTHROPIC_BASE_URL=\"http://localhost:{port}\"");
    println!("  export ANTHROPIC_AUTH_TOKEN=\"anything\"");
    println!("  export ANTHROPIC_MODEL=\"gpt-5.6-sol\"");
    println!("  export ANTHROPIC_SMALL_FAST_MODEL=\"gpt-5.6-luna\"");
    println!("  export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1");
}

#[allow(dead_code)]
fn alias_names() -> usize {
    ANTHROPIC_STYLE_ALIASES.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    #[test]
    fn default_serve_selects_monitor_on_tty() {
        assert_eq!(select_serve_mode(true, false), ServeMode::Monitor);
    }

    #[test]
    fn no_monitor_selects_plain_mode() {
        assert_eq!(select_serve_mode(true, true), ServeMode::Plain);
    }

    #[test]
    fn non_tty_stdout_selects_plain_mode() {
        assert_eq!(select_serve_mode(false, false), ServeMode::Plain);
    }

    #[test]
    fn demo_command_parses_without_server_options() {
        let cli = Cli::try_parse_from(["claude-code-proxy", "demo"]).unwrap();

        assert!(matches!(cli.command, Some(Commands::Demo)));
    }

    #[test]
    fn listen_url_brackets_ipv6_addresses() {
        assert_eq!(listen_url("::1", 18765), "http://[::1]:18765");
    }

    #[test]
    fn claude_profile_forwards_trailing_arguments() {
        let cli = Cli::try_parse_from([
            "claude-code-proxy",
            "claude",
            "gpt",
            "--",
            "--effort",
            "max",
            "hello world",
        ])
        .unwrap();

        let Some(Commands::Claude { profile, args }) = cli.command else {
            panic!("expected Claude profile command");
        };
        assert_eq!(profile, ClaudeProfile::Gpt);
        assert_eq!(args, ["--effort", "max", "hello world"].map(OsString::from));
    }

    #[test]
    fn argv_zero_shortcuts_select_profile_and_preserve_arguments() {
        let (profile, args) = claude_profile_from_argv([
            OsString::from("/home/user/.local/bin/co"),
            OsString::from("--effort"),
            OsString::from("max"),
        ])
        .expect("co should select a profile");
        assert_eq!(profile, ClaudeProfile::Gpt);
        assert_eq!(args, ["--effort", "max"].map(OsString::from));

        let (profile, args) =
            claude_profile_from_argv([OsString::from("cg"), OsString::from("--continue")])
                .expect("cg should select a profile");
        assert_eq!(profile, ClaudeProfile::Grok);
        assert_eq!(args, ["--continue"].map(OsString::from));
    }

    #[test]
    fn normal_binary_name_does_not_select_profile_shortcut() {
        assert!(
            claude_profile_from_argv([
                OsString::from("claude-code-proxy"),
                OsString::from("serve")
            ])
            .is_none()
        );
    }

    #[test]
    fn gpt_profile_builds_codex_model_environment() {
        let command = build_claude_command(
            ClaudeProfile::Gpt,
            &[OsString::from("--effort"), OsString::from("max")],
            "http://127.0.0.1:18765",
        )
        .unwrap();

        assert_eq!(command.get_program(), OsStr::new("claude"));
        let command_args = command.get_args().collect::<Vec<_>>();
        assert_eq!(command_args[0], OsStr::new("--settings"));
        assert_eq!(command_args[2], OsStr::new("--agents"));
        assert_eq!(
            &command_args[4..],
            [OsStr::new("--effort"), OsStr::new("max")]
        );
        let settings = command_inline_settings(&command);
        let agents = command_inline_agents(&command);
        assert_complete_inline_agents(&agents);
        assert_eq!(settings["model"], "gpt-5.6-sol");
        assert_eq!(settings["effortLevel"], "xhigh");
        assert_eq!(settings["ultracode"], true);
        assert_eq!(settings["enforceAvailableModels"], true);
        assert_eq!(settings["fallbackModel"], serde_json::json!([]));
        assert_eq!(agents["Explore"]["model"], "gpt-5.6-luna");
        assert_eq!(agents["Explore"]["effort"], "medium");
        assert_eq!(agents["Plan"]["model"], "gpt-5.6-terra");
        assert_eq!(agents["Plan"]["effort"], "high");
        assert_eq!(agents["general-purpose"]["model"], "gpt-5.6-sol");
        assert_eq!(agents["general-purpose"]["effort"], "high");
        assert!(
            settings["availableModels"]
                .as_array()
                .unwrap()
                .iter()
                .any(|model| model == "gpt-5.6-terra")
        );
        assert!(
            !settings["availableModels"]
                .as_array()
                .unwrap()
                .iter()
                .any(|model| model == "opus")
        );
        assert_eq!(settings["env"]["CLAUDE_CODE_MAX_CONTEXT_TOKENS"], "272000");
        assert_eq!(command_env(&command, "ANTHROPIC_MODEL"), "gpt-5.6-sol");
        assert_eq!(
            command_env(&command, "ANTHROPIC_DEFAULT_SONNET_MODEL"),
            "gpt-5.6-terra"
        );
        assert_eq!(
            command_env(&command, "ANTHROPIC_DEFAULT_HAIKU_MODEL"),
            "gpt-5.6-luna"
        );
        assert_eq!(
            command_env(&command, "CLAUDE_CODE_MAX_CONTEXT_TOKENS"),
            "272000"
        );
        assert_eq!(
            command_env(&command, "CLAUDE_CODE_AUTO_COMPACT_WINDOW"),
            "272000"
        );
        assert!(
            command_env(&command, "ANTHROPIC_CUSTOM_HEADERS")
                .lines()
                .any(|line| line == CODEX_XHIGH_AS_MAX_HEADER)
        );
    }

    #[test]
    fn grok_profile_builds_grok_model_environment() {
        let command =
            build_claude_command(ClaudeProfile::Grok, &[], "http://127.0.0.1:18765").unwrap();

        let settings = command_inline_settings(&command);
        let agents = command_inline_agents(&command);
        assert_complete_inline_agents(&agents);
        assert_eq!(settings["model"], "grok-4.5-high");
        assert_eq!(settings["effortLevel"], "high");
        assert_eq!(settings["ultracode"], false);
        assert_eq!(agents["Explore"]["model"], "grok-4.5-medium");
        assert_eq!(agents["Explore"]["effort"], "medium");
        assert_eq!(agents["Plan"]["model"], "grok-4.5-high");
        assert_eq!(agents["Plan"]["effort"], "high");
        assert_eq!(agents["general-purpose"]["effort"], "high");
        assert!(
            settings["availableModels"]
                .as_array()
                .unwrap()
                .iter()
                .any(|model| model == "grok-4.5-medium")
        );
        assert_eq!(settings["env"]["CLAUDE_CODE_AUTO_COMPACT_WINDOW"], "500000");
        assert_eq!(command_env(&command, "ANTHROPIC_MODEL"), "grok-4.5-high");
        assert_eq!(
            command_env(&command, "ANTHROPIC_DEFAULT_OPUS_MODEL"),
            "grok-4.5-high"
        );
        assert_eq!(
            command_env(&command, "ANTHROPIC_DEFAULT_HAIKU_MODEL"),
            "grok-4.5-medium"
        );
        assert_eq!(
            command_env(&command, "ANTHROPIC_SMALL_FAST_MODEL"),
            "grok-4.5-medium"
        );
        assert_eq!(
            command_env(&command, "CLAUDE_CODE_MAX_CONTEXT_TOKENS"),
            "500000"
        );
        assert_eq!(
            command_env(&command, "CLAUDE_CODE_AUTO_COMPACT_WINDOW"),
            "500000"
        );
        assert_eq!(command_env(&command, "CLAUDE_CODE_DISABLE_1M_CONTEXT"), "1");
        assert_eq!(
            command_env(&command, "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE"),
            "90"
        );
        assert!(command_env_optional(&command, "ANTHROPIC_CUSTOM_HEADERS").is_none());
    }

    #[test]
    fn codex_max_marker_merges_existing_headers_and_deduplicates_it() {
        let merged = merged_anthropic_custom_headers(Some(
            "x-existing: keep\nX-CCPROXY-CODEX-XHIGH-AS-MAX: 0\n\nsecond: value\n\
             x-ccproxy-codex-xhigh-as-max: duplicate",
        ));

        assert_eq!(
            merged,
            "x-existing: keep\nsecond: value\nx-ccproxy-codex-xhigh-as-max: 1"
        );
        assert_eq!(
            merged
                .lines()
                .filter(|line| {
                    line.split_once(':').is_some_and(|(name, _)| {
                        name.eq_ignore_ascii_case(CODEX_XHIGH_AS_MAX_HEADER_NAME)
                    })
                })
                .count(),
            1
        );
    }

    #[test]
    fn claude_profiles_reject_cross_family_models() {
        let error = build_claude_command(
            ClaudeProfile::Grok,
            &[OsString::from("--model=gpt-5.6-sol")],
            "http://127.0.0.1:18765",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("outside the Grok launch profile")
        );

        build_claude_command(
            ClaudeProfile::Gpt,
            &[OsString::from("--model"), OsString::from("gpt-5.6-terra")],
            "http://127.0.0.1:18765",
        )
        .expect("same-family model should remain selectable");

        let error = build_claude_command(
            ClaudeProfile::Gpt,
            &[
                OsString::from("--fallback-model"),
                OsString::from("gpt-5.6-luna,grok-4.5-medium"),
            ],
            "http://127.0.0.1:18765",
        )
        .unwrap_err();
        assert!(error.to_string().contains("--fallback-model model"));

        let error = build_claude_command(
            ClaudeProfile::Gpt,
            &[OsString::from("--model=GPT-5.6-SOL")],
            "http://127.0.0.1:18765",
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside the GPT launch profile"));

        for args in [
            vec![OsString::from("-m"), OsString::from("grok-4.5-high")],
            vec![OsString::from("-m=grok-4.5-high")],
            vec![OsString::from("-mgrok-4.5-high")],
        ] {
            let error = build_claude_command(ClaudeProfile::Gpt, &args, "http://127.0.0.1:18765")
                .unwrap_err();
            assert!(error.to_string().contains("outside the GPT launch profile"));
        }
    }

    #[test]
    fn claude_profiles_reject_settings_override() {
        let error = build_claude_command(
            ClaudeProfile::Gpt,
            &[OsString::from("--settings"), OsString::from("{}")],
            "http://127.0.0.1:18765",
        )
        .unwrap_err();
        assert!(error.to_string().contains("--settings is disabled"));

        let error = build_claude_command(
            ClaudeProfile::Grok,
            &[OsString::from("--managed-settings"), OsString::from("{}")],
            "http://127.0.0.1:18765",
        )
        .unwrap_err();
        assert!(error.to_string().contains("--managed-settings"));

        for option in ["--agents", "--autocompact", "--advisor"] {
            let error = build_claude_command(
                ClaudeProfile::Gpt,
                &[OsString::from(option), OsString::from("override")],
                "http://127.0.0.1:18765",
            )
            .unwrap_err();
            assert!(error.to_string().contains(option));
        }
    }

    #[test]
    fn proxy_client_url_uses_loopback_for_unspecified_listener() {
        assert_eq!(proxy_client_url("0.0.0.0", 18765), "http://127.0.0.1:18765");
        assert_eq!(proxy_client_url("::", 18765), "http://[::1]:18765");
    }

    fn command_env(command: &Command, key: &str) -> String {
        command_env_optional(command, key)
            .unwrap_or_else(|| panic!("missing UTF-8 command environment variable: {key}"))
    }

    fn command_env_optional(command: &Command, key: &str) -> Option<String> {
        command
            .get_envs()
            .find_map(|(name, value)| {
                (name == OsStr::new(key)).then(|| value.and_then(OsStr::to_str))
            })
            .flatten()
            .map(str::to_string)
    }

    fn command_inline_settings(command: &Command) -> serde_json::Value {
        let args = command.get_args().collect::<Vec<_>>();
        assert_eq!(args.first().copied(), Some(OsStr::new("--settings")));
        serde_json::from_str(
            args.get(1)
                .and_then(|value| value.to_str())
                .expect("inline settings must be a UTF-8 argument"),
        )
        .expect("inline settings must be valid JSON")
    }

    fn command_inline_agents(command: &Command) -> serde_json::Value {
        let args = command.get_args().collect::<Vec<_>>();
        assert_eq!(args.get(2).copied(), Some(OsStr::new("--agents")));
        serde_json::from_str(
            args.get(3)
                .and_then(|value| value.to_str())
                .expect("inline agents must be a UTF-8 argument"),
        )
        .expect("inline agents must be valid JSON")
    }

    fn assert_complete_inline_agents(agents: &serde_json::Value) {
        assert!(
            agents.get("claude").is_none(),
            "the private built-in claude agent must not be overridden"
        );
        for name in ["Explore", "general-purpose", "Plan"] {
            assert!(
                agents[name]["description"]
                    .as_str()
                    .is_some_and(|value| !value.trim().is_empty()),
                "{name} must have a nonempty description"
            );
            assert!(
                agents[name]["prompt"]
                    .as_str()
                    .is_some_and(|value| !value.trim().is_empty()),
                "{name} must have a nonempty prompt"
            );
        }

        let read_only_tools = serde_json::json!(["Read", "Glob", "Grep"]);
        for name in ["Explore", "Plan"] {
            assert_eq!(agents[name]["tools"], read_only_tools);
            assert_eq!(agents[name]["permissionMode"], "plan");
        }
        assert!(agents["general-purpose"].get("tools").is_none());
        assert!(agents["general-purpose"].get("permissionMode").is_none());
    }
}
