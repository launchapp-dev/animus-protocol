use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::cli::{
    ensure_flag, ensure_flag_value, parse_launch_from_runtime_contract, LaunchInvocation,
};
use crate::error::{Error, Result};

use super::parser::parse_claude_stdout_line;
use crate::session::{
    session_event::SessionEvent, session_request::SessionRequest, session_run::SessionRun,
};

pub(crate) async fn start_claude_session(
    request: SessionRequest,
    resume_session_id: Option<String>,
) -> Result<SessionRun> {
    let mcp_config_path = write_claude_mcp_config(&request)?;
    let invocation = claude_invocation_for_request(
        &request,
        resume_session_id.as_deref(),
        mcp_config_path.as_deref(),
    )?;
    let control_session_id = Uuid::new_v4().to_string();
    let control_session_id_for_run = control_session_id.clone();
    let (event_tx, event_rx) = mpsc::channel(128);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (pid_tx, pid_rx) = oneshot::channel::<Option<u32>>();
    register_session(control_session_id.clone(), cancel_tx);

    tokio::spawn(async move {
        let backend_label = "claude-native".to_string();
        let session_id_for_event = Some(control_session_id.clone());

        let run_result = run_claude_session(
            request,
            invocation,
            event_tx.clone(),
            cancel_rx,
            pid_tx,
            backend_label,
            session_id_for_event,
        )
        .await;
        if let Some(path) = &mcp_config_path {
            let _ = std::fs::remove_file(path);
        }
        if let Err(error) = run_result {
            let _ = event_tx
                .send(SessionEvent::Error {
                    message: error.to_string(),
                    recoverable: false,
                })
                .await;
            let _ = event_tx
                .send(SessionEvent::Finished { exit_code: Some(1) })
                .await;
        }
        unregister_session(&control_session_id);
    });

    let pid = pid_rx.await.ok().flatten();
    Ok(SessionRun {
        session_id: Some(control_session_id_for_run),
        events: event_rx,
        selected_backend: "claude-native".to_string(),
        fallback_reason: None,
        pid,
    })
}

pub(crate) async fn terminate_claude_session(session_id: &str) -> Result<()> {
    let Some(cancel_tx) = take_session(session_id) else {
        return Err(Error::ExecutionFailed(format!(
            "claude backend does not track active child process for session '{}'",
            session_id
        )));
    };
    let _ = cancel_tx.send(());
    Ok(())
}

/// Normalize a `reasoning_effort` extras value to a Claude CLI `--effort`
/// level. The Claude CLI accepts `low`, `medium`, `high`, `xhigh`, and
/// `max`; Animus exposes `low`/`medium`/`high`, which map through
/// unchanged. Unrecognized or empty values yield `None` so the flag is
/// omitted and the CLI uses its own default effort.
fn reasoning_effort_to_claude(level: &str) -> Option<&'static str> {
    match level.trim().to_ascii_lowercase().as_str() {
        "low" => Some("low"),
        "medium" => Some("medium"),
        "high" => Some("high"),
        _ => None,
    }
}

/// Apply `extras.reasoning_effort` to a Claude argv as `--effort <level>`.
///
/// The flag pair is inserted at the FRONT of the argv (the options region),
/// not next to the trailing token. `claude [options] [prompt]` accepts
/// options ahead of the positional prompt, and a runtime-contract launch may
/// send the prompt via stdin — leaving a flag value (e.g. `stream-json`)
/// last — so prompt-relative insertion could split a flag pair.
///
/// A caller-supplied `--effort` (e.g. inside a runtime-contract launch
/// block) wins: this only inserts the level when no `--effort` flag is
/// already present.
fn apply_claude_reasoning_effort(args: &mut Vec<String>, request: &SessionRequest) {
    let Some(level) = request
        .extras
        .get("reasoning_effort")
        .and_then(serde_json::Value::as_str)
        .and_then(reasoning_effort_to_claude)
    else {
        return;
    };
    if args.iter().any(|arg| arg == "--effort") {
        return;
    }
    args.insert(0, "--effort".to_string());
    args.insert(1, level.to_string());
}

/// Write the per-run `--mcp-config` document (`{"mcpServers": {...}}`)
/// into a user-only (`0600`) temp file and return its path, or `None`
/// when the request carries no MCP servers. The config goes through a
/// private file rather than inline argv JSON so secret-bearing entries
/// (env tokens, auth headers) never show up in process listings. The
/// file is removed once the session finishes.
fn write_claude_mcp_config(request: &SessionRequest) -> Result<Option<std::path::PathBuf>> {
    let Some(servers) = request.mcp_servers_object() else {
        return Ok(None);
    };
    let config = serde_json::json!({ "mcpServers": servers });
    let path = std::env::temp_dir().join(format!("animus-claude-mcp-{}.json", Uuid::new_v4()));
    crate::session::write_private_file(&path, &config.to_string())?;
    Ok(Some(path))
}

/// Apply a written MCP config file to a Claude argv as
/// `--mcp-config <path>` plus `--strict-mcp-config`.
///
/// The flag triple is inserted at the FRONT of the argv (the options
/// region) for the same reason as `--effort`: the trailing token is not
/// guaranteed to be the prompt. A caller-supplied `--mcp-config` (e.g.
/// inside a runtime-contract launch block) wins, and an absent config
/// path (no/empty `mcp_servers`) leaves the argv untouched.
fn apply_claude_mcp_config(args: &mut Vec<String>, mcp_config_path: Option<&std::path::Path>) {
    let Some(path) = mcp_config_path else {
        return;
    };
    if args.iter().any(|arg| arg == "--mcp-config") {
        return;
    }
    args.insert(0, "--mcp-config".to_string());
    args.insert(1, path.display().to_string());
    args.insert(2, "--strict-mcp-config".to_string());
}

pub(crate) fn claude_invocation_for_request(
    request: &SessionRequest,
    resume_session_id: Option<&str>,
    mcp_config_path: Option<&std::path::Path>,
) -> Result<LaunchInvocation> {
    if let Some(mut invocation) =
        parse_launch_from_runtime_contract(request.extras.get("runtime_contract"))?
    {
        apply_claude_reasoning_effort(&mut invocation.args, request);
        apply_claude_mcp_config(&mut invocation.args, mcp_config_path);
        if !invocation.env.contains_key("ANTHROPIC_BASE_URL") {
            if let Some((base_url, api_key)) = resolve_anthropic_compatible_provider(&request.model)
            {
                invocation
                    .env
                    .insert("ANTHROPIC_BASE_URL".to_string(), base_url);
                invocation
                    .env
                    .insert("ANTHROPIC_API_KEY".to_string(), api_key);
                invocation
                    .env
                    .insert("DISABLE_PROMPT_CACHING".to_string(), "true".to_string());
                if let Some(pos) = invocation.args.iter().position(|a| a == "--model") {
                    if let Some(model_arg) = invocation.args.get_mut(pos + 1) {
                        if let Some(bare) = model_arg.split('/').nth(1) {
                            *model_arg = bare.to_string();
                        }
                    }
                }
            }
        }
        return Ok(invocation);
    }

    let mut args = vec![
        "--print".to_string(),
        "--verbose".to_string(),
        "--output-format".to_string(),
        "stream-json".to_string(),
    ];

    if let Some(permission_mode) = request
        .permission_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        args.push("--permission-mode".to_string());
        args.push(permission_mode.to_string());
    } else {
        args.push("--dangerously-skip-permissions".to_string());
    }

    if let Some(session_id) = resume_session_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        args.push("--resume".to_string());
        args.push(session_id.to_string());
    } else if let Some(session_id) = configured_claude_session_id(request) {
        args.push("--session-id".to_string());
        args.push(session_id);
    }

    let mut resolved_model = request.model.clone();

    let mut env: std::collections::BTreeMap<String, String> = Default::default();
    for (key, value) in &request.env_vars {
        match key.as_str() {
            "ANTHROPIC_BASE_URL"
            | "ANTHROPIC_API_KEY"
            | "ENABLE_TOOL_SEARCH"
            | "DISABLE_PROMPT_CACHING" => {
                env.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }
    if !env.contains_key("ANTHROPIC_BASE_URL") {
        if let Some((base_url, api_key)) = resolve_anthropic_compatible_provider(&request.model) {
            env.insert("ANTHROPIC_BASE_URL".to_string(), base_url);
            env.insert("ANTHROPIC_API_KEY".to_string(), api_key);
            env.insert("DISABLE_PROMPT_CACHING".to_string(), "true".to_string());
            if let Some(bare) = resolved_model.split('/').nth(1) {
                resolved_model = bare.to_string();
            }
        }
    }
    if !resolved_model.trim().is_empty() {
        args.push("--model".to_string());
        args.push(resolved_model);
    }
    args.push(request.prompt.clone());
    apply_claude_reasoning_effort(&mut args, request);
    apply_claude_mcp_config(&mut args, mcp_config_path);
    let mut invocation = LaunchInvocation {
        command: "claude".to_string(),
        args,
        env,
        prompt_via_stdin: false,
    };
    ensure_flag(&mut invocation.args, "--verbose", 1);
    ensure_flag_value(&mut invocation.args, "--output-format", "stream-json", 2);

    Ok(invocation)
}

/// Resolve Anthropic-compatible provider credentials from model prefix.
/// Reads `~/.animus/credentials.json` and returns `(base_url, api_key)` if the
/// model's provider has both fields set.
fn resolve_anthropic_compatible_provider(model: &str) -> Option<(String, String)> {
    let normalized = model.to_ascii_lowercase();
    let provider = normalized.split('/').next().unwrap_or(&normalized);

    if provider.starts_with("claude") || provider.is_empty() {
        return None;
    }

    let home = std::env::var("HOME").ok()?;
    let creds_path = std::path::PathBuf::from(home)
        .join(".animus")
        .join("credentials.json");
    let content = std::fs::read_to_string(&creds_path).ok()?;
    let creds: serde_json::Value = serde_json::from_str(&content).ok()?;
    let providers = creds.get("providers")?.as_object()?;

    let entry = providers.get(provider).or_else(|| {
        providers.iter().find_map(|(key, val)| {
            if normalized.contains(key) || key.contains(&normalized) {
                Some(val)
            } else {
                None
            }
        })
    })?;
    let base_url = entry.get("base_url")?.as_str()?.to_string();
    let api_key = entry.get("api_key")?.as_str()?.to_string();

    if base_url.is_empty() || api_key.is_empty() {
        return None;
    }

    Some((base_url, api_key))
}

async fn run_claude_session(
    request: SessionRequest,
    invocation: LaunchInvocation,
    event_tx: mpsc::Sender<SessionEvent>,
    mut cancel_rx: oneshot::Receiver<()>,
    pid_tx: oneshot::Sender<Option<u32>>,
    backend: String,
    session_id: Option<String>,
) -> Result<()> {
    let mut command = Command::new(&invocation.command);
    command
        .args(&invocation.args)
        .current_dir(&request.cwd)
        .env_remove("CLAUDECODE")
        .env_remove("CLAUDE_CODE_ENTRYPOINT")
        .env_remove("CLAUDE_CODE_SESSION_ACCESS_TOKEN")
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .envs(invocation.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn()?;
    let _ = pid_tx.send(child.id());

    let pid = child.id();
    let _ = event_tx
        .send(SessionEvent::Started {
            backend,
            session_id,
            pid,
        })
        .await;

    if let Some(mut stdin) = child.stdin.take() {
        if invocation.prompt_via_stdin && !request.prompt.is_empty() {
            stdin.write_all(request.prompt.as_bytes()).await?;
        }
        drop(stdin);
    }

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::ExecutionFailed("failed to capture claude stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::ExecutionFailed("failed to capture claude stderr".to_string()))?;

    let stdout_tx = event_tx.clone();
    let stdout_task = tokio::spawn(async move {
        let mut last_final_text: Option<String> = None;
        let mut lines = BufReader::new(stdout).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            for event in parse_claude_stdout_line(&line) {
                if let SessionEvent::FinalText { text } = &event {
                    if last_final_text.as_deref() == Some(text.as_str()) {
                        continue;
                    }
                    last_final_text = Some(text.clone());
                }
                let _ = stdout_tx.send(event).await;
            }
        }
    });

    let stderr_tx = event_tx.clone();
    let stderr_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let _ = stderr_tx
                .send(SessionEvent::Error {
                    message: line,
                    recoverable: true,
                })
                .await;
        }
    });

    let exit_code = wait_for_claude_child(&mut child, request.timeout_secs, &mut cancel_rx).await?;

    let _ = stdout_task.await;
    let _ = stderr_task.await;

    let _ = event_tx.send(SessionEvent::Finished { exit_code }).await;

    Ok(())
}

async fn wait_for_claude_child(
    child: &mut Child,
    timeout_secs: Option<u64>,
    cancel_rx: &mut oneshot::Receiver<()>,
) -> Result<Option<i32>> {
    match timeout_secs {
        Some(secs) => {
            let timeout_sleep = tokio::time::sleep(Duration::from_secs(secs));
            tokio::pin!(timeout_sleep);
            tokio::select! {
                status = child.wait() => Ok(status?.code()),
                _ = &mut timeout_sleep => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed(format!(
                        "claude session timed out after {} seconds",
                        secs
                    )))
                }
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed("claude session cancelled".to_string()))
                }
            }
        }
        None => {
            tokio::select! {
                status = child.wait() => Ok(status?.code()),
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed("claude session cancelled".to_string()))
                }
            }
        }
    }
}

fn session_registry() -> &'static Mutex<HashMap<String, oneshot::Sender<()>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, oneshot::Sender<()>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_session(session_id: String, cancel_tx: oneshot::Sender<()>) {
    if let Ok(mut registry) = session_registry().lock() {
        registry.insert(session_id, cancel_tx);
    }
}

fn unregister_session(session_id: &str) {
    if let Ok(mut registry) = session_registry().lock() {
        registry.remove(session_id);
    }
}

fn take_session(session_id: &str) -> Option<oneshot::Sender<()>> {
    session_registry()
        .lock()
        .ok()
        .and_then(|mut registry| registry.remove(session_id))
}

fn configured_claude_session_id(request: &SessionRequest) -> Option<String> {
    let raw = request
        .extras
        .pointer("/runtime_contract/cli/session/session_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    if Uuid::parse_str(raw).is_ok() {
        return Some(raw.to_string());
    }

    Some(Uuid::new_v4().to_string())
}

#[cfg(test)]
mod reasoning_effort_tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    pub(super) fn request_with_extras(extras: serde_json::Value) -> SessionRequest {
        SessionRequest {
            tool: "claude".into(),
            model: "claude-sonnet-4-6".into(),
            prompt: "say hi".into(),
            cwd: PathBuf::from("."),
            project_root: None,
            mcp_endpoint: None,
            mcp_servers: None,
            permission_mode: None,
            timeout_secs: None,
            env_vars: Vec::new(),
            extras,
        }
    }

    fn effort_value(args: &[String]) -> Option<String> {
        args.iter()
            .position(|arg| arg == "--effort")
            .and_then(|index| args.get(index + 1).cloned())
    }

    #[test]
    fn bare_args_inject_effort_flag() {
        let request = request_with_extras(json!({ "reasoning_effort": "high" }));
        let invocation = claude_invocation_for_request(&request, None, None).expect("invocation");
        assert_eq!(effort_value(&invocation.args), Some("high".to_string()));
        assert_eq!(invocation.args.last().map(String::as_str), Some("say hi"));
    }

    #[test]
    fn absent_reasoning_effort_omits_flag() {
        let request = request_with_extras(json!({}));
        let invocation = claude_invocation_for_request(&request, None, None).expect("invocation");
        assert!(effort_value(&invocation.args).is_none());
    }

    #[test]
    fn unknown_level_is_ignored() {
        let request = request_with_extras(json!({ "reasoning_effort": "ludicrous" }));
        let invocation = claude_invocation_for_request(&request, None, None).expect("invocation");
        assert!(effort_value(&invocation.args).is_none());
    }

    #[test]
    fn runtime_contract_effort_not_duplicated() {
        let request = request_with_extras(json!({
            "reasoning_effort": "low",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": [
                            "--print", "--verbose", "--output-format", "stream-json",
                            "--effort", "max", "say hi"
                        ]
                    }
                }
            }
        }));
        let invocation = claude_invocation_for_request(&request, None, None).expect("invocation");
        let count = invocation
            .args
            .iter()
            .filter(|arg| *arg == "--effort")
            .count();
        assert_eq!(count, 1, "caller-supplied --effort must not be duplicated");
        assert_eq!(effort_value(&invocation.args), Some("max".to_string()));
    }

    #[test]
    fn runtime_contract_path_gets_effort_when_absent() {
        let request = request_with_extras(json!({
            "reasoning_effort": "medium",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": ["--print", "say hi"]
                    }
                }
            }
        }));
        let invocation = claude_invocation_for_request(&request, None, None).expect("invocation");
        assert_eq!(effort_value(&invocation.args), Some("medium".to_string()));
    }

    #[test]
    fn stdin_contract_does_not_split_trailing_flag_pair() {
        // prompt_via_stdin => the argv ends with a flag VALUE (`stream-json`).
        // The `--effort` pair must go to the front, never between
        // `--output-format` and its value.
        let request = request_with_extras(json!({
            "reasoning_effort": "high",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": ["--print", "--output-format", "stream-json"],
                        "prompt_via_stdin": true
                    }
                }
            }
        }));
        let invocation = claude_invocation_for_request(&request, None, None).expect("invocation");
        let fmt_pos = invocation
            .args
            .iter()
            .position(|a| a == "--output-format")
            .expect("--output-format present");
        assert_eq!(
            invocation.args.get(fmt_pos + 1).map(String::as_str),
            Some("stream-json"),
            "--output-format must stay adjacent to its value; got args: {:?}",
            invocation.args
        );
        assert_eq!(effort_value(&invocation.args), Some("high".to_string()));
    }
}

#[cfg(test)]
mod mcp_config_tests {
    use super::*;
    use serde_json::json;

    use super::reasoning_effort_tests::request_with_extras;

    fn request_with_mcp_servers(mcp_servers: Option<serde_json::Value>) -> SessionRequest {
        let mut request = request_with_extras(json!({}));
        request.mcp_servers = mcp_servers;
        request
    }

    fn mcp_config_arg(args: &[String]) -> Option<String> {
        args.iter()
            .position(|arg| arg == "--mcp-config")
            .and_then(|index| args.get(index + 1))
            .cloned()
    }

    #[test]
    fn present_servers_inject_mcp_config_file_path_and_strict_flag() {
        let servers = json!({
            "docs": {
                "command": "npx",
                "args": ["-y", "docs-mcp"],
                "env": { "TOKEN": "t" }
            },
            "linear": { "type": "sse", "url": "https://mcp.linear.app/sse" }
        });
        let request = request_with_mcp_servers(Some(servers.clone()));
        let path = write_claude_mcp_config(&request)
            .expect("write config")
            .expect("config path");
        let invocation =
            claude_invocation_for_request(&request, None, Some(&path)).expect("invocation");

        assert_eq!(invocation.args[0], "--mcp-config");
        assert_eq!(invocation.args[1], path.display().to_string());
        assert_eq!(invocation.args[2], "--strict-mcp-config");
        assert_eq!(invocation.args.last().map(String::as_str), Some("say hi"));

        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read config"))
                .expect("config JSON");
        assert_eq!(written, json!({ "mcpServers": servers }));
        assert!(
            !invocation.args.iter().any(|arg| arg.contains("TOKEN")),
            "secret values must never reach argv"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn mcp_config_file_is_user_only() {
        use std::os::unix::fs::PermissionsExt;
        let request = request_with_mcp_servers(Some(json!({ "docs": { "command": "npx" } })));
        let path = write_claude_mcp_config(&request)
            .expect("write config")
            .expect("config path");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn absent_servers_write_no_config_and_leave_argv_untouched() {
        let request = request_with_mcp_servers(None);
        assert!(write_claude_mcp_config(&request)
            .expect("write config")
            .is_none());
        let baseline = claude_invocation_for_request(&request, None, None).expect("invocation");
        assert!(!baseline.args.iter().any(|arg| arg == "--mcp-config"));
        assert!(!baseline.args.iter().any(|arg| arg == "--strict-mcp-config"));
    }

    #[test]
    fn empty_servers_object_writes_no_config_and_leaves_argv_byte_identical() {
        let empty_request = request_with_mcp_servers(Some(json!({})));
        assert!(write_claude_mcp_config(&empty_request)
            .expect("write config")
            .is_none());
        let baseline = claude_invocation_for_request(&request_with_mcp_servers(None), None, None)
            .expect("invocation");
        let empty = claude_invocation_for_request(&empty_request, None, None).expect("invocation");
        assert_eq!(baseline.args, empty.args);
    }

    #[test]
    fn runtime_contract_mcp_config_is_not_duplicated() {
        let mut request = request_with_extras(json!({
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": ["--print", "--mcp-config", "{\"mcpServers\":{}}", "say hi"]
                    }
                }
            }
        }));
        request.mcp_servers = Some(json!({ "docs": { "command": "npx" } }));
        let invocation =
            claude_invocation_for_request(&request, None, Some(std::path::Path::new("/tmp/x")))
                .expect("invocation");
        let count = invocation
            .args
            .iter()
            .filter(|arg| *arg == "--mcp-config")
            .count();
        assert_eq!(count, 1, "caller-supplied --mcp-config must win");
    }

    #[test]
    fn runtime_contract_path_gets_mcp_config_when_absent() {
        let mut request = request_with_extras(json!({
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": ["--print", "say hi"]
                    }
                }
            }
        }));
        let servers = json!({ "docs": { "command": "npx" } });
        request.mcp_servers = Some(servers.clone());
        let path = write_claude_mcp_config(&request)
            .expect("write config")
            .expect("config path");
        let invocation =
            claude_invocation_for_request(&request, None, Some(&path)).expect("invocation");
        assert_eq!(
            mcp_config_arg(&invocation.args),
            Some(path.display().to_string()),
        );
        assert!(invocation.args.iter().any(|a| a == "--strict-mcp-config"));
        let _ = std::fs::remove_file(&path);
    }
}
