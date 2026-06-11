use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::cli::{ensure_flag_value, parse_launch_from_runtime_contract, LaunchInvocation};
use crate::error::{Error, Result};
use crate::session::{
    session_event::SessionEvent, session_request::SessionRequest, session_run::SessionRun,
};

use super::parser::parse_gemini_json_chunk;

pub(crate) async fn start_gemini_session(
    request: SessionRequest,
    resume_session_id: Option<String>,
) -> Result<SessionRun> {
    let invocation = gemini_invocation_for_request(&request, resume_session_id.as_deref())?;
    let control_session_id = Uuid::new_v4().to_string();
    let control_session_id_for_run = control_session_id.clone();
    let (event_tx, event_rx) = mpsc::channel(128);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (pid_tx, pid_rx) = oneshot::channel::<Option<u32>>();
    register_session(control_session_id.clone(), cancel_tx);

    tokio::spawn(async move {
        let backend_label = "gemini-native".to_string();
        let session_id_for_event = Some(control_session_id.clone());

        if let Err(error) = run_gemini_session(
            request,
            invocation,
            event_tx.clone(),
            cancel_rx,
            pid_tx,
            backend_label,
            session_id_for_event,
        )
        .await
        {
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
        selected_backend: "gemini-native".to_string(),
        fallback_reason: None,
        pid,
    })
}

pub(crate) async fn terminate_gemini_session(session_id: &str) -> Result<()> {
    let Some(cancel_tx) = take_session(session_id) else {
        return Err(Error::ExecutionFailed(format!(
            "gemini backend does not track active child process for session '{}'",
            session_id
        )));
    };
    let _ = cancel_tx.send(());
    Ok(())
}

/// Map a protocol permission mode onto Gemini's `--approval-mode` values
/// (`default`, `auto_edit`, `yolo`, `plan`). Gemini-native values pass
/// through; unknown values yield `None` so the caller falls back to
/// `--yolo` (headless autonomy) instead of crashing CLI argument parsing.
fn gemini_approval_mode(permission_mode: &str) -> Option<&'static str> {
    match permission_mode {
        "default" => Some("default"),
        "plan" => Some("plan"),
        "acceptEdits" | "auto_edit" => Some("auto_edit"),
        "bypassPermissions" | "yolo" => Some("yolo"),
        _ => None,
    }
}

pub(crate) fn gemini_invocation_for_request(
    request: &SessionRequest,
    resume_session_id: Option<&str>,
) -> Result<LaunchInvocation> {
    if let Some(invocation) =
        parse_launch_from_runtime_contract(request.extras.get("runtime_contract"))?
    {
        return Ok(invocation);
    }

    let mut args = Vec::new();

    if let Some(session_id) = resume_session_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        args.push("--resume".to_string());
        args.push(session_id.to_string());
    } else if let Some(session_id) = configured_gemini_session_id(request) {
        args.push("--resume".to_string());
        args.push(session_id);
    }

    if let Some(approval_mode) = request
        .permission_mode
        .as_deref()
        .map(str::trim)
        .and_then(gemini_approval_mode)
    {
        args.push("--approval-mode".to_string());
        args.push(approval_mode.to_string());
    } else {
        args.push("--yolo".to_string());
    }

    if !request.model.trim().is_empty() {
        args.push("--model".to_string());
        args.push(request.model.clone());
    }

    args.push("--output-format".to_string());
    args.push("json".to_string());
    args.push("-p".to_string());
    args.push(request.prompt.clone());

    let mut invocation = LaunchInvocation {
        command: "gemini".to_string(),
        args,
        env: Default::default(),
        prompt_via_stdin: false,
    };
    let insert_at = invocation.args.len();
    ensure_flag_value(&mut invocation.args, "--output-format", "json", insert_at);

    Ok(invocation)
}

/// Build the settings JSON injected via `GEMINI_CLI_SYSTEM_SETTINGS_PATH`
/// when the request carries MCP servers, or `None` when `mcp_servers` is
/// absent or empty. The Gemini CLI settings `mcpServers` schema accepts
/// the canonical entries unchanged (`command`/`args`/`env` for stdio,
/// `type` + `url` + `headers` for remote), so they pass through as-is.
/// `existing` is the current system settings document (if any); its keys
/// are preserved, with the per-run servers shallow-merged into
/// `mcpServers` (per-run entries win on name conflicts).
fn gemini_settings_with_mcp_servers(
    request: &SessionRequest,
    existing: Option<&str>,
) -> Option<String> {
    let servers = request.mcp_servers_object()?;
    let mut settings = existing
        .and_then(|content| serde_json::from_str::<serde_json::Value>(content).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let mut merged = settings
        .get("mcpServers")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (name, entry) in servers {
        merged.insert(name.clone(), entry.clone());
    }
    settings.insert("mcpServers".to_string(), serde_json::Value::Object(merged));
    Some(serde_json::Value::Object(settings).to_string())
}

/// Resolve the system settings file the Gemini CLI would load for this
/// run: an explicit `GEMINI_CLI_SYSTEM_SETTINGS_PATH` from the request's
/// env vars or the parent environment, falling back to the CLI's
/// platform default.
fn gemini_system_settings_path(request: &SessionRequest) -> std::path::PathBuf {
    if let Some((_, value)) = request
        .env_vars
        .iter()
        .find(|(key, _)| key == "GEMINI_CLI_SYSTEM_SETTINGS_PATH")
    {
        return std::path::PathBuf::from(value);
    }
    if let Ok(value) = std::env::var("GEMINI_CLI_SYSTEM_SETTINGS_PATH") {
        return std::path::PathBuf::from(value);
    }
    if cfg!(target_os = "macos") {
        std::path::PathBuf::from("/Library/Application Support/GeminiCli/settings.json")
    } else if cfg!(windows) {
        std::path::PathBuf::from("C:\\ProgramData\\gemini-cli\\settings.json")
    } else {
        std::path::PathBuf::from("/etc/gemini-cli/settings.json")
    }
}

/// Write the per-run settings file carrying `mcp_servers` (merged with any
/// existing system settings) into the OS temp dir and return its path, or
/// `None` when the request carries no MCP servers. The file is removed
/// once the session finishes.
fn write_gemini_mcp_settings(request: &SessionRequest) -> Result<Option<std::path::PathBuf>> {
    let existing = std::fs::read_to_string(gemini_system_settings_path(request)).ok();
    let Some(settings) = gemini_settings_with_mcp_servers(request, existing.as_deref()) else {
        return Ok(None);
    };
    let path = std::env::temp_dir().join(format!("animus-gemini-settings-{}.json", Uuid::new_v4()));
    write_private_file(&path, &settings)?;
    Ok(Some(path))
}

/// Write `contents` to a fresh file readable only by the current user
/// (mode `0600` on Unix) so secret-bearing MCP config is not exposed to
/// other local users via the shared temp dir.
fn write_private_file(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(contents.as_bytes())
}

async fn run_gemini_session(
    request: SessionRequest,
    invocation: LaunchInvocation,
    event_tx: mpsc::Sender<SessionEvent>,
    mut cancel_rx: oneshot::Receiver<()>,
    pid_tx: oneshot::Sender<Option<u32>>,
    backend: String,
    session_id: Option<String>,
) -> Result<()> {
    let mcp_settings_path = write_gemini_mcp_settings(&request)?;
    let mut command = Command::new(&invocation.command);
    command
        .args(&invocation.args)
        .current_dir(&request.cwd)
        .env_remove("CLAUDECODE")
        .env_remove("CLAUDE_CODE_ENTRYPOINT")
        .env_remove("CLAUDE_CODE_SESSION_ACCESS_TOKEN")
        .env_remove("CLAUDE_CODE_SESSION_ID")
        // Gemini CLI refuses to run in an "untrusted" directory in headless
        // mode (exit 55) unless the workspace is trusted. Animus always
        // launches gemini against the caller's own project root in an
        // automated context, so opt into trust here — the documented
        // headless escape hatch — rather than requiring every operator to
        // pre-trust each directory interactively.
        .env("GEMINI_CLI_TRUST_WORKSPACE", "true")
        .envs(request.env_vars.iter().cloned())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(path) = &mcp_settings_path {
        command.env("GEMINI_CLI_SYSTEM_SETTINGS_PATH", path);
    }
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
        .ok_or_else(|| Error::ExecutionFailed("failed to capture gemini stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::ExecutionFailed("failed to capture gemini stderr".to_string()))?;

    let stdout_tx = event_tx.clone();
    let stdout_task = tokio::spawn(async move {
        let mut last_final_text: Option<String> = None;
        let mut lines = BufReader::new(stdout).lines();
        let mut json_accum = String::new();
        let mut depth = 0i32;
        let mut accumulating = false;

        while let Ok(Some(line)) = lines.next_line().await {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let is_json_line = serde_json::from_str::<serde_json::Value>(trimmed).is_ok();
            if !is_json_line && (trimmed == "{" || accumulating) {
                if trimmed == "{" && !accumulating {
                    accumulating = true;
                    json_accum.clear();
                    depth = 0;
                }
                json_accum.push_str(trimmed);
                json_accum.push('\n');
                for ch in trimmed.chars() {
                    match ch {
                        '{' => depth += 1,
                        '}' => depth -= 1,
                        _ => {}
                    }
                }
                if depth <= 0 {
                    accumulating = false;
                    emit_gemini_events(&json_accum, &stdout_tx, &mut last_final_text).await;
                    json_accum.clear();
                    depth = 0;
                }
                continue;
            }

            emit_gemini_events(trimmed, &stdout_tx, &mut last_final_text).await;
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

    let exit_result = wait_for_gemini_child(&mut child, request.timeout_secs, &mut cancel_rx).await;

    let _ = stdout_task.await;
    let _ = stderr_task.await;

    if let Some(path) = &mcp_settings_path {
        let _ = std::fs::remove_file(path);
    }

    let exit_code = exit_result?;

    let _ = event_tx.send(SessionEvent::Finished { exit_code }).await;

    Ok(())
}

async fn emit_gemini_events(
    chunk: &str,
    tx: &mpsc::Sender<SessionEvent>,
    last_final_text: &mut Option<String>,
) {
    for event in parse_gemini_json_chunk(chunk) {
        if let SessionEvent::FinalText { text } = &event {
            if last_final_text.as_deref() == Some(text.as_str()) {
                continue;
            }
            *last_final_text = Some(text.clone());
        }
        let _ = tx.send(event).await;
    }
}

async fn wait_for_gemini_child(
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
                        "gemini session timed out after {} seconds",
                        secs
                    )))
                }
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed("gemini session cancelled".to_string()))
                }
            }
        }
        None => {
            tokio::select! {
                status = child.wait() => Ok(status?.code()),
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed("gemini session cancelled".to_string()))
                }
            }
        }
    }
}

fn configured_gemini_session_id(request: &SessionRequest) -> Option<String> {
    request
        .extras
        .pointer("/runtime_contract/cli/session/session_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
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

#[cfg(test)]
mod mcp_settings_tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn request_with_mcp_servers(mcp_servers: Option<serde_json::Value>) -> SessionRequest {
        SessionRequest {
            tool: "gemini".into(),
            model: "gemini-2.5-pro".into(),
            prompt: "say hi".into(),
            cwd: PathBuf::from("."),
            project_root: None,
            mcp_endpoint: None,
            mcp_servers,
            permission_mode: None,
            timeout_secs: None,
            env_vars: Vec::new(),
            extras: json!({}),
        }
    }

    #[test]
    fn present_servers_produce_settings_document() {
        let servers = json!({
            "docs": {
                "command": "npx",
                "args": ["-y", "docs-mcp"],
                "env": { "TOKEN": "t" }
            },
            "linear": { "type": "http", "url": "https://mcp.linear.app/mcp" }
        });
        let request = request_with_mcp_servers(Some(servers.clone()));
        let settings = gemini_settings_with_mcp_servers(&request, None).expect("settings");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&settings).expect("json"),
            json!({ "mcpServers": servers }),
        );
    }

    #[test]
    fn existing_system_settings_are_preserved_and_merged() {
        let request = request_with_mcp_servers(Some(json!({
            "docs": { "command": "npx" }
        })));
        let existing = r#"{"tools":{"sandbox":false},"mcpServers":{"corp":{"url":"https://corp.example/mcp"}}}"#;
        let settings =
            gemini_settings_with_mcp_servers(&request, Some(existing)).expect("settings");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&settings).expect("json"),
            json!({
                "tools": { "sandbox": false },
                "mcpServers": {
                    "corp": { "url": "https://corp.example/mcp" },
                    "docs": { "command": "npx" }
                }
            }),
        );
    }

    #[test]
    fn absent_servers_produce_no_settings() {
        let request = request_with_mcp_servers(None);
        assert!(gemini_settings_with_mcp_servers(&request, None).is_none());
        assert!(write_gemini_mcp_settings(&request)
            .expect("no settings write")
            .is_none());
    }

    #[test]
    fn empty_servers_object_produces_no_settings() {
        let request = request_with_mcp_servers(Some(json!({})));
        assert!(gemini_settings_with_mcp_servers(&request, None).is_none());
    }

    #[test]
    fn argv_is_unchanged_by_mcp_servers() {
        let baseline = gemini_invocation_for_request(&request_with_mcp_servers(None), None)
            .expect("invocation");
        let with_servers = gemini_invocation_for_request(
            &request_with_mcp_servers(Some(json!({ "docs": { "command": "npx" } }))),
            None,
        )
        .expect("invocation");
        assert_eq!(baseline.args, with_servers.args);
    }

    #[test]
    fn permission_modes_map_to_gemini_approval_modes() {
        for (protocol, gemini) in [
            ("default", "default"),
            ("plan", "plan"),
            ("acceptEdits", "auto_edit"),
            ("auto_edit", "auto_edit"),
            ("bypassPermissions", "yolo"),
            ("yolo", "yolo"),
        ] {
            let mut request = request_with_mcp_servers(None);
            request.permission_mode = Some(protocol.to_string());
            let invocation = gemini_invocation_for_request(&request, None).expect("invocation");
            let position = invocation
                .args
                .iter()
                .position(|arg| arg == "--approval-mode")
                .unwrap_or_else(|| panic!("--approval-mode missing for {protocol}"));
            assert_eq!(invocation.args[position + 1], gemini);
        }
    }

    #[test]
    fn unknown_permission_mode_falls_back_to_yolo() {
        let mut request = request_with_mcp_servers(None);
        request.permission_mode = Some("definitely-not-a-mode".to_string());
        let invocation = gemini_invocation_for_request(&request, None).expect("invocation");
        assert!(!invocation.args.iter().any(|arg| arg == "--approval-mode"));
        assert!(invocation.args.iter().any(|arg| arg == "--yolo"));
    }
}
