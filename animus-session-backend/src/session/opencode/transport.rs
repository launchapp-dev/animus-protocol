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

use super::parser::parse_opencode_json_line;

pub(crate) async fn start_opencode_session(
    mut request: SessionRequest,
    resume_session_id: Option<String>,
) -> Result<SessionRun> {
    // opencode has no headless approval hook, so approvals ride a voluntary
    // prompt preamble directing the agent to the Animus MCP tools.
    crate::session::apply_approvals_prompt_preamble(&mut request);
    let invocation = opencode_invocation_for_request(&request, resume_session_id.as_deref())?;
    let control_session_id = Uuid::new_v4().to_string();
    let control_session_id_for_run = control_session_id.clone();
    let (event_tx, event_rx) = mpsc::channel(128);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (pid_tx, pid_rx) = oneshot::channel::<Option<u32>>();
    register_session(control_session_id.clone(), cancel_tx);

    tokio::spawn(async move {
        let backend_label = "opencode-native".to_string();
        let session_id_for_event = Some(control_session_id.clone());

        if let Err(error) = run_opencode_session(
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
        selected_backend: "opencode-native".to_string(),
        fallback_reason: None,
        pid,
    })
}

pub(crate) async fn terminate_opencode_session(session_id: &str) -> Result<()> {
    let Some(cancel_tx) = take_session(session_id) else {
        return Err(Error::ExecutionFailed(format!(
            "opencode backend does not track active child process for session '{}'",
            session_id
        )));
    };
    let _ = cancel_tx.send(());
    Ok(())
}

pub(crate) fn opencode_invocation_for_request(
    request: &SessionRequest,
    resume_session_id: Option<&str>,
) -> Result<LaunchInvocation> {
    if let Some(invocation) =
        parse_launch_from_runtime_contract(request.extras.get("runtime_contract"))?
    {
        return Ok(invocation);
    }

    let mut args = vec!["run".to_string()];
    if let Some(session_id) = resume_session_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        args.push("--session".to_string());
        args.push(session_id.to_string());
    }
    if !request.model.trim().is_empty() {
        args.push("-m".to_string());
        args.push(request.model.clone());
    }
    args.push("--format".to_string());
    args.push("json".to_string());
    args.push(request.prompt.clone());

    let mut invocation = LaunchInvocation {
        command: "opencode".to_string(),
        args,
        env: Default::default(),
        prompt_via_stdin: false,
    };
    ensure_flag_value(&mut invocation.args, "--format", "json", 1);
    Ok(invocation)
}

/// Translate one canonical `mcp_servers` entry into the opencode config
/// `mcp` shape: stdio entries become `{"type": "local", "command":
/// [command, ...args], "environment": {...}}`, remote entries become
/// `{"type": "remote", "url": "...", "headers": {...}}`. Entries with
/// neither `url` nor `command` yield `None`.
fn opencode_mcp_entry(entry: &serde_json::Value) -> Option<serde_json::Value> {
    use serde_json::{Map, Value};

    if let Some(url) = entry.get("url").and_then(Value::as_str) {
        let mut out = Map::new();
        out.insert("type".to_string(), Value::String("remote".to_string()));
        out.insert("url".to_string(), Value::String(url.to_string()));
        if let Some(headers) = entry
            .get("headers")
            .and_then(Value::as_object)
            .filter(|headers| !headers.is_empty())
        {
            out.insert("headers".to_string(), Value::Object(headers.clone()));
        }
        out.insert("enabled".to_string(), Value::Bool(true));
        return Some(Value::Object(out));
    }
    let command = entry.get("command").and_then(Value::as_str)?;
    let mut argv = vec![Value::String(command.to_string())];
    if let Some(args) = entry.get("args").and_then(Value::as_array) {
        argv.extend(args.iter().filter(|arg| arg.is_string()).cloned());
    }
    let mut out = Map::new();
    out.insert("type".to_string(), Value::String("local".to_string()));
    out.insert("command".to_string(), Value::Array(argv));
    if let Some(env) = entry
        .get("env")
        .and_then(Value::as_object)
        .filter(|env| !env.is_empty())
    {
        out.insert("environment".to_string(), Value::Object(env.clone()));
    }
    out.insert("enabled".to_string(), Value::Bool(true));
    Some(Value::Object(out))
}

/// Build the JSON config injected via the `OPENCODE_CONFIG_CONTENT` env
/// var (which opencode merges over project config) when the request
/// carries MCP servers, or `None` when `mcp_servers` is absent or empty.
/// Any caller-supplied `OPENCODE_CONFIG_CONTENT` in the request's env
/// vars is preserved, with the per-run servers merged into its `mcp` key.
fn opencode_config_content_for_request(request: &SessionRequest) -> Option<String> {
    use serde_json::{Map, Value};

    let servers = request.mcp_servers_object()?;
    let mut mcp = Map::new();
    for (name, entry) in servers {
        if let Some(translated) = opencode_mcp_entry(entry) {
            mcp.insert(name.clone(), translated);
        }
    }
    if mcp.is_empty() {
        return None;
    }
    let mut config = request
        .env_vars
        .iter()
        .find(|(key, _)| key == "OPENCODE_CONFIG_CONTENT")
        .and_then(|(_, value)| serde_json::from_str::<Value>(value).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let mut merged = config
        .get("mcp")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (name, entry) in mcp {
        merged.insert(name, entry);
    }
    config.insert("mcp".to_string(), Value::Object(merged));
    Some(Value::Object(config).to_string())
}

async fn run_opencode_session(
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
        .envs(request.env_vars.iter().cloned())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(content) = opencode_config_content_for_request(&request) {
        command.env("OPENCODE_CONFIG_CONTENT", content);
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
        .ok_or_else(|| Error::ExecutionFailed("failed to capture opencode stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::ExecutionFailed("failed to capture opencode stderr".to_string()))?;

    let stdout_tx = event_tx.clone();
    let stdout_task = tokio::spawn(async move {
        let mut last_final_text: Option<String> = None;
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            for event in parse_opencode_json_line(&line) {
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

    let exit_code =
        wait_for_child(&mut child, request.timeout_secs, &mut cancel_rx, "opencode").await?;
    let _ = stdout_task.await;
    let _ = stderr_task.await;
    let _ = event_tx.send(SessionEvent::Finished { exit_code }).await;
    Ok(())
}

async fn wait_for_child(
    child: &mut Child,
    timeout_secs: Option<u64>,
    cancel_rx: &mut oneshot::Receiver<()>,
    label: &str,
) -> Result<Option<i32>> {
    match timeout_secs {
        Some(secs) => {
            let timeout_sleep = tokio::time::sleep(Duration::from_secs(secs));
            tokio::pin!(timeout_sleep);
            tokio::select! {
                status = child.wait() => Ok(status?.code()),
                _ = &mut timeout_sleep => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed(format!("{label} session timed out after {secs} seconds")))
                }
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed(format!("{label} session cancelled")))
                }
            }
        }
        None => {
            tokio::select! {
                status = child.wait() => Ok(status?.code()),
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed(format!("{label} session cancelled")))
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

#[cfg(test)]
mod approvals_preamble_tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn request_with_extras(extras: serde_json::Value) -> SessionRequest {
        SessionRequest {
            tool: "opencode".into(),
            model: "anthropic/claude-sonnet-4-5".into(),
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

    #[test]
    fn approvals_preamble_reaches_argv_prompt() {
        let mut request = request_with_extras(json!({ "approvals": true }));
        crate::session::apply_approvals_prompt_preamble(&mut request);
        let invocation = opencode_invocation_for_request(&request, None).expect("invocation");
        let prompt = invocation.args.last().expect("prompt token");
        assert!(
            prompt.starts_with(crate::session::APPROVALS_PROMPT_PREAMBLE),
            "the preamble must lead the prompt; got: {prompt}"
        );
        assert!(prompt.ends_with("say hi"));
    }

    #[test]
    fn absent_approvals_leaves_invocation_byte_identical() {
        let mut request = request_with_extras(json!({}));
        crate::session::apply_approvals_prompt_preamble(&mut request);
        let baseline = opencode_invocation_for_request(&request_with_extras(json!({})), None)
            .expect("invocation");
        let unchanged = opencode_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(baseline.args, unchanged.args);
    }
}

#[cfg(test)]
mod mcp_config_tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn request_with_mcp_servers(mcp_servers: Option<serde_json::Value>) -> SessionRequest {
        SessionRequest {
            tool: "opencode".into(),
            model: "anthropic/claude-sonnet-4-5".into(),
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
    fn stdio_and_remote_servers_translate_to_opencode_mcp_config() {
        let request = request_with_mcp_servers(Some(json!({
            "docs": {
                "command": "npx",
                "args": ["-y", "docs-mcp"],
                "env": { "TOKEN": "t" }
            },
            "linear": {
                "type": "http",
                "url": "https://mcp.linear.app/mcp",
                "headers": { "Authorization": "Bearer x" }
            }
        })));
        let content = opencode_config_content_for_request(&request).expect("config content");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&content).expect("json"),
            json!({
                "mcp": {
                    "docs": {
                        "type": "local",
                        "command": ["npx", "-y", "docs-mcp"],
                        "environment": { "TOKEN": "t" },
                        "enabled": true
                    },
                    "linear": {
                        "type": "remote",
                        "url": "https://mcp.linear.app/mcp",
                        "headers": { "Authorization": "Bearer x" },
                        "enabled": true
                    }
                }
            }),
        );
    }

    #[test]
    fn absent_servers_produce_no_config_content() {
        let request = request_with_mcp_servers(None);
        assert!(opencode_config_content_for_request(&request).is_none());
    }

    #[test]
    fn empty_servers_object_produces_no_config_content() {
        let request = request_with_mcp_servers(Some(json!({})));
        assert!(opencode_config_content_for_request(&request).is_none());
    }

    #[test]
    fn caller_supplied_config_content_is_preserved_and_merged() {
        let mut request = request_with_mcp_servers(Some(json!({
            "docs": { "command": "npx" }
        })));
        request.env_vars.push((
            "OPENCODE_CONFIG_CONTENT".to_string(),
            r#"{"theme":"dark","mcp":{"corp":{"type":"remote","url":"https://corp.example/mcp"}}}"#
                .to_string(),
        ));
        let content = opencode_config_content_for_request(&request).expect("config content");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&content).expect("json"),
            json!({
                "theme": "dark",
                "mcp": {
                    "corp": { "type": "remote", "url": "https://corp.example/mcp" },
                    "docs": { "type": "local", "command": ["npx"], "enabled": true }
                }
            }),
        );
    }

    #[test]
    fn argv_is_unchanged_by_mcp_servers() {
        let baseline = opencode_invocation_for_request(&request_with_mcp_servers(None), None)
            .expect("invocation");
        let with_servers = opencode_invocation_for_request(
            &request_with_mcp_servers(Some(json!({ "docs": { "command": "npx" } }))),
            None,
        )
        .expect("invocation");
        assert_eq!(baseline.args, with_servers.args);
    }
}
