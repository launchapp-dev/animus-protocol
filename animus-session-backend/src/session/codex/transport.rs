use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::cli::{
    ensure_codex_config_override, ensure_flag, parse_launch_from_runtime_contract, LaunchInvocation,
};
use crate::error::{Error, Result};
use crate::session::{
    session_event::SessionEvent, session_request::SessionRequest, session_run::SessionRun,
};

use super::parser::CodexParser;

pub(crate) async fn start_codex_session(
    mut request: SessionRequest,
    resume_session_id: Option<&str>,
) -> Result<SessionRun> {
    // Codex has no exec-mode approval hook, so approvals ride a voluntary
    // prompt preamble directing the agent to the Animus MCP tools.
    crate::session::apply_approvals_prompt_preamble(&mut request);
    let invocation = codex_invocation_for_request(&request, resume_session_id)?;
    let control_session_id = Uuid::new_v4().to_string();
    let control_session_id_for_run = control_session_id.clone();
    let (event_tx, event_rx) = mpsc::channel(128);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (pid_tx, pid_rx) = oneshot::channel::<Option<u32>>();
    register_session(control_session_id.clone(), cancel_tx);

    tokio::spawn(async move {
        let backend_label = "codex-native".to_string();
        let session_id_for_event = Some(control_session_id.clone());

        if let Err(error) = run_codex_session(
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
        selected_backend: "codex-native".to_string(),
        fallback_reason: None,
        pid,
    })
}

pub(crate) async fn terminate_codex_session(session_id: &str) -> Result<()> {
    let Some(cancel_tx) = take_session(session_id) else {
        return Err(Error::ExecutionFailed(format!(
            "codex backend does not track active child process for session '{}'",
            session_id
        )));
    };
    let _ = cancel_tx.send(());
    Ok(())
}

/// Normalize a `reasoning_effort` extras value to the Codex
/// `model_reasoning_effort` level. Codex accepts `low`, `medium`, and
/// `high`; anything else (including an empty string) yields `None` so the
/// flag is omitted and Codex falls back to its own default.
fn reasoning_effort_to_codex(level: &str) -> Option<&'static str> {
    match level.trim().to_ascii_lowercase().as_str() {
        "low" => Some("low"),
        "medium" => Some("medium"),
        "high" => Some("high"),
        _ => None,
    }
}

/// Apply `extras.reasoning_effort` to a Codex argv as a
/// `-c model_reasoning_effort="<level>"` override.
///
/// The `-c` pair is inserted into the options region (immediately after the
/// `exec` subcommand, or at the front when absent) rather than next to the
/// trailing argv token. The trailing token is not guaranteed to be the
/// prompt — a runtime-contract launch may send the prompt via stdin, leaving
/// a flag value (e.g. `--model gpt-5`) last — so prompt-relative insertion
/// could split a flag pair.
///
/// A user-supplied override (an existing `-c model_reasoning_effort=...`
/// token, e.g. from a `--context-json` runtime contract) wins: this only
/// inserts the level when no such override is already present.
fn apply_codex_reasoning_effort(args: &mut Vec<String>, request: &SessionRequest) {
    let Some(level) = request
        .extras
        .get("reasoning_effort")
        .and_then(serde_json::Value::as_str)
        .and_then(reasoning_effort_to_codex)
    else {
        return;
    };
    if codex_reasoning_effort_already_set(args) {
        return;
    }
    let insert_at = args
        .iter()
        .position(|token| token == "exec")
        .map(|index| index + 1)
        .unwrap_or(0);
    args.insert(insert_at, "-c".to_string());
    args.insert(insert_at + 1, format!("model_reasoning_effort=\"{level}\""));
}

/// True when the argv already carries a `-c model_reasoning_effort=...`
/// override (so a caller-provided value is never clobbered).
fn codex_reasoning_effort_already_set(args: &[String]) -> bool {
    let mut index = 0usize;
    while index + 1 < args.len() {
        if matches!(args[index].as_str(), "-c" | "--config")
            && args[index + 1].starts_with("model_reasoning_effort=")
        {
            return true;
        }
        index += 1;
    }
    false
}

/// Encode a string as a TOML basic string (double-quoted, with `\`, `"`,
/// and control characters escaped) for use inside a Codex `-c` override
/// value, which Codex parses as TOML.
fn toml_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Encode a TOML key, quoting it when it is not a bare key
/// (`A-Za-z0-9_-`).
fn toml_key(name: &str) -> String {
    let bare = !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if bare {
        name.to_string()
    } else {
        toml_string(name)
    }
}

/// Encode a JSON object of string values as a TOML inline table
/// (`{KEY="v", K2="v2"}`). Non-string values are skipped.
fn toml_inline_table(map: &serde_json::Map<String, serde_json::Value>) -> String {
    let entries: Vec<String> = map
        .iter()
        .filter_map(|(key, value)| {
            value
                .as_str()
                .map(|text| format!("{}={}", toml_key(key), toml_string(text)))
        })
        .collect();
    format!("{{{}}}", entries.join(", "))
}

/// Encode a JSON array of strings as a TOML array (`["a", "b"]`).
/// Non-string elements are skipped.
fn toml_string_array(values: &[serde_json::Value]) -> String {
    let entries: Vec<String> = values
        .iter()
        .filter_map(|value| value.as_str().map(toml_string))
        .collect();
    format!("[{}]", entries.join(", "))
}

/// Deterministic env-var name carrying one HTTP header value to the
/// Codex CLI through the child environment instead of argv. The name ends
/// in an FNV-1a hash of the raw `(server, header)` pair so distinct names
/// that sanitize identically (e.g. `foo-bar` vs `foo_bar`) can never
/// collide and route one server's secret to another.
fn header_env_var_name(server: &str, header: &str) -> String {
    let sanitize = |raw: &str| -> String {
        raw.chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect()
    };
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in server
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0u8))
        .chain(header.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!(
        "ANIMUS_MCP_{}_HDR_{}_{:08X}",
        sanitize(server),
        sanitize(header),
        hash as u32
    )
}

/// Build the `-c` override values plus the secret-carrying env pairs for
/// one canonical `mcp_servers` entry. Remote entries (`url`) become
/// `mcp_servers.<name>.url` plus `env_http_headers` (header values travel
/// through the child environment, never argv); stdio entries become
/// `command`/`args` plus `env_vars` (env values likewise travel through
/// the child environment and are forwarded by name). Entries with
/// neither `url` nor `command` yield nothing.
fn codex_mcp_overrides_for_server(
    name: &str,
    entry: &serde_json::Value,
) -> (Vec<String>, Vec<(String, String)>) {
    let key = toml_key(name);
    let mut overrides = Vec::new();
    let mut env_pairs = Vec::new();
    if let Some(url) = entry.get("url").and_then(serde_json::Value::as_str) {
        overrides.push(format!("mcp_servers.{key}.url={}", toml_string(url)));
        if let Some(headers) = entry
            .get("headers")
            .and_then(serde_json::Value::as_object)
            .filter(|headers| !headers.is_empty())
        {
            let mut mapping = serde_json::Map::new();
            for (header, value) in headers {
                let Some(value) = value.as_str() else {
                    continue;
                };
                let env_name = header_env_var_name(name, header);
                mapping.insert(header.clone(), serde_json::Value::String(env_name.clone()));
                env_pairs.push((env_name, value.to_string()));
            }
            if !mapping.is_empty() {
                overrides.push(format!(
                    "mcp_servers.{key}.env_http_headers={}",
                    toml_inline_table(&mapping)
                ));
            }
        }
        return (overrides, env_pairs);
    }
    let Some(command) = entry.get("command").and_then(serde_json::Value::as_str) else {
        return (overrides, env_pairs);
    };
    overrides.push(format!(
        "mcp_servers.{key}.command={}",
        toml_string(command)
    ));
    if let Some(args) = entry
        .get("args")
        .and_then(serde_json::Value::as_array)
        .filter(|args| !args.is_empty())
    {
        overrides.push(format!(
            "mcp_servers.{key}.args={}",
            toml_string_array(args)
        ));
    }
    if let Some(env) = entry
        .get("env")
        .and_then(serde_json::Value::as_object)
        .filter(|env| !env.is_empty())
    {
        let mut names = Vec::new();
        for (env_key, value) in env {
            let Some(value) = value.as_str() else {
                continue;
            };
            names.push(serde_json::Value::String(env_key.clone()));
            env_pairs.push((env_key.clone(), value.to_string()));
        }
        if !names.is_empty() {
            overrides.push(format!(
                "mcp_servers.{key}.env_vars={}",
                toml_string_array(&names)
            ));
        }
    }
    (overrides, env_pairs)
}

/// Apply `mcp_servers` to a Codex invocation as per-server
/// `-c mcp_servers.<name>.*=<toml>` overrides, inserted into the options
/// region (after `exec`, or at the front when absent) for the same reason
/// as the reasoning-effort override, with secret values routed through
/// the invocation env rather than argv. Servers are emitted in name order
/// so the argv is deterministic. A caller-supplied `mcp_servers.*`
/// override (e.g. from a runtime contract) suppresses injection entirely,
/// and an absent or empty `mcp_servers` object changes nothing.
///
/// Stdio env values are forwarded by NAME through the codex process
/// environment, so two servers declaring the same env key with different
/// values cannot both be satisfied; that case fails validation instead of
/// silently routing one server's secret to the other.
fn apply_codex_mcp_servers(
    args: &mut Vec<String>,
    env: &mut std::collections::BTreeMap<String, String>,
    request: &SessionRequest,
) -> Result<()> {
    let Some(servers) = request.mcp_servers_object() else {
        return Ok(());
    };
    if codex_mcp_servers_already_set(args) {
        return Ok(());
    }
    let mut names: Vec<&String> = servers.keys().collect();
    names.sort();
    let mut insert_at = args
        .iter()
        .position(|token| token == "exec")
        .map(|index| index + 1)
        .unwrap_or(0);
    for name in names {
        let (overrides, env_pairs) = codex_mcp_overrides_for_server(name, &servers[name]);
        for override_value in overrides {
            args.insert(insert_at, "-c".to_string());
            args.insert(insert_at + 1, override_value);
            insert_at += 2;
        }
        for (env_name, value) in env_pairs {
            let conflicting = env
                .get(&env_name)
                .map(|existing| existing != &value)
                .unwrap_or(false)
                || request
                    .env_vars
                    .iter()
                    .any(|(key, existing)| key == &env_name && existing != &value);
            if conflicting {
                return Err(Error::ValidationFailed(format!(
                    "mcp_servers env key '{env_name}' conflicts with another value for the \
                     same name (another server's declaration or the request env); the codex \
                     transport forwards env values by name and cannot route both"
                )));
            }
            env.insert(env_name, value);
        }
    }
    Ok(())
}

/// True when the argv already carries a `-c mcp_servers.*` override (so a
/// caller-provided MCP configuration is never clobbered).
fn codex_mcp_servers_already_set(args: &[String]) -> bool {
    let mut index = 0usize;
    while index + 1 < args.len() {
        if matches!(args[index].as_str(), "-c" | "--config")
            && args[index + 1].starts_with("mcp_servers.")
        {
            return true;
        }
        index += 1;
    }
    false
}

pub(crate) fn codex_invocation_for_request(
    request: &SessionRequest,
    resume_session_id: Option<&str>,
) -> Result<LaunchInvocation> {
    if let Some(mut invocation) =
        parse_launch_from_runtime_contract(request.extras.get("runtime_contract"))?
    {
        apply_codex_reasoning_effort(&mut invocation.args, request);
        apply_codex_mcp_servers(&mut invocation.args, &mut invocation.env, request)?;
        return Ok(invocation);
    }

    let mut args = vec!["exec".to_string()];
    if let Some(raw) = resume_session_id {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(Error::ValidationFailed(
                "codex resume requested with empty session id".to_string(),
            ));
        }
        args.push("resume".to_string());
        args.push(trimmed.to_string());
    }
    args.push("--json".to_string());
    args.push("--full-auto".to_string());
    args.push("--skip-git-repo-check".to_string());

    if let Some(permission_mode) = request
        .permission_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        ensure_codex_config_override(
            &mut args,
            "approval_policy",
            &format!("\"{permission_mode}\""),
        );
    }

    ensure_codex_config_override(&mut args, "sandbox_workspace_write.network_access", "true");

    if !request.model.trim().is_empty() {
        args.push("--model".to_string());
        args.push(request.model.clone());
    }

    args.push(request.prompt.clone());

    apply_codex_reasoning_effort(&mut args, request);

    let mut invocation = LaunchInvocation {
        command: "codex".to_string(),
        args,
        env: Default::default(),
        prompt_via_stdin: false,
    };
    apply_codex_mcp_servers(&mut invocation.args, &mut invocation.env, request)?;
    ensure_flag(&mut invocation.args, "--json", 1);

    Ok(invocation)
}

async fn run_codex_session(
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
        .ok_or_else(|| Error::ExecutionFailed("failed to capture codex stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::ExecutionFailed("failed to capture codex stderr".to_string()))?;

    let stdout_tx = event_tx.clone();
    let stdout_task = tokio::spawn(async move {
        let mut last_final_text: Option<String> = None;
        let mut parser = CodexParser::new();
        let mut lines = BufReader::new(stdout).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            for event in parser.parse_line(&line) {
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

    let exit_code = wait_for_codex_child(&mut child, request.timeout_secs, &mut cancel_rx).await?;

    let _ = stdout_task.await;
    let _ = stderr_task.await;

    let _ = event_tx.send(SessionEvent::Finished { exit_code }).await;

    Ok(())
}

async fn wait_for_codex_child(
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
                        "codex session timed out after {} seconds",
                        secs
                    )))
                }
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed("codex session cancelled".to_string()))
                }
            }
        }
        None => {
            tokio::select! {
                status = child.wait() => Ok(status?.code()),
                _ = cancel_rx => {
                    crate::session::kill_and_reap_child(child).await;
                    Err(Error::ExecutionFailed("codex session cancelled".to_string()))
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
mod reasoning_effort_tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    pub(super) fn request_with_extras(extras: serde_json::Value) -> SessionRequest {
        SessionRequest {
            tool: "codex".into(),
            model: "gpt-5-codex".into(),
            prompt: "say hi".into(),
            cwd: PathBuf::from("."),
            project_root: None,
            mcp_endpoint: None,
            mcp_servers: None,
            permission_mode: None,
            timeout_secs: None,
            env_vars: Vec::new(),
            extras,
            actor: None,
        }
    }

    fn override_value(args: &[String], key: &str) -> Option<String> {
        let prefix = format!("{key}=");
        let mut index = 0usize;
        while index + 1 < args.len() {
            if matches!(args[index].as_str(), "-c" | "--config")
                && args[index + 1].starts_with(&prefix)
            {
                return Some(args[index + 1][prefix.len()..].to_string());
            }
            index += 1;
        }
        None
    }

    #[test]
    fn bare_args_inject_reasoning_effort_override() {
        let request = request_with_extras(json!({ "reasoning_effort": "high" }));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            override_value(&invocation.args, "model_reasoning_effort"),
            Some("\"high\"".to_string()),
        );
        // Prompt stays the final argv token.
        assert_eq!(invocation.args.last().map(String::as_str), Some("say hi"));
    }

    #[test]
    fn absent_reasoning_effort_leaves_args_unchanged() {
        let request = request_with_extras(json!({}));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert!(override_value(&invocation.args, "model_reasoning_effort").is_none());
    }

    #[test]
    fn unknown_level_is_ignored() {
        let request = request_with_extras(json!({ "reasoning_effort": "turbo" }));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert!(override_value(&invocation.args, "model_reasoning_effort").is_none());
    }

    #[test]
    fn user_supplied_override_is_not_clobbered() {
        let request = request_with_extras(json!({
            "reasoning_effort": "low",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "codex",
                        "args": [
                            "exec",
                            "-c",
                            "model_reasoning_effort=\"high\"",
                            "say hi"
                        ]
                    }
                }
            }
        }));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            override_value(&invocation.args, "model_reasoning_effort"),
            Some("\"high\"".to_string()),
            "caller-supplied override must win over extras.reasoning_effort"
        );
    }

    #[test]
    fn runtime_contract_path_gets_effort_when_absent() {
        let request = request_with_extras(json!({
            "reasoning_effort": "medium",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "codex",
                        "args": ["exec", "say hi"]
                    }
                }
            }
        }));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            override_value(&invocation.args, "model_reasoning_effort"),
            Some("\"medium\"".to_string()),
        );
    }

    #[test]
    fn stdin_contract_does_not_split_trailing_flag_pair() {
        // prompt_via_stdin => the argv ends with a flag VALUE (`gpt-5`), not a
        // prompt. The `-c` pair must land in the options region after `exec`,
        // never between `--model` and its value.
        let request = request_with_extras(json!({
            "reasoning_effort": "high",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "codex",
                        "args": ["exec", "--model", "gpt-5"],
                        "prompt_via_stdin": true
                    }
                }
            }
        }));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        let model_pos = invocation
            .args
            .iter()
            .position(|a| a == "--model")
            .expect("--model present");
        assert_eq!(
            invocation.args.get(model_pos + 1).map(String::as_str),
            Some("gpt-5"),
            "--model must stay adjacent to its value; got args: {:?}",
            invocation.args
        );
        assert_eq!(
            override_value(&invocation.args, "model_reasoning_effort"),
            Some("\"high\"".to_string()),
        );
    }
}

#[cfg(test)]
mod approvals_preamble_tests {
    use super::*;
    use serde_json::json;

    use super::reasoning_effort_tests::request_with_extras;

    #[test]
    fn approvals_preamble_reaches_argv_prompt() {
        let mut request = request_with_extras(json!({ "approvals": true }));
        crate::session::apply_approvals_prompt_preamble(&mut request);
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
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
        let baseline = codex_invocation_for_request(&request_with_extras(json!({})), None)
            .expect("invocation");
        let unchanged = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(baseline.args, unchanged.args);
    }
}

#[cfg(test)]
mod mcp_servers_tests {
    use super::*;
    use serde_json::json;

    use super::reasoning_effort_tests::request_with_extras;

    fn request_with_mcp_servers(mcp_servers: Option<serde_json::Value>) -> SessionRequest {
        let mut request = request_with_extras(json!({}));
        request.mcp_servers = mcp_servers;
        request
    }

    fn config_overrides(args: &[String]) -> Vec<String> {
        let mut overrides = Vec::new();
        let mut index = 0usize;
        while index + 1 < args.len() {
            if matches!(args[index].as_str(), "-c" | "--config")
                && args[index + 1].starts_with("mcp_servers.")
            {
                overrides.push(args[index + 1].clone());
            }
            index += 1;
        }
        overrides
    }

    #[test]
    fn stdio_server_emits_command_args_and_env_var_forwarding() {
        let request = request_with_mcp_servers(Some(json!({
            "docs": {
                "command": "npx",
                "args": ["-y", "docs-mcp"],
                "env": { "TOKEN": "t" }
            }
        })));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            config_overrides(&invocation.args),
            vec![
                "mcp_servers.docs.command=\"npx\"".to_string(),
                "mcp_servers.docs.args=[\"-y\", \"docs-mcp\"]".to_string(),
                "mcp_servers.docs.env_vars=[\"TOKEN\"]".to_string(),
            ],
        );
        assert_eq!(invocation.env.get("TOKEN").map(String::as_str), Some("t"));
        assert!(
            !invocation
                .args
                .iter()
                .any(|arg| arg.contains("TOKEN=\"t\"")),
            "secret env values must never reach argv"
        );
        assert_eq!(invocation.args.first().map(String::as_str), Some("exec"));
        assert_eq!(invocation.args.get(1).map(String::as_str), Some("-c"));
        assert_eq!(invocation.args.last().map(String::as_str), Some("say hi"));
    }

    #[test]
    fn remote_server_emits_url_and_env_http_headers() {
        let request = request_with_mcp_servers(Some(json!({
            "linear": {
                "type": "http",
                "url": "https://mcp.linear.app/mcp",
                "headers": { "Authorization": "Bearer x" }
            }
        })));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        let env_name = header_env_var_name("linear", "Authorization");
        assert!(env_name.starts_with("ANIMUS_MCP_LINEAR_HDR_AUTHORIZATION_"));
        assert_eq!(
            config_overrides(&invocation.args),
            vec![
                "mcp_servers.linear.url=\"https://mcp.linear.app/mcp\"".to_string(),
                format!("mcp_servers.linear.env_http_headers={{Authorization=\"{env_name}\"}}"),
            ],
        );
        assert_eq!(
            invocation.env.get(&env_name).map(String::as_str),
            Some("Bearer x"),
        );
        assert!(
            !invocation.args.iter().any(|arg| arg.contains("Bearer x")),
            "secret header values must never reach argv"
        );
    }

    #[test]
    fn header_env_names_do_not_collide_after_sanitization() {
        let a = header_env_var_name("foo-bar", "Authorization");
        let b = header_env_var_name("foo_bar", "Authorization");
        assert_ne!(a, b, "sanitized-identical names must stay distinct");
        assert_eq!(
            a,
            header_env_var_name("foo-bar", "Authorization"),
            "names must be deterministic"
        );
    }

    #[test]
    fn servers_are_emitted_in_name_order_with_toml_escaping() {
        let request = request_with_mcp_servers(Some(json!({
            "b server": { "command": "run \"it\"\\now" },
            "a": { "command": "tool" }
        })));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            config_overrides(&invocation.args),
            vec![
                "mcp_servers.a.command=\"tool\"".to_string(),
                "mcp_servers.\"b server\".command=\"run \\\"it\\\"\\\\now\"".to_string(),
            ],
        );
    }

    #[test]
    fn conflicting_stdio_env_keys_fail_validation() {
        let request = request_with_mcp_servers(Some(json!({
            "a": { "command": "tool-a", "env": { "TOKEN": "secret-a" } },
            "b": { "command": "tool-b", "env": { "TOKEN": "secret-b" } }
        })));
        let error = codex_invocation_for_request(&request, None)
            .expect_err("same env key with different values must fail validation");
        assert!(
            error.to_string().contains("TOKEN"),
            "error must name the conflicting key: {error}"
        );
    }

    #[test]
    fn same_valued_env_keys_across_servers_are_allowed() {
        let request = request_with_mcp_servers(Some(json!({
            "a": { "command": "tool-a", "env": { "TOKEN": "shared" } },
            "b": { "command": "tool-b", "env": { "TOKEN": "shared" } }
        })));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            invocation.env.get("TOKEN").map(String::as_str),
            Some("shared")
        );
    }

    #[test]
    fn mcp_env_key_conflicting_with_request_env_fails_validation() {
        let mut request = request_with_mcp_servers(Some(json!({
            "a": { "command": "tool-a", "env": { "TOKEN": "secret" } }
        })));
        request
            .env_vars
            .push(("TOKEN".to_string(), "outer".to_string()));
        let error = codex_invocation_for_request(&request, None)
            .expect_err("MCP env key conflicting with request env must fail validation");
        assert!(
            error.to_string().contains("TOKEN"),
            "error must name the conflicting key: {error}"
        );
    }

    #[test]
    fn absent_servers_leave_argv_byte_identical() {
        let baseline = codex_invocation_for_request(&request_with_mcp_servers(None), None)
            .expect("invocation");
        assert!(config_overrides(&baseline.args).is_empty());
    }

    #[test]
    fn empty_servers_object_leaves_argv_byte_identical() {
        let baseline = codex_invocation_for_request(&request_with_mcp_servers(None), None)
            .expect("invocation");
        let empty = codex_invocation_for_request(&request_with_mcp_servers(Some(json!({}))), None)
            .expect("invocation");
        assert_eq!(baseline.args, empty.args);
    }

    #[test]
    fn caller_supplied_mcp_override_suppresses_injection() {
        let mut request = request_with_extras(json!({
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "codex",
                        "args": [
                            "exec",
                            "-c",
                            "mcp_servers.docs.command=\"custom\"",
                            "say hi"
                        ]
                    }
                }
            }
        }));
        request.mcp_servers = Some(json!({ "docs": { "command": "npx" } }));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            config_overrides(&invocation.args),
            vec!["mcp_servers.docs.command=\"custom\"".to_string()],
        );
    }

    #[test]
    fn runtime_contract_path_gets_overrides_when_absent() {
        let mut request = request_with_extras(json!({
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "codex",
                        "args": ["exec", "say hi"]
                    }
                }
            }
        }));
        request.mcp_servers = Some(json!({ "docs": { "command": "npx" } }));
        let invocation = codex_invocation_for_request(&request, None).expect("invocation");
        assert_eq!(
            config_overrides(&invocation.args),
            vec!["mcp_servers.docs.command=\"npx\"".to_string()],
        );
    }
}
