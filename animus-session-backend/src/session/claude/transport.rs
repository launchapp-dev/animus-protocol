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
    let mcp_config = write_claude_mcp_config(&request)?;
    let settings_config = write_claude_approval_settings(&request)?;
    let invocation = claude_invocation_for_request(
        &request,
        resume_session_id.as_deref(),
        mcp_config.as_ref().map(|guard| guard.path()),
        settings_config.as_ref().map(|guard| guard.path()),
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
        drop(mcp_config);
        drop(settings_config);
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

/// The flattened MCP tool name the Claude CLI uses for the kernel's
/// `animus.agent.request_approval` tool, passed to
/// `--permission-prompt-tool` when approvals are enabled.
///
/// Claude Code exposes tools from an `--mcp-config` server as
/// `mcp__<server>__<tool>`, sanitizing each segment by replacing every
/// character outside `[a-zA-Z0-9_-]` with `_` (and trimming leading or
/// trailing `_`); its permission-rule grammar only accepts
/// `mcp__[A-Za-z0-9_-]+__[A-Za-z0-9_-]+`, so dots can never survive.
/// The kernel injects the server under the name `animus` with tool id
/// `animus.agent.request_approval`, which therefore flattens to this
/// value (dot -> underscore). Verified against the claude CLI 2.1.172
/// binary; if a future CLI changes the mangling, this constant is the
/// single place to fix.
pub const CLAUDE_PERMISSION_PROMPT_TOOL: &str = "mcp__animus__animus_agent_request_approval";

/// Apply the approvals hook to a Claude argv as
/// `--permission-prompt-tool mcp__animus__animus_agent_request_approval`
/// when the request carries `extras.approvals == true`.
///
/// Approvals also strip any `--dangerously-skip-permissions` token from
/// the argv: that flag bypasses Claude's permission checks entirely, so
/// leaving it alongside the hook would make approvals silently
/// ineffective (the tool would never be consulted). The default argv
/// builder adds it whenever no `permission_mode` is set, and a
/// runtime-contract launch block may carry it too — approvals are
/// "enforced, not voluntary", so they win over both.
///
/// The flag pair is inserted at the FRONT of the argv (the options
/// region) for the same reason as `--effort`: the trailing token is not
/// guaranteed to be the prompt. A caller-supplied
/// `--permission-prompt-tool` (e.g. inside a runtime-contract launch
/// block) wins, and a request without approvals leaves the argv
/// byte-identical. Claude is the only transport with this native hook —
/// it does NOT additionally receive the voluntary prompt preamble used
/// by codex/gemini/opencode, to avoid double-prompting.
fn apply_claude_permission_prompt_tool(args: &mut Vec<String>, request: &SessionRequest) {
    if !crate::session::approvals_enabled(request) {
        return;
    }
    args.retain(|arg| arg != "--dangerously-skip-permissions");
    if args.iter().any(|arg| arg == "--permission-prompt-tool") {
        return;
    }
    args.insert(0, "--permission-prompt-tool".to_string());
    args.insert(1, CLAUDE_PERMISSION_PROMPT_TOOL.to_string());
}

/// Write the per-run `--mcp-config` document (`{"mcpServers": {...}}`)
/// into a user-only (`0600`) temp file and return its path, or `None`
/// when the request carries no MCP servers. The config goes through a
/// private file rather than inline argv JSON so secret-bearing entries
/// (env tokens, auth headers) never show up in process listings. The
/// returned guard removes the file when dropped — on session completion
/// or on any earlier error path.
fn write_claude_mcp_config(
    request: &SessionRequest,
) -> Result<Option<crate::session::PrivateFileGuard>> {
    let Some(servers) = request.mcp_servers_object() else {
        return Ok(None);
    };
    let config = serde_json::json!({ "mcpServers": servers });
    let path = std::env::temp_dir().join(format!("animus-claude-mcp-{}.json", Uuid::new_v4()));
    let guard = crate::session::PrivateFileGuard::new(path);
    crate::session::write_private_file(guard.path(), &config.to_string())?;
    Ok(Some(guard))
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

/// Resolve the agent id the approval hook must report to. Two sources, in
/// priority order:
///
/// 1. `extras.agent_id` — set directly by callers that already know the id.
/// 2. The `animus` MCP server's args. The kernel pins the active agent by
///    spawning that server as `["--project-root", <root>, "mcp", "serve",
///    "--agent-id", <id>]`, so the id rides along in the args array. The
///    `--permission-prompt-tool` hook is hard-coded to the `animus` server,
///    so this resolver mirrors it: it reads the `--agent-id` from the entry
///    named `animus` only. A `--agent-id` on some other server (a different
///    tool entirely) must not be mistaken for this session's agent.
///
/// Returns `None` when no id is resolvable — without one the
/// `agent approve-hook` verb cannot look up a policy, so the caller skips
/// writing the settings entirely (the `--permission-prompt-tool` gate still
/// covers normal-mode requests).
fn resolve_approval_agent_id(request: &SessionRequest) -> Option<String> {
    if let Some(id) = request
        .extras
        .get("agent_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(id.to_string());
    }

    animus_server_flag_value(request, "--agent-id")
}

/// Read a single `<flag> <value>` pair out of the `animus` MCP server's
/// `args` array, returning the trimmed non-empty value following the first
/// occurrence of `flag`. The kernel pins this session's identity by spawning
/// that server as `["--project-root", <root>, "mcp", "serve", "--agent-id",
/// <id>]`, so both the agent id and the project root ride along there. We
/// inspect the entry named `animus` only — flags on some other server belong
/// to a different tool and must not be mistaken for this session's.
fn animus_server_flag_value(request: &SessionRequest, flag: &str) -> Option<String> {
    let servers = request.mcp_servers.as_ref()?.as_object()?;
    let args = servers
        .get("animus")?
        .get("args")
        .and_then(serde_json::Value::as_array)?;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg.as_str() == Some(flag) {
            if let Some(value) = iter
                .next()
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Quote a value for safe embedding in the hook `command` string Claude
/// runs through the platform shell, so spaces and metacharacters in a bin
/// path, agent id, or project root cannot word-split or inject.
///
/// On Unix (where Animus runs in production) Claude invokes the hook via
/// `/bin/sh -c`, so we POSIX single-quote: wrap in `'...'` and rewrite each
/// embedded `'` as `'\''`. On Windows Claude runs the command through
/// `cmd.exe`, which does not honor single quotes — there we double-quote and
/// escape embedded double quotes so the executable path and arguments stay
/// intact.
fn shell_quote(value: &str) -> String {
    #[cfg(not(windows))]
    {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
    #[cfg(windows)]
    {
        format!("\"{}\"", value.replace('"', "\\\""))
    }
}

/// Write the per-run `--settings` document carrying a `PreToolUse` hook that
/// shells out to `animus agent approve-hook` before every tool call, and
/// return its path (or `None` when approvals are disabled or no agent id is
/// resolvable).
///
/// This closes a gap in the `--permission-prompt-tool` gate: when the
/// operator's global Claude settings set `permissions.defaultMode: "auto"`,
/// the permission-prompt tool is never consulted and an approval-gated run
/// would proceed unchecked. A `PreToolUse` hook fires BEFORE the permission
/// mode is evaluated, so a hook-driven deny holds even under `auto`. The two
/// mechanisms are complementary — the prompt tool drives the interactive
/// allow/deny UX, the hook is the hard backstop.
///
/// The settings go through a private (`0600`) temp file rather than inline
/// argv JSON to mirror `write_claude_mcp_config`; the returned guard removes
/// the file when dropped, on session completion or any earlier error path.
fn write_claude_approval_settings(
    request: &SessionRequest,
) -> Result<Option<crate::session::PrivateFileGuard>> {
    if !crate::session::approvals_enabled(request) {
        return Ok(None);
    }
    // TODO(codex-p2): this fails OPEN — when approvals are on but no agent id
    // is resolvable we skip the hook (the `--permission-prompt-tool` gate
    // still covers normal mode, but under `permissions.defaultMode: "auto"`
    // an un-id'd run proceeds unchecked). The current contract deliberately
    // returns Ok(None) here (a hook without an agent id cannot resolve a
    // policy via `approve-hook`); failing the launch closed instead is a
    // follow-up once a fallback agent-id derivation exists.
    let Some(agent_id) = resolve_approval_agent_id(request) else {
        return Ok(None);
    };
    // Resolve the binary the hook shells out to, in priority order:
    //   1. ANIMUS_BIN — explicit operator override.
    //   2. the `animus` MCP server's `command` — the very binary the kernel
    //      already pinned for this session, which is what makes the
    //      permission-prompt tool reachable; in dev/non-PATH deployments it
    //      is an absolute path, and the bare `animus` fallback would miss it.
    //   3. bare `animus` on PATH.
    let animus_bin = std::env::var("ANIMUS_BIN")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            request
                .mcp_servers
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .and_then(|servers| servers.get("animus"))
                .and_then(|server| server.get("command"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "animus".to_string());
    // Resolve the project root, in priority order:
    //   1. request.project_root — the explicit session root.
    //   2. the `animus` MCP server's pinned `--project-root` — the root the
    //      active agent is actually scoped to (e.g. when `cwd` is a
    //      subdirectory), mirroring how the agent id is resolved.
    //   3. request.cwd.
    let project_root = request
        .project_root
        .as_deref()
        .map(|root| root.display().to_string())
        .or_else(|| animus_server_flag_value(request, "--project-root"))
        .unwrap_or_else(|| request.cwd.display().to_string());
    // Claude runs the hook `command` through a shell, so every interpolated
    // value (bin path, agent id, project root) must be quoted for that shell —
    // a repo path like `/Users/me/My Repo` would otherwise word-split and
    // mis-route the hook, and an attacker-influenced field could inject
    // arbitrary shell. `--timeout-secs 600` is the wait the verb itself
    // applies before defaulting a pending decision.
    let command = format!(
        "{bin} agent approve-hook --format claude --agent-id {agent} \
         --project-root {root} --timeout-secs 600",
        bin = shell_quote(&animus_bin),
        agent = shell_quote(&agent_id),
        root = shell_quote(&project_root),
    );
    // The hook process must be allowed to outlive the verb's own 600s wait,
    // so the per-hook `timeout` (seconds) is set above 600 — otherwise Claude
    // would kill a legitimately-pending human approval early.
    let settings = serde_json::json!({
        "hooks": {
            "PreToolUse": [
                {
                    "matcher": "",
                    "hooks": [
                        { "type": "command", "command": command, "timeout": 660 }
                    ]
                }
            ]
        }
    });
    let path = std::env::temp_dir().join(format!("animus-claude-settings-{}.json", Uuid::new_v4()));
    let guard = crate::session::PrivateFileGuard::new(path);
    crate::session::write_private_file(guard.path(), &settings.to_string())?;
    Ok(Some(guard))
}

/// Apply the written approval-settings file to a Claude argv as
/// `--settings <path>`.
///
/// `claude` honors a single `--settings` value (a file path or an inline
/// JSON string) that layers ON TOP of the global/project settings, so —
/// unlike `--mcp-config` — a caller-supplied `--settings` cannot simply be
/// left to win: that would drop our `PreToolUse` approval hook and reopen
/// the bypass this whole mechanism exists to close (under
/// `permissions.defaultMode: "auto"` the `--permission-prompt-tool` gate is
/// never consulted). Approvals are enforced, not voluntary, so when a
/// `--settings` is already present we MERGE our hook into it: the existing
/// value (path or inline JSON) is parsed, our `PreToolUse` hook entry is
/// appended to any `hooks.PreToolUse` array already there, the merged
/// document is written back into our own guard file, and the argv value is
/// repointed at that file. When no `--settings` is present we front-insert
/// the flag pair (options region — the trailing token is not guaranteed to
/// be the prompt). An absent path (approvals off or no resolvable agent id)
/// leaves the argv untouched.
///
/// A malformed or unreadable existing value cannot be safely merged, so we
/// fall back to forcing our standalone hook settings — losing the caller's
/// other options is preferable to silently dropping the approval backstop.
///
/// `cwd` is the session working directory the child claude process runs in;
/// a relative existing `--settings` path is resolved against it (claude
/// loads it relative to the child cwd, not the daemon's).
fn apply_claude_settings(
    args: &mut Vec<String>,
    settings_path: Option<&std::path::Path>,
    cwd: &std::path::Path,
) {
    let Some(path) = settings_path else {
        return;
    };
    // Claude accepts both the split `--settings VALUE` form and the attached
    // `--settings=VALUE` form; both must be merged or an unmerged caller
    // value would let claude honor settings WITHOUT our approval hook.
    for index in 0..args.len() {
        if args[index] == "--settings" {
            if let Some(existing) = args.get(index + 1).cloned() {
                merge_approval_hook_into_existing_settings(path, &existing, cwd);
                if let Some(slot) = args.get_mut(index + 1) {
                    *slot = path.display().to_string();
                }
            }
            return;
        }
        if let Some(existing) = args[index].strip_prefix("--settings=") {
            let existing = existing.to_string();
            merge_approval_hook_into_existing_settings(path, &existing, cwd);
            args[index] = format!("--settings={}", path.display());
            return;
        }
    }
    args.insert(0, "--settings".to_string());
    args.insert(1, path.display().to_string());
}

/// Merge our standalone approval-hook document (already written at
/// `hook_path`) with a caller-supplied `--settings` value, rewriting
/// `hook_path` in place with the union. `existing` is either a filesystem
/// path or an inline JSON string (both shapes `claude --settings` accepts);
/// a relative path is resolved against `cwd` (the child claude process's
/// working directory).
///
/// The merge is shallow except for `hooks`, where each event array (e.g.
/// `PreToolUse`) is concatenated so our hook is ADDED rather than replacing
/// the caller's hooks. On any parse/read failure we leave `hook_path` as the
/// standalone hook document — the approval backstop must survive even if the
/// caller's settings cannot be merged.
fn merge_approval_hook_into_existing_settings(
    hook_path: &std::path::Path,
    existing: &str,
    cwd: &std::path::Path,
) {
    let Ok(hook_text) = std::fs::read_to_string(hook_path) else {
        return;
    };
    let Ok(hook_value) = serde_json::from_str::<serde_json::Value>(&hook_text) else {
        return;
    };
    // `existing` is an inline JSON string when it parses as a JSON object,
    // otherwise treat it as a path to a settings file — resolved against the
    // child process cwd when relative so it matches what claude itself reads.
    let existing_value = match serde_json::from_str::<serde_json::Value>(existing) {
        Ok(value @ serde_json::Value::Object(_)) => value,
        _ => {
            let candidate = std::path::Path::new(existing);
            let resolved = if candidate.is_absolute() {
                candidate.to_path_buf()
            } else {
                cwd.join(candidate)
            };
            match std::fs::read_to_string(&resolved) {
                Ok(text) => match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(value) => value,
                    Err(_) => return,
                },
                Err(_) => return,
            }
        }
    };
    let (Some(merged_obj), Some(hook_obj)) =
        (existing_value.as_object().cloned(), hook_value.as_object())
    else {
        return;
    };
    let mut merged = merged_obj;
    for (key, value) in hook_obj {
        if key == "hooks" {
            // A caller's `hooks` that is not an object (e.g. `[]` or a
            // string) cannot be extended; coerce it to an empty object so our
            // approval hook events still land — the backstop must never be
            // dropped to honor a malformed caller value.
            let slot = merged
                .entry("hooks")
                .or_insert_with(|| serde_json::json!({}));
            if !slot.is_object() {
                *slot = serde_json::json!({});
            }
            merge_hook_events(slot, value);
        } else {
            merged.insert(key.clone(), value.clone());
        }
    }
    let document = serde_json::Value::Object(merged).to_string();
    let _ = std::fs::write(hook_path, document);
}

/// Concatenate per-event hook arrays from `addition` into `target` so an
/// added hook event extends the existing list instead of overwriting it.
/// `target` is guaranteed to be a JSON object by the caller.
fn merge_hook_events(target: &mut serde_json::Value, addition: &serde_json::Value) {
    let (Some(target_obj), Some(addition_obj)) = (target.as_object_mut(), addition.as_object())
    else {
        return;
    };
    for (event, hooks) in addition_obj {
        match target_obj
            .get_mut(event)
            .and_then(serde_json::Value::as_array_mut)
        {
            Some(existing) => {
                if let Some(extra) = hooks.as_array() {
                    existing.extend(extra.iter().cloned());
                }
            }
            None => {
                target_obj.insert(event.clone(), hooks.clone());
            }
        }
    }
}

pub(crate) fn claude_invocation_for_request(
    request: &SessionRequest,
    resume_session_id: Option<&str>,
    mcp_config_path: Option<&std::path::Path>,
    settings_path: Option<&std::path::Path>,
) -> Result<LaunchInvocation> {
    if let Some(mut invocation) =
        parse_launch_from_runtime_contract(request.extras.get("runtime_contract"))?
    {
        apply_claude_reasoning_effort(&mut invocation.args, request);
        apply_claude_mcp_config(&mut invocation.args, mcp_config_path);
        apply_claude_settings(&mut invocation.args, settings_path, &request.cwd);
        apply_claude_permission_prompt_tool(&mut invocation.args, request);
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
    apply_claude_settings(&mut args, settings_path, &request.cwd);
    apply_claude_permission_prompt_tool(&mut args, request);
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
            actor: None,
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
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        assert_eq!(effort_value(&invocation.args), Some("high".to_string()));
        assert_eq!(invocation.args.last().map(String::as_str), Some("say hi"));
    }

    #[test]
    fn absent_reasoning_effort_omits_flag() {
        let request = request_with_extras(json!({}));
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        assert!(effort_value(&invocation.args).is_none());
    }

    #[test]
    fn unknown_level_is_ignored() {
        let request = request_with_extras(json!({ "reasoning_effort": "ludicrous" }));
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
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
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
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
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
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
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
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
mod approvals_hook_tests {
    use super::*;
    use serde_json::json;

    use super::reasoning_effort_tests::request_with_extras;

    fn permission_prompt_tool(args: &[String]) -> Option<String> {
        args.iter()
            .position(|arg| arg == "--permission-prompt-tool")
            .and_then(|index| args.get(index + 1).cloned())
    }

    #[test]
    fn approvals_true_injects_permission_prompt_tool_flag() {
        let request = request_with_extras(json!({ "approvals": true }));
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        assert_eq!(
            permission_prompt_tool(&invocation.args),
            Some(CLAUDE_PERMISSION_PROMPT_TOOL.to_string()),
        );
        assert_eq!(
            invocation.args.last().map(String::as_str),
            Some("say hi"),
            "the prompt must stay the final argv token"
        );
        assert!(
            !invocation
                .args
                .iter()
                .any(|arg| arg.contains("Human-in-the-loop")),
            "claude gets the native hook only — never the voluntary prompt preamble"
        );
        assert!(
            !invocation
                .args
                .iter()
                .any(|arg| arg == "--dangerously-skip-permissions"),
            "approvals must strip --dangerously-skip-permissions or the hook is never consulted"
        );
    }

    #[test]
    fn absent_approvals_keeps_default_skip_permissions_flag() {
        let invocation =
            claude_invocation_for_request(&request_with_extras(json!({})), None, None, None)
                .expect("invocation");
        assert!(
            invocation
                .args
                .iter()
                .any(|arg| arg == "--dangerously-skip-permissions"),
            "without approvals the default headless argv keeps its skip-permissions behavior"
        );
    }

    #[test]
    fn approvals_strip_contract_supplied_skip_permissions() {
        let request = request_with_extras(json!({
            "approvals": true,
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": [
                            "--print",
                            "--dangerously-skip-permissions",
                            "say hi"
                        ]
                    }
                }
            }
        }));
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        assert!(
            !invocation
                .args
                .iter()
                .any(|arg| arg == "--dangerously-skip-permissions"),
            "approvals are enforced — a contract-supplied skip flag must not defeat the hook"
        );
        assert_eq!(
            permission_prompt_tool(&invocation.args),
            Some(CLAUDE_PERMISSION_PROMPT_TOOL.to_string()),
        );
    }

    #[test]
    fn absent_approvals_leaves_argv_byte_identical() {
        let baseline =
            claude_invocation_for_request(&request_with_extras(json!({})), None, None, None)
                .expect("invocation");
        let disabled = claude_invocation_for_request(
            &request_with_extras(json!({ "approvals": false })),
            None,
            None,
            None,
        )
        .expect("invocation");
        assert_eq!(baseline.args, disabled.args);
        assert!(permission_prompt_tool(&baseline.args).is_none());
    }

    #[test]
    fn caller_supplied_permission_prompt_tool_is_not_clobbered() {
        let request = request_with_extras(json!({
            "approvals": true,
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": [
                            "--print",
                            "--permission-prompt-tool", "mcp__custom__gate",
                            "say hi"
                        ]
                    }
                }
            }
        }));
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        let count = invocation
            .args
            .iter()
            .filter(|arg| *arg == "--permission-prompt-tool")
            .count();
        assert_eq!(count, 1, "caller-supplied hook must win");
        assert_eq!(
            permission_prompt_tool(&invocation.args),
            Some("mcp__custom__gate".to_string()),
        );
    }

    #[test]
    fn runtime_contract_path_gets_hook_when_absent() {
        let request = request_with_extras(json!({
            "approvals": true,
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": ["--print", "say hi"]
                    }
                }
            }
        }));
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        assert_eq!(
            permission_prompt_tool(&invocation.args),
            Some(CLAUDE_PERMISSION_PROMPT_TOOL.to_string()),
        );
    }

    #[test]
    fn stdin_contract_does_not_split_trailing_flag_pair() {
        let request = request_with_extras(json!({
            "approvals": true,
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
        let invocation =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
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
        assert_eq!(
            permission_prompt_tool(&invocation.args),
            Some(CLAUDE_PERMISSION_PROMPT_TOOL.to_string()),
        );
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
        let guard = write_claude_mcp_config(&request)
            .expect("write config")
            .expect("config path");
        let invocation = claude_invocation_for_request(&request, None, Some(guard.path()), None)
            .expect("invocation");

        assert_eq!(invocation.args[0], "--mcp-config");
        assert_eq!(invocation.args[1], guard.path().display().to_string());
        assert_eq!(invocation.args[2], "--strict-mcp-config");
        assert_eq!(invocation.args.last().map(String::as_str), Some("say hi"));

        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read config"))
                .expect("config JSON");
        assert_eq!(written, json!({ "mcpServers": servers }));
        assert!(
            !invocation.args.iter().any(|arg| arg.contains("TOKEN")),
            "secret values must never reach argv"
        );
        let path = guard.path().to_path_buf();
        drop(guard);
        assert!(!path.exists(), "guard drop must remove the config file");
    }

    #[cfg(unix)]
    #[test]
    fn mcp_config_file_is_user_only() {
        use std::os::unix::fs::PermissionsExt;
        let request = request_with_mcp_servers(Some(json!({ "docs": { "command": "npx" } })));
        let guard = write_claude_mcp_config(&request)
            .expect("write config")
            .expect("config path");
        let mode = std::fs::metadata(guard.path())
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn absent_servers_write_no_config_and_leave_argv_untouched() {
        let request = request_with_mcp_servers(None);
        assert!(write_claude_mcp_config(&request)
            .expect("write config")
            .is_none());
        let baseline =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        assert!(!baseline.args.iter().any(|arg| arg == "--mcp-config"));
        assert!(!baseline.args.iter().any(|arg| arg == "--strict-mcp-config"));
    }

    #[test]
    fn empty_servers_object_writes_no_config_and_leaves_argv_byte_identical() {
        let empty_request = request_with_mcp_servers(Some(json!({})));
        assert!(write_claude_mcp_config(&empty_request)
            .expect("write config")
            .is_none());
        let baseline =
            claude_invocation_for_request(&request_with_mcp_servers(None), None, None, None)
                .expect("invocation");
        let empty =
            claude_invocation_for_request(&empty_request, None, None, None).expect("invocation");
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
        let invocation = claude_invocation_for_request(
            &request,
            None,
            Some(std::path::Path::new("/tmp/x")),
            None,
        )
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
        let guard = write_claude_mcp_config(&request)
            .expect("write config")
            .expect("config path");
        let invocation = claude_invocation_for_request(&request, None, Some(guard.path()), None)
            .expect("invocation");
        assert_eq!(
            mcp_config_arg(&invocation.args),
            Some(guard.path().display().to_string()),
        );
        assert!(invocation.args.iter().any(|a| a == "--strict-mcp-config"));
    }
}

#[cfg(test)]
mod approval_settings_tests {
    use super::*;
    use serde_json::json;

    use super::reasoning_effort_tests::request_with_extras;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Serializes the tests that depend on `ANIMUS_BIN`: the binary
    /// resolution reads process env, so concurrent set/remove would race.
    fn animus_bin_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Hold the `ANIMUS_BIN` lock and force the env var to a known state
    /// (`Some(value)` to set it, `None` to clear it) for the test body,
    /// restoring the prior value on drop. This makes binary-resolution
    /// assertions deterministic regardless of the ambient environment.
    struct AnimusBinGuard {
        _lock: MutexGuard<'static, ()>,
        previous: Option<String>,
    }

    impl AnimusBinGuard {
        fn set(value: Option<&str>) -> Self {
            let lock = animus_bin_lock();
            let previous = std::env::var("ANIMUS_BIN").ok();
            match value {
                Some(value) => std::env::set_var("ANIMUS_BIN", value),
                None => std::env::remove_var("ANIMUS_BIN"),
            }
            Self {
                _lock: lock,
                previous,
            }
        }
    }

    impl Drop for AnimusBinGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var("ANIMUS_BIN", value),
                None => std::env::remove_var("ANIMUS_BIN"),
            }
        }
    }

    /// Read the single `PreToolUse` hook command string out of a written
    /// settings document so assertions can inspect the resolved argv.
    fn hook_command(settings: &serde_json::Value) -> String {
        settings
            .pointer("/hooks/PreToolUse/0/hooks/0/command")
            .and_then(serde_json::Value::as_str)
            .expect("PreToolUse hook command present")
            .to_string()
    }

    fn settings_arg(args: &[String]) -> Option<String> {
        args.iter()
            .position(|arg| arg == "--settings")
            .and_then(|index| args.get(index + 1))
            .cloned()
    }

    #[test]
    fn approvals_with_extras_agent_id_writes_hook_settings() {
        let request = request_with_extras(json!({
            "approvals": true,
            "agent_id": "agent:alpha"
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");

        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read settings"))
                .expect("settings JSON");
        let command = hook_command(&written);
        assert!(command.contains("agent approve-hook --format claude"));
        // Interpolated values are POSIX single-quoted for the shell.
        assert!(command.contains("--agent-id 'agent:alpha'"));
        assert!(command.contains("--timeout-secs 600"));

        let invocation = claude_invocation_for_request(&request, None, None, Some(guard.path()))
            .expect("invocation");
        // `--settings <path>` is present in the options region. The exact
        // index varies: `apply_claude_permission_prompt_tool` runs last and
        // also front-inserts when approvals are on, so it lands ahead of
        // `--settings`. Match by flag, not position.
        assert_eq!(
            settings_arg(&invocation.args),
            Some(guard.path().display().to_string()),
        );
        assert_eq!(invocation.args.last().map(String::as_str), Some("say hi"));
    }

    #[test]
    fn agent_id_resolved_from_mcp_server_args() {
        let mut request = request_with_extras(json!({ "approvals": true }));
        request.mcp_servers = Some(json!({
            "animus": {
                "command": "animus",
                "args": [
                    "--project-root", "/repo",
                    "mcp", "serve",
                    "--agent-id", "agent:from-mcp"
                ]
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read settings"))
                .expect("settings JSON");
        assert!(hook_command(&written).contains("--agent-id 'agent:from-mcp'"));
    }

    #[test]
    fn hook_binary_honors_animus_bin_env() {
        // ANIMUS_BIN, when set, wins over the server command and the default.
        let _bin = AnimusBinGuard::set(Some("/usr/local/bin/animus-dev"));
        let mut request = request_with_extras(json!({ "approvals": true }));
        request.mcp_servers = Some(json!({
            "animus": {
                "command": "/opt/animus/bin/animus",
                "args": ["mcp", "serve", "--agent-id", "agent:alpha"]
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read settings"))
                .expect("settings JSON");
        assert!(
            hook_command(&written).starts_with("'/usr/local/bin/animus-dev' agent approve-hook")
        );
    }

    #[test]
    fn malformed_caller_hooks_does_not_drop_approval_backstop() {
        // A caller `--settings` whose `hooks` is an array (not an object)
        // cannot be extended; the approval hook must still survive.
        let request = request_with_extras(json!({
            "approvals": true,
            "agent_id": "agent:alpha",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": ["--print", "--settings", "{\"hooks\":[]}", "say hi"]
                    }
                }
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let invocation = claude_invocation_for_request(&request, None, None, Some(guard.path()))
            .expect("invocation");
        assert_eq!(
            settings_arg(&invocation.args),
            Some(guard.path().display().to_string()),
        );
        let merged: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read merged"))
                .expect("merged JSON");
        assert!(merged
            .pointer("/hooks/PreToolUse/0/hooks/0/command")
            .and_then(serde_json::Value::as_str)
            .expect("approval hook present")
            .contains("agent approve-hook --format claude"));
    }

    #[test]
    fn hook_binary_falls_back_to_animus_server_command() {
        // ANIMUS_BIN cleared => the hook binary is taken from the `animus`
        // MCP server's configured `command`, single-quoted.
        let _bin = AnimusBinGuard::set(None);
        let mut request = request_with_extras(json!({ "approvals": true }));
        request.mcp_servers = Some(json!({
            "animus": {
                "command": "/opt/animus/bin/animus",
                "args": ["mcp", "serve", "--agent-id", "agent:alpha"]
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read settings"))
                .expect("settings JSON");
        assert!(hook_command(&written).starts_with("'/opt/animus/bin/animus' agent approve-hook"));
    }

    #[test]
    fn agent_id_only_read_from_animus_server() {
        // A `--agent-id` on a non-`animus` server must not be mistaken for
        // this session's agent: the prompt-tool hook is pinned to `animus`,
        // so the resolver reads that entry only.
        let mut request = request_with_extras(json!({ "approvals": true }));
        request.mcp_servers = Some(json!({
            "other": {
                "command": "other",
                "args": ["--agent-id", "agent:wrong"]
            }
        }));
        assert!(write_claude_approval_settings(&request)
            .expect("write settings")
            .is_none());
    }

    #[test]
    fn extras_agent_id_wins_over_mcp_server_args() {
        let mut request = request_with_extras(json!({
            "approvals": true,
            "agent_id": "agent:extras"
        }));
        request.mcp_servers = Some(json!({
            "animus": {
                "command": "animus",
                "args": ["mcp", "serve", "--agent-id", "agent:from-mcp"]
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read settings"))
                .expect("settings JSON");
        let command = hook_command(&written);
        assert!(command.contains("--agent-id 'agent:extras'"));
        assert!(!command.contains("agent:from-mcp"));
    }

    #[test]
    fn project_root_falls_back_to_cwd() {
        // No project_root set => the hook command uses cwd (`.`). Force the
        // default binary so the `'animus'` prefix assertion is deterministic.
        let _bin = AnimusBinGuard::set(None);
        let request = request_with_extras(json!({
            "approvals": true,
            "agent_id": "agent:alpha"
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read settings"))
                .expect("settings JSON");
        // The default command binary is `animus` (ANIMUS_BIN unset in tests).
        assert!(hook_command(&written).starts_with("'animus' agent approve-hook"));
        assert!(hook_command(&written).contains("--project-root '.'"));
    }

    #[test]
    fn project_root_resolved_from_pinned_mcp_args() {
        // request.project_root absent => the pinned `--project-root` from the
        // `animus` server args wins over cwd.
        let mut request = request_with_extras(json!({ "approvals": true }));
        request.mcp_servers = Some(json!({
            "animus": {
                "command": "animus",
                "args": [
                    "--project-root", "/repo/root",
                    "mcp", "serve",
                    "--agent-id", "agent:alpha"
                ]
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read settings"))
                .expect("settings JSON");
        assert!(hook_command(&written).contains("--project-root '/repo/root'"));
    }

    #[test]
    fn approvals_without_resolvable_agent_id_writes_nothing() {
        // Approvals on but no extras.agent_id and no --agent-id in any mcp
        // server args => no settings written (the prompt-tool still gates).
        let mut request = request_with_extras(json!({ "approvals": true }));
        request.mcp_servers = Some(json!({
            "animus": { "command": "animus", "args": ["mcp", "serve"] }
        }));
        assert!(write_claude_approval_settings(&request)
            .expect("write settings")
            .is_none());
    }

    #[test]
    fn approvals_off_writes_nothing_and_leaves_argv_byte_identical() {
        let request = request_with_extras(json!({}));
        assert!(write_claude_approval_settings(&request)
            .expect("write settings")
            .is_none());

        // apply_claude_settings(None) must not perturb a baseline invocation.
        let baseline =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        let with_none =
            claude_invocation_for_request(&request, None, None, None).expect("invocation");
        assert_eq!(baseline.args, with_none.args);
        assert!(!baseline.args.iter().any(|arg| arg == "--settings"));
        assert!(settings_arg(&baseline.args).is_none());
    }

    #[test]
    fn caller_supplied_inline_settings_is_merged_not_clobbered() {
        // A caller-supplied inline-JSON `--settings` carrying its own option
        // AND its own PreToolUse hook: approvals must MERGE — the argv value
        // repoints to our guard file, which keeps the caller's option and
        // both PreToolUse hooks (theirs + our approval backstop).
        let request = request_with_extras(json!({
            "approvals": true,
            "agent_id": "agent:alpha",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": [
                            "--print",
                            "--settings",
                            "{\"model\":\"opus\",\"hooks\":{\"PreToolUse\":[{\"matcher\":\"Bash\",\"hooks\":[{\"type\":\"command\",\"command\":\"echo caller\"}]}]}}",
                            "say hi"
                        ]
                    }
                }
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let invocation = claude_invocation_for_request(&request, None, None, Some(guard.path()))
            .expect("invocation");
        let count = invocation
            .args
            .iter()
            .filter(|arg| *arg == "--settings")
            .count();
        assert_eq!(count, 1, "a single merged --settings value is honored");
        assert_eq!(
            settings_arg(&invocation.args),
            Some(guard.path().display().to_string()),
            "the argv value repoints to our merged guard file"
        );

        let merged: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read merged"))
                .expect("merged JSON");
        // Caller's non-hook option survives.
        assert_eq!(
            merged.pointer("/model").and_then(serde_json::Value::as_str),
            Some("opus")
        );
        // Both PreToolUse hooks are present (caller's first, then ours).
        let pre = merged
            .pointer("/hooks/PreToolUse")
            .and_then(serde_json::Value::as_array)
            .expect("PreToolUse array");
        assert_eq!(pre.len(), 2, "caller hook + approval hook both retained");
        let commands: Vec<String> = pre
            .iter()
            .filter_map(|entry| entry.pointer("/hooks/0/command"))
            .filter_map(serde_json::Value::as_str)
            .map(str::to_string)
            .collect();
        assert!(commands.iter().any(|c| c == "echo caller"));
        assert!(commands
            .iter()
            .any(|c| c.contains("agent approve-hook --format claude")));
    }

    #[test]
    fn caller_supplied_attached_settings_form_is_merged() {
        // The `--settings=<value>` attached form must be merged too, else an
        // unmerged caller value would let claude run without the hook.
        let request = request_with_extras(json!({
            "approvals": true,
            "agent_id": "agent:alpha",
            "runtime_contract": {
                "cli": {
                    "launch": {
                        "command": "claude",
                        "args": [
                            "--print",
                            "--settings={\"model\":\"opus\"}",
                            "say hi"
                        ]
                    }
                }
            }
        }));
        let guard = write_claude_approval_settings(&request)
            .expect("write settings")
            .expect("settings path");
        let invocation = claude_invocation_for_request(&request, None, None, Some(guard.path()))
            .expect("invocation");
        // Exactly one `--settings=` token, repointed at our merged guard file.
        let attached: Vec<&String> = invocation
            .args
            .iter()
            .filter(|arg| arg.starts_with("--settings="))
            .collect();
        assert_eq!(attached.len(), 1);
        assert_eq!(
            attached[0],
            &format!("--settings={}", guard.path().display())
        );
        // No separate split `--settings` flag was added.
        assert!(!invocation.args.iter().any(|arg| arg == "--settings"));

        let merged: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(guard.path()).expect("read merged"))
                .expect("merged JSON");
        assert_eq!(
            merged.pointer("/model").and_then(serde_json::Value::as_str),
            Some("opus")
        );
        assert!(merged
            .pointer("/hooks/PreToolUse/0/hooks/0/command")
            .and_then(serde_json::Value::as_str)
            .expect("hook command")
            .contains("agent approve-hook --format claude"));
    }
}
