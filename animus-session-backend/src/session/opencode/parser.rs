use serde_json::{json, Value};

use crate::session::session_event::SessionEvent;

pub(crate) fn parse_opencode_json_line(line: &str) -> Vec<SessionEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return vec![SessionEvent::TextDelta {
            text: line.to_string(),
        }];
    };

    let kind = value.get("type").and_then(Value::as_str);

    // opencode v1.2+ wraps every frame as `{type, timestamp, sessionID, part}`
    // where the payload lives in `part`. A `tool` part carries the call AND
    // its result together (part.tool + part.state.{input,output,status}). Older
    // opencode builds used flat `{type:"tool_use", tool_use:{...}}` frames; the
    // fallbacks below keep those working.
    if let Some(part) = value.get("part") {
        return parse_opencode_part(kind, part);
    }

    match kind {
        Some("text") => {
            if let Some(text) = value.get("text").and_then(Value::as_str) {
                return vec![SessionEvent::TextDelta {
                    text: text.to_string(),
                }];
            }
        }
        Some("tool_use") => {
            let body = value.get("tool_use").unwrap_or(&value);
            return vec![tool_use_to_event(body)];
        }
        Some("tool_result") => {
            let body = value.get("tool_result").unwrap_or(&value);
            return vec![tool_result_to_event(body)];
        }
        Some("error") => {
            let message = value
                .pointer("/error/data/message")
                .and_then(Value::as_str)
                .or_else(|| value.pointer("/error/name").and_then(Value::as_str))
                .unwrap_or("opencode reported an error")
                .to_string();
            return vec![SessionEvent::Error {
                message,
                recoverable: false,
            }];
        }
        _ => {}
    }

    if let Some(text) = value.get("content").and_then(Value::as_str) {
        return vec![SessionEvent::FinalText {
            text: text.to_string(),
        }];
    }

    Vec::new()
}

/// Parse a v1.2+ opencode `part` payload. Text parts become a TextDelta; a
/// completed/errored tool part becomes a ToolCall (name + input) paired with a
/// ToolResult (output + exit status), so opencode streams its steps with the
/// same fidelity as the other providers. Non-terminal tool states are skipped —
/// the terminal frame carries the input too, so emitting on it alone avoids a
/// duplicate ToolCall.
fn parse_opencode_part(_kind: Option<&str>, part: &Value) -> Vec<SessionEvent> {
    match part.get("type").and_then(Value::as_str) {
        Some("text") => part
            .get("text")
            .and_then(Value::as_str)
            .map(|text| {
                vec![SessionEvent::TextDelta {
                    text: text.to_string(),
                }]
            })
            .unwrap_or_default(),
        Some("tool") => {
            let tool_name = part
                .get("tool")
                .and_then(Value::as_str)
                .unwrap_or("unknown_tool")
                .to_string();
            let state = part.get("state").cloned().unwrap_or(Value::Null);
            let status = state.get("status").and_then(Value::as_str).unwrap_or("");
            if status != "completed" && status != "error" {
                return Vec::new();
            }
            let arguments = state.get("input").cloned().unwrap_or_else(|| json!({}));
            let output = state.get("output").cloned().unwrap_or(Value::Null);
            let success = status == "completed"
                && state
                    .get("metadata")
                    .and_then(|m| m.get("exit"))
                    .and_then(Value::as_i64)
                    .map(|exit| exit == 0)
                    .unwrap_or(true);
            vec![
                SessionEvent::ToolCall {
                    tool_name: tool_name.clone(),
                    arguments,
                    server: None,
                },
                SessionEvent::ToolResult {
                    tool_name,
                    output,
                    success,
                },
            ]
        }
        _ => Vec::new(),
    }
}

fn tool_use_to_event(body: &Value) -> SessionEvent {
    let tool_name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("unknown_tool")
        .to_string();
    let arguments = body
        .get("input")
        .cloned()
        .or_else(|| body.get("arguments").cloned())
        .unwrap_or_else(|| json!({}));
    SessionEvent::ToolCall {
        tool_name,
        arguments,
        server: None,
    }
}

fn tool_result_to_event(body: &Value) -> SessionEvent {
    let tool_name = body
        .get("name")
        .and_then(Value::as_str)
        .or_else(|| body.get("tool_name").and_then(Value::as_str))
        .or_else(|| body.get("tool_use_id").and_then(Value::as_str))
        .unwrap_or("unknown_tool")
        .to_string();
    let output = body
        .get("content")
        .cloned()
        .or_else(|| body.get("output").cloned())
        .unwrap_or(Value::Null);
    let success = !body
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    SessionEvent::ToolResult {
        tool_name,
        output,
        success,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opencode_v12_tool_part_emits_call_and_result() {
        // Real opencode v1.2.x frame: payload nested under `part`, a `tool`
        // part carrying both the input and the completed output.
        let line = r#"{"type":"tool_use","sessionID":"ses_1","part":{"type":"tool","callID":"call_1","tool":"bash","state":{"status":"completed","input":{"command":"echo hi"},"output":"hi\n","metadata":{"exit":0}}}}"#;
        let events = parse_opencode_json_line(line);
        assert_eq!(
            events.len(),
            2,
            "completed tool part -> ToolCall + ToolResult"
        );
        match &events[0] {
            SessionEvent::ToolCall {
                tool_name,
                arguments,
                ..
            } => {
                assert_eq!(tool_name, "bash");
                assert_eq!(arguments, &json!({"command": "echo hi"}));
            }
            other => panic!("expected ToolCall, got {other:?}"),
        }
        match &events[1] {
            SessionEvent::ToolResult {
                tool_name,
                output,
                success,
            } => {
                assert_eq!(tool_name, "bash");
                assert_eq!(output, &json!("hi\n"));
                assert!(*success);
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn opencode_v12_text_part_emits_text_delta() {
        let line = r#"{"type":"text","part":{"type":"text","text":"hello-oc"}}"#;
        let events = parse_opencode_json_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            SessionEvent::TextDelta { text } => assert_eq!(text, "hello-oc"),
            other => panic!("expected TextDelta, got {other:?}"),
        }
    }

    #[test]
    fn opencode_parser_emits_tool_call_for_tool_use_event() {
        let line =
            r#"{"type":"tool_use","tool_use":{"id":"tool_1","name":"shell","input":{"cmd":"ls"}}}"#;
        let events = parse_opencode_json_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            SessionEvent::ToolCall {
                tool_name,
                arguments,
                server,
            } => {
                assert_eq!(tool_name, "shell");
                assert_eq!(arguments, &json!({"cmd": "ls"}));
                assert!(server.is_none());
            }
            other => panic!("expected ToolCall, got {other:?}"),
        }
    }

    #[test]
    fn opencode_parser_emits_tool_result_for_tool_result_event() {
        let line = r#"{"type":"tool_result","tool_result":{"tool_use_id":"tool_1","content":"file_a\nfile_b\n"}}"#;
        let events = parse_opencode_json_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            SessionEvent::ToolResult {
                output,
                success,
                tool_name,
            } => {
                assert_eq!(tool_name, "tool_1");
                assert_eq!(output, &json!("file_a\nfile_b\n"));
                assert!(*success);
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn opencode_parser_still_emits_text_delta_for_text_event() {
        let line = r#"{"type":"text","text":"hello"}"#;
        let events = parse_opencode_json_line(line);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], SessionEvent::TextDelta { .. }));
    }

    #[test]
    fn opencode_parser_emits_error_for_error_frame() {
        let line = r#"{"type":"error","timestamp":1781030806746,"sessionID":"ses_1","error":{"name":"UnknownError","data":{"message":"Model not found: gpt-5.2/."}}}"#;
        let events = parse_opencode_json_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            SessionEvent::Error {
                message,
                recoverable,
            } => {
                assert_eq!(message, "Model not found: gpt-5.2/.");
                assert!(!recoverable);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
