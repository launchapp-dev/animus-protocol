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
        _ => {}
    }

    if let Some(text) = value.get("content").and_then(Value::as_str) {
        return vec![SessionEvent::FinalText {
            text: text.to_string(),
        }];
    }

    Vec::new()
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
}
