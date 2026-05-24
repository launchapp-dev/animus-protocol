use serde_json::{json, Value};

use crate::session::session_event::SessionEvent;

pub(crate) fn parse_gemini_json_chunk(chunk: &str) -> Vec<SessionEvent> {
    let trimmed = chunk.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return vec![SessionEvent::TextDelta {
            text: chunk.to_string(),
        }];
    };

    let event_type = value.get("type").and_then(Value::as_str).unwrap_or("");

    match event_type {
        "partialResult" => {
            let mut events = Vec::new();
            if let Some(text) = value.pointer("/partialResult/text").and_then(Value::as_str) {
                events.push(SessionEvent::TextDelta {
                    text: text.to_string(),
                });
            }
            events.extend(extract_function_calls_from_candidates(
                value.pointer("/partialResult/candidates"),
            ));
            return events;
        }
        "functionCall" => {
            if let Some(call) = value.get("functionCall") {
                return vec![function_call_to_event(call)];
            }
        }
        "functionResponse" => {
            if let Some(resp) = value.get("functionResponse") {
                return vec![function_response_to_event(resp)];
            }
        }
        _ => {}
    }

    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return vec![SessionEvent::TextDelta {
            text: text.to_string(),
        }];
    }

    let mut events = Vec::new();

    if let Some(session_id) = value.get("session_id") {
        events.push(SessionEvent::Metadata {
            metadata: json!({
                "type": "gemini_session",
                "session_id": session_id,
            }),
        });
    }

    if let Some(stats) = value.get("stats") {
        events.push(SessionEvent::Metadata {
            metadata: json!({
                "type": "gemini_stats",
                "stats": stats,
            }),
        });
    }

    events.extend(extract_function_calls_from_candidates(
        value.get("candidates"),
    ));

    if let Some(text) = extract_gemini_final_text(&value) {
        events.push(SessionEvent::FinalText { text });
    }

    events
}

fn function_call_to_event(call: &Value) -> SessionEvent {
    let tool_name = call
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("unknown_tool")
        .to_string();
    let arguments = call.get("args").cloned().unwrap_or_else(|| json!({}));
    SessionEvent::ToolCall {
        tool_name,
        arguments,
        server: None,
    }
}

fn function_response_to_event(resp: &Value) -> SessionEvent {
    let tool_name = resp
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("unknown_tool")
        .to_string();
    let output = resp.get("response").cloned().unwrap_or(Value::Null);
    SessionEvent::ToolResult {
        tool_name,
        output,
        success: true,
    }
}

fn extract_function_calls_from_candidates(candidates: Option<&Value>) -> Vec<SessionEvent> {
    let Some(candidates) = candidates.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut events = Vec::new();
    for candidate in candidates {
        let Some(parts) = candidate
            .pointer("/content/parts")
            .and_then(Value::as_array)
        else {
            continue;
        };
        for part in parts {
            if let Some(call) = part.get("functionCall") {
                events.push(function_call_to_event(call));
            }
            if let Some(resp) = part.get("functionResponse") {
                events.push(function_response_to_event(resp));
            }
        }
    }
    events
}

fn extract_gemini_final_text(value: &Value) -> Option<String> {
    if let Some(text) = value.get("response").and_then(Value::as_str) {
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }

    if let Some(text) = value.pointer("/content/text").and_then(Value::as_str) {
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }

    if let Some(parts) = value.pointer("/content/parts").and_then(Value::as_array) {
        let mut text = String::new();
        for part in parts {
            if let Some(segment) = part.get("text").and_then(Value::as_str) {
                text.push_str(segment);
            }
        }
        if !text.is_empty() {
            return Some(text);
        }
    }

    if let Some(candidates) = value.get("candidates").and_then(Value::as_array) {
        for candidate in candidates {
            if let Some(parts) = candidate
                .pointer("/content/parts")
                .and_then(Value::as_array)
            {
                let mut text = String::new();
                for part in parts {
                    if let Some(segment) = part.get("text").and_then(Value::as_str) {
                        text.push_str(segment);
                    }
                }
                if !text.is_empty() {
                    return Some(text);
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gemini_parser_emits_tool_call_for_function_call_event() {
        let chunk =
            r#"{"type":"functionCall","functionCall":{"name":"shell","args":{"cmd":"ls"}}}"#;
        let events = parse_gemini_json_chunk(chunk);
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
    fn gemini_parser_emits_tool_result_for_function_response_event() {
        let chunk = r#"{"type":"functionResponse","functionResponse":{"name":"shell","response":{"stdout":"file_a\nfile_b\n"}}}"#;
        let events = parse_gemini_json_chunk(chunk);
        assert_eq!(events.len(), 1);
        match &events[0] {
            SessionEvent::ToolResult {
                tool_name,
                output,
                success,
            } => {
                assert_eq!(tool_name, "shell");
                assert_eq!(output, &json!({"stdout": "file_a\nfile_b\n"}));
                assert!(*success);
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn gemini_parser_emits_tool_call_from_candidates_parts() {
        let chunk = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"shell","args":{"cmd":"pwd"}}}]}}]}"#;
        let events = parse_gemini_json_chunk(chunk);
        let tool_calls: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, SessionEvent::ToolCall { .. }))
            .collect();
        assert_eq!(tool_calls.len(), 1);
    }

    #[test]
    fn gemini_parser_preserves_partial_text() {
        let chunk = r#"{"type":"partialResult","partialResult":{"text":"Looking up... "}}"#;
        let events = parse_gemini_json_chunk(chunk);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], SessionEvent::TextDelta { .. }));
    }
}
