use std::collections::HashMap;

use serde_json::{json, Value};

use crate::session::session_event::SessionEvent;

#[derive(Default)]
pub(crate) struct CodexParser {
    pending: HashMap<String, PendingFunctionCall>,
}

struct PendingFunctionCall {
    name: String,
    arguments: String,
}

impl CodexParser {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn parse_line(&mut self, line: &str) -> Vec<SessionEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }

        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            return vec![SessionEvent::TextDelta {
                text: line.to_string(),
            }];
        };

        let event_type = value.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "thread.started" | "turn.started" => vec![SessionEvent::Metadata { metadata: value }],
            "turn.completed" => parse_codex_turn_completed(&value),
            "item.added" => self.parse_item_added(&value),
            "item.delta" => self.parse_item_delta(&value),
            "item.completed" => self.parse_item_completed(&value),
            _ => Vec::new(),
        }
    }

    fn parse_item_added(&mut self, value: &Value) -> Vec<SessionEvent> {
        let Some(item) = value.get("item") else {
            return Vec::new();
        };
        if item.get("type").and_then(Value::as_str) != Some("function_call") {
            return Vec::new();
        }
        let Some(id) = function_call_id(item) else {
            return Vec::new();
        };
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown_tool")
            .to_string();
        let arguments = item
            .get("arguments")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        self.pending
            .insert(id, PendingFunctionCall { name, arguments });
        Vec::new()
    }

    fn parse_item_delta(&mut self, value: &Value) -> Vec<SessionEvent> {
        let Some(item) = value.get("item") else {
            return Vec::new();
        };
        if item.get("type").and_then(Value::as_str) != Some("function_call") {
            return Vec::new();
        }
        let Some(id) = function_call_id(item) else {
            return Vec::new();
        };
        let entry = self
            .pending
            .entry(id)
            .or_insert_with(|| PendingFunctionCall {
                name: item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown_tool")
                    .to_string(),
                arguments: String::new(),
            });
        if let Some(name) = item.get("name").and_then(Value::as_str) {
            if entry.name.is_empty() || entry.name == "unknown_tool" {
                entry.name = name.to_string();
            }
        }
        if let Some(delta) = item.get("arguments_delta").and_then(Value::as_str) {
            entry.arguments.push_str(delta);
        } else if let Some(delta) = item.get("arguments").and_then(Value::as_str) {
            entry.arguments.push_str(delta);
        }
        Vec::new()
    }

    fn parse_item_completed(&mut self, value: &Value) -> Vec<SessionEvent> {
        let Some(item) = value.get("item") else {
            return Vec::new();
        };

        let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
        match item_type {
            "reasoning" => item
                .get("text")
                .and_then(Value::as_str)
                .map(|text| {
                    vec![SessionEvent::Thinking {
                        text: text.to_string(),
                    }]
                })
                .unwrap_or_default(),
            "agent_message" | "message" => parse_codex_message_item(item),
            "function_call" => self.finalize_function_call(item),
            "function_call_output" | "tool_result" => parse_codex_function_call_output(item),
            _ => Vec::new(),
        }
    }

    fn finalize_function_call(&mut self, item: &Value) -> Vec<SessionEvent> {
        let id = function_call_id(item);
        let direct_args = item.get("arguments").and_then(Value::as_str);
        let direct_name = item.get("name").and_then(Value::as_str);

        let (tool_name, arg_string) = if let Some(id) = id.as_ref() {
            if let Some(pending) = self.pending.remove(id) {
                let name = direct_name
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or(pending.name);
                let args = direct_args
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or(pending.arguments);
                (name, args)
            } else {
                (
                    direct_name.unwrap_or("unknown_tool").to_string(),
                    direct_args.unwrap_or("").to_string(),
                )
            }
        } else {
            (
                direct_name.unwrap_or("unknown_tool").to_string(),
                direct_args.unwrap_or("").to_string(),
            )
        };

        let arguments = if arg_string.is_empty() {
            json!({})
        } else {
            serde_json::from_str::<Value>(&arg_string).unwrap_or(Value::String(arg_string))
        };

        vec![SessionEvent::ToolCall {
            tool_name,
            arguments,
            server: None,
        }]
    }
}

fn function_call_id(item: &Value) -> Option<String> {
    item.get("call_id")
        .and_then(Value::as_str)
        .or_else(|| item.get("id").and_then(Value::as_str))
        .map(str::to_string)
}

fn parse_codex_turn_completed(value: &Value) -> Vec<SessionEvent> {
    let usage = value.get("usage").cloned().unwrap_or_else(|| json!({}));
    vec![SessionEvent::Metadata {
        metadata: json!({
            "type": "codex_usage",
            "usage": usage,
        }),
    }]
}

fn parse_codex_function_call_output(item: &Value) -> Vec<SessionEvent> {
    let tool_name = item
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            item.get("call_id")
                .and_then(Value::as_str)
                .unwrap_or("unknown_tool")
        })
        .to_string();
    let raw_output = item
        .get("output")
        .cloned()
        .or_else(|| item.get("content").cloned())
        .unwrap_or(Value::Null);
    let success = !item
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    vec![SessionEvent::ToolResult {
        tool_name,
        output: raw_output,
        success,
    }]
}

fn parse_codex_message_item(item: &Value) -> Vec<SessionEvent> {
    if let Some(text) = item.get("text").and_then(Value::as_str) {
        if !text.is_empty() {
            return vec![SessionEvent::FinalText {
                text: text.to_string(),
            }];
        }
    }

    let Some(content) = item.get("content").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut text = String::new();
    for block in content {
        let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
        if matches!(block_type, "output_text" | "text") {
            if let Some(segment) = block.get("text").and_then(Value::as_str) {
                text.push_str(segment);
            }
        }
    }

    if text.is_empty() {
        Vec::new()
    } else {
        vec![SessionEvent::FinalText { text }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(lines: &[&str]) -> Vec<SessionEvent> {
        let mut parser = CodexParser::new();
        let mut all = Vec::new();
        for line in lines {
            all.extend(parser.parse_line(line));
        }
        all
    }

    #[test]
    fn codex_parser_emits_tool_call_for_function_call_item() {
        let line = r#"{"type":"item.completed","item":{"type":"function_call","call_id":"call_1","name":"shell","arguments":"{\"cmd\":\"ls\"}"}}"#;
        let events = parse(&[line]);
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
    fn codex_parser_emits_tool_result_for_function_call_output_item() {
        let line = r#"{"type":"item.completed","item":{"type":"function_call_output","call_id":"call_1","output":"file_a\nfile_b\n"}}"#;
        let events = parse(&[line]);
        assert_eq!(events.len(), 1);
        match &events[0] {
            SessionEvent::ToolResult {
                output, success, ..
            } => {
                assert_eq!(output, &json!("file_a\nfile_b\n"));
                assert!(*success);
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn codex_parser_emits_tool_call_with_accumulated_streaming_arguments() {
        let lines = [
            r#"{"type":"item.added","item":{"type":"function_call","call_id":"call_2","name":"shell","arguments":""}}"#,
            r#"{"type":"item.delta","item":{"type":"function_call","call_id":"call_2","arguments_delta":"{\"cmd\":\""}}"#,
            r#"{"type":"item.delta","item":{"type":"function_call","call_id":"call_2","arguments_delta":"pwd\"}"}}"#,
            r#"{"type":"item.completed","item":{"type":"function_call","call_id":"call_2","name":"shell"}}"#,
        ];
        let events = parse(&lines);
        assert_eq!(events.len(), 1, "expected single ToolCall, got {events:?}");
        match &events[0] {
            SessionEvent::ToolCall {
                tool_name,
                arguments,
                ..
            } => {
                assert_eq!(tool_name, "shell");
                assert_eq!(arguments, &json!({"cmd": "pwd"}));
            }
            other => panic!("expected ToolCall, got {other:?}"),
        }
    }

    #[test]
    fn codex_parser_parallel_tool_calls_emit_two_tool_calls() {
        let lines = [
            r#"{"type":"item.completed","item":{"type":"function_call","call_id":"call_a","name":"shell","arguments":"{\"cmd\":\"pwd\"}"}}"#,
            r#"{"type":"item.completed","item":{"type":"function_call","call_id":"call_b","name":"shell","arguments":"{\"cmd\":\"whoami\"}"}}"#,
        ];
        let events = parse(&lines);
        let tool_calls: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, SessionEvent::ToolCall { .. }))
            .collect();
        assert_eq!(tool_calls.len(), 2);
    }
}
