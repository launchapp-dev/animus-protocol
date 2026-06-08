//! Export JSON Schema artifacts for every public wire type in
//! `animus-chat-protocol`.
//!
//! Usage:
//!
//! ```text
//! cargo run -p animus-chat-protocol --bin animus-chat-protocol-export-schema -- [--out <dir>]
//! ```
//!
//! Mirrors the binary in `animus-plugin-protocol`. The default output
//! directory is `schemas/animus-chat-protocol/` resolved relative to the
//! workspace root. One file is written per type plus an `_all.json` bundle for
//! tooling that wants a single artifact.
//!
//! The `error_codes` const module is intentionally omitted: it is not a wire
//! message type. The wire-level error shape is
//! `animus_plugin_protocol::RpcError`, exported by the sibling
//! `animus-plugin-protocol` schema binary.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use animus_chat_protocol::{
    BlockDelta, ChatMessage, ChatModelInfo, ChatProviderCapabilities, ChatRole, ChatStreamEvent,
    ChatStreamNotification, ChatStreamRequest, ContentBlock, ContentBlockStart, ConversationMeta,
    CountTokensResponse, ImageSource, StopReason, ToolSchema, Usage,
};
use schemars::{schema_for, Schema};

fn default_out_dir() -> PathBuf {
    let base = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .and_then(|dir| dir.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    base.join("schemas").join("animus-chat-protocol")
}

fn parse_out_dir(args: &[String]) -> Option<PathBuf> {
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--out" | "-o" => {
                if let Some(value) = iter.next() {
                    return Some(PathBuf::from(value));
                }
            }
            other if other.starts_with("--out=") => {
                return Some(PathBuf::from(&other["--out=".len()..]));
            }
            _ => {}
        }
    }
    None
}

/// Build the list of `(TypeName, Schema)` pairs. Centralized so the smoke test
/// and the binary stay in sync.
pub fn all_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        ("ChatRole", schema_for!(ChatRole)),
        ("StopReason", schema_for!(StopReason)),
        ("Usage", schema_for!(Usage)),
        ("ImageSource", schema_for!(ImageSource)),
        ("ContentBlock", schema_for!(ContentBlock)),
        ("ChatMessage", schema_for!(ChatMessage)),
        ("ContentBlockStart", schema_for!(ContentBlockStart)),
        ("BlockDelta", schema_for!(BlockDelta)),
        ("ChatStreamEvent", schema_for!(ChatStreamEvent)),
        (
            "ChatStreamNotification",
            schema_for!(ChatStreamNotification),
        ),
        ("ToolSchema", schema_for!(ToolSchema)),
        ("ChatStreamRequest", schema_for!(ChatStreamRequest)),
        ("ChatModelInfo", schema_for!(ChatModelInfo)),
        ("CountTokensResponse", schema_for!(CountTokensResponse)),
        ("ConversationMeta", schema_for!(ConversationMeta)),
        (
            "ChatProviderCapabilities",
            schema_for!(ChatProviderCapabilities),
        ),
    ]
}

/// Write every type's schema to `out_dir` and emit a combined `_all.json`
/// bundle. Used by the binary and the smoke test.
///
/// The bundle places every type — and every nested `$defs` entry — under a
/// single top-level `$defs`, so `#/$defs/<Name>` references inside the bundled
/// schemas resolve against the bundle root.
/// Rewrite every `"$ref": "#"` (a recursive type's self-reference to its own
/// document root) into `"#/$defs/<name>"` so the reference still resolves once
/// the schema is nested under the bundle's `$defs`.
fn rewrite_root_self_refs(value: &mut serde_json::Value, name: &str) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(r)) = map.get_mut("$ref") {
                if r == "#" {
                    *r = format!("#/$defs/{name}");
                }
            }
            for v in map.values_mut() {
                rewrite_root_self_refs(v, name);
            }
        }
        serde_json::Value::Array(items) => {
            for v in items {
                rewrite_root_self_refs(v, name);
            }
        }
        _ => {}
    }
}

pub fn export_to(out_dir: &Path) -> std::io::Result<usize> {
    fs::create_dir_all(out_dir)?;
    let schemas = all_schemas();
    let mut defs: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (name, schema) in &schemas {
        let path = out_dir.join(format!("{name}.json"));
        let pretty = serde_json::to_string_pretty(schema).expect("schema serializes to JSON");
        fs::write(&path, format!("{pretty}\n"))?;

        let mut as_value = serde_json::to_value(schema).expect("schema serializes to JSON value");
        if let Some(obj) = as_value.as_object_mut() {
            if let Some(serde_json::Value::Object(inner)) = obj.remove("$defs") {
                for (k, v) in inner {
                    defs.entry(k).or_insert(v);
                }
            }
            obj.remove("$schema");
        }
        // schemars emits a recursive type's self-reference as `$ref: "#"`,
        // which points at the schema document root. Once this value is nested
        // under `_all.json`'s `$defs.<Name>`, the root is the bundle, not the
        // type — so rewrite root self-refs to `#/$defs/<Name>` to keep
        // recursive constraints (e.g. `ContentBlock`'s nested `content`) intact.
        rewrite_root_self_refs(&mut as_value, name);
        defs.insert((*name).to_string(), as_value);
    }

    let bundle = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "animus-chat-protocol",
        "$defs": defs,
    });
    let bundle_path = out_dir.join("_all.json");
    let bundle_pretty = serde_json::to_string_pretty(&bundle).expect("bundle serializes to JSON");
    fs::write(&bundle_path, format!("{bundle_pretty}\n"))?;
    Ok(schemas.len())
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let out_dir = parse_out_dir(&args).unwrap_or_else(default_out_dir);
    match export_to(&out_dir) {
        Ok(count) => {
            println!("wrote {count} schemas + _all.json to {}", out_dir.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("export-schema: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn export_writes_one_file_per_type_and_bundle() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let count = export_to(tmp.path()).expect("export ok");
        assert!(count > 0);

        for (name, _) in all_schemas() {
            let path = tmp.path().join(format!("{name}.json"));
            let raw = std::fs::read_to_string(&path).expect("schema file readable");
            let value: Value = serde_json::from_str(&raw).expect("schema file parses");
            assert!(value.is_object(), "{name} schema should be a JSON object");
            assert!(
                value.get("$schema").is_some(),
                "{name} schema should include $schema"
            );
            assert!(
                value.get("title").is_some(),
                "{name} schema should include title"
            );
        }

        let bundle_raw =
            std::fs::read_to_string(tmp.path().join("_all.json")).expect("bundle readable");
        let bundle: Value = serde_json::from_str(&bundle_raw).expect("bundle parses");
        let defs = bundle
            .get("$defs")
            .and_then(|d| d.as_object())
            .expect("bundle has $defs");
        for (name, _) in all_schemas() {
            assert!(
                defs.contains_key(name),
                "bundle $defs should contain {name}"
            );
        }
        assert!(
            bundle.get("$schema").is_some(),
            "bundle should advertise $schema"
        );
        assert!(
            !contains_root_self_ref(&bundle),
            "bundle must not retain bare `$ref: #` self-refs — they would \
             resolve to the bundle root instead of the owning type"
        );
        let content_block = defs.get("ContentBlock").expect("ContentBlock in bundle");
        assert!(
            json_to_string(content_block).contains("#/$defs/ContentBlock"),
            "ContentBlock self-ref should be rewritten to #/$defs/ContentBlock"
        );
    }

    fn json_to_string(v: &Value) -> String {
        serde_json::to_string(v).expect("serialize")
    }

    fn contains_root_self_ref(v: &Value) -> bool {
        match v {
            Value::Object(map) => {
                if matches!(map.get("$ref"), Some(Value::String(r)) if r == "#") {
                    return true;
                }
                map.values().any(contains_root_self_ref)
            }
            Value::Array(items) => items.iter().any(contains_root_self_ref),
            _ => false,
        }
    }

    #[test]
    fn chat_message_emits_object_type() {
        let schema = schema_for!(ChatMessage);
        let value = serde_json::to_value(&schema).expect("serializes");
        let type_field = value.get("type").expect("schema has a type field").clone();
        assert!(
            type_field == Value::String("object".to_string())
                || type_field
                    .as_array()
                    .map(|arr| arr.iter().any(|v| v == "object"))
                    .unwrap_or(false),
            "ChatMessage schema should report object type, got {type_field}"
        );
    }
}
