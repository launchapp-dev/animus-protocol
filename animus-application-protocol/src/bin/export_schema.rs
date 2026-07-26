//! Export JSON Schema artifacts for application-facing Animus wire types.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use animus_application_protocol::{
    AllowedAction, AllowedApplicationChatControls, ApplicationChatControls,
    ApplicationChatReceiptFrame, ApplicationChatTurnStatus, ApplicationResourceKind,
    ResourceVisibility, APPLICATION_CHAT_CONTROLS_SCHEMA, MAX_APPLICATION_CHAT_CONTROLS_BYTES,
    MAX_APPLICATION_CHAT_CONTROL_REF_BYTES, MAX_APPLICATION_CHAT_ERROR_BYTES,
    MAX_APPLICATION_CHAT_SEQUENCE, MAX_APPLICATION_PROTOCOL_STRING_BYTES,
};
use schemars::{schema_for, Schema};

fn default_out_dir() -> PathBuf {
    let base = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .and_then(|dir| dir.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    base.join("schemas").join("animus-application-protocol")
}

fn parse_out_dir(args: &[String]) -> Option<PathBuf> {
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--out" | "-o" => return iter.next().map(PathBuf::from),
            other if other.starts_with("--out=") => {
                return Some(PathBuf::from(&other["--out=".len()..]));
            }
            _ => {}
        }
    }
    None
}

/// Build all public application schema artifacts.
pub fn all_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        ("AllowedAction", schema_for!(AllowedAction)),
        (
            "ApplicationResourceKind",
            schema_for!(ApplicationResourceKind),
        ),
        ("ResourceVisibility", schema_for!(ResourceVisibility)),
        (
            "ApplicationChatControls",
            schema_for!(ApplicationChatControls),
        ),
        (
            "AllowedApplicationChatControls",
            schema_for!(AllowedApplicationChatControls),
        ),
        (
            "ApplicationChatReceiptFrame",
            schema_for!(ApplicationChatReceiptFrame),
        ),
        (
            "ApplicationChatTurnStatus",
            schema_for!(ApplicationChatTurnStatus),
        ),
    ]
}

/// Write one file per type and a combined `_all.json` bundle.
pub fn export_to(out_dir: &Path) -> std::io::Result<usize> {
    fs::create_dir_all(out_dir)?;
    let schemas = all_schemas();
    let mut defs: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (name, schema) in &schemas {
        let pretty = serde_json::to_string_pretty(schema).expect("schema serializes");
        fs::write(out_dir.join(format!("{name}.json")), format!("{pretty}\n"))?;
        let mut value = serde_json::to_value(schema).expect("schema serializes");
        if let Some(object) = value.as_object_mut() {
            if let Some(serde_json::Value::Object(inner)) = object.remove("$defs") {
                for (key, definition) in inner {
                    defs.entry(key).or_insert(definition);
                }
            }
            object.remove("$schema");
        }
        defs.insert((*name).to_string(), value);
    }
    let limits = serde_json::json!({
        "schema": "animus.application.limits.v1",
        "application_chat_controls_schema": APPLICATION_CHAT_CONTROLS_SCHEMA,
        "application_chat_controls_max_utf8_bytes": MAX_APPLICATION_CHAT_CONTROLS_BYTES,
        "application_chat_control_ref_max_utf8_bytes": MAX_APPLICATION_CHAT_CONTROL_REF_BYTES,
        "application_protocol_string_max_utf8_bytes": MAX_APPLICATION_PROTOCOL_STRING_BYTES,
        "application_chat_error_max_utf8_bytes": MAX_APPLICATION_CHAT_ERROR_BYTES,
        "application_chat_sequence_max": MAX_APPLICATION_CHAT_SEQUENCE,
    });
    let limits_pretty = serde_json::to_string_pretty(&limits).expect("limits serialize");
    fs::write(out_dir.join("_limits.json"), format!("{limits_pretty}\n"))?;
    let bundle = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "animus-application-protocol",
        "x-animus-limits": limits,
        "$defs": defs,
    });
    let pretty = serde_json::to_string_pretty(&bundle).expect("bundle serializes");
    fs::write(out_dir.join("_all.json"), format!("{pretty}\n"))?;
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
        Err(error) => {
            eprintln!("export-schema: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_is_complete() {
        let tmp = tempfile::tempdir().unwrap();
        let count = export_to(tmp.path()).unwrap();
        assert_eq!(count, all_schemas().len());
        let bundle: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(tmp.path().join("_all.json")).unwrap())
                .unwrap();
        for (name, _) in all_schemas() {
            assert!(bundle["$defs"].get(name).is_some(), "missing {name}");
        }
        let limits: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(tmp.path().join("_limits.json")).unwrap())
                .unwrap();
        assert_eq!(limits["application_protocol_string_max_utf8_bytes"], 512);
        assert_eq!(
            limits["application_chat_sequence_max"],
            9_007_199_254_740_991_u64
        );
        assert_eq!(bundle["x-animus-limits"], limits);
    }
}
