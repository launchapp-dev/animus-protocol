//! Export JSON Schema artifacts for every public wire type in
//! `animus-durable-store-protocol`.
//!
//! Usage:
//!
//! ```text
//! cargo run -p animus-durable-store-protocol --bin animus-durable-store-protocol-export-schema -- [--out <dir>]
//! ```
//!
//! Mirrors the binary in `animus-plugin-protocol`. The default output
//! directory is `schemas/animus-durable-store-protocol/` resolved relative
//! to the workspace root. One file is written per type plus an `_all.json`
//! bundle for tooling that wants a single artifact.
//!
//! The `step_status` / `commit_outcome` / `run_status` / `error_codes`
//! const modules are intentionally omitted: they are not wire message
//! types. The wire-level error shape is `animus_plugin_protocol::RpcError`,
//! exported by the sibling `animus-plugin-protocol` schema binary.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use animus_durable_store_protocol::{
    AbandonStepRequest, AbandonStepResponse, BeginStepRequest, BeginStepResponse,
    BeginWorkflowRunRequest, BeginWorkflowRunResponse, CommitStepRequest, CommitStepResponse,
    DurableStoreCapabilities, InFlightRun, QueryRunRequest, QueryRunResponse,
    RecoverInFlightRequest, RecoverInFlightResponse, StepError, StepRecord,
};
use schemars::{schema_for, Schema};

fn default_out_dir() -> PathBuf {
    let base = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .and_then(|dir| dir.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    base.join("schemas").join("animus-durable-store-protocol")
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

/// Build the list of `(TypeName, Schema)` pairs. Centralized so the
/// smoke test and the binary stay in sync.
pub fn all_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (
            "BeginWorkflowRunRequest",
            schema_for!(BeginWorkflowRunRequest),
        ),
        (
            "BeginWorkflowRunResponse",
            schema_for!(BeginWorkflowRunResponse),
        ),
        ("BeginStepRequest", schema_for!(BeginStepRequest)),
        ("BeginStepResponse", schema_for!(BeginStepResponse)),
        ("CommitStepRequest", schema_for!(CommitStepRequest)),
        ("CommitStepResponse", schema_for!(CommitStepResponse)),
        ("StepError", schema_for!(StepError)),
        ("AbandonStepRequest", schema_for!(AbandonStepRequest)),
        ("AbandonStepResponse", schema_for!(AbandonStepResponse)),
        (
            "RecoverInFlightRequest",
            schema_for!(RecoverInFlightRequest),
        ),
        (
            "RecoverInFlightResponse",
            schema_for!(RecoverInFlightResponse),
        ),
        ("InFlightRun", schema_for!(InFlightRun)),
        ("QueryRunRequest", schema_for!(QueryRunRequest)),
        ("QueryRunResponse", schema_for!(QueryRunResponse)),
        ("StepRecord", schema_for!(StepRecord)),
        (
            "DurableStoreCapabilities",
            schema_for!(DurableStoreCapabilities),
        ),
    ]
}

/// Write every type's schema to `out_dir` and emit a combined
/// `_all.json` bundle. Used by the binary and the smoke test.
///
/// The bundle places every type — and every nested `$defs` entry —
/// under a single top-level `$defs`, so `#/$defs/<Name>` references
/// inside the bundled schemas resolve against the bundle root.
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
        defs.insert((*name).to_string(), as_value);
    }

    let bundle = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "animus-durable-store-protocol",
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
    }

    #[test]
    fn begin_step_request_emits_object_type() {
        let schema = schema_for!(BeginStepRequest);
        let value = serde_json::to_value(&schema).expect("serializes");
        let type_field = value.get("type").expect("schema has a type field").clone();
        assert!(
            type_field == Value::String("object".to_string())
                || type_field
                    .as_array()
                    .map(|arr| arr.iter().any(|v| v == "object"))
                    .unwrap_or(false),
            "BeginStepRequest schema should report object type, got {type_field}"
        );
    }
}
