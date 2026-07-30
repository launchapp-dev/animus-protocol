//! Export JSON Schema artifacts for `animus-execution-protocol`.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use animus_execution_protocol::{
    ExecutionFence, QueueLeaseFence, RepositoryReservation, SubjectGeneration,
};
use schemars::{schema_for, Schema};

fn default_out_dir() -> PathBuf {
    let base = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .and_then(|dir| dir.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    base.join("schemas").join("animus-execution-protocol")
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

/// All public schema-bearing types in this crate.
pub fn all_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        ("ExecutionFence", schema_for!(ExecutionFence)),
        ("SubjectGeneration", schema_for!(SubjectGeneration)),
        ("QueueLeaseFence", schema_for!(QueueLeaseFence)),
        ("RepositoryReservation", schema_for!(RepositoryReservation)),
    ]
}

/// Write per-type schemas and a combined bundle.
pub fn export_to(out_dir: &Path) -> std::io::Result<usize> {
    fs::create_dir_all(out_dir)?;
    let schemas = all_schemas();
    let mut defs: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (name, schema) in &schemas {
        fs::write(
            out_dir.join(format!("{name}.json")),
            format!(
                "{}\n",
                serde_json::to_string_pretty(schema).expect("schema serializes")
            ),
        )?;
        let mut value = serde_json::to_value(schema).expect("schema serializes");
        if let Some(object) = value.as_object_mut() {
            if let Some(serde_json::Value::Object(inner)) = object.remove("$defs") {
                for (key, nested) in inner {
                    defs.entry(key).or_insert(nested);
                }
            }
            object.remove("$schema");
        }
        defs.insert((*name).to_string(), value);
    }
    let bundle = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "animus-execution-protocol",
        "$defs": defs,
    });
    fs::write(
        out_dir.join("_all.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&bundle).expect("bundle serializes")
        ),
    )?;
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
    fn export_writes_every_schema_and_bundle() {
        let temp = tempfile::tempdir().unwrap();
        let count = export_to(temp.path()).unwrap();
        assert_eq!(count, all_schemas().len());
        assert!(temp.path().join("ExecutionFence.json").is_file());
        assert!(temp.path().join("_all.json").is_file());
    }
}
