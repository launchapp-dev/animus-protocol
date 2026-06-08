#!/usr/bin/env bash
# Regenerate JSON Schema artifacts for every plugin-kind protocol crate.
#
# Each crate ships an `export-schema` binary that writes one `<Type>.json`
# per public wire type plus an `_all.json` bundle into
# `schemas/<crate>/`. This script runs all of them so a single command
# refreshes the full set, which downstream TypeScript / Python SDKs codegen
# from.
#
# Usage:
#   bash scripts/export-all-schemas.sh
set -euo pipefail

# Resolve the workspace root from this script's location so it works
# regardless of the current working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${WORKSPACE_ROOT}"

# Each entry is "<crate>" — the bin name is "<crate>-export-schema" by
# convention.
CRATES=(
    "animus-plugin-protocol"
    "animus-subject-protocol"
    "animus-provider-protocol"
    "animus-trigger-protocol"
    "animus-log-storage-protocol"
    "animus-transport-protocol"
    "animus-queue-protocol"
    "animus-workflow-runner-protocol"
    "animus-durable-store-protocol"
    "animus-memory-store-protocol"
    "animus-notifier-protocol"
)

for crate in "${CRATES[@]}"; do
    echo "==> ${crate}"
    cargo run -q -p "${crate}" --bin "${crate}-export-schema"
done

echo "All schemas exported under ${WORKSPACE_ROOT}/schemas/"
