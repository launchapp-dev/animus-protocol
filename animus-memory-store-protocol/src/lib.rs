//! Protocol types for `memory_store` plugins.
//!
//! Memory stores provide persistent semantic memory across runs, agents,
//! and tasks. The v0.5 reference implementation is
//! `launchapp-dev/animus-memory-zep` (a TypeScript plugin backed by
//! Zep Cloud).
//!
//! Plugin authors implement five JSON-RPC methods:
//!
//! - [`METHOD_MEMORY_PUT`] — store a value under a key in a scope.
//! - [`METHOD_MEMORY_GET`] — retrieve a value by exact key.
//! - [`METHOD_MEMORY_QUERY`] — semantic search within a scope.
//! - [`METHOD_MEMORY_LIST_SCOPES`] — paginated list of known scopes.
//! - [`METHOD_MEMORY_DELETE_SCOPE`] — delete a scope and all its entries.
//!
//! Memory scopes are hierarchical: project-wide (project_id only), per-agent
//! (project_id + agent_id), or per-task (project_id + agent_id + task_id).
//! Plugins map this hierarchy to backend-specific structures
//! (Zep: standalone Graphs with id prefixes; SQLite-backed: tables).
//!
//! Project root is bound at `initialize` time via the
//! `init_extensions.project_binding` extension.

#![warn(missing_docs)]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `PluginKind` wire value for this kind.
pub const KIND: &str = "memory_store";

/// Per-crate semver protocol version.
pub const PROTOCOL_VERSION: &str = "0.1.0";

/// Store a value under `key` in `scope`.
pub const METHOD_MEMORY_PUT: &str = "memory/put";
/// Retrieve a value by exact key.
pub const METHOD_MEMORY_GET: &str = "memory/get";
/// Semantic search within a scope.
pub const METHOD_MEMORY_QUERY: &str = "memory/query";
/// Paginated list of scopes (optionally filtered by project_id).
pub const METHOD_MEMORY_LIST_SCOPES: &str = "memory/list_scopes";
/// Delete a scope and all its entries.
pub const METHOD_MEMORY_DELETE_SCOPE: &str = "memory/delete_scope";

// =====================================================================
// Types
// =====================================================================

/// Memory scope identifier. The scope id is derived by the plugin from
/// these fields as a flat namespace. Project-wide memory: `project_id`
/// only. Per-agent: `project_id` + `agent_id`. Per-task: `project_id` +
/// `agent_id` + `task_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct MemoryScope {
    /// Project identifier (required).
    pub project_id: String,
    /// Optional agent identifier (per-agent or per-task scopes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// Optional task identifier (per-task scopes only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

/// Request for [`METHOD_MEMORY_PUT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PutMemoryRequest {
    /// Target scope.
    pub scope: MemoryScope,
    /// Key under which to store the value.
    pub key: String,
    /// Value to store (JSON).
    pub value: Value,
    /// Optional hint; backends MAY ignore. Backends MUST declare
    /// `native_ttl` in their capabilities; when false, the TTL is recorded
    /// in metadata only and the caller is responsible for eviction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

/// Response for [`METHOD_MEMORY_PUT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PutMemoryResponse {
    /// `true` if the value was accepted.
    pub ack: bool,
    /// `true` if the backend has fully indexed the value and `query` is
    /// expected to find it immediately. `false` when ingestion is async
    /// (e.g., Zep). Callers needing read-after-write semantics must
    /// surface this.
    pub indexed_immediately: bool,
    /// Backend-issued identifier for the stored entry. Useful for
    /// delete-by-id or audit. Empty string when the backend doesn't expose
    /// stable record ids.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub record_id: String,
}

/// Request for [`METHOD_MEMORY_GET`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GetMemoryRequest {
    /// Scope to read from.
    pub scope: MemoryScope,
    /// Exact key to look up.
    pub key: String,
}

/// Response for [`METHOD_MEMORY_GET`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GetMemoryResponse {
    /// `true` iff the key was found.
    pub found: bool,
    /// The stored value, if found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

/// Request for [`METHOD_MEMORY_QUERY`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueryMemoryRequest {
    /// Scope to search.
    pub scope: MemoryScope,
    /// Natural-language query.
    pub query: String,
    /// Maximum results to return. Backends MAY clamp to their declared
    /// `max_query_top_k` capability.
    pub top_k: u32,
}

/// Response for [`METHOD_MEMORY_QUERY`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueryMemoryResponse {
    /// Top-k semantic matches in descending relevance.
    pub results: Vec<MemoryQueryResult>,
}

/// A single semantic query hit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MemoryQueryResult {
    /// The key the value was stored under.
    pub key: String,
    /// The stored value.
    pub value: Value,
    /// Relevance score (backend-specific; higher is more relevant).
    pub score: f32,
}

/// Request for [`METHOD_MEMORY_LIST_SCOPES`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct ListScopesRequest {
    /// If set, only return scopes under this project. Else return all
    /// scopes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Cursor-based pagination. `None` for the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Page size. Backends MAY clamp to their declared per-backend
    /// maximum. Defaults to 100 if unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
}

/// Response for [`METHOD_MEMORY_LIST_SCOPES`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ListScopesResponse {
    /// Returned scopes.
    pub scopes: Vec<MemoryScope>,
    /// Cursor for the next page, or `None` if exhausted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Request for [`METHOD_MEMORY_DELETE_SCOPE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DeleteScopeRequest {
    /// Scope to delete.
    pub scope: MemoryScope,
}

/// Response for [`METHOD_MEMORY_DELETE_SCOPE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DeleteScopeResponse {
    /// `true` if the scope was deleted (or didn't exist; idempotent).
    pub ack: bool,
}

// =====================================================================
// Capabilities
// =====================================================================

/// Capability flags for memory_store plugins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct MemoryStoreCapabilities {
    /// `true` if the backend natively honors `ttl_secs`. Zep: `false`.
    /// SQLite-backed: `true`.
    #[serde(default)]
    pub native_ttl: bool,
    /// `true` if `memory/get` is O(1) exact-key. Zep: `false`
    /// (search-based fallback).
    #[serde(default)]
    pub native_key_get: bool,
    /// `true` if put → query is immediately consistent. Zep: `false`.
    #[serde(default)]
    pub strong_consistency: bool,
    /// Maximum `top_k` allowed on a query.
    #[serde(default)]
    pub max_query_top_k: u32,
}

// =====================================================================
// Error codes
// =====================================================================

/// JSON-RPC error codes for the memory_store protocol. The
/// `-32400..-32499` range is reserved for this kind.
pub mod error_codes {
    /// Scope not found (e.g., delete_scope on unknown id).
    pub const SCOPE_NOT_FOUND: i32 = -32401;
    /// Memory key not found (memory/get when key doesn't exist).
    pub const KEY_NOT_FOUND: i32 = -32402;
    /// Two distinct scopes normalize to the same backend graph id.
    pub const MEMORY_SCOPE_COLLISION: i32 = -32403;
    /// Backend (Zep, etc.) unavailable.
    pub const BACKEND_UNAVAILABLE: i32 = -32404;
    /// Backend rate-limited the request. Caller should back off.
    pub const RATE_LIMITED: i32 = -32405;
    /// `top_k` exceeded backend-declared maximum.
    pub const QUERY_TOP_K_EXCEEDED: i32 = -32406;
    /// Project root mismatch.
    pub const PROJECT_BINDING_MISMATCH: i32 = -32407;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_round_trips() {
        let s = MemoryScope {
            project_id: "proj_1".into(),
            agent_id: Some("agent_1".into()),
            task_id: None,
        };
        let v = serde_json::to_value(&s).unwrap();
        assert!(v.get("task_id").is_none(), "None should be omitted");
        let back: MemoryScope = serde_json::from_value(v).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn put_response_carries_indexed_immediately_and_record_id() {
        let r = PutMemoryResponse {
            ack: true,
            indexed_immediately: false,
            record_id: "ep_123".into(),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v.get("indexed_immediately"),
            Some(&serde_json::json!(false))
        );
        let back: PutMemoryResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }
}
