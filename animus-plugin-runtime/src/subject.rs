//! Thin `subject_backend_main` helper for plugins that implement
//! [`animus_subject_protocol::SubjectBackend`].
//!
//! Mirrors the v0.1.13 entrypoint of the same name. Internally it wires the
//! five non-streaming subject verbs (`list`, `get`, `update`, `delete`,
//! `schema`) onto a [`Plugin`] instance and drives [`Plugin::run`]. The
//! `subject/watch` streaming subscription is **not** registered by the
//! helper because the generic `Plugin` shell does not yet model
//! per-request notification streams; backends that need watch should
//! drive the [`Plugin`] builder directly and register a custom
//! notification handler. Plugin authors that need full control of the
//! generic shell (custom method handlers, init hooks, additional
//! capabilities) can call [`subject_plugin`] to obtain the partially-built
//! [`Plugin`] and continue customizing it before `.run().await`.
//!
//! # Example
//!
//! ```ignore
//! use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_SUBJECT_BACKEND};
//! use animus_plugin_runtime::subject_backend_main;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let info = PluginInfo {
//!         name: "animus-subject-linear".into(),
//!         version: env!("CARGO_PKG_VERSION").into(),
//!         plugin_kind: PLUGIN_KIND_SUBJECT_BACKEND.into(),
//!         description: Some("Linear subject backend".into()),
//!     };
//!     subject_backend_main(info, my_backend::LinearBackend::new()).await
//! }
//! ```

use std::sync::Arc;

use animus_plugin_protocol::{error_codes, PluginInfo, RpcError, PLUGIN_KIND_SUBJECT_BACKEND};
use animus_subject_protocol::{
    BackendError, SubjectBackend, SubjectFilter, SubjectId, SubjectPatch, METHOD_SUBJECT_DELETE,
    METHOD_SUBJECT_GET, METHOD_SUBJECT_LIST, METHOD_SUBJECT_SCHEMA, METHOD_SUBJECT_UPDATE,
};
use serde::Deserialize;

use crate::Plugin;

/// Build a [`Plugin`] for the supplied [`SubjectBackend`] without consuming
/// the builder.
///
/// The five non-streaming subject verbs (`list`, `get`, `update`,
/// `delete`, `schema`) are registered with their canonical
/// `subject/<verb>` method names. The streaming `subject/watch`
/// subscription is **not** registered: the generic `Plugin` shell does
/// not yet model per-subscription notification streams, so backends that
/// need to deliver `subject/changed` events must drive the [`Plugin`]
/// builder directly and register a custom subscription handler.
///
/// Plugin authors that want the kind-prefixed `<kind>/<verb>` forms (the
/// form the daemon's `SubjectRouter` actually emits today) should also
/// call [`subject_plugin_with_kind_aliases`].
///
/// Returns the [`Plugin`] so the caller can continue chaining builder
/// methods (`.description(...)`, `.on_init(...)`, additional capabilities,
/// etc.) before invoking `.run().await`.
pub fn subject_plugin<B: SubjectBackend + 'static>(info: PluginInfo, backend: B) -> Plugin {
    let backend = Arc::new(backend);
    let methods = vec![
        METHOD_SUBJECT_LIST.to_string(),
        METHOD_SUBJECT_GET.to_string(),
        METHOD_SUBJECT_UPDATE.to_string(),
        METHOD_SUBJECT_DELETE.to_string(),
        METHOD_SUBJECT_SCHEMA.to_string(),
    ];

    let health_backend = backend.clone();
    Plugin::new(info.name, info.version, PLUGIN_KIND_SUBJECT_BACKEND)
        .description(info.description.unwrap_or_default())
        .methods(methods)
        .on_health(move || {
            let backend = health_backend.clone();
            async move { backend.health().await.map_err(backend_error_to_rpc) }
        })
        .register_subject_verbs(backend)
}

/// Build a [`Plugin`] and register both the canonical `subject/<verb>` and
/// the kind-prefixed `<kind>/<verb>` aliases for every kind in
/// `subject_kinds`.
///
/// The daemon's `SubjectRouter` dispatches by `<kind>/<verb>` (e.g.
/// `task/list`, `task/create`, `task/delete`) — the kind-prefixed form is
/// the durable shape in production. Backends should pass their supported
/// kinds (e.g. `["task"]`, `["requirement"]`, `["issue", "epic"]`) so the
/// daemon's `<kind>/<verb>` invocations reach the same handler as the
/// protocol-canonical `subject/<verb>`.
pub fn subject_plugin_with_kind_aliases<B: SubjectBackend + 'static, I, S>(
    info: PluginInfo,
    backend: B,
    subject_kinds: I,
) -> Plugin
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let backend = Arc::new(backend);
    let kinds: Vec<String> = subject_kinds.into_iter().map(Into::into).collect();

    let mut methods = vec![
        METHOD_SUBJECT_LIST.to_string(),
        METHOD_SUBJECT_GET.to_string(),
        METHOD_SUBJECT_UPDATE.to_string(),
        METHOD_SUBJECT_DELETE.to_string(),
        METHOD_SUBJECT_SCHEMA.to_string(),
    ];
    for kind in &kinds {
        methods.push(format!("{kind}/list"));
        methods.push(format!("{kind}/get"));
        methods.push(format!("{kind}/update"));
        methods.push(format!("{kind}/delete"));
        methods.push(format!("{kind}/schema"));
    }

    let health_backend = backend.clone();
    let mut plugin = Plugin::new(info.name, info.version, PLUGIN_KIND_SUBJECT_BACKEND)
        .description(info.description.unwrap_or_default())
        .methods(methods)
        .subject_kinds(kinds.clone())
        .on_health(move || {
            let backend = health_backend.clone();
            async move { backend.health().await.map_err(backend_error_to_rpc) }
        })
        .register_subject_verbs(backend.clone());

    for kind in kinds {
        plugin = plugin.register_kind_aliases(&kind, backend.clone());
    }
    plugin
}

/// Run a subject-backend plugin's stdio JSON-RPC loop with the default
/// configuration.
///
/// Drop-in for the v0.1.13 entrypoint of the same name. Reads
/// `backend.schema().kinds` once at startup and registers both the
/// canonical `subject/<verb>` and the kind-prefixed `<kind>/<verb>`
/// aliases for every declared kind, matching the dispatcher shape the
/// daemon's `SubjectRouter` produces in production. The function returns
/// when stdin closes (clean shutdown) or on a fatal I/O error.
pub async fn subject_backend_main<B: SubjectBackend + 'static>(
    info: PluginInfo,
    backend: B,
) -> anyhow::Result<()> {
    let kinds = backend.schema().kinds.clone();
    subject_plugin_with_kind_aliases(info, backend, kinds)
        .run()
        .await
}

/// Run a subject-backend plugin's stdio JSON-RPC loop with the supplied
/// kind aliases registered alongside the canonical `subject/*` verbs.
///
/// Use this when the daemon dispatches through the kind-prefixed
/// `<kind>/<verb>` form (which it does for every first-party subject
/// backend as of v0.5).
pub async fn subject_backend_main_with_kinds<B: SubjectBackend + 'static, I, S>(
    info: PluginInfo,
    backend: B,
    subject_kinds: I,
) -> anyhow::Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    subject_plugin_with_kind_aliases(info, backend, subject_kinds)
        .run()
        .await
}

/// Run a subject-backend plugin's stdio JSON-RPC loop, advertising
/// additional capability strings alongside the runtime-derived defaults.
///
/// Restored in v0.5.7 for parity with the v0.1.13 entrypoint of the same
/// name. The extra capability strings are appended to the methods list
/// reported in the manifest and `initialize` response. Plugins built
/// against v0.1.13 that called this entrypoint continue to compile against
/// v0.5.7 unchanged.
pub async fn subject_backend_main_with_capabilities<B: SubjectBackend + 'static>(
    info: PluginInfo,
    backend: B,
    extra_capabilities: Vec<String>,
) -> anyhow::Result<()> {
    let kinds = backend.schema().kinds.clone();
    let mut plugin = subject_plugin_with_kind_aliases(info, backend, kinds);
    let mut methods: Vec<String> = plugin.advertised_methods().to_vec();
    for cap in extra_capabilities {
        if !methods.contains(&cap) {
            methods.push(cap);
        }
    }
    plugin = plugin.methods(methods);
    plugin.run().await
}

// =====================================================================
// Internal: shared verb registration logic
// =====================================================================

#[derive(Debug, Deserialize)]
struct GetRequest {
    id: SubjectId,
}

#[derive(Debug, Deserialize)]
struct UpdateRequest {
    id: SubjectId,
    patch: SubjectPatch,
}

#[derive(Debug, Deserialize)]
struct DeleteRequest {
    id: SubjectId,
}

#[derive(Debug, Deserialize, Default)]
struct ListRequest {
    #[serde(default, flatten)]
    filter: SubjectFilter,
}

fn backend_error_to_rpc(err: BackendError) -> RpcError {
    err.into()
}

/// Extension trait that registers all five non-streaming subject verbs on a
/// [`Plugin`] for a given backend, plus a stub `subject/watch` that returns
/// `METHOD_NOT_SUPPORTED` (streaming subscription would need first-class
/// support from the generic `Plugin` shell, which v0.5.7 does not yet add).
trait PluginSubjectExt {
    fn register_subject_verbs<B: SubjectBackend + 'static>(self, backend: Arc<B>) -> Self;
    fn register_kind_aliases<B: SubjectBackend + 'static>(
        self,
        kind: &str,
        backend: Arc<B>,
    ) -> Self;
}

impl PluginSubjectExt for Plugin {
    fn register_subject_verbs<B: SubjectBackend + 'static>(self, backend: Arc<B>) -> Self {
        register_subject_verbs_with_prefix(self, "subject", backend)
    }

    fn register_kind_aliases<B: SubjectBackend + 'static>(
        self,
        kind: &str,
        backend: Arc<B>,
    ) -> Self {
        register_subject_verbs_with_prefix(self, kind, backend)
    }
}

fn register_subject_verbs_with_prefix<B: SubjectBackend + 'static>(
    plugin: Plugin,
    prefix: &str,
    backend: Arc<B>,
) -> Plugin {
    let list_method = format!("{prefix}/list");
    let get_method = format!("{prefix}/get");
    let update_method = format!("{prefix}/update");
    let delete_method = format!("{prefix}/delete");
    let schema_method = format!("{prefix}/schema");

    // When the prefix is a kind alias (anything other than the canonical
    // `subject` prefix), v0.1.13's dispatcher injected the kind into
    // `SubjectFilter.kind` so the backend could distinguish `task/list`
    // from `issue/list`. Preserve that behavior.
    let filter_kind: Option<String> = if prefix == "subject" {
        None
    } else {
        Some(prefix.to_string())
    };

    let list_backend = backend.clone();
    let get_backend = backend.clone();
    let update_backend = backend.clone();
    let delete_backend = backend.clone();
    let schema_backend = backend.clone();

    plugin
        .register_raw_method(list_method, move |params, _ctx| {
            let backend = list_backend.clone();
            let filter_kind = filter_kind.clone();
            async move {
                let req: ListRequest = if params.is_null() {
                    ListRequest::default()
                } else {
                    serde_json::from_value(params).map_err(|error| RpcError {
                        code: error_codes::INVALID_PARAMS,
                        message: format!("invalid subject/list params: {error}"),
                        data: None,
                    })?
                };
                let mut filter = req.filter;
                if let Some(kind) = filter_kind {
                    if !filter.kind.iter().any(|k| k == &kind) {
                        filter.kind.push(kind);
                    }
                }
                let list = backend.list(filter).await.map_err(backend_error_to_rpc)?;
                serde_json::to_value(list).map_err(|error| RpcError {
                    code: error_codes::INTERNAL_ERROR,
                    message: format!("failed to encode subject/list response: {error}"),
                    data: None,
                })
            }
        })
        .register_method::<GetRequest, _, _, _>(get_method, move |req, _ctx| {
            let backend = get_backend.clone();
            async move { backend.get(&req.id).await.map_err(backend_error_to_rpc) }
        })
        .register_method::<UpdateRequest, _, _, _>(update_method, move |req, _ctx| {
            let backend = update_backend.clone();
            async move {
                backend
                    .update(&req.id, req.patch)
                    .await
                    .map_err(backend_error_to_rpc)
            }
        })
        .register_method::<DeleteRequest, _, _, _>(delete_method, move |req, _ctx| {
            let backend = delete_backend.clone();
            async move { backend.delete(&req.id).await.map_err(backend_error_to_rpc) }
        })
        .register_raw_method(schema_method, move |_params, _ctx| {
            let backend = schema_backend.clone();
            async move {
                let schema = backend.schema();
                serde_json::to_value(schema).map_err(|error| RpcError {
                    code: error_codes::INTERNAL_ERROR,
                    message: format!("failed to encode subject/schema response: {error}"),
                    data: None,
                })
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_plugin_protocol::HealthCheckResult;
    use animus_subject_protocol::{
        EventStream, Subject, SubjectList, SubjectSchema, SubjectStatus,
    };
    use async_trait::async_trait;
    use chrono::Utc;
    use serde_json::Value;
    use std::collections::BTreeMap;

    struct StubBackend;

    #[async_trait]
    impl SubjectBackend for StubBackend {
        async fn list(&self, _filter: SubjectFilter) -> Result<SubjectList, BackendError> {
            Ok(SubjectList {
                subjects: vec![],
                next_cursor: None,
                fetched_at: Utc::now(),
            })
        }
        async fn get(&self, id: &SubjectId) -> Result<Subject, BackendError> {
            Ok(Subject {
                id: id.clone(),
                kind: "task".into(),
                title: "stub".into(),
                description: None,
                status: SubjectStatus::Ready,
                priority: None,
                assignee: None,
                labels: vec![],
                parent: None,
                children: vec![],
                url: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                custom: BTreeMap::new(),
                native_status: None,
                status_metadata: Value::Null,
                attachments: vec![],
            })
        }
        async fn update(
            &self,
            id: &SubjectId,
            _patch: SubjectPatch,
        ) -> Result<Subject, BackendError> {
            self.get(id).await
        }
        async fn watch(&self) -> Option<EventStream> {
            None
        }
        fn schema(&self) -> SubjectSchema {
            SubjectSchema {
                kinds: vec!["task".into()],
                status_values: vec![SubjectStatus::Ready],
                supports_watch: false,
                supports_create: true,
                supports_delete: false,
                supports_pagination: false,
                native_status_values: vec![],
                status_dispatch_hints: vec![],
                custom_fields: vec![],
            }
        }
        async fn health(&self) -> Result<HealthCheckResult, BackendError> {
            Ok(HealthCheckResult {
                status: animus_plugin_protocol::HealthStatus::Healthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: None,
            })
        }
        // Use the default delete impl returning Unsupported.
    }

    #[test]
    fn subject_plugin_advertises_the_five_non_streaming_methods() {
        let info = PluginInfo {
            name: "stub".into(),
            version: "0.0.0".into(),
            plugin_kind: PLUGIN_KIND_SUBJECT_BACKEND.into(),
            description: Some("stub".into()),
        };
        let plugin = subject_plugin(info, StubBackend);
        let advertised = plugin.advertised_methods();
        for expected in [
            METHOD_SUBJECT_LIST,
            METHOD_SUBJECT_GET,
            METHOD_SUBJECT_UPDATE,
            METHOD_SUBJECT_DELETE,
            METHOD_SUBJECT_SCHEMA,
        ] {
            assert!(
                advertised.iter().any(|m| m == expected),
                "expected {expected} in advertised methods"
            );
        }
        assert!(
            !advertised.iter().any(|m| m == "subject/watch"),
            "subject/watch must NOT be advertised by the default helper",
        );
    }

    #[test]
    fn subject_plugin_with_kind_aliases_advertises_kind_forms() {
        let info = PluginInfo {
            name: "stub".into(),
            version: "0.0.0".into(),
            plugin_kind: PLUGIN_KIND_SUBJECT_BACKEND.into(),
            description: Some("stub".into()),
        };
        let plugin = subject_plugin_with_kind_aliases(info, StubBackend, ["task"]);
        let advertised = plugin.advertised_methods();
        for expected in [
            "subject/list",
            "subject/delete",
            "task/list",
            "task/get",
            "task/update",
            "task/delete",
            "task/schema",
        ] {
            assert!(
                advertised.iter().any(|m| m == expected),
                "expected {expected} in advertised methods"
            );
        }
    }

    #[test]
    fn subject_plugin_registers_handlers_for_the_five_non_streaming_verbs() {
        let info = PluginInfo {
            name: "stub".into(),
            version: "0.0.0".into(),
            plugin_kind: PLUGIN_KIND_SUBJECT_BACKEND.into(),
            description: None,
        };
        let plugin = subject_plugin(info, StubBackend);
        for verb in [
            METHOD_SUBJECT_LIST,
            METHOD_SUBJECT_GET,
            METHOD_SUBJECT_UPDATE,
            METHOD_SUBJECT_DELETE,
            METHOD_SUBJECT_SCHEMA,
        ] {
            assert!(
                plugin.has_method_handler(verb),
                "expected handler for {verb}"
            );
        }
        assert!(
            !plugin.has_method_handler("subject/watch"),
            "subject/watch must NOT have a stub handler",
        );
    }
}
