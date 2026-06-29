//! Opaque, transport-asserted [`Actor`] identity for Animus per-user scoping.
//!
//! # The contract: the transport asserts, the kernel relays, plugins scope
//!
//! Animus 0.7 introduces per-user actors. The load-bearing rule is that the
//! **kernel never authenticates and never interprets an actor**. A transport
//! (HTTP/GraphQL/MCP front door) authenticates the caller and *asserts* an
//! [`Actor`] onto the request it forwards inward. The kernel relays that actor
//! verbatim through the control / workflow-runner / session hops without ever
//! branching on it. Downstream **plugins** (subject backends, config sources,
//! journals, conversation/memory stores, ...) are the only components that read
//! the actor — and only to *scope* the data they own (rows owned by
//! [`Actor::user_id`], tenant partitioning by [`Actor::tenant_id`]).
//!
//! Because the kernel treats the actor as opaque relay payload, this crate is a
//! deliberately tiny, zero-dependency leaf (serde + schemars only) that every
//! other protocol crate can depend on without taking on weight or coupling.
//!
//! # Claims are transport-asserted, never kernel-enforced
//!
//! [`Actor::claims`] (e.g. [`CLAIM_ADMIN`]) are advisory strings the transport
//! stamps based on its own authentication. The kernel MUST NOT branch on them.
//! A plugin MAY consult them for its own authorization decisions, but the
//! source of truth for "is this caller an admin" is the transport that asserted
//! the claim, not the kernel.
//!
//! # Back-compat
//!
//! Every consumer carries the actor as `Option<Actor>` with
//! `#[serde(default, skip_serializing_if = "Option::is_none")]`, so a peer on
//! an older protocol version simply omits the field and continues to
//! deserialize. The actor is purely additive.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An opaque, transport-asserted caller identity.
///
/// Constructed by the transport after it authenticates the caller, relayed
/// verbatim by the kernel, and consumed only by plugins for data scoping. See
/// the [crate-level docs](crate) for the full contract.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct Actor {
    /// Stable identifier for the authenticated user. Opaque to the kernel;
    /// plugins use it to scope owned rows to a user.
    pub user_id: String,

    /// Transport-asserted claims (e.g. `["admin"]`). Advisory only: the kernel
    /// never branches on these. A plugin MAY consult them for its own
    /// authorization decisions. Omitted from the wire when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claims: Vec<String>,

    /// Optional tenant / organization partition the user belongs to. Plugins
    /// use it for multi-tenant data isolation. Omitted from the wire when
    /// absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
}

/// The well-known claim string a transport asserts for an administrative
/// caller. Advisory only — see [`Actor::claims`].
pub const CLAIM_ADMIN: &str = "admin";

impl Actor {
    /// Construct an actor for `user_id` with no claims and no tenant.
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            claims: Vec::new(),
            tenant_id: None,
        }
    }

    /// True iff the transport asserted the [`CLAIM_ADMIN`] claim. Convenience
    /// for plugins; the kernel must not call this to gate behavior.
    pub fn is_admin(&self) -> bool {
        self.claims.iter().any(|c| c == CLAIM_ADMIN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn actor_round_trips() {
        let actor = Actor {
            user_id: "user-123".into(),
            claims: vec![CLAIM_ADMIN.into()],
            tenant_id: Some("tenant-7".into()),
        };
        let v = serde_json::to_value(&actor).unwrap();
        assert_eq!(
            v,
            json!({
                "user_id": "user-123",
                "claims": ["admin"],
                "tenant_id": "tenant-7"
            })
        );
        let back: Actor = serde_json::from_value(v).unwrap();
        assert_eq!(actor, back);
    }

    #[test]
    fn empty_claims_and_tenant_are_omitted() {
        let actor = Actor::new("solo");
        let v = serde_json::to_value(&actor).unwrap();
        assert_eq!(v, json!({ "user_id": "solo" }));
        assert!(v.get("claims").is_none());
        assert!(v.get("tenant_id").is_none());
        let back: Actor = serde_json::from_value(v).unwrap();
        assert_eq!(actor, back);
    }

    #[test]
    fn deserializes_with_missing_optional_fields() {
        // An old peer that only knows user_id.
        let back: Actor = serde_json::from_value(json!({ "user_id": "u" })).unwrap();
        assert_eq!(back, Actor::new("u"));
        assert!(!back.is_admin());
    }

    #[test]
    fn is_admin_reflects_claim() {
        assert!(!Actor::new("u").is_admin());
        let admin = Actor {
            user_id: "u".into(),
            claims: vec![CLAIM_ADMIN.into()],
            tenant_id: None,
        };
        assert!(admin.is_admin());
    }
}
