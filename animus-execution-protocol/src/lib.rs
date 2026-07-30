//! Generation-fenced execution identity shared by Animus queue, daemon,
//! environment, workflow-runner, and publication components.
//!
//! A workflow id by itself is not an ownership fence: after a crash or stale
//! lease, two processes can both believe they own it. [`ExecutionFence`] binds
//! the stable workflow id to monotonic workflow, subject, and queue-lease
//! generations plus the repository reservation that the execution owns.

#![warn(missing_docs)]

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Per-crate semver protocol version.
pub const PROTOCOL_VERSION: &str = "0.1.0";
/// Stable schema id for the first generation-fence contract.
pub const EXECUTION_FENCE_SCHEMA_ID: &str = "animus.execution-fence.v1";
/// Current generation-fence schema version.
pub const EXECUTION_FENCE_VERSION: u32 = 1;

/// A canonical subject identity and the immutable generation being executed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubjectGeneration {
    /// Canonical qualified id in `<kind>:<native-id>` form.
    pub qualified_id: String,
    /// Positive backend/queue generation. A later generation cannot be
    /// satisfied by work or proof from an earlier one.
    pub generation: u64,
}

impl SubjectGeneration {
    /// Validate the qualified identity and positive generation.
    pub fn validate(&self) -> Result<(), String> {
        validate_nonempty("subject qualified_id", &self.qualified_id)?;
        let Some((kind, native_id)) = self.qualified_id.split_once(':') else {
            return Err("subject qualified_id must use <kind>:<native-id> form".to_string());
        };
        validate_nonempty("subject kind", kind)?;
        validate_nonempty("subject native id", native_id)?;
        validate_positive("subject generation", self.generation)
    }
}

/// Repository/ref ownership reserved for one execution generation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RepositoryReservation {
    /// Canonical repository identity. Hosts SHOULD use a normalized remote URL
    /// or provider `owner/name` identity and compare it case-insensitively where
    /// the provider does.
    pub repository: String,
    /// Fully-qualified base ref, for example `refs/heads/main`.
    pub base_ref: String,
    /// Fully-qualified mutable head ref owned by this run, for example
    /// `refs/heads/animus/TASK-1175`.
    pub head_ref: String,
}

impl RepositoryReservation {
    /// Validate that the reservation is exact rather than an ambiguous short
    /// branch name.
    pub fn validate(&self) -> Result<(), String> {
        validate_nonempty("repository", &self.repository)?;
        validate_git_ref("base_ref", &self.base_ref)?;
        validate_git_ref("head_ref", &self.head_ref)?;
        if self.base_ref == self.head_ref {
            return Err("repository reservation base_ref and head_ref must differ".to_string());
        }
        Ok(())
    }

    /// Stable collision key for schedulers and durable stores.
    pub fn collision_key(&self) -> String {
        format!(
            "{}\n{}",
            self.repository.trim().to_ascii_lowercase(),
            self.head_ref
        )
    }
}

/// Compare-and-swap identity for the queue lease currently owning a workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueLeaseFence {
    /// Stable queue-entry id.
    pub entry_id: String,
    /// Daemon/scheduler instance currently authorized to renew or terminalize
    /// the lease.
    pub owner_id: String,
    /// Positive monotonic lease generation. Recovery transfers ownership by
    /// incrementing this value, fencing the previous daemon instance.
    pub generation: u64,
    /// Backend-clock expiry. Expiry makes the lease recoverable, never an
    /// ordinary fresh-leasing candidate.
    pub expires_at: DateTime<Utc>,
}

impl QueueLeaseFence {
    /// Validate the durable queue lease identity.
    pub fn validate(&self) -> Result<(), String> {
        validate_nonempty("queue lease entry_id", &self.entry_id)?;
        validate_nonempty("queue lease owner_id", &self.owner_id)?;
        validate_positive("queue lease generation", self.generation)
    }
}

/// Complete ownership envelope for one workflow execution generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionFence {
    /// Stable schema id; must equal [`EXECUTION_FENCE_SCHEMA_ID`].
    pub schema: String,
    /// Schema version; must equal [`EXECUTION_FENCE_VERSION`].
    pub version: u32,
    /// Stable workflow id. Recovery preserves this id.
    pub workflow_id: String,
    /// Positive workflow generation. Reattachment keeps it; an intentional new
    /// execution generation must increment it.
    pub workflow_generation: u64,
    /// Subject generation, absent only for a genuinely subjectless workflow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<SubjectGeneration>,
    /// Queue lease CAS identity. Direct execution may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_lease: Option<QueueLeaseFence>,
    /// Repository/ref reservation. Non-code workflows may omit it; coding
    /// schedulers must require it before workspace preparation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryReservation>,
}

impl ExecutionFence {
    /// Construct a direct (non-queue-backed) execution fence.
    pub fn direct(
        workflow_id: impl Into<String>,
        workflow_generation: u64,
        subject: Option<SubjectGeneration>,
    ) -> Self {
        Self {
            schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
            version: EXECUTION_FENCE_VERSION,
            workflow_id: workflow_id.into(),
            workflow_generation,
            subject,
            queue_lease: None,
            repository: None,
        }
    }

    /// Validate schema/version and every nested identity.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != EXECUTION_FENCE_SCHEMA_ID {
            return Err(format!(
                "unsupported execution fence schema '{}'; expected '{EXECUTION_FENCE_SCHEMA_ID}'",
                self.schema
            ));
        }
        if self.version != EXECUTION_FENCE_VERSION {
            return Err(format!(
                "unsupported execution fence version {}; expected {EXECUTION_FENCE_VERSION}",
                self.version
            ));
        }
        validate_nonempty("workflow_id", &self.workflow_id)?;
        validate_positive("workflow_generation", self.workflow_generation)?;
        if let Some(subject) = &self.subject {
            subject.validate()?;
        }
        if let Some(queue_lease) = &self.queue_lease {
            queue_lease.validate()?;
        }
        if let Some(repository) = &self.repository {
            repository.validate()?;
        }
        Ok(())
    }

    /// Validate the stricter queue-backed form.
    pub fn validate_queue_backed(&self) -> Result<(), String> {
        self.validate()?;
        if self.queue_lease.is_none() {
            return Err("queue-backed execution fence requires queue_lease".to_string());
        }
        Ok(())
    }

    /// Validate the stricter coding form before any workspace/node is prepared.
    pub fn validate_coding(&self) -> Result<(), String> {
        self.validate_queue_backed()?;
        if self.subject.is_none() {
            return Err("coding execution fence requires a subject generation".to_string());
        }
        if self.repository.is_none() {
            return Err("coding execution fence requires a repository reservation".to_string());
        }
        Ok(())
    }

    /// Assert that another envelope names the exact same immutable execution
    /// generation. Queue ownership/expiry may legitimately change on recovery,
    /// so this compares workflow and subject generations only.
    pub fn same_execution_generation(&self, other: &Self) -> bool {
        self.workflow_id == other.workflow_id
            && self.workflow_generation == other.workflow_generation
            && self.subject == other.subject
    }
}

fn validate_nonempty(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{field} must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_positive(field: &str, value: u64) -> Result<(), String> {
    if value == 0 {
        Err(format!("{field} must be greater than zero"))
    } else {
        Ok(())
    }
}

fn validate_git_ref(field: &str, value: &str) -> Result<(), String> {
    validate_nonempty(field, value)?;
    if !value.starts_with("refs/") || value.contains("..") || value.ends_with('/') {
        return Err(format!("{field} must be a safe fully-qualified git ref"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ExecutionFence {
        ExecutionFence {
            schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
            version: EXECUTION_FENCE_VERSION,
            workflow_id: "workflow-1".to_string(),
            workflow_generation: 3,
            subject: Some(SubjectGeneration {
                qualified_id: "task:TASK-1175".to_string(),
                generation: 9,
            }),
            queue_lease: Some(QueueLeaseFence {
                entry_id: "entry-1".to_string(),
                owner_id: "daemon-a".to_string(),
                generation: 2,
                expires_at: "2030-01-01T00:00:00Z".parse().unwrap(),
            }),
            repository: Some(RepositoryReservation {
                repository: "https://github.com/launchapp-dev/animus-cli.git".to_string(),
                base_ref: "refs/heads/main".to_string(),
                head_ref: "refs/heads/animus/TASK-1175".to_string(),
            }),
        }
    }

    #[test]
    fn coding_fence_round_trips_without_loss() {
        let fence = sample();
        fence.validate_coding().unwrap();
        let value = serde_json::to_value(&fence).unwrap();
        let decoded: ExecutionFence = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, fence);
    }

    #[test]
    fn rejects_zero_generations_and_ambiguous_refs() {
        let mut fence = sample();
        fence.workflow_generation = 0;
        assert!(fence
            .validate()
            .unwrap_err()
            .contains("workflow_generation"));

        let mut fence = sample();
        fence.repository.as_mut().unwrap().head_ref = "main".to_string();
        assert!(fence.validate().unwrap_err().contains("fully-qualified"));
    }

    #[test]
    fn lease_transfer_does_not_change_execution_generation() {
        let first = sample();
        let mut recovered = first.clone();
        let lease = recovered.queue_lease.as_mut().unwrap();
        lease.owner_id = "daemon-b".to_string();
        lease.generation += 1;
        assert!(first.same_execution_generation(&recovered));
        assert_ne!(first.queue_lease, recovered.queue_lease);
    }

    #[test]
    fn repository_collision_key_is_case_stable() {
        let reservation = sample().repository.unwrap();
        let mut upper = reservation.clone();
        upper.repository = upper.repository.to_ascii_uppercase();
        assert_eq!(reservation.collision_key(), upper.collision_key());
    }
}
