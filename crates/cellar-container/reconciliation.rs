// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, credential-free lifecycle observations shared by provider facades.
use serde::{Deserialize, Serialize};

/// Evidence observed now, rather than a promise about future remote availability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationState {
    Unbound,
    Running,
    Stopped,
    Deleting,
    Deleted,
}

/// Reconcile one configured binding without creating, resuming or deleting
/// provider resources. Unresolved or ambiguous creation evidence returns an
/// error and retains the reservation. A remote generation is a decimal string;
/// the native engine has no remote journal and returns `None`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconciliation {
    pub state: ReconciliationState,
    pub generation: Option<String>,
}

impl Reconciliation {
    pub(crate) fn value(state: ReconciliationState, generation: Option<u64>) -> serde_json::Value {
        serde_json::json!({"state":state,"generation":generation.map(|value| value.to_string())})
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if let Some(generation) = &self.generation {
            let value = generation.parse::<u64>()?;
            anyhow::ensure!(
                value > 0 && value.to_string() == *generation,
                "invalid reconciliation generation"
            );
        }
        anyhow::ensure!(
            self.state != ReconciliationState::Unbound || self.generation.is_none(),
            "unbound reconciliation has a generation"
        );
        Ok(())
    }
}
