// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

/// A parent closes admission before retiring its children. Retirement is final.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxParentState {
    Active,
    Deleting,
    Deleted,
}

/// Durable parent identity. An activation epoch is deliberately not its identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxParent {
    pub scope: String,
    pub incarnation: String,
    pub revision: u64,
    pub state: SandboxParentState,
}

/// Reserved includes a lost creation response; it never permits blind replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxAllocationState {
    Reserved,
    Ready,
    Deleting,
    Deleted,
}

/// One immutable execution-cell identity per named generation. This is metadata,
/// not a callable child capability or evidence that a container is running.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxAllocation {
    pub parent_incarnation: String,
    pub name: String,
    pub binding: String,
    pub parameters_digest: String,
    pub generation: u64,
    pub revision: u64,
    pub execution_name: String,
    pub execution_scope: String,
    pub state: SandboxAllocationState,
}

/// Only the first durable reservation reports `fresh`. Later callers must
/// reconcile that exact child, including when the first reply was lost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxReservation {
    pub allocation: SandboxAllocation,
    pub fresh: bool,
}

/// Bounded keyset page. Restart cleanup scans at `None` after reaching the end:
/// concurrent transitions can make earlier names eligible for cleanup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxAllocationPage {
    pub allocations: Vec<SandboxAllocation>,
    pub next: Option<String>,
}

pub(crate) fn name(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid sandbox name or binding"
    );
    Ok(())
}

pub(crate) fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn page(after: Option<&str>, limit: usize) -> Result<()> {
    ensure!(
        (1..=256).contains(&limit),
        "sandbox page limit must be 1..=256"
    );
    if let Some(after) = after {
        name(after)?;
    }
    Ok(())
}

impl SandboxParent {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            cellar_logic::cell::valid_cell_scope(&self.scope) && self.scope.contains(':'),
            "invalid sandbox parent scope"
        );
        ensure!(
            hex(&self.incarnation, 32) && (1..=i64::MAX as u64).contains(&self.revision),
            "invalid sandbox parent identity"
        );
        Ok(())
    }
}

impl SandboxAllocation {
    pub(crate) fn validate(&self) -> Result<()> {
        name(&self.name)?;
        name(&self.binding)?;
        ensure!(
            hex(&self.parent_incarnation, 32) && hex(&self.parameters_digest, 64),
            "invalid sandbox allocation digest"
        );
        ensure!(
            (1..=i64::MAX as u64).contains(&self.generation)
                && (1..=i64::MAX as u64).contains(&self.revision),
            "invalid sandbox allocation revision"
        );
        ensure!(
            self.execution_name
                .strip_prefix("sandbox-")
                .is_some_and(|digest| hex(digest, 64)),
            "invalid sandbox execution name"
        );
        ensure!(
            cellar_logic::cell::valid_cell_scope(&self.execution_scope)
                && self.execution_scope.contains(':'),
            "invalid sandbox execution scope"
        );
        Ok(())
    }
}
