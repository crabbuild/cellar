// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private parent provenance and the bounded native execution class.
use crate::application::{consensus::*, Cell, Context, HandlerFuture, Invocation, Output};
use crate::runtime::sandbox_execution;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{atomic::Ordering, Arc};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    cluster: [u8; 16],
    namespace: [u8; 16],
    incarnation: [u8; 16],
    cell: String,
}

impl Source {
    fn from_history(history: &CellHistory) -> Self {
        Self {
            cluster: history.cluster.bytes(),
            namespace: history.namespace.bytes(),
            incarnation: history.incarnation.bytes(),
            cell: history.cell.as_str().into(),
        }
    }

    pub(crate) fn matches(&self, history: &CellHistory) -> bool {
        self.cluster == history.cluster.bytes()
            && self.namespace == history.namespace.bytes()
            && self.incarnation == history.incarnation.bytes()
            && self.cell == history.cell.as_str()
    }

    fn digest(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(serde_json::to_vec(self)?).into())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Authority {
    pub(crate) source: Source,
    pub(crate) execution: sandbox_execution::Authority,
}

impl Authority {
    pub(crate) fn new(
        history: &CellHistory,
        execution: sandbox_execution::Authority,
    ) -> Result<Self> {
        ensure!(
            execution.parent_scope() == history.cell.as_str()
                && !execution.requires_native_capacity(),
            "sandbox parent/source differs or requires Fleet placement"
        );
        Ok(Self {
            source: Source::from_history(history),
            execution,
        })
    }

    pub(crate) fn target_name(&self, scope: &str, method: &str) -> Result<String> {
        ensure!(
            self.source.cell == self.execution.parent_scope(),
            "sandbox parent provenance differs"
        );
        self.execution.validate_target(scope, method)?;
        self.execution.execution_name()
    }

    pub(crate) fn configuration(&self) -> &str {
        self.execution.configuration()
    }

    pub(crate) fn parent_digest(&self) -> Result<[u8; 32]> {
        self.source.digest()
    }
}

pub(crate) fn history(context: &Context) -> Result<CellHistory> {
    context.check_binding_turn()?;
    let history = crate::storage::capture_raft_dispatch()?
        .ok_or(CommandFailure::UnsupportedCapability)?
        .source_history()?;
    ensure!(
        context
            .identity()
            .is_some_and(|id| id.scope == history.cell.as_str()),
        "sandbox Context/history differs"
    );
    Ok(history)
}

pub(crate) fn parent_history(context: &Context) -> Result<Option<CellHistory>> {
    let Some(storage) = &context.raft_storage else {
        return Ok(None);
    };
    ensure!(
        storage.resources.sandbox_catalog.load(Ordering::Acquire),
        CommandFailure::UnsupportedCapability
    );
    Ok(Some(history(context)?))
}

/// Validate history and authenticated parent before Container entry can perform
/// even a physical observation or ownership transition.
pub(crate) fn prepare_entry(context: &Context) -> Result<()> {
    ensure!(
        context
            .raft_storage
            .as_ref()
            .is_some_and(|s| s.resources.sandbox_execution.load(Ordering::Acquire)),
        CommandFailure::UnsupportedCapability
    );
    let authority = context
        .sandbox_authority
        .as_ref()
        .ok_or(CommandFailure::UnsupportedCapability)?;
    let current = history(context)?;
    let name = authority.target_name(current.cell.as_str(), sandbox_execution::METHOD)?;
    ensure!(
        context.identity().and_then(|id| id.name.as_deref()) == Some(name.as_str()),
        "sandbox private routing name differs"
    );
    crate::storage::sandbox_shared::attach_child(&current, authority.parent_digest()?)
}

pub(crate) struct ExecutionCell;

impl Cell for ExecutionCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            ensure!(
                context
                    .raft_storage
                    .as_ref()
                    .is_some_and(|s| s.resources.sandbox_execution.load(Ordering::Acquire)),
                CommandFailure::UnsupportedCapability
            );
            let authority = context
                .sandbox_authority
                .clone()
                .ok_or(CommandFailure::UnsupportedCapability)?;
            let current = history(&context)?;
            let name = authority.target_name(current.cell.as_str(), sandbox_execution::METHOD)?;
            ensure!(
                context.identity().and_then(|id| id.name.as_deref()) == Some(name.as_str()),
                "sandbox private routing name differs"
            );
            crate::storage::sandbox_shared::child(&current, authority.parent_digest()?)?;
            sandbox_execution::invoke(context, invocation, authority.execution).await
        })
    }
}
