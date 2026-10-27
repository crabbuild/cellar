// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-only durable provider metadata. Guest storage cannot authorize a remote ID.
use super::{containers::ProviderAuthority, Context, HandlerFuture};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

pub const MAX_BINDINGS: usize = 64;
pub const MAX_BINDING_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingIntent {
    Running,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingObservation {
    Pending,
    Confirmed,
}

/// Credential-free backend identity and lifecycle intent. Pending includes lost
/// responses; it must be reconciled, never treated as permission to replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingRecord {
    pub provider: String,
    pub configuration_digest: String,
    pub parameters_digest: String,
    pub generation: u64,
    pub request_id: String,
    pub resource_id: Option<String>,
    pub intent: BindingIntent,
    pub observation: BindingObservation,
}

impl BindingRecord {
    /// Durable retirement evidence permits a fresh generation under a new host
    /// configuration. A pending delete or elapsed local timer does not.
    pub fn is_retired(&self) -> bool {
        self.intent == BindingIntent::Deleted && self.observation == BindingObservation::Confirmed
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let hex = |value: &str, length| {
            value.len() == length
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        ensure!(
            !self.provider.is_empty()
                && self.provider.len() <= 128
                && self
                    .provider
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
            "invalid binding provider"
        );
        ensure!(
            hex(&self.configuration_digest, 64)
                && hex(&self.parameters_digest, 64)
                && hex(&self.request_id, 32),
            "invalid binding digest or request identity"
        );
        ensure!(
            (1..=i64::MAX as u64).contains(&self.generation),
            "invalid binding generation"
        );
        ensure!(
            self.resource_id
                .as_ref()
                .is_none_or(|id| !id.is_empty() && id.len() <= 4096 && !id.contains('\0')),
            "invalid provider resource ID"
        );
        ensure!(
            self.intent != BindingIntent::Running
                || self.observation != BindingObservation::Confirmed
                || self.resource_id.is_some(),
            "a confirmed running binding requires a resource ID"
        );
        Ok(())
    }

    pub(crate) fn follows(&self, previous: Option<&Self>) -> Result<()> {
        self.validate()?;
        let initial = self.intent == BindingIntent::Running
            && self.observation == BindingObservation::Pending
            && self.resource_id.is_none();
        let Some(previous) = previous else {
            ensure!(
                initial && self.generation == 1,
                "a binding must begin with its first pending create"
            );
            return Ok(());
        };
        let retired = previous.is_retired();
        if self.generation != previous.generation {
            ensure!(
                retired
                    && previous.generation.checked_add(1) == Some(self.generation)
                    && initial
                    && self.request_id != previous.request_id,
                "new binding generations require confirmed retirement and a fresh request identity"
            );
            return Ok(());
        }
        ensure!(
            self.provider == previous.provider
                && self.configuration_digest == previous.configuration_digest
                && self.parameters_digest == previous.parameters_digest
                && self.request_id == previous.request_id,
            "binding configuration or request identity changed within a generation"
        );
        ensure!(
            previous
                .resource_id
                .as_ref()
                .is_none_or(|id| self.resource_id.as_ref() == Some(id)),
            "a binding cannot replace or forget a known resource"
        );
        ensure!(
            !retired || self == previous,
            "a retired binding cannot be reopened in its old generation"
        );
        ensure!(
            previous.intent != BindingIntent::Deleted || self.intent == BindingIntent::Deleted,
            "deletion intent cannot be reversed"
        );
        ensure!(
            previous.observation != BindingObservation::Confirmed
                || self.observation == BindingObservation::Confirmed
                || (previous.intent == BindingIntent::Running
                    && self.intent == BindingIntent::Deleted),
            "confirmed work cannot become pending again"
        );
        Ok(())
    }
}

/// Activation epoch records provenance only. It is not a globally monotonic
/// ownership token and cannot replace fencing enforced by the remote service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindingCheckpoint {
    pub revision: u64,
    pub activation_epoch: u64,
    pub record: BindingRecord,
}

/// One host-configured binding, bound to one original root-cell invocation.
/// Records survive events, deleteAll and activation recovery; this handle does not.
#[derive(Clone)]
pub struct BindingJournal {
    name: Arc<str>,
    authority: Arc<ProviderAuthority>,
}

/// Serializes lifecycle transitions for one binding in the current activation.
/// Dropping releases the slot. This supplies no remote fencing or rollback.
#[must_use = "keep the guard until lifecycle I/O and checkpointing have settled"]
pub struct BindingLifecycleGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

#[derive(Default)]
pub(crate) struct LifecycleLocks {
    locks: Mutex<HashMap<Arc<str>, Weak<tokio::sync::Mutex<()>>>>,
}

impl LifecycleLocks {
    fn get(&self, name: &Arc<str>) -> Result<Arc<tokio::sync::Mutex<()>>> {
        let mut locks = self.locks.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(name).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        ensure!(
            locks.len() < MAX_BINDINGS,
            "provider lifecycle limit exceeded"
        );
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(name.clone(), Arc::downgrade(&lock));
        Ok(lock)
    }
}

impl BindingJournal {
    pub fn open(context: &Context, name: &str) -> Result<Self> {
        crate::storage::container_bindings::validate_name(name)?;
        let journal = Self {
            name: name.into(),
            authority: Arc::new(context.container_provider_authority()?),
        };
        journal.check(context)?;
        Ok(journal)
    }

    fn check<'a>(&self, context: &'a Context) -> Result<&'a str> {
        self.authority.check(context)?;
        let storage = context.storage()?;
        ensure!(
            storage.0.resources.transaction.lock().unwrap().is_none(),
            "provider journals cannot be used inside storage callbacks"
        );
        Ok(&context.identity().unwrap().scope)
    }

    pub fn load(&self, context: &Context) -> Result<Option<BindingCheckpoint>> {
        let scope = self.check(context)?;
        crate::storage::container_bindings::load(scope, &self.name)
    }

    /// Wait for exclusive lifecycle admission across event sessions and native
    /// maintenance for this binding. Check authority again after waiting, then
    /// load the checkpoint under the guard before deciding or issuing effects.
    /// Command/file work stays concurrent and must check its original generation.
    /// Acquisition is cancellable; do not reacquire while holding this guard.
    pub fn lock_lifecycle(&self, context: &Context) -> HandlerFuture<BindingLifecycleGuard> {
        let prepared = (|| {
            self.check(context)?;
            context
                .storage()?
                .0
                .resources
                .provider_lifecycle
                .get(&self.name)
        })();
        let journal = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let lock = prepared?;
            journal.check(&context)?;
            let guard = lock.lock_owned().await;
            journal.check(&context)?;
            Ok(BindingLifecycleGuard { _guard: guard })
        })
    }

    fn check_running_generation(&self, context: &Context, generation: u64) -> Result<()> {
        let checkpoint = self.load(context)?;
        ensure!(
            checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.record.generation == generation
                    && checkpoint.record.intent == BindingIntent::Running
                    && checkpoint.record.observation == BindingObservation::Confirmed
            }),
            "provider binding changed during operation; outcome may be unknown"
        );
        Ok(())
    }

    /// A successful application interaction may extend idle retention, never
    /// the original maximum lifetime. Provider polling must not call this.
    /// Publication requires the same confirmed running generation before and
    /// after durability. A failure after provider work does not prove rollback.
    pub fn activity(&self, context: &Context, generation: u64) -> HandlerFuture<()> {
        let prepared = (|| {
            self.check_running_generation(context, generation)?;
            let scope = self.check(context)?;
            let changed = crate::storage::container_maintenance::activity(
                scope,
                &self.name,
                generation,
                crate::asyncrt::wall_ms().max(0),
            )?;
            if changed {
                super::container_supervision::controller::publish(context)?;
            }
            changed
                .then(|| context.storage().map(|storage| storage.sync()))
                .transpose()
        })();
        let journal = self.clone();
        let context = context.clone();
        Box::pin(async move {
            if let Some(flush) = prepared? {
                flush.await?;
            }
            journal.check_running_generation(&context, generation)?;
            Ok(())
        })
    }

    /// Refuse new application work after a supervised retention deadline.
    /// Reconciliation and explicit destruction remain available.
    pub fn check_retention(&self, context: &Context) -> Result<()> {
        let scope = self.check(context)?;
        if let Some(task) = crate::storage::container_maintenance::list(scope)?
            .into_iter()
            .find(|task| task.binding == self.name.as_ref())
        {
            if task.deadline()? <= crate::asyncrt::wall_ms().max(task.logical_ms) {
                return Err(super::container_supervision::RetentionExpired.into());
            }
        }
        Ok(())
    }

    /// Atomically replace the expected revision and await its required durability
    /// proof before returning. An error after the write can leave a retained
    /// pending intent. A successful return does not grant remote fencing.
    pub fn compare_exchange(
        &self,
        context: &Context,
        expected: Option<u64>,
        record: BindingRecord,
    ) -> HandlerFuture<BindingCheckpoint> {
        let prepared = (|| {
            let scope = self.check(context)?;
            let checkpoint = crate::storage::container_bindings::compare_exchange(
                scope,
                &self.name,
                expected,
                context.identity().unwrap().epoch,
                record.clone(),
                context
                    .container_supervision
                    .as_ref()
                    .and_then(|registry| registry.find(&self.name, &record))
                    .map(|registration| registration.policy),
                crate::asyncrt::wall_ms().max(0),
            )?;
            super::container_supervision::controller::publish(context)?;
            Ok::<_, anyhow::Error>((checkpoint, context.storage()?.sync()))
        })();
        let journal = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let (checkpoint, flush) = prepared?;
            flush.await?;
            let current = journal.load(&context)?;
            ensure!(
                current.as_ref() == Some(&checkpoint),
                "binding revision changed while awaiting durability"
            );
            Ok(checkpoint)
        })
    }
}
