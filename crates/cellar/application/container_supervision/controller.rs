// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::registry::Backend;
use super::{DeleteSafety, MaintenanceAction, MaintenancePreparation};
use crate::application::{
    containers::{BindingIntent, BindingJournal, BindingObservation},
    Context, Output,
};
use crate::storage::{container_bindings as bindings, container_maintenance as tasks};
use anyhow::{ensure, Result};
use cellar_logic::work::{Installation, Reason};
use std::time::Duration;

fn scope(context: &Context) -> Result<&str> {
    context.container_provider_authority()?.check(context)?;
    Ok(if context.raft_storage.is_some() {
        crate::storage::RAFT_STORAGE_SCOPE
    } else {
        &context.identity().unwrap().scope
    })
}

pub(crate) fn publish(context: &Context) -> Result<()> {
    let scope = scope(context)?;
    if context.raft_storage.is_some() {
        // Task changes already project the shared work ledger in the same SQL
        // transaction. No Fleet alarm or node-local callback may stand in for it.
        return Ok(());
    }
    if let Some(at) = tasks::publish(scope).filter(|at| *at >= 0) {
        crate::host_gates::spawn_arm_gate(scope, at, crate::host_io::installed_context());
    }
    Ok(())
}

/// Reattach existing obligations without granting unknown old resources a new
/// lifetime. This runs in a native turn before business construction/dispatch.
pub(crate) fn recover(context: &Context) -> Result<()> {
    let Some(registry) = context.container_supervision.as_ref() else {
        return Ok(());
    };
    let Some(_) = context.identity().filter(|_| context.facet().is_none()) else {
        return Ok(());
    };
    let scope = scope(context)?;
    for (binding, checkpoint) in bindings::list(scope)? {
        if checkpoint.record.is_retired() {
            continue;
        }
        if let Some(registration) = registry.find(&binding, &checkpoint.record) {
            tasks::recover(
                scope,
                &binding,
                &checkpoint.record,
                registration.policy,
                crate::asyncrt::wall_ms().max(0),
            )?;
        }
    }
    publish(context)
}

pub(crate) async fn maintain(context: Context, installation: Installation) -> Result<Output> {
    let db = context.storage()?;
    if db.work_installation(Reason::ResourceReconciliation)? != Some(installation) {
        return Ok(Output::Done);
    }
    let scope = scope(&context)?.to_owned();
    let now = crate::asyncrt::wall_ms().max(0);
    let Some(task) = tasks::list(&scope)?
        .into_iter()
        .find(|task| task.due_ms <= now)
    else {
        // Existing tasks may have advanced while this alarm was in flight.
        // Never consume a replacement installation or the application's alarm.
        if tasks::list(&scope)?.is_empty() {
            db.clear_work(installation)?;
        } else {
            tasks::refresh(&scope)?;
            publish(&context)?;
            db.sync().await?;
        }
        return Ok(Output::Done);
    };
    let journal = BindingJournal::open(&context, &task.binding)?;
    // A parked foreground command can still own this binding's lifecycle.
    // Shared-group maintenance has not proved its private framework work claim
    // yet: refuse without checkpointing or waiting so that the obligation is
    // retried from committed state. Foreground calls retain their cancellable
    // wait, and ordinary Fleet maintenance keeps its existing serialization.
    let _lifecycle = if context.raft_storage.is_some() {
        journal
            .try_lock_lifecycle(&context)?
            .ok_or(crate::application::consensus::CommandFailure::Unavailable)?
    } else {
        journal.lock_lifecycle(&context).await?
    };
    if db.work_installation(Reason::ResourceReconciliation)? != Some(installation) {
        return Ok(Output::Done);
    }
    let now = crate::asyncrt::wall_ms().max(0);
    if !tasks::list(&scope)?
        .iter()
        .any(|current| current == &task && current.due_ms <= now)
    {
        return Ok(Output::Done);
    }
    let expected = journal
        .load(&context)?
        .ok_or_else(|| anyhow::anyhow!("maintenance binding missing"))?;
    ensure!(
        expected.record.generation == task.generation && !expected.record.is_retired(),
        "maintenance generation does not match binding"
    );
    let Some(claim) = tasks::claim(&scope, &task, now)? else {
        return Ok(Output::Done);
    };
    publish(&context)?;
    // Shared-group effect admission proves this reservation together with the
    // provider fence before any controller/backend I/O. A cancellation before
    // that proof leaves the previous committed obligation discoverable. After
    // it, the reserved next attempt survives cancellation and host loss.
    if context.raft_storage.is_none() {
        db.sync().await?;
    }
    let registration = context
        .container_supervision
        .as_ref()
        .and_then(|registry| registry.find(&task.binding, &expected.record))
        .cloned();
    let result = async {
        let registration = registration
            .ok_or_else(|| anyhow::anyhow!("original maintenance backend is not registered"))?;
        let deleting = expected.record.intent == BindingIntent::Deleted;
        let expired = claim.deadline()? <= claim.logical_ms;
        let submitted_delete =
            deleting && expected.record.observation == BindingObservation::Pending;
        let action = if (deleting || expired)
            && (!submitted_delete
                || registration.backend.delete_safety() == DeleteSafety::RevalidateExactResource)
        {
            MaintenanceAction::Delete
        } else {
            MaintenanceAction::Inspect
        };
        ensure!(
            journal.load(&context)?.as_ref() == Some(&expected),
            "maintenance checkpoint changed before dispatch"
        );
        let future: crate::application::HandlerFuture<()> = match registration.backend {
            Backend::Direct(backend) => {
                context
                    .container_provider_authority()?
                    .before_effect(&context)
                    .await?;
                ensure!(
                    journal.load(&context)?.as_ref() == Some(&expected),
                    "maintenance checkpoint changed at effect admission"
                );
                backend.maintain(context.clone(), expected, action)
            }
            Backend::Fenced { backend, authority } => {
                let expected = match backend.prepare(&expected, action)? {
                    MaintenancePreparation::Unchanged => expected,
                    MaintenancePreparation::DeleteIntent => {
                        // Validate before mutating private state. A preparation
                        // decision cannot publish a result or change resources.
                        ensure!(
                            action == MaintenanceAction::Delete
                                && expected.record.intent == BindingIntent::Running
                                && expected.record.resource_id.is_some()
                                && !expected.record.is_retired(),
                            "invalid maintenance deletion preparation"
                        );
                        let mut record = expected.record;
                        record.intent = BindingIntent::Deleted;
                        record.observation = BindingObservation::Pending;
                        journal.prepare_exchange(&context, Some(expected.revision), record)?
                    }
                };
                let lifecycle = _lifecycle.retained();
                let journal = journal.clone();
                let context = context.clone();
                Box::pin(async move {
                    let grant = journal
                        .fence_execution(&context, lifecycle, authority)
                        .await?;
                    ensure!(
                        grant.claim().checkpoint() == &expected,
                        "maintenance checkpoint changed during execution fencing"
                    );
                    grant.before_effect(&context).await?;
                    backend.maintain(context, expected, action, grant).await
                })
            }
        };
        crate::asyncrt::timeout(
            Duration::from_millis(claim.policy.attempt_timeout_ms),
            future,
        )
        .await
        .map_err(|_| anyhow::anyhow!("provider maintenance outcome is unknown"))?
    }
    .await;
    context.container_provider_authority()?.check(&context)?;
    tasks::finish(&scope, &claim, result.is_ok())?;
    publish(&context)?;
    // The shared system turn proves task settlement and its own completion in
    // one final output. No further I/O can observe this private settlement.
    if context.raft_storage.is_none() {
        db.confirm_checkpoint().await?;
    }
    if result.is_err() {
        // Provider exception text can contain credentials; persist only bounded
        // failure accounting. The exact obligation remains in protected state.
        tracing::warn!(
            event = "container_maintenance_pending",
            scope = context.identity().map(|identity| identity.scope.as_str()),
            binding = task.binding,
            "provider maintenance remains unresolved"
        );
    }
    Ok(Output::Done)
}
