// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{DeleteSafety, MaintenanceAction};
use crate::application::{
    containers::{BindingIntent, BindingJournal, BindingObservation},
    Context, Output,
};
use crate::storage::{container_bindings as bindings, container_maintenance as tasks};
use anyhow::{ensure, Result};
use cellar_logic::work::{Installation, Reason};
use std::time::Duration;

pub(crate) fn publish(context: &Context) -> Result<()> {
    context.check_turn()?;
    let scope = &context
        .identity()
        .ok_or_else(|| anyhow::anyhow!("provider work requires a root cell"))?
        .scope;
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
    let Some(identity) = context.identity().filter(|_| context.facet().is_none()) else {
        return Ok(());
    };
    context.check_turn()?;
    for (binding, checkpoint) in bindings::list(&identity.scope)? {
        if checkpoint.record.is_retired() {
            continue;
        }
        if let Some(registration) = registry.find(&binding, &checkpoint.record) {
            tasks::recover(
                &identity.scope,
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
    let scope = context
        .identity()
        .ok_or_else(|| anyhow::anyhow!("maintenance requires root identity"))?
        .scope
        .clone();
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
    // Reserve the next attempt durably before I/O. Cancellation or host loss
    // then leaves discoverable work instead of relying on a finally callback.
    db.sync().await?;
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
        context
            .container_provider_authority()?
            .before_effect(&context)
            .await?;
        ensure!(
            journal.load(&context)?.as_ref() == Some(&expected),
            "maintenance checkpoint changed at effect admission"
        );
        crate::asyncrt::timeout(
            Duration::from_millis(claim.policy.attempt_timeout_ms),
            registration
                .backend
                .maintain(context.clone(), expected, action),
        )
        .await
        .map_err(|_| anyhow::anyhow!("provider maintenance outcome is unknown"))?
    }
    .await;
    context.check_turn()?;
    tasks::finish(&scope, &claim, result.is_ok())?;
    publish(&context)?;
    db.sync().await?;
    if result.is_err() {
        // Provider exception text can contain credentials; persist only bounded
        // failure accounting. The exact obligation remains in protected state.
        tracing::warn!(
            event = "container_maintenance_pending",
            scope,
            binding = task.binding,
            "provider maintenance remains unresolved"
        );
    }
    Ok(Output::Done)
}
