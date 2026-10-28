// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::SandboxCatalog;
use crate::application::{Context, Output};
use crate::storage::sandbox_work;
use anyhow::Result;
use cellar_logic::work::{Installation, Reason};

pub(crate) fn publish(context: &Context) -> Result<()> {
    context.check_turn()?;
    if let Some(identity) = context.identity().filter(|_| context.facet().is_none()) {
        if let Some(at) = sandbox_work::publish(&identity.scope).filter(|at| *at >= 0) {
            crate::host_gates::spawn_arm_gate(
                &identity.scope,
                at,
                crate::host_io::installed_context(),
            );
        }
    }
    Ok(())
}

pub(crate) fn recover(context: &Context) -> Result<()> {
    if let Some(identity) = context.identity().filter(|_| context.facet().is_none()) {
        sandbox_work::refresh(&identity.scope)?;
        publish(context)?;
    }
    Ok(())
}

pub(crate) async fn maintain(context: Context, installation: Installation) -> Result<Output> {
    let storage = context.storage()?;
    if storage.work_installation(Reason::SandboxCleanup)? != Some(installation) {
        return Ok(Output::Done);
    }
    let scope = context
        .identity()
        .ok_or_else(|| anyhow::anyhow!("sandbox cleanup has no parent"))?
        .scope
        .clone();
    let next = sandbox_work::claim(&scope, crate::asyncrt::wall_ms().max(0))?;
    publish(&context)?;
    storage.sync().await?;
    if let Some(allocation) = next {
        // The claim's future retry is already durable before child/provider I/O.
        // A timeout or host loss leaves it discoverable by the next owner.
        let result = crate::asyncrt::timeout(std::time::Duration::from_secs(20), async {
            SandboxCatalog::open(&context)?
                .destroy_child(&context, &allocation)
                .await
        })
        .await;
        context.check_turn()?;
        sandbox_work::refresh(&scope)?;
        publish(&context)?;
        storage.sync().await?;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => anyhow::bail!("sandbox cleanup outcome is unknown; retry retained"),
        }
    }
    let catalog = SandboxCatalog::open(&context)?;
    if catalog
        .parent(&context)?
        .is_some_and(|parent| parent.state == super::SandboxParentState::Deleting)
        && catalog.cleanup(&context, None, 1)?.allocations.is_empty()
    {
        catalog.finish_retirement(&context).await?;
    }
    Ok(Output::Done)
}
