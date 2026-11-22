// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::SandboxCatalog;
use crate::application::{Context, Output};
use crate::storage::sandbox_work;
use anyhow::Result;
use cellar_logic::work::{Installation, Reason};

pub(crate) fn publish(context: &Context) -> Result<()> {
    context.check_turn()?;
    if context.raft_storage.is_some() {
        // The private image already arms shared discovery atomically with its
        // allocation changes. A Fleet host gate cannot own this cell's work.
        super::shared::parent_history(context)?;
        return Ok(());
    }
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
    if let Some(history) = super::shared::parent_history(context)? {
        return crate::storage::sandbox_shared::refresh(&history);
    }
    if let Some(identity) = context.identity().filter(|_| context.facet().is_none()) {
        if crate::storage::sandbox_erasure::load(&identity.scope)?.is_some() {
            if crate::storage::work::installation(&identity.scope, Reason::SandboxCleanup)?
                .is_none()
            {
                crate::storage::work::set(&identity.scope, Reason::SandboxCleanup, 0)?;
            }
            return publish(context);
        }
        if context.worker_config.source_recovery {
            if let Some(parent) = crate::storage::sandbox_catalog::parent(&identity.scope)? {
                if parent.state == super::SandboxParentState::Active {
                    crate::storage::sandbox_catalog::retire(
                        &identity.scope,
                        &parent.incarnation,
                        false,
                    )?;
                }
            }
        }
        sandbox_work::refresh(&identity.scope)?;
        publish(context)?;
    }
    Ok(())
}

pub(crate) async fn maintain(context: Context, installation: Installation) -> Result<Output> {
    if context.raft_storage.is_some() {
        return maintain_shared(context, installation).await;
    }
    let storage = context.storage()?;
    if storage.work_installation(Reason::SandboxCleanup)? != Some(installation) {
        return Ok(Output::Done);
    }
    let scope = context
        .identity()
        .ok_or_else(|| anyhow::anyhow!("sandbox cleanup has no parent"))?
        .scope
        .clone();
    if let Some(record) = crate::storage::sandbox_erasure::load(&scope)? {
        let owner = storage.native_activation()?;
        owner.close_for_erasure();
        storage.sync().await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let activation = std::sync::Arc::downgrade(owner);
        crate::host_channels::FACET_TX
            .get()
            .ok_or_else(|| anyhow::anyhow!("erasure requires a node host"))?
            .send(crate::host_channels::FacetReq::Erase {
                request: crate::host_channels::CellErasureRequest {
                    record,
                    epoch: owner.epoch,
                    validate_admission: Box::new(move |scope, epoch| {
                        let activation = activation
                            .upgrade()
                            .ok_or_else(|| anyhow::anyhow!("erasure activation has retired"))?;
                        anyhow::ensure!(
                            activation.scope == scope
                                && activation.epoch == epoch
                                && activation
                                    .resources
                                    .erasure_closed
                                    .load(std::sync::atomic::Ordering::Acquire)
                                && !activation
                                    .resources
                                    .retired
                                    .load(std::sync::atomic::Ordering::Acquire),
                            "erasure requires the original activation's closed business admission"
                        );
                        Ok(())
                    }),
                },
                reply,
            })
            .map_err(|_| anyhow::anyhow!("erasure host channel closed"))?;
        receive.await??;
        storage.delete_alarm()?;
        storage.clear_work(installation)?;
        storage.sync().await?;
        return Ok(Output::Done);
    }
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

async fn maintain_shared(context: Context, installation: Installation) -> Result<Output> {
    let history = super::shared::parent_history(&context)?
        .ok_or(crate::application::consensus::CommandFailure::UnsupportedCapability)?;
    let storage = context.storage()?;
    if storage.work_installation(Reason::SandboxCleanup)? != Some(installation) {
        return Ok(Output::Done);
    }
    let next = crate::storage::sandbox_shared::claim(&history, crate::asyncrt::wall_ms().max(0))?;
    // destroy_child proves the retry claim together with exact deletion intent
    // before crossing into the child. Losing the call retains discovery.
    let catalog = SandboxCatalog::open(&context)?;
    if let Some(allocation) = next {
        let result = catalog.destroy_child(&context, &allocation).await;
        context.check_turn()?;
        crate::storage::sandbox_shared::refresh(&history)?;
        // The work result proves refresh. Parent retirement is a separate
        // bounded turn, already discoverable after the last child settles.
        return result.map(|_| Output::Done);
    }
    if catalog
        .parent(&context)?
        .is_some_and(|parent| parent.state == super::SandboxParentState::Deleting)
        && catalog.cleanup(&context, None, 1)?.allocations.is_empty()
    {
        catalog.finish_retirement(&context).await?;
    }
    if let Some(parent) = catalog
        .parent(&context)?
        .filter(|p| p.state == super::SandboxParentState::Deleted)
    {
        // Reuse an unchanged proof; dirty/newly observed metadata must pass the
        // mandatory gate. Another parked callback cannot lend us its authority.
        // An expired/failed proof leaves exact confirmation work installed.
        storage.confirm_checkpoint().await?;
        ensure_terminal(&catalog, &context, &parent)?;
        crate::storage::sandbox_shared::confirm_retirement(&history, &parent)?;
    }
    Ok(Output::Done)
}

fn ensure_terminal(
    catalog: &SandboxCatalog,
    context: &Context,
    parent: &super::SandboxParent,
) -> Result<()> {
    anyhow::ensure!(
        catalog.parent(context)?.as_ref() == Some(parent),
        "sandbox terminal revision changed while proving cleanup"
    );
    Ok(())
}
