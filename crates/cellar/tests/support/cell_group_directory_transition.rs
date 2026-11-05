// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

async fn split_plan(f: &Fixture) -> Result<(DirectoryMutation, Vec<GroupRoute>, GroupRoute)> {
    let mut parent = assigned(1000, CellRange::all(), 1);
    f.reserve(std::slice::from_mut(&mut parent)).await?;
    let d = &f.directory;
    let initialize = d.prepare_initialization(operation(100), std::slice::from_ref(&parent))?;
    f.run(d.publish(&initialize)).await?;
    let initial = f.run(d.observe()).await?;
    let mut children = split(&parent, 2000);
    f.reserve(&mut children).await?;
    let plan = d.prepare_replacement(
        &initial,
        operation(101),
        std::slice::from_ref(&parent),
        &children,
    )?;
    Ok((plan, children, parent))
}

pub(crate) async fn seal_authorization_cancel_race() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let d = &f.directory;
    let (plan, children, parent) = split_plan(&f).await?;
    let planned = f.run(d.plan_transition(&plan)).await?;
    anyhow::ensure!(planned.phase() == TransitionPhase::Planned);
    anyhow::ensure!(matches!(
        f.run(d.authorize_transition_seal(&plan)).await,
        Err(CatalogError::Conflict(_))
    ));
    let view = f.run(d.observe()).await?;
    anyhow::ensure!(view.groups() == 1 && view.revision() == 1);
    let foreign = d.prepare_replacement(
        &view,
        operation(102),
        std::slice::from_ref(&parent),
        &children,
    )?;
    anyhow::ensure!(matches!(
        f.run(d.publish(&foreign)).await,
        Err(CatalogError::Fenced)
    ));
    anyhow::ensure!(matches!(
        f.run(d.freeze_group_births(operation(103))).await,
        Err(CatalogError::Fenced)
    ));
    let prepared = f.run(d.prepare_transition(&plan, [72; 32])).await?;
    anyhow::ensure!(
        prepared.phase() == TransitionPhase::Prepared && prepared.preparation() == Some([72; 32])
    );
    let reopened = f.reopen().await?;
    let (request, retained) = f
        .run(reopened.active_transition())
        .await?
        .context("active transition")?;
    anyhow::ensure!(request.encode() == plan.encode() && retained == prepared);
    // Hold authorization after it read Prepared and built its next head.
    // Cancellation commits against that same head while the first CAS waits.
    f.store.range_before.armed.store(true, Ordering::Release);
    let entered = f.store.range_before.entered.notified();
    let (authorize, cancel) = f
        .run(cellar::asyncrt::timeout(Duration::from_secs(15), async {
            tokio::join!(reopened.authorize_transition_seal(&request), async {
                entered.await;
                let result = d.cancel_transition(&plan).await;
                f.store.range_before.resume.notify_one();
                result
            })
        }))
        .await?;
    anyhow::ensure!(matches!(authorize, Err(CatalogError::Conflict(_))));
    let winner = cancel?;
    anyhow::ensure!(winner.phase() == TransitionPhase::Cancelled);
    anyhow::ensure!(f.run(reopened.active_transition()).await?.is_none());
    anyhow::ensure!(f.run(d.plan_transition(&plan)).await? == winner);
    let current = f.run(d.observe()).await?;
    let reused = d.prepare_replacement(
        &current,
        operation(101),
        std::slice::from_ref(&parent),
        &children,
    )?;
    anyhow::ensure!(matches!(
        f.run(d.publish(&reused)).await,
        Err(CatalogError::Conflict(_))
    ));
    anyhow::ensure!(matches!(
        f.run(d.plan_transition(&reused)).await,
        Err(CatalogError::Conflict(_))
    ));
    anyhow::ensure!(f.run(reopened.observe()).await?.groups() == 1);
    f.domain.shutdown().await;
    Ok(())
}

pub(crate) async fn unknown_transition_authorization() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let d = &f.directory;
    let (plan, _, parent) = split_plan(&f).await?;
    f.run(d.plan_transition(&plan)).await?;
    f.run(d.prepare_transition(&plan, [81; 32])).await?;
    f.store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    anyhow::ensure!(matches!(
        f.run(d.authorize_transition_seal(&plan)).await,
        Err(CatalogError::Unconfirmed)
    ));
    f.store.fail_read.store(false, Ordering::Release);
    let restarted = f.reopen().await?;
    let (original, journal) = f
        .run(restarted.active_transition())
        .await?
        .context("rooted authorization")?;
    anyhow::ensure!(original.encode() == plan.encode());
    anyhow::ensure!(
        journal.phase() == TransitionPhase::SealAuthorized
            && journal.preparation() == Some([81; 32])
    );
    anyhow::ensure!(
        f.run(restarted.authorize_transition_seal(&original))
            .await?
            == journal
    );
    anyhow::ensure!(matches!(
        f.run(restarted.cancel_transition(&original)).await,
        Err(CatalogError::Conflict(_))
    ));
    anyhow::ensure!(matches!(
        f.run(restarted.prepare_transition(&original, [82; 32]))
            .await,
        Err(CatalogError::Conflict(_))
    ));
    let view = f.run(restarted.observe()).await?;
    anyhow::ensure!(
        f.run(restarted.lookup(&view, &CellId::new("unchanged-cell")?))
            .await?
            == parent
    );
    f.domain.shutdown().await;
    Ok(())
}
