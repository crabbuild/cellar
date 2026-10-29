// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Restarted controllers recover exact reservation authority, never a new birth.
use super::*;
use cellar::application::consensus::verification::{BirthDisposition, CatalogError};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settlement_recovers_birth_winners_after_controller_restart() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let mut scopes = Vec::new();
    for index in 0..2_u8 {
        let mut proposal = s.proposal.clone();
        proposal.scope.group = GroupId::new([241 + index; 16])?;
        proposal.scope.cell = format!("Counter:pending-restart-{index}");
        let cut = s.f.catalog.read(TIMEOUT).await?;
        let reserved =
            s.f.catalog
                .reserve_birth(
                    &cut,
                    &request(220 + index, cut.revision()),
                    &s.namespace,
                    proposal.clone(),
                    TIMEOUT,
                )
                .await?;
        if index == 0 {
            s.f.domain
                .run(
                    s.namespace
                        .resolve_managed_birth(&reserved.authorization, TIMEOUT),
                )
                .await??;
        }
        scopes.push(proposal.scope);
        // Both capabilities are lost; one birth has already won in the namespace.
        drop(reserved);
    }
    let boot = s.nodes[0]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("restart boot")?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let intent = restart(230, cut.revision(), &boot)?;
    s.f.catalog
        .begin_restart_maintenance(&cut, &intent, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let operation = ProposalId::new(intent.identity.operation)?;
    let cut = reopened.read(TIMEOUT).await?;
    ensure!(matches!(
        reopened
            .pending_restart_work(&cut, operation, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(matches!(
        reopened
            .pending_restart_work(
                &cut,
                ProposalId::new([239; 16])?,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let pending = reopened
        .pending_restart_work(&cut, operation, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    ensure!(
        pending.pending_births == 2
            && pending
                .next_birth
                .as_ref()
                .is_some_and(|scope| scopes.contains(scope))
    );
    ensure!(pending.movements.is_empty() && pending.reservations.is_empty());
    let mut wrong = scopes[0].clone();
    wrong.incarnation = IncarnationId::new([238; 16])?;
    ensure!(reopened
        .recover_birth_reservation(&cut, &s.namespace, &wrong, TIMEOUT)
        .await
        .is_err());
    let wrong_namespace = Catalog::new(
        s.f.store.clone(),
        CatalogOptions {
            prefix: "restart-wrong-namespace/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(s.namespace.config().clone())
    .await?;
    ensure!(reopened
        .recover_birth_reservation(&cut, &wrong_namespace, &scopes[0], TIMEOUT)
        .await
        .is_err());

    for (index, scope) in scopes.iter().enumerate() {
        let cut = reopened.read(TIMEOUT).await?;
        let recovered = reopened
            .recover_birth_reservation(&cut, &s.namespace, scope, TIMEOUT)
            .await?;
        let winner =
            s.f.domain
                .run(
                    s.namespace
                        .cancel_managed_birth(&recovered, operation, TIMEOUT),
                )
                .await??;
        if index == 0 {
            ensure!(matches!(winner, BirthDisposition::Live(ref birth) if birth.scope() == scope));
        } else {
            ensure!(
                matches!(winner, BirthDisposition::Cancelled { scope: ref actual, operation: actual_op } if actual == scope && actual_op == operation)
            );
            ensure!(matches!(
                s.f.domain
                    .run(s.namespace.resolve_managed_birth(&recovered, TIMEOUT))
                    .await?,
                Err(CatalogError::BirthCancelled)
            ));
        }
        let confirm = request(234 + index as u8, cut.revision());
        s.f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        ensure!(reopened
            .confirm_birth(&cut, &confirm, &s.namespace, &recovered, TIMEOUT)
            .await
            .is_err());
        s.f.store.fail_fleet_read.store(false, Ordering::Release);
        let now = reopened.read(TIMEOUT).await?;
        let confirmed = reopened
            .confirm_birth(&now, &confirm, &s.namespace, &recovered, TIMEOUT)
            .await?;
        ensure!(confirmed.applied_revision == cut.revision() + 1);
        let pending = reopened
            .pending_restart_work(&now, operation, std::slice::from_ref(&s.namespace), TIMEOUT)
            .await?;
        ensure!(pending.pending_births == 1 - index as u64);
        // Recovery still follows the same original reservation after confirmation.
        let recovered = reopened
            .recover_birth_reservation(&now, &s.namespace, scope, TIMEOUT)
            .await?;
        if index == 1 {
            ensure!(reopened
                .authorize_group(&now, scope, TIMEOUT)
                .await
                .is_err());
            ensure!(matches!(
                s.f.domain
                    .run(s.namespace.resolve_managed_birth(&recovered, TIMEOUT))
                    .await?,
                Err(CatalogError::BirthCancelled)
            ));
        }
    }
    let now = reopened.read(TIMEOUT).await?;
    ensure!(matches!(
        reopened
            .pending_restart_work(&cut, operation, std::slice::from_ref(&s.namespace), TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let pending = reopened
        .pending_restart_work(&now, operation, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    ensure!(pending.pending_births == 0 && pending.next_birth.is_none());
    let mut cancel = intent.clone();
    cancel.identity.id = [240; 16];
    cancel.identity.expected_revision = now.revision();
    let cancelled = reopened
        .cancel_restart_preparation(&now, &cancel, TIMEOUT)
        .await?;
    ensure!(matches!(
        reopened
            .pending_restart_work(
                &cancelled.snapshot,
                operation,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    s.close().await
}
