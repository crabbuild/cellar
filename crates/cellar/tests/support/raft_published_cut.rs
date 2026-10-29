//! Actual quorum cuts preserve publication identity across configuration and SQL.
use super::retirement::{discover, reserve};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_published_cut_configuration_progress() -> Result<()> {
    let f = Fixture::with_domains(true, true).await?;
    let nodes = fleet(&f).await?;
    let (_root, observer_node, observer) = f.peer_host(7).await?;
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    write_read(&f, &cells, 1, 4, 4).await?;
    let p = primary(&f, &cells).await?;
    let hosts = [&nodes[0].host, &nodes[1].host, &nodes[2].host, &observer];
    discover(&f, &hosts).await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    let before = observer
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await?;
    let before_cut = before.published_cut().context("initial published cut")?;
    let original_image = before_cut.image().context("initial image")?.clone();
    let (_plan, permit) = reserve(
        &f,
        &cells[p],
        &proposal.scope,
        1,
        160,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: DiskLineage::new([17; 16])?,
        },
    )
    .await?;
    let fenced = cells[p].fence_fleet_permit(permit, TIMEOUT).await?;
    ensure!(fenced.phase == Some(FleetPermitPhase::Fenced));
    discover(&f, &hosts).await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    let observed = observer
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await?;
    let fixed = observed
        .published_cut()
        .context("configuration-only publication cut")?
        .clone();
    ensure!(fixed.image() == Some(&original_image));
    ensure!(fixed.applied().index > original_image.position().index);
    ensure!(fixed.membership_revision() > before_cut.membership_revision());
    ensure!(matches!(
        fixed.covers(before_cut),
        Err(CommandFailure::CommandConflict)
    ));
    for node in &nodes {
        let replica = observer
            .observe_peer_fleet_membership(node.spec.node, auth.clone(), revision, TIMEOUT)
            .await?;
        let published = replica.published_cut().context("surviving publication")?;
        ensure!(published.covers(&fixed)?);
        ensure!(published.replica().node == node.spec.node);
        ensure!(published.storage_instance() == replica.node().node.storage_instance);
        ensure!(published.applied().index >= replica.membership().barrier());
    }
    let retained = stop(nodes).await?;
    let nodes = restart(&f, retained, &proposal).await?;
    let cells = handles(&nodes, &proposal)?;
    let p = primary(&f, &cells).await?;
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &observer],
    )
    .await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    for node in &nodes {
        let restored = observer
            .observe_peer_fleet_membership(node.spec.node, auth.clone(), revision, TIMEOUT)
            .await?;
        let cut = restored.published_cut().context("restored publication")?;
        ensure!(cut.image().unwrap().position().index >= fixed.applied().index);
        ensure!(cut.image().unwrap().digest()? == original_image.digest()?);
        ensure!(cut.image().unwrap().txid() == original_image.txid());
        ensure!(cut.covers(&fixed)?);
    }
    write_read(&f, &cells, 2, 5, 9).await?;
    let newer = observer
        .observe_peer_fleet_membership(nodes[p].spec.node, auth, revision, TIMEOUT)
        .await?;
    let newer = newer.published_cut().context("later published data")?;
    ensure!(newer.covers(&fixed)?);
    ensure!(!fixed.covers(newer)?);
    ensure!(newer.image().unwrap().position().index > fixed.applied().index);
    ensure!(f.calls.load(Ordering::Acquire) == 2);
    ensure!(observer.observe_node(TIMEOUT).await?.node.groups == 0);
    observer_node.shutdown(ShutdownMode::Preserve).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_published_cut_excludes_private_sql_image() -> Result<()> {
    let mut f = Fixture::with_domains(true, true).await?;
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let definition = public_node::counter(f.calls.clone(), gate.clone())?;
    ensure!(definition.schema == f.definition.schema);
    f.definition = definition;
    let nodes = fleet(&f).await?;
    let (_root, observer_node, observer) = f.peer_host(7).await?;
    let (_proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    let p = primary(&f, &cells).await?;
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &observer],
    )
    .await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    write_read(&f, &cells, 1, 4, 4).await?;
    let before = observer
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await?;
    let before = before
        .published_cut()
        .context("published image before private SQL")?
        .clone();
    let pending = cells[p].command(
        f.definition.increment.clone(),
        7_i64.to_be_bytes().to_vec(),
        public_node::archived_options(2)?,
    );
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, entered.recv()))
        .await??
        .context("private SQL did not enter")?;
    let observed = observer
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await?;
    let visible = observed
        .published_cut()
        .context("published cut during private SQL")?
        .clone();
    ensure!(visible.image() == before.image());
    ensure!(visible.covers(&before)?);
    ensure!(
        observed.progress().member.as_ref().unwrap().published
            == Some(before.image().unwrap().position())
    );
    ensure!(f.calls.load(Ordering::Acquire) == 2);
    ensure!(observer.resource_usage().query_pins.in_use == 0);
    ensure!(nodes[p].host.resource_usage().query_pins.in_use == 0);
    gate.release();
    let receipt = pending.await?;
    ensure!(receipt.durability == Durability::S3);
    let next = observer
        .observe_peer_fleet_membership(nodes[p].spec.node, auth, revision, TIMEOUT)
        .await?;
    let next = next.published_cut().context("committed published image")?;
    ensure!(next.image().unwrap().position() == receipt.version.position);
    ensure!(next.covers(&before)?);
    let read = cells[p]
        .query(
            f.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(i64::from_be_bytes(read.result.as_slice().try_into()?) == 11);
    ensure!(f.calls.load(Ordering::Acquire) == 2);
    observer_node.shutdown(ShutdownMode::Preserve).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}
