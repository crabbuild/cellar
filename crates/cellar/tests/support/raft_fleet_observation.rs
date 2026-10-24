//! Fresh membership observations over real TLS links and native SQLite hosts.
//! These are group observations; none certifies a complete fleet import.
use super::*;

async fn authorization(f: &Fixture) -> Result<FleetGroupAuthorization> {
    let root = f.catalog.read(TIMEOUT).await?;
    Ok(f.catalog
        .authorize_group(&root, &f.registration.birth.scope, TIMEOUT)
        .await?)
}

fn check(f: &Fixture, observation: &FleetMembershipObservation) -> Result<()> {
    ensure!(observation.replica().scope == f.registration.birth.scope);
    ensure!(observation.status().binding == f.binding);
    ensure!(observation.schema_digest() == f.registration.namespace.schema_digest);
    ensure!(observation.birth_digest() != [0; 32]);
    ensure!(observation.barrier().get() > 0);
    ensure!(observation.applied().index >= observation.barrier());
    ensure!(observation.applied().index >= observation.membership_revision());
    ensure!(observation.membership_revision() >= observation.status().changed_at);
    Ok(())
}

async fn control_idle(f: &Fixture, node: usize) -> Result<()> {
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let usage = f.cluster.services[node].resource_usage();
                ensure!(usage.control.bytes <= usage.control.max_bytes);
                ensure!(usage.control.items <= usage.control.max_items);
                ensure!(usage.query_pins.in_use == 0);
                if usage.control.bytes == 0 && usage.control.items == 0 {
                    return Ok(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_observation_without_image() -> Result<()> {
    let f = Fixture::create(false, true).await?;
    let authorization = authorization(&f).await?;
    for (index, cell) in f.handles.iter().enumerate() {
        ensure!(cell.status().await?.version.is_none());
        let observation = cell
            .observe_fleet_membership(authorization.clone(), TIMEOUT)
            .await?;
        check(&f, &observation)?;
        ensure!(observation.configuration().voters().len() == 3);
        ensure!(observation.configuration().outgoing_voters().is_empty());
        ensure!(observation.configuration().learners().is_empty());
        ensure!(observation.configuration().next_learners().is_empty());
        ensure!(!observation.configuration().auto_leave());
        ensure!(observation.repair_status()?.is_none());
        ensure!(observation.replica().node.bytes() == [index as u8 + 1; 16]);
        ensure!(observation.replica().disk.bytes() == [index as u8 + 10; 16]);
        ensure!(observation.replica().process.bytes() == [index as u8 + 20; 16]);
    }
    ensure!(f
        .calls
        .iter()
        .all(|calls| calls.load(Ordering::SeqCst) == 0));
    let receipt = f.write(1, 7, false).await?;
    let next = f.handles[(f.primary + 1) % 3]
        .observe_fleet_membership(authorization, TIMEOUT)
        .await?;
    check(&f, &next)?;
    ensure!(next.barrier() >= receipt.version.position.index);
    ensure!(
        f.calls
            .iter()
            .map(|calls| calls.load(Ordering::SeqCst))
            .sum::<usize>()
            == 1
    );
    control_idle(&f, f.primary).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_observation_requires_enrollment() -> Result<()> {
    let f = Fixture::create(false, false).await?;
    let authorization = authorization(&f).await?;
    ensure!(matches!(
        f.handles[f.primary]
            .observe_fleet_membership(authorization.clone(), TIMEOUT)
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    let expected = f.handles[f.primary].status().await?.membership_revision;
    f.handles[f.primary]
        .enroll_fleet(authorization.clone(), expected, TIMEOUT)
        .await?;
    let enrolled = f.handles[f.primary]
        .observe_fleet_membership(authorization, TIMEOUT)
        .await?;
    check(&f, &enrolled)?;

    // The same scope and fleet binding under another immutable namespace root
    // is not this history. A rooted authorization still must match its birth.
    let namespace = Catalog::new(
        f.cluster.store.clone(),
        CatalogOptions {
            prefix: "observation/foreign-data".into(),
            ..Default::default()
        },
    )?
    .open_namespace(f.registration.namespace.clone())
    .await?;
    let birth = namespace
        .resolve_birth(f.registration.birth.clone())
        .await?;
    ensure!(birth.digest() != enrolled.birth_digest());
    let root = f.catalog.read(TIMEOUT).await?;
    // Use a separate fleet catalog with the same fleet ID so its opaque
    // capability has the same binding but a different actual birth digest.
    let bucket = Bucket::from_clients(
        f.cluster.store.clone(),
        f.cluster.store.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "foreign-birth".into(),
        "".into(),
    )?;
    let foreign = FleetCatalog::new(
        &f.domain,
        bucket,
        f.registration.namespace.cluster,
        IncarnationId::new(f.binding.fleet())?,
        "observation/foreign-coordination",
    )?;
    foreign.initialize(TIMEOUT).await?;
    let foreign_root = foreign.read(TIMEOUT).await?;
    foreign
        .enroll_history(
            &foreign_root,
            &FleetRequestIdentity {
                id: [91; 16],
                operation: [92; 16],
                principal: "observation-test".into(),
                expected_revision: foreign_root.revision(),
            },
            &namespace,
            &birth,
            TIMEOUT,
        )
        .await?;
    let foreign_root = foreign.read(TIMEOUT).await?;
    let wrong = foreign
        .authorize_group(&foreign_root, &f.registration.birth.scope, TIMEOUT)
        .await?;
    ensure!(matches!(
        f.handles[f.primary]
            .observe_fleet_membership(wrong, TIMEOUT)
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_observation_partition_and_recovery() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = authorization(&f).await?;
    let old = f.handles[f.primary]
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    check(&f, &old)?;
    f.cluster.isolate(f.primary, false).await?;
    ensure!(f.handles[f.primary]
        .fleet_membership_status()
        .await?
        .is_some());
    for _ in 0..2 {
        ensure!(f.handles[f.primary]
            .observe_fleet_membership(authorization.clone(), Duration::from_millis(500))
            .await
            .is_err());
    }
    let survivors: Vec<_> = f
        .handles
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != f.primary)
        .map(|(_, cell)| cell.clone())
        .collect();
    let successor = leader(&f.domain, &survivors).await?;
    let receipt = survivors[successor]
        .command(
            f.definitions[0].increment.clone(),
            9i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await?;
    let fresh = survivors[successor]
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    check(&f, &fresh)?;
    ensure!(fresh.barrier() >= receipt.version.position.index);
    ensure!(fresh.barrier() > old.barrier());
    f.cluster.heal();
    let healed = f.handles[f.primary]
        .observe_fleet_membership(authorization, TIMEOUT)
        .await?;
    check(&f, &healed)?;
    ensure!(healed.barrier() >= receipt.version.position.index);
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_observation_permit_phases() -> Result<()> {
    let mut f = Fixture::new().await?;
    let group = authorization(&f).await?;
    let plan = f.plan(40).await?;
    let permit = f.reserve(1, 20, &plan).await?;
    f.handles[f.primary]
        .begin_managed_repair(permit.clone(), plan.clone(), TIMEOUT)
        .await?;
    let seeding = f.handles[f.primary]
        .observe_fleet_membership(group.clone(), TIMEOUT)
        .await?;
    check(&f, &seeding)?;
    ensure!(seeding.status().phase == Some(FleetPermitPhase::Seeding));
    ensure!(seeding.configuration().learners() == [plan.replacement.member]);
    ensure!(
        seeding
            .joined_member(plan.replacement.member)?
            .context("joined learner")?
            .0
            == plan.replacement
    );
    let target = f.standby().await?;
    target
        .activate_managed_learner(permit.clone(), f.activation(false)?)
        .await?;
    f.step(&permit, RepairStep::PromoteAndRemove).await?;
    let joint = target
        .observe_fleet_membership(group.clone(), TIMEOUT)
        .await?;
    check(&f, &joint)?;
    ensure!(joint.status().phase == Some(FleetPermitPhase::Joint));
    ensure!(joint
        .configuration()
        .voters()
        .contains(&plan.replacement.member));
    ensure!(!joint.configuration().voters().contains(&plan.obsolete));
    ensure!(joint
        .configuration()
        .outgoing_voters()
        .contains(&plan.obsolete));
    ensure!(joint.configuration().outgoing_voters().len() == 3);
    ensure!(joint.repair_status()?.context("joint repair")?.phase == RepairPhase::Joint);
    let terminal = f.step(&permit, RepairStep::FinishJoint).await?;
    let complete = target.observe_fleet_membership(group, TIMEOUT).await?;
    check(&f, &complete)?;
    ensure!(complete.status().phase == Some(FleetPermitPhase::Complete));
    ensure!(complete.membership_revision() == terminal.changed_at);
    ensure!(complete.configuration().outgoing_voters().is_empty());
    ensure!(complete.configuration().voters().len() == 3);
    ensure!(complete.configuration().learners().is_empty());
    ensure!(complete.configuration().next_learners().is_empty());
    ensure!(f.strong().await? == 5);
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_observation_cancellation_pressure_and_close() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = authorization(&f).await?;
    f.cluster.isolate(f.primary, false).await?;
    let pending = f.handles[f.primary].observe_fleet_membership(authorization.clone(), TIMEOUT);
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let usage = f.cluster.services[f.primary].resource_usage();
                if usage.control.bytes >= 64 * 1024 + 4096 {
                    break;
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    drop(pending);
    control_idle(&f, f.primary).await?;
    let requests: Vec<_> = (0..64)
        .map(|_| {
            f.handles[f.primary]
                .observe_fleet_membership(authorization.clone(), Duration::from_millis(500))
        })
        .collect();
    let usage = f.cluster.services[f.primary].resource_usage();
    ensure!(usage.control.bytes <= usage.control.max_bytes);
    ensure!(usage.control.items <= usage.control.max_items);
    let results = futures_util::future::join_all(requests).await;
    ensure!(results
        .iter()
        .any(|result| matches!(result, Err(CommandFailure::Overloaded))));
    ensure!(results.iter().all(Result::is_err));
    control_idle(&f, f.primary).await?;
    let pending = f.handles[f.primary].observe_fleet_membership(authorization, TIMEOUT);
    ensure!(f.cluster.services[f.primary]
        .close()
        .await?
        .unfinished
        .is_empty());
    ensure!(pending.await.is_err());
    control_idle(&f, f.primary).await?;
    f.close().await
}
