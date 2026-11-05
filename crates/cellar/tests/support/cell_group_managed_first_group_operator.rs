// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The real operator CLI must preserve the first group's admitted request.
use super::*;
use cellar::sdk::fleet_operations::*;
#[path = "raft_first_group_operator_cli.rs"]
mod cli;
use cli::{first_group_plan, OperatorCli, FLEET, OWNER, READER};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_first_group_cli_admin_and_cold_quorum() -> Result<()> {
    let first = Fixture::with_class(false, counter_class()?).await?;
    let cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Operator,
    ))
    .await?;
    let work = seed_cells(&cluster.hosts, cluster.history).await;
    let joined = close_group_nodes(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined?;
    Ok(())
}

pub(super) async fn provision(
    f: &Fixture,
    directory: &RangeDirectory,
    setup: RaftManagedFirstGroupRequest,
) -> Result<FleetGroupAuthorization> {
    let bucket = Bucket::from_clients(
        f.store.clone(),
        f.store.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "managed-component".into(),
        "tenant/".into(),
    )?;
    let control = RaftFleetControl::new(&f.domain, f.catalog.clone(), vec![directory.clone()])?;
    let root = tempfile::tempdir()?;
    let cli = OperatorCli::start(
        &f.domain,
        bucket,
        control,
        root.path(),
        &setup.reservation.principal,
    )
    .await?;
    let result = exercise(f, &cli, root.path(), setup).await;
    cli.close().await?;
    result
}

async fn exercise(
    f: &Fixture,
    cli: &OperatorCli,
    root: &Path,
    setup: RaftManagedFirstGroupRequest,
) -> Result<FleetGroupAuthorization> {
    let history = setup.group.history;
    let plan = first_group_plan(&cli.catalog, setup)?;
    let namespace = &plan.namespace_id;
    let url = &cli.url;
    let confirmation_id = hex::encode([61; 16]);
    let confirmation_revision = plan.expected_revision + 1;
    let observed: RaftFleetFirstGroupStandby = serde_json::from_value(
        cli.call(&["first-group", "standby", namespace], READER)
            .await?
            .success()?,
    )?;
    ensure!(observed.standby == plan.standby);
    let plan_path = root.join("plan.json");
    std::fs::write(&plan_path, serde_json::to_vec(&plan)?)?;
    let path = plan_path.to_str().context("UTF-8 plan path")?;
    let reserve = ["first-group", "reserve", namespace, "--plan", path];
    let revision = confirmation_revision.to_string();
    let publish = [
        "first-group",
        "publish",
        namespace,
        "--plan",
        path,
        "--request-id",
        &confirmation_id,
        "--expected-revision",
        &revision,
    ];

    let denied = cli.call(&reserve, READER).await?;
    ensure!(denied.code == 4);
    let early = cli.call(&publish, OWNER).await?;
    ensure!(early.code == 3);
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == plan.expected_revision);
    ensure!(cli.revision().await? == plan.expected_revision);

    let wrong = reqwest::Client::builder()
        .no_proxy()
        .build()?
        .post(format!(
            "{url}/api/v1/fleets/{FLEET}/raft/namespaces/{}/first-group/reserve",
            hex::encode([77; 16])
        ))
        .bearer_auth(OWNER)
        .json(&plan)
        .send()
        .await?;
    ensure!(wrong.status() == reqwest::StatusCode::BAD_REQUEST);

    let reservation = cli.call(&reserve, OWNER).await?.success()?;
    ensure!(reservation["stage"] == "reserved");
    ensure!(cli.call(&reserve, OWNER).await?.success()? == reservation);
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == confirmation_revision);

    let mut changed = plan.clone();
    changed.publication_id = hex::encode([126; 16]);
    let changed_path = root.join("changed.json");
    std::fs::write(&changed_path, serde_json::to_vec(&changed)?)?;
    let altered = cli
        .call(
            &[
                "first-group",
                "reserve",
                namespace,
                "--plan",
                changed_path.to_str().context("UTF-8 plan path")?,
            ],
            OWNER,
        )
        .await?;
    ensure!(altered.code == 3 && altered.value["error"]["code"] == "payload_conflict");
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == confirmation_revision);

    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = cli.call(&publish, OWNER).await?;
    ensure!(lost.code == 5 && lost.value["error"]["outcome"] == "unknown");
    ensure!(!f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let selected = cli.call(&publish, OWNER).await?.success()?;
    ensure!(selected["stage"] == "published");
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == confirmation_revision + 1);
    Ok(f.catalog.authorize_group(&cut, &history, TIMEOUT).await?)
}
