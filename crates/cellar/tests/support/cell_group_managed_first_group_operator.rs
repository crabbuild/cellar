// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The real operator CLI must preserve the first group's admitted request.
use super::*;
use cellar::sdk::fleet_operations::*;
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
#[path = "raft_first_group_operator_cli.rs"]
mod cli;
use cli::{first_group_plan, OperatorCli, FLEET, OWNER, READER};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_first_group_cli_admin_and_cold_quorum() -> Result<()> {
    let first = Fixture::with_class(false, counter_class()?).await?;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[],
        ArchiveSetup::Initialized,
        true,
        None,
        FirstGroupSetup::Operator,
    ))
    .await?;
    let work = async {
        seed_cells(&cluster.hosts, cluster.history).await?;
        let peers = cluster.nodes.split_off(1);
        close_group_nodes(peers, &cluster.hosts[1..]).await?;
        let survivor = &cluster.hosts[0];
        tokio::time::timeout(TIMEOUT, async {
            let group = survivor.group(cluster.history)?;
            loop {
                let status = group.status().await?;
                if !status.ready && status.leader.is_none() {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("observe first group quorum loss")??;
        let readiness = survivor
            .start_managed_first_group(cluster.history, TIMEOUT)
            .await;
        ensure!(
            matches!(
                readiness,
                Err(CellGroupStartupError::Unconfirmed(
                    CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded)
                ))
            ),
            "retained attachment must refuse readiness without quorum: {readiness:?}"
        );
        ensure!(survivor.resource_usage().host.controller.groups == 1);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = close_group_nodes(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined?;
    Ok(())
}

pub(super) async fn provision(
    f: &Fixture,
    directory: &RangeDirectory,
    voter: &RaftCells,
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
    let result = exercise(f, &cli, root.path(), voter, setup).await;
    cli.close().await?;
    result
}

async fn exercise(
    f: &Fixture,
    cli: &OperatorCli,
    root: &Path,
    voter: &RaftCells,
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

    let archive_id = hex::encode([62; 16]);
    let archive_revision = (confirmation_revision + 1).to_string();
    let archive = [
        "first-group",
        "archive",
        namespace,
        "--plan",
        path,
        "--request-id",
        &archive_id,
        "--expected-revision",
        &archive_revision,
    ];
    ensure!(cli.call(&archive, READER).await?.code == 4);
    ensure!(cli.call(&archive, OWNER).await?.code == 3);
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

    let archive_receipt = cli.call(&archive, OWNER).await?.success()?;
    ensure!(archive_receipt["stage"] == "archive_initialized");
    let writes = f.store.registry_writes.load(Ordering::Acquire);
    ensure!(cli.call(&archive, OWNER).await?.success()? == archive_receipt);
    ensure!(f.store.registry_writes.load(Ordering::Acquire) == writes);
    let changed_id = hex::encode([63; 16]);
    let mut competing = archive;
    competing[6] = &changed_id;
    let conflict = cli.call(&competing, OWNER).await?;
    ensure!(conflict.code == 3 && conflict.value["error"]["code"] == "payload_conflict");

    // The fixture owns its store prefix and no voter has attached yet. Remove
    // its one registry while retaining the fleet intent and archive identity.
    let registry = f
        .store
        .list(None)
        .try_filter(|meta| {
            let is_registry = meta.location.as_ref().contains("/cell-group-archives/")
                && meta.location.as_ref().ends_with("/ownership/registry");
            std::future::ready(is_registry)
        })
        .try_collect::<Vec<_>>()
        .await?;
    let [registry] = registry.as_slice() else {
        anyhow::bail!("one archive registry must exist before activation");
    };
    let key = &registry.location;
    let bytes = f.store.get(key).await?.bytes().await?;
    f.store.delete(key).await?;
    let replay = cli.call(&archive, OWNER).await?;
    ensure!(replay.code == 5 && replay.value["error"]["outcome"] == "unknown");
    ensure!(matches!(
        f.store.head(key).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let readiness = voter
        .start_managed_first_group(history, Duration::from_millis(250))
        .await;
    ensure!(matches!(readiness, Err(CellGroupStartupError::Refused(_))));
    ensure!(voter.observe_node(TIMEOUT).await?.node.groups == 0);
    ensure!(matches!(
        f.store.head(key).await,
        Err(object_store::Error::NotFound { .. })
    ));
    f.store.put(key, bytes.into()).await?;
    ensure!(cli.call(&archive, OWNER).await?.success()? == archive_receipt);

    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == confirmation_revision + 2);
    Ok(f.catalog.authorize_group(&cut, &history, TIMEOUT).await?)
}
