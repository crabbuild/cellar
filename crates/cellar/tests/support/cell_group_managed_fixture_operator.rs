// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real operator CLI and admin admission for the managed fixture.

use super::*;
use cellar::application::consensus::{CellGroupRecoveryPool, FleetGroupArchiveVerifier};
use cellar::sdk::fleet_operations::*;
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
#[path = "raft_first_group_operator_cli.rs"]
mod cli;
use cli::{first_group_plan, OperatorCli, FLEET, OWNER, READER};

pub(super) async fn provision(
    f: &Fixture,
    directory: &RangeDirectory,
    voter: &RaftCells,
    setup: RaftManagedFirstGroupRequest,
) -> Result<FleetGroupAuthorization> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(
        f,
        directory,
        root.path(),
        &setup.reservation.principal,
        None,
    )
    .await?;
    let result = exercise(f, &cli, root.path(), voter, setup).await;
    cli.close().await?;
    result
}

async fn open_cli(
    fixture: &Fixture,
    directory: &RangeDirectory,
    root: &Path,
    principal: &str,
    observer: Option<&RaftCells>,
) -> Result<OperatorCli> {
    let (bucket, mut control) = control(fixture, directory)?;
    if let Some(observer) = observer {
        control = control.with_node_observer(observer.clone());
    }
    OperatorCli::start(&fixture.domain, bucket, control, root, principal).await
}

fn control(fixture: &Fixture, directory: &RangeDirectory) -> Result<(Bucket, RaftFleetControl)> {
    let bucket = Bucket::from_clients(
        fixture.store.clone(),
        fixture.store.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "managed-component".into(),
        "tenant/".into(),
    )?;
    let catalog = Fixture::open_catalog(
        &fixture.domain,
        fixture.store.clone(),
        directory.namespace_config().cluster,
    )?;
    Ok((
        bucket,
        RaftFleetControl::new(&fixture.domain, catalog, vec![directory.clone()])?,
    ))
}

pub(crate) async fn prepare_transition(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    request: &RaftFleetTransitionPrepareRequest,
    preparer: Option<(FleetGroupArchiveVerifier, CellGroupRecoveryPool)>,
) -> Result<RaftFleetTransitionPreparation> {
    let root = tempfile::tempdir()?;
    let (bucket, mut control) = control(fixture, directory)?;
    if let Some((verifier, recovery)) = preparer {
        control = control.with_transition_preparer(verifier, recovery);
    }
    let cli = OperatorCli::start(&fixture.domain, bucket, control, root.path(), principal).await?;
    let result = async {
        let plan = root.path().join("transition-prepare.json");
        std::fs::write(&plan, serde_json::to_vec(request)?)?;
        let command = [
            "transition",
            "prepare",
            &request.namespace_id,
            "--plan",
            plan.to_str()
                .context("UTF-8 transition preparation request path")?,
        ];
        ensure!(cli.call(&command, READER).await?.code == 4);
        Ok::<_, anyhow::Error>(serde_json::from_value(
            cli.call(&command, OWNER).await?.success()?,
        )?)
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn capture_transition_source(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    request: &RaftFleetTransitionSourceSealRequest,
    preparer: (FleetGroupArchiveVerifier, CellGroupRecoveryPool),
) -> Result<RaftFleetTransitionSourceArchive> {
    let root = tempfile::tempdir()?;
    let (bucket, mut control) = control(fixture, directory)?;
    control = control.with_transition_preparer(preparer.0, preparer.1);
    let cli = OperatorCli::start(&fixture.domain, bucket, control, root.path(), principal).await?;
    let result = async {
        let plan = root.path().join("transition-capture-source.json");
        std::fs::write(&plan, serde_json::to_vec(request)?)?;
        let command = [
            "transition",
            "capture-source",
            &request.namespace_id,
            "--plan",
            plan.to_str().context("UTF-8 source capture request path")?,
        ];
        ensure!(cli.call(&command, READER).await?.code == 4);
        Ok::<_, anyhow::Error>(serde_json::from_value(
            cli.call_with_timeout(&command, OWNER, Duration::from_secs(32))
                .await?
                .success()?,
        )?)
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn verify_transition_archive(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    request: &RaftFleetTransitionPrepareRequest,
    preparer: (FleetGroupArchiveVerifier, CellGroupRecoveryPool),
) -> Result<RaftFleetTransitionArchiveVerification> {
    let root = tempfile::tempdir()?;
    let (bucket, mut control) = control(fixture, directory)?;
    control = control.with_transition_preparer(preparer.0, preparer.1);
    let cli = OperatorCli::start(&fixture.domain, bucket, control, root.path(), principal).await?;
    let result = async {
        let plan = root.path().join("transition-verify-archive.json");
        std::fs::write(&plan, serde_json::to_vec(request)?)?;
        let command = [
            "transition",
            "verify-archive",
            &request.namespace_id,
            "--plan",
            plan.to_str()
                .context("UTF-8 archive verification request path")?,
        ];
        ensure!(cli.call(&command, READER).await?.code == 4);
        Ok::<_, anyhow::Error>(serde_json::from_value(
            cli.call_with_timeout(&command, OWNER, Duration::from_secs(32))
                .await?
                .success()?,
        )?)
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn setup_transition_child(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    observer: &RaftCells,
    request: &RaftFleetTransitionChildSetupRequest,
    preparer: Option<(FleetGroupArchiveVerifier, CellGroupRecoveryPool)>,
) -> Result<RaftFleetTransitionChildSetup> {
    let root = tempfile::tempdir()?;
    let (bucket, mut control) = control(fixture, directory)?;
    control = control.with_node_observer(observer.clone());
    if let Some((verifier, recovery)) = preparer {
        control = control.with_transition_preparer(verifier, recovery);
    }
    let cli = OperatorCli::start(&fixture.domain, bucket, control, root.path(), principal).await?;
    let result = async {
        let plan = root.path().join("transition-setup-child.json");
        std::fs::write(&plan, serde_json::to_vec(request)?)?;
        let command = [
            "transition",
            "setup-child",
            &request.namespace_id,
            "--plan",
            plan.to_str().context("UTF-8 child setup request path")?,
        ];
        ensure!(cli.call(&command, READER).await?.code == 4);
        Ok::<_, anyhow::Error>(serde_json::from_value(
            cli.call_with_timeout(&command, OWNER, Duration::from_secs(32))
                .await?
                .success()?,
        )?)
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn transition_status(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    expected: &RaftFleetTransitionStatus,
) -> Result<()> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(fixture, directory, root.path(), principal, None).await?;
    let result = cli
        .call(
            &[
                "transition",
                "status",
                &expected.namespace_id,
                "--operation-id",
                &expected.operation_id,
            ],
            READER,
        )
        .await;
    cli.close().await?;
    let observed: RaftFleetTransitionStatus = serde_json::from_value(result?.success()?)?;
    ensure!(observed == *expected);
    Ok(())
}

pub(crate) async fn resume_transition(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    observer: &RaftCells,
    request: &RaftFleetTransitionResume,
) -> Result<RaftFleetTransitionResumeAcceptance> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(fixture, directory, root.path(), principal, Some(observer)).await?;
    let plan = root.path().join("transition-resume.json");
    std::fs::write(&plan, serde_json::to_vec(request)?)?;
    let path = plan.to_str().context("UTF-8 transition plan path")?;
    let result = cli
        .call(
            &[
                "transition",
                "resume",
                &request.namespace_id,
                "--plan",
                path,
            ],
            OWNER,
        )
        .await;
    cli.close().await?;
    Ok(serde_json::from_value(result?.success()?)?)
}

pub(crate) async fn admit_transition(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    request: &RaftFleetTransitionAdmissionRequest,
) -> Result<RaftFleetTransitionAdmission> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(fixture, directory, root.path(), principal, None).await?;
    let plan = root.path().join("transition-admission.json");
    std::fs::write(&plan, serde_json::to_vec(request)?)?;
    let path = plan
        .to_str()
        .context("UTF-8 transition admission plan path")?;
    let command = ["transition", "admit", &request.namespace_id, "--plan", path];
    let result = async {
        let basis: RaftFleetTransitionBasis = serde_json::from_value(
            cli.call(&["transition", "basis", &request.namespace_id], READER)
                .await?
                .success()?,
        )?;
        ensure!(
            basis.directory_digest == request.expected_directory_digest
                && basis.fleet_revision >= request.expected_fleet_revision
        );
        ensure!(cli.call(&command, READER).await?.code == 4);
        Ok::<_, anyhow::Error>(serde_json::from_value(
            cli.call(&command, OWNER).await?.success()?,
        )?)
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn authorize_transition_seal(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    request: &RaftFleetTransitionSealRequest,
) -> Result<RaftFleetTransitionSealAuthorization> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(fixture, directory, root.path(), principal, None).await?;
    let plan = root.path().join("transition-seal-authorization.json");
    std::fs::write(&plan, serde_json::to_vec(request)?)?;
    let command = [
        "transition",
        "authorize-seal",
        &request.namespace_id,
        "--plan",
        plan.to_str()
            .context("UTF-8 transition authorization plan path")?,
    ];
    let result = async {
        ensure!(cli.call(&command, READER).await?.code == 4);
        Ok::<_, anyhow::Error>(serde_json::from_value(
            cli.call(&command, OWNER).await?.success()?,
        )?)
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn seal_transition_source(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    observer: &RaftCells,
    request: &RaftFleetTransitionSourceSealRequest,
) -> Result<RaftFleetTransitionSourceCut> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(fixture, directory, root.path(), principal, Some(observer)).await?;
    let result = async {
        let plan = root.path().join("transition-seal-source.json");
        std::fs::write(&plan, serde_json::to_vec(request)?)?;
        let command = [
            "transition",
            "seal-source",
            &request.namespace_id,
            "--plan",
            plan.to_str().context("UTF-8 source seal request path")?,
        ];
        ensure!(cli.call(&command, READER).await?.code == 4);
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let reply = cli.call(&command, OWNER).await?;
                if reply.code == 5 {
                    let failure: RaftFleetFailure =
                        serde_json::from_value(reply.value["error"].clone())?;
                    ensure!(
                        matches!(
                            failure.code,
                            RaftFleetFailureCode::Busy | RaftFleetFailureCode::Unavailable
                        ),
                        "unexpected source seal failure: {failure:?}"
                    );
                } else {
                    let sealed: RaftFleetTransitionSourceSeal =
                        serde_json::from_value(reply.success()?)?;
                    ensure!(&sealed.request == request);
                    if let Some(cut) = sealed.applied {
                        let replay: RaftFleetTransitionSourceSeal =
                            serde_json::from_value(cli.call(&command, OWNER).await?.success()?)?;
                        ensure!(
                            &replay.request == request && replay.applied.as_ref() == Some(&cut)
                        );
                        return Ok(cut);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("operator parent seal did not apply")?
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn publish_transition(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    request: &RaftFleetTransitionPublishRequest,
) -> Result<RaftFleetTransitionPublication> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(fixture, directory, root.path(), principal, None).await?;
    let plan = root.path().join("transition-publication.json");
    std::fs::write(&plan, serde_json::to_vec(request)?)?;
    let path = plan
        .to_str()
        .context("UTF-8 transition publication plan path")?;
    let command = [
        "transition",
        "publish",
        &request.namespace_id,
        "--plan",
        path,
    ];
    let result = async {
        ensure!(cli.call(&command, READER).await?.code == 4);
        let value = cli.call(&command, OWNER).await?.success()?;
        Ok::<_, anyhow::Error>(serde_json::from_value(value)?)
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) async fn activate_transition_child(
    fixture: &Fixture,
    directory: &RangeDirectory,
    principal: &str,
    observer: &RaftCells,
    request: &RaftFleetTransitionActivationRequest,
) -> Result<RaftFleetTransitionActivation> {
    let root = tempfile::tempdir()?;
    let cli = open_cli(fixture, directory, root.path(), principal, Some(observer)).await?;
    let plan = root.path().join("transition-activation.json");
    std::fs::write(&plan, serde_json::to_vec(request)?)?;
    let path = plan
        .to_str()
        .context("UTF-8 transition activation plan path")?;
    let command = [
        "transition",
        "activate",
        &request.namespace_id,
        "--plan",
        path,
    ];
    let result = async {
        ensure!(cli.call(&command, READER).await?.code == 4);
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let reply = cli.call(&command, OWNER).await?;
                if reply.code != 5 {
                    let applied: RaftFleetTransitionActivation =
                        serde_json::from_value(reply.success()?)?;
                    if applied.applied.is_some() {
                        return Ok::<_, anyhow::Error>(applied);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("operator did not confirm the child activation")?
    }
    .await;
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
    let apply_path = root.join("setup.json");
    let mut setup = serde_json::json!({
        "reservation": plan,
        "publication": {
            "request_id": confirmation_id,
            "expected_revision": confirmation_revision,
        },
        "archive": {
            "request_id": confirmation_id,
            "expected_revision": confirmation_revision + 1,
        },
    });
    let apply = [
        "first-group",
        "apply",
        namespace,
        "--plan",
        apply_path.to_str().context("UTF-8 setup path")?,
    ];
    std::fs::write(&apply_path, serde_json::to_vec(&setup)?)?;
    ensure!(cli.call(&apply, OWNER).await?.code == 2);
    ensure!(cli.revision().await? == plan.expected_revision);
    setup["archive"]["request_id"] = serde_json::Value::String(archive_id.clone());
    std::fs::write(&apply_path, serde_json::to_vec(&setup)?)?;
    ensure!(cli.call(&archive, READER).await?.code == 4);
    ensure!(cli.call(&archive, OWNER).await?.code == 3);
    let denied = cli.call(&reserve, READER).await?;
    ensure!(denied.code == 4);
    let early = cli.call(&publish, OWNER).await?;
    ensure!(early.code == 3);
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == plan.expected_revision);
    ensure!(cli.revision().await? == plan.expected_revision);

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()?;
    let foreign_namespace = hex::encode([77; 16]);
    for (fleet, namespace, status) in [
        (
            "foreign-fleet",
            namespace.as_str(),
            reqwest::StatusCode::FORBIDDEN,
        ),
        (
            FLEET,
            foreign_namespace.as_str(),
            reqwest::StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = client
            .post(format!(
                "{url}/api/v1/fleets/{fleet}/raft/namespaces/{namespace}/first-group/reserve"
            ))
            .bearer_auth(OWNER)
            .json(&plan)
            .send()
            .await?;
        ensure!(
            response.status() == status,
            "foreign fleet or namespace route"
        );
    }
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == plan.expected_revision);

    let reservation = cli.call(&reserve, OWNER).await?.success()?;
    ensure!(reservation["stage"] == "reserved");
    ensure!(cli.call(&reserve, OWNER).await?.success()? == reservation);
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == confirmation_revision);

    let mut forged_plan = plan.clone();
    forged_plan.group_id = hex::encode([125; 16]);
    let forged = client
        .post(format!(
            "{url}/api/v1/fleets/{FLEET}/raft/namespaces/{namespace}/first-group/publish"
        ))
        .bearer_auth(OWNER)
        .json(&RaftFleetFirstGroupPublish {
            plan: forged_plan,
            request_id: hex::encode([126; 16]),
            expected_revision: confirmation_revision,
        })
        .send()
        .await?;
    ensure!(
        forged.status() == reqwest::StatusCode::CONFLICT,
        "forged group confirmation: {}",
        forged.status()
    );
    let failure = forged.json::<serde_json::Value>().await?;
    ensure!(failure["error"]["code"] == "conflict");
    ensure!(cli.call(&reserve, OWNER).await?.success()? == reservation);

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
    let lost = cli.call(&apply, OWNER).await?;
    ensure!(lost.code == 5 && lost.value["error"]["outcome"] == "unknown");
    ensure!(lost.value["error"]["message"]
        .as_str()
        .is_some_and(|message| message.starts_with("First group publish stage.")));
    ensure!(!f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let selected = cli.call(&publish, OWNER).await?.success()?;
    ensure!(selected["stage"] == "published");
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == confirmation_revision + 1);

    // Lose the fleet CAS reply and its first confirmation read after the
    // archive intent commits. The active initializer must reconcile it.
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let archive_receipt = cli.call(&archive, OWNER).await?.success()?;
    ensure!(!f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    ensure!(!f.store.fail_fleet_read.load(Ordering::Acquire));
    ensure!(archive_receipt["stage"] == "archive_initialized");
    let writes = f.store.registry_writes.load(Ordering::Acquire);
    ensure!(cli.call(&archive, OWNER).await?.success()? == archive_receipt);
    let completed_revision = cli.revision().await?;
    let replay = cli.call(&apply, OWNER).await?.success()?;
    ensure!(
        replay["reservation"] == reservation
            && replay["publication"] == selected
            && replay["archive"] == archive_receipt
    );
    ensure!(cli.revision().await? == completed_revision);
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
