// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// The external admin listener, CLI processes and credential files are fixture
// I/O outside every Node execution boundary. Node work retains its own domain.
#![allow(clippy::disallowed_methods)]
use super::*;
use cellar::sdk::fleet_operations::*;
use cellar_admin::{AuditStore, Console, Fleet, FleetInfo, Permission, Principal};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tokio::sync::oneshot;

const OWNER: &str = "fixture-owner-bearer-token-with-at-least-32-bytes";
const READER: &str = "fixture-reader-bearer-token-with-at-least-32-bytes";
const CANCELLER: &str = "fixture-canceller-token-with-at-least-32-bytes";

struct Server {
    domain: Arc<ExecutionDomain>,
    control: RaftFleetControl,
    root: tempfile::TempDir,
    url: String,
    stop: Option<oneshot::Sender<()>>,
    joined: Option<Pin<Box<dyn Future<Output = Result<()>> + Send>>>,
}

impl Server {
    async fn new(f: &Scenario) -> Result<Self> {
        let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
        let root = tempfile::tempdir()?;
        let bucket = cellar::bucket::Bucket::from_clients(
            f.s.f.store.clone(),
            f.s.f.store.clone(),
            Arc::new(NoListing),
            cellar::bucket::StorageBackend::S3,
            "enrollment".into(),
            "tenant/".into(),
        )?;
        let cfg = f.s.namespace.config();
        let connection = RaftFleetConnection {
            cluster_id: hex::encode(cfg.cluster.bytes()),
            incarnation_id: hex::encode([80; 16]),
            coordination_prefix: "hosts/".into(),
            namespaces: vec![RaftFleetNamespace {
                namespace_id: hex::encode(cfg.namespace.bytes()),
                schema_digest: hex::encode(cfg.schema_digest),
                catalog_prefix: "host-enrollment/".into(),
                scope: RaftFleetNamespaceScope::StoreRoot,
                managed: cfg.managed_fleet.is_some(),
            }],
        };
        let control = domain
            .run(RaftFleetControl::connect(
                &domain,
                bucket.clone(),
                connection,
                TIMEOUT,
            ))
            .await??;
        for (name, value) in [
            ("owner", OWNER),
            ("reader", READER),
            ("canceller", CANCELLER),
        ] {
            std::fs::write(root.path().join(name), value)?;
        }
        let principals = [
            (
                "owner",
                OWNER,
                vec![Permission::MetadataRead, Permission::FleetTopology],
            ),
            (
                "reader",
                READER,
                vec![Permission::MetadataRead, Permission::Operate],
            ),
            (
                "canceller",
                CANCELLER,
                vec![Permission::MetadataRead, Permission::FleetCancel],
            ),
        ]
        .into_iter()
        .map(|(id, token, grants)| {
            Principal::new(id.into(), token, BTreeMap::from([("test".into(), grants)]))
        })
        .collect::<Result<Vec<_>>>()?;
        let router = cellar_admin::router(Console {
            domain: domain.clone(),
            fleets: BTreeMap::from([(
                "test".into(),
                Fleet {
                    info: FleetInfo {
                        id: "test".into(),
                        name: "test".into(),
                        environment: "fixture".into(),
                    },
                    client: cellar::management::FleetClient::new(bucket, false)
                        .with_raft_fleet(control.clone()),
                },
            )]),
            principals,
            audit: AuditStore::open(&root.path().join("audit.sqlite"))?,
            assets: root.path().join("assets"),
            origins: vec![],
        })?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = oneshot::channel();
        let server = domain.spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        })?;
        Ok(Self {
            domain,
            control,
            root,
            url,
            stop: Some(stop),
            joined: Some(Box::pin(async move {
                server.await??;
                Ok(())
            })),
        })
    }

    fn begin_args(&self, request: &RaftFleetBegin, file: &str) -> Result<Vec<String>> {
        let path = self.root.path().join(file);
        std::fs::write(&path, serde_json::to_vec(&request.policy)?)?;
        Ok(vec![
            "fleet".into(),
            "rebalance".into(),
            "--operation-id".into(),
            request.operation_id.clone(),
            "--expected-revision".into(),
            request.expected_revision.to_string(),
            "--keep".into(),
            request.keep.join(","),
            "--policy".into(),
            path.to_string_lossy().into_owned(),
        ])
    }

    async fn cli(&self, args: &[String], credential: &str) -> Result<(i32, Value)> {
        self.cli_for_catalog(args, credential, self.control.catalog_id())
            .await
    }

    async fn cli_for_catalog(
        &self,
        args: &[String],
        credential: &str,
        catalog_id: &str,
    ) -> Result<(i32, Value)> {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_cellar"))
            .arg("raft")
            .args(args)
            .args([
                "--fleet",
                "test",
                "--admin-url",
                &self.url,
                "--catalog-id",
                catalog_id,
                "--token-file",
            ])
            .arg(self.root.path().join(credential))
            .arg("--json")
            .kill_on_drop(true)
            .output()
            .await?;
        for token in [OWNER, READER, CANCELLER] {
            ensure!(!String::from_utf8_lossy(&output.stdout).contains(token));
            ensure!(!String::from_utf8_lossy(&output.stderr).contains(token));
        }
        let value = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("CLI output: {}", String::from_utf8_lossy(&output.stderr)))?;
        Ok((
            output.status.code().context("CLI terminated by signal")?,
            value,
        ))
    }

    async fn close(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(joined) = self.joined.take() {
            joined.await?;
        }
        self.domain.shutdown().await;
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

async fn request(f: &Scenario, operation: u8) -> Result<RaftFleetBegin> {
    let desired = f.request().await?;
    Ok(RaftFleetBegin {
        catalog_id: RaftFleetControl::new(
            &f.s.f.domain,
            f.s.f.catalog.clone(),
            vec![f.s.namespace.clone()],
        )?
        .catalog_id()
        .into(),
        operation_id: hex::encode([operation; 16]),
        expected_revision: f.s.f.catalog.read(TIMEOUT).await?.revision(),
        keep: desired
            .keep
            .iter()
            .rev()
            .map(|n| hex::encode(n.bytes()))
            .collect(),
        policy: desired.policy,
    })
}

fn op_args(command: &str, id: &str) -> Vec<String> {
    vec![
        "fleet".into(),
        "operation".into(),
        command.into(),
        id.into(),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_cli_admin_converges_and_reconciles_unknown_admission() -> Result<()> {
    Box::pin(converge()).await
}

async fn converge() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let server = Server::new(&f).await?;
    let begin = request(&f, 160).await?;
    let args = server.begin_args(&begin, "policy.json")?;
    let revision = begin.expected_revision;
    let mut wrong_catalog = begin.clone();
    wrong_catalog.catalog_id = hex::encode([99; 32]);
    let rejected = reqwest::Client::new()
        .post(format!("{}/api/v1/fleets/test/raft/operations", server.url))
        .bearer_auth(OWNER)
        .json(&wrong_catalog)
        .send()
        .await?;
    ensure!(rejected.status() == reqwest::StatusCode::CONFLICT);
    let rejected: Value = rejected.json().await?;
    ensure!(rejected["error"]["outcome"] == "not_submitted");
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, failure) = server.cli(&args, "owner").await?;
    ensure!(
        exit == 5 && failure["error"]["outcome"] == "unknown",
        "{exit}: {failure}"
    );
    let committed = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(committed.revision() == revision + 1);
    let (exit, accepted) = server.cli(&args, "owner").await?;
    ensure!(
        exit == 0 && accepted["reconciled"] == true && accepted["operation"]["phase"] == "running",
        "{exit}: {accepted}"
    );
    ensure!(
        accepted["operation"]["principal"] == "owner"
            && accepted["operation"]["converged"] == false
    );
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == committed.revision());
    let (exit, denied) = server.cli(&args, "reader").await?;
    ensure!(exit == 4 && denied["error"]["code"] == "forbidden");
    let mut changed = begin.clone();
    changed.policy.revision += 1;
    let changed_args = server.begin_args(&changed, "changed.json")?;
    let (exit, conflict) = server.cli(&changed_args, "owner").await?;
    ensure!(exit == 3 && conflict["error"]["code"] == "payload_conflict");
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == committed.revision());
    let forged = reqwest::Client::new().post(format!("{}/api/v1/fleets/test/raft/operations", server.url)).bearer_auth(OWNER)
        .json(&json!({"catalog_id":begin.catalog_id,"operation_id":begin.operation_id,"expected_revision":revision,"keep":begin.keep,"policy":begin.policy,"principal":"forged"})).send().await?;
    ensure!(forged.status() == reqwest::StatusCode::BAD_REQUEST);
    let evidence = Arc::new(Inputs::new(&f).await?);
    let service =
        f.s.f
            .catalog
            .start_topology_service_with_collection(
                f.s.nodes[0].host.clone(),
                evidence.clone(),
                evidence,
                options(),
                FleetTopologyCollectionOptions::default(),
            )
            .await?;
    let mut wait_args = op_args("wait", &begin.operation_id);
    wait_args.extend(["--timeout-seconds".into(), "40".into()]);
    let (exit, done) = server.cli(&wait_args, "reader").await?;
    ensure!(
        exit == 0
            && done["phase"] == "complete"
            && done["converged"] == true
            && done["admitted_movements"] == 2,
        "{exit}: {done}"
    );
    service.shutdown().await;
    completed_sql(&mut f, 160).await?;
    let (exit, mismatch) = server
        .cli_for_catalog(
            &op_args("status", &begin.operation_id),
            "reader",
            &hex::encode([99; 32]),
        )
        .await?;
    ensure!(exit == 3 && mismatch["error"]["code"] == "conflict");
    let status = server.control.status(TIMEOUT).await?;
    ensure!(
        status.active.is_none()
            && status
                .placement
                .as_ref()
                .is_some_and(|p| p.phase == FleetTopologyPhase::Complete)
    );
    let mut cursor: Option<String> = None;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        let mut args = op_args("progress", &begin.operation_id);
        args.extend(["--limit".into(), "1".into()]);
        if let Some(value) = &cursor {
            args.extend(["--cursor".into(), value.clone()]);
        }
        let (exit, value) = server.cli(&args, "reader").await?;
        ensure!(exit == 0, "{exit}: {value}");
        let page: RaftFleetMovements = serde_json::from_value(value)?;
        for movement in page.entries {
            ensure!(seen.insert(movement.reservation));
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    ensure!(seen.len() == 2);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let foreign =
        f.s.f
            .catalog
            .page(&cut, FleetTable::Policy, None, 1, TIMEOUT)
            .await?
            .next
            .context("policy cursor")?
            .to_token()?;
    let mut args = op_args("progress", &begin.operation_id);
    args.extend(["--cursor".into(), foreign]);
    let (exit, rejected) = server.cli(&args, "reader").await?;
    ensure!(exit == 3 && rejected["error"]["code"] == "conflict");
    let (exit, retry) = server
        .cli(&server.begin_args(&begin, "policy.json")?, "owner")
        .await?;
    ensure!(exit == 0 && retry["operation"]["phase"] == "complete");
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    server.close().await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_cli_admin_partial_cancel_and_wait_timeout() -> Result<()> {
    Box::pin(cancel()).await
}

async fn cancel() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let server = Server::new(&f).await?;
    let begin = request(&f, 160).await?;
    let args = server.begin_args(&begin, "policy.json")?;
    ensure!(server.cli(&args, "owner").await?.0 == 0);
    let before = f.s.f.catalog.read(TIMEOUT).await?;
    let timeout = server
        .control
        .wait(
            &begin.operation_id,
            Duration::from_millis(80),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
    ensure!(timeout.code == RaftFleetFailureCode::WaitTimeout);
    let mut waiting = op_args("wait", &begin.operation_id);
    waiting.extend(["--timeout-seconds".into(), "1".into()]);
    let (exit, timed_out) = server.cli(&waiting, "reader").await?;
    ensure!(exit == 6 && timed_out["error"]["code"] == "wait_timeout");
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == before.revision());
    let current =
        f.s.f
            .catalog
            .active_topology_operation(&before, TIMEOUT)
            .await?
            .context("active operation")?;
    let intent = FleetTopologyIntentRequest {
        identity: current.identity,
        keep: current.keep,
        policy: current.policy,
    };
    let plan = preview(&f, &intent).await?;
    let identity = identity(164, 160, plan.inventory_revision());
    let movement = plan
        .first_movement(identity.clone())?
        .context("first move")?;
    f.s.f
        .catalog
        .reserve_topology_movement(
            &plan,
            identity,
            &f.s.nodes[0].host,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await?
        .context("admitted move")?;
    let target = destination(&f, &movement)?;
    apply_move(&mut f, &movement, &target).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let mut cancel_args = op_args("cancel", &begin.operation_id);
    cancel_args.extend([
        "--operation-id".into(),
        hex::encode([165; 16]),
        "--expected-revision".into(),
        cut.revision().to_string(),
    ]);
    ensure!(server.cli(&cancel_args, "owner").await?.0 == 4);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, unknown) = server.cli(&cancel_args, "canceller").await?;
    ensure!(exit == 5 && unknown["error"]["outcome"] == "unknown");
    let (exit, accepted) = server.cli(&cancel_args, "canceller").await?;
    ensure!(
        exit == 0
            && accepted["operation"]["phase"] == "cancelling"
            && accepted["reconciled"] == true
    );
    let evidence = Arc::new(Inputs::new(&f).await?);
    let service =
        f.s.f
            .catalog
            .start_topology_service(
                f.s.nodes[0].host.clone(),
                evidence.clone(),
                evidence,
                options(),
            )
            .await?;
    let done = server
        .control
        .wait(&begin.operation_id, TIMEOUT * 4, Duration::from_millis(50))
        .await?;
    ensure!(
        done.phase == FleetTopologyPhase::Cancelled
            && !done.converged
            && done.terminal
            && done.admitted_movements == 1
    );
    service.shutdown().await;
    cancel_args.extend(["--wait".into(), "--timeout-seconds".into(), "5".into()]);
    let (exit, cancelled) = server.cli(&cancel_args, "canceller").await?;
    ensure!(exit == 0 && cancelled["phase"] == "cancelled" && cancelled["converged"] == false);
    ensure!(server.cli(&waiting, "reader").await?.0 == 4);
    let status = server.control.status(TIMEOUT).await?;
    ensure!(status.active.is_none() && status.placement.is_none());
    let page = server
        .control
        .movements(&begin.operation_id, None, 128, TIMEOUT)
        .await?;
    ensure!(
        page.entries.len() == 1
            && page.entries[0].reservation == hex::encode(movement.resources.identity.id)
    );
    for (index, live) in f.live.iter().enumerate() {
        let cells = live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>();
        if f.s.proposals[index].scope == movement.resources.group {
            write_read(&f.s.f, &cells, 3, 9, 21).await?;
        } else {
            write_read(&f.s.f, &cells, 2, 5, 12).await?;
        }
    }
    let audit = reqwest::Client::new()
        .get(format!("{}/api/v1/fleets/test/operations", server.url))
        .bearer_auth(READER)
        .send()
        .await?
        .json::<Value>()
        .await?;
    ensure!(
        audit.to_string().contains("raft_topology_cancel")
            && audit.to_string().contains("canceller")
    );
    server.close().await?;
    f.close().await
}
