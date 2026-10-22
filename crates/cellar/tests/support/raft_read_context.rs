//! Capture a live ordinary invocation, then probe its capabilities from a Raft
//! query on the same execution domain. Retirement is not the refusal oracle.
use super::public_node;
use anyhow::{ensure, Result};
use cellar::sdk::{consensus::*, *};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::Semaphore;

struct Shared {
    captured: Mutex<Option<(Context, Storage)>>,
    active: AtomicBool,
    background_polled: Arc<AtomicBool>,
    entered: Semaphore,
    release: Semaphore,
}

struct Release(Arc<Shared>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release.add_permits(1);
    }
}

struct Ordinary(Arc<Shared>);

impl Cell for Ordinary {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(_) = event else {
                return Ok(Output::Done);
            };
            let storage = context.storage()?;
            storage.put_json("value", &serde_json::json!(1))?;
            *self.0.captured.lock().unwrap() = Some((context.clone(), storage.clone()));
            self.0.active.store(true, Ordering::Release);
            self.0.entered.add_permits(1);
            self.0.release.acquire().await?.forget();
            // The original capability remains usable when its own invocation
            // resumes, and none of the foreign attempts changed its state.
            ensure!(storage.get_json("value")? == Some(serde_json::json!(1)));
            let (_, rows, _) = storage.sql(
                "SELECT count(*) FROM sqlite_master WHERE name='escaped'",
                &[],
            )?;
            ensure!(rows[0][0] == 0);
            self.0.active.store(false, Ordering::Release);
            Ok(Output::Http(
                http::Response::from_json(&serde_json::json!({"value": 1}))?.into_http(&context)?,
            ))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn active_ordinary_context_and_storage_are_refused() -> Result<()> {
    let shared = Arc::new(Shared {
        captured: Mutex::new(None),
        active: AtomicBool::new(false),
        background_polled: Arc::new(AtomicBool::new(false)),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let _release = Release(shared.clone());
    let probe = shared.clone();
    let class = RaftClass::new("ReadFence", 1, [41; 32])?
        .command("Init", [42; 32], |context, _| {
            context.sql().execute_batch(
                "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1)",
            )?;
            Ok(vec![1])
        })?
        .query("Probe", [43; 32], move |context, _| {
            ensure!(
                probe.active.load(Ordering::Acquire),
                "ordinary invocation retired before probe"
            );
            let captured = probe.captured.lock().unwrap();
            let (ordinary, storage) = captured
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no captured invocation"))?;
            assert!(ordinary.storage().is_err());
            assert!(ordinary.exported_namespace("Ordinary").is_err());
            assert!(ordinary.namespace("unused").is_err());
            let polled = probe.background_polled.clone();
            assert!(ordinary
                .wait_until(async move {
                    polled.store(true, Ordering::Release);
                    Ok(())
                })
                .is_err());
            assert!(storage.get_json("value").is_err());
            assert!(storage.put_json("value", &serde_json::json!(999)).is_err());
            assert!(storage
                .sql("CREATE TABLE escaped(value INTEGER)", &[])
                .is_err());
            assert!(storage.set_alarm(0).is_err());
            let value: i64 =
                context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            ensure!(value == 1 && probe.active.load(Ordering::Acquire));
            Ok(vec![1])
        })?;
    let schema = class.schema_digest();
    let init = class.contract("Init").unwrap();
    let query = class.contract("Probe").unwrap();
    let ordinary = shared.clone();
    let application: Arc<dyn Application> = Arc::new(
        Registry::new(|context, event| {
            Box::pin(async move {
                let Invocation::Fetch(request) = event else {
                    return Ok(Output::Done);
                };
                let cell = context
                    .exported_namespace("Ordinary")?
                    .get_by_name("capture");
                Ok(Output::Http(cell.fetch(&context, request).await?))
            })
        })
        .cell("Ordinary", move |_, _| {
            Ok(Arc::new(Ordinary(ordinary.clone())))
        })?
        .raft_class(class)?,
    );
    let root = tempfile::tempdir()?;
    let node = public_node::node_declared(
        root.path(),
        application.clone(),
        "raft-host",
        vec!["Ordinary".into()],
    )
    .await?;
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "ReadFence:one".into(),
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let mut namespace =
        NamespaceConfig::new(scope.cluster, scope.namespace, NamespaceMode::Raft, schema);
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    let service = public_node::cells(
        &node,
        application,
        Arc::new(object_store::memory::InMemory::new()),
        RaftCellRegistration {
            placement: cellar::sdk::consensus::RaftCellPlacement::Replica,
            namespace,
            class: "ReadFence".into(),
            birth: BirthProposal {
                scope: scope.clone(),
                voters: vec![InitialMember {
                    node: NodeId::new([1; 16])?,
                    member: MemberId::new(1)?,
                }],
                learners: vec![],
            },
        },
        false,
    )
    .await?;
    let cell = service.cell(scope.namespace, &scope.cell)?;
    public_node::ready(&cell).await?;
    cell.command(init, vec![], public_node::options(1)?).await?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;
    let address = node.info().public_address;
    let request =
        tokio::spawn(async move { client.get(format!("http://{address}/hold")).send().await });
    tokio::time::timeout(Duration::from_secs(2), shared.entered.acquire())
        .await??
        .forget();
    let result = cell
        .query(
            query,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(result.result, vec![1]);
    assert!(shared.active.load(Ordering::Acquire));
    assert!(!shared.background_polled.load(Ordering::Acquire));
    shared.release.add_permits(1);
    let response = request.await??;
    assert!(response.status().is_success());
    assert_eq!(response.json::<serde_json::Value>().await?["value"], 1);
    assert!(!shared.active.load(Ordering::Acquire));
    assert!(!shared.background_polled.load(Ordering::Acquire));
    assert!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}
