//! Host integration starts with immutable catalog and initial-member admission.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_cluster.rs"]
mod cluster;
#[path = "../../../examples/raft-counter/counter.rs"]
mod counter_example;
#[path = "support/raft_node.rs"]
mod public_node;
#[path = "support/raft_catalog_fault.rs"]
mod store;

use anyhow::{bail, Context as _, Result};
use cellar::application::consensus::verification::*;
use cellar::sdk::{Application, ExecutionDomain, Output, RaftClass, RegistrationError, Registry};
use cellar_logic::consensus::*;
use cellar_logic::NamespaceMode;
use cellar_ltx::DirectFileSystem;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::path::Path;
use std::sync::{atomic::Ordering, Arc};

fn registered_counter() -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [7; 32])?
        .command("Increment", [8; 32], |_, _| Ok(vec![]))?
        .query("Get", [9; 32], |_, _| Ok(vec![]))?)
}

#[test]
fn command_manifest_binds_voting_compatibility_without_query_requirements() -> Result<()> {
    let base = || RaftClass::new("Counter", 1, [7; 32]);
    let complete = registered_counter()?;
    let commands = base()?.command("Increment", [8; 32], |_, _| Ok(vec![]))?;
    assert_eq!(complete.schema_digest(), commands.schema_digest());
    assert_eq!(
        complete.schema_digest(),
        commands
            .query("Other", [10; 32], |_, _| Ok(vec![]))?
            .schema_digest()
    );
    assert_ne!(complete.schema_digest(), base()?.schema_digest());
    assert_ne!(
        complete.schema_digest(),
        base()?
            .command("Increment", [9; 32], |_, _| Ok(vec![]))?
            .schema_digest()
    );
    assert_ne!(
        complete.schema_digest(),
        base()?
            .command("Different", [8; 32], |_, _| Ok(vec![]))?
            .schema_digest()
    );
    let first =
        base()?
            .command("A", [11; 32], |_, _| Ok(vec![]))?
            .command("B", [12; 32], |_, _| Ok(vec![]))?;
    let second =
        base()?
            .command("B", [12; 32], |_, _| Ok(vec![]))?
            .command("A", [11; 32], |_, _| Ok(vec![]))?;
    assert_eq!(first.schema_digest(), second.schema_digest());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_mode_admission_refuses_conflicts_before_mutable_execution() -> Result<()> {
    use cellar::sdk::{consensus::*, ShutdownMode};
    use futures_util::TryStreamExt;
    let root = tempfile::tempdir()?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_class(registered_counter()?)?,
    );
    let denied = public_node::node_declared(
        root.path(),
        application.clone(),
        "raft-host",
        vec!["Counter".into()],
    )
    .await;
    let error = denied
        .err()
        .ok_or_else(|| anyhow::anyhow!("Raft class admitted by Fleet startup"))?;
    assert!(matches!(
        error.downcast_ref::<RegistrationError>(),
        Some(RegistrationError::WrongMode {
            mode: NamespaceMode::Fleet,
            ..
        })
    ));
    let node = public_node::node(root.path(), application.clone()).await?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = registered_counter()?.schema_digest();
    let registration = RaftCellRegistration {
        namespace,
        class: "Counter".into(),
        birth: proposal(2, "Counter:one", 5, 1),
    };
    // An implementation missing a required command cannot vote under the
    // original application root, even with its name/version/definition repeated.
    let incompatible = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_class(RaftClass::new("Counter", 1, [7; 32])?)?,
    );
    assert!(public_node::cells(
        &node,
        incompatible,
        store.clone(),
        registration.clone(),
        false
    )
    .await
    .is_err());
    assert!(store.list(None).try_collect::<Vec<_>>().await?.is_empty());
    assert!(!node.info().data_dir.join("raft").exists());
    let called = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = called.clone();
    let mutable = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).cell(
            "Counter",
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                bail!("mutable constructor called")
            },
        )?,
    );
    let error = public_node::cells(&node, mutable, store.clone(), registration.clone(), false)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("mutable registration admitted by Raft startup"))?;
    assert!(matches!(
        error.downcast_ref::<RegistrationError>(),
        Some(RegistrationError::UnsupportedFactory(_))
    ));
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(store.list(None).try_collect::<Vec<_>>().await?.is_empty());
    assert!(!node.info().data_dir.join("raft").exists());
    let catalog = Catalog::new(
        store.clone(),
        CatalogOptions {
            prefix: "host/test".into(),
            ..Default::default()
        },
    )?;
    let mut fleet = registration.namespace.clone();
    fleet.mode = NamespaceMode::Fleet;
    catalog.open_namespace(fleet).await?;
    assert!(
        public_node::cells(&node, application, store, registration, false)
            .await
            .is_err()
    );
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(!node.info().data_dir.join("raft").exists());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn submitted_partition_retains_producer_until_follower_barrier() -> Result<()> {
    use cellar::sdk::consensus::*;
    use std::{sync::atomic::AtomicUsize, time::Duration};
    for retain_entry in [false, true] {
        let calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let (gate, _entered) = public_node::Gate::new();
        gate.release();
        let definitions = calls
            .iter()
            .map(|calls| public_node::counter(calls.clone(), gate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let mut namespace = config(2, NamespaceMode::Raft, false);
        namespace.schema_digest = definitions[0].schema;
        let cluster = cluster::Cluster::start(
            &definitions,
            vec![RaftCellRegistration {
                namespace,
                class: "Counter".into(),
                birth: proposal(2, "Counter:one", 5, 3),
            }],
            200,
        )
        .await?;
        let handles = cluster
            .services
            .iter()
            .map(|service| service.cell(NamespaceId::new([2; 16]).unwrap(), "Counter:one"))
            .collect::<Result<Vec<_>, _>>()?;

        async fn leader(handles: &[RaftCellHandle], excluded: Option<usize>) -> Result<usize> {
            Ok(tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    for (i, handle) in handles.iter().enumerate() {
                        if Some(i) != excluded
                            && handle
                                .status()
                                .await
                                .is_ok_and(|status| status.command_ready)
                        {
                            return i;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?)
        }

        async fn reads(
            handles: &[RaftCellHandle],
            contract: &OperationContract,
            expected: i64,
        ) -> Result<()> {
            for handle in handles {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if let Ok(read) = handle
                            .query(contract.clone(), vec![], QueryOptions::default())
                            .await
                        {
                            if read.result == expected.to_be_bytes() {
                                return;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
            }
            Ok(())
        }

        async fn status(handle: &RaftCellHandle) -> Result<CellStatus> {
            Ok(tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match handle.status().await {
                        Err(CommandFailure::Overloaded) => {
                            tokio::time::sleep(Duration::from_millis(5)).await
                        }
                        result => return result,
                    }
                }
            })
            .await??)
        }
        let primary = leader(&handles, None).await?;
        let receipt = handles[primary]
            .command(
                definitions[primary].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::options(1)?,
            )
            .await?;
        assert_eq!(receipt.result, 1_i64.to_be_bytes());
        reads(&handles, &definitions[0].get, 1).await?;
        cluster.isolate(primary, retain_entry).await?;
        let mut options = public_node::options(2)?;
        options.timeout = Duration::from_millis(500);
        let id = options.id;
        let pending = handles[primary].command(
            definitions[primary].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            options,
        );
        assert_eq!(
            pending.await,
            Err(CommandFailure::Ambiguous { command: id })
        );
        let isolated = status(&handles[primary])
            .await
            .context("isolated leader status")?;
        assert!(
            isolated.persisted > receipt.version.position.index,
            "no persisted submitted suffix; data reconnects {}",
            cluster.data_attempts(primary)
        );
        assert_eq!(isolated.committed, receipt.version.position.index);
        assert_eq!(isolated.term, receipt.version.position.term);
        if retain_entry {
            for (i, handle) in handles.iter().enumerate() {
                if i != primary {
                    let follower = status(handle)
                        .await
                        .context("unacknowledged follower status")?;
                    assert_eq!(follower.persisted, isolated.persisted);
                    assert_eq!(follower.committed, receipt.version.position.index);
                }
            }
        }
        assert_eq!(calls[primary].load(Ordering::SeqCst), 2);
        assert!(
            cluster.nodes[primary]
                .services()
                .application_work()
                .pending_events
                > 0
        );
        assert!(matches!(
            handles[primary]
                .command(
                    definitions[primary].increment.clone(),
                    1_i64.to_be_bytes().to_vec(),
                    public_node::options(3)?
                )
                .await,
            Err(CommandFailure::Overloaded)
        ));
        reads(&handles, &definitions[0].get, 1).await?;
        let replacement = leader(&handles, Some(primary)).await?;
        assert!(status(&handles[replacement]).await?.term > receipt.version.position.term);
        // S3/catalog access remains available, but it cannot grant an isolated
        // minority mutation authority or dispose of its unresolved producer.
        assert!(
            cluster.nodes[primary]
                .services()
                .application_work()
                .pending_events
                > 0
        );
        if retain_entry {
            let majority: Vec<_> = handles
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != primary)
                .map(|(_, handle)| handle.clone())
                .collect();
            reads(&majority, &definitions[0].get, 2).await?;
        }
        cluster.heal();
        tokio::time::timeout(Duration::from_secs(15), async {
            while cluster.nodes[primary]
                .services()
                .application_work()
                .pending_events
                != 0
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let old = status(&handles[primary])
            .await
            .context("healed follower status")?;
        assert_eq!(old.leader, Some(MemberId::new(replacement as u64 + 1)?));
        assert!(old.term > receipt.version.position.term);
        assert!(!old.command_ready);
        reads(
            &handles,
            &definitions[0].get,
            if retain_entry { 2 } else { 1 },
        )
        .await?;
        let retried = handles[replacement]
            .command(
                definitions[replacement].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::options(2)?,
            )
            .await?;
        assert_eq!(retried.result, 2_i64.to_be_bytes());
        reads(&handles, &definitions[0].get, 2).await?;
        assert_eq!(
            calls
                .iter()
                .map(|calls| calls.load(Ordering::SeqCst))
                .sum::<usize>(),
            if retain_entry { 2 } else { 3 }
        );
        cluster.close().await?;
    }
    Ok(())
}

#[test]
fn fleet_gate_unchanged() {
    use cellar_logic::{
        on_event, CasOutcome, Channel, Config, Effect, Event, Failure, OwnerRecord,
        OwnershipOnEvict, Phase, ProofSource, RequestError, RestoreOutcome, State,
    };
    for source in [ProofSource::Fleet, ProofSource::Bucket] {
        for (proof, ownership) in [(9, true), (8, true), (9, false)] {
            let config = Config {
                max_resident: 8,
                max_cached_routes: 0,
                max_activations: 8,
                max_evictions: 4,
                max_releases: 4,
                max_outbound_websockets: 8,
                ownership_on_evict: OwnershipOnEvict::Sticky,
                require_node_lease: false,
                peer_protocol: 1,
                operation_deadline_ms: Some(3000),
                owner_log_recovery_backoff_ms: 50,
                owner_log_recovery_attempts: 3,
                alarm_resident_ms: 100,
                idle_evict_ms: None,
                pressure: Default::default(),
            };
            let mut state = State::new("fleet", config);
            let cell = "Counter:one";
            let op = on_event(
                &mut state,
                Event::Request {
                    request: 1,
                    cell: cell.into(),
                },
            )
            .into_iter()
            .find_map(|effect| match effect {
                Effect::ReadOwner { op, .. } => Some(op),
                _ => None,
            })
            .unwrap();
            on_event(
                &mut state,
                Event::OwnerRead {
                    op,
                    now_ms: 0,
                    result: Ok(Some(OwnerRecord {
                        lineage: None,
                        node: Some("fleet".into()),
                        epoch: 7,
                        etag: "owner".into(),
                    })),
                },
            );
            for _ in 0..4 {
                let event = match state.phase(cell).cloned().unwrap() {
                    Phase::Acquiring { op, .. } => Event::OwnerCasCompleted {
                        op,
                        result: Ok(CasOutcome::Applied),
                    },
                    Phase::Restoring { op, .. } => Event::RestoreCompleted {
                        op,
                        result: Ok(RestoreOutcome {
                            restored: true,
                            alarm: None,
                        }),
                    },
                    Phase::Starting { op, .. } => Event::RuntimeStarted {
                        op,
                        isolate: Some(cellar_logic::isolate::HeapId::new(1)),
                        generation: 1,
                        result: Ok(()),
                    },
                    Phase::Publishing { op, .. } => Event::Published { op, result: Ok(()) },
                    phase => panic!("unexpected activation {phase:?}"),
                };
                on_event(&mut state, event);
            }
            let held = on_event(
                &mut state,
                Event::Output {
                    request: 1,
                    channel: Channel::Response,
                    position: Some(9),
                    observed: None,
                    epoch: Some(8),
                },
            );
            assert!(!held
                .iter()
                .any(|effect| matches!(effect, Effect::Release { .. })));
            let op = held
                .into_iter()
                .find_map(|effect| match effect {
                    Effect::AwaitDurable { op, .. } => Some(op),
                    _ => None,
                })
                .unwrap();
            let mut effects = on_event(
                &mut state,
                Event::DurableReached {
                    op,
                    result: Ok(proof),
                    source,
                },
            );
            if source == ProofSource::Bucket && proof >= 9 {
                assert!(effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::VerifyOwnership { .. })));
                assert!(!effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::Release { .. })));
                effects = on_event(
                    &mut state,
                    Event::OwnershipVerified {
                        op,
                        result: if ownership {
                            Ok(())
                        } else {
                            Err(Failure::Definite)
                        },
                    },
                );
            } else {
                assert!(!effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::VerifyOwnership { .. })));
            }
            let expected = if proof >= 9 && (source == ProofSource::Fleet || ownership) {
                Ok(())
            } else {
                Err(RequestError::DurabilityUnproven)
            };
            assert!(effects.iter().any(|effect| matches!(effect, Effect::Release { request: 1, result, .. } if *result == expected)));
            assert!(on_event(
                &mut state,
                Event::DurableReached {
                    op,
                    result: Ok(99),
                    source
                }
            )
            .is_empty());
            state.validate().unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unpolled_waiter_cannot_submit_after_its_deadline() -> Result<()> {
    use cellar::sdk::{consensus::*, ShutdownMode};
    use std::{sync::atomic::AtomicUsize, time::Duration};
    let calls = Arc::new(AtomicUsize::new(0));
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let definition = public_node::counter(calls.clone(), gate.clone())?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let service = public_node::cells(
        &node,
        definition.application,
        Arc::new(object_store::memory::InMemory::new()),
        RaftCellRegistration {
            namespace,
            class: "Counter".into(),
            birth: proposal(2, "Counter:one", 5, 1),
        },
        false,
    )
    .await?;
    let cell = service.cell(NamespaceId::new([2; 16])?, "Counter:one")?;
    public_node::ready(&cell).await?;
    cell.command(
        definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        public_node::options(1)?,
    )
    .await?;
    let mut options = public_node::options(2)?;
    options.timeout = Duration::from_millis(100);
    let pending = cell.command(
        definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        options,
    );
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing callback"))?;
    // Do not poll the returned future, including its timeout branch.
    tokio::time::sleep(Duration::from_millis(200)).await;
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while node.services().application_work().pending_events != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(matches!(pending.await, Err(CommandFailure::Unavailable)));
    assert_eq!(
        cell.query(definition.get, vec![], QueryOptions::default())
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    assert_eq!(
        cell.command(
            definition.increment,
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?
        )
        .await?
        .result,
        2_i64.to_be_bytes()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_axum_command_parity() -> Result<()> {
    use cellar::sdk::{consensus::*, ShutdownMode};
    let root = tempfile::tempdir()?;
    let class = counter_example::definition()?;
    let schema = class.schema_digest();
    let namespace = NamespaceId::new([2; 16])?;
    let secret = "counter-test-authorization";
    let api = counter_example::CounterApi::new(secret.into(), namespace, &class)?;
    let application = counter_example::application(api.clone(), class)?;
    let node = public_node::node(root.path(), application.clone()).await?;
    let mut namespace_config = config(2, NamespaceMode::Raft, true);
    namespace_config.schema_digest = schema;
    let service = public_node::cells(
        &node,
        application,
        Arc::new(object_store::memory::InMemory::new()),
        RaftCellRegistration {
            namespace: namespace_config,
            class: "Counter".into(),
            birth: proposal(2, "Counter:one", 5, 1),
        },
        false,
    )
    .await?;
    *api.cells.lock().unwrap() = Some(service.clone());
    let cell = service.cell(namespace, "Counter:one")?;
    public_node::ready(&cell).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let router = counter_example::router(api.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let urls = [
        format!("http://{}/counters/one", node.info().public_address),
        format!("http://{address}/counters/one"),
    ];
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let options = public_node::options(1)?;
    let original = cell
        .command(
            api.increment.clone(),
            7_i64.to_be_bytes().to_vec(),
            options.clone(),
        )
        .await?;
    let command_id = format!("{}:1", hex::encode([7; 16]));
    for url in &urls {
        let response = client
            .post(format!("{url}?delta=7"))
            .bearer_auth(secret)
            .header("x-command-id", &command_id)
            .header("x-retry-until-ms", options.retry_until_ms)
            .send()
            .await?;
        assert_eq!(response.status(), 200);
        let receipt: serde_json::Value = response.json().await?;
        assert_eq!(receipt["value"], 7);
        assert_eq!(receipt["index"], original.version.position.index.get());
        let token = receipt["version_token"].as_str().unwrap();
        VersionToken::from_encoded(token)?;
        assert!(receipt["served_by"].is_null());
        for consistency in ["stale", "at-least", "linearizable"] {
            for routed in [false, true] {
                let mut request = client
                    .get(url)
                    .bearer_auth(secret)
                    .header("x-read-consistency", consistency);
                if consistency == "at-least" {
                    request = request.header("x-min-version", token);
                }
                if routed {
                    request = request
                        .header("x-query-members", "1")
                        .header("x-query-attempt-ms", "1000")
                        .header("x-query-timeout-ms", "2000")
                        .header("x-leader-fallback", "true");
                }
                let response = request.send().await?;
                assert_eq!(response.status(), 200, "{consistency}, routed={routed}");
                let read: serde_json::Value = response.json().await?;
                assert_eq!(read["value"], 7);
                assert_eq!(read["served_by"], 1);
                assert_eq!(read["group"], receipt["group"]);
                assert_eq!(read["incarnation"], receipt["incarnation"]);
                assert!(read["index"].as_u64().unwrap() >= original.version.position.index.get());
                VersionToken::from_encoded(read["version_token"].as_str().unwrap())?;
            }
        }
        let response = client
            .get(url)
            .bearer_auth(secret)
            .header("x-read-policy", "disconnected-historical")
            .send()
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.json::<serde_json::Value>().await?["value"], 7);

        // Reject invalid or mismatched options at both real ingress paths.
        // None may quietly execute with default consistency or routing.
        let invalid = vec![
            vec![("x-read-consistency", "at-least")],
            vec![("x-min-version", token)],
            vec![
                ("x-read-consistency", "linearizable"),
                ("x-min-version", token),
            ],
            vec![("x-read-consistency", "unknown")],
            vec![("x-read-policy", "unknown")],
            vec![
                ("x-read-consistency", "linearizable"),
                ("x-read-policy", "recent-source"),
            ],
            vec![("x-query-timeout-ms", "0")],
            vec![("x-query-timeout-ms", "5001")],
            vec![("x-query-members", "1,1")],
            vec![("x-query-members", "0")],
            vec![("x-query-members", "01")],
            vec![("x-query-members", "1,2,3,4,5,6,7,8,9")],
            vec![("x-query-attempt-ms", "10")],
            vec![("x-leader-fallback", "true")],
            vec![("x-query-members", "1"), ("x-query-attempt-ms", "0")],
            vec![
                ("x-query-members", "1"),
                ("x-query-attempt-ms", "1001"),
                ("x-query-timeout-ms", "1000"),
            ],
            vec![("x-query-members", "1"), ("x-leader-fallback", "yes")],
            vec![
                ("x-read-consistency", "stale"),
                ("x-read-consistency", "linearizable"),
            ],
        ];
        for headers in invalid {
            let mut request = client.get(url).bearer_auth(secret);
            for (key, value) in &headers {
                request = request.header(*key, *value);
            }
            let response = request.send().await?;
            assert_eq!(response.status(), 400, "invalid options: {headers:?}");
        }
        let oversized = "a".repeat(1900);
        let response = client
            .get(url)
            .bearer_auth(secret)
            .header("x-read-consistency", "at-least")
            .header("x-min-version", oversized)
            .send()
            .await?;
        assert_eq!(response.status(), 400);
        // A structurally valid token with a bad signature reaches server-side
        // authentication and cannot downgrade to an ordinary stale query.
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let mut forged = URL_SAFE_NO_PAD.decode(token)?;
        *forged.last_mut().unwrap() ^= 1;
        let response = client
            .get(url)
            .bearer_auth(secret)
            .header("x-read-consistency", "at-least")
            .header("x-min-version", URL_SAFE_NO_PAD.encode(forged))
            .send()
            .await?;
        assert_eq!(response.status(), 503);
        assert_eq!(response.text().await?, "VersionUnavailable");
        let response = client
            .get(url)
            .bearer_auth(secret)
            .header("x-read-consistency", "linearizable")
            .header("x-contract-digest", hex::encode([99; 32]))
            .send()
            .await?;
        assert_eq!(response.status(), 422);
        // Digest and kind checking happen before a conflicting duplicate can
        // return a stored success or call application code.
        let bad = client
            .post(format!("{url}?delta=7"))
            .bearer_auth(secret)
            .header("x-command-id", &command_id)
            .header("x-retry-until-ms", options.retry_until_ms)
            .header("x-contract-digest", hex::encode([99; 32]))
            .send()
            .await?;
        assert_eq!(bad.status(), 422);
        let conflict = client
            .post(format!("{url}?delta=8"))
            .bearer_auth(secret)
            .header("x-command-id", &command_id)
            .header("x-retry-until-ms", options.retry_until_ms)
            .send()
            .await?;
        assert_eq!(conflict.status(), 409);
        let invalid_command = client
            .post(format!("{url}?delta=7"))
            .bearer_auth(secret)
            .header("x-command-id", &command_id)
            .header("x-retry-until-ms", options.retry_until_ms)
            .header("x-read-consistency", "linearizable")
            .send()
            .await?;
        assert_eq!(invalid_command.status(), 400);
        assert_eq!(client.get(url).send().await?.status(), 401);
    }
    for (i, url) in urls.iter().enumerate() {
        let response = client
            .post(format!("{url}?delta=2"))
            .bearer_auth(secret)
            .header(
                "x-command-id",
                format!("{}:{}", hex::encode([7; 16]), i + 2),
            )
            .header("x-retry-until-ms", options.retry_until_ms)
            .send()
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.json::<serde_json::Value>().await?["value"],
            9 + i as i64 * 2
        );
    }
    for url in &urls {
        let response = client.get(url).bearer_auth(secret).send().await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.json::<serde_json::Value>().await?["value"], 11);
    }
    assert_eq!(
        cell.query(api.get.clone(), vec![], QueryOptions::default())
            .await?
            .result,
        11_i64.to_be_bytes()
    );
    let _ = stop.send(());
    server.await??;
    assert!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn public_multi_group_replication_and_leader_failover() -> Result<()> {
    use cellar::sdk::{consensus::*, NodeRaftConfig, NodeRaftListeners, ShutdownMode};
    use std::{sync::atomic::AtomicUsize, time::Duration};
    let roots: Vec<_> = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<Result<_, _>>()?;
    let calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definitions: Vec<_> = calls
        .iter()
        .map(|calls| public_node::counter(calls.clone(), gate.clone()))
        .collect::<Result<_>>()?;
    let mut nodes = Vec::new();
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for i in 0..3 {
        let label = format!("raft-host-{}", i + 1);
        nodes.push(
            public_node::node_named(roots[i].path(), definitions[i].application.clone(), &label)
                .await?,
        );
        let control = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let data = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let query = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        peers.push(RaftPeerNode {
            node: NodeId::new([i as u8 + 1; 16])?,
            disk: DiskLineage::new([i as u8 + 10; 16])?,
            process: ProcessGeneration::new([i as u8 + 20; 16])?,
            label,
            server_name: "localhost".into(),
            control_address: control.local_addr()?,
            data_address: data.local_addr()?,
            query_address: query.local_addr()?,
        });
        listeners.push(NodeRaftListeners {
            control,
            data,
            query,
        });
    }
    let store = Arc::new(object_store::memory::InMemory::new());
    let namespace_id = NamespaceId::new([2; 16])?;
    let mut namespace = config(2, NamespaceMode::Raft, false);
    namespace.schema_digest = definitions[0].schema;
    let registrations = vec![
        RaftCellRegistration {
            namespace: namespace.clone(),
            class: "Counter".into(),
            birth: proposal(2, "Counter:one", 5, 3),
        },
        RaftCellRegistration {
            namespace,
            class: "Counter".into(),
            birth: proposal(2, "Counter:two", 6, 3),
        },
    ];
    let mut services = Vec::new();
    for (i, listeners) in listeners.into_iter().enumerate() {
        services.push(
            nodes[i]
                .start_raft_cells(
                    RaftCellsConfig {
                        infrastructure: NodeRaftConfig {
                            log: ReplicaLogOptions::new(
                                nodes[i].info().data_dir.join("raft"),
                                peers[i].node,
                                peers[i].disk,
                                peers[i].process,
                                LogOpenMode::Create,
                            ),
                            consensus: RaftRuntimeOptions {
                                tick_interval_ms: Some(20),
                                ..Default::default()
                            },
                            transport: TransportOptions::default(),
                        },
                        catalog: CatalogOptions {
                            prefix: "multi-group/test".into(),
                            ..Default::default()
                        },
                        cells: registrations.clone(),
                        peers: peers.clone(),
                        host: RaftHostOptions::default(),
                    },
                    RaftCellsServices {
                        listeners,
                        tls: public_node::tls()?,
                        store: store.clone(),
                        application: definitions[i].application.clone(),
                        versions: public_node::version_keys()?,
                    },
                )
                .await?,
        );
    }
    let handles: Vec<_> = services
        .iter()
        .map(|service| {
            ["Counter:one", "Counter:two"]
                .iter()
                .map(|name| service.cell(namespace_id, name))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<_, _>>()?;

    async fn leader(
        handles: &[Vec<RaftCellHandle>],
        group: usize,
        excluded: Option<usize>,
    ) -> Result<usize> {
        Ok(tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for (i, cells) in handles.iter().enumerate() {
                    if excluded != Some(i)
                        && cells[group].status().await.is_ok_and(|s| s.command_ready)
                    {
                        return i;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?)
    }
    let mut receipts = Vec::new();
    for group in 0..2 {
        let primary = leader(&handles, group, None).await?;
        let value = (group + 1) as i64;
        let receipt = handles[primary][group]
            .command(
                definitions[primary].increment.clone(),
                value.to_be_bytes().to_vec(),
                public_node::options(1)?,
            )
            .await
            .with_context(|| {
                format!(
                    "initial command in group {group}, leader {primary}, callbacks {}",
                    calls[primary].load(Ordering::SeqCst)
                )
            })?;
        assert_eq!(receipt.result, value.to_be_bytes());
        receipts.push(receipt);
        // The same ID is independent between groups; followers execute no
        // business callback and eventually publish the identical committed cut.
        for cells in &handles {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Ok(read) = cells[group]
                        .query(definitions[0].get.clone(), vec![], QueryOptions::default())
                        .await
                    {
                        if read.version == receipts[group].version {
                            assert_eq!(read.result, value.to_be_bytes());
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
        }
    }
    assert_eq!(
        calls
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .sum::<usize>(),
        2
    );
    let failed = leader(&handles, 0, None).await?;
    assert!(services[failed].close().await?.unfinished.is_empty());
    nodes
        .remove(failed)
        .shutdown(ShutdownMode::Preserve)
        .await?;
    let replacement = leader(&handles, 0, Some(failed)).await?;
    let retry = handles[replacement][0]
        .command(
            definitions[replacement].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await
        .context("retry after election")?;
    assert_eq!(retry.result, receipts[0].result);
    assert_eq!(retry.version, receipts[0].version);
    assert_eq!(
        calls
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .sum::<usize>(),
        2
    );
    let next = handles[replacement][0]
        .command(
            definitions[replacement].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await
        .context("new command after election")?;
    assert_eq!(next.result, 2_i64.to_be_bytes());
    assert_eq!(
        calls
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .sum::<usize>(),
        3
    );
    assert!(handles[failed][0]
        .command(
            definitions[failed].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(4)?
        )
        .await
        .is_err());
    for (i, service) in services.iter().enumerate() {
        if i != failed {
            assert!(service.close().await?.unfinished.is_empty());
        }
    }
    for node in nodes {
        node.shutdown(ShutdownMode::Preserve).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_gate_exact_receipt() -> Result<()> {
    use cellar::sdk::consensus::{CommandReceipt, QueryOptions, RaftCellRegistration};
    use cellar::sdk::ShutdownMode;
    use std::sync::atomic::AtomicUsize;
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let class = RaftClass::new("Counter", 1, [21;32])?
        .command("Increment", [22;32], move |context, input| {
            called.fetch_add(1, Ordering::SeqCst);
            let delta = i64::from_be_bytes(input.try_into()?);
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)")?;
            context.sql().execute("UPDATE counter SET value=value+?1", [delta])?;
            let value:i64=context.sql().query_row("SELECT value FROM counter",[],|row|row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get",[23;32],|context,_| {
            let value:i64=context.query_row("SELECT value FROM counter",[],|row|row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    let schema = class.schema_digest();
    let increment = class.contract("Increment").unwrap();
    let get = class.contract("Get").unwrap();
    let app =
        Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let node = public_node::node(root.path(), app.clone()).await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = schema;
    let registration = RaftCellRegistration {
        namespace,
        class: "Counter".into(),
        birth: proposal(2, "Counter:one", 5, 1),
    };
    let service = public_node::cells(
        &node,
        app.clone(),
        store.clone(),
        registration.clone(),
        false,
    )
    .await?;
    let cell = service.cell(NamespaceId::new([2; 16])?, "Counter:one")?;
    public_node::ready(&cell).await?;
    let request = public_node::options(1)?;
    let CommandReceipt {
        result, version, ..
    } = cell
        .command(
            increment.clone(),
            7_i64.to_be_bytes().to_vec(),
            request.clone(),
        )
        .await?;
    assert_eq!(result, 7_i64.to_be_bytes());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let read = cell
        .query(get.clone(), vec![], QueryOptions::default())
        .await?;
    assert_eq!(read.result, result);
    assert_eq!(read.version, version);
    // Lose a successful response and retry the same identity. The node-owned
    // producer and immutable receipt remain, so business code runs once.
    let repeated = cell
        .command(
            increment.clone(),
            7_i64.to_be_bytes().to_vec(),
            request.clone(),
        )
        .await?;
    assert_eq!(repeated.result, result);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        cell.command(increment.clone(), 8_i64.to_be_bytes().to_vec(), request)
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    assert!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    assert!(service.is_closed());

    let node = public_node::node(root.path(), app.clone()).await?;
    let resumed = public_node::cells(&node, app, store, registration, true).await?;
    let resumed_cell = resumed.cell(NamespaceId::new([2; 16])?, "Counter:one")?;
    public_node::ready(&resumed_cell).await?;
    let result = resumed_cell
        .command(
            increment,
            7_i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await?;
    assert_eq!(result.result, 7_i64.to_be_bytes());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        resumed_cell
            .query(get, vec![], QueryOptions::default())
            .await?
            .result,
        result.result
    );
    assert!(resumed.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_private_callback_is_joined_before_its_slot_is_reused() -> Result<()> {
    use cellar::sdk::{
        consensus::{QueryOptions, RaftCellRegistration},
        ShutdownMode,
    };
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    let calls = Arc::new(AtomicUsize::new(0));
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let definition = public_node::counter(calls.clone(), gate.clone())?;
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let service = public_node::cells(
        &node,
        definition.application,
        store,
        RaftCellRegistration {
            namespace,
            class: "Counter".into(),
            birth: proposal(2, "Counter:one", 5, 1),
        },
        false,
    )
    .await?;
    let cell = service.cell(NamespaceId::new([2; 16])?, "Counter:one")?;
    public_node::ready(&cell).await?;
    cell.command(
        definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        public_node::options(1)?,
    )
    .await?;
    let mut options = public_node::options(2)?;
    options.timeout = Duration::from_millis(100);
    let pending = cell.command(
        definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        options,
    );
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("callback missing"))?;
    assert!(node.services().application_work().pending_events >= 1);
    assert!(matches!(pending.await, Err(CommandFailure::Unavailable)));
    assert!(matches!(
        cell.command(
            definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(3)?
        )
        .await,
        Err(CommandFailure::Overloaded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while node.services().application_work().pending_events != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let read = cell
        .query(definition.get.clone(), vec![], QueryOptions::default())
        .await?;
    assert_eq!(read.result, 1_i64.to_be_bytes());
    let result = cell
        .command(
            definition.increment,
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await?;
    assert_eq!(result.result, 2_i64.to_be_bytes());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_handle_fenced() -> Result<()> {
    use cellar::sdk::{
        consensus::{QueryOptions, RaftCellRegistration},
        ShutdownMode,
    };
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    let calls = Arc::new(AtomicUsize::new(0));
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let definition = public_node::counter(calls.clone(), gate.clone())?;
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let registration = RaftCellRegistration {
        namespace,
        class: "Counter".into(),
        birth: proposal(2, "Counter:one", 5, 1),
    };
    let service = public_node::cells(
        &node,
        definition.application.clone(),
        store.clone(),
        registration.clone(),
        false,
    )
    .await?;
    let old = service.cell(NamespaceId::new([2; 16])?, "Counter:one")?;
    public_node::ready(&old).await?;
    old.command(
        definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        public_node::options(1)?,
    )
    .await?;
    let pending = old.command(
        definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        public_node::options(2)?,
    );
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("callback missing"))?;
    let joined = tokio::spawn(node.cancel());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !joined.is_finished(),
        "node cannot release a directory while its callback is running"
    );
    assert!(
        public_node::node(root.path(), definition.application.clone())
            .await
            .is_err()
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), joined).await???;
    assert!(matches!(pending.await, Err(CommandFailure::Unavailable)));
    assert!(service.is_closed());
    assert!(matches!(
        old.command(
            definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(3)?
        )
        .await,
        Err(CommandFailure::Unavailable)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let current =
        public_node::cells(&node, definition.application, store, registration, true).await?;
    let cell = current.cell(NamespaceId::new([2; 16])?, "Counter:one")?;
    public_node::ready(&cell).await?;
    assert_eq!(
        cell.query(definition.get, vec![], QueryOptions::default())
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    assert_eq!(
        cell.command(
            definition.increment,
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?
        )
        .await?
        .result,
        2_i64.to_be_bytes()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(current.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registered_callbacks_refuse_captured_sdk_handles() -> Result<()> {
    use cellar::sdk::{
        consensus::{QueryOptions, RaftCellHandle, RaftCellRegistration},
        ShutdownMode,
    };
    use futures_util::FutureExt;
    let captured = Arc::new(std::sync::Mutex::new(
        None::<std::sync::Weak<RaftCellHandle>>,
    ));
    let command = captured.clone();
    let query = captured.clone();
    let class = RaftClass::new("Counter", 1, [21; 32])?
        .command("Increment", [22; 32], move |context, _| {
            let handle = command.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert!(matches!(
                handle.status().now_or_never(),
                Some(Err(CommandFailure::UnsupportedCapability))
            ));
            context.sql().execute_batch(
                "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(1)",
            )?;
            Ok(vec![1])
        })?
        .query("Get", [23; 32], move |context, _| {
            let handle = query.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert!(matches!(
                handle.status().now_or_never(),
                Some(Err(CommandFailure::UnsupportedCapability))
            ));
            let value: u8 = context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(vec![value])
        })?;
    let schema = class.schema_digest();
    let increment = class.contract("Increment").unwrap();
    let get = class.contract("Get").unwrap();
    let application =
        Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let node = public_node::node(root.path(), application.clone()).await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = schema;
    let service = public_node::cells(
        &node,
        application,
        store,
        RaftCellRegistration {
            namespace,
            class: "Counter".into(),
            birth: proposal(2, "Counter:one", 5, 1),
        },
        false,
    )
    .await?;
    let cell = Arc::new(service.cell(NamespaceId::new([2; 16])?, "Counter:one")?);
    public_node::ready(&cell).await?;
    *captured.lock().unwrap() = Some(Arc::downgrade(&cell));
    assert_eq!(
        cell.command(increment, vec![], public_node::options(1)?)
            .await?
            .result,
        vec![1]
    );
    assert_eq!(
        cell.query(get, vec![], QueryOptions::default())
            .await?
            .result,
        vec![1]
    );
    assert!(cell.status().await?.command_ready);
    assert!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[test]
fn unsupported_factory_refused() -> Result<()> {
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let invoked = called.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
        .cell("Mutable", move |_, _| {
            invoked.store(true, Ordering::SeqCst);
            bail!("a mutable factory must never be called by Raft dispatch")
        })?
        .raft_class(registered_counter()?)?;
    assert!(matches!(
        registry.resolve_raft_class("Mutable"),
        Err(RegistrationError::UnsupportedFactory(class)) if class == "Mutable"
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert!(registry.supports_cell("Mutable"));
    assert!(!registry.supports_cell("Counter"));
    let counter = registry.resolve_raft_class("Counter")?;
    assert_eq!(counter.schema_version(), 1);
    assert_eq!(counter.name(), "Counter");
    assert!(matches!(
        registry.resolve_raft_class("Missing"),
        Err(RegistrationError::UnknownClass(_))
    ));

    // Registration is exclusive in either order. No fallback factory can be
    // installed to grant a second mutation authority for the class.
    assert!(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
        .cell("Counter", |_, _| bail!("unused"))?
        .raft_class(registered_counter()?)
        .is_err());
    assert!(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
        .raft_class(registered_counter()?)?
        .cell("Counter", |_, _| bail!("unused"))
        .is_err());
    assert!(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
        .workflow("Counter", |_, _, _| Box::pin(async {
            bail!("unused workflow")
        }))?
        .raft_class(registered_counter()?)
        .is_err());
    assert!(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
        .raft_class(registered_counter()?)?
        .workflow("Counter", |_, _, _| Box::pin(async {
            bail!("unused workflow")
        }))
        .is_err());

    let mut options = cellar::engine_api::WorkerConfigOptions {
        src: String::new(),
        script_name: "test".into(),
        do_classes: vec!["Mutable".into()],
        bindings: vec![],
        blob_bindings: vec![],
        sql_bindings: vec![],
        kv_bindings: vec![],
        queue_bindings: vec![],
        stream_bindings: vec![],
        queue_consumers: vec![],
        workflow_bindings: vec![],
        vars: vec![],
        node: "test".into(),
        modules: vec![],
        compat: Default::default(),
    };
    registry.validate_configuration(&options)?;
    options.do_classes.push("Counter".into());
    let error = registry.validate_configuration(&options).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RegistrationError>(),
        Some(RegistrationError::WrongMode {
            mode: NamespaceMode::Fleet,
            ..
        })
    ));
    Ok(())
}

#[test]
fn operation_registration_binds_schema_kind_and_digest() -> Result<()> {
    use cellar::sdk::OperationKind;
    let counter = registered_counter()?;
    let increment = counter.contract("Increment").unwrap();
    counter.validate_operation(&increment)?;
    let mut wrong = increment.clone();
    wrong.kind = OperationKind::Query;
    assert_eq!(
        counter.validate_operation(&wrong),
        Err(RegistrationError::KindMismatch)
    );
    wrong.kind = OperationKind::Command;
    wrong.digest = [10; 32];
    assert_eq!(
        counter.validate_operation(&wrong),
        Err(RegistrationError::ContractMismatch)
    );
    wrong.operation = "Missing".into();
    assert!(matches!(
        counter.validate_operation(&wrong),
        Err(RegistrationError::UnknownOperation(_))
    ));
    wrong.operation = "x".repeat(129);
    assert!(matches!(
        counter.validate_operation(&wrong),
        Err(RegistrationError::Invalid(_))
    ));
    assert!(RaftClass::new("Counter", 0, [7; 32]).is_err());
    assert!(RaftClass::new("__SQLDatabase", 1, [7; 32]).is_err());
    assert!(RaftClass::new("../Counter", 1, [7; 32]).is_err());
    assert!(RaftClass::new("Counter", 1, [0; 32]).is_err());
    assert!(RaftClass::new("Counter", 1, [7; 32])?
        .command("Increment", [0; 32], |_, _| Ok(vec![]))
        .is_err());
    assert!(matches!(
        counter.query("Increment", [8; 32], |_, _| Ok(vec![])),
        Err(RegistrationError::Duplicate(_))
    ));
    assert_ne!(
        registered_counter()?.schema_digest(),
        RaftClass::new("Counter", 2, [7; 32])?.schema_digest()
    );
    assert_ne!(
        registered_counter()?.schema_digest(),
        RaftClass::new("Other", 1, [7; 32])?.schema_digest()
    );
    Ok(())
}

fn config(id: u8, mode: NamespaceMode, development: bool) -> NamespaceConfig {
    let mut config = NamespaceConfig::new(
        ClusterId::new([1; 16]).unwrap(),
        NamespaceId::new([id; 16]).unwrap(),
        mode,
        [3; 32],
    );
    if development {
        config.voters = VoterPolicy::SingleVoterDevelopment;
    }
    config
}

fn proposal(id: u8, cell: &str, group: u8, count: u64) -> BirthProposal {
    BirthProposal {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([id; 16]).unwrap(),
            cell: cell.into(),
            group: GroupId::new([group; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        voters: (1..=count)
            .map(|id| InitialMember {
                member: MemberId::new(id).unwrap(),
                node: NodeId::new([id as u8; 16]).unwrap(),
            })
            .collect(),
        learners: vec![],
    }
}

fn catalog(store: Arc<store::Store>) -> Result<Catalog> {
    Ok(Catalog::new(
        store,
        CatalogOptions {
            prefix: "fleet/test".into(),
            ..Default::default()
        },
    )?)
}

async fn open_log(
    root: &Path,
    disk: u8,
    process: u8,
    mode: LogOpenMode,
) -> Result<(ExecutionDomain, ReplicaLog)> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.into(),
                NodeId::new([1; 16])?,
                DiskLineage::new([disk; 16])?,
                ProcessGeneration::new([process; 16])?,
                mode,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    Ok((domain, log))
}

async fn close(
    domain: ExecutionDomain,
    log: ReplicaLog,
    runtime: Option<RaftRuntime>,
) -> Result<()> {
    if let Some(runtime) = runtime {
        runtime.close().await?;
    }
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namespace_mode_conflict() -> Result<()> {
    let store = Arc::new(store::Store::default());
    let catalog = catalog(store.clone())?;
    let fleet = catalog
        .open_namespace(config(2, NamespaceMode::Fleet, false))
        .await?;
    assert_eq!(fleet.data_prefix(), "fleet/test/cells/");
    assert!(matches!(
        catalog
            .open_namespace(config(2, NamespaceMode::Raft, false))
            .await,
        Err(CatalogError::Conflict(_))
    ));
    let mut schema = config(2, NamespaceMode::Fleet, false);
    schema.schema_digest = [8; 32];
    assert!(matches!(
        catalog.open_namespace(schema).await,
        Err(CatalogError::Conflict(_))
    ));
    assert!(matches!(
        fleet.resolve_birth(proposal(2, "Counter:one", 5, 3)).await,
        Err(CatalogError::WrongMode)
    ));
    let raft = catalog
        .open_namespace(config(3, NamespaceMode::Raft, false))
        .await?;
    assert!(raft.data_prefix().starts_with("fleet/test/raft/"));
    let (a, b) = futures_util::future::join(
        raft.resolve_birth(proposal(3, "Counter:one", 5, 3)),
        raft.resolve_birth(proposal(3, "Counter:one", 6, 3)),
    )
    .await;
    let (a, b) = (a?, b?);
    assert_eq!(a.scope(), b.scope());
    assert_eq!(a.digest(), b.digest());
    assert_eq!(
        raft.birth("Counter:one").await?.unwrap().digest(),
        a.digest()
    );
    let mut reused = proposal(3, "Counter:two", 7, 3);
    reused.scope.group = a.scope().group;
    assert!(matches!(
        raft.resolve_birth(reused).await,
        Err(CatalogError::Conflict(_))
    ));
    let other_namespace = catalog
        .open_namespace(config(4, NamespaceMode::Raft, false))
        .await?;
    let mut reused = proposal(4, "Counter:one", 8, 3);
    reused.scope.group = a.scope().group;
    assert!(matches!(
        other_namespace.resolve_birth(reused).await,
        Err(CatalogError::Conflict(_))
    ));
    assert_eq!(store.non_create.load(Ordering::Acquire), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_bootstrap_requires_stable_claim() -> Result<()> {
    let store = Arc::new(store::Store::default());
    let catalog = catalog(store)?;
    let namespace = catalog
        .open_namespace(config(2, NamespaceMode::Raft, true))
        .await?;
    let birth = namespace
        .resolve_birth(proposal(2, "Counter:one", 5, 1))
        .await?;
    let root = tempfile::tempdir()?;
    let (domain, log) = open_log(&root.path().join("member"), 11, 21, LogOpenMode::Create).await?;
    let permit = namespace.bootstrap_member(&birth, &log).await?;
    let (runtime, mut events) = RaftRuntime::start_parked(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            election_seed: Some([7; 32]),
            ..Default::default()
        },
    )
    .await?;
    for _ in 0..30 {
        runtime.tick().await?;
    }
    assert!(matches!(
        runtime.status(birth.scope()).await,
        Err(RuntimeError::NotFound)
    ));
    assert!(events.try_recv().is_err());
    permit.attach(&runtime).await?;
    runtime.campaign(birth.scope()).await?;
    let mut ready = false;
    for _ in 0..3000 {
        while let Ok(event) = events.try_recv() {
            if let RuntimeEventKind::Failed { error, .. } = event.kind {
                return Err(error.into());
            }
        }
        if runtime.status(birth.scope()).await?.protocol_ready {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(ready, "authorized member never elected");
    close(domain, log, Some(runtime)).await?;
    let (domain, log) = open_log(&root.path().join("member"), 11, 22, LogOpenMode::Resume).await?;
    let resumed = namespace.bootstrap_member(&birth, &log).await?;
    let (runtime, _events) =
        RaftRuntime::start_parked(log.clone(), RaftRuntimeOptions::default()).await?;
    assert!(matches!(
        permit.attach(&runtime).await,
        Err(CatalogError::Conflict(_))
    ));
    resumed.attach(&runtime).await?;
    close(domain, log, Some(runtime)).await?;
    let (domain, log) =
        open_log(&root.path().join("lost-local"), 11, 23, LogOpenMode::Create).await?;
    assert!(matches!(
        namespace.bootstrap_member(&birth, &log).await,
        Err(CatalogError::LostStableState)
    ));
    assert!(log.groups()?.is_empty());
    close(domain, log, None).await?;
    let (domain, log) = open_log(
        &root.path().join("replacement-disk"),
        12,
        24,
        LogOpenMode::Create,
    )
    .await?;
    assert!(matches!(
        namespace.bootstrap_member(&birth, &log).await,
        Err(CatalogError::Conflict(_))
    ));
    assert!(log.groups()?.is_empty());
    close(domain, log, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_reconciles_lost_create_responses() -> Result<()> {
    let store = Arc::new(store::Store::default());
    let catalog = catalog(store.clone())?;
    store.fail_after.store(true, Ordering::Release);
    let namespace = catalog
        .open_namespace(config(2, NamespaceMode::Raft, true))
        .await?;
    store.fail_before.store(true, Ordering::Release);
    assert!(namespace
        .resolve_birth(proposal(2, "Counter:one", 5, 1))
        .await
        .is_err());
    let birth = namespace
        .resolve_birth(proposal(2, "Counter:one", 5, 1))
        .await?;
    let root = tempfile::tempdir()?;
    let (domain, log) = open_log(root.path(), 11, 21, LogOpenMode::Create).await?;
    store.fail_after.store(true, Ordering::Release);
    let permit = namespace.bootstrap_member(&birth, &log).await?;
    assert_eq!(permit.replica().scope, *birth.scope());
    assert_eq!(store.non_create.load(Ordering::Acquire), 0);
    close(domain, log, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_bootstrap_disks_cannot_both_join() -> Result<()> {
    let store = Arc::new(store::Store::default());
    let namespace = catalog(store)?
        .open_namespace(config(2, NamespaceMode::Raft, true))
        .await?;
    let birth = namespace
        .resolve_birth(proposal(2, "Counter:one", 5, 1))
        .await?;
    let root = tempfile::tempdir()?;
    let (domain_a, log_a) = open_log(&root.path().join("a"), 11, 21, LogOpenMode::Create).await?;
    let (domain_b, log_b) = open_log(&root.path().join("b"), 12, 22, LogOpenMode::Create).await?;
    let (a, b) = futures_util::future::join(
        namespace.bootstrap_member(&birth, &log_a),
        namespace.bootstrap_member(&birth, &log_b),
    )
    .await;
    assert_ne!(
        a.is_ok(),
        b.is_ok(),
        "two disks acquired one initial voting identity"
    );
    let failure = if a.is_ok() {
        b.err().unwrap()
    } else {
        a.err().unwrap()
    };
    assert!(matches!(failure, CatalogError::Conflict(_)));
    close(domain_a, log_a, None).await?;
    close(domain_b, log_b, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_refuses_corrupt_and_oversized_records() -> Result<()> {
    let store = Arc::new(store::Store::default());
    let catalog = catalog(store.clone())?;
    let config = config(2, NamespaceMode::Raft, false);
    catalog.open_namespace(config.clone()).await?;
    let key = ObjectPath::from(format!(
        "fleet/test/namespaces/{}/root",
        hex::encode(config.namespace.bytes())
    ));
    let original = store.inner.get(&key).await?.bytes().await?;
    for at in [0, 8, 13, original.len() - 1] {
        let mut bad = original.to_vec();
        bad[at] ^= 1;
        store.inner.put(&key, bad.into()).await?;
        assert!(matches!(
            catalog.open_namespace(config.clone()).await,
            Err(CatalogError::Corrupt(_))
        ));
    }
    store.inner.put(&key, vec![0; 64 * 1024 + 1].into()).await?;
    assert!(matches!(
        catalog.open_namespace(config.clone()).await,
        Err(CatalogError::Limit)
    ));
    store.inner.put(&key, original.into()).await?;
    let namespace = catalog.open_namespace(config).await?;
    if namespace.mode() != NamespaceMode::Raft {
        bail!("corrupt root changed the mode");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconfirmed_claim_keeps_the_group_parked() -> Result<()> {
    let store = Arc::new(store::Store::default());
    let catalog = catalog(store.clone())?;
    store.fail_read.store(true, Ordering::Release);
    let cfg = config(2, NamespaceMode::Raft, true);
    assert!(catalog.open_namespace(cfg.clone()).await.is_err());
    let namespace = catalog.open_namespace(cfg).await?;
    let birth = namespace
        .resolve_birth(proposal(2, "Counter:one", 5, 1))
        .await?;
    let root = tempfile::tempdir()?;
    let (domain, log) = open_log(root.path(), 11, 21, LogOpenMode::Create).await?;
    store.fail_confirmation.store(true, Ordering::Release);
    assert!(namespace.bootstrap_member(&birth, &log).await.is_err());
    assert_eq!(
        log.groups()?.len(),
        1,
        "initial state must precede the claim write"
    );
    let (runtime, mut events) = RaftRuntime::start_parked(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            election_seed: Some([7; 32]),
            ..Default::default()
        },
    )
    .await?;
    assert!(matches!(
        runtime.campaign(birth.scope()).await,
        Err(RuntimeError::NotFound)
    ));
    for _ in 0..30 {
        runtime.tick().await?;
    }
    assert!(events.try_recv().is_err());
    let permit = namespace.bootstrap_member(&birth, &log).await?;
    permit.attach(&runtime).await?;
    close(domain, log, Some(runtime)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn birth_handle_cannot_cross_catalog_instances() -> Result<()> {
    let namespace_a = catalog(Arc::new(store::Store::default()))?
        .open_namespace(config(2, NamespaceMode::Raft, true))
        .await?;
    let namespace_b = catalog(Arc::new(store::Store::default()))?
        .open_namespace(config(2, NamespaceMode::Raft, true))
        .await?;
    let birth = namespace_a
        .resolve_birth(proposal(2, "Counter:one", 5, 1))
        .await?;
    let root = tempfile::tempdir()?;
    let (domain, log) = open_log(root.path(), 11, 21, LogOpenMode::Create).await?;
    assert!(matches!(
        namespace_b.bootstrap_member(&birth, &log).await,
        Err(CatalogError::Conflict(_))
    ));
    assert!(log.groups()?.is_empty());
    close(domain, log, None).await
}
