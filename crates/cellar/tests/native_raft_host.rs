//! Host integration starts with immutable catalog and initial-member admission.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_catalog_fault.rs"]
mod store;

use anyhow::{bail, Result};
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
