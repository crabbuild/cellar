// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual loopback TLS with a protocol backend. These are not native-compute,
//! quorum, RustFS or infrastructure-fencing qualification.
use super::*;
use crate::controller::ControllerEndpoint;
use anyhow::{ensure, Result};
use cellar::sdk::{consensus::*, containers::*, ExecutionDomain, HandlerFuture};
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    RootCertStore,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{io::AsyncWriteExt, sync::Notify};

#[derive(Default)]
struct Backend {
    active: Mutex<Option<ExecutionClaim>>,
    results: Mutex<BTreeMap<[u8; 16], Vec<u8>>>,
    calls: AtomicUsize,
    fences: AtomicUsize,
    hold: AtomicBool,
    entered: Notify,
}

impl ExecutionControllerBackend for Backend {
    fn identity(&self) -> BackendIdentity {
        BackendIdentity {
            binding: "TOOL".into(),
            provider: "tls-fixture".into(),
            configuration_digest: "a".repeat(64),
        }
    }

    fn revoke(
        self: Arc<Self>,
        _: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        _: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            self.fences.fetch_add(1, Ordering::AcqRel);
            *self.active.lock().unwrap() = successor;
            Ok(())
        })
    }

    fn revoke_history(
        self: Arc<Self>,
        _: CellHistory,
        _: CellHistory,
        _: Option<ExecutionClaim>,
        _: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        // The ordinary RPC protocol has no recovery administration operation.
        Box::pin(async { anyhow::bail!("recovery is not supplied by this protocol fixture") })
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            ensure!(
                self.active.lock().unwrap().as_ref() == Some(&claim),
                "stale backend call"
            );
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.results
                .lock()
                .unwrap()
                .insert(operation.id, payload.clone());
            self.entered.notify_one();
            if self.hold.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            Ok(ExecutionOutcome::Completed(payload))
        })
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<Option<ExecutionOutcome>> {
        Box::pin(async move {
            ensure!(
                self.active.lock().unwrap().as_ref() == Some(&current),
                "stale backend lookup"
            );
            Ok(self
                .results
                .lock()
                .unwrap()
                .get(&original.operation.id)
                .cloned()
                .map(ExecutionOutcome::Completed))
        })
    }
}

struct Credentials {
    ca: CertificateDer<'static>,
    server: CertificateDer<'static>,
    server_key: Vec<u8>,
    client: CertificateDer<'static>,
    client_key: Vec<u8>,
    other: CertificateDer<'static>,
    other_key: Vec<u8>,
}

impl Credentials {
    fn new() -> Result<Self> {
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let issuer = CertifiedIssuer::self_signed(params, KeyPair::generate()?)?;
        let server_key = KeyPair::generate()?;
        let server = CertificateParams::new(vec!["localhost".into()])?
            .signed_by(&server_key, &issuer)?
            .der()
            .clone();
        let client_key = KeyPair::generate()?;
        let client = CertificateParams::new(vec!["cellar-host".into()])?
            .signed_by(&client_key, &issuer)?
            .der()
            .clone();
        let other_key = KeyPair::generate()?;
        let other = CertificateParams::new(vec!["other-host".into()])?
            .signed_by(&other_key, &issuer)?
            .der()
            .clone();
        Ok(Self {
            ca: issuer.der().clone(),
            server,
            server_key: server_key.serialize_der(),
            client,
            client_key: client_key.serialize_der(),
            other,
            other_key: other_key.serialize_der(),
        })
    }

    fn roots(&self) -> Result<RootCertStore> {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca.clone())?;
        Ok(roots)
    }

    fn server(&self) -> Result<ServerTls> {
        Ok(ServerTls {
            certificates: vec![self.server.clone()],
            key: key(&self.server_key),
            client_roots: self.roots()?,
            permissions: vec![Permission {
                certificate_sha256: Sha256::digest(self.client.as_ref()).into(),
                cluster: [1; 16],
                namespace: [2; 16],
            }],
        })
    }

    fn client(&self, other: bool) -> Result<ClientTls> {
        Ok(ClientTls {
            certificates: vec![if other {
                self.other.clone()
            } else {
                self.client.clone()
            }],
            key: key(if other {
                &self.other_key
            } else {
                &self.client_key
            }),
            server_roots: self.roots()?,
            server_name: "localhost".try_into()?,
            server_certificate_sha256: Sha256::digest(self.server.as_ref()).into(),
        })
    }
}

fn key(bytes: &[u8]) -> PrivateKeyDer<'static> {
    PrivatePkcs8KeyDer::from(bytes.to_vec()).into()
}

fn domain() -> Arc<ExecutionDomain> {
    Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()))
}

fn options() -> RemoteOptions {
    RemoteOptions {
        address: "127.0.0.1:0".parse().unwrap(),
        max_frame_bytes: 1024 * 1024,
        max_connections: 8,
        timeout_ms: 5000,
    }
}

// Trusted metadata codec, derived from the core controller test. It supplies no
// quorum proof; actual source admission is exercised through SDK RF3 fixtures.
fn claim(identity: &BackendIdentity, namespace: u8, sequence: u64) -> Result<ExecutionClaim> {
    let history = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([namespace; 16])?,
        cell: CellId::new("Counter:controller")?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let mut hash = Sha256::new();
    hash.update(b"cellar-provider-execution-resource-1");
    hash.update(
        cellar::application::consensus::verification::cell_groups::cell_image_history_digest(
            &history,
        ),
    );
    hash.update((identity.binding.len() as u16).to_be_bytes());
    hash.update(identity.binding.as_bytes());
    let key: [u8; 32] = hash.finalize().into();
    Ok(serde_json::from_value(serde_json::json!({
        "format":1,"cluster":history.cluster.bytes(),"namespace":history.namespace.bytes(),"cell":history.cell.as_str(),"incarnation":history.incarnation.bytes(),"binding":identity.binding,
        "key":key,"sequence":sequence,"owner":format!("{sequence:064x}"),
        "checkpoint":{"revision":1,"activation_epoch":1,"record":{"provider":identity.provider,"configuration_digest":identity.configuration_digest,"parameters_digest":"b".repeat(64),"generation":1,"request_id":"c".repeat(32),"resource_id":null,"intent":"running","observation":"pending"}}
    }))?)
}

fn failure(error: anyhow::Error, expected: ExecutionControllerFailure) {
    assert_eq!(
        error.downcast_ref::<ExecutionControllerFailure>(),
        Some(&expected)
    );
}

async fn operation(domain: &ExecutionDomain, id: u8, payload: &[u8]) -> Result<ExecutionOperation> {
    let expires = domain
        .run(async { u64::try_from(cellar::asyncrt::wall_ms()).unwrap() + 5000 })
        .await?;
    ExecutionOperation::new([id; 16], ExecutionOperationKind::Execute, payload, expires)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_authorization_and_controller_pins_precede_backend_entry() -> Result<()> {
    let domain = domain();
    let root = tempfile::tempdir()?;
    let backend = Arc::new(Backend::default());
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.path().join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        backend.clone(),
        [42; 32],
    )
    .await?;
    let tls = Credentials::new()?;
    let server =
        ControllerServer::bind(domain.clone(), controller.clone(), options(), tls.server()?)
            .await?;
    let options = RemoteOptions {
        address: server.address(),
        ..options()
    };
    let identity = controller.identity();
    let current = claim(&identity, 2, 1)?;
    let foreign = Credentials::new()?;
    let mut foreign_client = foreign.client(false)?;
    foreign_client.server_roots = tls.roots()?;
    foreign_client.server_certificate_sha256 = Sha256::digest(tls.server.as_ref()).into();
    let untrusted = RemoteController::new(
        domain.clone(),
        options,
        identity.clone(),
        controller.options().limits,
        foreign_client,
    )?;
    failure(
        untrusted.activate(current.clone()).await.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    let other = RemoteController::new(
        domain.clone(),
        options,
        identity.clone(),
        controller.options().limits,
        tls.client(true)?,
    )?;
    failure(
        other.activate(current.clone()).await.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    let mut wrong_pin = tls.client(false)?;
    wrong_pin.server_certificate_sha256 = [7; 32];
    let wrong = RemoteController::new(
        domain.clone(),
        options,
        identity.clone(),
        controller.options().limits,
        wrong_pin,
    )?;
    failure(
        wrong.activate(current.clone()).await.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    let mut wrong_identity = identity.clone();
    wrong_identity.configuration_digest = "d".repeat(64);
    let wrong = RemoteController::new(
        domain.clone(),
        options,
        wrong_identity,
        controller.options().limits,
        tls.client(false)?,
    )?;
    failure(
        wrong.activate(current.clone()).await.unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    let mut wrong_limits = controller.options().limits;
    wrong_limits.max_cells += 1;
    let wrong = RemoteController::new(
        domain.clone(),
        options,
        identity.clone(),
        wrong_limits,
        tls.client(false)?,
    )?;
    failure(
        wrong.activate(current.clone()).await.unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    let client = RemoteController::new(
        domain.clone(),
        options,
        identity.clone(),
        controller.options().limits,
        tls.client(false)?,
    )?;
    failure(
        client
            .clone()
            .activate(claim(&identity, 4, 1)?)
            .await
            .unwrap_err(),
        ExecutionControllerFailure::RevokedHistory,
    );
    assert_eq!(backend.fences.load(Ordering::Acquire), 0);
    assert_eq!(backend.calls.load(Ordering::Acquire), 0);
    client.activate(current).await?;
    assert_eq!(backend.fences.load(Ordering::Acquire), 1);
    server.shutdown().await?;
    controller.shutdown().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_preserves_receipts_fences_and_expired_operation_seals() -> Result<()> {
    let domain = domain();
    let root = tempfile::tempdir()?;
    let backend = Arc::new(Backend::default());
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.path().join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        backend.clone(),
        [42; 32],
    )
    .await?;
    let tls = Credentials::new()?;
    let server =
        ControllerServer::bind(domain.clone(), controller.clone(), options(), tls.server()?)
            .await?;
    let client = RemoteController::new(
        domain.clone(),
        RemoteOptions {
            address: server.address(),
            ..options()
        },
        controller.identity(),
        controller.options().limits,
        tls.client(false)?,
    )?;
    let old = claim(&controller.identity(), 2, 1)?;
    client.clone().activate(old.clone()).await?;
    let request = operation(&domain, 9, b"once").await?;
    assert_eq!(
        client
            .clone()
            .execute(old.clone(), request.clone(), b"once".to_vec())
            .await?,
        ExecutionOutcome::Completed(b"once".to_vec())
    );
    assert_eq!(
        client
            .clone()
            .execute(old.clone(), request.clone(), b"once".to_vec())
            .await?,
        ExecutionOutcome::Completed(b"once".to_vec())
    );
    assert!(client.clone().pending(old.clone()).await?.is_empty());
    assert!(client
        .clone()
        .lifecycle(old.clone(), ExecutionOperationKind::Create)
        .await?
        .is_none());
    let new = claim(&controller.identity(), 2, 2)?;
    client.clone().activate(new.clone()).await?;
    failure(
        client.clone().activate(old.clone()).await.unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    let original = ExecutionPendingOperation {
        claim: old,
        operation: request,
    };
    assert_eq!(
        client.clone().reconcile(new.clone(), original).await?,
        ExecutionOutcome::Completed(b"once".to_vec())
    );
    let sealed = ExecutionOperation::new([10; 16], ExecutionOperationKind::Execute, b"late", 1)?;
    let original = client
        .clone()
        .seal_unsubmitted(new.clone(), sealed.clone())
        .await?;
    assert_eq!(original.operation, sealed);
    assert_eq!(
        client
            .clone()
            .execute(new, sealed, b"late".to_vec())
            .await?,
        ExecutionOutcome::Refused
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    server.shutdown().await?;
    controller.shutdown().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_lost_reply_reconciles_after_listener_and_controller_restart_without_replay(
) -> Result<()> {
    let domain = domain();
    let root = tempfile::tempdir()?;
    let backend = Arc::new(Backend::default());
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.path().join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        backend.clone(),
        [42; 32],
    )
    .await?;
    let tls = Credentials::new()?;
    let server =
        ControllerServer::bind(domain.clone(), controller.clone(), options(), tls.server()?)
            .await?;
    let client = RemoteController::new(
        domain.clone(),
        RemoteOptions {
            address: server.address(),
            ..options()
        },
        controller.identity(),
        controller.options().limits,
        tls.client(false)?,
    )?;
    let current = claim(&controller.identity(), 2, 1)?;
    client.clone().activate(current.clone()).await?;
    let request = operation(&domain, 11, b"original").await?;
    backend.hold.store(true, Ordering::Release);
    let executing = {
        let client = client.clone();
        let current = current.clone();
        let request = request.clone();
        domain.spawn(async move { client.execute(current, request, b"original".to_vec()).await })?
    };
    domain
        .run(async {
            cellar::asyncrt::timeout(Duration::from_secs(2), backend.entered.notified()).await
        })
        .await??;
    server.shutdown().await?;
    failure(
        executing.await?.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    let pending = client.clone(); // Closed address cannot silently route elsewhere.
    failure(
        pending.pending(current.clone()).await.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    let resume = controller.options();
    controller.shutdown().await?;
    drop(controller);
    let controller = ExecutionController::open(domain.clone(), resume, backend.clone()).await?;
    let server =
        ControllerServer::bind(domain.clone(), controller.clone(), options(), tls.server()?)
            .await?;
    let client = RemoteController::new(
        domain.clone(),
        RemoteOptions {
            address: server.address(),
            ..options()
        },
        controller.identity(),
        controller.options().limits,
        tls.client(false)?,
    )?;
    client.clone().activate(current.clone()).await?;
    assert_eq!(
        client
            .clone()
            .reconcile(
                current.clone(),
                ExecutionPendingOperation {
                    claim: current.clone(),
                    operation: request.clone()
                }
            )
            .await?,
        ExecutionOutcome::Completed(b"original".to_vec())
    );
    assert_eq!(
        client
            .execute(current, request, b"original".to_vec())
            .await?,
        ExecutionOutcome::Completed(b"original".to_vec())
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    server.shutdown().await?;
    controller.shutdown().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_client_bounds_observations_and_reserves_fencing_capacity() -> Result<()> {
    let domain = domain();
    let root = tempfile::tempdir()?;
    let backend = Arc::new(Backend::default());
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.path().join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        backend.clone(),
        [42; 32],
    )
    .await?;
    let tls = Credentials::new()?;
    let server =
        ControllerServer::bind(domain.clone(), controller.clone(), options(), tls.server()?)
            .await?;
    let client = RemoteController::new(
        domain.clone(),
        RemoteOptions {
            address: server.address(),
            max_connections: 2,
            ..options()
        },
        controller.identity(),
        controller.options().limits,
        tls.client(false)?,
    )?;
    let current = claim(&controller.identity(), 2, 1)?;
    client.clone().activate(current.clone()).await?;
    let expires = operation(&domain, 12, b"observation")
        .await?
        .expires_unix_ms;
    let request = ExecutionOperation::new(
        [12; 16],
        ExecutionOperationKind::Inspect,
        b"observation",
        expires,
    )?;
    backend.hold.store(true, Ordering::Release);
    let held = {
        let client = client.clone();
        let current = current.clone();
        let request = request.clone();
        domain.spawn(async move {
            client
                .execute(current, request, b"observation".to_vec())
                .await
        })?
    };
    domain
        .run(async {
            cellar::asyncrt::timeout(Duration::from_secs(2), backend.entered.notified()).await
        })
        .await??;
    let second = ExecutionOperation::new(
        [13; 16],
        ExecutionOperationKind::Inspect,
        b"observation",
        expires,
    )?;
    failure(
        client
            .clone()
            .execute(current, second, b"observation".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::Busy,
    );
    client
        .activate(claim(&controller.identity(), 2, 2)?)
        .await?;
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    assert_eq!(backend.fences.load(Ordering::Acquire), 2);
    server.shutdown().await?;
    failure(
        held.await?.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    controller.shutdown().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_malformed_and_oversized_frames_refuse_before_backend_entry_and_shutdown_joins(
) -> Result<()> {
    let domain = domain();
    let root = tempfile::tempdir()?;
    let backend = Arc::new(Backend::default());
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.path().join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        backend.clone(),
        [42; 32],
    )
    .await?;
    let tls = Credentials::new()?;
    let server =
        ControllerServer::bind(domain.clone(), controller.clone(), options(), tls.server()?)
            .await?;
    let address = server.address();
    let mut forbidden = serde_json::to_value(wire::Request {
        format: 1,
        identity: controller.identity(),
        limits: controller.options().limits,
        claim: claim(&controller.identity(), 2, 1)?,
        action: wire::Action::Pending,
    })?;
    forbidden["action"] = serde_json::json!({"method":"recover_history"});
    let forbidden = serde_json::to_vec(&forbidden)?;
    domain
        .run(async {
            for payload in [None, Some(forbidden.as_slice())] {
                let (connector, name, _) = tls.client(false)?.build()?;
                let mut stream = connector
                    .connect(name, tokio::net::TcpStream::connect(address).await?)
                    .await?;
                if let Some(payload) = payload {
                    stream.write_u32(payload.len() as u32).await?;
                    stream.write_all(payload).await?;
                } else {
                    stream.write_u32(u32::MAX).await?;
                }
                assert!(
                    wire::read::<wire::Response>(&mut stream, options().max_frame_bytes)
                        .await
                        .is_err()
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    let stalled = domain
        .run(tokio::net::TcpStream::connect(address))
        .await??;
    domain
        .run(async { cellar::asyncrt::timeout(Duration::from_secs(1), server.shutdown()).await })
        .await???;
    drop(stalled);
    let rebound = domain.run(tokio::net::TcpListener::bind(address)).await??;
    drop(rebound);
    assert_eq!(backend.calls.load(Ordering::Acquire), 0);
    assert_eq!(backend.fences.load(Ordering::Acquire), 0);
    controller.shutdown().await?;
    domain.shutdown().await;
    Ok(())
}
