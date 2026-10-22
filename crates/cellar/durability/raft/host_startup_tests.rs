//! A startup error owns infrastructure until actual file-lock release.
#![allow(clippy::disallowed_methods)]
use super::storage_fault as fault;
use super::*;
use crate::raft::{
    options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions},
    storage::ReplicaLog,
    transport_options::{RaftTls, TransportOptions},
    version::{VersionKeys, VersionSigningKey},
};
use anyhow::Context as _;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_preflight_joins_infrastructure_with_retained_handles() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let options = ReplicaLogOptions::new(
        root.path().join("raft"),
        NodeId::new([1; 16])?,
        DiskLineage::new([2; 16])?,
        ProcessGeneration::new([3; 16])?,
        LogOpenMode::Create,
    );
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let (runtime, events) =
        RaftRuntime::start_parked(log.clone(), RaftRuntimeOptions::default()).await?;
    let tls = RaftTls::new(
        vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into()],
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?;
    let transport = RaftTransport::start(
        runtime.clone(),
        Arc::new(crate::peer_auth::PeerAuth::new([8; 32], "startup")?),
        tls,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        TransportOptions::default(),
    )
    .await?;
    let service = crate::daemon::NodeRaftService {
        log: log.clone(),
        runtime: runtime.clone(),
        events,
        transport: transport.clone(),
    };
    let release = gate.arm(fault::HOLD_LOCK_DROP);
    let mut startup = Box::pin(domain.run(start(
        service,
        Registrations {
            cells: vec![],
            peers: vec![],
            origins: vec![],
        },
        root.path().join("cells"),
        "startup".into(),
        RaftHostOptions::default(),
        VersionKeys::new(
            VersionSigningKey::new([31; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(86400),
        )?,
    )));
    let early = tokio::time::timeout(Duration::from_millis(30), &mut startup).await;
    let returned_early = early.is_ok();
    if !returned_early {
        let waiting = futures_util::future::select(Box::pin(gate.entered.notified()), &mut startup);
        match tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .context("wait for startup lock-drop gate")?
        {
            futures_util::future::Either::Left(_) => (),
            futures_util::future::Either::Right(_) => {
                anyhow::bail!("startup settled while lock drop was held")
            }
        }
    }
    drop(release);
    let result = match early {
        Ok(result) => result,
        Err(_) => tokio::time::timeout(Duration::from_secs(5), startup)
            .await
            .context("wait for failed startup after lock release")?,
    }?;
    let admission_closed = matches!(
        log.storage_instance(),
        Err(super::super::storage::LogError::Closed)
    );
    if !returned_early {
        // This is deliberately before cleanup through the retained handles.
        let mut resumed = options;
        resumed.mode = LogOpenMode::Resume;
        resumed.process = ProcessGeneration::new([9; 16])?;
        let reopened = domain
            .run(ReplicaLog::open(
                resumed,
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        reopened.close().await?;
    }
    let _ = runtime.close().await;
    transport.close().await;
    log.close().await?;
    domain.shutdown().await;
    assert!(
        result.is_err(),
        "empty registration unexpectedly started a host"
    );
    assert!(
        !returned_early,
        "failed startup returned while its infrastructure still owned storage"
    );
    assert!(
        admission_closed,
        "failed startup left retained storage handles admitted"
    );
    Ok(())
}
