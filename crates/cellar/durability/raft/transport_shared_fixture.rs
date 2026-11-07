// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Synced shared engines and real TLS listeners for transport ownership tests.
//! Groups are parked: these fixtures grant no quorum or serving authority.
use super::*;
use crate::raft::{
    cell_groups::{CellGroupRuntime, CellGroupRuntimeEvents},
    options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions},
    storage::{CellGroupBootstrap, ReplicaLog},
};
use cellar_logic::consensus::{DiskLineage, ProcessGeneration};

pub(super) struct Fixture {
    pub(super) domain: crate::asyncrt::ExecutionDomain,
    pub(super) log: ReplicaLog,
    pub(super) runtime: CellGroupRuntime,
    pub(super) transport: RaftTransport,
    _events: CellGroupRuntimeEvents,
    _directory: tempfile::TempDir,
}

impl Fixture {
    pub(super) async fn start(
        options: TransportOptions,
        groups: Vec<CellGroupBootstrap>,
    ) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    directory.path().join("log"),
                    NodeId::new([1; 16])?,
                    DiskLineage::new([2; 16])?,
                    ProcessGeneration::new([3; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        for group in groups {
            log.create_cell_group(group).await?;
        }
        let (runtime, events) =
            CellGroupRuntime::start(log.clone(), RaftRuntimeOptions::default()).await?;
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
        let transport = RaftTransport::start_shared(
            runtime.clone(),
            Arc::new(PeerAuth::new([8; 32], "node-1")?),
            tls,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            options,
        )
        .await?;
        Ok(Self {
            domain,
            log,
            runtime,
            transport,
            _events: events,
            _directory: directory,
        })
    }

    pub(super) async fn close(self) -> anyhow::Result<()> {
        self.transport.close().await;
        self.runtime.close().await?;
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}
