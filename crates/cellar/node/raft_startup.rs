// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Validated Raft startup shared by embedded nodes and the process runner.
use crate::application::consensus::{RaftCells, RaftCellsConfig, RaftCellsServices};
use crate::asyncrt::DomainToken;
use crate::peer_auth::PeerAuth;
use std::{path::Path, sync::Arc};

pub(super) struct Startup<'a> {
    pub domain: DomainToken,
    pub data_dir: &'a Path,
    pub auth: Arc<PeerAuth>,
}

impl Startup<'_> {
    pub async fn start(
        &self,
        config: RaftCellsConfig,
        services: RaftCellsServices,
    ) -> anyhow::Result<RaftCells> {
        let starting = crate::asyncrt::services().raft_lifecycle().begin(
            config.host.managed.is_some(),
            config.host.managed.as_ref().is_some_and(|m| {
                m.authorization.state()
                    == crate::raft::fleet_catalog::FleetNodeState::RestartPending
            }),
        )?;
        match self.inner(config, services).await {
            Ok((service, stop)) => match starting.install(service.clone(), stop) {
                Ok(()) => Ok(service),
                Err(error) => {
                    let _ = service.close().await;
                    Err(error.into())
                }
            },
            Err(error) => {
                starting.failed();
                Err(error)
            }
        }
    }

    async fn inner(
        &self,
        mut config: crate::application::consensus::RaftCellsConfig,
        services: crate::application::consensus::RaftCellsServices,
    ) -> anyhow::Result<(
        crate::application::consensus::RaftCells,
        Option<crate::raft::fleet_stop::Services>,
    )> {
        if let Ok(cron) = services
            .application
            .resolve_raft_class(cellar_logic::cron::RESERVED_CLASS)
        {
            anyhow::ensure!(
                config.namespaces.iter().any(|namespace| {
                    namespace.class == cellar_logic::cron::RESERVED_CLASS
                        && namespace.namespace.schema_digest == cron.schema_digest()
                }),
                "Raft cron requires its declared shared namespace"
            );
        }
        anyhow::ensure!(
            config.infrastructure.log.directory == self.data_dir.join("raft"),
            "Raft log must use this node's raft directory"
        );
        if services.recovery.is_some() {
            anyhow::ensure!(
                config.host.managed.is_none()
                    && config.infrastructure.log.mode == crate::raft::options::LogOpenMode::Resume,
                "recovered startup requires an existing seeded log and unmanaged recovery authority"
            );
        }
        let (performance, prepared) = self
            .domain
            .run(async {
                let performance = crate::raft::performance::Registry::new(self.domain.clone());
                let store = crate::raft::object_metrics::wrap(services.store, performance.clone());
                let prepared = crate::raft::group_host::prepare(
                    &mut config,
                    services.application.as_ref(),
                    store,
                    &services.versions,
                )
                .await?;
                Ok::<_, anyhow::Error>((performance, prepared))
            })
            .await??;
        let infrastructure = self
            .infrastructure(
                config.infrastructure,
                services.listeners,
                services.tls,
                Some(performance),
                config.host.settle_periodic_raft_purges,
            )
            .await?;
        let log = infrastructure.log.clone();
        let runtime = infrastructure.runtime.clone();
        let transport = infrastructure.transport.clone();
        let result = self
            .domain
            .run(async {
                let service = crate::raft::group_host::start(
                    infrastructure,
                    prepared,
                    self.data_dir.join("raft-cells"),
                    config.host,
                    config.shared,
                    services.versions,
                    services.recovery,
                )
                .await?;
                match service.fleet_stop_services() {
                    Ok(stop) => Ok((service, stop)),
                    Err(error) => {
                        let _ = service.close().await;
                        Err(anyhow::Error::new(error))
                    }
                }
            })
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        if result.is_err() {
            let _ = runtime.close().await;
            transport.close().await;
            let _ = log.close().await;
        }
        result
    }

    pub(super) async fn infrastructure(
        &self,
        config: super::NodeRaftConfig,
        listeners: super::NodeRaftListeners,
        tls: crate::application::consensus::verification::RaftTls,
        performance: Option<Arc<crate::raft::performance::Registry>>,
        settle_periodic_purges: bool,
    ) -> anyhow::Result<super::NodeRaftService> {
        use crate::raft::{
            cell_groups::CellGroupRuntime, storage::ReplicaLog, transport::RaftTransport,
        };
        anyhow::ensure!(
            config.log.directory == self.data_dir.join("raft"),
            "Raft log must use this node's raft directory"
        );
        self.domain
            .run(async {
                let log = ReplicaLog::open_observed(
                    config.log,
                    crate::asyncrt::fs(),
                    performance,
                    settle_periodic_purges,
                )
                .await?;
                let (runtime, events) =
                    match CellGroupRuntime::start(log.clone(), config.consensus).await {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = log.close().await;
                            return Err(anyhow::Error::new(error));
                        }
                    };
                let transport = match RaftTransport::start_shared(
                    runtime.clone(),
                    self.auth.clone(),
                    tls,
                    listeners.control,
                    listeners.data,
                    listeners.query,
                    config.transport,
                )
                .await
                {
                    Ok(transport) => transport,
                    Err(error) => {
                        let _ = runtime.close().await;
                        let _ = log.close().await;
                        return Err(anyhow::Error::new(error));
                    }
                };
                Ok(super::NodeRaftService {
                    log,
                    runtime,
                    events,
                    transport,
                })
            })
            .await?
    }
}
