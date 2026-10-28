// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Validated Raft startup shared by embedded nodes and the process runner.
use crate::application::consensus::{RaftCells, RaftCellsConfig, RaftCellsServices};
use crate::asyncrt::DomainToken;
use crate::peer_auth::PeerAuth;
use std::{path::Path, sync::Arc};

pub(super) struct Startup<'a> {
    pub domain: DomainToken,
    pub data_dir: &'a Path,
    pub node: &'a str,
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
        use crate::application::consensus::{RaftCellPlacement, RegistrationError};
        anyhow::ensure!(
            config.infrastructure.log.directory == self.data_dir.join("raft"),
            "Raft log must use this node's raft directory"
        );
        config.host.validate()?;
        config.infrastructure.transport.validate()?;
        config
            .infrastructure
            .transport
            .snapshot_limits(config.host.execution.max_image_bytes)?;
        anyhow::ensure!(
            (!config.cells.is_empty() || config.host.managed.is_some())
                && config.cells.len() <= config.host.max_groups,
            "invalid registered Raft cell count"
        );
        let mut definitions = Vec::new();
        for cell in &config.cells {
            anyhow::ensure!(
                cell.birth.voters.len() <= 32 && cell.birth.learners.len() <= 32,
                "registered birth membership exceeds the catalog bound"
            );
            if let Some(managed) = &config.host.managed {
                anyhow::ensure!(
                    managed
                        .namespaces
                        .iter()
                        .any(|n| n.namespace == cell.namespace && n.class == cell.class),
                    "cell is outside managed host capabilities"
                );
            }
            anyhow::ensure!(
                cell.namespace.image_storage
                    != crate::application::consensus::RaftImageStorage::Extents
                    || matches!(cell.placement, RaftCellPlacement::QueryRouter { .. })
                    || config.host.extents.is_some(),
                "extent namespace requires node extent storage"
            );
            anyhow::ensure!(
                !services.application.supports_cell(&cell.class)
                    && !services.application.supports_workflow(&cell.class),
                RegistrationError::UnsupportedFactory(cell.class.clone())
            );
            let class = services.application.resolve_raft_class(&cell.class)?;
            class.validate_host(&config.host)?;
            anyhow::ensure!(
                cell.namespace.mode == cellar_logic::NamespaceMode::Raft
                    && cell.namespace.schema_digest == class.schema_digest(),
                "Raft namespace mode/schema disagrees with registration"
            );
            anyhow::ensure!(
                cellar_logic::cell::valid_cell_scope(&cell.birth.scope.cell)
                    && cell
                        .birth
                        .scope
                        .cell
                        .split_once(':')
                        .is_some_and(|(name, _)| name == class.name()),
                "Raft cell scope must name its registered class"
            );
            definitions.push(class);
        }
        let mut declared_definitions = Vec::new();
        if let Some(managed) = &config.host.managed {
            anyhow::ensure!(
                managed.authorization.node() == config.infrastructure.log.node
                    && managed.authorization.disk() == config.infrastructure.log.disk,
                "managed startup identity mismatch"
            );
            for entry in &managed.namespaces {
                anyhow::ensure!(
                    !services.application.supports_cell(&entry.class)
                        && !services.application.supports_workflow(&entry.class),
                    RegistrationError::UnsupportedFactory(entry.class.clone())
                );
                let class = services.application.resolve_raft_class(&entry.class)?;
                class.validate_host(&config.host)?;
                anyhow::ensure!(
                    class.schema_digest() == entry.namespace.schema_digest,
                    "managed compiled schema mismatch"
                );
                declared_definitions.push(class);
            }
        }
        let (performance, registrations, declared_namespaces) = self
            .domain
            .run(async {
                let performance = crate::raft::performance::Registry::new(self.domain.clone());
                let store = crate::raft::object_metrics::wrap(services.store, performance.clone());
                let catalog = crate::raft::catalog::Catalog::new(store, config.catalog)?;
                let mut declared_namespaces = Vec::new();
                if let Some(managed) = &mut config.host.managed {
                    managed.authorization = managed
                        .authorization
                        .refresh(std::time::Duration::from_secs(5))
                        .await?;
                    if managed.authorization.state()
                        == crate::raft::fleet_catalog::FleetNodeState::RestartPending
                    {
                        crate::asyncrt::services()
                            .raft_lifecycle()
                            .hold_startup_maintenance();
                    }
                    for (entry, class) in managed.namespaces.iter().zip(declared_definitions) {
                        let namespace = catalog.existing_namespace(entry.namespace.clone()).await?;
                        namespace
                            .require_version_keys(
                                &services.versions,
                                u64::try_from(crate::asyncrt::wall_ms())?,
                            )
                            .await?;
                        declared_namespaces
                            .push(crate::raft::host::NamespaceRegistration { namespace, class });
                    }
                }
                let managed = config.host.managed.is_some();
                let mut registrations = Vec::new();
                for (cell, class) in config.cells.into_iter().zip(definitions) {
                    let namespace = match cell.placement {
                        RaftCellPlacement::Replica if !managed => {
                            catalog.open_namespace(cell.namespace).await?
                        }
                        _ => catalog.existing_namespace(cell.namespace).await?,
                    };
                    let now_ms = u64::try_from(crate::asyncrt::wall_ms())?;
                    namespace
                        .require_version_keys(&services.versions, now_ms)
                        .await?;
                    let birth =
                        match cell.placement {
                            RaftCellPlacement::Replica if !managed => {
                                namespace.resolve_birth(cell.birth).await?
                            }
                            _ => {
                                let mut expected = cell.birth;
                                expected.voters.sort_by_key(|member| member.member);
                                expected.learners.sort_by_key(|member| member.member);
                                let birth =
                                    namespace.birth(&expected.scope.cell).await?.ok_or_else(
                                        || anyhow::anyhow!("existing member/route birth missing"),
                                    )?;
                                anyhow::ensure!(
                                    birth.scope() == &expected.scope
                                        && birth.voters() == expected.voters.as_slice()
                                        && birth.learners() == expected.learners.as_slice(),
                                    "member/route disagrees with existing birth"
                                );
                                birth
                            }
                        };
                    registrations.push(crate::raft::host::Registration {
                        placement: cell.placement,
                        namespace,
                        birth,
                        class,
                    });
                }
                Ok::<_, anyhow::Error>((performance, registrations, declared_namespaces))
            })
            .await??;
        let service = self
            .infrastructure(
                config.infrastructure,
                services.listeners,
                services.tls,
                Some(performance),
            )
            .await?;
        let log = service.log.clone();
        let runtime = service.runtime.clone();
        let transport = service.transport.clone();
        let stop = config
            .host
            .managed
            .as_ref()
            .map(|managed| {
                Ok::<_, anyhow::Error>(crate::raft::fleet_stop::Services {
                    authorization: managed.authorization.clone(),
                    log: log.clone(),
                    signer: crate::asyncrt::services().peer_probe_signer()?,
                })
            })
            .transpose()?;
        let result = self
            .domain
            .run(crate::raft::host::start(
                service,
                crate::raft::host::Registrations {
                    cells: registrations,
                    namespaces: declared_namespaces,
                    peers: config.peers,
                    origins: config.query_origins,
                },
                self.data_dir.join("raft-cells"),
                self.node.to_owned(),
                config.host,
                services.versions,
            ))
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        if result.is_err() {
            // Domain entry/cancellation can fail before the host's owned
            // startup guard runs. Join the infrastructure here as well.
            let _ = runtime.close().await;
            transport.close().await;
            let _ = log.close().await;
        }
        result.map(|service| (service, stop))
    }

    pub(super) async fn infrastructure(
        &self,
        config: super::NodeRaftConfig,
        listeners: super::NodeRaftListeners,
        tls: crate::application::consensus::verification::RaftTls,
        performance: Option<Arc<crate::raft::performance::Registry>>,
    ) -> anyhow::Result<super::NodeRaftService> {
        use crate::application::consensus::verification::{RaftRuntime, RaftTransport, ReplicaLog};
        anyhow::ensure!(
            config.log.directory == self.data_dir.join("raft"),
            "Raft log must use this node's raft directory"
        );
        self.domain
            .run(async {
                let log = ReplicaLog::open_observed(config.log, crate::asyncrt::fs(), performance)
                    .await?;
                let (runtime, events) =
                    match RaftRuntime::start_parked(log.clone(), config.consensus).await {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = log.close().await;
                            return Err(anyhow::Error::new(error));
                        }
                    };
                let transport = match RaftTransport::start(
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
