// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Opt-in managed lab deployment. The ordinary performance fixture stays static.
use super::*;
use cellar::docker::{ContainerId, Docker};
use cellar::sdk::consensus::verification::{Catalog, FleetCatalog, NamespaceCatalog};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;

mod config;
mod recovery;
pub use config::Config;

#[cfg(test)]
#[path = "../raft_fleet_tests.rs"]
mod tests;

const TIMEOUT: Duration = Duration::from_secs(5);
const PREFIX: &str = "perf/managed-fleet/";

pub struct Managed {
    pub domain: ExecutionDomain,
    pub catalog: FleetCatalog,
    pub namespace: NamespaceCatalog,
    pub declaration: RaftNamespaceRegistration,
    pub config: Config,
}

impl Managed {
    pub async fn open(
        bucket: Bucket,
        config: Config,
        namespace: NamespaceConfig,
        class: &str,
        initialize: bool,
    ) -> Result<Self> {
        config.validate()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = async {
            let catalog = FleetCatalog::new(
                &domain,
                bucket.clone(),
                namespace.cluster,
                IncarnationId::new(config.incarnation)?,
                PREFIX,
            )?;
            if initialize {
                // Every container shares one physical VM. Never advertise host separation.
                catalog
                    .initialize_with_replica_separation(FleetReplicaSeparation::Nodes, TIMEOUT)
                    .await?;
            } else {
                ensure!(
                    catalog.read(TIMEOUT).await?.replica_separation()
                        == FleetReplicaSeparation::Nodes,
                    "managed lab requires explicit node separation"
                );
            }
            let mut namespace = catalog.bind_namespace(namespace)?;
            namespace.image_storage = RaftImageStorage::Extents;
            let declaration = RaftNamespaceRegistration {
                namespace: namespace.clone(),
                class: class.into(),
            };
            let data = Catalog::new(bucket.raft_store(), Self::catalog_options())?;
            let namespace = domain
                .run(async {
                    if initialize {
                        data.open_namespace(namespace).await
                    } else {
                        data.existing_namespace(namespace).await
                    }
                })
                .await??;
            Ok::<_, anyhow::Error>((catalog, namespace, declaration))
        }
        .await;
        match result {
            Ok((catalog, namespace, declaration)) => Ok(Self {
                domain,
                catalog,
                namespace,
                declaration,
                config,
            }),
            Err(error) => {
                domain.shutdown().await;
                Err(error)
            }
        }
    }

    pub fn catalog_options() -> CatalogOptions {
        CatalogOptions {
            prefix: PREFIX.into(),
            ..Default::default()
        }
    }

    /// Only run before starting hosts. Fixed revisions and identities allow
    /// exact retries after an unknown response, including after later writes.
    /// Conflicting bootstrap inputs fail; they never silently adopt a fleet.
    pub async fn bootstrap(&self) -> Result<Value> {
        let cut = self.catalog.read(TIMEOUT).await?;
        self.catalog
            .declare_namespace(
                &cut,
                &self.config.request(1, 1),
                &self.namespace,
                &self.declaration.class,
                TIMEOUT,
            )
            .await?;
        for index in 1..=self.config.nodes {
            let cut = self.catalog.read(TIMEOUT).await?;
            let spec = FleetNodeSpec {
                node: NodeId::new([index; 16])?,
                disk: DiskLineage::new([index + 10; 16])?,
                physical_host: IncarnationId::new(self.config.physical_host)?,
                label: format!("raft-node{index}"),
                server_name: "localhost".into(),
                capability_digest: RaftManagedHostOptions::capability_digest(
                    std::slice::from_ref(&self.declaration),
                )?,
                storage_instance: None,
            };
            self.catalog
                .register_node(
                    &cut,
                    &self.config.request(index + 1, u64::from(index) + 1),
                    &spec,
                    TIMEOUT,
                )
                .await?;
        }
        Ok(json!({"catalog": hex::encode(self.catalog.catalog_id()?),
            "revision": self.catalog.read(TIMEOUT).await?.revision(), "nodes": self.config.nodes}))
    }

    pub async fn configure(&self, config: &mut RaftCellsConfig, node: NodeId) -> Result<()> {
        let cut = self.catalog.read(TIMEOUT).await?;
        let authorization = self.catalog.authorize_host(&cut, node, TIMEOUT).await?;
        ensure!(authorization.state() == FleetNodeState::Registered, "managed startup requires a new enrolled boot; use the intact-return protocol for retained volumes");
        config.cells.clear();
        config.peers.retain(|peer| peer.node == node);
        ensure!(
            config.peers.len() == 1,
            "managed node needs its own endpoint"
        );
        config.catalog = Self::catalog_options();
        let host = &mut config.host;
        host.max_groups = self.config.group_capacity as usize;
        host.close_report.max_groups = host.max_groups;
        host.images.max_bytes = self.config.image_bytes * 8;
        host.node_profile = Some(RaftNodeProfile {
            pool: "managed-lab".into(),
            region: "local".into(),
            zone: "shared-colima".into(),
            group_capacity: self.config.group_capacity,
            byte_capacity: self.config.durable_bytes,
            bytes_per_group: host.images.max_bytes,
            leader_capacity: self.config.group_capacity,
            draining: false,
        });
        host.node_directory = Some(RaftNodeDirectoryOptions::default());
        host.managed = Some(RaftManagedHostOptions {
            authorization,
            namespaces: vec![self.declaration.clone()],
        });
        config.infrastructure.log.max_groups = host.max_groups;
        config.infrastructure.consensus.max_groups = host.max_groups;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
enum Command {
    Capacity {
        request: Box<FleetCapacityRequest>,
    },
    Reserve {
        request: FleetRequestIdentity,
        history: History,
        voters: [u8; 3],
    },
    Confirm {
        request: FleetRequestIdentity,
        history: History,
    },
    ActivateInitial {
        history: History,
    },
    StartRecovery,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct History {
    cell: u32,
    group: [u8; 16],
    incarnation: [u8; 16],
}

impl History {
    fn scope(&self, declaration: &RaftNamespaceRegistration) -> Result<GroupScope> {
        ensure!(self.cell < 1_000_000, "cell outside lab range");
        Ok(GroupScope {
            cluster: declaration.namespace.cluster,
            namespace: declaration.namespace.namespace,
            cell: format!("{}:{}", declaration.class, self.cell),
            group: GroupId::new(self.group)?,
            incarnation: IncarnationId::new(self.incarnation)?,
        })
    }
}

struct Api {
    managed: Arc<Managed>,
    cells: RaftCells,
    isolation: FleetDockerIsolation,
    resources: FleetDockerResources,
    node: NodeId,
    container: ContainerId,
    turn: Mutex<()>,
    recovery: std::sync::Mutex<Option<FleetMovementService>>,
}

impl Api {
    async fn command(&self, command: Command) -> Result<Value> {
        let _guard = self
            .turn
            .try_lock()
            .context("another deployment operation is in progress")?;
        let m = &self.managed;
        let cut = m.catalog.read(TIMEOUT).await?;
        match command {
            Command::StartRecovery => self.start_recovery().await,
            Command::Capacity { request } => {
                ensure!(
                    request.attestation.boot.node == self.node.bytes(),
                    "capacity request must name this node"
                );
                if request.action == FleetCapacityAction::Activate
                    && request.identity.expected_revision == cut.revision()
                {
                    // Ready -> exact immutable container binding -> live resource
                    // measurement -> Active. Failure never admits placement.
                    self.isolation
                        .bind_node(self.node, &self.container, TIMEOUT)
                        .await?;
                    self.resources.observe(self.node, TIMEOUT).await?;
                }
                // An older revision can only reconcile its committed receipt:
                // native admission refuses a new mutation at any other root.
                // Do not require a still-running process/Engine for that receipt.
                let accepted = m
                    .catalog
                    .admit_node_capacity(
                        &cut,
                        &request,
                        &self.cells,
                        std::slice::from_ref(&m.namespace),
                        TIMEOUT,
                    )
                    .await?;
                Ok(
                    json!({"revision":accepted.applied_revision(), "state":format!("{:?}",accepted.state())}),
                )
            }
            Command::Reserve {
                request,
                history,
                voters,
            } => {
                ensure!(
                    voters.iter().all(|n| (1..=m.config.nodes).contains(n)),
                    "unknown initial voter"
                );
                let proposal = BirthProposal {
                    scope: history.scope(&m.declaration)?,
                    voters: voters
                        .into_iter()
                        .map(|n| {
                            Ok(InitialMember {
                                node: NodeId::new([n; 16])?,
                                member: MemberId::new(u64::from(n))?,
                            })
                        })
                        .collect::<Result<_>>()?,
                    learners: vec![],
                };
                let reserved = m
                    .catalog
                    .reserve_birth(&cut, &request, &m.namespace, proposal, TIMEOUT)
                    .await?;
                m.domain
                    .run(
                        m.namespace
                            .resolve_managed_birth(&reserved.authorization, TIMEOUT),
                    )
                    .await??;
                Ok(json!({"reserved": true, "revision":m.catalog.read(TIMEOUT).await?.revision()}))
            }
            Command::Confirm { request, history } => {
                let scope = history.scope(&m.declaration)?;
                let auth = m
                    .catalog
                    .recover_birth_reservation(&cut, &m.namespace, &scope, TIMEOUT)
                    .await?;
                // A lost reserve response can leave either side of the physical
                // birth boundary. Reconcile its original authorization first.
                m.domain
                    .run(m.namespace.resolve_managed_birth(&auth, TIMEOUT))
                    .await??;
                let committed = m
                    .catalog
                    .confirm_birth(&cut, &request, &m.namespace, &auth, TIMEOUT)
                    .await?;
                Ok(json!({"revision":committed.applied_revision}))
            }
            Command::ActivateInitial { history } => {
                let scope = history.scope(&m.declaration)?;
                let auth = m.catalog.authorize_group(&cut, &scope, TIMEOUT).await?;
                let cell = self
                    .cells
                    .register_managed_history(auth.clone(), TIMEOUT)
                    .await?;
                let guard = cell.activate_managed_initial_member(auth, TIMEOUT).await?;
                Ok(
                    json!({"changed_at":guard.changed_at.get(), "consumed_sequence":guard.consumed_sequence}),
                )
            }
        }
    }

    async fn status(&self) -> Result<Value> {
        let cut = self.managed.catalog.read(TIMEOUT).await?;
        let auth = self
            .managed
            .catalog
            .authorize_host(&cut, self.node, TIMEOUT)
            .await?;
        let view = self
            .cells
            .inspect_peer_node(self.node, cut.revision(), TIMEOUT)
            .await?
            .view;
        Ok(
            json!({"revision":cut.revision(), "state":format!("{:?}",auth.state()),
            "boot":view.fleet_boot, "profile_digest":fleet_capacity_profile_digest(&view.node.profile)?,
            "groups":view.node.groups, "reserved_bytes":view.node.reserved_bytes,
            "bounded_buffer_bytes":view.startup_bounds.bounded_buffer_bytes,
            "registered_histories":view.registered_histories, "trust_revision":view.fleet_trust_revision,
            "group_capacity":view.node.profile.group_capacity, "durable_bytes":view.node.profile.byte_capacity,
            "recovery":self.recovery_status()?}),
        )
    }
}

pub fn router(
    managed: Arc<Managed>,
    cells: RaftCells,
    node: NodeId,
    secret: String,
    container: ContainerId,
) -> Result<axum::Router> {
    let isolation = FleetDockerIsolation::new(
        managed.catalog.clone(),
        Docker::new("/var/run/docker.sock"),
        &managed.config.owner,
        vec![managed.namespace.clone()],
    )?;
    let resources = FleetDockerResources::new(
        isolation.clone(),
        cells.clone(),
        managed.config.resources.clone(),
    )?;
    let api = Arc::new(Api {
        managed,
        cells,
        isolation,
        resources,
        node,
        container,
        turn: Mutex::new(()),
        recovery: std::sync::Mutex::new(None),
    });
    Ok(axum::Router::new().route(
        "/fleet",
        axum::routing::any(move |request: axum::extract::Request| {
            let api = api.clone();
            let secret = secret.clone();
            async move {
                use axum::http::{Method, StatusCode};
                if request
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    != Some(format!("Bearer {secret}").as_str())
                {
                    return (
                        StatusCode::UNAUTHORIZED,
                        json!({"error":"unauthorized"}).to_string(),
                    );
                }
                let method = request.method().clone();
                if method != Method::GET && method != Method::POST {
                    return (StatusCode::METHOD_NOT_ALLOWED, "method not allowed".into());
                }
                let result = api
                    .managed
                    .domain
                    .run(async {
                        cellar::asyncrt::timeout(Duration::from_secs(30), async {
                            if method == Method::GET {
                                return api.status().await;
                            }
                            let bytes =
                                axum::body::to_bytes(request.into_body(), 16 * 1024).await?;
                            api.command(serde_json::from_slice(&bytes)?).await
                        })
                        .await
                        .context("deployment operation timed out; reconcile the exact request")?
                    })
                    .await;
                match result {
                    Ok(Ok(value)) => (StatusCode::OK, value.to_string()),
                    error => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        json!({"error":format!("{error:?}"),
                    "outcome":"unconfirmed; retain and reconcile the original request"})
                        .to_string(),
                    ),
                }
            }
        }),
    ))
}
