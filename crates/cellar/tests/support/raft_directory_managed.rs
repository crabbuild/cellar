// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root and attach both directories for authenticated inspection tests.
use super::*;
use cellar::application::consensus::verification::cell_groups::RangeDirectory;
use cellar::bucket::Bucket;

pub(super) struct Managed {
    _root: tempfile::TempDir,
    pub(super) domain: Arc<ExecutionDomain>,
    pub(super) fleet: FleetCatalog,
    catalog: Catalog,
    pub(super) namespaces: Vec<RaftNamespaceRegistration>,
    directories: Vec<RangeDirectory>,
}

fn request(id: u8, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [id; 16],
        operation: [id; 16],
        expected_revision: revision,
        principal: "directory-qualification".into(),
    }
}

impl Managed {
    pub(super) async fn open(
        store: Arc<store::Store>,
        namespaces: &[NamespaceConfig],
        class: &RaftClass,
    ) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
        let (fleet, catalog, namespaces, directories) = domain
            .run(async {
                let fleet = FleetCatalog::new(
                    &domain,
                    Bucket::open_dev(&root.path().join("fleet.sqlite"))?,
                    namespaces[0].cluster,
                    IncarnationId::new([80; 16])?,
                    "directory/",
                )?;
                fleet.initialize(TIMEOUT).await?;
                let catalog = Catalog::new(store, catalog_options())?;
                let mut declarations = Vec::new();
                let mut directories = Vec::new();
                for (index, config) in namespaces.iter().enumerate() {
                    let cut = fleet.read(TIMEOUT).await?;
                    let declaration = fleet
                        .provision_managed_sql_namespace(
                            &request(10 + index as u8, cut.revision()),
                            &catalog,
                            config.namespace,
                            class,
                            limits().directory,
                            TIMEOUT,
                        )
                        .await?;
                    let namespace = catalog
                        .existing_namespace(declaration.namespace.clone())
                        .await?;
                    directories.push(RangeDirectory::new(namespace, limits().directory)?);
                    declarations.push(declaration);
                }
                Ok::<_, anyhow::Error>((fleet, catalog, declarations, directories))
            })
            .await??;
        Ok(Self {
            _root: root,
            domain,
            fleet,
            catalog,
            namespaces,
            directories,
        })
    }

    pub(super) async fn enroll(&self, peers: &[RaftPeerNode]) -> Result<()> {
        self.domain
            .run(async {
                let capability_digest =
                    RaftManagedHostOptions::capability_digest(&self.namespaces)?;
                for (index, peer) in peers.iter().enumerate() {
                    let cut = self.fleet.read(TIMEOUT).await?;
                    self.fleet
                        .register_node(
                            &cut,
                            &request(20 + index as u8, cut.revision()),
                            &FleetNodeSpec {
                                node: peer.node,
                                disk: peer.disk,
                                physical_host: IncarnationId::new([90 + index as u8; 16])?,
                                label: peer.label.clone(),
                                server_name: peer.server_name.clone(),
                                capability_digest,
                                storage_instance: None,
                            },
                            TIMEOUT,
                        )
                        .await?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?
    }

    pub(super) async fn attach(&self, cluster: &Cluster) -> Result<()> {
        self.domain
            .run(async {
                for index in 0..cluster.services.len() {
                    let service = cluster.service(index)?;
                    let observed = service.observe_node(TIMEOUT).await?;
                    ensure!(observed.node.groups == 0);
                    let attestation = FleetCapacityAttestation {
                        boot: observed
                            .fleet_boot
                            .context("directory qualification boot")?,
                        profile_revision: 1,
                        profile_digest: fleet_capacity_profile_digest(&observed.node.profile)?,
                        cpu_millis: 1000,
                        memory_bytes: 3 * 1024 * 1024 * 1024,
                        durable_bytes: observed.node.profile.byte_capacity,
                        engine_slots: 2,
                        memory_swap_disabled: true,
                        used_memory_bytes: 64 * 1024 * 1024,
                        used_durable_bytes: 0,
                        evidence_digest: [75; 32],
                        evidence_reference: "fixture/directory".into(),
                    };
                    for (offset, action) in
                        [FleetCapacityAction::Qualify, FleetCapacityAction::Activate]
                            .into_iter()
                            .enumerate()
                    {
                        let cut = self.fleet.read(TIMEOUT).await?;
                        cluster.inspect(index, index, cut.revision()).await?;
                        self.fleet
                            .admit_node_capacity(
                                &cut,
                                &FleetCapacityRequest {
                                    identity: request(
                                        30 + (index * 2 + offset) as u8,
                                        cut.revision(),
                                    ),
                                    attestation: attestation.clone(),
                                    action,
                                },
                                service,
                                &self.directories,
                                TIMEOUT,
                            )
                            .await?;
                    }
                }
                for (index, birth) in cluster.births.iter().enumerate() {
                    let namespace = &self.namespaces[index];
                    let standby = self
                        .fleet
                        .observe_managed_standby(
                            &self.catalog,
                            namespace,
                            limits().directory,
                            TIMEOUT,
                        )
                        .await?;
                    let cut = self.fleet.read(TIMEOUT).await?;
                    let identity = request(50 + index as u8 * 2, cut.revision());
                    let reservation = self
                        .fleet
                        .reserve_managed_first_group(
                            &self.catalog,
                            namespace,
                            RaftManagedFirstGroupRequest {
                                reservation: identity.clone(),
                                standby,
                                publication: ProposalId::new([64 + index as u8; 16])?,
                                group: birth.clone(),
                            },
                            limits().directory,
                            TIMEOUT,
                        )
                        .await?;
                    let cut = self.fleet.read(TIMEOUT).await?;
                    let mut confirmation = request(51 + index as u8 * 2, cut.revision());
                    confirmation.operation = identity.operation;
                    let group = self
                        .fleet
                        .publish_managed_first_group(&reservation, &confirmation, TIMEOUT)
                        .await?;
                    let cut = self.fleet.read(TIMEOUT).await?;
                    for voter in 0..VOTERS {
                        cluster.inspect(voter, voter, cut.revision()).await?;
                        cluster
                            .service(voter)?
                            .activate_managed_initial_group(group.clone(), TIMEOUT)
                            .await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?
    }
}
