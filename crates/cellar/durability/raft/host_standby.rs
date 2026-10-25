// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registered standby initialization stays in the existing node's serialized
//! host, engine and SQL pool. Waiter lifetime never owns a submitted initializer.
use super::*;
use crate::raft::transport::seeds::SeedOptions;

#[derive(Clone)]
pub(super) struct Dormant {
    pub namespace: NamespaceCatalog,
    pub birth: GroupBirth,
    pub class: Arc<RaftClass>,
    pub storage: sql::CellStorage,
    pub worker: usize,
    pub failed: bool,
}

pub(super) enum Completed {
    Seeded {
        request: Box<Request>,
        member: MemberId,
        result: anyhow::Result<BootstrapPermit>,
    },
    Initialized {
        request: Box<Request>,
        member: MemberId,
        permit: BootstrapPermit,
        result: Box<Result<sql::Output, ExecutionError>>,
    },
    Attached {
        request: Box<Request>,
        member: MemberId,
        result: Result<(), CatalogError>,
    },
}

struct Initializer {
    dormant: Dormant,
    activation: client::LearnerActivation,
    source: Option<RaftPeerNode>,
    log: super::super::storage::ReplicaLog,
    transport: RaftTransport,
    worker: sql::Worker,
    limits: super::super::snapshot::SnapshotLimits,
    archive: super::super::archive::ArchiveLimits,
    deadline: u64,
}

impl Initializer {
    async fn prepare(self) -> anyhow::Result<BootstrapPermit> {
        let Self {
            dormant,
            activation,
            source,
            log,
            transport,
            worker,
            limits,
            archive,
            deadline,
        } = self;
        // This preflight refuses a claimed blank identity and reconciles an
        // earlier synced initialization before considering another network seed.
        let existing = transport
            .seed_control_until(deadline, async {
                Ok(dormant
                    .namespace
                    .resume_joined(&dormant.birth, &log, activation.member)
                    .await)
            })
            .await??;
        if let Some(permit) = existing {
            return Ok(permit);
        }
        let peer = source.ok_or_else(|| anyhow::anyhow!("missing standby seed source"))?;
        peer.validate_endpoint()?;
        let local = log.limits();
        anyhow::ensure!(
            peer.node != local.node && activation.member != activation.source,
            "standby source identity"
        );
        let scope = dormant.birth.scope().clone();
        let target = ReplicaIdentity {
            scope: scope.clone(),
            node: local.node,
            disk: local.disk,
            process: local.process,
            member: activation.member,
        };
        let source = RaftPeer {
            replica: ReplicaIdentity {
                scope: scope.clone(),
                node: peer.node,
                disk: peer.disk,
                process: peer.process,
                member: activation.source,
            },
            label: peer.label,
            server_name: peer.server_name,
            control_address: peer.control_address,
            data_address: peer.data_address,
            query_address: peer.query_address,
        };
        let options = SeedOptions { limits, deadline };
        let action = if activation.from_s3 {
            let archive = dormant
                .namespace
                .cell_archive(&dormant.birth, dormant.storage.host.clone(), archive)?
                .with_scratch_directory(log.limits().directory.clone())
                .with_snapshot_scratch(log.snapshot_scratch());
            let received = transport
                .seed_from_archive_until(
                    target,
                    source,
                    &dormant.birth,
                    dormant.class.schema_digest(),
                    options,
                    archive,
                )
                .await?;
            Action::SealArchiveSeed {
                received: Box::new(received),
                storage: dormant.storage,
            }
        } else {
            let received = transport
                .seed_from_peer_until(
                    target,
                    source,
                    &dormant.birth,
                    dormant.class.schema_digest(),
                    options,
                )
                .await?;
            Action::SealSeed {
                received: Box::new(received),
                storage: dormant.storage,
            }
        };
        // This SQL future remains node-owned even after the caller disappears.
        // The sealed grant is rechecked before the atomic engine write.
        let output = worker.call(scope, action, None, None).await?;
        let Value::Seeded(seed) = output.value else {
            anyhow::bail!("standby SQL seed response")
        };
        log.create_seeded(*seed).await?;
        transport
            .seed_control_until(deadline, async {
                Ok(dormant
                    .namespace
                    .resume_joined(&dormant.birth, &log, activation.member)
                    .await)
            })
            .await??
            .ok_or_else(|| anyhow::anyhow!("standby state missing after initialization fence"))
    }
}

impl Cell {
    pub(super) fn recovered(
        group: GroupLog,
        birth: GroupBirth,
        archive: CellArchive,
        image: Option<ImageView>,
        worker: usize,
        minimum: Durability,
        schema_digest: [u8; 32],
    ) -> anyhow::Result<Self> {
        let progress = progress(&group, image.as_ref(), LogIndex::new(0))?
            .ok_or_else(|| anyhow::anyhow!("unverified recovered image"))?;
        Ok(Self {
            birth,
            route_revision: group.membership()?.revision(),
            configuration: None,
            retired: false,
            archive,
            archived: LogIndex::new(0),
            archive_retained: super::admission::Retained::default(),
            archive_retry_at: 0,
            group,
            schema_digest,
            worker,
            core: CommandState::new(progress, minimum)?,
            image,
            pending: None,
            sequence: 0,
            status_busy: false,
            prefer_delivery: false,
            sql_busy: false,
            apply: None,
            publication: None,
            apply_ticket: None,
            apply_retry_at: 0,
            failed: false,
            incoming: None,
            source_request: None,
            apply_inflight: false,
            checkpoint_busy: false,
            checkpoint_retry_at: 0,
        })
    }
}

impl Driver {
    fn standby_receipt(
        &self,
        scope: &GroupScope,
        member: MemberId,
    ) -> Result<client::LearnerReceipt, CommandFailure> {
        let cell = self.cells.get(scope).ok_or(CommandFailure::Unavailable)?;
        if cell.failed || cell.retired || cell.group.identity().member != member {
            return Err(CommandFailure::Unavailable);
        }
        let (binding, joined_at) = cell
            .group
            .membership()
            .map_err(|_| CommandFailure::CorruptState)?
            .joined(member)
            .map_err(|_| CommandFailure::CorruptState)?
            .ok_or(CommandFailure::Unavailable)?;
        if binding.node != cell.group.identity().node || binding.disk != cell.group.identity().disk
        {
            return Err(CommandFailure::CorruptState);
        }
        Ok(client::LearnerReceipt {
            scope: scope.clone(),
            member: binding,
            joined_at,
        })
    }

    fn fail_standby(&mut self, mut request: Box<Request>, error: CommandFailure, fatal: bool) {
        if self.activating.as_ref() == Some(&request.scope) {
            self.activating = None;
        }
        if fatal {
            if let Some(dormant) = self.standbys.get_mut(&request.scope) {
                dormant.failed = true;
            }
            if let Some(cell) = self.cells.get_mut(&request.scope) {
                cell.failed = true;
                cell.core.close();
            }
        }
        reply(&mut request, Err(error));
    }

    pub(super) fn activate_learner(
        &mut self,
        mut request: Request,
        activation: client::LearnerActivation,
    ) {
        let scope = request.scope.clone();
        let Some(dormant) = self.standbys.get(&scope).cloned() else {
            reply(&mut request, Err(CommandFailure::UnsupportedCapability));
            return;
        };
        if dormant.failed {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        if self.activating.is_some() {
            reply(&mut request, Err(CommandFailure::Overloaded));
            return;
        }
        if self.cells.contains_key(&scope) {
            let result = self
                .standby_receipt(&scope, activation.member)
                .map(Response::Learner);
            reply(&mut request, result);
            return;
        }
        // Placement is a hint. Recheck this node's current admission before a
        // new history consumes storage; exact existing members still resume.
        let admission = match self.log.group(&scope) {
            Ok(_) => Ok(()),
            Err(super::super::storage::LogError::NotFound) => self
                .options
                .node_profile
                .as_ref()
                .map_or(Ok(()), |profile| {
                    let groups = self
                        .log
                        .group_count()
                        .map_err(|_| CommandFailure::Unavailable)?;
                    let next = u64::try_from(groups)
                        .ok()
                        .and_then(|groups| groups.checked_add(1))
                        .and_then(|groups| groups.checked_mul(profile.bytes_per_group));
                    if profile.draining
                        || groups >= profile.group_capacity as usize
                        || !next.is_some_and(|bytes| bytes <= profile.byte_capacity)
                    {
                        Err(CommandFailure::Overloaded)
                    } else {
                        Ok(())
                    }
                }),
            Err(_) => Err(CommandFailure::Unavailable),
        };
        if let Err(error) = admission {
            reply(&mut request, Err(error));
            return;
        }
        let setup = (|| -> anyhow::Result<Initializer> {
            Ok(Initializer {
                worker: self.pool.worker(dormant.worker)?,
                dormant,
                source: self.peers.get(&activation.source_node).cloned(),
                activation: activation.clone(),
                log: self.log.clone(),
                transport: self.transport.clone(),
                limits: self.snapshot_limits,
                archive: self.options.archive.limits,
                deadline: request
                    .control
                    .deadline()
                    .min(self.transport.seed_deadline()?),
            })
        })();
        let initializer = match setup {
            Ok(initializer) => initializer,
            Err(_) => {
                reply(&mut request, Err(CommandFailure::Unavailable));
                return;
            }
        };
        if !request.control.begin_submission() {
            return;
        }
        self.activating = Some(scope);
        self.jobs.push(Box::pin(async move {
            Job::Standby(Box::new(Completed::Seeded {
                request: Box::new(request),
                member: activation.member,
                result: initializer.prepare().await,
            }))
        }));
    }

    pub(super) fn on_standby_job(&mut self, completed: Completed) {
        match completed {
            Completed::Seeded {
                request,
                member,
                result,
            } => {
                let permit = match result {
                    Ok(permit) if permit.replica().member == member => permit,
                    Ok(_) => {
                        self.fail_standby(request, CommandFailure::CorruptState, true);
                        return;
                    }
                    Err(error) => {
                        let fatal = matches!(
                            error.downcast_ref::<CatalogError>(),
                            Some(
                                CatalogError::LostStableState
                                    | CatalogError::Conflict(_)
                                    | CatalogError::Corrupt(_)
                                    | CatalogError::WrongMode
                            )
                        );
                        self.fail_standby(request, CommandFailure::Unavailable, fatal);
                        return;
                    }
                };
                let Some(dormant) = self.standbys.get(&request.scope) else {
                    self.fail_standby(request, CommandFailure::CorruptState, true);
                    return;
                };
                let group = match self.log.group(&request.scope) {
                    Ok(group) => group,
                    Err(_) => {
                        self.fail_standby(request, CommandFailure::CorruptState, true);
                        return;
                    }
                };
                let initialize = self.pool.call(
                    dormant.worker,
                    request.scope.clone(),
                    Action::Initialize {
                        group,
                        class: dormant.class.clone(),
                        storage: dormant.storage.clone(),
                    },
                    None,
                    None,
                );
                self.jobs.push(Box::pin(async move {
                    Job::Standby(Box::new(Completed::Initialized {
                        request,
                        member,
                        permit,
                        result: Box::new(initialize.await),
                    }))
                }));
            }
            Completed::Initialized {
                request,
                member,
                permit,
                result,
            } => {
                let result = (|| -> anyhow::Result<()> {
                    let output = (*result)?;
                    let Value::Initialized(image) = output.value else {
                        anyhow::bail!("standby SQL initialization response")
                    };
                    let dormant = self
                        .standbys
                        .get(&request.scope)
                        .ok_or_else(|| anyhow::anyhow!("standby registration missing"))?;
                    let group = self.log.group(&request.scope)?;
                    anyhow::ensure!(
                        permit.replica() == group.identity(),
                        "standby attachment identity"
                    );
                    let owner = self
                        .archive_owner
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("standby archive owner missing"))?;
                    let archive = dormant
                        .namespace
                        .cell_archive(&dormant.birth, ltx_host(), self.options.archive.limits)?
                        .with_scratch_directory(self.runtime.log().limits().directory.clone())
                        .with_snapshot_scratch(self.runtime.log().snapshot_scratch())
                        .with_cancellation(owner.cancellation.subscribe());
                    let cell = Cell::recovered(
                        group,
                        dormant.birth.clone(),
                        archive,
                        image,
                        dormant.worker,
                        dormant.namespace.minimum_durability(),
                        dormant.class.schema_digest(),
                    )?;
                    self.transport.install_group(membership::route(
                        &cell.group,
                        &cell.birth,
                        &self.peers,
                        cell.schema_digest,
                        &self.label,
                        &self.transport,
                    )?)?;
                    anyhow::ensure!(
                        !self.cells.contains_key(&request.scope),
                        "standby already initialized"
                    );
                    self.cells.insert(request.scope.clone(), cell);
                    Ok(())
                })();
                if result.is_err() {
                    self.fail_standby(request, CommandFailure::Unavailable, true);
                    return;
                }
                let runtime = self.runtime.clone();
                self.jobs.push(Box::pin(async move {
                    Job::Standby(Box::new(Completed::Attached {
                        request,
                        member,
                        result: permit.attach(&runtime).await,
                    }))
                }));
            }
            Completed::Attached {
                mut request,
                member,
                result,
            } => {
                if result.is_err() {
                    self.fail_standby(request, CommandFailure::Unavailable, true);
                    return;
                }
                let ready = self
                    .standby_receipt(&request.scope, member)
                    .and_then(|receipt| {
                        let binding = self
                            .local
                            .bindings
                            .get(&(request.scope.namespace, request.scope.cell.clone()))
                            .filter(|binding| binding.scope == request.scope)
                            .ok_or(CommandFailure::CorruptState)?;
                        binding.member.attach(member)?;
                        Ok(Response::Learner(receipt))
                    });
                self.activating = None;
                let result = if request.control.cancelled() {
                    Err(CommandFailure::Unavailable)
                } else {
                    ready
                };
                reply(&mut request, result);
            }
        }
    }
}
