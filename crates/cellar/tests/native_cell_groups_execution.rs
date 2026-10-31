// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real raft-rs owners, three independent engines and cell-addressed SQLite/LTX.
//! Selected fixed-pool and consensus boundaries; not SDK or snapshot qualification.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod fault;

#[path = "support/cell_group_runtime.rs"]
mod runtime_fixture;

#[path = "support/cell_group_checkpoint.rs"]
mod checkpoint_fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_persistence_materialized_revocation_and_sync_outcomes() -> Result<()> {
    Box::pin(runtime_fixture::seed_persistence(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_persistence_extents_revocation_and_sync_outcomes() -> Result<()> {
    Box::pin(runtime_fixture::seed_persistence(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_receiver_materialized_refuses_forged_transfers_and_revoked_process() -> Result<()> {
    Box::pin(runtime_fixture::seed_receiver_faults(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_receiver_extents_refuse_forged_transfers_and_revoked_process() -> Result<()> {
    Box::pin(runtime_fixture::seed_receiver_faults(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_install_materialized_replaces_deleted_voter_and_resumes() -> Result<()> {
    Box::pin(runtime_fixture::seed_install(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_install_extents_replaces_deleted_voter_and_resumes() -> Result<()> {
    Box::pin(runtime_fixture::seed_install(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_group_materialized_requires_quorum_and_bound_membership() -> Result<()> {
    runtime_fixture::seed_source(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_group_extents_require_quorum_and_bound_membership() -> Result<()> {
    runtime_fixture::seed_source(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn seed_group_refuses_isolated_leader_and_expired_stream() -> Result<()> {
    runtime_fixture::seed_minority().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn membership_group_materialized_joint_changes_preserve_cells() -> Result<()> {
    runtime_fixture::membership_lifecycle(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn membership_group_extent_joint_changes_preserve_cells() -> Result<()> {
    runtime_fixture::membership_lifecycle(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn membership_group_configuration_waits_for_sync_and_joins_close() -> Result<()> {
    runtime_fixture::membership_sync().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_controller_compacts_materialized_groups_during_writes() -> Result<()> {
    runtime_fixture::automatic_checkpoints(false, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_controller_compacts_extent_groups_during_writes() -> Result<()> {
    runtime_fixture::automatic_checkpoints(true, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_controller_busy_pool_preserves_apply_and_other_group_progress() -> Result<()> {
    runtime_fixture::automatic_checkpoints(false, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_tcp_refuses_invalid_streams_and_preserves_other_group_progress() -> Result<()> {
    runtime_fixture::tcp_protocol().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_tcp_accepted_transfer_survives_dropped_delivery() -> Result<()> {
    runtime_fixture::tcp_cancelled_delivery().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_tcp_failed_transfer_allows_raft_retry() -> Result<()> {
    runtime_fixture::tcp_retry().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_tcp_materialized_catches_up_and_reopens() -> Result<()> {
    runtime_fixture::tcp_publication(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_tcp_extents_catch_up_and_reopen() -> Result<()> {
    runtime_fixture::tcp_publication(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_local_publication_preserves_suffix_and_catches_up() -> Result<()> {
    runtime_fixture::local_publication(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_local_extents_preserve_suffix_and_catch_up() -> Result<()> {
    runtime_fixture::local_publication(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_local_runtime_joins_publication_shutdown() -> Result<()> {
    runtime_fixture::local_shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_owner_rejects_overflow_cuts_before_raft() -> Result<()> {
    runtime_fixture::checkpoint_bounds().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_owner_replays_matching_log_without_install() -> Result<()> {
    runtime_fixture::checkpoint_matching_log().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_owner_restores_only_after_raft_sync() -> Result<()> {
    runtime_fixture::checkpoint_owner(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_owner_restores_extents_only_after_raft_sync() -> Result<()> {
    runtime_fixture::checkpoint_owner(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_runtime_joins_accepted_snapshot_shutdown() -> Result<()> {
    runtime_fixture::checkpoint_shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_receive_installs_and_recovers_materialized_closure() -> Result<()> {
    runtime_fixture::install_checkpoint(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_receive_installs_and_recovers_extent_closure() -> Result<()> {
    runtime_fixture::install_checkpoint(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_receive_rejects_partial_corrupt_and_over_budget_streams() -> Result<()> {
    runtime_fixture::refuse_checkpoint().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_capture_streams_one_cut_during_writes() -> Result<()> {
    checkpoint_fixture::coherent().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_capture_rejects_stale_foreign_and_unpublished_roots() -> Result<()> {
    checkpoint_fixture::refusals().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_capture_retains_bounds_until_physical_release() -> Result<()> {
    checkpoint_fixture::ownership().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_stream_detects_corruption_and_closed_engine() -> Result<()> {
    checkpoint_fixture::corruption().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_owned_commands_survive_sql_disconnect() -> Result<()> {
    runtime_fixture::owned_commands(0, 0).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_owned_commands_seal_late_preparations() -> Result<()> {
    runtime_fixture::owned_commands(0, 1).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_owned_commands_survive_accepted_disconnect() -> Result<()> {
    runtime_fixture::owned_commands(1, 0).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_owned_commands_resolve_after_response_deadline() -> Result<()> {
    runtime_fixture::owned_commands(1, 1).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_owned_commands_join_cancelled_shutdown() -> Result<()> {
    runtime_fixture::owned_commands(0, 2).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_owned_commands_release_completed_callback_errors() -> Result<()> {
    runtime_fixture::owned_command_failure(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_owned_commands_join_a_lost_sql_reply() -> Result<()> {
    runtime_fixture::owned_command_failure(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn group_controller_drives_tls_apply_and_bounded_retry() -> Result<()> {
    runtime_fixture::controller_replication(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_group_controller_drives_tls_apply_and_bounded_retry() -> Result<()> {
    runtime_fixture::controller_replication(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn group_controller_checks_exact_owners() -> Result<()> {
    runtime_fixture::controller_owners().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn group_controller_cancelled_close_joins_sql() -> Result<()> {
    runtime_fixture::controller_close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn group_controller_apply_failure_and_shutdown_preserve_selected_root() -> Result<()> {
    runtime_fixture::controller_apply_faults(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_group_controller_apply_failure_and_shutdown_preserve_selected_root() -> Result<()> {
    runtime_fixture::controller_apply_faults(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn sql_submission_rejection_seals_delayed_retries() -> Result<()> {
    runtime_fixture::submission_rejections(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_sql_submission_rejection_seals_delayed_retries() -> Result<()> {
    runtime_fixture::submission_rejections(true).await
}

#[path = "support/cell_group_admission.rs"]
mod admission_fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pending_enrollments_respect_group_capacity() -> Result<()> {
    admission_fixture::capacity().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn proposal_acceptance_retries_do_not_append_twice() -> Result<()> {
    admission_fixture::mutation_retries().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn inherited_enrollments_count_after_applied_term_barrier() -> Result<()> {
    admission_fixture::restart_capacity().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn node_runtime_bounds_proposal_acceptance_across_groups() -> Result<()> {
    runtime_fixture::shared_proposal_budget().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn node_runtime_replicates_independent_cells() -> Result<()> {
    runtime_fixture::replicated_cells().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn node_runtime_cancelled_close_joins_physical_sync() -> Result<()> {
    runtime_fixture::cancelled_close_joins_sync().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn node_runtime_batches_distinct_group_histories() -> Result<()> {
    runtime_fixture::shared_batches().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn node_runtime_reserved_completion_releases_apply_admission() -> Result<()> {
    runtime_fixture::bounded_apply_completion().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn node_runtime_attachment_reserves_before_owner() -> Result<()> {
    runtime_fixture::attachment_reserves_before_owner().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cell_receipt_not_group_progress() -> Result<()> {
    runtime_fixture::cell_receipts(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_cell_receipt_not_group_progress() -> Result<()> {
    runtime_fixture::cell_receipts(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_group_fair_admission() -> Result<()> {
    runtime_fixture::sql_pool_admission(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_shared_group_fair_admission() -> Result<()> {
    runtime_fixture::sql_pool_admission(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cell_sql_reconciles_after_leadership_change() -> Result<()> {
    runtime_fixture::reconcile_sql_producers(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_cell_sql_reconciles_after_leadership_change() -> Result<()> {
    runtime_fixture::reconcile_sql_producers(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cell_sql_reconciles_expired_and_conflicting_receipts() -> Result<()> {
    runtime_fixture::reconcile_sql_refusals(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_cell_sql_reconciles_expired_and_conflicting_receipts() -> Result<()> {
    runtime_fixture::reconcile_sql_refusals(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn concurrent_enrollment_one_cell() -> Result<()> {
    runtime_fixture::initialize_cells(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_concurrent_enrollment_one_cell() -> Result<()> {
    runtime_fixture::initialize_cells(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn enrollment_recovery_after_leadership_change() -> Result<()> {
    runtime_fixture::recover_initialization(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_enrollment_recovery_after_leadership_change() -> Result<()> {
    runtime_fixture::recover_initialization(true).await
}

#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;

use anyhow::{Context, Result};
use cellar::application::consensus::verification::cell_groups::GroupRoute;
use cellar::application::consensus::{
    verification::{cell_groups::*, *},
    *,
};
use cellar::sdk::ExecutionDomain;
use cellar_ltx::{
    apply::{ImageState, ImageUpdate, LtxSegment},
    generation::{GenerationPin, GenerationStore, ImageId},
    DirectFileSystem, TXID,
};
use raft::prelude::{Entry, Message, MessageType};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, sync::Arc, time::Duration};

struct Voter {
    log: ReplicaLog,
    owner: Option<CellGroupOwner>,
    pages: Option<ManifestPageStore>,
    images: GenerationStore,
    extents: Option<cellar_ltx::internal::NodeExtentStore>,
    tree: ManifestTree,
    gate: Arc<fault::Gate>,
    paused: bool,
    held: Option<CellGroupApply>,
}

struct Cluster {
    _root: tempfile::TempDir,
    domain: ExecutionDomain,
    directory: RangeDirectory,
    store: Arc<object_store::memory::InMemory>,
    birth: CellGroupBirth,
    voters: Vec<Voter>,
    network: VecDeque<Message>,
    reads: Vec<(usize, Term, LogIndex, Vec<u8>)>,
    options: RaftRuntimeOptions,
}

impl Cluster {
    async fn new() -> Result<Self> {
        Self::with_options(RaftRuntimeOptions::default()).await
    }

    async fn with_options(options: RaftRuntimeOptions) -> Result<Self> {
        Box::pin(Self::with_storage(
            options,
            tempfile::tempdir()?,
            Arc::new(DirectFileSystem),
        ))
        .await
    }

    async fn with_storage(
        options: RaftRuntimeOptions,
        root: tempfile::TempDir,
        filesystem: Arc<dyn cellar_ltx::FileSystem>,
    ) -> Result<Self> {
        Box::pin(Self::with_storage_schema(
            options, root, filesystem, [6; 32],
        ))
        .await
    }

    async fn with_storage_schema(
        options: RaftRuntimeOptions,
        root: tempfile::TempDir,
        filesystem: Arc<dyn cellar_ltx::FileSystem>,
        schema: [u8; 32],
    ) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let history = GroupHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        let store = Arc::new(object_store::memory::InMemory::new());
        let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
        let namespace = domain
            .run(catalog.open_namespace(NamespaceConfig::new(
                history.cluster,
                history.namespace,
                cellar_logic::NamespaceMode::Raft,
                schema,
            )))
            .await??;
        let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
        let request = directory.prepare_group_birth(CellGroupBirthSpec {
            history,
            range: CellRange::all(),
            epoch: RouteEpoch::new(1)?,
            creation: ProposalId::new([5; 16])?,
            voters: (1..=3)
                .map(|n| InitialMember {
                    member: MemberId::new(n).unwrap(),
                    node: NodeId::new([n as u8; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        })?;
        domain
            .run(directory.reserve_group_birth(&request))
            .await??;
        let init = directory
            .prepare_initialization(ProposalId::new([7; 16])?, &[request.route().clone()])?;
        domain.run(directory.publish(&init)).await??;
        let birth = domain
            .run(directory.materialize_group_birth(&request))
            .await??;
        let mut voters = Vec::new();
        for n in 1..=3 {
            let base = root.path().join(format!("node-{n}"));
            let gate = fault::Gate::new(None);
            let log = domain
                .run(ReplicaLog::open(
                    ReplicaLogOptions::new(
                        base.join("log"),
                        NodeId::new([n; 16])?,
                        DiskLineage::new([n + 10; 16])?,
                        ProcessGeneration::new([20; 16])?,
                        LogOpenMode::Create,
                    ),
                    Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
                ))
                .await??;
            let permit = domain
                .run(directory.bootstrap_group_member(&birth, &log))
                .await??;
            let tree = ManifestTree::new(birth.route().clone(), ManifestLimits::default())?;
            let mut pages = ManifestPageStore::open(
                &base.join("pages"),
                filesystem.clone(),
                1024,
                16 * 1024 * 1024,
                16,
            )?;
            let marker = permit.group().group_manifest()?.context("genesis root")?;
            let prepared = tree.prepare_complete(
                permit.group(),
                &mut pages,
                marker.root(),
                marker.position(),
                marker.membership_revision(),
                |_| panic!("empty genesis"),
            )?;
            let owner = domain
                .run(CellGroupOwner::start(
                    permit,
                    prepared,
                    options.clone(),
                    [n; 32],
                ))
                .await??;
            let images =
                GenerationStore::open(base.join("images"), filesystem.clone(), Default::default())?;
            voters.push(Voter {
                log,
                owner: Some(owner),
                pages: Some(pages),
                images,
                extents: None,
                tree,
                gate,
                paused: false,
                held: None,
            });
        }
        Ok(Self {
            _root: root,
            domain,
            directory,
            store,
            birth,
            voters,
            network: VecDeque::new(),
            reads: vec![],
            options,
        })
    }

    async fn permit(&self, voter: usize) -> Result<CellGroupBootstrapPermit> {
        Ok(self
            .domain
            .run(
                self.directory
                    .bootstrap_group_member(&self.birth, &self.voters[voter].log),
            )
            .await??)
    }

    async fn drive(&mut self, isolated: bool) -> Result<()> {
        self.drive_partition(isolated, None).await
    }

    async fn drive_partition(&mut self, isolated: bool, excluded: Option<usize>) -> Result<()> {
        for _ in 0..2000 {
            let mut progress = false;
            for (n, voter) in self.voters.iter_mut().enumerate() {
                if excluded == Some(n) {
                    continue;
                }
                let event = voter.owner.as_mut().unwrap().poll()?;
                let Some(event) = event else { continue };
                progress = true;
                match event {
                    CellGroupEffect::Persist(work) => {
                        let done = work.run().await?;
                        voter.owner.as_mut().unwrap().complete_persist(done)?;
                    }
                    CellGroupEffect::Message(message) => {
                        if !isolated {
                            self.network.push_back(message);
                        }
                    }
                    CellGroupEffect::Apply(apply) => {
                        if voter.paused {
                            assert!(voter.held.replace(*apply).is_none());
                        } else {
                            let prepared = prepare(voter, &apply)?;
                            voter
                                .owner
                                .as_mut()
                                .unwrap()
                                .complete_apply(apply.ticket, prepared)?;
                        }
                    }
                    CellGroupEffect::Applied { ticket, manifest } => {
                        assert_eq!(ticket.position(), manifest.position());
                        assert_eq!(
                            voter.log.cell_groups()?[0].group_manifest()?.as_ref(),
                            Some(manifest.as_ref())
                        );
                    }
                    CellGroupEffect::CheckpointInstalled { manifest }
                    | CellGroupEffect::CheckpointPublished { manifest } => {
                        assert_eq!(
                            voter.log.cell_groups()?[0].group_manifest()?.as_ref(),
                            Some(manifest.as_ref())
                        );
                    }
                    CellGroupEffect::ReadIndex {
                        term,
                        index,
                        context,
                    } => self.reads.push((n, term, index, context)),
                }
            }
            for _ in 0..self.network.len() {
                let message = self.network.pop_front().unwrap();
                let target = message.to as usize - 1;
                if excluded == Some(target) || excluded == Some(message.from as usize - 1) {
                    continue;
                }
                match self.voters[target]
                    .owner
                    .as_mut()
                    .unwrap()
                    .step(message.clone())
                {
                    Ok(()) => progress = true,
                    Err(RuntimeError::NotReady) => self.network.push_back(message),
                    Err(error) => return Err(error.into()),
                }
            }
            // A bounded receiver may refuse messages until a held apply settles.
            // Keep those messages for the next pump instead of busy-spinning.
            if !progress {
                return Ok(());
            }
        }
        anyhow::bail!("group did not quiesce")
    }

    async fn elect(&mut self) -> Result<()> {
        self.voters[0].owner.as_mut().unwrap().campaign()?;
        self.drive(false).await?;
        assert!(self.voters[0].owner.as_ref().unwrap().status()?.ready);
        Ok(())
    }

    async fn restart(&mut self, n: usize) -> Result<()> {
        let mut old = self.voters.remove(n);
        drop(old.held.take());
        drop(old.owner.take());
        old.log.close().await?;
        drop(old);
        let number = n as u8 + 1;
        let base = self._root.path().join(format!("node-{number}"));
        let gate = fault::Gate::new(None);
        let log = self
            .domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    base.join("log"),
                    NodeId::new([number; 16])?,
                    DiskLineage::new([number + 10; 16])?,
                    ProcessGeneration::new([21; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
            ))
            .await??;
        let permit = self
            .domain
            .run(self.directory.bootstrap_group_member(&self.birth, &log))
            .await??;
        let tree = ManifestTree::new(self.birth.route().clone(), ManifestLimits::default())?;
        let mut pages = ManifestPageStore::open(
            &base.join("pages"),
            Arc::new(DirectFileSystem),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let images = GenerationStore::open(
            base.join("images"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let marker = permit.group().group_manifest()?.context("selected root")?;
        let prepared = tree.prepare_complete(
            permit.group(),
            &mut pages,
            marker.root(),
            marker.position(),
            marker.membership_revision(),
            |leaf| {
                let CellImageCommitment::Materialized(digest) = leaf.version.image.commitment
                else {
                    return Err(ManifestError::Invalid("fixture materialized image"));
                };
                let state = ImageState {
                    epoch: leaf.version.image.epoch,
                    txid: TXID(leaf.version.image.txid),
                    checksum: leaf.version.image.checksum,
                    digest,
                    length: leaf.geometry.length,
                    page_size: leaf.geometry.page_size,
                };
                let id = image_id(&leaf.version.history, leaf.version.sequence.get())
                    .map_err(|_| ManifestError::Invalid("fixture image id"))?;
                images
                    .adopt(id, state)
                    .map(CellImagePin::Materialized)
                    .map_err(|_| ManifestError::Invalid("fixture recovery image"))
            },
        )?;
        let owner = self
            .domain
            .run(CellGroupOwner::start(
                permit,
                prepared,
                self.options.clone(),
                [number; 32],
            ))
            .await??;
        self.voters.insert(
            n,
            Voter {
                log,
                owner: Some(owner),
                tree,
                pages: Some(pages),
                images,
                extents: None,
                gate,
                paused: false,
                held: None,
            },
        );
        Ok(())
    }

    async fn close(self) -> Result<()> {
        for mut voter in self.voters {
            drop(voter.owner.take());
            voter.log.close().await?;
        }
        self.domain.shutdown().await;
        Ok(())
    }
}

fn image_id(history: &CellHistory, sequence: u64) -> Result<ImageId> {
    let mut hash = Sha256::new();
    hash.update(history.cell.as_str());
    hash.update(sequence.to_be_bytes());
    Ok(ImageId::new(hash.finalize()[..16].try_into()?)?)
}

fn apply_image(
    store: &GenerationStore,
    history: &CellHistory,
    sequence: u64,
    base: Option<&GenerationPin>,
    image: LtxImage,
    files: &[GroupLtx<'_>],
) -> Result<GenerationPin> {
    let header = cellar_ltx::ltx::Header::parse(files.last().context("LTX")?.bytes)?;
    let ImageCommitment::Sha256(digest) = image.commitment else {
        anyhow::bail!("materialized commitment required")
    };
    let result = ImageState {
        epoch: image.epoch,
        txid: TXID(image.txid),
        checksum: image.checksum,
        digest,
        page_size: header.page_size,
        length: u64::from(header.page_size) * u64::from(header.commit),
    };
    let update = ImageUpdate {
        base: match base {
            Some(pin) => pin.state(),
            None => ImageState::empty(image.epoch)?,
        },
        result,
        segments: files
            .iter()
            .map(|f| LtxSegment {
                bytes: f.bytes,
                min_txid: TXID(f.min_txid),
                max_txid: TXID(f.max_txid),
                page_count: f.page_count as usize,
                decoded_bytes: f.decoded_bytes,
            })
            .collect(),
    };
    Ok(store.prepare(image_id(history, sequence)?, base, &update)?)
}

fn prepare_cell_image(
    images: &GenerationStore,
    extents: Option<&cellar_ltx::internal::NodeExtentStore>,
    cell: &CellHistory,
    sequence: u64,
    base: Option<&CellImagePin>,
    image: LtxImage,
    files: &[GroupLtx<'_>],
) -> Result<EnrollmentImage> {
    use cellar_ltx::internal::{
        ExtentManifest, ExtentRootReference, Geometry, ImageBinding, ImageRootUpdate, EXTENT_BYTES,
    };
    if image.txid == 0 {
        anyhow::ensure!(base.is_none() && files.is_empty());
        return Ok(EnrollmentImage {
            pin: CellImagePin::Empty,
            geometry: CellImageGeometry {
                length: 0,
                page_size: 512,
            },
            effects: EffectSummary::default(),
        });
    }
    let (pin, geometry) = match image.commitment {
        ImageCommitment::Sha256(_) => {
            let base = match base {
                Some(CellImagePin::Materialized(pin)) => Some(pin),
                None => None,
                _ => anyhow::bail!("materialized base"),
            };
            let pin = apply_image(images, cell, sequence, base, image, files)?;
            let geometry = CellImageGeometry {
                length: pin.state().length,
                page_size: pin.state().page_size,
            };
            (CellImagePin::Materialized(pin), geometry)
        }
        ImageCommitment::Extents(result) => {
            let store = extents.context("extent store")?;
            let base = match base {
                Some(CellImagePin::Extents(pin)) => Some(pin.as_ref()),
                None => None,
                _ => anyhow::bail!("extent base"),
            };
            let binding = ImageBinding {
                history: cell_image_history_digest(cell),
                schema: [6; 32],
                epoch: image.epoch,
                txid: image.txid,
                checksum: image.checksum,
            };
            let empty = ExtentManifest::new(
                ImageBinding {
                    txid: 0,
                    checksum: 0,
                    ..binding
                },
                Geometry::new(0, 0)?,
                &[],
            )?;
            let before = base
                .map(|pin| pin.reference())
                .unwrap_or(ExtentRootReference::new(
                    empty.root(),
                    empty.encode()?.len().try_into()?,
                )?);
            let header = cellar_ltx::ltx::Header::parse(files.last().context("extent LTX")?.bytes)?;
            let geometry = Geometry::new(
                header.page_size,
                u64::from(header.page_size) * u64::from(header.commit),
            )?;
            let update = ImageRootUpdate {
                base: before,
                result,
                binding,
                geometry,
                segments: files
                    .iter()
                    .map(|file| LtxSegment {
                        bytes: file.bytes,
                        min_txid: TXID(file.min_txid),
                        max_txid: TXID(file.max_txid),
                        page_count: file.page_count as usize,
                        decoded_bytes: file.decoded_bytes,
                    })
                    .collect(),
            };
            let prepared = store.prepare_image_root(
                base,
                &update,
                &images.apply_limits(),
                &mut vec![0; EXTENT_BYTES as usize],
            )?;
            (
                CellImagePin::Extents(Arc::new(prepared.image)),
                CellImageGeometry {
                    length: geometry.length(),
                    page_size: geometry.page_size(),
                },
            )
        }
    };
    Ok(EnrollmentImage {
        pin,
        geometry,
        effects: EffectSummary::default(),
    })
}

fn prepare(voter: &mut Voter, apply: &CellGroupApply) -> Result<PreparedGroupManifest> {
    if apply.entry.data.is_empty() {
        return Ok(voter.tree.prepare_unchanged(
            voter.pages.as_ref().context("fixture page owner")?,
            &apply.base,
            apply.ticket.position(),
        )?);
    }
    let decoded = CellGroupEntry::decode(
        &apply.entry.data,
        apply.base.marker().route(),
        GroupEntryLimits::default(),
    )?;
    match decoded.command {
        GroupCommand::Enroll { cell, image, files } => {
            let (next, _) = voter.tree.prepare_enrollment(
                voter.pages.as_mut().context("fixture page owner")?,
                &apply.base,
                EnrollmentApply {
                    position: apply.ticket.position(),
                    phase: EnrollmentPhase::Active,
                    entry: &apply.entry.data,
                    limits: GroupEntryLimits::default(),
                },
                |_| {
                    prepare_cell_image(
                        &voter.images,
                        voter.extents.as_ref(),
                        &cell,
                        1,
                        None,
                        image,
                        &files,
                    )
                    .map_err(|_| ManifestError::Invalid("fixture initial LTX"))
                },
            )?;
            Ok(next)
        }
        GroupCommand::Mutation(mutation) => {
            let (old, base) = apply.base.pin_cell(&mutation.cell.cell).context("cell")?;
            let prepared = prepare_cell_image(
                &voter.images,
                voter.extents.as_ref(),
                &mutation.cell,
                mutation.sequence.get(),
                Some(&base),
                mutation.result,
                &mutation.files,
            )?;
            let leaf = CellManifestLeaf {
                enrollment: old.enrollment.clone(),
                version: CellVersion {
                    history: mutation.cell,
                    sequence: mutation.sequence,
                    image: mutation.result.into(),
                },
                geometry: prepared.geometry,
                last_mutation: apply.ticket.position(),
                witness: CommitWitness {
                    group: decoded.binding.group,
                    position: apply.ticket.position(),
                    proposal: decoded.binding.proposal,
                },
                effects: old.effects,
            };
            Ok(voter.tree.prepare_update(
                voter.pages.as_mut().context("fixture page owner")?,
                &apply.base,
                leaf,
                Some(&old.version),
                prepared.pin,
            )?)
        }
        _ => anyhow::bail!("unexpected administration"),
    }
}

fn history(route: &GroupRoute, name: &str) -> Result<CellHistory> {
    Ok(CellHistory {
        cluster: route.history.cluster,
        namespace: route.history.namespace,
        cell: CellId::new(name)?,
        incarnation: IncarnationId::new([8; 16])?,
    })
}

fn request(
    route: &GroupRoute,
    cell: CellHistory,
    sequence: u64,
    cut: &images::Cut,
) -> Result<Vec<u8>> {
    let image = |state: ImageState| LtxImage {
        epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        commitment: state.digest.into(),
    };
    let files = vec![GroupLtx {
        min_txid: cut.base.txid.0 + 1,
        max_txid: cut.result.txid.0,
        page_count: cut.pages.len() as u64,
        decoded_bytes: cut.pages.len() as u64 * 512,
        bytes: &cut.bytes,
    }];
    let command = if sequence == 1 {
        GroupCommand::Enroll {
            cell,
            image: image(cut.result),
            files,
        }
    } else {
        GroupCommand::Mutation(GroupMutation {
            cell,
            sequence: MutationSequence::new(sequence)?,
            base: image(cut.base),
            result: image(cut.result),
            files,
        })
    };
    Ok(CellGroupEntry {
        binding: GroupEntryBinding {
            group: route.history,
            route_epoch: route.epoch,
            birth_digest: route.birth_digest,
            schema_digest: route.schema_digest,
            proposal: ProposalId::new([sequence as u8; 16])?,
        },
        command,
    }
    .encode(route, GroupEntryLimits::default())?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn three_cells_one_rawnode_per_voter() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    let route = cluster.birth.route().clone();
    let mut expected = Vec::new();
    for (n, name) in ["alpha", "beta", "gamma"].into_iter().enumerate() {
        let bytes = images::image(
            None,
            &format!(
                "CREATE TABLE value(n); INSERT INTO value VALUES ({});",
                n + 1
            ),
        )?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let cell = history(&route, name)?;
        cluster.voters[0].owner.as_mut().unwrap().propose(request(
            &route,
            cell.clone(),
            1,
            &cut,
        )?)?;
        cluster.drive(false).await?;
        expected.push((cell, bytes));
    }
    for step in 0..9 {
        let i = step % 3;
        let (cell, before) = &mut expected[i];
        let sequence = step / 3 + 2;
        let after = images::image(Some(before), "UPDATE value SET n = n + 10;")?;
        let cut = images::Cut::new(before, &after, sequence as u64 - 1, true, false);
        cluster.voters[0].owner.as_mut().unwrap().propose(request(
            &route,
            cell.clone(),
            sequence as u64,
            &cut,
        )?)?;
        cluster.drive(false).await?;
        *before = after;
    }
    assert_eq!(cluster.voters.len(), 3);
    for n in 0..3 {
        let owner = cluster.voters[n].owner.as_ref().unwrap();
        assert_eq!(owner.manifest().marker().root().cells(), 3);
        for (cell, bytes) in &expected {
            let (leaf, CellImagePin::Materialized(pin)) =
                owner.manifest().pin_cell(&cell.cell).context("cell")?
            else {
                anyhow::bail!("image")
            };
            assert_eq!(leaf.version.history, *cell);
            assert_eq!(leaf.version.sequence.get(), 4);
            assert_eq!(&images::read(&pin)?, bytes);
        }
        let duplicate = cluster
            .domain
            .run(CellGroupOwner::start(
                cluster.permit(n).await?,
                owner.manifest().clone(),
                RaftRuntimeOptions::default(),
                [50; 32],
            ))
            .await?;
        assert!(matches!(duplicate, Err(RuntimeError::AlreadyAttached)));
        assert_eq!(
            owner.manifest().marker(),
            cluster.voters[0]
                .owner
                .as_ref()
                .unwrap()
                .manifest()
                .marker()
        );
    }
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .read_index(b"barrier".to_vec())?;
    cluster.drive(false).await?;
    assert!(cluster.reads.iter().any(|(n, term, index, context)| *n == 0
        && context == b"barrier"
        && *term
            == cluster.voters[0]
                .owner
                .as_ref()
                .unwrap()
                .status()
                .unwrap()
                .term
        && *index
            <= cluster.voters[0]
                .owner
                .as_ref()
                .unwrap()
                .manifest()
                .marker()
                .position()
                .index));
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn physical_owner_rejects_exhausted_peer_entry_counters() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    let before = cluster.voters[0].owner.as_ref().unwrap().status()?;
    let message = Message {
        msg_type: MessageType::MsgAppend,
        from: 2,
        to: 1,
        term: 2,
        log_term: 1,
        index: u64::MAX - 3,
        entries: vec![Entry {
            term: 1,
            index: u64::MAX - 2,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    assert!(matches!(
        cluster.voters[0].owner.as_mut().unwrap().step(message),
        Err(RuntimeError::Invalid(_))
    ));
    let after = cluster.voters[0].owner.as_ref().unwrap().status()?;
    assert_eq!(before.term, after.term);
    assert_eq!(before.applied, after.applied);
    cluster.elect().await?;
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn physical_owner_requires_read_quorum() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .read_index(b"minority".to_vec())?;
    cluster.drive(true).await?;
    assert!(cluster.reads.is_empty());
    for _ in 0..50 {
        cluster.voters[0].owner.as_mut().unwrap().tick()?;
        cluster.drive(true).await?;
    }
    assert!(!cluster.voters[0].owner.as_ref().unwrap().status()?.ready);
    assert!(cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .read_index(b"stale".to_vec())
        .is_err());
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn apply_prefix_never_skips_slow_cell() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    let route = cluster.birth.route().clone();
    let mut expected = Vec::new();
    for name in ["slow", "later"] {
        let cell = history(&route, name)?;
        let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        cluster.voters[0].owner.as_mut().unwrap().propose(request(
            &route,
            cell.clone(),
            1,
            &cut,
        )?)?;
        cluster.drive(false).await?;
        expected.push((cell, bytes));
    }
    let before = cluster.voters[0]
        .owner
        .as_ref()
        .unwrap()
        .manifest()
        .marker()
        .clone();
    cluster.voters[1].paused = true;
    cluster.voters[2].paused = true;
    for (cell, bytes) in &mut expected {
        let after = images::image(Some(bytes), "UPDATE value SET n = 2;")?;
        let cut = images::Cut::new(bytes, &after, 1, true, false);
        cluster.voters[0].owner.as_mut().unwrap().propose(request(
            &route,
            cell.clone(),
            2,
            &cut,
        )?)?;
        cluster.drive(false).await?;
        *bytes = after;
    }
    for voter in &cluster.voters[1..] {
        let status = voter.owner.as_ref().unwrap().status()?;
        assert_eq!(status.applied, before.position());
        assert_eq!(status.committed.get(), before.position().index.get() + 2);
        assert_eq!(voter.owner.as_ref().unwrap().manifest().marker(), &before);
        assert_eq!(
            voter.held.as_ref().unwrap().ticket.position().index.get(),
            before.position().index.get() + 1
        );
    }
    let foreign = cluster.voters[2].held.as_ref().unwrap().ticket.clone();
    let voter = &mut cluster.voters[1];
    let apply = voter.held.take().context("held slow apply")?;
    let next = prepare(voter, &apply)?;
    assert!(matches!(
        voter
            .owner
            .as_mut()
            .unwrap()
            .complete_apply(foreign, next.clone()),
        Err(RuntimeError::StaleCompletion)
    ));
    // Correct image and apply position cannot authorize a fabricated proposal
    // witness. Completion must match the exact committed cell-addressed entry.
    let (leaf, image) = next.pin_cell(&expected[0].0.cell).context("changed cell")?;
    let mut forged_leaf = leaf.clone();
    forged_leaf.witness.proposal = ProposalId::new([254; 16])?;
    assert_ne!(forged_leaf.witness, leaf.witness);
    let old = apply
        .base
        .pin_cell(&expected[0].0.cell)
        .context("old cell")?
        .0;
    let forged = voter.tree.prepare_update(
        voter.pages.as_mut().context("fixture page owner")?,
        &apply.base,
        forged_leaf,
        Some(&old.version),
        image,
    )?;
    assert!(matches!(
        voter
            .owner
            .as_mut()
            .unwrap()
            .complete_apply(apply.ticket.clone(), forged,),
        Err(RuntimeError::Invalid(_))
    ));
    assert_eq!(voter.owner.as_ref().unwrap().manifest().marker(), &before);
    let unchanged = voter.tree.prepare_unchanged(
        voter.pages.as_ref().context("fixture page owner")?,
        &apply.base,
        apply.ticket.position(),
    )?;
    assert!(matches!(
        voter
            .owner
            .as_mut()
            .unwrap()
            .complete_apply(apply.ticket.clone(), unchanged),
        Err(RuntimeError::Invalid(_))
    ));
    voter.paused = false;
    voter
        .owner
        .as_mut()
        .unwrap()
        .complete_apply(apply.ticket, next)?;
    cluster.drive(false).await?;
    assert_eq!(
        cluster.voters[1]
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .marker(),
        cluster.voters[0]
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .marker()
    );

    // Restart the other follower with both entries committed but neither applied.
    // The restored marker stays behind until the real owner replays its log tail.
    cluster.restart(2).await?;
    assert_eq!(
        cluster.voters[2]
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .marker(),
        &before
    );
    cluster.drive(false).await?;
    for voter in &cluster.voters {
        let owner = voter.owner.as_ref().unwrap();
        assert_eq!(
            owner.manifest().marker(),
            cluster.voters[0]
                .owner
                .as_ref()
                .unwrap()
                .manifest()
                .marker()
        );
        for (cell, bytes) in &expected {
            let (leaf, CellImagePin::Materialized(pin)) = owner
                .manifest()
                .pin_cell(&cell.cell)
                .context("restored cell")?
            else {
                anyhow::bail!("image")
            };
            assert_eq!(leaf.version.sequence.get(), 2);
            assert_eq!(&images::read(&pin)?, bytes);
        }
    }
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn physical_owner_backpressures_slow_apply_without_losing_commits() -> Result<()> {
    let mut cluster = Cluster::with_options(RaftRuntimeOptions {
        max_proposal_bytes: 8192,
        max_message_bytes: 12_288,
        max_group_apply_bytes: 16_384,
        max_apply_bytes: 32_768,
        ..Default::default()
    })
    .await?;
    cluster.elect().await?;
    let route = cluster.birth.route().clone();
    let cell = history(&route, "busy")?;
    let mut bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .propose(request(&route, cell.clone(), 1, &cut)?)?;
    cluster.drive(false).await?;
    let before = cluster.voters[2]
        .owner
        .as_ref()
        .unwrap()
        .manifest()
        .marker()
        .clone();
    cluster.voters[2].paused = true;
    for sequence in 2..=40 {
        let after = images::image(Some(&bytes), "UPDATE value SET n = n + 1;")?;
        let cut = images::Cut::new(&bytes, &after, sequence - 1, true, false);
        cluster.voters[0].owner.as_mut().unwrap().propose(request(
            &route,
            cell.clone(),
            sequence,
            &cut,
        )?)?;
        cluster.drive(false).await?;
        bytes = after;
    }
    assert!(
        !cluster.network.is_empty(),
        "the slow voter must exert real transport backpressure"
    );
    assert_eq!(
        cluster.voters[2]
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .marker(),
        &before
    );
    let voter = &mut cluster.voters[2];
    let apply = voter.held.take().context("held apply")?;
    let next = prepare(voter, &apply)?;
    voter.paused = false;
    voter
        .owner
        .as_mut()
        .unwrap()
        .complete_apply(apply.ticket, next)?;
    cluster.drive(false).await?;
    assert!(cluster.network.is_empty());
    for voter in &cluster.voters {
        let (leaf, CellImagePin::Materialized(pin)) = voter
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .pin_cell(&cell.cell)
            .context("caught-up cell")?
        else {
            anyhow::bail!("image")
        };
        assert_eq!(leaf.version.sequence.get(), 40);
        assert_eq!(images::read(&pin)?, bytes);
    }
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn physical_owner_retains_lease_through_cancelled_sync() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    let permit = cluster.permit(0).await?;
    let root = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let route = cluster.birth.route().clone();
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let entry = request(&route, history(&route, "held")?, 1, &cut)?;
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    owner.propose(entry)?;
    let CellGroupEffect::Persist(work) = owner.poll()?.context("append persistence")? else {
        anyhow::bail!("append must persist before sending")
    };
    assert!(owner.poll()?.is_none(), "no append messages before sync");
    for _ in 0..100 {
        assert!(matches!(owner.tick(), Err(RuntimeError::NotReady)));
    }
    let gate = cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let mut write = Box::pin(work.run());
    match tokio::time::timeout(
        Duration::from_secs(5),
        futures_util::future::select(&mut write, Box::pin(gate.entered.notified())),
    )
    .await?
    {
        futures_util::future::Either::Left(_) => anyhow::bail!("sync did not enter held gate"),
        futures_util::future::Either::Right(_) => (),
    }
    drop(write);
    drop(cluster.voters[0].owner.take());
    let duplicate = cluster
        .domain
        .run(CellGroupOwner::start(
            permit,
            root.clone(),
            RaftRuntimeOptions::default(),
            [61; 32],
        ))
        .await?;
    assert!(matches!(duplicate, Err(RuntimeError::AlreadyAttached)));
    drop(release);
    cluster.voters[0].log.purge().await?;
    let permit = cluster.permit(0).await?;
    let owner = cluster
        .domain
        .run(CellGroupOwner::start(
            permit,
            root,
            RaftRuntimeOptions::default(),
            [62; 32],
        ))
        .await??;
    cluster.voters[0].owner = Some(owner);
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn strong_read_at_group_barrier() -> Result<()> {
    runtime_fixture::coherent_reads(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_strong_read_at_group_barrier() -> Result<()> {
    runtime_fixture::coherent_reads(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cell_query_pins_deadline_and_cancelled_cleanup() -> Result<()> {
    runtime_fixture::query_lifecycle(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_cell_query_pins_deadline_and_cancelled_cleanup() -> Result<()> {
    runtime_fixture::query_lifecycle(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_workers_prepare_committed_cell_images() -> Result<()> {
    runtime_fixture::apply_images(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_shared_workers_prepare_committed_cell_images() -> Result<()> {
    runtime_fixture::apply_images(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_apply_cancelled_close_joins_preparation() -> Result<()> {
    runtime_fixture::apply_cancellation(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_shared_apply_cancelled_close_joins_preparation() -> Result<()> {
    runtime_fixture::apply_cancellation(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_apply_sync_failure_preserves_selected_root() -> Result<()> {
    runtime_fixture::apply_failure(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_shared_apply_sync_failure_preserves_selected_root() -> Result<()> {
    runtime_fixture::apply_failure(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_apply_recovers_selected_images_before_replay() -> Result<()> {
    runtime_fixture::recover_apply(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_shared_apply_recovers_selected_images_before_replay() -> Result<()> {
    runtime_fixture::recover_apply(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn registered_commands_use_cell_identity_and_receipts() -> Result<()> {
    runtime_fixture::registered_commands(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_registered_commands_use_cell_identity_and_receipts() -> Result<()> {
    runtime_fixture::registered_commands(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn registered_queries_keep_cell_observation_and_ownership() -> Result<()> {
    Box::pin(runtime_fixture::registered_queries(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_registered_queries_keep_cell_observation_and_ownership() -> Result<()> {
    Box::pin(runtime_fixture::registered_queries(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn catalog_routed_queries_preserve_invocation_and_authority() -> Result<()> {
    Box::pin(runtime_fixture::routed_queries(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_catalog_routed_queries_preserve_invocation_and_authority() -> Result<()> {
    Box::pin(runtime_fixture::routed_queries(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn catalog_routed_commands_preserve_producer_and_receipts() -> Result<()> {
    Box::pin(runtime_fixture::routed_commands(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_catalog_routed_commands_preserve_producer_and_receipts() -> Result<()> {
    Box::pin(runtime_fixture::routed_commands(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn physical_group_transport_multiplexes_real_raft() -> Result<()> {
    Box::pin(runtime_fixture::physical_transport()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn catalog_commands_replicate_over_physical_tls() -> Result<()> {
    Box::pin(runtime_fixture::commands_over_tcp(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_catalog_commands_replicate_over_physical_tls() -> Result<()> {
    Box::pin(runtime_fixture::commands_over_tcp(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_survives_initializer_disconnect() -> Result<()> {
    Box::pin(runtime_fixture::owned_enrollment(0, 0)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_seals_late_preparation() -> Result<()> {
    Box::pin(runtime_fixture::owned_enrollment(0, 1)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_survives_accepted_disconnect() -> Result<()> {
    Box::pin(runtime_fixture::owned_enrollment(1, 0)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_resolves_after_deadline() -> Result<()> {
    Box::pin(runtime_fixture::owned_enrollment(1, 1)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_joins_cancelled_shutdown() -> Result<()> {
    Box::pin(runtime_fixture::owned_enrollment(0, 2)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_releases_completed_initializer_errors() -> Result<()> {
    Box::pin(runtime_fixture::owned_enrollment_failure(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_joins_lost_worker_reply() -> Result<()> {
    Box::pin(runtime_fixture::owned_enrollment_failure(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_enrollment_requires_registered_initializer() -> Result<()> {
    Box::pin(runtime_fixture::enrollment_requires_registration()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_queries_bind_cell_versions_and_gateway_owners() -> Result<()> {
    Box::pin(runtime_fixture::remote_queries()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_queries_retain_cancelled_sql_until_join() -> Result<()> {
    Box::pin(runtime_fixture::remote_query_lifecycle(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_queries_retain_expired_sql_until_join() -> Result<()> {
    Box::pin(runtime_fixture::remote_query_lifecycle(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_nonmember_queries_require_grants_without_local_replicas() -> Result<()> {
    Box::pin(runtime_fixture::nonmember_queries()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_nonmember_query_revocation_retains_physical_sql() -> Result<()> {
    Box::pin(runtime_fixture::nonmember_query_revocation(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_nonmember_query_route_replacement_refuses_stale_reply() -> Result<()> {
    Box::pin(runtime_fixture::nonmember_query_revocation(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_cached_dispatch_preserves_authority_during_catalog_outage() -> Result<()> {
    Box::pin(runtime_fixture::cached_dispatch_execution()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_commands_require_write_grants_and_exact_receipts() -> Result<()> {
    Box::pin(runtime_fixture::remote_commands()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_commands_survive_disconnected_sql_waiters() -> Result<()> {
    Box::pin(runtime_fixture::remote_command_lifecycle(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_command_revocation_preserves_admitted_ownership() -> Result<()> {
    Box::pin(runtime_fixture::remote_command_lifecycle(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_enrollment_requires_write_grants_and_exact_creation() -> Result<()> {
    Box::pin(runtime_fixture::remote_enrollment()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_enrollment_survives_disconnected_initializer() -> Result<()> {
    Box::pin(runtime_fixture::remote_enrollment_lifecycle(0)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_enrollment_revocation_retains_admitted_creation() -> Result<()> {
    Box::pin(runtime_fixture::remote_enrollment_lifecycle(1)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_remote_enrollment_resolves_after_publication_deadline() -> Result<()> {
    Box::pin(runtime_fixture::remote_enrollment_lifecycle(2)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_receipt_lookup_requires_grants_and_never_executes_commands() -> Result<()> {
    Box::pin(runtime_fixture::receipt_lookup_ingress()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_receipt_lookup_keeps_unresolved_producers_at_capacity() -> Result<()> {
    Box::pin(runtime_fixture::receipt_lookup_pending()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_receipt_lookup_cancellation_preserves_active_writer() -> Result<()> {
    Box::pin(runtime_fixture::receipt_lookup_cancelled()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn dynamic_registration_preserves_live_groups_and_disconnected_waiters() -> Result<()> {
    runtime_fixture::registration_live().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn dynamic_registration_refuses_foreign_owners_and_retries_failed_catalog() -> Result<()> {
    runtime_fixture::registration_refusals().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn dynamic_registration_joins_attachment_before_cancelled_shutdown() -> Result<()> {
    runtime_fixture::registration_close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn dynamic_registration_recovers_selected_roots_into_an_empty_controller() -> Result<()> {
    runtime_fixture::registration_recovery().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn group_quarantine_retains_producer_and_serves_healthy_group() -> Result<()> {
    runtime_fixture::group_quarantine_retains_producer_and_serves_healthy_group().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn shared_engine_sync_failure_still_stops_all_groups() -> Result<()> {
    runtime_fixture::shared_engine_sync_failure_still_stops_all_groups().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn provisioning_coalesces_and_serves() -> Result<()> {
    runtime_fixture::provisioning_coalesces_and_serves().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn provisioning_retries_retained_owner() -> Result<()> {
    runtime_fixture::provisioning_retries_retained_owner().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn provisioning_joins_cancelled_close() -> Result<()> {
    runtime_fixture::provisioning_joins_cancelled_close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn provisioning_recovers_selected_roots() -> Result<()> {
    runtime_fixture::provisioning_recovers_selected_roots().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn provisioning_refusals_preserve_healthy_group() -> Result<()> {
    runtime_fixture::provisioning_refusals_preserve_healthy_group().await
}
