// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Shared at each integration target root to preserve the support modules' paths.
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
use raft::prelude::{Message, MessageType};
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
    if apply.entry.get_entry_type() == raft::prelude::EntryType::EntryConfChangeV2 {
        let group = voter.log.cell_group(&apply.base.marker().route().history)?;
        return Ok(voter.tree.prepare_membership(
            voter.pages.as_ref().context("fixture page owner")?,
            &apply.base,
            &apply.entry,
            &raft::Storage::initial_state(&group)?.conf_state,
            &group.membership()?,
        )?);
    }
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
