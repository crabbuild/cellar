//! Shared real-engine/archive-cut fixture for public and private boundary tests.
#![allow(clippy::disallowed_methods)]
use super::{faults, images};
use anyhow::Result;
use cellar::application::consensus::verification::*;
use cellar::application::consensus::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate};
use cellar::sdk::consensus::*;
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_logic::NamespaceMode;
use cellar_ltx::generation::{GenerationLimits, GenerationPin, GenerationStore};
use cellar_ltx::{DirectFileSystem, LtxHost};
use protobuf::Message as _;
use raft::{
    prelude::{ConfChangeSingle, ConfChangeType, ConfChangeV2, Entry, EntryType},
    Storage,
};
use std::sync::Arc;

pub(super) struct Fixture {
    pub(super) _root: tempfile::TempDir,
    pub(super) domain: ExecutionDomain,
    pub(super) log: ReplicaLog,
    pub(super) group: GroupLog,
    pub(super) images: GenerationStore,
    pub(super) store: Arc<faults::Store>,
    pub(super) archive: CellArchive,
    pub(super) verifier: ArchiveVerifier,
    pub(super) namespace: NamespaceCatalog,
    pub(super) birth: GroupBirth,
    pub(super) cut: Option<CommittedArchiveCut>,
    pub(super) pin: Option<GenerationPin>,
    pub(super) bytes: Vec<u8>,
    pub(super) txid: u8,
}

impl Fixture {
    pub(super) async fn new(limits: ArchiveLimits) -> Result<Self> {
        Self::new_group(limits, Arc::new(faults::Store::default()), 7).await
    }

    pub(super) async fn new_group(
        limits: ArchiveLimits,
        store: Arc<faults::Store>,
        id: u8,
    ) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let catalog = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "archive-test".into(),
                ..Default::default()
            },
        )?;
        let mut config = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            [9; 32],
        );
        config.voters = VoterPolicy::SingleVoterDevelopment;
        let namespace = catalog.open_namespace(config).await?;
        let birth = namespace
            .resolve_birth(BirthProposal {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: format!("Counter:{id}"),
                    group: GroupId::new([id; 16])?,
                    incarnation: IncarnationId::new([3; 16])?,
                },
                voters: vec![InitialMember {
                    member: MemberId::new(1)?,
                    node: NodeId::new([5; 16])?,
                }],
                learners: vec![],
            })
            .await?;
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.path().join("raft"),
                    NodeId::new([5; 16])?,
                    DiskLineage::new([6; 16])?,
                    ProcessGeneration::new([7; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let group = log
            .create_group(GroupBootstrap {
                scope: birth.scope().clone(),
                member: MemberId::new(1)?,
                birth_digest: birth.digest(),
                voters: vec![MemberId::new(1)?],
                learners: vec![],
            })
            .await?;
        let generations = GenerationStore::open(
            root.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let archive = namespace.cell_archive(&birth, LtxHost::default(), limits)?;
        let mut verifier_limits = GenerationLimits::default();
        verifier_limits.apply.decode.max_encoded_bytes = 64 * 1024 * 1024;
        verifier_limits.apply.max_total_encoded_bytes = 64 * 1024 * 1024;
        let verifier = ArchiveVerifier::new(
            GenerationStore::open(
                root.path().join("archive-verifier"),
                Arc::new(DirectFileSystem),
                verifier_limits,
            )?,
            images::id(92),
        )?;
        Ok(Self {
            _root: root,
            domain,
            log,
            group,
            images: generations,
            store,
            archive,
            verifier,
            namespace,
            birth,
            cut: None,
            pin: None,
            bytes: Vec::new(),
            txid: 0,
        })
    }

    pub(super) fn payload(&self, cut: &images::Cut) -> Result<Vec<u8>> {
        let position = |state: cellar_ltx::apply::ImageState| LtxImage {
            epoch: state.epoch,
            txid: state.txid.0,
            checksum: state.checksum,
            image_digest: state.digest,
        };
        Ok(CellUpdate {
            scope: self.group.identity().scope.clone(),
            application_digest: [9; 32],
            proposal: ProposalId::new([self.txid + 1; 16])?,
            base: position(cut.base),
            result: position(cut.result),
            files: vec![LtxUpdate {
                min_txid: cut.base.txid.0 + 1,
                max_txid: cut.result.txid.0,
                page_count: cut.pages.len() as u64,
                decoded_bytes: cut.pages.len() as u64 * 512,
                bytes: cut.bytes.clone(),
            }],
        }
        .encode(EnvelopeLimits::default())?)
    }

    pub(super) async fn commit(
        &mut self,
        value: i64,
        dependencies: Vec<ArchiveDependency>,
    ) -> Result<CommittedArchiveCut> {
        self.commit_payload(value, dependencies, false).await
    }

    pub(super) async fn commit_payload(
        &mut self,
        value: i64,
        dependencies: Vec<ArchiveDependency>,
        corrupt: bool,
    ) -> Result<CommittedArchiveCut> {
        let next = if self.txid == 0 {
            images::image(
                None,
                &format!("CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES({value})"),
            )?
        } else {
            images::image(
                Some(&self.bytes),
                &format!("UPDATE counter SET value={value}"),
            )?
        };
        self.commit_bytes(next, dependencies, |update| {
            if corrupt {
                let bytes = &mut update.files[0].bytes;
                let last = bytes.len() - 1;
                bytes[last] ^= 0xff;
            }
        })
        .await
    }

    pub(super) async fn commit_bytes(
        &mut self,
        next: Vec<u8>,
        dependencies: Vec<ArchiveDependency>,
        mutate: impl FnOnce(&mut CellUpdate),
    ) -> Result<CommittedArchiveCut> {
        let cut = images::Cut::new(
            &self.bytes,
            &next,
            u64::from(self.txid),
            true,
            self.txid.is_multiple_of(2),
        );
        let pin =
            self.images
                .prepare(images::id(self.txid + 1), self.pin.as_ref(), &cut.update())?;
        let index = self.group.applied()?.position.index.get() + 1;
        let mut write = GroupWrite::new(&self.group);
        // The mechanical storage fixture can lie about applied bytes. A real
        // ImageApplier refuses such entries; archive replay must also refuse
        // them before advancing the independently verified head.
        let mut update = CellUpdate::decode(&self.payload(&cut)?, EnvelopeLimits::default())?;
        mutate(&mut update);
        let payload = update.encode(EnvelopeLimits::default())?;
        write.entries.push(Entry {
            index,
            term: 1,
            data: payload.into(),
            ..Default::default()
        });
        let mut hard = self.group.initial_state()?.hard_state;
        hard.commit = index;
        write.hard_state = Some(hard);
        let state = pin.state();
        write.applied = Some(DurableApply {
            position: LogPosition {
                index: LogIndex::new(index),
                term: Term::new(1),
            },
            image: Some(CommittedImage {
                name: format!("{}.sqlite", hex::encode(pin.id().bytes())),
                digest: state.digest,
                length: state.length,
                ltx_epoch: state.epoch,
                txid: state.txid.0,
                checksum: state.checksum,
                page_size: state.page_size,
            }),
        });
        self.log
            .persist(OperationId::new(index), vec![write])
            .await?;
        let published = ImageApplier::recover(
            self.group.clone(),
            self.images.clone(),
            [9; 32],
            EnvelopeLimits::default(),
        )?
        .current()
        .ok_or_else(|| anyhow::anyhow!("missing published image"))?;
        let archive_cut = CommittedArchiveCut::capture(
            &self.group,
            published,
            dependencies,
            ArchiveLimits::default(),
        )?;
        if let Some(old) = self.pin.take() {
            self.images.retire(old.id())?;
        }
        self.pin = Some(pin);
        self.bytes = next;
        self.txid += 1;
        self.cut = Some(archive_cut.clone());
        self.images.reclaim(8)?;
        Ok(archive_cut)
    }

    pub(super) async fn publish(
        &self,
        cut: CommittedArchiveCut,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        self.archive.publish(cut, &self.verifier).await
    }

    pub(super) async fn close(self) -> Result<()> {
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }

    pub(super) async fn protocol(&self, learner: bool) -> Result<CommittedArchiveCut> {
        let mut marker = self.group.applied()?;
        let index = marker.position.index.get() + 1;
        marker.position.index = LogIndex::new(index);
        let mut write = GroupWrite::new(&self.group);
        let mut entry = Entry {
            index,
            term: 1,
            ..Default::default()
        };
        let mut hard = self.group.initial_state()?.hard_state;
        hard.commit = index;
        write.hard_state = Some(hard);
        write.applied = Some(marker);
        if learner {
            let mut change = ConfChangeV2::default();
            change.mut_changes().push(ConfChangeSingle {
                node_id: 2,
                change_type: ConfChangeType::AddLearnerNode,
                ..Default::default()
            });
            entry.set_entry_type(EntryType::EntryConfChangeV2);
            let mut conf = self.group.initial_state()?.conf_state;
            let before = conf.clone();
            conf.learners.push(2);
            let membership = self.group.membership()?;
            membership.bind_change(
                &mut change,
                &[JoiningMember {
                    member: MemberId::new(2)?,
                    node: NodeId::new([2; 16])?,
                    disk: DiskLineage::new([12; 16])?,
                }],
            )?;
            entry.data = change.write_to_bytes()?.into();
            write.membership =
                Some(membership.applied(&change, &before, &conf, LogIndex::new(index))?);
            write.conf_state = Some(conf);
        }
        write.entries.push(entry);
        self.log
            .persist(OperationId::new(index), vec![write])
            .await?;
        let image = ImageApplier::recover(
            self.group.clone(),
            self.images.clone(),
            [9; 32],
            EnvelopeLimits::default(),
        )?
        .current()
        .ok_or_else(|| anyhow::anyhow!("missing published image"))?;
        Ok(CommittedArchiveCut::capture(
            &self.group,
            image,
            vec![],
            ArchiveLimits::default(),
        )?)
    }
}
