// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::PreparedGroupManifest;
use super::{objects::Object, *};
use crate::raft::{cache::GroupLog, member_state::GroupMembership};
use raft::prelude::ConfState;
use std::sync::atomic::{AtomicBool, Ordering};

const TAIL_DOMAIN: &str = "cellar-cell-group-archive-tail-1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// An untrusted immutable-tail locator; recovery must verify its entire closure.
pub struct CellGroupArchiveTailReference {
    registry: [u8; 32],
    generation: u64,
    digest: [u8; 32],
    length: u64,
}

impl CellGroupArchiveTailReference {
    pub fn registry(&self) -> [u8; 32] {
        self.registry
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn object(&self) -> Object {
        Object {
            registry: self.registry,
            generation: self.generation,
            digest: self.digest,
            length: self.length,
        }
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn length(&self) -> u64 {
        self.length
    }
}

/// An upload's exact committed-source witness, captured before asynchronous I/O.
/// This value cannot be deserialized. Persist its untrusted reference separately;
/// publication needs this witness or reconciliation of an existing publication.
#[derive(Clone)]
pub struct CellGroupArchiveUploadedTail {
    reference: CellGroupArchiveTailReference,
    pub(super) source: Arc<super::publication::SourceCut>,
}

impl CellGroupArchiveUploadedTail {
    pub fn reference(&self) -> &CellGroupArchiveTailReference {
        &self.reference
    }

    pub fn into_reference(self) -> CellGroupArchiveTailReference {
        self.reference
    }
}

impl std::ops::Deref for CellGroupArchiveUploadedTail {
    type Target = CellGroupArchiveTailReference;

    fn deref(&self) -> &Self::Target {
        &self.reference
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cut {
    pub marker: GroupManifest,
    pub conf: Vec<u8>,
    pub membership: GroupMembership,
}

impl Cut {
    pub(super) fn configuration(&self) -> Result<ConfState> {
        crate::raft::manifest::decode_conf(&self.conf)
            .map_err(|_| CellGroupArchiveError::Invalid("tail configuration"))
    }

    fn validate(&self, history: GroupHistory, limits: CellGroupArchiveTailLimits) -> Result<()> {
        self.marker
            .check()
            .map_err(CellGroupCheckpointError::from)?;
        if self.marker.route().history != history
            || self.marker.root().cells() > limits.max_cells
            || self.conf.len() > 64 * 1024
            || self.membership.revision().get() != self.marker.membership_revision()
        {
            return Err(CellGroupArchiveError::Invalid("tail cut binding/bounds"));
        }
        self.membership
            .validate(&self.configuration()?, self.marker.position().index.get())
            .map_err(CellGroupCheckpointError::from)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Tail {
    domain: String,
    pub checkpoint: CellGroupArchiveReference,
    pub base: GroupManifest,
    pub cut: Cut,
    bytes: u64,
    pub entries: Vec<EntryObject>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EntryObject {
    index: u64,
    term: u64,
    pub object: Object,
}

impl Tail {
    fn validate(
        &self,
        archive: &CellGroupArchive,
        limits: CellGroupArchiveTailLimits,
    ) -> Result<()> {
        self.base.check().map_err(CellGroupCheckpointError::from)?;
        self.cut.validate(archive.history, limits)?;
        if self.domain != TAIL_DOMAIN
            || self.base.route() != self.cut.marker.route()
            || self.checkpoint.length == 0
            || self.checkpoint.length > archive.limits.max_index_bytes as u64
            || self.entries.len() > limits.max_entries
            || self.bytes > limits.max_tail_bytes as u64
            || self
                .cut
                .marker
                .position()
                .index
                .get()
                .checked_sub(self.base.position().index.get())
                != Some(self.entries.len() as u64)
        {
            return Err(CellGroupArchiveError::Invalid("tail domain/cuts/limits"));
        }
        let mut index = self.base.position().index.get();
        let mut term = self.base.position().term.get();
        let mut bytes = 0u64;
        for entry in &self.entries {
            index = index.checked_add(1).ok_or(CellGroupArchiveError::Limit)?;
            if entry.index != index
                || entry.term < term
                || entry.object.length == 0
                || entry.object.length > limits.max_tail_bytes as u64
            {
                return Err(CellGroupArchiveError::Invalid("tail continuity"));
            }
            term = entry.term;
            bytes = bytes
                .checked_add(entry.object.length)
                .ok_or(CellGroupArchiveError::Limit)?;
        }
        if bytes != self.bytes || term != self.cut.marker.position().term.get() {
            return Err(CellGroupArchiveError::Invalid("tail total/final term"));
        }
        Ok(())
    }
}

struct Capture {
    pub cut: Cut,
    entries: Vec<(u64, u64, Vec<u8>)>,
    authority: Arc<super::publication::SourceCut>,
}

fn capture(
    group: GroupLog<GroupHistory>,
    base: &GroupManifest,
    current: PreparedGroupManifest,
    limits: CellGroupArchiveTailLimits,
    source_budget: Arc<Budget>,
) -> Result<Capture> {
    if !Arc::ptr_eq(&group.owner, &current.engine_owner)
        || base.route() != current.marker().route()
        || current.marker().root().cells() > limits.max_cells
    {
        return Err(CellGroupArchiveError::Invalid("tail source owner/route"));
    }
    let cache = group.read().map_err(CellGroupCheckpointError::from)?;
    let target = current.marker().position();
    let count = target
        .index
        .get()
        .checked_sub(base.position().index.get())
        .filter(|n| *n <= limits.max_entries as u64)
        .ok_or(CellGroupArchiveError::Limit)?;
    if cache.group_manifest.as_ref() != Some(current.marker())
        || cache.applied.position != target
        || cache.applied.image.is_some()
        || cache.hard_state.commit < target.index.get()
        || cache.term(target.index.get()) != Some(target.term.get())
        || cache.term(base.position().index.get()) != Some(base.position().term.get())
    {
        return Err(CellGroupArchiveError::Invalid("tail source committed cut"));
    }
    let mut entries = Vec::with_capacity(count as usize);
    let mut bytes = 0usize;
    let mut index = base.position().index.get();
    for _ in 0..count {
        index += 1;
        let entry = cache
            .entries
            .get(&index)
            .ok_or(CellGroupArchiveError::Invalid(
                "tail source log hole/compaction",
            ))?;
        bytes = bytes
            .checked_add(tail_codec::length(entry)?)
            .filter(|n| *n <= limits.max_tail_bytes)
            .ok_or(CellGroupArchiveError::Limit)?;
        entries.push(entry.clone());
    }
    let cut = Cut {
        marker: current.marker().clone(),
        conf: crate::raft::manifest::encode_conf(&cache.conf_state)
            .map_err(|_| CellGroupArchiveError::Invalid("tail configuration encoding"))?,
        membership: cache.membership.clone(),
    };
    let configuration = cache.conf_state.clone();
    let membership = cache.membership.clone();
    drop(cache); // No engine/cache lock across encoding or physical/S3 work.
    cut.validate(group.identity().scope, limits)?;
    let entries = entries
        .iter()
        .map(|entry| {
            Ok((
                entry.index,
                entry.term,
                tail_codec::encode(entry, limits.max_tail_bytes)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Capture {
        cut,
        entries,
        authority: Arc::new(super::publication::SourceCut::from_capture(
            group,
            current,
            configuration,
            membership,
            source_budget,
        )?),
    })
}

enum Origin<'a> {
    Checkpoint(&'a CellGroupArchiveReference),
    Tail(&'a CellGroupArchiveTailReference),
}

struct Stop(Arc<AtomicBool>);

impl Drop for Stop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl CellGroupArchiveUpload {
    /// Upload one complete captured cut without recapturing the live engine
    /// after object-store I/O. Quorum may continue applying throughout upload.
    /// The checkpoint pool must admit the source before this call; its slot is
    /// released before the caller starts independent recovery verification.
    pub async fn upload_cut(
        &self,
        domain: &ExecutionDomain,
        pool: &CellGroupArchiveTailPool,
        checkpoint: CellGroupCheckpoint,
    ) -> Result<CellGroupArchiveUploadedTail> {
        domain
            .run(async {
                let _retained = pool.admit()?;
                let (group, root, configuration, membership) = checkpoint.archive_source()?;
                let cut = Cut {
                    marker: root.marker().clone(),
                    conf: crate::raft::manifest::encode_conf(&configuration).map_err(|_| {
                        CellGroupArchiveError::Invalid("tail configuration encoding")
                    })?,
                    membership: membership.clone(),
                };
                cut.validate(self.history, pool.limits)?;
                let source = Arc::new(super::publication::SourceCut::from_capture(
                    group,
                    root,
                    configuration,
                    membership,
                    self.source_budget(),
                )?);
                let reference = self.upload_checkpoint(domain, checkpoint).await?;
                let io = self.admit()?;
                self.registry().pending(&self.ticket()).await?;
                let tail = Tail {
                    domain: TAIL_DOMAIN.into(),
                    checkpoint: reference,
                    base: cut.marker.clone(),
                    cut,
                    bytes: 0,
                    entries: Vec::new(),
                };
                tail.validate(self, pool.limits)?;
                let mut output = checkpoint::Bounded {
                    bytes: Vec::new(),
                    limit: self.limits.max_index_bytes,
                };
                serde_json::to_writer(&mut output, &tail)
                    .map_err(|_| CellGroupArchiveError::Limit)?;
                let object = self
                    .put("tails", output.bytes, self.limits.max_index_bytes)
                    .await?;
                drop(io);
                Ok(CellGroupArchiveUploadedTail {
                    reference: CellGroupArchiveTailReference {
                        registry: object.registry,
                        generation: object.generation,
                        digest: object.digest,
                        length: object.length,
                    },
                    source,
                })
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    /// Copy an exact contiguous committed suffix under the engine cache lock,
    /// then upload immutable entry objects without holding that lock. This
    /// returns an untrusted locator; all base bytes and replay must still verify.
    pub async fn upload_tail(
        &self,
        domain: &ExecutionDomain,
        pool: &CellGroupArchiveTailPool,
        checkpoint: &CellGroupArchiveReference,
        group: &GroupLog<GroupHistory>,
        current: &PreparedGroupManifest,
    ) -> Result<CellGroupArchiveUploadedTail> {
        self.upload_from(domain, pool, Origin::Checkpoint(checkpoint), group, current)
            .await
    }

    /// Extend an immutable archived prefix while copying only the local suffix
    /// after its last cut. This works after local prefix compaction; references
    /// to prior entry objects are reused. Full closure verification is still
    /// required, and the caller's publication owner must protect the base/prefix.
    pub async fn extend_tail(
        &self,
        domain: &ExecutionDomain,
        pool: &CellGroupArchiveTailPool,
        previous: &CellGroupArchiveTailReference,
        group: &GroupLog<GroupHistory>,
        current: &PreparedGroupManifest,
    ) -> Result<CellGroupArchiveUploadedTail> {
        self.upload_from(domain, pool, Origin::Tail(previous), group, current)
            .await
    }

    async fn upload_from(
        &self,
        domain: &ExecutionDomain,
        pool: &CellGroupArchiveTailPool,
        origin: Origin<'_>,
        group: &GroupLog<GroupHistory>,
        current: &PreparedGroupManifest,
    ) -> Result<CellGroupArchiveUploadedTail> {
        domain
            .run(async {
                let io = self.admit()?;
                self.registry().pending(&self.ticket()).await?;
                let retention = pool.admit()?;
                let limits = pool.limits;
                let (checkpoint, base, worker_base, prefix, prefix_bytes) = match origin {
                    Origin::Checkpoint(checkpoint) => {
                        let marker = self
                            .checkpoint_marker(domain, checkpoint, io.clone())
                            .await?;
                        (checkpoint.clone(), marker.clone(), marker, Vec::new(), 0)
                    }
                    Origin::Tail(previous) => {
                        let tail = self
                            .read_tail_index(
                                domain,
                                previous,
                                limits,
                                io.clone(),
                                retention.clone(),
                            )
                            .await?;
                        (
                            tail.checkpoint,
                            tail.base,
                            tail.cut.marker,
                            tail.entries,
                            tail.bytes,
                        )
                    }
                };
                let group = group.clone();
                let root = current.clone();
                let retained = retention.clone();
                let retained_io = io.clone();
                let source_budget = self.source_budget();
                let captured = domain
                    .blocking(move || {
                        let (_retained, _io) = (retained, retained_io);
                        capture(group, &worker_base, root, limits, source_budget)
                    })
                    .map_err(|_| CellGroupArchiveError::Stopped)?
                    .await
                    .map_err(CellGroupArchiveError::Worker)??;
                let mut tail = Tail {
                    domain: TAIL_DOMAIN.into(),
                    checkpoint,
                    base,
                    cut: captured.cut,
                    bytes: prefix_bytes,
                    entries: prefix,
                };
                let total_entries = tail
                    .entries
                    .len()
                    .checked_add(captured.entries.len())
                    .ok_or(CellGroupArchiveError::Limit)?;
                let total_bytes = captured
                    .entries
                    .iter()
                    .try_fold(tail.bytes, |sum, (_, _, bytes)| {
                        sum.checked_add(bytes.len() as u64)
                    })
                    .ok_or(CellGroupArchiveError::Limit)?;
                if total_entries > limits.max_entries || total_bytes > limits.max_tail_bytes as u64
                {
                    return Err(CellGroupArchiveError::Limit);
                }
                for (index, term, entry) in captured.entries {
                    let object = self.put("entries", entry, limits.max_tail_bytes).await?;
                    tail.bytes = tail
                        .bytes
                        .checked_add(object.length)
                        .ok_or(CellGroupArchiveError::Limit)?;
                    tail.entries.push(EntryObject {
                        index,
                        term,
                        object,
                    });
                }
                tail.validate(self, limits)?;
                let archive = self.clone();
                let retained = retention.clone();
                let retained_io = io.clone();
                let encoded = domain
                    .blocking(move || {
                        let (_retained, _io) = (retained, retained_io);
                        let mut output = checkpoint::Bounded {
                            bytes: Vec::new(),
                            limit: archive.limits.max_index_bytes,
                        };
                        serde_json::to_writer(&mut output, &tail)
                            .map_err(|_| CellGroupArchiveError::Limit)?;
                        Ok::<_, CellGroupArchiveError>(output.bytes)
                    })
                    .map_err(|_| CellGroupArchiveError::Stopped)?
                    .await
                    .map_err(CellGroupArchiveError::Worker)??;
                let object = self
                    .put("tails", encoded, self.limits.max_index_bytes)
                    .await?;
                Ok(CellGroupArchiveUploadedTail {
                    reference: CellGroupArchiveTailReference {
                        registry: object.registry,
                        generation: object.generation,
                        digest: object.digest,
                        length: object.length,
                    },
                    source: captured.authority,
                })
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

impl CellGroupArchive {
    pub(super) async fn read_tail_index(
        &self,
        domain: &ExecutionDomain,
        reference: &CellGroupArchiveTailReference,
        limits: CellGroupArchiveTailLimits,
        io: Arc<Permit>,
        retention: Arc<Permit>,
    ) -> Result<Tail> {
        let bytes = self
            .get(
                "tails",
                &Object {
                    registry: reference.registry,
                    generation: reference.generation,
                    digest: reference.digest,
                    length: reference.length,
                },
                self.limits.max_index_bytes,
            )
            .await?;
        let archive = self.clone();
        let retained = retention.clone();
        let retained_io = io.clone();
        domain
            .blocking(move || {
                let (_retained, _io) = (retained, retained_io);
                let tail: Tail = serde_json::from_slice(&bytes)
                    .map_err(|_| CellGroupArchiveError::Invalid("tail index"))?;
                tail.validate(&archive, limits)?;
                Ok::<_, CellGroupArchiveError>(tail)
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?
            .await
            .map_err(CellGroupArchiveError::Worker)?
    }

    /// Independently recover the base and replay every cell/control entry.
    /// Retains replay admission with the returned physical closure. No archive
    /// head/GC protection, serving permission or S3 settlement is supplied here.
    pub async fn verify_tail(
        &self,
        domain: &ExecutionDomain,
        pool: &CellGroupArchiveTailPool,
        reference: &CellGroupArchiveTailReference,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<VerifiedCellGroupArchiveRecovery> {
        domain
            .run(async {
                let io = self.admit()?;
                let retention = pool.admit()?;
                let limits = pool.limits;
                let tail = self
                    .read_tail_index(domain, reference, limits, io.clone(), retention.clone())
                    .await?;
                let mut encoded = Vec::with_capacity(tail.entries.len());
                for entry in &tail.entries {
                    encoded.push(
                        self.get("entries", &entry.object, limits.max_tail_bytes)
                            .await?,
                    );
                }
                let retained = retention.clone();
                let retained_io = io.clone();
                let descriptors = tail.entries;
                let entries = domain
                    .blocking(move || {
                        let (_retained, _io) = (retained, retained_io);
                        encoded
                            .into_iter()
                            .zip(descriptors)
                            .map(|(bytes, expected)| {
                                let entry = tail_codec::decode(&bytes, limits.max_tail_bytes)?;
                                if entry.index != expected.index || entry.term != expected.term {
                                    return Err(CellGroupArchiveError::Invalid(
                                        "tail entry descriptor",
                                    ));
                                }
                                Ok(entry)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .map_err(|_| CellGroupArchiveError::Stopped)?
                    .await
                    .map_err(CellGroupArchiveError::Worker)??;
                let base = self
                    .verify_admitted(domain, &tail.checkpoint, receiver, io.clone())
                    .await?;
                if base.checkpoint().marker() != &tail.base {
                    return Err(CellGroupArchiveError::Invalid("tail checkpoint binding"));
                }
                let stop = Stop(Arc::new(AtomicBool::new(false)));
                let cancelled = stop.0.clone();
                let reference = reference.clone();
                let retained_io = io.clone();
                domain
                    .blocking(move || {
                        let _io = retained_io;
                        replay::replay(
                            reference, base, tail.cut, entries, limits, retention, cancelled,
                        )
                    })
                    .map_err(|_| CellGroupArchiveError::Stopped)?
                    .await
                    .map_err(CellGroupArchiveError::Worker)?
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}
