// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh-history data preparation. No engine mapping, catalog publication,
//! membership, callback, log acknowledgement or serving permission is created.
use super::{super::*, CellGroupRecoveryPool};
use cellar_logic::{cell_group::*, consensus::*};
use cellar_ltx::{
    internal::{ImageBinding, EXTENT_BYTES},
    stream_snapshot::ScratchPool,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

type Result<T> = std::result::Result<T, CellGroupArchiveError>;

/// Complete imported data under fresh histories. Clones retain the same
/// physical owners and admission. This grants no engine or serving authority;
/// only a catalog-authorized genesis installation may select it in a Raft log.
#[derive(Clone)]
pub struct PreparedCellGroupRecovery {
    fence: FencedCellGroupRecovery,
    store: CellGroupArchiveStore,
    manifest: PreparedGroupManifest,
    commitment: [u8; 32],
}

impl PreparedCellGroupRecovery {
    pub fn request(&self) -> &CellGroupRecoveryRequest {
        self.fence.request()
    }

    pub fn source(&self) -> &FencedCellGroupRecovery {
        &self.fence
    }

    pub fn store(&self) -> &CellGroupArchiveStore {
        &self.store
    }

    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.manifest
    }

    pub fn commitment(&self) -> [u8; 32] {
        self.commitment
    }
}

struct Stop(Arc<AtomicBool>);

impl Drop for Stop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl CellGroupRecoveryPool {
    /// Copy the complete fenced cut into dedicated replacement stores, with
    /// new cell histories and genesis witnesses. SQLite bytes (including receipt
    /// results, client floors and effect identity) remain exact. The source hold
    /// is provenance and must remain retained after this preparation is dropped.
    pub async fn prepare(
        &self,
        domain: &impl CellGroupArchiveExecution,
        fence: FencedCellGroupRecovery,
        target: CellGroupArchiveStore,
        scratch: ScratchPool,
        directory: PathBuf,
    ) -> Result<PreparedCellGroupRecovery> {
        domain
            .run(async {
                let admission = self.admit()?;
                let stop = Stop(Arc::new(AtomicBool::new(false)));
                let cancelled = stop.0.clone();
                let limits = self.limits;
                domain
                    .blocking(move || {
                        let check = || {
                            if cancelled.load(Ordering::Acquire) {
                                return Err(CellGroupArchiveError::Stopped);
                            }
                            target.require_live()?;
                            fence
                                .coverage()
                                .recovery()
                                .checkpoint()
                                .checkpoint()
                                .store()
                                .require_live()?;
                            Ok(())
                        };
                        check()?;
                        if target.route() != fence.request().replacement().route()
                            || !directory.is_absolute()
                        {
                            return Err(CellGroupArchiveError::Invalid(
                                "recovery destination binding",
                            ));
                        }
                        let source = fence.coverage().recovery().manifest();
                        if source.pinned_images() > limits.max_cells as usize {
                            return Err(CellGroupArchiveError::Limit);
                        }
                        let mut total = 0u64;
                        source.images.visit(&mut |_, leaf, _, _, _| -> Result<()> {
                            if leaf.geometry.length > limits.max_image_bytes {
                                return Err(CellGroupArchiveError::Limit);
                            }
                            total = total
                                .checked_add(leaf.geometry.length)
                                .filter(|n| *n <= limits.max_restore_bytes)
                                .ok_or(CellGroupArchiveError::Limit)?;
                            Ok(())
                        })?;
                        let mut owner = target.storage.try_lock().map_err(|error| match error {
                            std::sync::TryLockError::WouldBlock => CellGroupArchiveError::Busy,
                            std::sync::TryLockError::Poisoned(_) => {
                                CellGroupArchiveError::Invalid("poisoned recovery destination")
                            }
                        })?;
                        let apply::Storage {
                            tree,
                            pages,
                            images,
                            ..
                        } = &mut *owner;
                        let mut pins = ManifestPins::default();
                        let mut root = ManifestRoot::default();
                        source
                            .images
                            .visit(&mut |_, old, pin, _, _| -> Result<()> {
                                check()?;
                                let mut leaf = old.clone();
                                leaf.version.history.incarnation =
                                    target.route().history.incarnation;
                                leaf.version.sequence = MutationSequence::new(1).map_err(|_| {
                                    CellGroupArchiveError::Invalid("recovery sequence")
                                })?;
                                let pin = copy_image(
                                    images, pin, &mut leaf, &scratch, &directory, &check,
                                )?;
                                leaf.enrollment = EnrollmentRecord::new(
                                    leaf.version.history.clone(),
                                    fence.request().operation(),
                                    target.route().schema_digest,
                                    leaf.version.image,
                                )
                                .map_err(CellGroupArchiveError::Enrollment)?;
                                leaf.last_mutation = LogPosition {
                                    index: LogIndex::new(1),
                                    term: Term::new(1),
                                };
                                leaf.witness = CommitWitness {
                                    group: target.route().history,
                                    position: leaf.last_mutation,
                                    proposal: fence.request().operation(),
                                };
                                root = tree
                                    .update(pages, root, leaf.clone(), None)
                                    .map_err(manifest_error)?;
                                pins = pins
                                    .updated(tree, pages, root, &leaf, &pin)
                                    .map_err(manifest_error)?;
                                Ok(())
                            })?;
                        let marker = GroupManifest::recovery_genesis(
                            target.route().clone(),
                            root,
                            fence.request().digest(),
                            fence.request().operation(),
                        )
                        .map_err(manifest_error)?;
                        let commitment = marker
                            .recovery
                            .as_deref()
                            .ok_or(CellGroupArchiveError::Invalid("recovery lifecycle missing"))?
                            .seed_commitment(&marker)
                            .map_err(manifest_error)?;
                        let mut manifest = tree
                            .prepare_archive_complete(&target.owner, pages, marker, &mut |leaf| {
                                let key = crate::application::consensus::cell_routing_key(
                                    target.route().history.namespace,
                                    &leaf.version.history.cell,
                                );
                                pins.get(&key)
                                    .filter(|(selected, _)| *selected == leaf)
                                    .map(|(_, pin)| pin)
                                    .ok_or(ManifestError::Invalid("recovery closure changed"))
                            })
                            .map_err(manifest_error)?;
                        manifest.recovery_admission = Some(admission);
                        check()?;
                        drop(owner);
                        Ok(PreparedCellGroupRecovery {
                            fence,
                            store: target,
                            manifest,
                            commitment,
                        })
                    })
                    .map_err(|_| CellGroupArchiveError::Stopped)?
                    .await
                    .map_err(CellGroupArchiveError::Worker)?
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

fn manifest_error(error: ManifestError) -> CellGroupArchiveError {
    CellGroupCheckpointError::from(error).into()
}

fn physical(error: impl Into<CellGroupCheckpointError>) -> CellGroupArchiveError {
    error.into().into()
}

fn copy_image(
    images: &CellGroupApplyImages,
    source: &CellImagePin,
    leaf: &mut CellManifestLeaf,
    scratch: &ScratchPool,
    directory: &std::path::Path,
    check: &impl Fn() -> Result<()>,
) -> Result<CellImagePin> {
    match (images, source) {
        (_, CellImagePin::Empty) => Ok(CellImagePin::Empty),
        (CellGroupApplyImages::Materialized { images, .. }, CellImagePin::Materialized(source)) => {
            source.verify_and_sync().map_err(physical)?;
            let state = source.state();
            let id = apply::images::image_id(&leaf.version.history, 1, state)
                .map_err(CellGroupArchiveError::Replay)?;
            let mut receive = images
                .receive_image(id, state, scratch, directory)
                .map_err(physical)?;
            let mut reader = source.open_reader().map_err(physical)?;
            let mut offset = 0;
            while offset < state.length {
                check()?;
                let bytes = reader
                    .read_exact_at(offset, (state.length - offset).min(64 * 1024) as usize)
                    .map_err(physical)?;
                receive.append(offset, &bytes).map_err(physical)?;
                offset += bytes.len() as u64;
            }
            Ok(CellImagePin::Materialized(
                receive.finish().map_err(physical)?,
            ))
        }
        (CellGroupApplyImages::Extents { images, .. }, CellImagePin::Extents(source)) => {
            let old = source.manifest();
            let manifest = old
                .with_updates(
                    ImageBinding {
                        history: cell_image_history_digest(&leaf.version.history),
                        ..old.binding()
                    },
                    old.geometry(),
                    &[],
                )
                .map_err(cellar_ltx::internal::StoreError::from)
                .map_err(physical)?;
            // Protect introduced object identities from concurrent collection
            // before the first copy, including deduplicated existing extents.
            let staged = images
                .stage_transient_root(manifest.clone())
                .map_err(physical)?;
            for index in 0..old.geometry().extent_count() {
                check()?;
                let id = old
                    .extent(index)
                    .ok_or(CellGroupArchiveError::Invalid("source extent missing"))?;
                let bytes = source.view().read_extent(id).map_err(physical)?;
                images.put_extent_bytes(id, &bytes).map_err(physical)?;
            }
            let pin = Arc::new(
                images
                    .seal_image_root(manifest, &mut vec![0; EXTENT_BYTES as usize])
                    .map_err(physical)?,
            );
            drop(staged);
            leaf.version.image.commitment = CellImageCommitment::Extents {
                root: pin.reference().root().bytes(),
                manifest_bytes: pin.reference().encoded_length(),
            };
            Ok(CellImagePin::Extents(pin))
        }
        _ => Err(CellGroupArchiveError::Invalid(
            "recovery image representation",
        )),
    }
}
