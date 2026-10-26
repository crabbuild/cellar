// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Incremental physical observations, independent of proposed LTX data.

use super::{overlay::CommittedOverlay, Db, Error, Result};
use crate::extent::tree::{ExtentId, ExtentManifest, Geometry, ImageBinding, EXTENT_BYTES};
use sha2::Digest;
use std::{collections::BTreeSet, sync::Arc};

/// Work performed by this physical observation, not a recovery-time guarantee.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservationWork {
    pub reused_base: bool,
    pub image_bytes_read: u64,
    pub extents_hashed: u32,
}

/// An immutable observation bound to one live private Db owner. A reopened or
/// rebuilt workspace cannot reuse it. It grants no sealing or Raft authority.
pub struct ExtentObservation {
    manifest: ExtentManifest,
    owner: Arc<()>,
    wal_header: Vec<u8>,
    commit_end: u64,
    prefix: [u8; 32],
    wal_extents: Vec<u32>,
    main: Option<(Arc<crate::extent::working::MainWorkspace>, u64)>,
    work: ObservationWork,
}

impl ExtentObservation {
    pub fn manifest(&self) -> &ExtentManifest {
        &self.manifest
    }

    pub fn work(&self) -> ObservationWork {
        self.work
    }
}

/// Independently read committed main/WAL bytes into typed extent identities.
/// The caller must exclude all writers and checkpoints throughout this call.
/// No proposed LTX bytes, caller-supplied dirty pages or flat image digests are
/// used. Existing observations remain immutable if inspection fails.
///
/// Reuse requires the same live Db, history/schema/epoch, page size and a complete
/// SHA-256 match of the previous committed WAL prefix. A tracked extent working
/// VFS can additionally establish a sealed baseline and actual main-file writes
/// across checkpoints. On WAL reset, both prior and current WAL extents are
/// reread, so missing old frames cannot silently reuse an old logical image.
/// Other VFS providers fall back to full inspection. Every WAL frame is checksum verified within
/// the independent WAL budget; image reads/hash buffers remain at most 64 KiB.
/// The owner must still independently seal destination extents and authenticate
/// the new manifest through its exact generation/consensus publication gates.
pub fn observe_extents(
    db: &Db,
    previous: Option<&ExtentObservation>,
    binding: ImageBinding,
    length: u64,
    max_wal_bytes: u64,
) -> Result<ExtentObservation> {
    let geometry = Geometry::new(db.page_size(), length).map_err(root_error)?;
    binding.validate(geometry).map_err(root_error)?;
    let owner = db.extent_observer.get_or_init(|| Arc::new(()));
    if let Some(previous) = previous {
        let before = previous.manifest.binding();
        if !Arc::ptr_eq(&previous.owner, owner)
            || before.history != binding.history
            || before.schema != binding.schema
            || before.epoch != binding.epoch
            || before.txid > binding.txid
            || previous.manifest.geometry().page_size() != geometry.page_size()
        {
            return Err(Error::Other(
                "extent observation owner or history differs".into(),
            ));
        }
    }
    let main = crate::paged_vfs::extent_workspace(&db.conn)?;
    let main_cut = main
        .as_ref()
        .map(|main| {
            let since = match previous.and_then(|before| before.main.as_ref()) {
                Some((owner, sequence)) if Arc::ptr_eq(owner, main) => *sequence,
                Some(_) => return Err(Error::Other("extent main owner differs".into())),
                None => 0,
            };
            main.cut(since)
                .map_err(|_| Error::Other("extent main observation failed".into()))
        })
        .transpose()?;
    if main.is_none() && previous.is_some_and(|before| before.main.is_some()) {
        return Err(Error::Other("extent main owner disappeared".into()));
    }
    if let Some(cut) = &main_cut {
        let before = cut.baseline.binding();
        if before.history != binding.history
            || before.schema != binding.schema
            || before.epoch != binding.epoch
            || before.txid > binding.txid
            || (cut.baseline.geometry().length() != 0
                && cut.baseline.geometry().page_size() != geometry.page_size())
        {
            return Err(Error::Other("extent main baseline history differs".into()));
        }
    }
    let mut overlay = CommittedOverlay::open(
        db,
        length,
        db.page_size(),
        max_wal_bytes,
        previous.map(|previous| previous.commit_end),
        true,
    )?;
    let continuous = previous.filter(|previous| {
        previous.wal_header == overlay.header && overlay.previous_prefix == Some(previous.prefix)
    });
    let baseline = if main_cut.is_some() {
        previous
            .map(|before| &before.manifest)
            .or_else(|| main_cut.as_ref().map(|cut| &cut.baseline))
    } else {
        continuous.map(|before| &before.manifest)
    };
    let wal_extents = overlay.changed_extents(0).collect::<BTreeSet<_>>();
    let mut changed = BTreeSet::new();
    if let Some(baseline) = baseline {
        changed.extend(overlay.changed_extents(continuous.map_or(0, |before| before.commit_end)));
        if let Some(cut) = &main_cut {
            changed.extend(&cut.changed);
        }
        if continuous.is_none() {
            if let Some(before) = previous {
                changed.extend(&before.wal_extents);
            }
        }
        changed.retain(|index| geometry.extent_length(*index).is_some());
        let before = baseline.geometry();
        changed.extend(before.extent_count()..geometry.extent_count());
        if let Some(boundary) = before
            .extent_count()
            .min(geometry.extent_count())
            .checked_sub(1)
        {
            if before.extent_length(boundary) != geometry.extent_length(boundary) {
                changed.insert(boundary);
            }
        }
    } else {
        changed.extend(0..geometry.extent_count());
    }
    let mut updates = Vec::with_capacity(changed.len());
    let mut image_bytes_read = 0;
    for index in changed {
        let length = geometry
            .extent_length(index)
            .ok_or_else(|| Error::Other("extent observation coverage".into()))?;
        let mut hash = ExtentId::new_hasher(length);
        let mut offset = 0;
        while offset < u64::from(length) {
            let count = (u64::from(length) - offset).min(65536) as usize;
            hash.update(overlay.read(db, u64::from(index) * EXTENT_BYTES + offset, count)?);
            offset += count as u64;
        }
        image_bytes_read += u64::from(length);
        updates.push((
            index,
            ExtentId::from_stream(length, hash).map_err(root_error)?,
        ));
    }
    overlay.unchanged(db)?;
    if let (Some(main), Some(cut)) = (&main, &main_cut) {
        main.unchanged(cut.sequence)
            .map_err(|_| Error::Other("main file changed during extent observation".into()))?;
    }
    let reused_base = baseline.is_some();
    let manifest = if let Some(baseline) = baseline {
        baseline.with_updates(binding, geometry, &updates)
    } else {
        ExtentManifest::new(
            binding,
            geometry,
            &updates.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        )
    }
    .map_err(root_error)?;
    Ok(ExtentObservation {
        manifest,
        owner: owner.clone(),
        wal_header: overlay.header,
        commit_end: overlay.commit_end,
        prefix: overlay.prefix,
        wal_extents: wal_extents.into_iter().collect(),
        main: main.zip(main_cut.map(|cut| cut.sequence)),
        work: ObservationWork {
            reused_base,
            image_bytes_read,
            extents_hashed: updates.len() as u32,
        },
    })
}

fn root_error(error: crate::extent::tree::RootError) -> Error {
    Error::Other(Box::new(error))
}
