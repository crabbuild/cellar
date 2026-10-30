// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable storage views. Raft reads these bounded caches without disk I/O.

use super::cell_groups::{GroupManifest, ManifestPins, PreparedGroupManifest};
use super::member_state::GroupMembership;
use super::storage::LogError;
use crate::application::consensus::ImageCommitment;
use ::raft::prelude::{ConfState, Entry, EntryType, HardState, Snapshot};
use ::raft::{GetEntriesContext, RaftState, Storage, StorageError};
use cellar_logic::cell_group::GroupHistory;
use cellar_logic::consensus::{GroupScope, LogIndex, LogPosition, ReplicaIdentity, Term};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedImage {
    pub name: String,
    pub commitment: ImageCommitment,
    pub length: u64,
    pub ltx_epoch: u64,
    pub txid: u64,
    pub checksum: u64,
    pub page_size: u32,
}

impl CommittedImage {
    pub(crate) fn validate(&self) -> Result<(), LogError> {
        if self.name.is_empty()
            || self.name.len() > 128
            || self.name == "."
            || self.name == ".."
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            || self.length > cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES
            || self.ltx_epoch == 0
            || !cellar_ltx::ltx::is_valid_page_size(self.page_size)
        {
            return Err(LogError::Invalid("invalid committed image marker".into()));
        }
        if let ImageCommitment::Extents(reference) = self.commitment {
            let encoded = 112 + 32 * self.length.div_ceil(cellar_ltx::internal::EXTENT_BYTES);
            if self.length == 0
                || self.txid == 0
                || !self.length.is_multiple_of(u64::from(self.page_size))
                || u64::from(reference.encoded_length()) != encoded
                || self.name != format!("{}.root", hex::encode(reference.root().bytes()))
            {
                return Err(LogError::Invalid("invalid extent image marker".into()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod image_limit_tests {
    use super::*;

    #[test]
    fn durable_marker_accepts_decimal_quota_but_not_a_page_beyond_it() {
        let mut image = CommittedImage {
            name: "verified.sqlite".into(),
            commitment: [9; 32].into(),
            length: 10_000_000_000 / 4096 * 4096,
            ltx_epoch: 1,
            txid: 1,
            checksum: 0,
            page_size: 4096,
        };
        assert!(image.validate().is_ok());
        image.length += 4096;
        assert!(image.validate().is_err());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableApply {
    pub position: LogPosition,
    /// None denotes the canonical empty state before the schema command.
    pub image: Option<CommittedImage>,
}

#[derive(Clone, Debug)]
pub enum SnapshotChange {
    /// A verified local snapshot; retain entries beyond its included index.
    Checkpoint(Snapshot),
    /// A verified incoming snapshot and its already durable state image.
    Install {
        snapshot: Snapshot,
        applied: DurableApply,
    },
}

#[derive(Clone)]
pub struct GroupWrite<S = GroupScope> {
    pub(crate) consensus_pin: Option<Arc<dyn Send + Sync>>,
    pub group: GroupLog<S>,
    pub entries: Vec<Entry>,
    pub hard_state: Option<HardState>,
    pub conf_state: Option<ConfState>,
    pub membership: Option<GroupMembership>,
    pub applied: Option<DurableApply>,
    pub group_manifest: Option<PreparedGroupManifest>,
    pub snapshot: Option<SnapshotChange>,
}

impl<S: Clone> GroupWrite<S> {
    pub fn new(group: &GroupLog<S>) -> Self {
        Self {
            consensus_pin: None,
            group: group.clone(),
            entries: Vec::new(),
            hard_state: None,
            conf_state: None,
            membership: None,
            applied: None,
            group_manifest: None,
            snapshot: None,
        }
    }

    pub(crate) fn estimated_bytes(&self) -> Result<usize, LogError> {
        let mut bytes = 1024_usize;
        for entry in &self.entries {
            bytes = bytes
                .checked_add(entry.compute_size() as usize + 128)
                .ok_or(LogError::Overloaded)?;
        }
        if let Some(snapshot) = &self.snapshot {
            let snapshot = match snapshot {
                SnapshotChange::Checkpoint(s) | SnapshotChange::Install { snapshot: s, .. } => s,
            };
            if snapshot.compute_size() > 64 * 1024 {
                return Err(LogError::Overloaded);
            }
            bytes = bytes
                .checked_add(snapshot.compute_size() as usize)
                .ok_or(LogError::Overloaded)?;
        }
        if let Some(conf) = &self.conf_state {
            if conf.compute_size() > 16 * 1024 {
                return Err(LogError::Overloaded);
            }
            bytes = bytes
                .checked_add(conf.compute_size() as usize)
                .ok_or(LogError::Overloaded)?;
        }
        if let Some(membership) = &self.membership {
            bytes = bytes
                .checked_add(membership.reserved_bytes())
                .ok_or(LogError::Overloaded)?;
        }
        if let Some(manifest) = &self.group_manifest {
            bytes = bytes
                .checked_add(16 * 1024 + manifest.reserved_bytes())
                .ok_or(LogError::Overloaded)?;
        }
        Ok(bytes)
    }
}

/// Engine-owned storage handle. `GroupHistory` is the cell-group scope;
/// `GroupScope` remains only for the staged single-cell application adapter.
/// The engine rejects mixed application domains and never casts between them.
#[derive(Clone)]
pub struct GroupLog<S = GroupScope> {
    pub(crate) consensus_claimed: Arc<AtomicBool>,
    pub(crate) scratch_directory: std::path::PathBuf,
    pub(crate) snapshot_scratch: cellar_ltx::stream_snapshot::ScratchPool,
    pub(crate) filesystem: Arc<dyn cellar_ltx::FileSystem>,
    pub(crate) key: u64,
    pub(crate) birth_digest: [u8; 32],
    pub(crate) identity: ReplicaIdentity<S>,
    pub(crate) owner: Arc<()>,
    pub(crate) closed: Arc<AtomicBool>,
    pub(crate) cache: Arc<RwLock<CachedGroup>>,
    pub(crate) cache_revision: Arc<AtomicU64>,
}

impl<S: std::fmt::Debug> std::fmt::Debug for GroupLog<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupLog")
            .field("key", &self.key)
            .field("identity", &self.identity)
            .finish()
    }
}

impl<S> GroupLog<S> {
    pub fn identity(&self) -> &ReplicaIdentity<S> {
        &self.identity
    }

    pub fn numeric_key(&self) -> u64 {
        self.key
    }

    pub fn applied(&self) -> Result<DurableApply, LogError> {
        let cache = self.read()?;
        if cache.group_manifest.is_some() {
            return Err(LogError::Invalid(
                "cell-group root requires complete-manifest recovery".into(),
            ));
        }
        Ok(cache.applied.clone())
    }

    /// Engine-selected metadata only. Reverify every page/image before admission.
    pub fn group_manifest(&self) -> Result<Option<GroupManifest>, LogError> {
        Ok(self.read()?.group_manifest.clone())
    }

    /// Last synced bindings for this group history. This observation alone is
    /// neither a fresh-member seeding capability nor proof of live quorum.
    pub fn membership(&self) -> Result<GroupMembership, LogError> {
        Ok(self.read()?.membership.clone())
    }

    /// Invalidate derived accounting after synced cache publication. This is
    /// local to this cache instance, not a transferable durability proof.
    pub(crate) fn cache_revision(&self) -> Result<u64, LogError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LogError::Closed);
        }
        if self.cache.is_poisoned() {
            self.closed.store(true, Ordering::Release);
            return Err(LogError::Corrupt("Raft cache lock poisoned".into()));
        }
        Ok(self.cache_revision.load(Ordering::Acquire))
    }

    pub(crate) fn read(&self) -> Result<RwLockReadGuard<'_, CachedGroup>, LogError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LogError::Closed);
        }
        self.cache.read().map_err(|_| {
            self.closed.store(true, Ordering::Release);
            LogError::Corrupt("Raft cache lock poisoned".into())
        })
    }
}

/// One engine catalog, with explicit staged application domains. A caller must
/// select the right handle type; enumeration never silently skips another kind.
#[derive(Clone)]
pub(crate) enum StoredGroup {
    Cell(GroupLog),
    Cells(GroupLog<GroupHistory>),
}

impl StoredGroup {
    pub(crate) fn as_cell(&self) -> Result<&GroupLog, LogError> {
        match self {
            Self::Cell(group) => Ok(group),
            Self::Cells(_) => Err(LogError::WrongIdentity),
        }
    }

    pub(crate) fn as_cells(&self) -> Result<&GroupLog<GroupHistory>, LogError> {
        match self {
            Self::Cells(group) => Ok(group),
            Self::Cell(_) => Err(LogError::WrongIdentity),
        }
    }

    pub(crate) fn history(&self) -> GroupHistory {
        match self {
            Self::Cell(group) => GroupHistory::from(&group.identity.scope),
            Self::Cells(group) => group.identity.scope,
        }
    }

    pub(crate) fn is_cells(&self) -> bool {
        matches!(self, Self::Cells(_))
    }
}

fn raft_error(error: LogError) -> ::raft::Error {
    ::raft::Error::Store(StorageError::Other(Box::new(error)))
}

/// Mechanical Raft lookups may finish against the last durable cache while an
/// engine closes. This private view cannot establish an application proof.
#[derive(Clone)]
pub(crate) struct ConsensusStorage {
    cache: Arc<RwLock<CachedGroup>>,
    closed: Arc<AtomicBool>,
}

impl ConsensusStorage {
    fn read(&self) -> Result<RwLockReadGuard<'_, CachedGroup>, LogError> {
        self.cache.read().map_err(|_| {
            self.closed.store(true, Ordering::Release);
            LogError::Corrupt("Raft cache lock poisoned".into())
        })
    }
}

impl<S> GroupLog<S> {
    pub(crate) fn consensus_storage(&self) -> ConsensusStorage {
        ConsensusStorage {
            cache: self.cache.clone(),
            closed: self.closed.clone(),
        }
    }

    fn checked_storage(&self) -> ::raft::Result<ConsensusStorage> {
        drop(self.read().map_err(raft_error)?);
        Ok(self.consensus_storage())
    }
}

impl<S> Storage for GroupLog<S> {
    fn initial_state(&self) -> ::raft::Result<RaftState> {
        self.checked_storage()?.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> ::raft::Result<Vec<Entry>> {
        self.checked_storage()?
            .entries(low, high, max_size, context)
    }

    fn term(&self, index: u64) -> ::raft::Result<u64> {
        self.checked_storage()?.term(index)
    }

    fn first_index(&self) -> ::raft::Result<u64> {
        self.checked_storage()?.first_index()
    }

    fn last_index(&self) -> ::raft::Result<u64> {
        self.checked_storage()?.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> ::raft::Result<Snapshot> {
        self.checked_storage()?.snapshot(request_index, to)
    }
}

impl Storage for ConsensusStorage {
    fn initial_state(&self) -> ::raft::Result<RaftState> {
        let cache = self.read().map_err(raft_error)?;
        Ok(RaftState::new(
            cache.hard_state.clone(),
            cache.conf_state.clone(),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> ::raft::Result<Vec<Entry>> {
        let cache = self.read().map_err(raft_error)?;
        if low < cache.first_index() {
            return Err(StorageError::Compacted.into());
        }
        if low > high || high > cache.last_index().saturating_add(1) {
            return Err(StorageError::Unavailable.into());
        }
        let maximum = max_size.into().unwrap_or(u64::MAX);
        let mut bytes = 0_u64;
        let mut entries = Vec::new();
        for (_, entry) in cache.entries.range(low..high) {
            let next = bytes.saturating_add(entry.compute_size() as u64);
            if !entries.is_empty() && next > maximum {
                break;
            }
            entries.push(entry.clone());
            bytes = next;
        }
        Ok(entries)
    }

    fn term(&self, index: u64) -> ::raft::Result<u64> {
        let cache = self.read().map_err(raft_error)?;
        if index < cache.snapshot.get_metadata().index {
            return Err(StorageError::Compacted.into());
        }
        cache
            .term(index)
            .ok_or_else(|| StorageError::Unavailable.into())
    }

    fn first_index(&self) -> ::raft::Result<u64> {
        Ok(self.read().map_err(raft_error)?.first_index())
    }

    fn last_index(&self) -> ::raft::Result<u64> {
        Ok(self.read().map_err(raft_error)?.last_index())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> ::raft::Result<Snapshot> {
        let cache = self.read().map_err(raft_error)?;
        if cache.snapshot.get_metadata().index < request_index {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        Ok(cache.snapshot.clone())
    }
}

#[derive(Clone)]
pub(crate) struct CachedGroup {
    pub hard_state: HardState,
    pub conf_state: ConfState,
    pub membership: GroupMembership,
    pub snapshot: Snapshot,
    pub entries: BTreeMap<u64, Entry>,
    pub applied: DurableApply,
    pub group_manifest: Option<GroupManifest>,
    pub manifest_pins: Option<ManifestPins>,
}

impl CachedGroup {
    /// Advance a verified image view across durable protocol-only entries. The
    /// complete marker must remain identical; an older or uncommitted position
    /// cannot relabel this image. The caller already owns its verified pin.
    pub(crate) fn coherent_image_position(
        &self,
        image: &CommittedImage,
        position: LogPosition,
    ) -> Option<LogPosition> {
        let applied = &self.applied;
        (applied.image.as_ref() == Some(image)
            && applied.position.index >= position.index
            && applied.position.term >= position.term
            && (applied.position.index != position.index || applied.position.term == position.term)
            && applied.position.index.get() <= self.hard_state.commit)
            .then_some(applied.position)
    }

    pub fn first_index(&self) -> u64 {
        self.snapshot.get_metadata().index + 1
    }

    pub fn last_index(&self) -> u64 {
        self.entries
            .last_key_value()
            .map_or(self.snapshot.get_metadata().index, |(&index, _)| index)
    }

    pub fn term(&self, index: u64) -> Option<u64> {
        if index == self.snapshot.get_metadata().index {
            return Some(self.snapshot.get_metadata().term);
        }
        self.entries.get(&index).map(|entry| entry.term)
    }

    pub fn bytes(&self) -> usize {
        1024 + self.group_manifest.as_ref().map_or(0, |m| {
            16 * 1024
                + self.manifest_pins.as_ref().map_or(
                    ManifestPins::maximum_bytes(m.root().cells()),
                    ManifestPins::reserved_bytes,
                )
        }) + self.membership.reserved_bytes()
            + self.snapshot.compute_size() as usize
            + self.conf_state.compute_size() as usize
            + self
                .entries
                .values()
                .map(|entry| entry.compute_size() as usize + 128)
                .sum::<usize>()
    }

    pub fn validate(&self, max_entries: usize) -> Result<(), LogError> {
        validate_conf(&self.conf_state)?;
        self.membership
            .validate(&self.conf_state, self.applied.position.index.get())
            .map_err(|error| LogError::Corrupt(error.to_string()))?;
        validate_conf(self.snapshot.get_metadata().get_conf_state())?;
        let metadata = self.snapshot.get_metadata();
        if metadata.index == 0
            || metadata.term == 0
            || metadata.index == u64::MAX
            || self.snapshot.compute_size() > 64 * 1024
            || self.entries.len() > max_entries
            || self.hard_state.term < metadata.term
            || self.hard_state.commit < metadata.index
            || self.hard_state.commit > self.last_index()
            || self.applied.position.index.get() < metadata.index
            || self.applied.position.index.get() > self.hard_state.commit
            || self.term(self.applied.position.index.get())
                != Some(self.applied.position.term.get())
        {
            return Err(LogError::Corrupt("incoherent Raft checkpoint".into()));
        }
        let mut previous_index = metadata.index;
        let mut previous_term = metadata.term;
        for (&index, entry) in &self.entries {
            if previous_index.checked_add(1) != Some(index)
                || index != entry.index
                || index == u64::MAX
                || entry.term < previous_term
                || entry.term > self.hard_state.term
            {
                return Err(LogError::Corrupt("noncontiguous Raft log".into()));
            }
            previous_index = index;
            previous_term = entry.term;
        }
        if let Some(image) = &self.applied.image {
            image.validate()?;
        }
        if let Some(manifest) = &self.group_manifest {
            manifest
                .check()
                .map_err(|e| LogError::Corrupt(e.to_string()))?;
            if self.applied.image.is_some()
                || manifest.position() != self.applied.position
                || manifest.membership_revision() != self.membership.revision().get()
                || self
                    .manifest_pins
                    .as_ref()
                    .is_some_and(|pins| pins.root() != manifest.root())
            {
                return Err(LogError::Corrupt("incoherent cell-group root".into()));
            }
        } else if self.manifest_pins.is_some() {
            return Err(LogError::Corrupt("image pins without group root".into()));
        }
        Ok(())
    }

    pub fn changed<S>(
        &self,
        write: &GroupWrite<S>,
        max_entries: usize,
        history: GroupHistory,
    ) -> Result<Self, LogError> {
        let mut next = self.clone();
        if (self.group_manifest.is_some() || write.group_manifest.is_some())
            && (write.applied.is_some() || write.snapshot.is_some())
        {
            return Err(LogError::Invalid(
                "mixed image/group apply or unsupported group snapshot".into(),
            ));
        }
        if let Some(change) = &write.snapshot {
            let snapshot = match change {
                SnapshotChange::Checkpoint(s) | SnapshotChange::Install { snapshot: s, .. } => s,
            };
            let metadata = snapshot.get_metadata();
            if metadata.index < self.snapshot.get_metadata().index
                || metadata.index == u64::MAX
                || (metadata.index == self.snapshot.get_metadata().index
                    && snapshot != &self.snapshot)
            {
                return Err(LogError::Invalid("conflicting or stale snapshot".into()));
            }
            match change {
                SnapshotChange::Checkpoint(_) => {
                    if metadata.index > self.applied.position.index.get()
                        || self.term(metadata.index) != Some(metadata.term)
                    {
                        return Err(LogError::Invalid("unapplied local snapshot".into()));
                    }
                    next.entries = next.entries.split_off(&(metadata.index + 1));
                }
                SnapshotChange::Install { applied, .. } => {
                    if metadata.index < self.hard_state.commit
                        || applied.position.index.get() != metadata.index
                        || applied.position.term.get() != metadata.term
                        || applied.position.index < self.applied.position.index
                        || (self.applied.image.is_some() && applied.image.is_none())
                        || (applied.position.index == self.applied.position.index
                            && applied != &self.applied)
                    {
                        return Err(LogError::Invalid("invalid snapshot install marker".into()));
                    }
                    next.entries.clear();
                    next.applied = applied.clone();
                    next.conf_state = metadata.get_conf_state().clone();
                }
            }
            next.snapshot = snapshot.clone();
        }
        let mut previous = None;
        for entry in &write.entries {
            if entry.index < next.first_index()
                || entry.index == u64::MAX
                || entry.term == 0
                || previous.is_some_and(|last: u64| last.checked_add(1) != Some(entry.index))
            {
                return Err(LogError::Invalid("invalid Raft append range".into()));
            }
            previous = Some(entry.index);
            if entry.index <= self.hard_state.commit
                && self.entries.get(&entry.index) != Some(entry)
            {
                return Err(LogError::Invalid(
                    "attempt to replace committed history".into(),
                ));
            }
        }
        if let Some(first) = write.entries.first() {
            if first.index > next.last_index() + 1 {
                return Err(LogError::Invalid("Raft append gap".into()));
            }
            // Even an identical committed prefix must not truncate committed
            // entries that the incoming shorter slice did not include.
            let replacement_end = write.entries.last().map_or(0, |entry| entry.index);
            if replacement_end < self.hard_state.commit {
                return Err(LogError::Invalid(
                    "append would truncate the committed suffix".into(),
                ));
            }
            next.entries.split_off(&first.index);
            next.entries.extend(
                write
                    .entries
                    .iter()
                    .map(|entry| (entry.index, entry.clone())),
            );
        }
        if let Some(hard) = &write.hard_state {
            if hard.term < self.hard_state.term
                || hard.commit < self.hard_state.commit
                || (hard.term == self.hard_state.term
                    && self.hard_state.vote != 0
                    && hard.vote != self.hard_state.vote)
            {
                return Err(LogError::Invalid("term, vote or commit regression".into()));
            }
            next.hard_state = hard.clone();
        }
        if let Some(applied) = &write.applied {
            if applied.position.index < next.applied.position.index
                || (next.applied.image.is_some() && applied.image.is_none())
                || (applied.position.index == next.applied.position.index
                    && applied != &next.applied)
            {
                return Err(LogError::Invalid("apply marker regression".into()));
            }
            next.applied = applied.clone();
        }
        if let Some(prepared) = &write.group_manifest {
            let marker = prepared.marker();
            if let Some(base) = &prepared.base {
                let fresh = self.group_manifest.is_none()
                    && base.root().cells() == 0
                    && base.position() == self.applied.position
                    && self.applied.position.index.get() == 1;
                if self.group_manifest.as_ref() != Some(marker)
                    && self.group_manifest.as_ref() != Some(base)
                    && !fresh
                {
                    return Err(LogError::Invalid(
                        "cell-group preparation base changed".into(),
                    ));
                }
            }
            marker
                .check()
                .map_err(|e| LogError::Invalid(e.to_string()))?;
            if !Arc::ptr_eq(&prepared.engine_owner, &write.group.owner)
                || !marker.matches_group(&history, write.group.birth_digest)
                || marker.position().index < self.applied.position.index
                || (marker.position().index == self.applied.position.index
                    && self.group_manifest.as_ref() != Some(marker))
                || (marker.position().index > self.applied.position.index
                    && self.applied.position.index.get().checked_add(1)
                        != Some(marker.position().index.get()))
                || self.applied.image.is_some()
            {
                return Err(LogError::Invalid(
                    "invalid cell-group apply binding or prefix".into(),
                ));
            }
            if let Some(previous) = &self.group_manifest {
                if marker.route() != previous.route() {
                    return Err(LogError::Invalid(
                        "group route changed outside topology protocol".into(),
                    ));
                }
            } else if self.applied.position.index.get() != 1
                || self.snapshot.get_metadata().index != 1
                || !self.snapshot.data.is_empty()
            {
                return Err(LogError::Invalid(
                    "cell-group state requires a fresh group".into(),
                ));
            }
            next.applied = DurableApply {
                position: marker.position(),
                image: None,
            };
            next.group_manifest = Some(marker.clone());
            next.manifest_pins = Some(prepared.images.clone());
        }
        if let Some(conf) = &write.conf_state {
            if next.applied.position.index <= self.applied.position.index
                && conf != &self.conf_state
            {
                return Err(LogError::Invalid(
                    "configuration change lacks applied progress".into(),
                ));
            }
            next.conf_state = conf.clone();
        }
        if let Some(membership) = &write.membership {
            if write.conf_state.is_none()
                && !matches!(&write.snapshot, Some(SnapshotChange::Install { .. }))
            {
                return Err(LogError::Invalid(
                    "member binding lacks configuration or install".into(),
                ));
            }
            membership.succeeds(&self.membership)?;
            next.membership = membership.clone();
        }
        if (next.conf_state != self.conf_state && write.membership.is_none())
            || (write.conf_state.is_some()
                && (write.membership.is_none()
                    || next.membership.revision() != next.applied.position.index))
        {
            return Err(LogError::Invalid(
                "configuration lacks exact member binding marker".into(),
            ));
        }
        if next.applied.position.index > self.applied.position.index
            && write.conf_state.is_none()
            && next
                .entries
                .range(self.applied.position.index.get() + 1..=next.applied.position.index.get())
                .any(|(_, entry)| entry.get_entry_type() != EntryType::EntryNormal)
        {
            return Err(LogError::Invalid(
                "applied configuration lacks ConfState".into(),
            ));
        }
        next.validate(max_entries)
            .map_err(|error| LogError::Invalid(error.to_string()))?;
        Ok(next)
    }
}

pub(crate) fn validate_conf(conf: &ConfState) -> Result<(), LogError> {
    let lists = [
        &conf.voters,
        &conf.voters_outgoing,
        &conf.learners,
        &conf.learners_next,
    ];
    if conf.voters.is_empty() || lists.iter().map(|list| list.len()).sum::<usize>() > 128 {
        return Err(LogError::Corrupt("invalid Raft configuration size".into()));
    }
    for list in lists {
        let mut unique = std::collections::BTreeSet::new();
        if list
            .iter()
            .any(|&member| member == 0 || !unique.insert(member))
        {
            return Err(LogError::Corrupt("duplicate or zero Raft member".into()));
        }
    }
    if conf
        .learners
        .iter()
        .any(|id| conf.voters.contains(id) || conf.voters_outgoing.contains(id))
        || conf.learners_next.iter().any(|id| {
            !conf.voters_outgoing.contains(id)
                || conf.voters.contains(id)
                || conf.learners.contains(id)
        })
        || (conf.voters_outgoing.is_empty() && (conf.auto_leave || !conf.learners_next.is_empty()))
    {
        return Err(LogError::Corrupt("invalid joint Raft configuration".into()));
    }
    Ok(())
}

pub(crate) fn genesis(conf_state: ConfState) -> CachedGroup {
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.mut_metadata().term = 1;
    snapshot.mut_metadata().set_conf_state(conf_state.clone());
    let hard_state = HardState {
        term: 1,
        commit: 1,
        ..Default::default()
    };
    CachedGroup {
        hard_state,
        membership: GroupMembership::initial(&conf_state),
        conf_state,
        snapshot,
        entries: BTreeMap::new(),
        group_manifest: None,
        manifest_pins: None,
        applied: DurableApply {
            position: LogPosition {
                index: LogIndex::new(1),
                term: Term::new(1),
            },
            image: None,
        },
    }
}
