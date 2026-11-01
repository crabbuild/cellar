// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Engine-local retained entry debt and owned reservations. Remote archive
//! observations never release this accounting; only selected storage state can.
use crate::raft::storage::LogError;
use ::raft::prelude::{Entry, EntryType};
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard,
};

// Includes the cached entry's fixed metadata and an outstanding lease/map node.
const ENTRY_OVERHEAD: usize = 512;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellGroupLogCapacity {
    pub bytes: usize,
    pub entries: usize,
}

impl CellGroupLogCapacity {
    fn add(self, other: Self) -> Result<Self, LogError> {
        Ok(Self {
            bytes: self
                .bytes
                .checked_add(other.bytes)
                .ok_or(LogError::Overloaded)?,
            entries: self
                .entries
                .checked_add(other.entries)
                .ok_or(LogError::Overloaded)?,
        })
    }

    fn sub(self, other: Self) -> Result<Self, LogError> {
        Ok(Self {
            bytes: self
                .bytes
                .checked_sub(other.bytes)
                .ok_or_else(accounting_error)?,
            entries: self
                .entries
                .checked_sub(other.entries)
                .ok_or_else(accounting_error)?,
        })
    }

    fn fits(self, limit: Self) -> bool {
        self.bytes <= limit.bytes && self.entries <= limit.entries
    }
}

/// Hard node/group entry admission, including separately reserved capacity for
/// Raft's empty election and automatic joint-leave entries. This bounds logical
/// retained entries and reservations, not physical engine file amplification.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupLogRetentionLimits {
    pub node: CellGroupLogCapacity,
    pub group: CellGroupLogCapacity,
    pub progress_node: CellGroupLogCapacity,
    pub progress_group: CellGroupLogCapacity,
}

impl Default for CellGroupLogRetentionLimits {
    fn default() -> Self {
        Self {
            node: CellGroupLogCapacity {
                bytes: 192 * 1024 * 1024,
                entries: 32_768,
            },
            group: CellGroupLogCapacity {
                bytes: 64 * 1024 * 1024,
                entries: 8192,
            },
            progress_node: CellGroupLogCapacity {
                bytes: 2 * 1024 * 1024,
                entries: 1024,
            },
            progress_group: CellGroupLogCapacity {
                bytes: 128 * 1024,
                entries: 64,
            },
        }
    }
}

impl CellGroupLogRetentionLimits {
    pub(crate) fn validate(self) -> Result<(), LogError> {
        if self.node.bytes > 2 * 1024 * 1024 * 1024
            || self.node.entries > 1_048_576
            || !self.group.fits(self.node)
            || !self.progress_group.fits(self.progress_node)
            || [
                self.node,
                self.group,
                self.progress_node,
                self.progress_group,
            ]
            .iter()
            .any(|x| x.bytes == 0 || x.entries == 0)
            || self.progress_node.bytes >= self.node.bytes
            || self.progress_node.entries >= self.node.entries
            || self.progress_group.bytes >= self.group.bytes
            || self.progress_group.entries >= self.group.entries
        {
            return Err(LogError::Invalid("cell-group log retention limits".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellGroupLogRetentionUsage {
    pub retained_bytes: usize,
    pub retained_entries: usize,
    pub reserved_bytes: usize,
    pub reserved_entries: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct Charge {
    data: CellGroupLogCapacity,
    progress: CellGroupLogCapacity,
}

impl Charge {
    fn entry(entry: &Entry) -> Result<Self, LogError> {
        let capacity = CellGroupLogCapacity {
            bytes: (entry.compute_size() as usize)
                .checked_add(ENTRY_OVERHEAD)
                .ok_or(LogError::Overloaded)?,
            entries: 1,
        };
        Ok(Self::new(capacity, progress_entry(entry)))
    }

    fn new(capacity: CellGroupLogCapacity, progress: bool) -> Self {
        if progress {
            Self {
                progress: capacity,
                ..Self::default()
            }
        } else {
            Self {
                data: capacity,
                ..Self::default()
            }
        }
    }

    fn add(self, other: Self) -> Result<Self, LogError> {
        Ok(Self {
            data: self.data.add(other.data)?,
            progress: self.progress.add(other.progress)?,
        })
    }

    fn sub(self, other: Self) -> Result<Self, LogError> {
        Ok(Self {
            data: self.data.sub(other.data)?,
            progress: self.progress.sub(other.progress)?,
        })
    }

    fn fits(
        self,
        limit: CellGroupLogCapacity,
        progress: CellGroupLogCapacity,
    ) -> Result<bool, LogError> {
        Ok(self.data.fits(limit.sub(progress)?)
            && self.progress.fits(progress)
            && self.data.add(self.progress)?.fits(limit))
    }

    fn total(self) -> Result<CellGroupLogCapacity, LogError> {
        self.data.add(self.progress)
    }
}

fn accounting_error() -> LogError {
    LogError::Corrupt("cell-group retained log accounting".into())
}

fn progress_entry(entry: &Entry) -> bool {
    entry.data.is_empty()
        && entry.context.is_empty()
        && matches!(
            entry.get_entry_type(),
            EntryType::EntryNormal | EntryType::EntryConfChangeV2
        )
}

fn binding(entry: &Entry) -> Result<[u8; 32], LogError> {
    Ok(Sha256::digest(
        entry
            .write_to_bytes()
            .map_err(|e| LogError::Invalid(e.to_string()))?,
    )
    .into())
}

fn retained<'a>(mut entries: impl Iterator<Item = &'a Entry>) -> Result<Charge, LogError> {
    entries.try_fold(Charge::default(), |sum, entry| {
        sum.add(Charge::entry(entry)?)
    })
}

#[derive(Default)]
struct Group {
    durable: Charge,
    usage: Charge,
}

struct Hold {
    group: u64,
    charge: Charge,
    binding: Option<[u8; 32]>,
}

#[derive(Default)]
struct State {
    groups: BTreeMap<u64, Group>,
    holds: BTreeMap<u64, Hold>,
    next: u64,
    durable: Charge,
    usage: Charge,
}

pub(crate) struct NodeLogRetention {
    state: Mutex<State>,
    limits: CellGroupLogRetentionLimits,
    closed: Arc<AtomicBool>,
}

pub(crate) struct GroupLogRetention {
    node: Arc<NodeLogRetention>,
    key: u64,
}

/// Clones share one charge. Storage transfers it into actual durable debt in
/// the same accounting turn; losing the waiter cannot refund entered work.
pub(crate) struct LogReservation {
    group: Arc<GroupLogRetention>,
    id: u64,
}

impl NodeLogRetention {
    pub(crate) fn new(limits: CellGroupLogRetentionLimits, closed: Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            limits,
            closed,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, LogError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LogError::Closed);
        }
        self.state.lock().map_err(|_| {
            self.closed.store(true, Ordering::Release);
            LogError::Closed
        })
    }

    /// Reconstruct debt from actual selected entries, including when reopening
    /// under a smaller ceiling. New reservations refuse until the debt fits.
    pub(crate) fn register<'a>(
        self: &Arc<Self>,
        key: u64,
        entries: impl Iterator<Item = &'a Entry>,
    ) -> Result<Arc<GroupLogRetention>, LogError> {
        let durable = retained(entries)?;
        let mut state = self.lock()?;
        if state.groups.contains_key(&key) {
            return Err(LogError::WrongIdentity);
        }
        let usage = state.usage.add(durable)?;
        let all_durable = state.durable.add(durable)?;
        state.groups.insert(
            key,
            Group {
                durable,
                usage: durable,
            },
        );
        state.usage = usage;
        state.durable = all_durable;
        Ok(Arc::new(GroupLogRetention {
            node: self.clone(),
            key,
        }))
    }

    pub(crate) fn usage(&self) -> Result<CellGroupLogRetentionUsage, LogError> {
        let state = self.lock()?;
        usage(state.durable, state.usage)
    }
}

fn usage(durable: Charge, all: Charge) -> Result<CellGroupLogRetentionUsage, LogError> {
    let retained = durable.total()?;
    let reserved = all.sub(durable)?.total()?;
    Ok(CellGroupLogRetentionUsage {
        retained_bytes: retained.bytes,
        retained_entries: retained.entries,
        reserved_bytes: reserved.bytes,
        reserved_entries: reserved.entries,
    })
}

impl GroupLogRetention {
    pub(crate) fn usage(&self) -> Result<CellGroupLogRetentionUsage, LogError> {
        let state = self.node.lock()?;
        let group = state.groups.get(&self.key).ok_or(LogError::WrongIdentity)?;
        usage(group.durable, group.usage)
    }

    pub(crate) fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        progress: bool,
    ) -> Result<Arc<LogReservation>, LogError> {
        if bytes < ENTRY_OVERHEAD {
            return Err(LogError::Invalid("entry reservation overhead".into()));
        }
        let charge = Charge::new(CellGroupLogCapacity { bytes, entries: 1 }, progress);
        let mut state = self.node.lock()?;
        let group = state.groups.get(&self.key).ok_or(LogError::WrongIdentity)?;
        let next_group = group.usage.add(charge)?;
        let next_node = state.usage.add(charge)?;
        let limits = self.node.limits;
        if !next_group.fits(limits.group, limits.progress_group)?
            || !next_node.fits(limits.node, limits.progress_node)?
        {
            return Err(LogError::Overloaded);
        }
        let id = state.next.checked_add(1).ok_or(LogError::Overloaded)?;
        state.next = id;
        state.holds.insert(
            id,
            Hold {
                group: self.key,
                charge,
                binding: None,
            },
        );
        state
            .groups
            .get_mut(&self.key)
            .ok_or(LogError::WrongIdentity)?
            .usage = next_group;
        state.usage = next_node;
        Ok(Arc::new(LogReservation {
            group: self.clone(),
            id,
        }))
    }

    pub(crate) fn reserve_execution(
        self: &Arc<Self>,
        envelope: crate::application::consensus::EnvelopeLimits,
    ) -> Result<Arc<LogReservation>, LogError> {
        let bytes = envelope
            .max_header_bytes
            .checked_add(envelope.max_payload_bytes)
            .and_then(|n| n.checked_add(1024))
            .ok_or(LogError::Overloaded)?;
        self.reserve(bytes, false)
    }

    pub(crate) fn reserve_entry(
        self: &Arc<Self>,
        entry: &Entry,
    ) -> Result<Arc<LogReservation>, LogError> {
        let lease = self.reserve(Charge::entry(entry)?.total()?.bytes, progress_entry(entry))?;
        lease.bind(entry)?;
        Ok(lease)
    }

    /// Call only after the exact cache/root state is durably selected. Every new
    /// entry must have an entered lease; accounting can only stay equal or fall.
    pub(crate) fn publish<'a>(
        self: &Arc<Self>,
        entries: impl Iterator<Item = &'a Entry>,
        leases: &[Arc<LogReservation>],
    ) -> Result<(), LogError> {
        let next = retained(entries)?;
        let mut state = self.node.lock()?;
        let old = state
            .groups
            .get(&self.key)
            .ok_or(LogError::WrongIdentity)?
            .durable;
        let mut consumed = Charge::default();
        let mut ids = BTreeSet::new();
        for lease in leases {
            if !Arc::ptr_eq(self, &lease.group) || !ids.insert(lease.id) {
                return Err(LogError::WrongIdentity);
            }
            let hold = state.holds.get(&lease.id).ok_or(LogError::WrongIdentity)?;
            if hold.group != self.key || hold.binding.is_none() {
                return Err(LogError::WrongIdentity);
            }
            consumed = consumed.add(hold.charge)?;
        }
        let admitted = old.add(consumed)?;
        if !next.data.fits(admitted.data) || !next.progress.fits(admitted.progress) {
            return Err(accounting_error());
        }
        let next_group = state
            .groups
            .get(&self.key)
            .ok_or(LogError::WrongIdentity)?
            .usage
            .sub(admitted)?
            .add(next)?;
        let next_node = state.usage.sub(admitted)?.add(next)?;
        let next_durable = state.durable.sub(old)?.add(next)?;
        for id in ids {
            state.holds.remove(&id);
        }
        state.groups.insert(
            self.key,
            Group {
                durable: next,
                usage: next_group,
            },
        );
        state.usage = next_node;
        state.durable = next_durable;
        Ok(())
    }
}

impl LogReservation {
    /// Shrink the pre-callback envelope to the actual encoded proposal, retaining
    /// enough space for its later Raft term/index and fixed entry bookkeeping.
    pub(crate) fn encoded(&self, bytes: usize) -> Result<(), LogError> {
        let charge = Charge::new(
            CellGroupLogCapacity {
                bytes: bytes
                    .checked_add(64 + ENTRY_OVERHEAD)
                    .ok_or(LogError::Overloaded)?,
                entries: 1,
            },
            false,
        );
        let mut state = self.group.node.lock()?;
        let hold = state.holds.get(&self.id).ok_or(LogError::WrongIdentity)?;
        if hold.binding.is_some()
            || !charge.data.fits(hold.charge.data)
            || !charge.progress.fits(hold.charge.progress)
        {
            return Err(LogError::WrongIdentity);
        }
        let released = hold.charge.sub(charge)?;
        let next_group = state
            .groups
            .get(&self.group.key)
            .ok_or(LogError::WrongIdentity)?
            .usage
            .sub(released)?;
        state
            .groups
            .get_mut(&self.group.key)
            .ok_or(LogError::WrongIdentity)?
            .usage = next_group;
        state.usage = state.usage.sub(released)?;
        state
            .holds
            .get_mut(&self.id)
            .ok_or(LogError::WrongIdentity)?
            .charge = charge;
        Ok(())
    }

    /// Only a sealed pre-append rejection or authoritative reconciliation may
    /// release a live lease independently of its remaining client clones.
    pub(crate) fn release(&self) -> Result<(), LogError> {
        let mut state = self.group.node.lock()?;
        if let Some(hold) = state.holds.remove(&self.id) {
            let group = state
                .groups
                .get_mut(&self.group.key)
                .ok_or(LogError::WrongIdentity)?;
            group.usage = group.usage.sub(hold.charge)?;
            state.usage = state.usage.sub(hold.charge)?;
        }
        Ok(())
    }

    pub(crate) fn bind(&self, entry: &Entry) -> Result<(), LogError> {
        let digest = binding(entry)?;
        let charge = Charge::entry(entry)?;
        let mut state = self.group.node.lock()?;
        let hold = state.holds.get(&self.id).ok_or(LogError::WrongIdentity)?;
        if hold.group != self.group.key
            || hold.binding.is_some_and(|old| old != digest)
            || !charge.data.fits(hold.charge.data)
            || !charge.progress.fits(hold.charge.progress)
        {
            return Err(LogError::WrongIdentity);
        }
        let released = hold.charge.sub(charge)?;
        let group = state
            .groups
            .get_mut(&self.group.key)
            .ok_or(LogError::WrongIdentity)?;
        group.usage = group.usage.sub(released)?;
        state.usage = state.usage.sub(released)?;
        let hold = state
            .holds
            .get_mut(&self.id)
            .ok_or(LogError::WrongIdentity)?;
        hold.charge = charge;
        hold.binding = Some(digest);
        Ok(())
    }

    pub(crate) fn validate(
        &self,
        group: &Arc<GroupLogRetention>,
        entry: &Entry,
    ) -> Result<(), LogError> {
        if !Arc::ptr_eq(&self.group, group) {
            return Err(LogError::WrongIdentity);
        }
        let digest = binding(entry)?;
        let state = self.group.node.lock()?;
        let hold = state.holds.get(&self.id).ok_or(LogError::WrongIdentity)?;
        if hold.group != group.key || hold.binding != Some(digest) {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }
}

impl Drop for LogReservation {
    fn drop(&mut self) {
        // Even a closed engine may release an unselected reservation. No new
        // admission is possible there; reopen reconstructs actual durable debt.
        if let Ok(mut state) = self.group.node.state.lock() {
            if let Some(hold) = state.holds.remove(&self.id) {
                let result = (|| {
                    let group = state
                        .groups
                        .get_mut(&self.group.key)
                        .ok_or(LogError::WrongIdentity)?;
                    group.usage = group.usage.sub(hold.charge)?;
                    state.usage = state.usage.sub(hold.charge)?;
                    Ok::<_, LogError>(())
                })();
                if result.is_err() {
                    self.group.node.closed.store(true, Ordering::Release);
                }
            }
        }
    }
}
