// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node-owned prepared body. Async handles contain metadata and a mailbox;
//! the registered physical owner alone reads, replaces and closes its file.

use super::{marker_state, CommittedImage, GroupLog, SnapshotError, SnapshotLimits};
use crate::raft::{
    budget::{Budget, Permit},
    performance::{Counter, Registry},
};
use cellar_logic::consensus::ReplicaIdentity;
use cellar_ltx::stream_snapshot::{Metadata, SnapshotFile};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc,
};

#[cfg(test)]
#[path = "snapshot_prepared_tests.rs"]
mod tests;

const METADATA_BYTES: usize = 64 * 1024;
const MAX_LEASES: usize = 2;

#[derive(Clone)]
pub(super) struct Key {
    owner: Arc<()>,
    replica: ReplicaIdentity,
    birth: [u8; 32],
    schema: [u8; 32],
    image: CommittedImage,
    limits: SnapshotLimits,
}

impl Key {
    pub(super) fn new(
        group: &GroupLog,
        schema: [u8; 32],
        image: CommittedImage,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        if std::mem::size_of::<Self>() + group.identity().scope.cell.len() + image.name.len()
            > METADATA_BYTES
        {
            return Err(SnapshotError::Limit);
        }
        Ok(Self {
            owner: group.owner.clone(),
            replica: group.identity().clone(),
            birth: group.birth_digest,
            schema,
            image,
            limits,
        })
    }

    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owner, &other.owner)
            && self.replica == other.replica
            && self.birth == other.birth
            && self.schema == other.schema
            && self.image == other.image
    }

    fn validate(&self, metadata: Metadata) -> Result<(), SnapshotError> {
        self.limits.validate()?;
        if metadata.image != marker_state(&self.image)?
            || metadata.image.length > self.limits.max_image_bytes
            || metadata.encoded_length > self.limits.max_encoded_bytes as u64
        {
            return Err(SnapshotError::Invalid("prepared payload identity"));
        }
        Ok(())
    }
}

type Builder = Box<dyn FnOnce() -> Result<SnapshotFile, SnapshotError> + Send>;

enum Request {
    Prepare {
        key: Box<Key>,
        build: Builder,
        operation: Permit,
        reply: mpsc::SyncSender<Result<Prepared, SnapshotError>>,
    },
    Read {
        id: u64,
        offset: usize,
        length: usize,
        operation: Permit,
        reply: mpsc::SyncSender<Result<Vec<u8>, SnapshotError>>,
    },
    Trim {
        operation: Permit,
        reply: mpsc::SyncSender<bool>,
    },
    Invalidate(u64),
    Stop,
}

#[derive(Clone)]
pub(crate) struct Cache {
    requests: mpsc::Sender<Request>,
    operations: Arc<Budget>,
    metadata: Arc<Budget>,
    leases: Arc<Budget>,
    cached: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

struct Lease {
    live: AtomicUsize,
    invalid: AtomicBool,
}

pub(crate) struct Shared {
    cache: Cache,
    id: u64,
    metadata: Metadata,
    lease: Arc<Lease>,
    _metadata: Permit,
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Dropping a handle touches metadata only. The physical owner keeps
        // the file and is responsible for disposing of queued builders.
        self.lease.live.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Shared {
    pub(super) fn metadata(&self) -> Metadata {
        self.metadata
    }

    pub(super) fn chunk(&self, offset: usize, length: usize) -> Result<Vec<u8>, SnapshotError> {
        if self.lease.invalid.load(Ordering::Acquire)
            || length == 0
            || length > 1024 * 1024
            || offset
                .checked_add(length)
                .is_none_or(|end| end as u64 > self.metadata.encoded_length)
        {
            return Err(SnapshotError::Invalid("prepared payload read"));
        }
        let operation = self.cache.operation()?;
        let (reply, result) = mpsc::sync_channel(1);
        self.cache
            .requests
            .send(Request::Read {
                id: self.id,
                offset,
                length,
                operation,
                reply,
            })
            .map_err(|_| closed())?;
        result.recv().map_err(|_| closed())?
    }

    pub(super) fn invalidate(&self) {
        // At most one invalidation can be queued per admitted body. It must
        // wake the owner even when its ordinary operation slot is occupied.
        if !self.lease.invalid.swap(true, Ordering::AcqRel) {
            let _ = self.cache.requests.send(Request::Invalidate(self.id));
        }
    }
}

pub(crate) struct Prepared {
    pub(super) payload: Shared,
    pub(crate) reused: bool,
}

// Close the physical file before returning its metadata charge. Its own
// SnapshotFile storage closes before refunding shared scratch admission too.
struct Entry {
    file: SnapshotFile,
    key: Key,
    id: u64,
    lease: Arc<Lease>,
    _metadata: Permit,
}

pub(crate) struct Owner {
    cache: Cache,
    requests: mpsc::Receiver<Request>,
    entry: Option<Entry>,
    sequence: u64,
    performance: Arc<Registry>,
}

fn closed() -> SnapshotError {
    SnapshotError::Invalid("prepared payload owner closed")
}

pub(crate) fn channel(performance: Arc<Registry>) -> (Cache, Owner) {
    let (requests, receiver) = mpsc::channel();
    let cache = Cache {
        requests,
        operations: Budget::new(METADATA_BYTES, 1),
        metadata: Budget::new(METADATA_BYTES, 1),
        leases: Budget::new(MAX_LEASES * METADATA_BYTES, MAX_LEASES),
        cached: Arc::new(AtomicBool::new(false)),
        stopped: Arc::new(AtomicBool::new(false)),
    };
    let owner = Owner {
        cache: cache.clone(),
        requests: receiver,
        entry: None,
        sequence: 0,
        performance,
    };
    (cache, owner)
}

impl Cache {
    fn operation(&self) -> Result<Permit, SnapshotError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(closed());
        }
        self.operations
            .acquire(METADATA_BYTES)
            .ok_or(SnapshotError::Limit)
    }

    /// Called on the admitted source's blocking job. That job retains its
    /// codec/work reservations through this reply, even if its waiter cancels.
    pub(super) fn prepare(
        &self,
        key: Key,
        build: impl FnOnce() -> Result<SnapshotFile, SnapshotError> + Send + 'static,
    ) -> Result<Prepared, SnapshotError> {
        let operation = self.operation()?;
        let (reply, result) = mpsc::sync_channel(1);
        self.requests
            .send(Request::Prepare {
                key: Box::new(key),
                build: Box::new(build),
                operation,
                reply,
            })
            .map_err(|_| closed())?;
        result.recv().map_err(|_| closed())?
    }

    /// Recovery may reclaim an idle prepared file before receiving a body.
    /// Busy preparation/reads are never serialized behind a trim request.
    pub(crate) fn trim_idle(&self) -> Result<bool, SnapshotError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(closed());
        }
        if !self.cached.load(Ordering::Acquire) {
            return Ok(false);
        }
        let Some(operation) = self.operations.acquire(METADATA_BYTES) else {
            return Ok(false);
        };
        let (reply, result) = mpsc::sync_channel(1);
        self.requests
            .send(Request::Trim { operation, reply })
            .map_err(|_| closed())?;
        result.recv().map_err(|_| closed())
    }

    pub(crate) fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            let _ = self.requests.send(Request::Stop);
        }
    }
}

impl Owner {
    fn discard(&mut self) {
        self.entry.take();
        self.cache.cached.store(false, Ordering::Release);
    }

    pub(crate) fn run(mut self) {
        while let Ok(request) = self.requests.recv() {
            if self.cache.stopped.load(Ordering::Acquire) {
                break;
            }
            match request {
                Request::Prepare {
                    key,
                    build,
                    operation,
                    reply,
                } => {
                    let result = self.prepare(*key, build);
                    // All request-owned input is consumed. Returned handles
                    // carry their separate bounded metadata lease.
                    drop(operation);
                    let _ = reply.send(result);
                }
                Request::Read {
                    id,
                    offset,
                    length,
                    operation,
                    reply,
                } => {
                    let result = self.read(id, offset, length);
                    drop(operation);
                    let _ = reply.send(result);
                }
                Request::Trim { operation, reply } => {
                    let idle = self
                        .entry
                        .as_ref()
                        .is_some_and(|e| e.lease.live.load(Ordering::Acquire) == 0);
                    if idle {
                        self.discard();
                    }
                    drop(operation);
                    let _ = reply.send(idle);
                }
                Request::Invalidate(id) => {
                    if self.entry.as_ref().is_some_and(|e| e.id == id) {
                        self.discard();
                    }
                }
                Request::Stop => break,
            }
        }
        self.discard();
        self.cache.stopped.store(true, Ordering::Release);
        // Pending builders/pins and reply channels also drop on this owner.
    }

    fn prepare(&mut self, key: Key, build: Builder) -> Result<Prepared, SnapshotError> {
        if self
            .entry
            .as_ref()
            .is_some_and(|e| e.lease.invalid.load(Ordering::Acquire))
        {
            self.discard();
        }
        let reused = self.entry.as_ref().is_some_and(|e| e.key.matches(&key));
        if let Some(entry) = self.entry.as_ref().filter(|_| reused) {
            key.validate(entry.file.metadata())?;
        }
        if !reused {
            if self
                .entry
                .as_ref()
                .is_some_and(|e| e.lease.live.load(Ordering::Acquire) != 0)
            {
                return Err(SnapshotError::Limit);
            }
            self.discard();
            let metadata = self
                .cache
                .metadata
                .acquire(METADATA_BYTES)
                .ok_or(SnapshotError::Limit)?;
            self.performance
                .increment(Counter::SnapshotPreparedMisses, 1);
            let file = build()?;
            key.validate(file.metadata())?;
            if self.cache.stopped.load(Ordering::Acquire) {
                return Err(closed());
            }
            self.sequence = self.sequence.checked_add(1).ok_or(SnapshotError::Limit)?;
            self.entry = Some(Entry {
                file,
                key,
                id: self.sequence,
                lease: Arc::new(Lease {
                    live: AtomicUsize::new(0),
                    invalid: AtomicBool::new(false),
                }),
                _metadata: metadata,
            });
            self.cache.cached.store(true, Ordering::Release);
        }
        let entry = self.entry.as_ref().ok_or_else(closed)?;
        let permit = self
            .cache
            .leases
            .acquire(METADATA_BYTES)
            .ok_or(SnapshotError::Limit)?;
        entry.lease.live.fetch_add(1, Ordering::AcqRel);
        let metadata = entry.file.metadata();
        if reused {
            self.performance.increment(Counter::SnapshotPreparedHits, 1);
            self.performance.increment(
                Counter::SnapshotPreparedReusedBytes,
                metadata.encoded_length,
            );
        }
        Ok(Prepared {
            payload: Shared {
                cache: self.cache.clone(),
                id: entry.id,
                metadata,
                lease: entry.lease.clone(),
                _metadata: permit,
            },
            reused,
        })
    }

    fn read(&mut self, id: u64, offset: usize, length: usize) -> Result<Vec<u8>, SnapshotError> {
        let entry = self
            .entry
            .as_ref()
            .filter(|e| e.id == id && !e.lease.invalid.load(Ordering::Acquire))
            .ok_or_else(closed)?;
        let mut bytes = vec![0; length];
        let result = entry
            .file
            .read_into(offset as u64, &mut bytes)
            .map_err(SnapshotError::from);
        if let Err(error) = result {
            entry.lease.invalid.store(true, Ordering::Release);
            self.discard();
            return Err(error);
        }
        Ok(bytes)
    }
}
