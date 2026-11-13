// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable external values retained independently of physical group archives.
//! Publication confirms bytes, never a Raft mutation or permission to collect.

use crate::raft::budget::{Budget, RetainedBytes};
use cellar_logic::cell_group::CellHistory;
use cellar_logic::consensus::{ClusterId, NamespaceId};
use futures_util::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

const FORMAT: u8 = 1;
const MAX_BYTES: usize = cellar_logic::kv::MAX_VALUE_BYTES;
const MAX_CHUNKS: usize = 16_384;

#[derive(Debug)]
pub enum CellGroupPayloadError {
    Invalid(&'static str),
    Overloaded,
    DeadlineExceeded,
    Missing,
    Unconfirmed,
    Storage(object_store::Error),
}

impl std::fmt::Display for CellGroupPayloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cell-group payload: {self:?}")
    }
}

impl std::error::Error for CellGroupPayloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<object_store::Error> for CellGroupPayloadError {
    fn from(error: object_store::Error) -> Self {
        Self::Storage(error)
    }
}

type Result<T> = std::result::Result<T, CellGroupPayloadError>;

/// A locator, not publication, recovery, execution or deletion authority.
/// The logical owner remains stable across group movement and disaster recovery.
/// A new incarnation writes a separate immutable subtree and may read old rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellGroupPayloadReference {
    format: u8,
    owner: [u8; 32],
    writer: [u8; 16],
    digest: [u8; 32],
    length: u64,
}

fn owner(history: &CellHistory) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-cell-payload-owner-1\0");
    hash.update(history.cluster.bytes());
    hash.update(history.namespace.bytes());
    hash.update((history.cell.as_str().len() as u64).to_be_bytes());
    hash.update(history.cell.as_str());
    hash.finalize().into()
}

impl CellGroupPayloadReference {
    pub(crate) fn prepare(history: &CellHistory, digest: [u8; 32], length: u64) -> Self {
        Self {
            format: FORMAT,
            owner: owner(history),
            writer: history.incarnation.bytes(),
            digest,
            length,
        }
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    fn check(&self, history: &CellHistory, limit: usize) -> Result<()> {
        if self.format != FORMAT
            || self.owner != owner(history)
            || self.writer == [0; 16]
            || self.length == 0
            || self.length > limit as u64
        {
            return Err(CellGroupPayloadError::Invalid(
                "reference binding or length",
            ));
        }
        Ok(())
    }

    fn verify(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 != self.length
            || <[u8; 32]>::from(Sha256::digest(bytes)) != self.digest
        {
            return Err(CellGroupPayloadError::Invalid("object digest or length"));
        }
        Ok(())
    }
}

/// Exact bytes were read back from the immutable namespace-owned object key.
/// The caller must still commit the reference through its fenced Raft turn.
pub struct PublishedCellGroupPayload(CellGroupPayloadReference);

impl PublishedCellGroupPayload {
    pub fn reference(&self) -> &CellGroupPayloadReference {
        &self.0
    }
}

/// Returned bytes continue charging the node pool after the I/O slot is freed.
pub struct CellGroupPayloadBytes {
    bytes: bytes::Bytes,
    _retained: RetainedBytes,
}

impl AsRef<[u8]> for CellGroupPayloadBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CellGroupPayloadLimits {
    pub max_operations: usize,
    pub max_retained_bytes: usize,
    pub max_payload_bytes: usize,
    /// Complete publication or read, including every body chunk and read-back.
    pub io_timeout: Duration,
}

impl Default for CellGroupPayloadLimits {
    fn default() -> Self {
        Self {
            max_operations: 2,
            max_retained_bytes: 104 * 1024 * 1024,
            max_payload_bytes: MAX_BYTES,
            io_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellGroupPayloadUsage {
    pub operations: usize,
    pub retained_bytes: usize,
}

/// Share one pool across all namespaces on a node. Binding a store is lazy and
/// allocates no value buffer. Exhaustion refuses admission without queueing it.
#[derive(Clone)]
pub struct CellGroupPayloadPool {
    limits: CellGroupPayloadLimits,
    budget: Arc<Budget>,
}

impl CellGroupPayloadPool {
    pub fn new(limits: CellGroupPayloadLimits) -> Result<Self> {
        if !(1..=4096).contains(&limits.max_operations)
            || !(1..=MAX_BYTES).contains(&limits.max_payload_bytes)
            || limits.io_timeout.is_zero()
            || limits.io_timeout > Duration::from_secs(60)
            || limits.max_retained_bytes < Self::workspace(limits.max_payload_bytes)?
        {
            return Err(CellGroupPayloadError::Invalid("pool limits"));
        }
        Ok(Self {
            limits,
            budget: Budget::new(limits.max_retained_bytes, limits.max_operations),
        })
    }

    fn workspace(length: usize) -> Result<usize> {
        length
            .checked_mul(3)
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or(CellGroupPayloadError::Overloaded)
    }

    pub fn usage(&self) -> CellGroupPayloadUsage {
        let (retained_bytes, operations) = self.budget.usage();
        CellGroupPayloadUsage {
            operations,
            retained_bytes,
        }
    }

    pub(crate) fn bind(
        &self,
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        cluster: ClusterId,
        namespace: NamespaceId,
    ) -> CellGroupPayloadStore {
        CellGroupPayloadStore {
            store,
            prefix: prefix.child("cell-payloads-1"),
            cluster,
            namespace,
            pool: self.clone(),
        }
    }
}

/// Immutable namespace payloads. There is deliberately no listing or deletion
/// operation: live rows alone cannot enumerate retained checkpoints, readers,
/// interrupted publications or disaster-recovery cuts. Keep all objects until
/// a separate complete root/pin collector can prove retirement.
#[derive(Clone)]
pub struct CellGroupPayloadStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    cluster: ClusterId,
    namespace: NamespaceId,
    pool: CellGroupPayloadPool,
}

impl CellGroupPayloadStore {
    fn deadline(&self, timeout: Duration) -> Result<u64> {
        let duration = timeout.min(self.pool.limits.io_timeout);
        let millis = u64::try_from(duration.as_millis())
            .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?;
        if millis == 0 {
            return Err(CellGroupPayloadError::DeadlineExceeded);
        }
        crate::asyncrt::mono_ms()
            .checked_add(millis)
            .ok_or(CellGroupPayloadError::DeadlineExceeded)
    }

    fn check_deadline(deadline: u64) -> Result<()> {
        if crate::asyncrt::mono_ms() >= deadline {
            return Err(CellGroupPayloadError::DeadlineExceeded);
        }
        Ok(())
    }

    fn check(&self, history: &CellHistory, reference: &CellGroupPayloadReference) -> Result<()> {
        if history.cluster != self.cluster || history.namespace != self.namespace {
            return Err(CellGroupPayloadError::Invalid("namespace"));
        }
        reference.check(history, self.pool.limits.max_payload_bytes)
    }

    fn path(&self, reference: &CellGroupPayloadReference) -> Path {
        self.prefix
            .child(hex::encode(reference.owner))
            .child(hex::encode(reference.writer))
            .child(format!(
                "{}-{}",
                reference.length,
                hex::encode(reference.digest)
            ))
    }

    // Call only under the operation's single deadline and precharged workspace.
    async fn read(
        &self,
        reference: &CellGroupPayloadReference,
        deadline: u64,
    ) -> Result<Option<bytes::Bytes>> {
        Self::check_deadline(deadline)?;
        let outcome = self.store.get(&self.path(reference)).await;
        Self::check_deadline(deadline)?;
        let response = match outcome {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if response.meta.size != reference.length
            || response.range.start != 0
            || response.range.end != reference.length
        {
            return Err(CellGroupPayloadError::Invalid(
                "object response length or range",
            ));
        }
        let mut bytes = Vec::with_capacity(reference.length as usize);
        let mut stream = response.into_stream();
        let mut chunks = 0;
        while let Some(chunk) = stream.try_next().await? {
            Self::check_deadline(deadline)?;
            chunks += 1;
            if chunks > MAX_CHUNKS
                || bytes
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|n| n > reference.length as usize)
            {
                return Err(CellGroupPayloadError::Invalid("object body bounds"));
            }
            bytes.extend_from_slice(&chunk);
        }
        reference.verify(&bytes)?;
        Self::check_deadline(deadline)?;
        Ok(Some(bytes.into()))
    }

    /// Publish immutable bytes and confirm the entire object. An ambiguous PUT
    /// is successful only if the exact bounded GET proves the expected bytes.
    /// Never overwrite an existing corrupt object or convert its error to a miss.
    pub async fn publish(
        &self,
        history: &CellHistory,
        bytes: bytes::Bytes,
        timeout: Duration,
    ) -> Result<PublishedCellGroupPayload> {
        let deadline = self.deadline(timeout)?;
        if bytes.is_empty() || bytes.len() > self.pool.limits.max_payload_bytes {
            return Err(CellGroupPayloadError::Invalid("payload length"));
        }
        if history.cluster != self.cluster || history.namespace != self.namespace {
            return Err(CellGroupPayloadError::Invalid("namespace"));
        }
        // Reject saturation before spending CPU hashing another maximum-size
        // value. The same reservation survives the full publication protocol.
        let _permit = self
            .pool
            .budget
            .acquire(CellGroupPayloadPool::workspace(bytes.len())?)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let reference = CellGroupPayloadReference::prepare(
            history,
            Sha256::digest(&bytes).into(),
            bytes.len() as u64,
        );
        self.check(history, &reference)?;
        Self::check_deadline(deadline)?;
        crate::asyncrt::timeout_at(deadline, async {
            if self.read(&reference, deadline).await?.is_some() {
                return Ok(PublishedCellGroupPayload(reference));
            }
            let outcome = self
                .store
                .put_opts(&self.path(&reference), bytes.into(), PutMode::Create.into())
                .await;
            match self.read(&reference, deadline).await? {
                Some(_) => Ok(PublishedCellGroupPayload(reference)),
                None => match outcome {
                    Err(error) => Err(error.into()),
                    Ok(_) => Err(CellGroupPayloadError::Unconfirmed),
                },
            }
        })
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?
    }

    /// Read under the current logical cell identity. A fresh incarnation may
    /// read references restored by a separately verified recovery procedure;
    /// this read itself neither verifies that procedure nor admits a cell.
    pub async fn get(
        &self,
        history: &CellHistory,
        reference: &CellGroupPayloadReference,
        timeout: Duration,
    ) -> Result<CellGroupPayloadBytes> {
        let deadline = self.deadline(timeout)?;
        self.check(history, reference)?;
        let permit = self
            .pool
            .budget
            .acquire(CellGroupPayloadPool::workspace(reference.length as usize)?)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let bytes = crate::asyncrt::timeout_at(deadline, self.read(reference, deadline))
            .await
            .map_err(|_| CellGroupPayloadError::DeadlineExceeded)??
            .ok_or(CellGroupPayloadError::Missing)?;
        let retained = permit
            .retain_bytes(bytes.len())
            .ok_or(CellGroupPayloadError::Overloaded)?;
        Ok(CellGroupPayloadBytes {
            bytes,
            _retained: retained,
        })
    }
}
