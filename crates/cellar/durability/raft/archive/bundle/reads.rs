//! Share overlapping physical reads, never completed archive material or proofs.
use super::{CellArchive, Reference, MAX_GROUPS};
use crate::raft::archive::{ArchiveError, READ_WINDOW};
use crate::raft::performance::Counter;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

type Key = ([u8; 32], u64);
type Payload = Arc<Vec<u8>>;
type Waiter = oneshot::Sender<Option<Payload>>;

#[derive(Clone, Default)]
pub(super) struct Reads {
    pending: Arc<Mutex<BTreeMap<Key, Vec<Waiter>>>>,
}

enum Admission {
    Owner(Owner),
    Joined(oneshot::Receiver<Option<Payload>>),
}

struct Owner {
    reads: Reads,
    key: Option<Key>,
}

impl Reads {
    fn admit(&self, reference: &Reference) -> Result<Admission, ArchiveError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ArchiveError::Invalid("archive read owner is poisoned"))?;
        for waiters in pending.values_mut() {
            waiters.retain(|waiter| !waiter.is_closed());
        }
        let readers = pending.len() + pending.values().map(Vec::len).sum::<usize>();
        // Existing per-member download windows bound payload memory. Also
        // enforce that same bound on owners/waiters inside the shared context.
        if readers >= MAX_GROUPS * READ_WINDOW {
            return Err(ArchiveError::Limit);
        }
        let key = (reference.digest, reference.length);
        if let Some(waiters) = pending.get_mut(&key) {
            let (send, receive) = oneshot::channel();
            waiters.push(send);
            Ok(Admission::Joined(receive))
        } else {
            pending.insert(key, Vec::new());
            Ok(Admission::Owner(Owner {
                reads: self.clone(),
                key: Some(key),
            }))
        }
    }

    pub(super) async fn read(
        &self,
        archive: &CellArchive,
        reference: &Reference,
    ) -> Result<Payload, ArchiveError> {
        match self.admit(reference)? {
            Admission::Owner(mut owner) => {
                if let Some(performance) = &archive.bundle_read_performance {
                    performance.increment(Counter::ArchiveBundleReadOwner, 1);
                }
                let result = archive.read_bundle(reference).await;
                // Evict before waking peers: a later reader must observe the
                // store again, even while an earlier extraction is running.
                owner.complete(result.as_ref().ok().cloned());
                result
            }
            Admission::Joined(receive) => {
                if let Some(performance) = &archive.bundle_read_performance {
                    performance.increment(Counter::ArchiveBundleReadJoined, 1);
                }
                let deadline = archive.deadline()?;
                let bytes = archive
                    .io(deadline, async { Ok(receive.await.ok().flatten()) })
                    .await?;
                // A previously unpolled waiter may have both a delivered body
                // and an expired timer ready. A result-first I/O select alone
                // must not authorize extraction after this caller's deadline.
                let now = archive
                    .io_domain
                    .as_ref()
                    .ok_or(ArchiveError::Stopped)?
                    .mono_ms()
                    .map_err(|_| ArchiveError::Stopped)?;
                if now >= deadline {
                    return Err(ArchiveError::Timeout);
                }
                bytes.ok_or(ArchiveError::Unconfirmed)
            }
        }
    }
}

impl Owner {
    fn complete(&mut self, bytes: Option<Payload>) {
        let Some(key) = self.key.take() else {
            return;
        };
        let waiters = {
            // Cleanup must wake observers even if a panic poisoned admission.
            // Admission itself refuses a poisoned owner instead of reusing it.
            let mut pending = match self.reads.pending.lock() {
                Ok(pending) => pending,
                Err(poisoned) => poisoned.into_inner(),
            };
            pending.remove(&key).unwrap_or_default()
        };
        for waiter in waiters {
            let _ = waiter.send(bytes.clone());
        }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        // No task is detached. Cancelling the owner drops its physical read and
        // refuses joined readers; it cannot strand them or grant a proof.
        self.complete(None);
    }
}
