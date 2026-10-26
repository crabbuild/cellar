// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Advisory cross-replica publication coordination. A claim saves duplicate
//! encoding/upload work; it grants no ownership, membership or durability proof.
//! Unchanged conditional versions age on each observer's monotonic clock.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_BYTES: usize = 4096;
const MAX_VERSION_BYTES: usize = 8192;

#[derive(Clone)]
pub(super) struct Coordination {
    owner: [u8; 32],
    quiet_ms: u64,
    min_image_bytes: u64,
    seen: Arc<Mutex<Option<Seen>>>,
    next_attempt: Arc<AtomicU64>,
}

struct Seen {
    version: UpdateVersion,
    at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    birth: [u8; 32],
    owner: [u8; 32],
    sequence: u64,
    attempt: u64,
    released: bool,
}

pub(super) struct Claim {
    record: Record,
    version: UpdateVersion,
}

enum Acquisition {
    Acquired(Claim),
    Deferred,
    Uncoordinated,
}

impl Coordination {
    pub fn new(owner: [u8; 32], quiet_ms: u64, min_image_bytes: u64) -> Result<Self, ArchiveError> {
        if owner == [0; 32]
            || !(100..=60_000).contains(&quiet_ms)
            || min_image_bytes > stream_snapshot::MAX_IMAGE_BYTES
        {
            return Err(ArchiveError::Invalid("archive publication coordination"));
        }
        Ok(Self {
            owner,
            quiet_ms,
            min_image_bytes,
            seen: Arc::new(Mutex::new(None)),
            next_attempt: Arc::new(AtomicU64::new(1)),
        })
    }

    fn stale(&self, version: &UpdateVersion, now: u64) -> bool {
        let Ok(mut seen) = self.seen.lock() else {
            // Poisoned advisory state cannot authorize any data operation.
            // Refusing the optimization leaves the ordinary publication path.
            return true;
        };
        match seen.as_ref() {
            Some(before)
                if before.version == *version
                    && now
                        .checked_sub(before.at)
                        .is_some_and(|age| age >= self.quiet_ms) =>
            {
                true
            }
            Some(before) if before.version == *version && now >= before.at => false,
            _ => {
                *seen = Some(Seen {
                    version: version.clone(),
                    at: now,
                });
                false
            }
        }
    }

    async fn acquire(&self, archive: &CellArchive) -> Acquisition {
        // The data head is validated separately. A failed/malformed advisory
        // record cannot strand an otherwise publishable committed image.
        let observed = match archive
            .read(&archive.key("publication"), MAX_BYTES, None)
            .await
        {
            Ok(observed) => observed,
            Err(_) => return Acquisition::Uncoordinated,
        };
        let (sequence, mode) = match observed {
            Some((bytes, version)) => {
                let Ok(record) = decode(&bytes, archive.birth) else {
                    return Acquisition::Uncoordinated;
                };
                if !valid_version(&version) {
                    return Acquisition::Uncoordinated;
                }
                let Some(now) = archive.io_domain.as_ref().and_then(|d| d.mono_ms().ok()) else {
                    return Acquisition::Uncoordinated;
                };
                if !record.released && !self.stale(&version, now) {
                    return Acquisition::Deferred;
                }
                let Some(sequence) = record.sequence.checked_add(1) else {
                    return Acquisition::Uncoordinated;
                };
                (sequence, PutMode::Update(version))
            }
            None => (1, PutMode::Create),
        };
        let Ok(attempt) =
            self.next_attempt
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |before| {
                    before.checked_add(1)
                })
        else {
            return Acquisition::Uncoordinated;
        };
        let record = Record {
            birth: archive.birth,
            owner: self.owner,
            sequence,
            attempt,
            released: false,
        };
        match write(archive, &record, mode).await {
            Ok(Some(version)) => Acquisition::Acquired(Claim { record, version }),
            Ok(None) => Acquisition::Deferred,
            Err(_) => Acquisition::Uncoordinated,
        }
    }

    async fn renew(&self, archive: &CellArchive, mut claim: Claim) -> Option<Claim> {
        claim.record.sequence = claim.record.sequence.checked_add(1)?;
        claim.version = write(archive, &claim.record, PutMode::Update(claim.version))
            .await
            .ok()??;
        Some(claim)
    }

    async fn release(&self, archive: &CellArchive, mut claim: Claim) {
        let Some(sequence) = claim.record.sequence.checked_add(1) else {
            return;
        };
        claim.record.sequence = sequence;
        claim.record.released = true;
        // Never delete a mutable claim unconditionally: an old owner must not
        // erase a replacement's claim after its own renewal became ambiguous.
        let _ = write(archive, &claim.record, PutMode::Update(claim.version)).await;
    }

    async fn run(
        &self,
        archive: &CellArchive,
        cut: CommittedArchiveCut,
        verifier: &ArchiveVerifier,
        claim: Claim,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        let activity = IoActivity::default();
        let mut active = archive.clone();
        active.activity = Some(activity.clone());
        let (done, mut stopped) = tokio::sync::oneshot::channel();
        let work = async {
            let result = active.publish_inner(cut, verifier).await;
            let _ = done.send(());
            result
        };
        let renewal = async {
            let mut claim = Some(claim);
            let mut before = 0;
            if let Some(domain) = archive.io_domain.as_ref() {
                while let Ok(now) = domain.mono_ms() {
                    let Ok(timer) = domain.sleep_until(now.saturating_add(self.quiet_ms / 3))
                    else {
                        break;
                    };
                    crate::asyncrt::select_biased! { "publication completion stops advisory renewal; the renewal is joined before returning the data proof";
                        _ = &mut stopped => break,
                        _ = timer => (),
                    }
                    let next = activity.sequence();
                    if next == before {
                        continue;
                    }
                    before = next;
                    let Some(current) = claim.take() else { break };
                    claim = self.renew(archive, current).await;
                    if claim.is_none() {
                        break;
                    }
                }
            }
            claim
        };
        // No detached heartbeat exists. Even after the data result is ready,
        // an entered renewal finishes before its bounded owner is released.
        let (result, claim) = futures_util::future::join(work, renewal).await;
        if let Some(claim) = claim {
            self.release(archive, claim).await;
        }
        result
    }
}

fn valid_version(version: &UpdateVersion) -> bool {
    (version.e_tag.is_some() || version.version.is_some())
        && version
            .e_tag
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(version.version.as_ref().map_or(0, String::len))
            <= MAX_VERSION_BYTES
}

fn decode(bytes: &[u8], birth: [u8; 32]) -> Result<Record, ArchiveError> {
    let record: Record = manifest::decode(5, bytes, MAX_BYTES)?;
    if record.birth != birth
        || record.owner == [0; 32]
        || record.sequence == 0
        || record.attempt == 0
    {
        return Err(ArchiveError::Corrupt("archive publication claim binding"));
    }
    Ok(record)
}

async fn write(
    archive: &CellArchive,
    record: &Record,
    mode: PutMode,
) -> Result<Option<UpdateVersion>, ArchiveError> {
    let desired = manifest::encode(5, record, MAX_BYTES)?;
    let path = archive.key("publication");
    let _outcome = archive
        .io(
            archive.deadline()?,
            archive
                .store
                .put_opts(&path, desired.clone().into(), PutOptions::from(mode)),
        )
        .await;
    // A lost reply supplies no acquisition/renewal proof. Reconcile the exact
    // desired record and retain only the observed conditional version.
    Ok(archive
        .read(&path, MAX_BYTES, None)
        .await?
        .and_then(|(bytes, version)| {
            (bytes == desired && valid_version(&version)).then_some(version)
        }))
}

impl CellArchive {
    /// Optional scheduling only. Missing/corrupt hints never grant or replace
    /// any physical data, head, membership, ownership or output proof.
    pub fn with_publication_coordination(
        mut self,
        owner: [u8; 32],
        quiet_ms: u64,
        min_image_bytes: u64,
    ) -> Result<Self, ArchiveError> {
        self.coordination = Some(Coordination::new(owner, quiet_ms, min_image_bytes)?);
        Ok(self)
    }

    /// Publish only after every object has been read back and checked. CAS
    /// failures (including lost success responses) are reconciled with exact
    /// head reads; bounded contention never falls back to unconditional writes.
    pub async fn publish(
        &self,
        cut: CommittedArchiveCut,
        verifier: &ArchiveVerifier,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        self.validate_cut(&cut)?;
        let Some(coordination) = &self.coordination else {
            return self.publish_inner(cut, verifier).await;
        };
        if cut
            .image
            .as_ref()
            .map_or(0, |image| image.pin().state().length)
            < coordination.min_image_bytes
        {
            return self.publish_inner(cut, verifier).await;
        }
        // Covering heads remain independently downloaded and physically
        // replayed by every replica. Coordination only avoids creating another
        // full snapshot while a competing publisher is making progress.
        if self
            .load_head_metadata()
            .await?
            .as_ref()
            .is_some_and(|(head, _, _)| head.position.index >= cut.position.index)
        {
            return self.publish_inner(cut, verifier).await;
        }
        match coordination.acquire(self).await {
            Acquisition::Acquired(claim) => {
                tracing::info!(event = "raft_archive_phase", phase = "publication_acquired", cell = %self.scope.cell, index = cut.position.index.get());
                coordination.run(self, cut, verifier, claim).await
            }
            Acquisition::Deferred => Err(ArchiveError::Unconfirmed),
            Acquisition::Uncoordinated => {
                tracing::info!(event = "raft_archive_phase", phase = "publication_uncoordinated", cell = %self.scope.cell, index = cut.position.index.get());
                self.publish_inner(cut, verifier).await
            }
        }
    }
}
