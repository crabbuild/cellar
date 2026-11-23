// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent pins and irreversible retirement. A delayed DELETE can never
//! target a new allocation, even when its logical owner and bytes are identical.

use super::*;
use object_store::{PutOptions, UpdateVersion};
use std::collections::BTreeSet;

const MAX_CONTROL_BYTES: usize = 64 * 1024;
const MAX_PINS: usize = 128;

// Reserved framework scope for command-to-archive ownership handoff.
const COMMAND_SCOPE: [u8; 32] = [1; 32];

#[cfg(test)]
#[path = "ownership_tests.rs"]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::cell_groups) struct PayloadRootOwner {
    pub scope: [u8; 32],
    pub identity: [u8; 32],
    pub generation: u64,
    pub confirmed: bool,
}

pub(super) struct Reader {
    store: CellGroupPayloadStore,
    reference: CellGroupPayloadReference,
    id: [u8; 16],
    permit: Arc<crate::raft::budget::Permit>,
    domain: crate::asyncrt::DomainToken,
    armed: bool,
}

impl Reader {
    pub(super) async fn acquire(
        store: &CellGroupPayloadStore,
        reference: &CellGroupPayloadReference,
        permit: Arc<crate::raft::budget::Permit>,
    ) -> Result<Self> {
        let mut id = [0; 16];
        crate::asyncrt::rng("cell-group-payload-reader").fill_bytes(&mut id);
        let mut reader = Self {
            store: store.clone(),
            reference: reference.clone(),
            id,
            permit,
            domain: crate::asyncrt::services()
                .domain_token()
                .ok_or(CellGroupPayloadError::Retired)?,
            armed: false,
        };
        // Construct the cleanup owner before entering an ambiguous CAS.
        store
            .change_ownership(reference, |state| {
                if state.retired || (!state.upload && state.roots.is_empty()) {
                    return Err(CellGroupPayloadError::Retired);
                }
                if state.readers.contains(&id) {
                    return Err(CellGroupPayloadError::Conflict);
                }
                if state.readers.len() == MAX_PINS && !state.readers.contains(&id) {
                    return Err(CellGroupPayloadError::Overloaded);
                }
                reader.armed = true;
                Ok(state.readers.insert(id))
            })
            .await?;
        Ok(reader)
    }

    pub(super) async fn release(mut self) -> Result<()> {
        self.store.release_reader(&self.reference, self.id).await?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let store = self.store.clone();
        let reference = self.reference.clone();
        let id = self.id;
        let permit = self.permit.clone();
        // The domain joins entered cleanup, including shutdown races. Reuse
        // the original admission, so cancellation cannot multiply work slots
        // or refund its buffers while the control request is still entered.
        let _ = self.domain.spawn_cleanup(async move {
            let _permit = permit;
            let outcome = crate::asyncrt::timeout(
                store.pool.limits.io_timeout,
                store.release_reader(&reference, id),
            )
            .await;
            if !matches!(outcome, Ok(Ok(()))) {
                tracing::warn!("payload reader cleanup remains protected by its durable pin");
            }
        });
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    format: u8,
    reference: CellGroupPayloadReference,
    revision: u64,
    upload: bool,
    retired: bool,
    collected: bool,
    roots: BTreeSet<PayloadRootOwner>,
    readers: BTreeSet<[u8; 16]>,
}

impl State {
    fn check(&self, expected: &CellGroupPayloadReference) -> Result<()> {
        if self.format != FORMAT
            || &self.reference != expected
            || self.revision == 0
            || self.reference.format != FORMAT
            || self.reference.owner == [0; 32]
            || self.reference.writer == [0; 16]
            || self.reference.allocation == [0; 16]
            || self.reference.length == 0
            || self.reference.length > MAX_BYTES as u64
            || self.roots.len() > MAX_PINS
            || self.readers.len() > MAX_PINS
            || self.roots.iter().any(|root| {
                root.identity == [0; 32]
                    || ((root.scope == [0; 32]) != (root.generation == 0))
                    || (root.scope != [0; 32]
                        && root.generation < 2
                        && !(root.scope == COMMAND_SCOPE
                            && root.generation == 1
                            && !root.confirmed))
                    || root.generation == u64::MAX
                    || (root.scope == [0; 32] && !root.confirmed)
            })
            || self.readers.contains(&[0; 16])
            || (self.retired && (self.upload || !self.roots.is_empty() || !self.readers.is_empty()))
            || (self.collected && !self.retired)
        {
            return Err(CellGroupPayloadError::Invalid("payload ownership"));
        }
        Ok(())
    }
}

/// Persist with the owning image, archive, transfer or recovery operation.
/// Release only after that owner has retired its users and joined entered work.
/// A serialized hold identifies an owner; it is not execution authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellGroupPayloadHold {
    reference: CellGroupPayloadReference,
    root: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
pub struct CellGroupPayloadCollectionLimits {
    /// Maximum complete ownership inventory. Exceeding this bound refuses the
    /// pass before deleting anything; listing order cannot starve later keys.
    pub max_scan_objects: usize,
    pub max_delete_objects: usize,
}

impl Default for CellGroupPayloadCollectionLimits {
    fn default() -> Self {
        Self {
            max_scan_objects: 256,
            max_delete_objects: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellGroupPayloadCollectionReport {
    pub scanned_objects: usize,
    pub protected_objects: usize,
    pub deleted_objects: usize,
    pub deleted_bytes: u64,
}

impl CellGroupPayloadStore {
    /// Bridge source-proven I/O and the next selected source archive. It is
    /// installed before remote readers can release their transient pins, and
    /// survives even if a different logical owner retires its current value.
    pub(crate) async fn protect_command(
        &self,
        history: &CellHistory,
        reference: &CellGroupPayloadReference,
        guard: [u8; 32],
        upload: bool,
        timeout: Duration,
    ) -> Result<()> {
        self.check(history, reference)?;
        if guard == [0; 32] {
            return Err(CellGroupPayloadError::Invalid("command payload guard"));
        }
        let _permit = self
            .pool
            .acquire(MAX_CONTROL_BYTES * 3)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let deadline = self.deadline(timeout)?;
        crate::asyncrt::timeout_at(deadline, async {
            if upload {
                self.reserve_upload(reference).await?;
            }
            self.change_ownership(reference, |state| {
                if state.retired || (!state.upload && state.roots.is_empty()) {
                    return Err(CellGroupPayloadError::Retired);
                }
                let root = PayloadRootOwner {
                    scope: COMMAND_SCOPE,
                    identity: guard,
                    generation: 1,
                    confirmed: false,
                };
                if state.roots.contains(&root) {
                    return Ok(false);
                }
                if state.roots.len() == MAX_PINS {
                    return Err(CellGroupPayloadError::Overloaded);
                }
                Ok(state.roots.insert(root))
            })
            .await
        })
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?
    }

    /// Called only after a selected source root independently pins this exact
    /// journal dependency. Losing a response keeps either or both owners safe.
    pub(in crate::raft::cell_groups) async fn adopt_command(
        &self,
        reference: &CellGroupPayloadReference,
        guard: [u8; 32],
        timeout: Duration,
    ) -> Result<()> {
        if guard == [0; 32] {
            return Ok(());
        }
        let _permit = self
            .pool
            .acquire(MAX_CONTROL_BYTES * 3)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let deadline = self.deadline(timeout)?;
        crate::asyncrt::timeout_at(
            deadline,
            self.change_ownership(reference, |state| {
                Ok(state.roots.remove(&PayloadRootOwner {
                    scope: COMMAND_SCOPE,
                    identity: guard,
                    generation: 1,
                    confirmed: false,
                }))
            }),
        )
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?
    }

    fn control_path(&self, reference: &CellGroupPayloadReference) -> Path {
        self.prefix
            .child("ownership")
            .child(hex::encode(reference.owner))
            .child(hex::encode(reference.writer))
            .child(hex::encode(reference.allocation))
    }

    async fn load_ownership(
        &self,
        reference: &CellGroupPayloadReference,
    ) -> Result<(State, UpdateVersion)> {
        let (state, version) = self
            .load_ownership_at(&self.control_path(reference))
            .await?;
        state.check(reference)?;
        Ok((state, version))
    }

    async fn load_ownership_at(&self, path: &Path) -> Result<(State, UpdateVersion)> {
        let response = self.store.get(path).await?;
        if response.meta.size > MAX_CONTROL_BYTES as u64
            || response.range.start != 0
            || response.range.end != response.meta.size
        {
            return Err(CellGroupPayloadError::Invalid("ownership response"));
        }
        let version = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        if version.e_tag.as_ref().is_some_and(String::is_empty)
            || version.version.as_ref().is_some_and(String::is_empty)
            || version.e_tag.as_ref().map_or(0, String::len)
                + version.version.as_ref().map_or(0, String::len)
                > 8192
            || (version.e_tag.is_none() && version.version.is_none())
        {
            return Err(CellGroupPayloadError::Invalid(
                "ownership conditional version",
            ));
        }
        let length = response.meta.size as usize;
        let mut body = Vec::with_capacity(length);
        let mut stream = response.into_stream();
        let mut chunks = 0;
        while let Some(chunk) = stream.try_next().await? {
            chunks += 1;
            if chunks > MAX_CHUNKS
                || body
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|n| n > MAX_CONTROL_BYTES)
            {
                return Err(CellGroupPayloadError::Invalid("ownership body bounds"));
            }
            body.extend_from_slice(&chunk);
        }
        let state: State = serde_json::from_slice(&body)
            .map_err(|_| CellGroupPayloadError::Invalid("ownership encoding"))?;
        if body.len() != length || self.control_path(&state.reference) != *path {
            return Err(CellGroupPayloadError::Invalid(
                "ownership key or body length",
            ));
        }
        state.check(&state.reference)?;
        Ok((state, version))
    }

    async fn write_ownership(&self, state: &State, mode: PutMode) -> Result<()> {
        let bytes = serde_json::to_vec(state)
            .map_err(|_| CellGroupPayloadError::Invalid("ownership encoding"))?;
        if bytes.len() > MAX_CONTROL_BYTES {
            return Err(CellGroupPayloadError::Overloaded);
        }
        self.store
            .put_opts(
                &self.control_path(&state.reference),
                bytes.into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn change_ownership(
        &self,
        reference: &CellGroupPayloadReference,
        mut change: impl FnMut(&mut State) -> Result<bool>,
    ) -> Result<()> {
        for _ in 0..16 {
            let (mut state, version) = self.load_ownership(reference).await?;
            if !change(&mut state)? {
                return Ok(());
            }
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or(CellGroupPayloadError::Overloaded)?;
            state.check(reference)?;
            match self.write_ownership(&state, PutMode::Update(version)).await {
                Ok(()) => return Ok(()),
                Err(CellGroupPayloadError::Storage(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                )) => continue,
                // Lost CAS replies remain ambiguous. Reconciliation of the same
                // identity is safe, but an error never authorizes data deletion.
                Err(error) => return Err(error),
            }
        }
        Err(CellGroupPayloadError::Conflict)
    }

    pub(super) async fn reserve_upload(&self, reference: &CellGroupPayloadReference) -> Result<()> {
        let initial = State {
            format: FORMAT,
            reference: reference.clone(),
            revision: 1,
            upload: true,
            retired: false,
            collected: false,
            roots: BTreeSet::new(),
            readers: BTreeSet::new(),
        };
        let outcome = self.write_ownership(&initial, PutMode::Create).await;
        let (state, _) = self
            .load_ownership(reference)
            .await
            .map_err(|_| CellGroupPayloadError::Unconfirmed)?;
        if state.retired {
            return Err(CellGroupPayloadError::Retired);
        }
        // Exact readback reconciles even a lost create reply. Retrying a live
        // allocation protects its new potential reference commit as well.
        let _ = outcome;
        self.change_ownership(reference, |state| {
            if state.retired || (!state.upload && state.roots.is_empty()) {
                return Err(CellGroupPayloadError::Retired);
            }
            let changed = !state.upload;
            state.upload = true;
            Ok(changed)
        })
        .await
    }

    pub(super) async fn release_reader(
        &self,
        reference: &CellGroupPayloadReference,
        reader: [u8; 16],
    ) -> Result<()> {
        self.change_ownership(reference, |state| Ok(state.readers.remove(&reader)))
            .await
    }

    /// Atomically replace pending upload ownership with a durable root pin.
    /// Persist the exact hold before making its owner visible. Use a distinct
    /// identity for every retained image, recovery cut and transfer owner.
    pub async fn retain(
        &self,
        published: &PublishedCellGroupPayload,
        root: [u8; 32],
        timeout: Duration,
    ) -> Result<CellGroupPayloadHold> {
        if root == [0; 32] {
            return Err(CellGroupPayloadError::Invalid("payload root identity"));
        }
        self.pin(
            &published.0,
            PayloadRootOwner {
                scope: [0; 32],
                identity: root,
                generation: 0,
                confirmed: true,
            },
            true,
            timeout,
        )
        .await?;
        Ok(CellGroupPayloadHold {
            reference: published.0.clone(),
            root,
        })
    }

    async fn pin(
        &self,
        reference: &CellGroupPayloadReference,
        root: PayloadRootOwner,
        adopt: bool,
        timeout: Duration,
    ) -> Result<()> {
        let _permit = self
            .pool
            .acquire(MAX_CONTROL_BYTES * 3)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let deadline = self.deadline(timeout)?;
        crate::asyncrt::timeout_at(
            deadline,
            self.change_ownership(reference, |state| {
                if state.retired || (!state.upload && state.roots.is_empty()) {
                    return Err(CellGroupPayloadError::Retired);
                }
                let prior = state
                    .roots
                    .iter()
                    .find(|prior| {
                        prior.scope == root.scope
                            && prior.identity == root.identity
                            && prior.generation == root.generation
                    })
                    .cloned();
                if state.roots.len() == MAX_PINS && prior.is_none() {
                    return Err(CellGroupPayloadError::Overloaded);
                }
                let mut merged = root.clone();
                if let Some(prior) = &prior {
                    merged.confirmed |= prior.confirmed;
                }
                let changed = (adopt && state.upload) || prior.as_ref() != Some(&merged);
                if let Some(prior) = prior {
                    state.roots.remove(&prior);
                }
                state.roots.insert(merged);
                if adopt {
                    state.upload = false;
                }
                Ok(changed)
            }),
        )
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)??;
        Ok(())
    }

    pub(in crate::raft::cell_groups) async fn pin_archive(
        &self,
        history: &CellHistory,
        reference: &CellGroupPayloadReference,
        owner: PayloadRootOwner,
        adopt: bool,
        timeout: Duration,
    ) -> Result<()> {
        self.check(history, reference)?;
        if owner.scope == [0; 32]
            || owner.identity == [0; 32]
            || owner.generation < 2
            || owner.generation == u64::MAX
        {
            return Err(CellGroupPayloadError::Invalid("archive payload root"));
        }
        self.pin(reference, owner, adopt, timeout).await
    }

    pub(in crate::raft::cell_groups) async fn retire_archive_roots(
        &self,
        scope: [u8; 32],
        watermark: u64,
        pending: &BTreeSet<u64>,
        protected: &BTreeSet<(u64, [u8; 32])>,
        max_objects: usize,
        timeout: Duration,
    ) -> Result<()> {
        if !(1..=65_536).contains(&max_objects) {
            return Err(CellGroupPayloadError::Invalid(
                "payload collection inventory limit",
            ));
        }
        let _permit = self
            .pool
            .acquire(MAX_CONTROL_BYTES * 3 + max_objects * 512)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let deadline = self.deadline(timeout)?;
        crate::asyncrt::timeout_at(deadline, async {
            let prefix = self.prefix.child("ownership");
            let mut objects = self.store.list(Some(&prefix));
            // Complete bounded inventory; a partial scan cannot retire roots.
            let mut references = Vec::new();
            while let Some(object) = objects.try_next().await? {
                if references.len() == max_objects {
                    return Err(CellGroupPayloadError::Overloaded);
                }
                let (state, _) = self.load_ownership_at(&object.location).await?;
                references.push(state.reference);
            }
            for reference in references {
                self.change_ownership(&reference, |state| {
                    let before = state.roots.len();
                    state.roots.retain(|root| {
                        root.generation < 2
                            || root.scope != scope
                            || root.generation > watermark
                            || pending.contains(&root.generation)
                            || protected.contains(&(root.generation, root.identity))
                    });
                    Ok(before != state.roots.len())
                })
                .await?;
            }
            Ok(())
        })
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?
    }

    /// Release one exact durable owner after its independently authorized
    /// retirement. Dropping a hold or missing an acknowledgement is insufficient.
    pub async fn release(&self, hold: &CellGroupPayloadHold, timeout: Duration) -> Result<()> {
        let _permit = self
            .pool
            .acquire(MAX_CONTROL_BYTES * 3)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let deadline = self.deadline(timeout)?;
        crate::asyncrt::timeout_at(
            deadline,
            self.change_ownership(&hold.reference, |state| {
                Ok(state.roots.remove(&PayloadRootOwner {
                    scope: [0; 32],
                    identity: hold.root,
                    generation: 0,
                    confirmed: true,
                }))
            }),
        )
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?
    }

    /// Retire a known, unreferenced upload only after its publisher and any
    /// potential reference commits have been joined or permanently fenced.
    pub async fn abort_upload(
        &self,
        published: &PublishedCellGroupPayload,
        timeout: Duration,
    ) -> Result<()> {
        let _permit = self
            .pool
            .acquire(MAX_CONTROL_BYTES * 3)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let deadline = self.deadline(timeout)?;
        crate::asyncrt::timeout_at(
            deadline,
            self.change_ownership(&published.0, |state| {
                let changed = state.upload;
                state.upload = false;
                Ok(changed)
            }),
        )
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?
    }

    /// Bounded collection against persistent upload, root and reader pins.
    /// Mark retirement by CAS before DELETE; no later acquire or publish can
    /// reuse that allocation. Unknown owners never expire by wall-clock time.
    pub async fn collect(
        &self,
        limits: CellGroupPayloadCollectionLimits,
        timeout: Duration,
    ) -> Result<CellGroupPayloadCollectionReport> {
        if !(1..=65_536).contains(&limits.max_scan_objects)
            || limits.max_delete_objects > limits.max_scan_objects
        {
            return Err(CellGroupPayloadError::Invalid("payload collection limits"));
        }
        let workspace = CellGroupPayloadPool::workspace(self.pool.limits.max_payload_bytes)?
            .checked_add(limits.max_scan_objects * 512)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let _permit = self
            .pool
            .acquire(workspace)
            .ok_or(CellGroupPayloadError::Overloaded)?;
        let deadline = self.deadline(timeout)?;
        crate::asyncrt::timeout_at(deadline, async {
            let prefix = self.prefix.child("ownership");
            let mut objects = self.store.list(Some(&prefix));
            let mut report = CellGroupPayloadCollectionReport::default();
            let mut candidates = Vec::new();
            // Validate the complete bounded inventory before deleting anything.
            // ObjectStore listing order is unspecified; a truncated first page
            // would repeatedly visit tombstones and silently starve later keys.
            while let Some(object) = objects.try_next().await? {
                if report.scanned_objects == limits.max_scan_objects {
                    return Err(CellGroupPayloadError::Overloaded);
                }
                report.scanned_objects += 1;
                if object.size > MAX_CONTROL_BYTES as u64 {
                    return Err(CellGroupPayloadError::Invalid("ownership object length"));
                }
                let (candidate, _) = self.load_ownership_at(&object.location).await?;
                if candidate.upload || !candidate.roots.is_empty() || !candidate.readers.is_empty()
                {
                    if candidate.roots.iter().any(|root| root.confirmed) {
                        if candidate.reference.length > self.pool.limits.max_payload_bytes as u64 {
                            return Err(CellGroupPayloadError::Invalid("collector payload limit"));
                        }
                        self.read(&candidate.reference, deadline)
                            .await?
                            .ok_or(CellGroupPayloadError::Missing)?;
                    }
                    report.protected_objects += 1;
                    continue;
                }
                candidates.push(candidate.reference);
            }
            for reference in candidates {
                if report.deleted_objects == limits.max_delete_objects {
                    break;
                }
                let retired = self
                    .change_ownership(&reference, |state| {
                        if state.upload || !state.roots.is_empty() || !state.readers.is_empty() {
                            return Ok(false);
                        }
                        let changed = !state.retired;
                        state.retired = true;
                        Ok(changed)
                    })
                    .await;
                retired?;
                let (state, _) = self.load_ownership(&reference).await?;
                if !state.retired {
                    report.protected_objects += 1;
                    continue;
                }
                let bytes = match self.store.head(&self.path(&reference)).await {
                    Ok(object) => Some(object.size),
                    Err(object_store::Error::NotFound { .. }) => None,
                    Err(error) => return Err(error.into()),
                };
                match self.store.delete(&self.path(&reference)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => (),
                    Err(error) => return Err(error.into()),
                }
                self.change_ownership(&reference, |state| {
                    if !state.retired {
                        return Err(CellGroupPayloadError::Conflict);
                    }
                    let changed = !state.collected;
                    state.collected = true;
                    Ok(changed)
                })
                .await?;
                if let Some(bytes) = bytes {
                    report.deleted_objects += 1;
                    report.deleted_bytes = report
                        .deleted_bytes
                        .checked_add(bytes)
                        .ok_or(CellGroupPayloadError::Overloaded)?;
                }
            }
            Ok(report)
        })
        .await
        .map_err(|_| CellGroupPayloadError::DeadlineExceeded)?
    }
}
