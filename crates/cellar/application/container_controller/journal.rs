// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::values::*;
use crate::application::containers::{BackendIdentity, ExecutionClaim};
use anyhow::{ensure, Context as _, Result};
use cellar_ltx::{FileSystem, HostFileLock};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

const MAGIC: &[u8] = b"CELLAR_EXECUTION_CONTROLLER_1\n";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Operation {
    pub request: ExecutionOperation,
    pub claim: ExecutionClaim,
    pub outcome: Option<ExecutionOutcome>,
    pub submitted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct History {
    pub high_water: Option<ExecutionClaim>,
    pub active: Option<ExecutionClaim>,
    pub revoked: bool,
    pub imported: bool,
    pub operations: BTreeMap<String, Operation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Recovery {
    pub from: String,
    pub to: String,
    pub cluster: [u8; 16],
    pub namespace: [u8; 16],
    pub cell: String,
}

impl Recovery {
    pub fn histories(
        &self,
    ) -> Result<(
        cellar_logic::cell_group::CellHistory,
        cellar_logic::cell_group::CellHistory,
    )> {
        use cellar_logic::{
            cell_group::{CellHistory, CellId},
            consensus::{ClusterId, IncarnationId, NamespaceId},
        };
        let old = CellHistory {
            cluster: ClusterId::new(self.cluster)?,
            namespace: NamespaceId::new(self.namespace)?,
            cell: CellId::new(&self.cell)?,
            incarnation: IncarnationId::new(
                hex::decode(&self.from)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid recovery source incarnation"))?,
            )?,
        };
        let mut successor = old.clone();
        successor.incarnation = IncarnationId::new(
            hex::decode(&self.to)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid recovery successor incarnation"))?,
        )?;
        Ok((old, successor))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Slot {
    pub key: String,
    pub current: String,
    pub recovery: Option<Recovery>,
    pub histories: BTreeMap<String, History>,
}

impl Slot {
    pub fn initial(claim: &ExecutionClaim) -> Self {
        let current = hex::encode(claim.history().incarnation.bytes());
        Self {
            key: logical_key(claim.history(), claim.binding()),
            current: current.clone(),
            recovery: None,
            histories: BTreeMap::from([(
                current,
                History {
                    high_water: None,
                    active: None,
                    revoked: false,
                    imported: false,
                    operations: BTreeMap::new(),
                },
            )]),
        }
    }

    pub fn pending(&self) -> Vec<ExecutionPendingOperation> {
        self.histories
            .values()
            .flat_map(|history| history.operations.values())
            .filter(|operation| operation.outcome.is_none())
            .map(|operation| ExecutionPendingOperation {
                claim: operation.claim.clone(),
                operation: operation.request.clone(),
            })
            .collect()
    }

    pub fn validate(&self, options: &ExecutionControllerOptions) -> Result<()> {
        ensure!(
            hex_digest(&self.key)
                && self.histories.contains_key(&self.current)
                && !self.histories.is_empty()
                && self.histories.len() <= options.limits.max_incarnations_per_cell,
            "invalid controller history inventory"
        );
        let mut operation_ids = BTreeSet::new();
        for (incarnation, history) in &self.histories {
            ensure!(
                incarnation.len() == 32
                    && incarnation.bytes().all(|b| b.is_ascii_hexdigit())
                    && history.operations.len() <= options.limits.max_operations_per_incarnation,
                "invalid controller history or operation count"
            );
            if incarnation != &self.current {
                ensure!(history.revoked, "unrevoked historical incarnation");
            }
            if let Some(claim) = &history.high_water {
                check_fence(claim, options)?;
                ensure!(
                    self.key == logical_key(claim.history(), claim.binding())
                        && *incarnation == hex::encode(claim.history().incarnation.bytes()),
                    "controller claim history differs"
                );
            }
            if let Some(active) = &history.active {
                let high = history
                    .high_water
                    .as_ref()
                    .context("active controller claim without high-water")?;
                ensure!(
                    active.sequence() <= high.sequence()
                        && (active.sequence() != high.sequence() || active == high),
                    "controller high-water regressed"
                );
                ensure!(
                    logical_key(active.history(), active.binding()) == self.key
                        && hex::encode(active.history().incarnation.bytes()) == *incarnation,
                    "active execution history differs"
                );
                check_fence(active, options)?;
            }
            for (id, operation) in &history.operations {
                ensure!(
                    *id == hex::encode(operation.request.id)
                        && operation.request.id != [0; 16]
                        && operation.request.expires_unix_ms > 0
                        && operation_ids.insert(operation.request.id),
                    "invalid controller operation identity"
                );
                check_claim(&operation.claim, options)?;
                ensure!(
                    logical_key(operation.claim.history(), operation.claim.binding()) == self.key
                        && hex::encode(operation.claim.history().incarnation.bytes())
                            == *incarnation,
                    "operation history differs"
                );
                if let Some(ExecutionOutcome::Completed(result)) = &operation.outcome {
                    ensure!(
                        operation.submitted,
                        "completed operation was never submitted"
                    );
                    ensure!(
                        result.len() <= options.limits.max_result_bytes,
                        "controller result bound"
                    );
                }
                ensure!(
                    history
                        .high_water
                        .as_ref()
                        .is_some_and(|high| operation.claim.sequence() <= high.sequence()),
                    "operation exceeds controller high-water"
                );
            }
        }
        if let Some(recovery) = &self.recovery {
            let (old, successor) = recovery.histories()?;
            ensure!(
                logical_key(&old, &options.backend.binding) == self.key
                    && logical_key(&successor, &options.backend.binding) == self.key,
                "recovery logical history differs"
            );
            ensure!(
                recovery.to == self.current
                    && recovery.from != recovery.to
                    && self
                        .histories
                        .get(&recovery.from)
                        .is_some_and(|old| old.revoked),
                "invalid controller recovery transition"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    controller_id: [u8; 16],
    backend: BackendIdentity,
    limits: ExecutionControllerLimits,
    recovery_hash: [u8; 32],
    cells: BTreeMap<String, Option<Slot>>,
}

pub(super) struct Journal {
    fs: Arc<dyn FileSystem>,
    pub options: ExecutionControllerOptions,
    index: Mutex<Index>,
    ownership: Mutex<Option<HostFileLock>>,
    poisoned: AtomicBool,
}

pub(super) fn logical_key(
    history: &cellar_logic::cell_group::CellHistory,
    binding: &str,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"cellar-controller-logical-binding-1");
    hash.update(history.cluster.bytes());
    hash.update(history.namespace.bytes());
    hash.update((history.cell.as_str().len() as u16).to_be_bytes());
    hash.update(history.cell.as_str().as_bytes());
    hash.update((binding.len() as u16).to_be_bytes());
    hash.update(binding.as_bytes());
    hex::encode(hash.finalize())
}

pub(super) fn check_claim(
    claim: &ExecutionClaim,
    options: &ExecutionControllerOptions,
) -> Result<()> {
    check_fence(claim, options)?;
    ensure!(
        !claim.checkpoint().record.is_retired(),
        ExecutionControllerFailure::RevokedHistory
    );
    Ok(())
}

/// Validate metadata for revocation/confirmation, never permission to execute.
/// Stored operations and ordinary active requests still use check_claim.
pub(super) fn check_fence(
    claim: &ExecutionClaim,
    options: &ExecutionControllerOptions,
) -> Result<()> {
    let backend = options.fenced_backend();
    ensure!(
        claim.binding() == backend.binding
            && claim.checkpoint().record.provider == backend.provider
            && claim.checkpoint().record.configuration_digest == backend.configuration_digest,
        ExecutionControllerFailure::ConflictingRequest
    );
    Ok(())
}

fn recovery_hash(id: [u8; 16], secret: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-controller-recovery-1");
    hash.update(id);
    hash.update(secret);
    hash.finalize().into()
}

impl Journal {
    pub fn create(
        options: ExecutionControllerOptions,
        fs: Arc<dyn FileSystem>,
        secret: [u8; 32],
    ) -> Result<Arc<Self>> {
        options.validate()?;
        ensure!(secret != [0; 32], "empty controller recovery credential");
        match fs.symlink_metadata(&options.directory) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => anyhow::bail!("controller directory already exists; use open, never reset"),
            Err(e) => return Err(e.into()),
        }
        cellar_ltx::host::create_dir_all_durable(&*fs, &options.directory)?;
        let ownership = fs.lock_file(&options.directory.join("controller.lock"))?;
        cellar_ltx::host::create_dir_all_durable(&*fs, &options.directory.join("cells"))?;
        let index = Index {
            controller_id: options.controller_id,
            backend: options.backend.clone(),
            limits: options.limits,
            recovery_hash: recovery_hash(options.controller_id, &secret),
            cells: BTreeMap::new(),
        };
        let result = Arc::new(Self {
            fs,
            options,
            index: Mutex::new(index.clone()),
            ownership: Mutex::new(Some(ownership)),
            poisoned: AtomicBool::new(false),
        });
        result.write(&result.options.directory.join("controller"), &index)?;
        Ok(result)
    }

    pub fn open(options: ExecutionControllerOptions, fs: Arc<dyn FileSystem>) -> Result<Arc<Self>> {
        options.validate()?;
        ensure!(
            fs.symlink_metadata(&options.directory)?.is_dir,
            "controller directory is not a directory"
        );
        let ownership = fs.lock_file(&options.directory.join("controller.lock"))?;
        let index: Index = read(
            &*fs,
            &options.directory.join("controller"),
            options.limits.max_record_bytes,
        )?;
        ensure!(
            index.controller_id == options.controller_id
                && index.backend == options.backend
                && index.limits == options.limits
                && index.cells.len() <= options.limits.max_cells,
            "controller identity/configuration/inventory differs"
        );
        let result = Arc::new(Self {
            fs,
            options,
            index: Mutex::new(index),
            ownership: Mutex::new(Some(ownership)),
            poisoned: AtomicBool::new(false),
        });
        {
            let mut index = result
                .index
                .lock()
                .map_err(|_| ExecutionControllerFailure::Unavailable)?;
            for (key, preparing) in &mut index.cells {
                ensure!(hex_digest(key), "invalid controller registry key");
                let path = result.path(key);
                if let Some(initial) = preparing {
                    initial.validate(&result.options)?;
                    ensure!(&initial.key == key, "controller registration key differs");
                    match read::<Slot>(&*result.fs, &path, result.options.limits.max_record_bytes) {
                        Ok(slot) => {
                            ensure!(slot == *initial, "prepared controller registration differs")
                        }
                        Err(e)
                            if e.downcast_ref::<std::io::Error>()
                                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                        {
                            result.write(&path, initial)?
                        }
                        Err(e) => return Err(e),
                    }
                    *preparing = None;
                } else {
                    let slot: Slot =
                        read(&*result.fs, &path, result.options.limits.max_record_bytes)?;
                    slot.validate(&result.options)?;
                    ensure!(&slot.key == key, "controller registered file key differs");
                }
            }
            let mut entries = 0usize;
            for entry in result
                .fs
                .read_dir_iter(&result.options.directory.join("cells"))?
            {
                let entry = entry?;
                entries = entries
                    .checked_add(1)
                    .context("controller inventory overflow")?;
                ensure!(
                    entries <= result.options.limits.max_cells * 2,
                    "controller inventory bound"
                );
                let name = entry
                    .path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .context("controller state filename")?;
                let key = name.strip_suffix(".next").unwrap_or(name);
                ensure!(
                    hex_digest(key) && index.cells.contains_key(key),
                    "unregistered controller state file"
                );
            }
            result.write(&result.options.directory.join("controller"), &*index)?;
        }
        Ok(result)
    }

    fn path(&self, key: &str) -> PathBuf {
        self.options.directory.join("cells").join(key)
    }

    pub fn check(&self) -> Result<()> {
        ensure!(
            !self.poisoned.load(Ordering::Acquire),
            ExecutionControllerFailure::Unavailable
        );
        Ok(())
    }

    pub fn release(&self) -> Result<()> {
        self.poisoned.store(true, Ordering::Release);
        self.ownership
            .lock()
            .map_err(|_| ExecutionControllerFailure::Unavailable)?
            .take();
        Ok(())
    }

    pub fn load(&self, key: &str) -> Result<Option<Slot>> {
        self.check()?;
        ensure!(hex_digest(key), "invalid controller key");
        let registered = self
            .index
            .lock()
            .map_err(|_| ExecutionControllerFailure::Unavailable)?
            .cells
            .contains_key(key);
        if !registered {
            return Ok(None);
        }
        let slot: Slot = read(
            &*self.fs,
            &self.path(key),
            self.options.limits.max_record_bytes,
        )?;
        slot.validate(&self.options)?;
        ensure!(slot.key == key, "controller key differs");
        Ok(Some(slot))
    }

    pub fn register(&self, slot: &Slot) -> Result<()> {
        self.check()?;
        slot.validate(&self.options)?;
        let mut index = self
            .index
            .lock()
            .map_err(|_| ExecutionControllerFailure::Unavailable)?;
        ensure!(
            !index.cells.contains_key(&slot.key),
            ExecutionControllerFailure::ConflictingRequest
        );
        ensure!(
            index.cells.len() < self.options.limits.max_cells,
            ExecutionControllerFailure::Capacity
        );
        // No provider entry before the registry reaches Ready. An interrupted
        // Preparing registration is the sole missing-state repair allowed.
        let mut preparing = index.clone();
        preparing.cells.insert(slot.key.clone(), Some(slot.clone()));
        self.write(&self.options.directory.join("controller"), &preparing)?;
        *index = preparing;
        self.write(&self.path(&slot.key), slot)?;
        index.cells.insert(slot.key.clone(), None);
        self.write(&self.options.directory.join("controller"), &*index)
    }

    pub fn save(&self, slot: &Slot) -> Result<()> {
        self.check()?;
        slot.validate(&self.options)?;
        self.check_capacity(slot)?;
        self.write(&self.path(&slot.key), slot)
    }

    pub fn check_capacity(&self, slot: &Slot) -> Result<()> {
        let body = serde_json::to_vec(slot)?;
        let pending = slot
            .histories
            .values()
            .map(|history| {
                history
                    .operations
                    .values()
                    .filter(|op| op.outcome.is_none())
                    .count()
            })
            .sum::<usize>();
        // Reserve worst-case JSON bytes for every unresolved result before
        // dispatch. Observations cannot consume another request's receipt space.
        let per_result = self
            .options
            .limits
            .max_result_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(128))
            .context("controller result reservation overflow")?;
        let charge = pending
            .checked_mul(per_result)
            .and_then(|n| n.checked_add(body.len()))
            .and_then(|n| n.checked_add(MAGIC.len() + 40))
            .context("controller record reservation overflow")?;
        ensure!(
            charge <= self.options.limits.max_record_bytes,
            ExecutionControllerFailure::Capacity
        );
        Ok(())
    }

    pub fn keys(&self) -> Result<Vec<String>> {
        self.check()?;
        Ok(self
            .index
            .lock()
            .map_err(|_| ExecutionControllerFailure::Unavailable)?
            .cells
            .keys()
            .cloned()
            .collect())
    }

    pub fn authorize_recovery(&self, secret: &[u8; 32]) -> Result<()> {
        self.check()?;
        let index = self
            .index
            .lock()
            .map_err(|_| ExecutionControllerFailure::Unavailable)?;
        ensure!(
            cellar_native::crypto::timing_safe_equal(
                &index.recovery_hash,
                &recovery_hash(self.options.controller_id, secret)
            ),
            ExecutionControllerFailure::RevokedHistory
        );
        Ok(())
    }

    fn write<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        self.check()?;
        let body = serde_json::to_vec(value)?;
        let mut bytes = Vec::with_capacity(body.len().saturating_add(72));
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(body.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&Sha256::digest(&body));
        bytes.extend_from_slice(&body);
        ensure!(
            bytes.len() <= self.options.limits.max_record_bytes,
            ExecutionControllerFailure::Capacity
        );
        let temporary = path.with_extension("next");
        let result = (|| {
            // The root lock and per-cell admission exclude competing writers.
            // Leftover unpublished data may be overwritten; it granted no ack.
            let mut file = self.fs.create(&temporary)?;
            file.write_all_window(&bytes)?;
            file.sync_all()?;
            self.fs.rename(&temporary, path)?;
            self.fs
                .sync_all(path.parent().context("controller record parent")?)?;
            Ok(())
        })();
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }
}

fn read<T: DeserializeOwned>(fs: &dyn FileSystem, path: &Path, limit: usize) -> Result<T> {
    ensure!(
        fs.symlink_metadata(path)?.is_file,
        "controller state is not a regular file"
    );
    let mut file = fs.open(path)?;
    let len = usize::try_from(file.file_len()?)?;
    let header = MAGIC.len() + 8 + 32;
    ensure!(
        (header..=limit).contains(&len),
        "controller state length bound"
    );
    let bytes = file.read_exact_at(0, len)?;
    ensure!(
        &bytes[..MAGIC.len()] == MAGIC,
        "controller state format differs"
    );
    let body_len = u64::from_be_bytes(bytes[MAGIC.len()..MAGIC.len() + 8].try_into()?);
    ensure!(
        body_len == (len - header) as u64,
        "controller state length differs"
    );
    let digest = &bytes[MAGIC.len() + 8..header];
    ensure!(
        cellar_native::crypto::timing_safe_equal(digest, &Sha256::digest(&bytes[header..])),
        "controller state checksum differs"
    );
    Ok(serde_json::from_slice(&bytes[header..])?)
}
