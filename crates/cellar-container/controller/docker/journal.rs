// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently owned engine fences and original physical-operation outcomes.
use super::DockerOptions;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::containers::{
    ExecutionClaim, ExecutionControllerFailure, ExecutionOperation, ExecutionOutcome,
};
use cellar_ltx::{FileSystem, HostFileLock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

const MAGIC: &[u8] = b"CELLAR_DOCKER_BACKEND_1\n";
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PhysicalOperation {
    pub claim: ExecutionClaim,
    pub operation: ExecutionOperation,
    pub outcome: Option<ExecutionOutcome>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Slot {
    pub high: ExecutionClaim,
    pub active: bool,
    pub transition: u64,
    pub operations: BTreeMap<String, PhysicalOperation>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub format: u8,
    pub configuration: [u8; 32],
    pub engine: String,
    pub slots: BTreeMap<String, Slot>,
    pub revoked: BTreeSet<String>,
    pub incarnations: BTreeMap<String, String>,
    pub recoveries: BTreeMap<String, String>,
}

pub(super) struct Journal {
    options: DockerOptions,
    fs: Arc<dyn FileSystem>,
    state: Mutex<State>,
    _lock: HostFileLock,
    poisoned: AtomicBool,
}

pub(super) fn key(claim: &ExecutionClaim) -> String {
    super::hex(&claim.resource_key())
}

pub(super) fn history(claim: &ExecutionClaim) -> Result<String> {
    history_id(claim.history())
}

pub(super) fn logical(
    history: &cellar::sdk::consensus::CellHistory,
    binding: &str,
) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-docker-logical-binding-1");
    hash.update(history.cluster.bytes());
    hash.update(history.namespace.bytes());
    hash.update((history.cell.as_str().len() as u64).to_be_bytes());
    hash.update(history.cell.as_str().as_bytes());
    hash.update((binding.len() as u64).to_be_bytes());
    hash.update(binding.as_bytes());
    Ok(super::hex(&hash.finalize()))
}

pub(super) fn history_id(history: &cellar::sdk::consensus::CellHistory) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-docker-history-1");
    hash.update(history.cluster.bytes());
    hash.update(history.namespace.bytes());
    hash.update(history.incarnation.bytes());
    hash.update((history.cell.as_str().len() as u64).to_be_bytes());
    hash.update(history.cell.as_str().as_bytes());
    Ok(super::hex(&hash.finalize()))
}

impl Journal {
    pub fn create(
        options: DockerOptions,
        fs: Arc<dyn FileSystem>,
        engine: String,
    ) -> Result<Arc<Self>> {
        match fs.symlink_metadata(&options.directory) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => anyhow::bail!("Docker backend directory exists; open its original state"),
            Err(e) => return Err(e.into()),
        }
        cellar_ltx::host::create_dir_all_durable(&*fs, &options.directory)?;
        let lock = fs.lock_file(&options.directory.join("backend.lock"))?;
        let state = State {
            format: 1,
            configuration: options.digest()?,
            engine,
            slots: BTreeMap::new(),
            revoked: BTreeSet::new(),
            incarnations: BTreeMap::new(),
            recoveries: BTreeMap::new(),
        };
        let journal = Arc::new(Self {
            options,
            fs,
            state: Mutex::new(state.clone()),
            _lock: lock,
            poisoned: AtomicBool::new(false),
        });
        journal.write(&state)?;
        Ok(journal)
    }

    pub fn open(
        options: DockerOptions,
        fs: Arc<dyn FileSystem>,
        engine: &str,
    ) -> Result<Arc<Self>> {
        ensure!(
            fs.symlink_metadata(&options.directory)?.is_dir,
            "Docker backend directory missing or linked"
        );
        let lock = fs.lock_file(&options.directory.join("backend.lock"))?;
        let state = read(&*fs, &options.directory.join("state"))?;
        ensure!(
            state.format == 1 && state.configuration == options.digest()? && state.engine == engine,
            "Docker backend identity, configuration or engine changed"
        );
        ensure!(
            state.slots.len() <= options.max_resources
                && state.revoked.len() <= options.max_resources,
            "Docker backend inventory exceeds bound"
        );
        ensure!(
            state.incarnations.len() <= options.max_resources
                && state.recoveries.len() <= options.max_resources,
            "Docker backend history inventory exceeds bound"
        );
        for (key, slot) in &state.slots {
            ensure!(
                *key == self::key(&slot.high) && slot.operations.len() <= options.max_operations,
                "invalid Docker backend fence inventory"
            );
            for (id, operation) in &slot.operations {
                ensure!(
                    *id == super::hex(&operation.operation.id)
                        && self::key(&operation.claim) == *key
                        && operation.claim.sequence() <= slot.high.sequence(),
                    "invalid Docker backend operation inventory"
                );
            }
        }
        let journal = Arc::new(Self {
            options,
            fs,
            state: Mutex::new(state),
            _lock: lock,
            poisoned: AtomicBool::new(false),
        });
        // Opening is never admission. The controller must reassert every exact
        // fence, joining old execution before this backend accepts an effect.
        journal.change(|state| {
            for slot in state.slots.values_mut() {
                slot.active = false;
                slot.transition = slot
                    .transition
                    .checked_add(1)
                    .context("Docker fence transition exhausted")?;
            }
            Ok(())
        })?;
        Ok(journal)
    }

    pub fn view<T>(&self, read: impl FnOnce(&State) -> Result<T>) -> Result<T> {
        ensure!(
            !self.poisoned.load(Ordering::Acquire),
            ExecutionControllerFailure::Unavailable
        );
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Docker backend state poisoned"))?;
        read(&state)
    }

    pub fn check(&self, claim: &ExecutionClaim) -> Result<()> {
        self.view(|state| {
            ensure!(
                !state.revoked.contains(&history(claim)?),
                ExecutionControllerFailure::RevokedHistory
            );
            let logical = logical(claim.history(), claim.binding())?;
            ensure!(
                state.incarnations.get(&logical) == Some(&history(claim)?),
                ExecutionControllerFailure::RevokedHistory
            );
            ensure!(
                !state.recoveries.contains_key(&logical),
                ExecutionControllerFailure::RecoveryPending
            );
            let slot = state
                .slots
                .get(&key(claim))
                .ok_or(ExecutionControllerFailure::StaleClaim)?;
            ensure!(slot.high == *claim, ExecutionControllerFailure::StaleClaim);
            ensure!(slot.active, ExecutionControllerFailure::RecoveryPending);
            Ok(())
        })
    }

    pub fn change<T>(&self, update: impl FnOnce(&mut State) -> Result<T>) -> Result<T> {
        ensure!(
            !self.poisoned.load(Ordering::Acquire),
            "Docker backend storage is unavailable"
        );
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Docker backend state poisoned"))?;
        let mut next = guard.clone();
        let result = update(&mut next)?;
        ensure!(
            next.slots.len() <= self.options.max_resources
                && next.revoked.len() <= self.options.max_resources
                && next.incarnations.len() <= self.options.max_resources
                && next.recoveries.len() <= self.options.max_resources
                && next
                    .slots
                    .values()
                    .all(|slot| slot.operations.len() <= self.options.max_operations),
            "Docker backend receipt capacity exhausted"
        );
        if let Err(error) = self.write(&next) {
            self.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        *guard = next;
        Ok(result)
    }

    fn write(&self, state: &State) -> Result<()> {
        let body = serde_json::to_vec(state)?;
        // Reserve the largest bounded encoded result before physical entry.
        // Never discover receipt-capacity exhaustion only after an effect.
        let pending = state
            .slots
            .values()
            .map(|slot| {
                slot.operations
                    .values()
                    .filter(|op| op.outcome.is_none())
                    .count()
            })
            .sum::<usize>();
        let reservation = pending
            .checked_mul((self.options.max_body_bytes * 2 + 4096) * 4 + 256)
            .and_then(|reserved| reserved.checked_add(body.len()))
            .context("Docker receipt reservation overflow")?;
        ensure!(
            reservation <= MAX_RECORD_BYTES,
            "Docker result receipt capacity exhausted before execution"
        );
        let mut bytes = Vec::with_capacity(MAGIC.len() + 32 + body.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&Sha256::digest(&body));
        bytes.extend_from_slice(&body);
        ensure!(
            bytes.len() <= MAX_RECORD_BYTES,
            "Docker backend record exceeds bound"
        );
        let temporary = self.options.directory.join("state.next");
        let mut file = self.fs.create(&temporary)?;
        file.write_all_window(&bytes)?;
        file.sync_all()?;
        self.fs
            .rename(&temporary, &self.options.directory.join("state"))?;
        self.fs.sync_all(&self.options.directory)?;
        Ok(())
    }
}

fn read(fs: &dyn FileSystem, path: &Path) -> Result<State> {
    ensure!(
        fs.symlink_metadata(path)?.is_file,
        "Docker state is not a regular file"
    );
    let mut file = fs.open(path)?;
    let len = usize::try_from(file.file_len()?)?;
    let offset = MAGIC.len() + 32;
    ensure!(
        (offset..=MAX_RECORD_BYTES).contains(&len),
        "Docker state length exceeds bound"
    );
    let bytes = file.read_exact_at(0, len)?;
    ensure!(
        &bytes[..MAGIC.len()] == MAGIC
            && bytes[MAGIC.len()..offset] == Sha256::digest(&bytes[offset..])[..],
        "Docker state checksum differs"
    );
    serde_json::from_slice(&bytes[offset..]).context("decode Docker backend state")
}
