// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One atomically replaced, fsynced record under an exclusive node-disk lock.
use super::*;
use cellar_ltx::{FileSystem, HostFileLock};
use serde::{Deserialize, Serialize};

pub(super) const LIMIT: usize = 32 * 1024;
const SEED_DOMAIN: &str = "cellar-cell-group-seed-archive-journal-1";

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Identity {
    domain: String,
    engine: [u8; 16],
    creation: CellGroupArchiveCreation,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Pending {
    #[default]
    None,
    Acquire(CellGroupArchiveIntent),
    Reserve(CellGroupArchiveIntent),
    Upload(CellGroupArchiveUploadTicket),
    Publish {
        ticket: CellGroupArchiveUploadTicket,
        intent: CellGroupArchiveIntent,
    },
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub head: Option<CellGroupArchiveHold>,
    pub retire: Option<CellGroupArchiveHold>,
    pub pending: Pending,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    identity: Identity,
    record: Record,
}

pub(super) struct Journal {
    filesystem: Arc<dyn FileSystem>,
    directory: std::path::PathBuf,
    identity: Identity,
    _lock: HostFileLock,
}

impl Journal {
    pub(super) fn open(
        filesystem: Arc<dyn FileSystem>,
        directory: std::path::PathBuf,
        engine: [u8; 16],
        creation: CellGroupArchiveCreation,
    ) -> Result<Self> {
        Self::open_domain(
            filesystem,
            directory,
            engine,
            creation,
            "cellar-cell-group-archive-journal-1",
            true,
        )
    }

    pub(super) fn open_seed(
        filesystem: Arc<dyn FileSystem>,
        directory: std::path::PathBuf,
        engine: [u8; 16],
        creation: CellGroupArchiveCreation,
    ) -> Result<Self> {
        Self::open_domain(filesystem, directory, engine, creation, SEED_DOMAIN, true)
    }

    pub(super) fn open_seed_existing(
        filesystem: Arc<dyn FileSystem>,
        directory: std::path::PathBuf,
        engine: [u8; 16],
        creation: CellGroupArchiveCreation,
    ) -> Result<Self> {
        Self::open_domain(filesystem, directory, engine, creation, SEED_DOMAIN, false)
    }

    pub(super) fn inspect_seed(
        filesystem: &dyn FileSystem,
        directory: &std::path::Path,
        engine: [u8; 16],
    ) -> Result<CellGroupArchiveCreation> {
        let bytes = read(filesystem, directory, "identity")?
            .ok_or(Error::Invalid("seed journal identity missing"))?;
        let identity: Identity = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Invalid("seed journal identity encoding"))?;
        if identity.domain != SEED_DOMAIN
            || identity.engine != engine
            || encode(&identity)? != bytes
        {
            return Err(Error::Invalid("seed journal identity binding"));
        }
        Ok(identity.creation)
    }

    fn open_domain(
        filesystem: Arc<dyn FileSystem>,
        directory: std::path::PathBuf,
        engine: [u8; 16],
        creation: CellGroupArchiveCreation,
        purpose: &str,
        initialize: bool,
    ) -> Result<Self> {
        if initialize {
            cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &directory)?;
        } else if !filesystem.symlink_metadata(&directory)?.is_dir {
            return Err(Error::Invalid("seed journal directory type"));
        }
        let lock = filesystem.lock_file(&directory.join("owner.lock"))?;
        let identity = Identity {
            domain: purpose.into(),
            engine,
            creation,
        };
        let journal = Self {
            filesystem,
            directory,
            identity,
            _lock: lock,
        };
        let guard = journal.read("identity")?;
        let record = journal.read("state")?;
        match (guard, record) {
            (None, None) if initialize => {
                let bytes = encode(&journal.identity)?;
                let mut file = journal
                    .filesystem
                    .create_new(&journal.directory.join("identity"))?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                journal.filesystem.sync_all(&journal.directory)?;
                // Only this fresh, confirmed exclusive creation initializes a
                // journal. An interrupted initializer is never reconstructed.
                journal.store(&Record::default())?;
            }
            (Some(bytes), Some(_)) if bytes == encode(&journal.identity)? => {
                journal.load()?;
            }
            _ => {
                return Err(Error::Invalid(
                    "archive journal identity/state lost or changed",
                ))
            }
        }
        Ok(journal)
    }

    fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        read(self.filesystem.as_ref(), &self.directory, name)
    }

    pub(super) fn load(&self) -> Result<Record> {
        if self.read("identity")?.as_deref() != Some(encode(&self.identity)?.as_slice()) {
            return Err(Error::Invalid("archive journal identity changed"));
        }
        let bytes = self
            .read("state")?
            .ok_or(Error::Invalid("archive journal state lost"))?;
        let envelope: Envelope = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Invalid("archive journal encoding"))?;
        if envelope.identity != self.identity || encode(&envelope)? != bytes {
            return Err(Error::Invalid("archive journal binding/canonical bytes"));
        }
        Ok(envelope.record)
    }

    pub(super) fn store(&self, record: &Record) -> Result<()> {
        let bytes = encode(&Envelope {
            identity: self.identity.clone(),
            record: record.clone(),
        })?;
        let temporary = self.directory.join("next");
        // Only the locked owner removes an unselected interrupted temporary.
        match self.filesystem.remove_file(&temporary) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }
        let mut file = self.filesystem.create_new(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        self.filesystem
            .rename(&temporary, &self.directory.join("state"))?;
        self.filesystem.sync_all(&self.directory)?;
        if self.read("state")?.as_deref() != Some(bytes.as_slice()) {
            return Err(Error::Invalid("archive journal write readback"));
        }
        Ok(())
    }
}

pub(super) fn seed_name(history: GroupHistory) -> String {
    use sha2::Digest;
    let mut hash = sha2::Sha256::new();
    for bytes in [
        history.cluster.bytes(),
        history.namespace.bytes(),
        history.group.bytes(),
        history.incarnation.bytes(),
    ] {
        hash.update(bytes);
    }
    hex::encode(hash.finalize())
}

fn read(
    filesystem: &dyn FileSystem,
    directory: &std::path::Path,
    name: &str,
) -> Result<Option<Vec<u8>>> {
    let path = directory.join(name);
    let metadata = match filesystem.symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file || metadata.len == 0 || metadata.len > LIMIT as u64 {
        return Err(Error::Invalid("archive journal file bounds/type"));
    }
    let mut file = filesystem.open(&path)?;
    let length = file.file_len()?;
    if length != metadata.len {
        return Err(Error::Invalid("archive journal changed during read"));
    }
    Ok(Some(file.read_exact_at(0, length as usize)?))
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| Error::Invalid("archive journal encoding"))?;
    if bytes.len() > LIMIT {
        return Err(Error::Invalid("archive journal record limit"));
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
