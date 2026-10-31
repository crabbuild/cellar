// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One atomically replaced, fsynced record under an exclusive node-disk lock.
use super::*;
use cellar_ltx::{FileSystem, HostFileLock};
use serde::{Deserialize, Serialize};

const LIMIT: usize = 32 * 1024;

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
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &directory)?;
        let lock = filesystem.lock_file(&directory.join("owner.lock"))?;
        let identity = Identity {
            domain: "cellar-cell-group-archive-journal-1".into(),
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
            (None, None) => {
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
        let path = self.directory.join(name);
        let metadata = match self.filesystem.symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file || metadata.len == 0 || metadata.len > LIMIT as u64 {
            return Err(Error::Invalid("archive journal file bounds/type"));
        }
        let mut file = self.filesystem.open(&path)?;
        let length = file.file_len()?;
        if length != metadata.len {
            return Err(Error::Invalid("archive journal changed during read"));
        }
        Ok(Some(file.read_exact_at(0, length as usize)?))
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
