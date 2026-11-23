// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persist the file basis before purge and retain its completed local receipt.

use super::segments::INVENTORY_BYTES;
use super::{
    engine_error, records, LogError, RaftSegmentFile, RaftSegmentId, RaftSegmentQueue, Worker,
};
use crate::raft::filesystem::MAX_ENGINE_DIRECTORY_ENTRIES;
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, NodeId, ProcessGeneration};
use raft_engine::LogBatch;
use serde::{Deserialize, Serialize};
use std::path::Path;

const KEY: &[u8] = b"cellar-segment-purge";
const MAX_RECORD_BYTES: usize = 1024 + 48 * MAX_ENGINE_DIRECTORY_ENTRIES;
const CACHE_BYTES: usize = 4 * MAX_RECORD_BYTES;
pub(super) const PURGE_BYTES: usize = CACHE_BYTES + 2 * INVENTORY_BYTES;

/// Historical proof of a local purge. Lengths use the original segment-file
/// basis. A Fleet settlement also needs the original charge and a fresh
/// complete observation from the currently authorized boot.
#[derive(Debug, PartialEq, Eq)]
pub struct RaftSegmentPurgeReceipt {
    pub number: u64,
    pub node: NodeId,
    pub disk: DiskLineage,
    pub engine: EngineInstanceId,
    pub prepared_process: ProcessGeneration,
    pub prepared_sequence: u64,
    pub completed_process: ProcessGeneration,
    pub completed_sequence: u64,
    files: Vec<RaftSegmentFile>,
}

impl RaftSegmentPurgeReceipt {
    pub fn removed_files(&self) -> &[RaftSegmentFile] {
        &self.files
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    format: u8,
    number: u64,
    node: [u8; 16],
    disk: [u8; 16],
    engine: [u8; 16],
    prepared_process: [u8; 16],
    prepared_sequence: u64,
    completed: Option<([u8; 16], u64)>,
    // Queue, sequence, and original length. Prepared records retain the
    // complete inventory. Completed records retain its removed subset.
    files: Vec<(u8, u64, u64)>,
}

fn corrupt() -> LogError {
    LogError::Corrupt("invalid segment purge record".into())
}

fn file_id((queue, seq, _): (u8, u64, u64)) -> Result<RaftSegmentId, LogError> {
    if seq == 0 {
        return Err(corrupt());
    }
    let queue = match queue {
        0 => RaftSegmentQueue::Append,
        1 => RaftSegmentQueue::Rewrite,
        _ => return Err(corrupt()),
    };
    Ok(RaftSegmentId::new(queue, seq))
}

impl Record {
    fn receipt(self) -> Result<RaftSegmentPurgeReceipt, LogError> {
        let (process, sequence) = self.completed.ok_or_else(corrupt)?;
        Ok(RaftSegmentPurgeReceipt {
            number: self.number,
            node: NodeId::new(self.node).map_err(|_| corrupt())?,
            disk: DiskLineage::new(self.disk).map_err(|_| corrupt())?,
            engine: EngineInstanceId::new(self.engine).map_err(|_| corrupt())?,
            prepared_process: ProcessGeneration::new(self.prepared_process)
                .map_err(|_| corrupt())?,
            prepared_sequence: self.prepared_sequence,
            completed_process: ProcessGeneration::new(process).map_err(|_| corrupt())?,
            completed_sequence: sequence,
            files: self
                .files
                .into_iter()
                .map(|file| {
                    Ok(RaftSegmentFile {
                        id: file_id(file)?,
                        bytes: file.2,
                    })
                })
                .collect::<Result<_, LogError>>()?,
        })
    }
}

impl Worker {
    fn segment_purge_record(&self) -> Result<Option<Record>, LogError> {
        let Some(bytes) = self.engine.get(0, KEY) else {
            return Ok(None);
        };
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(corrupt());
        }
        let record: Record = records::decode(&bytes)?;
        if record.format != 1
            || record.number == 0
            || record.node != self.options.node.bytes()
            || record.disk != self.options.disk.bytes()
            || record.engine != self.root.identity.storage_instance
            || record.prepared_process == [0; 16]
            || record.prepared_sequence == 0
            || record.prepared_sequence > self.root.sequence
            || record.completed.is_some_and(|(process, sequence)| {
                process == [0; 16]
                    || sequence <= record.prepared_sequence
                    || sequence > self.root.sequence
            })
            || record.files.len() >= MAX_ENGINE_DIRECTORY_ENTRIES
        {
            return Err(corrupt());
        }
        let mut last = None;
        for &file in &record.files {
            let id = file_id(file)?;
            if last.is_some_and(|old| old >= id) {
                return Err(corrupt());
            }
            last = Some(id);
        }
        Ok(Some(record))
    }

    pub(super) fn recover_segment_purge_cache(&mut self) -> Result<usize, LogError> {
        Ok(if let Some(record) = self.segment_purge_record()? {
            self.segment_purge_number = record.number;
            CACHE_BYTES
        } else {
            0
        })
    }

    fn persist_segment_purge(&mut self, record: &Record, first: bool) -> Result<(), LogError> {
        let cache_bytes = self
            .cache_bytes
            .checked_add(if first { CACHE_BYTES } else { 0 })
            .filter(|bytes| *bytes <= self.options.max_cache_bytes)
            .ok_or(LogError::Overloaded)?;
        let bytes = records::encode(record)?;
        // Completion replaces the file list with a subset and adds one boot
        // and sequence. Reserve that space before admitting deletion.
        let completion_bytes = if record.completed.is_none() { 128 } else { 0 };
        let upper = bytes.len() + completion_bytes;
        if upper > MAX_RECORD_BYTES || upper > self.options.max_batch_bytes.saturating_sub(4096) {
            return Err(LogError::Overloaded);
        }
        let mut batch = LogBatch::default();
        batch.put(0, KEY.to_vec(), bytes).map_err(engine_error)?;
        self.fence(&mut batch)?;
        self.cache_bytes = cache_bytes;
        self.segment_purge_number = record.number;
        Ok(())
    }

    pub(super) fn purge_segments(
        &mut self,
        number: u64,
    ) -> Result<RaftSegmentPurgeReceipt, LogError> {
        let previous = self.segment_purge_record()?;
        let mut record = match previous {
            Some(record) if record.number == number => {
                if record.completed.is_some() {
                    return record.receipt();
                }
                record
            }
            previous => {
                let next = match &previous {
                    None => Some(1),
                    Some(record) if record.completed.is_some() => record.number.checked_add(1),
                    _ => None,
                };
                if next != Some(number) {
                    return Err(LogError::Invalid("segment purge number".into()));
                }
                let inventory = self.segment_inventory()?;
                let record = Record {
                    format: 1,
                    number,
                    node: inventory.node.bytes(),
                    disk: inventory.disk.bytes(),
                    engine: inventory.engine.bytes(),
                    prepared_process: inventory.process.bytes(),
                    prepared_sequence: inventory
                        .durable_sequence
                        .checked_add(1)
                        .ok_or_else(corrupt)?,
                    completed: None,
                    files: inventory
                        .files()
                        .iter()
                        .map(|file| (file.id.queue as u8, file.id.seq, file.bytes))
                        .collect(),
                };
                self.persist_segment_purge(&record, previous.is_none())?;
                record
            }
        };

        self.purge()?;
        // A previous process may have died after unlink and before directory
        // sync. Seal the current directory even if this pass unlinked nothing.
        self.filesystem.sync_all(Path::new(self.engine.path()))?;
        let after = self.segment_inventory()?;
        let first = [RaftSegmentQueue::Append, RaftSegmentQueue::Rewrite]
            .map(|queue| self.engine.file_span(queue).0);
        let mut removed = Vec::new();
        for file in record.files {
            let id = file_id(file)?;
            if after
                .files()
                .binary_search_by_key(&id, |row| row.id)
                .is_err()
            {
                if id.seq >= first[id.queue as usize] {
                    return Err(corrupt());
                }
                removed.push(file);
            }
        }
        record.files = removed;
        record.completed = Some((
            after.process.bytes(),
            after.durable_sequence.checked_add(1).ok_or_else(corrupt)?,
        ));
        let revisit = self.purge_pending;
        self.persist_segment_purge(&record, false)?;
        // The receipt write alone must not schedule another purge.
        self.purge_pending = revisit;
        record.receipt()
    }
}
