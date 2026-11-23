// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete segment-file inventory sampled by the engine worker.

use super::{LogError, Worker};
use crate::raft::filesystem::MAX_ENGINE_DIRECTORY_ENTRIES;
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, NodeId, ProcessGeneration};
use raft_engine::FileNameExt as _;
use std::path::Path;

pub use raft_engine::{FileId as RaftSegmentId, LogQueue as RaftSegmentQueue};

/// A segment's identity within its engine and its visible file length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftSegmentFile {
    pub id: RaftSegmentId,
    pub bytes: u64,
}

/// Complete append and rewrite files at a serialized storage observation.
/// File lengths measure the segment files independently of placement
/// reservations and filesystem block allocation.
#[derive(Debug, PartialEq, Eq)]
pub struct RaftSegmentInventory {
    pub node: NodeId,
    pub disk: DiskLineage,
    pub engine: EngineInstanceId,
    pub process: ProcessGeneration,
    pub durable_sequence: u64,
    pub bytes: u64,
    files: Vec<RaftSegmentFile>,
}

impl RaftSegmentInventory {
    pub fn files(&self) -> &[RaftSegmentFile] {
        &self.files
    }
}

pub(super) const INVENTORY_BYTES: usize =
    4096 + 2 * MAX_ENGINE_DIRECTORY_ENTRIES * std::mem::size_of::<RaftSegmentFile>();

fn incomplete() -> LogError {
    LogError::Corrupt("incomplete Raft segment inventory".into())
}

impl Worker {
    pub(super) fn segment_inventory(&self) -> Result<RaftSegmentInventory, LogError> {
        let spans =
            [RaftSegmentQueue::Append, RaftSegmentQueue::Rewrite].map(|q| self.engine.file_span(q));
        let expected = spans.iter().try_fold(0_u64, |count, &(first, last)| {
            last.checked_sub(first)
                .and_then(|width| count.checked_add(width))
                .and_then(|n| n.checked_add(1))
                .ok_or_else(incomplete)
        })?;
        if expected >= MAX_ENGINE_DIRECTORY_ENTRIES as u64 {
            return Err(LogError::Overloaded);
        }
        let dir = Path::new(self.engine.path());
        let mut files = Vec::with_capacity(expected as usize);
        let mut bytes = 0_u64;
        for (n, entry) in self.filesystem.read_dir_iter(dir)?.enumerate() {
            if n >= MAX_ENGINE_DIRECTORY_ENTRIES {
                return Err(LogError::Overloaded);
            }
            let entry = entry?;
            if entry.file_name == "LOCK" {
                continue;
            }
            let name = entry
                .file_name
                .to_str()
                .filter(|name| name.is_ascii())
                .ok_or_else(incomplete)?;
            let id = RaftSegmentId::parse_file_name(name).ok_or_else(incomplete)?;
            let (first, last) = spans[id.queue as usize];
            if id.build_file_name() != name
                || !(first..=last).contains(&id.seq)
                || files.len() == expected as usize
            {
                return Err(incomplete());
            }
            let meta = self.filesystem.symlink_metadata(&dir.join(name))?;
            if !meta.is_file {
                return Err(incomplete());
            }
            bytes = bytes.checked_add(meta.len).ok_or_else(incomplete)?;
            files.push(RaftSegmentFile {
                id,
                bytes: meta.len,
            });
        }
        files.sort_unstable_by_key(|file| file.id);
        if files.len() != expected as usize || files.windows(2).any(|w| w[0].id == w[1].id) {
            return Err(incomplete());
        }
        Ok(RaftSegmentInventory {
            node: self.options.node,
            disk: self.options.disk,
            engine: EngineInstanceId::new(self.root.identity.storage_instance)
                .map_err(|_| incomplete())?,
            process: self.options.process,
            durable_sequence: self.root.sequence,
            bytes,
            files,
        })
    }
}
