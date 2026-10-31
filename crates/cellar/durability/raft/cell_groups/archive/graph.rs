// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact dependency-key expansion. No collector may delete from a partial mark.
use super::{objects::Object, ownership::Descriptor, *};
use std::collections::BTreeSet;

pub(super) const ROOT_DOMAIN: &str = "cellar-cell-group-archive-root-1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Kind {
    Chunks,
    Indexes,
    Entries,
    Tails,
    Roots,
}

impl Kind {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "chunks" => Self::Chunks,
            "indexes" => Self::Indexes,
            "entries" => Self::Entries,
            "tails" => Self::Tails,
            "roots" => Self::Roots,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Chunks => "chunks",
            Self::Indexes => "indexes",
            Self::Entries => "entries",
            Self::Tails => "tails",
            Self::Roots => "roots",
        }
    }

    pub fn limit(self, archive: &CellGroupArchive, limits: CellGroupArchiveTailLimits) -> usize {
        match self {
            Self::Chunks => CHUNK_BYTES,
            Self::Indexes | Self::Tails => archive.limits.max_index_bytes,
            Self::Entries => limits.max_tail_bytes,
            Self::Roots => 8192,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key {
    pub kind: Kind,
    pub object: Object,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Root {
    pub domain: String,
    pub marker: GroupManifest,
    pub tail: CellGroupArchiveTailReference,
}

pub(super) struct Graph {
    pub objects: BTreeSet<Key>,
    limit: usize,
}

impl Graph {
    pub fn new(limit: usize) -> Self {
        Self {
            objects: BTreeSet::new(),
            limit,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn insert(&mut self, kind: Kind, object: Object, generation: u64) -> Result<()> {
        if object.registry == [0; 32]
            || object.generation < 2
            || object.generation > generation
            || object.length == 0
        {
            return Err(CellGroupArchiveError::Invalid(
                "archive dependency generation",
            ));
        }
        let key = Key { kind, object };
        if !self.objects.contains(&key) && self.objects.len() == self.limit {
            return Err(CellGroupArchiveError::Limit);
        }
        self.objects.insert(key);
        Ok(())
    }

    pub async fn load(
        archive: &CellGroupArchive,
        domain: &ExecutionDomain,
        descriptor: &Descriptor,
        limits: CellGroupArchiveTailLimits,
        io: Arc<Permit>,
        retained: Arc<Permit>,
        max_objects: usize,
    ) -> Result<Self> {
        let root = archive.get("roots", &descriptor.object, 8192).await?;
        let root: Root = serde_json::from_slice(&root)
            .map_err(|_| CellGroupArchiveError::Invalid("archive root encoding"))?;
        if root.domain != ROOT_DOMAIN || root.marker != descriptor.marker {
            return Err(CellGroupArchiveError::Invalid("archive root binding"));
        }
        let tail = archive
            .read_tail_index(domain, &root.tail, limits, io.clone(), retained.clone())
            .await?;
        if tail.cut.marker != root.marker {
            return Err(CellGroupArchiveError::Invalid("archive root/tail cut"));
        }
        let checkpoint = archive
            .checkpoint_index(domain, &tail.checkpoint, io)
            .await?;
        if checkpoint.marker != tail.base {
            return Err(CellGroupArchiveError::Invalid(
                "archive root/checkpoint cut",
            ));
        }
        let generation = descriptor.object.generation;
        let mut graph = Self::new(max_objects);
        graph.insert(Kind::Roots, descriptor.object.clone(), generation)?;
        graph.insert(Kind::Tails, root.tail.object(), generation)?;
        graph.insert(Kind::Indexes, tail.checkpoint.object(), generation)?;
        for entry in tail.entries {
            graph.insert(Kind::Entries, entry.object, generation)?;
        }
        for chunk in checkpoint.chunks {
            graph.insert(Kind::Chunks, chunk.object, generation)?;
        }
        if graph
            .objects
            .iter()
            .any(|key| key.object.registry != descriptor.object.registry)
        {
            return Err(CellGroupArchiveError::Invalid(
                "archive dependency registry",
            ));
        }
        Ok(graph)
    }

    pub async fn verify(
        &self,
        archive: &CellGroupArchive,
        limits: CellGroupArchiveTailLimits,
    ) -> Result<()> {
        for key in &self.objects {
            archive
                .get(
                    key.kind.name(),
                    &key.object,
                    key.kind.limit(archive, limits),
                )
                .await?;
        }
        Ok(())
    }
}
