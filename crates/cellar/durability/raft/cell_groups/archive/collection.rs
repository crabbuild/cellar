// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete marks precede deletion. Revision watermarks, protected parents and
//! never-reused generation keys make concurrent publication/delayed DELETE safe.
use super::{
    graph::{Graph, Key, Kind},
    *,
};
use futures_util::TryStreamExt;

#[derive(Clone, Copy, Debug)]
pub struct CellGroupArchiveCollectionLimits {
    pub max_scan_objects: usize,
    pub max_delete_objects: usize,
}

impl Default for CellGroupArchiveCollectionLimits {
    fn default() -> Self {
        Self {
            max_scan_objects: 4096,
            max_delete_objects: 256,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellGroupArchiveCollectionReport {
    pub revision: u64,
    pub protected_objects: usize,
    pub scanned_objects: usize,
    pub deleted_objects: usize,
    pub deleted_bytes: u64,
}

fn parse(path: &str, prefix: &Path, registry: [u8; 32]) -> Result<Key> {
    let suffix = path
        .strip_prefix(prefix.as_ref())
        .and_then(|s| s.strip_prefix('/'))
        .ok_or(CellGroupArchiveError::Invalid(
            "collection object namespace",
        ))?;
    let mut parts = suffix.split('/');
    let generation = parts
        .next()
        .ok_or(CellGroupArchiveError::Invalid("collection generation"))?;
    let parsed: u64 = generation
        .parse()
        .map_err(|_| CellGroupArchiveError::Invalid("collection generation"))?;
    if parsed < 2 || parsed == u64::MAX || parsed.to_string() != generation {
        return Err(CellGroupArchiveError::Invalid("collection generation"));
    }
    let kind = parts
        .next()
        .and_then(Kind::parse)
        .ok_or(CellGroupArchiveError::Invalid("collection object kind"))?;
    let (length, digest) = parts
        .next()
        .and_then(|p| p.split_once('-'))
        .ok_or(CellGroupArchiveError::Invalid("collection object identity"))?;
    let bytes: u64 = length
        .parse()
        .map_err(|_| CellGroupArchiveError::Invalid("collection object length"))?;
    if bytes == 0 || bytes.to_string() != length || digest.len() != 64 || parts.next().is_some() {
        return Err(CellGroupArchiveError::Invalid("collection object identity"));
    }
    let mut hash = [0; 32];
    hex::decode_to_slice(digest, &mut hash)
        .map_err(|_| CellGroupArchiveError::Invalid("collection digest"))?;
    if hex::encode(hash) != digest {
        return Err(CellGroupArchiveError::Invalid("collection digest encoding"));
    }
    Ok(Key {
        kind,
        object: super::objects::Object {
            registry,
            generation: parsed,
            length: bytes,
            digest: hash,
        },
    })
}

impl CellGroupArchiveRegistry {
    /// Mark selected roots, explicit holds and pending parents; retain entire
    /// pending/new generations. Verify every marked object before any DELETE.
    /// No unknown owner expires by time. Cancellation may leave partial garbage,
    /// but cannot permit a deleted key to be rebound by a later allocation.
    pub async fn collect(
        &self,
        domain: &ExecutionDomain,
        owners: &CellGroupArchiveUploadPool,
        tails: &CellGroupArchiveTailPool,
        limits: CellGroupArchiveCollectionLimits,
    ) -> Result<CellGroupArchiveCollectionReport> {
        if !(1..=65_536).contains(&limits.max_scan_objects)
            || limits.max_delete_objects == 0
            || limits.max_delete_objects > limits.max_scan_objects
            || limits.max_delete_objects > owners.limits.max_objects
        {
            return Err(CellGroupArchiveError::Limit);
        }
        domain
            .run(async {
                let retained = owners.admit()?;
                // Hashing entry objects needs the same bounded byte windows as tail
                // verification. This lease does not construct a physical replay.
                let _tail = tails.admit()?;
                let io = self.archive.admit()?;
                let protection = self.protection().await?;
                let mut marked = Graph::new(owners.limits.max_objects);
                for root in &protection.roots {
                    let closure = Graph::load(
                        &self.archive,
                        domain,
                        root,
                        tails.limits,
                        io.clone(),
                        retained.clone(),
                        owners.limits.max_objects,
                    )
                    .await?;
                    for key in closure.objects {
                        marked.insert(key.kind, key.object, u64::MAX - 1)?;
                    }
                }
                marked.verify(&self.archive, tails.limits).await?;
                let mut report = CellGroupArchiveCollectionReport {
                    revision: protection.revision,
                    protected_objects: marked.objects.len(),
                    ..Default::default()
                };
                let prefix = self
                    .archive
                    .prefix
                    .child("registries")
                    .child(hex::encode(self.id()))
                    .child("generations");
                let mut objects = self.archive.store.list(Some(&prefix));
                let mut candidates = Vec::new();
                self.archive
                    .io(async {
                        while report.scanned_objects < limits.max_scan_objects {
                            let Some(object) = objects
                                .try_next()
                                .await
                                .map_err(CellGroupArchiveError::Storage)?
                            else {
                                break;
                            };
                            report.scanned_objects += 1;
                            let key = parse(object.location.as_ref(), &prefix, self.id())?;
                            if key.object.generation <= protection.revision
                                && !protection
                                    .pending_generations
                                    .contains(&key.object.generation)
                                && !marked.objects.contains(&key)
                                && candidates.len() < limits.max_delete_objects
                            {
                                candidates.push((key, object.size));
                            }
                        }
                        Ok(())
                    })
                    .await?;
                // New reservations can inherit only roots protected by this mark;
                // their own generation is newer than its watermark. Released or
                // aborted allocations cannot resurrect an unmarked old key.
                for (key, bytes) in candidates {
                    let path = self.archive.object_path(key.kind.name(), &key.object);
                    self.archive
                        .io(async {
                            match self.archive.store.delete(&path).await {
                                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                                Err(error) => Err(CellGroupArchiveError::Storage(error)),
                            }
                        })
                        .await?;
                    report.deleted_objects += 1;
                    report.deleted_bytes = report
                        .deleted_bytes
                        .checked_add(bytes)
                        .ok_or(CellGroupArchiveError::Limit)?;
                }
                Ok(report)
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}
