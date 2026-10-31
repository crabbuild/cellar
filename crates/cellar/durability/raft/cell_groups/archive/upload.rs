// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-admitted upload owners. Persistent reservations, rather than local
//! handles or clocks, protect upload generations and inherited dependencies.
use super::{
    graph::{Graph, Kind},
    ownership::*,
    *,
};
use std::{collections::BTreeMap, ops::Deref};

#[derive(Clone, Copy, Debug)]
pub struct CellGroupArchiveUploadLimits {
    /// Counts upload maps, captured source witnesses and collector marks.
    /// At least two owners are needed for a map and its publication witness.
    pub max_owners: usize,
    pub max_retained_bytes: usize,
    pub max_objects: usize,
}

impl Default for CellGroupArchiveUploadLimits {
    fn default() -> Self {
        Self {
            max_owners: 8,
            max_retained_bytes: 64 * 1024 * 1024,
            max_objects: 16_384,
        }
    }
}

/// Share this admission across every group on the node. Upload clones retain
/// their map and permit; dropping a clone never aborts its persistent reservation.
#[derive(Clone)]
pub struct CellGroupArchiveUploadPool {
    pub(super) limits: CellGroupArchiveUploadLimits,
    budget: Arc<Budget>,
    workspace: usize,
}

impl CellGroupArchiveUploadPool {
    pub fn new(limits: CellGroupArchiveUploadLimits) -> Result<Self> {
        if !(2..=64).contains(&limits.max_owners) || !(1..=65_536).contains(&limits.max_objects) {
            return Err(CellGroupArchiveError::Limit);
        }
        // Compact object marks plus the content lookup index, allocation/tree
        // overhead and bounded control metadata. Archive I/O has its own pool.
        let workspace = limits
            .max_objects
            .checked_mul(512)
            .and_then(|n| n.checked_add(128 * 1024))
            .ok_or(CellGroupArchiveError::Limit)?;
        if workspace > limits.max_retained_bytes {
            return Err(CellGroupArchiveError::Limit);
        }
        Ok(Self {
            limits,
            budget: Budget::new(limits.max_retained_bytes, limits.max_owners),
            workspace,
        })
    }

    pub fn usage(&self) -> CellGroupArchiveUsage {
        let (retained_bytes, operations) = self.budget.usage();
        CellGroupArchiveUsage {
            operations,
            retained_bytes,
        }
    }

    pub(super) fn admit(&self) -> Result<Arc<Permit>> {
        self.budget
            .acquire(self.workspace)
            .map(Arc::new)
            .ok_or(CellGroupArchiveError::Busy)
    }
}

struct Owner {
    registry: CellGroupArchiveRegistry,
    ticket: CellGroupArchiveUploadTicket,
    inherited: Graph,
    reuse: BTreeMap<(Kind, [u8; 32], u64), super::objects::Object>,
    _reservation: Arc<Permit>,
    source_budget: Arc<Budget>,
}

/// The only producer of immutable group archive objects. All new keys include a
/// never-reused reservation generation. Reuse is restricted to its protected
/// parent closure; an arbitrary old content address cannot be resurrected.
#[derive(Clone)]
pub struct CellGroupArchiveUpload(Arc<Owner>);

impl Deref for CellGroupArchiveUpload {
    type Target = CellGroupArchive;

    fn deref(&self) -> &Self::Target {
        self.0.registry.archive()
    }
}

impl CellGroupArchiveRegistry {
    /// Resume only the exact still-pending persisted ticket. A replacement
    /// process must fence its predecessor before sharing/retiring that owner.
    pub async fn open_upload(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchiveUploadPool,
        ticket: &CellGroupArchiveUploadTicket,
        tails: &CellGroupArchiveTailPool,
    ) -> Result<CellGroupArchiveUpload> {
        domain
            .run(async {
                let reservation = pool.admit()?;
                let io = self.archive.admit()?;
                let pending = self.pending(ticket).await?;
                let inherited = match pending.parent {
                    Some(root) => {
                        Graph::load(
                            &self.archive,
                            domain,
                            &root,
                            tails.limits,
                            io.clone(),
                            reservation.clone(),
                            pool.limits.max_objects,
                        )
                        .await?
                    }
                    None => Graph::new(pool.limits.max_objects),
                };
                // Recheck after metadata I/O; an abort cannot produce a new owner.
                self.pending(ticket).await?;
                let reuse = inherited
                    .objects
                    .iter()
                    .map(|key| {
                        (
                            (key.kind, key.object.digest, key.object.length),
                            key.object.clone(),
                        )
                    })
                    .collect();
                Ok(CellGroupArchiveUpload(Arc::new(Owner {
                    registry: self.clone(),
                    ticket: *ticket,
                    inherited,
                    reuse,
                    _reservation: reservation,
                    source_budget: pool.budget.clone(),
                })))
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

impl CellGroupArchiveUpload {
    pub fn registry(&self) -> &CellGroupArchiveRegistry {
        &self.0.registry
    }

    pub fn ticket(&self) -> CellGroupArchiveUploadTicket {
        self.0.ticket
    }

    pub(super) fn retained(&self) -> Arc<Permit> {
        self.0._reservation.clone()
    }

    pub(super) fn max_objects(&self) -> usize {
        self.0.inherited.limit()
    }

    pub(super) fn source_budget(&self) -> Arc<Budget> {
        self.0.source_budget.clone()
    }

    pub(super) fn allows(&self, graph: &Graph) -> Result<()> {
        if graph.objects.iter().any(|key| {
            key.object.generation != self.0.ticket.generation()
                && !self.0.inherited.objects.contains(key)
        }) {
            return Err(CellGroupArchiveError::Invalid(
                "unprotected inherited archive object",
            ));
        }
        Ok(())
    }

    pub(super) async fn put(
        &self,
        kind: &str,
        bytes: Vec<u8>,
        limit: usize,
    ) -> Result<super::objects::Object> {
        if bytes.is_empty() || bytes.len() > limit {
            return Err(CellGroupArchiveError::Limit);
        }
        let kind =
            Kind::parse(kind).ok_or(CellGroupArchiveError::Invalid("archive object kind"))?;
        let own =
            super::objects::Object::of(self.registry().id(), self.ticket().generation(), &bytes);
        if let Some(inherited) = self.0.reuse.get(&(kind, own.digest, own.length)) {
            self.get(kind.name(), inherited, limit).await?;
            return Ok(inherited.clone());
        }
        self.put_generation(
            self.registry().id(),
            self.ticket().generation(),
            kind.name(),
            bytes,
            limit,
        )
        .await
    }
}
