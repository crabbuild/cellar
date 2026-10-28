// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Allocation intents are read-only preparations. Persist one before sending
//! its CAS, then reconcile that exact intent after restart or a lost response.

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Operation {
    Reserve {
        image: [u8; 32],
        parent: Option<RootReference>,
    },
    Bind {
        ticket: RootReservation,
        root: RootDescriptor,
        select: Option<Position>,
    },
    Clone {
        reference: RootReference,
    },
    Acquire {
        expected: RootDescriptor,
    },
}

/// A single allocation at one registry revision. Preparing does not reserve
/// anything. Persist the full intent before `commit`; repeat that exact intent
/// after an uncertain outcome. Concurrent changes may expire it. Once expired,
/// it can never recreate a removed reference, even after its result was released.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootIntent {
    pub(super) registry: RootRegistryId,
    pub(super) revision: u64,
    nonce: [u8; 32],
    pub(super) operation: Operation,
}

/// Confirmed ownership only; neither result verifies stored data or grants
/// native installation authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootAllocation {
    Reservation(RootReservation),
    Reference(RootReference),
}

impl RootIntent {
    pub(super) fn digest(&self) -> Result<[u8; 32], ArchiveError> {
        if self.nonce == [0; 32] || self.revision < 2 || self.revision == u64::MAX {
            return Err(ArchiveError::Invalid("root allocation intent"));
        }
        // Includes the complete operation, not just its output root. Distinct
        // callers preparing identical requests cannot accidentally share a hold.
        Ok(Sha256::digest(manifest::encode(14, self, 4096)?).into())
    }
}

impl RootRegistry {
    async fn prepare(&self, operation: Operation) -> Result<RootIntent, ArchiveError> {
        let (mut state, _, _) = self.load().await?;
        let revision = state.revision.checked_add(1).ok_or(ArchiveError::Limit)?;
        let mut nonce = [0; 32];
        crate::asyncrt::rng("raft-root-intent").fill_bytes(&mut nonce);
        let intent = RootIntent {
            registry: self.id,
            revision,
            nonce,
            operation,
        };
        state.allocate(&intent, intent.digest()?)?;
        state.validate(self.id.0)?;
        Ok(intent)
    }

    pub async fn prepare_reserve(
        &self,
        image: [u8; 32],
        parent: Option<RootReference>,
    ) -> Result<RootIntent, ArchiveError> {
        if parent.is_some_and(|parent| parent.registry != self.id.0) {
            return Err(ArchiveError::Conflict);
        }
        self.prepare(Operation::Reserve { image, parent }).await
    }

    /// The caller must independently verify the descriptor and its closure.
    /// Optional selection and reference creation share one conditional write.
    pub async fn prepare_bind(
        &self,
        ticket: RootReservation,
        root: RootDescriptor,
        select: Option<LogPosition>,
    ) -> Result<RootIntent, ArchiveError> {
        if ticket.registry != self.id.0 {
            return Err(ArchiveError::Conflict);
        }
        self.prepare(Operation::Bind {
            ticket,
            root,
            select: select.map(Into::into),
        })
        .await
    }

    pub async fn prepare_clone(
        &self,
        reference: RootReference,
    ) -> Result<RootIntent, ArchiveError> {
        if reference.registry != self.id.0 {
            return Err(ArchiveError::Conflict);
        }
        self.prepare(Operation::Clone { reference }).await
    }

    pub async fn prepare_acquire_selected(
        &self,
        expected: RootDescriptor,
    ) -> Result<RootIntent, ArchiveError> {
        self.prepare(Operation::Acquire { expected }).await
    }
}
