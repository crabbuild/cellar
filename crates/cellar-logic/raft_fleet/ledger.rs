// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conservative accounting before a membership move. Serving roles are not
//! physical resources: a retired source still occupies its engine and disk.

use crate::consensus::{
    ClusterId, EngineInstanceId, GroupScope, IncarnationId, NodeIdentity, ProposalId,
};
use std::collections::{BTreeMap, BTreeSet};

const MAX_RESERVATIONS: usize = 4096;

/// Hard resources. CPU/load predictions belong to placement scoring, not this
/// ledger. Memory includes the caller's conservative fixed and staging bounds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Resources {
    pub engine_slots: u64,
    pub durable_bytes: u64,
    pub memory_bytes: u64,
}

impl Resources {
    pub(super) fn checked_add(self, rhs: Self) -> Result<Self, LedgerError> {
        Ok(Self {
            engine_slots: self
                .engine_slots
                .checked_add(rhs.engine_slots)
                .ok_or(LedgerError::ArithmeticOverflow)?,
            durable_bytes: self
                .durable_bytes
                .checked_add(rhs.durable_bytes)
                .ok_or(LedgerError::ArithmeticOverflow)?,
            memory_bytes: self
                .memory_bytes
                .checked_add(rhs.memory_bytes)
                .ok_or(LedgerError::ArithmeticOverflow)?,
        })
    }

    pub(super) fn covers(self, rhs: Self) -> bool {
        self.engine_slots >= rhs.engine_slots
            && self.durable_bytes >= rhs.durable_bytes
            && self.memory_bytes >= rhs.memory_bytes
    }

    pub(super) fn subtract(self, rhs: Self) -> Self {
        // Every caller first proves the componentwise covers relation.
        Self {
            engine_slots: self.engine_slots - rhs.engine_slots,
            durable_bytes: self.durable_bytes - rhs.durable_bytes,
            memory_bytes: self.memory_bytes - rhs.memory_bytes,
        }
    }

    pub(super) fn high_water(self, other: Self) -> Self {
        Self {
            engine_slots: self.engine_slots.max(other.engine_slots),
            durable_bytes: self.durable_bytes.max(other.durable_bytes),
            memory_bytes: self.memory_bytes.max(other.memory_bytes),
        }
    }

    fn require_fit(self, capacity: Self) -> Result<(), LedgerError> {
        if self.engine_slots > capacity.engine_slots {
            Err(LedgerError::CapacityExceeded(ResourceKind::EngineSlots))
        } else if self.durable_bytes > capacity.durable_bytes {
            Err(LedgerError::CapacityExceeded(ResourceKind::DurableBytes))
        } else if self.memory_bytes > capacity.memory_bytes {
            Err(LedgerError::CapacityExceeded(ResourceKind::MemoryBytes))
        } else {
            Ok(())
        }
    }
}

/// The exact capacity qualification, including a boot nonce independent of a
/// reusable process hint. A new boot must retain unresolved old-boot debt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapacityScope {
    pub cluster: ClusterId,
    pub incarnation: IncarnationId,
    pub catalog: [u8; 32],
    pub node: NodeIdentity,
    pub engine: EngineInstanceId,
    pub boot: [u8; 16],
    pub profile_revision: u64,
}

impl CapacityScope {
    fn validate(self) -> Result<(), LedgerError> {
        if self.catalog == [0; 32] || self.boot == [0; 16] || self.profile_revision == 0 {
            return Err(LedgerError::InvalidScope);
        }
        Ok(())
    }

    fn same_engine(self, other: Self) -> bool {
        self.cluster == other.cluster
            && self.incarnation == other.incarnation
            && self.catalog == other.catalog
            && self.node.node == other.node.node
            && self.node.disk == other.node.disk
            && self.engine == other.engine
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LedgerPolicy {
    pub capacity: Resources,
    /// Resource-specific reserve, charged once per node, not once per action.
    pub reserve: Resources,
    pub max_active_moves: u32,
    /// Receiver-local monotonic time; no remote wall-clock comparison.
    pub max_sample_age_ms: u64,
}

impl LedgerPolicy {
    fn validate(self) -> Result<(), LedgerError> {
        if self.capacity.engine_slots == 0
            || self.capacity.durable_bytes == 0
            || self.capacity.memory_bytes == 0
            || !self.capacity.covers(self.reserve)
            || self.max_active_moves == 0
            || self.max_active_moves as usize > MAX_RESERVATIONS
            || self.max_sample_age_ms == 0
            || self.max_sample_age_ms > 3_600_000
        {
            return Err(LedgerError::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalSample {
    pub scope: CapacityScope,
    /// Strictly increasing within this exact boot/profile scope.
    pub sequence: u64,
    pub observed_ms: u64,
    /// Must include dormant, retired and unattached histories and fixed pools.
    /// A checkpoint retains the componentwise high water for this boot; lower
    /// later measurements alone never establish physical release.
    pub physical: Resources,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Materialization {
    pub sequence: u64,
    /// Cumulative attributed footprint, never larger than the reserved peak.
    pub physical: Resources,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub id: ProposalId,
    pub group: GroupScope,
    pub scope: CapacityScope,
    /// Increment beyond starting physical occupancy, including temporary work.
    pub peak: Resources,
    pub materialized: Option<Materialization>,
}

impl Reservation {
    fn validate(&self, scope: CapacityScope) -> Result<(), LedgerError> {
        self.scope.validate()?;
        self.group
            .validate()
            .map_err(|_| LedgerError::InvalidReservation)?;
        if !scope.same_engine(self.scope)
            || self.group.cluster != scope.cluster
            || self.peak == Resources::default()
        {
            return Err(LedgerError::InvalidReservation);
        }
        if let Some(materialized) = self.materialized {
            if materialized.sequence == 0 || !self.peak.covers(materialized.physical) {
                return Err(LedgerError::InvalidReservation);
            }
        }
        Ok(())
    }
}

/// Only an authenticated observer may attribute part of a sample to an action.
/// The pure core checks identity, monotonicity and arithmetic, not provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleAttribution {
    pub reservation: ProposalId,
    pub scope: CapacityScope,
    pub physical: Resources,
}

/// Codec-neutral state for an adapter's atomic root update. `complete` must
/// come from a complete rooted reservation inventory, never from a partial page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerSnapshot {
    pub scope: CapacityScope,
    pub policy: LedgerPolicy,
    pub sample: PhysicalSample,
    pub reservations: Vec<Reservation>,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceKind {
    EngineSlots,
    DurableBytes,
    MemoryBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LedgerError {
    InvalidScope,
    InvalidPolicy,
    InvalidObservation,
    InvalidReservation,
    Incomplete,
    Limit,
    ScopeMismatch,
    DuplicateReservation,
    PayloadConflict,
    UnknownReservation,
    StaleObservation,
    StaleMaterialization,
    UnattributedPhysical,
    ArithmeticOverflow,
    CapacityExceeded(ResourceKind),
    MovementLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceCharge {
    pub physical: Resources,
    pub unmaterialized: Resources,
    pub reserve: Resources,
    pub total: Resources,
    pub active_moves: u32,
}

/// No release operation is provided. Terminal group proof and joined physical
/// owners belong to the owning adapter; an observation or moved serving role
/// cannot cancel debt. Old-process reservations retain their full peak and
/// movement slot until that separate proof boundary is implemented.
#[derive(Clone, Debug)]
pub struct ResourceLedger {
    scope: CapacityScope,
    policy: LedgerPolicy,
    sample: PhysicalSample,
    reservations: BTreeMap<ProposalId, Reservation>,
}

impl ResourceLedger {
    pub fn restore(snapshot: LedgerSnapshot) -> Result<Self, LedgerError> {
        if !snapshot.complete {
            return Err(LedgerError::Incomplete);
        }
        snapshot.scope.validate()?;
        snapshot.policy.validate()?;
        validate_sample(snapshot.scope, snapshot.sample)?;
        if snapshot.reservations.len() > MAX_RESERVATIONS {
            return Err(LedgerError::Limit);
        }
        let mut reservations = BTreeMap::new();
        for reservation in snapshot.reservations {
            reservation.validate(snapshot.scope)?;
            if reservations.insert(reservation.id, reservation).is_some() {
                return Err(LedgerError::DuplicateReservation);
            }
        }
        let ledger = Self {
            scope: snapshot.scope,
            policy: snapshot.policy,
            sample: snapshot.sample,
            reservations,
        };
        ledger.validate_credits(ledger.sample.physical)?;
        // An overfull restored ledger is still inspectable, but no new action
        // can be admitted. Arithmetic overflow is never saturated into room.
        ledger.charge_inner()?;
        Ok(ledger)
    }

    pub fn snapshot(&self) -> LedgerSnapshot {
        LedgerSnapshot {
            scope: self.scope,
            policy: self.policy,
            sample: self.sample,
            reservations: self.reservations.values().cloned().collect(),
            complete: true,
        }
    }

    pub fn charge(&self, now_ms: u64) -> Result<ResourceCharge, LedgerError> {
        self.require_fresh(now_ms)?;
        self.charge_inner()
    }

    /// Reserve one candidate locally. `false` means historical exact retry,
    /// including after later materialization; it grants no current authorization.
    /// Callers must persist a successful result with their expected-root CAS.
    pub fn reserve(&mut self, reservation: Reservation, now_ms: u64) -> Result<bool, LedgerError> {
        reservation.validate(self.scope)?;
        if reservation.scope != self.scope || reservation.materialized.is_some() {
            return Err(LedgerError::InvalidReservation);
        }
        if let Some(previous) = self.reservations.get(&reservation.id) {
            if previous.scope == reservation.scope
                && previous.group == reservation.group
                && previous.peak == reservation.peak
            {
                return Ok(false);
            }
            return Err(LedgerError::PayloadConflict);
        }
        self.require_fresh(now_ms)?;
        if self.reservations.len() >= MAX_RESERVATIONS {
            return Err(LedgerError::Limit);
        }
        let charge = self.charge_inner()?;
        if charge.active_moves >= self.policy.max_active_moves {
            return Err(LedgerError::MovementLimit);
        }
        charge
            .total
            .checked_add(reservation.peak)?
            .require_fit(self.policy.capacity)?;
        self.reservations.insert(reservation.id, reservation);
        Ok(true)
    }

    /// Atomically install a newer sample and attributed cumulative footprints.
    /// Missing attribution retains the prior charge. Reduced physical readings
    /// retain the high water; they do not establish a release certificate.
    pub fn observe(
        &mut self,
        sample: PhysicalSample,
        attributions: &[SampleAttribution],
    ) -> Result<(), LedgerError> {
        validate_sample(self.scope, sample)?;
        if sample.sequence <= self.sample.sequence || sample.observed_ms < self.sample.observed_ms {
            return Err(LedgerError::StaleObservation);
        }
        if attributions.len() > MAX_RESERVATIONS {
            return Err(LedgerError::Limit);
        }
        // All checks run against a bounded candidate, so failed observations
        // cannot partially advance a sample or erase outstanding debt.
        let mut candidate = self.clone();
        candidate.sample = sample;
        let mut seen = BTreeSet::new();
        for attribution in attributions {
            if !seen.insert(attribution.reservation) {
                return Err(LedgerError::DuplicateReservation);
            }
            if attribution.scope != self.scope {
                return Err(LedgerError::ScopeMismatch);
            }
            let reservation = candidate
                .reservations
                .get_mut(&attribution.reservation)
                .ok_or(LedgerError::UnknownReservation)?;
            if reservation.scope != self.scope {
                return Err(LedgerError::ScopeMismatch);
            }
            if !reservation.peak.covers(attribution.physical) {
                return Err(LedgerError::InvalidObservation);
            }
            if reservation
                .materialized
                .is_some_and(|old| !attribution.physical.covers(old.physical))
            {
                return Err(LedgerError::StaleMaterialization);
            }
            reservation.materialized = Some(Materialization {
                sequence: sample.sequence,
                physical: attribution.physical,
            });
        }
        candidate.validate_credits(sample.physical)?;
        candidate.sample.physical = self.sample.physical.high_water(sample.physical);
        candidate.charge_inner()?;
        *self = candidate;
        Ok(())
    }

    fn require_fresh(&self, now_ms: u64) -> Result<(), LedgerError> {
        let age = now_ms
            .checked_sub(self.sample.observed_ms)
            .ok_or(LedgerError::InvalidObservation)?;
        if age > self.policy.max_sample_age_ms {
            return Err(LedgerError::StaleObservation);
        }
        Ok(())
    }

    fn credit(&self, reservation: &Reservation) -> Result<Resources, LedgerError> {
        if reservation.scope != self.scope {
            return Ok(Resources::default());
        }
        match reservation.materialized {
            Some(mark) if mark.sequence > self.sample.sequence => {
                Err(LedgerError::StaleObservation)
            }
            Some(mark) => Ok(mark.physical),
            None => Ok(Resources::default()),
        }
    }

    fn validate_credits(&self, physical: Resources) -> Result<(), LedgerError> {
        let mut credited = Resources::default();
        for reservation in self.reservations.values() {
            credited = credited.checked_add(self.credit(reservation)?)?;
        }
        if !physical.covers(credited) {
            return Err(LedgerError::UnattributedPhysical);
        }
        Ok(())
    }

    fn charge_inner(&self) -> Result<ResourceCharge, LedgerError> {
        let mut unmaterialized = Resources::default();
        for reservation in self.reservations.values() {
            unmaterialized =
                unmaterialized.checked_add(reservation.peak.subtract(self.credit(reservation)?))?;
        }
        Ok(ResourceCharge {
            physical: self.sample.physical,
            unmaterialized,
            reserve: self.policy.reserve,
            total: self
                .sample
                .physical
                .checked_add(unmaterialized)?
                .checked_add(self.policy.reserve)?,
            active_moves: self.reservations.len() as u32,
        })
    }
}

fn validate_sample(scope: CapacityScope, sample: PhysicalSample) -> Result<(), LedgerError> {
    if scope != sample.scope {
        return Err(LedgerError::ScopeMismatch);
    }
    if sample.sequence == 0 {
        return Err(LedgerError::InvalidObservation);
    }
    Ok(())
}
