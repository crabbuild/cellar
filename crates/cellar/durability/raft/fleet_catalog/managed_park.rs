// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The rooted exclusion for one exact managed park negotiation.

use super::disruption::{Disruption, SLOT_KEY};
use super::histories::{history_mutation, validate_request};
use super::managed_transitions::root_digest;
use super::resources::Authority;
use super::*;
use crate::raft::{
    cell_groups::{CellGroupQuorumMembership, ParkPrepareRequest},
    fleet_stop::BootBinding,
    member_state::FleetGroupBinding,
    transport::ParkDeliveryReceipt,
};
use cellar_logic::{
    cell_group::{GroupHistory, MembershipRevision, ParkGeneration},
    consensus::{LogIndex, LogPosition, MemberId, NodeId, ProposalId, Term},
};
use serde::Deserialize;

pub(super) const KIND: u8 = 202;
const SETTLEMENT_KIND: u8 = 203;
const OPERATION_PREFIX: &str = "managed-park/";
const SETTLEMENT_PREFIX: &str = "managed-park-settled/";
const RECORD_LIMIT: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    request: FleetRequestIdentity,
    input_root: [u8; 32],
    group: FleetGroupBinding,
    birth: [u8; 32],
    schema: [u8; 32],
    boot: BootBinding,
    member: u64,
    base: u64,
    term: u64,
    membership: u64,
}

impl Record {
    pub(super) fn validate(&self, fleet: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.boot
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("park boot"))?;
        let history = self.history()?;
        if self.group != fleet.binding(&history)?
            || self.boot.catalog != self.group.catalog()
            || self.boot.fleet != self.group.fleet()
            || [self.input_root, self.birth, self.schema].contains(&[0; 32])
            || self.member == 0
            || self.term == 0
            || self.term >= u64::MAX - 2
            || self.base >= u64::MAX - 2
            || self.membership == 0
            || self.membership > self.base
        {
            return Err(FleetCatalogError::Invalid("managed park binding"));
        }
        Ok(())
    }

    fn history(&self) -> Result<GroupHistory> {
        self.group
            .scope()
            .map_err(|_| FleetCatalogError::Invalid("park history"))
    }

    pub(super) fn operation(&self) -> [u8; 16] {
        self.request.operation
    }

    pub(super) fn key(&self) -> String {
        key(OPERATION_PREFIX, self.operation())
    }

    pub(super) fn check_basis(&self, snapshot: &FleetSnapshot) -> Result<()> {
        if self.request.expected_revision != snapshot.revision()
            || self.input_root != root_digest(snapshot)?
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    pub(super) fn mutation(&self) -> Result<FleetMutation> {
        let value = encode(KIND, self, RECORD_LIMIT)?;
        Ok(history_mutation(
            &self.request,
            vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: self.key(),
                    value: Some(value.clone()),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: SLOT_KEY.into(),
                    value: Some(value),
                },
            ],
        ))
    }

    fn settlement(&self, proof: ManagedParkSettlement<'_>) -> Result<SettlementEvidence> {
        match proof {
            ManagedParkSettlement::Delivered(receipt) => {
                let leader = receipt.leader();
                let cut = receipt.cut();
                if !self.group.matches_scope(&leader.scope)
                    || leader.member.get() != self.member
                    || leader.node.bytes() != self.boot.node
                    || leader.disk.bytes() != self.boot.disk
                    || leader.process.bytes() != self.boot.process
                    || receipt.engine().bytes() != self.boot.engine
                    || cut.generation() != self.base + 1
                    || cut.position().term.get() != self.term
                    || cut.membership_revision() != self.membership
                {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(SettlementEvidence::Delivered)
            }
            ManagedParkSettlement::Advanced(observed) => {
                let root = observed.root();
                let at = root.position();
                if !self.group.matches_scope(&observed.replica().scope)
                    || root.route().birth_digest != self.birth
                    || root.route().schema_digest != self.schema
                    || !observed.status().ready
                    || observed.status().leader != Some(observed.replica().member.get())
                    || !fences_park(self.base, self.term, at)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(SettlementEvidence::Advanced {
                    index: at.index.get(),
                    term: at.term.get(),
                })
            }
        }
    }
}

fn fences_park(base: u64, term: u64, position: LogPosition) -> bool {
    position.term.get() > term
        || (position.term.get() == term && position.index.get() > base.saturating_add(1))
}

fn key(prefix: &str, operation: [u8; 16]) -> String {
    format!("{prefix}{}", hex::encode(operation))
}

pub(crate) struct ManagedParkPermit(Record);

impl ManagedParkPermit {
    /// A later actor turn rechecks the exact selected base. The rooted
    /// operation cannot be reused at a different park generation.
    pub(crate) fn prepare_request(&self, deadline_ms: u64) -> Result<ParkPrepareRequest> {
        let term = self.term();
        Ok(ParkPrepareRequest {
            proposal: ProposalId::new(self.0.operation())
                .map_err(|_| FleetCatalogError::Invalid("park operation"))?,
            term,
            membership: MembershipRevision::new(self.0.membership)
                .map_err(|_| FleetCatalogError::Invalid("park membership revision"))?,
            expected_applied: LogPosition {
                index: LogIndex::new(self.0.base),
                term,
            },
            deadline_ms,
        })
    }

    pub(crate) fn term(&self) -> Term {
        Term::new(self.0.term)
    }

    pub(crate) fn operation(&self) -> Result<ProposalId> {
        ProposalId::new(self.0.operation())
            .map_err(|_| FleetCatalogError::Invalid("park operation"))
    }

    pub(crate) fn generation(&self) -> Result<ParkGeneration> {
        ParkGeneration::new(self.0.base + 1)
            .map_err(|_| FleetCatalogError::Invalid("park generation"))
    }

    pub(crate) fn matches_request(
        &self,
        history: GroupHistory,
        request: &FleetRequestIdentity,
    ) -> bool {
        self.0.group.matches_scope(&history) && &self.0.request == request
    }

    pub(crate) fn same_reservation(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub(crate) fn matches_fleet(&self, history: GroupHistory, boot: &BootBinding) -> bool {
        self.0.group.matches_scope(&history)
            && self.0.boot.fleet == boot.fleet
            && self.0.boot.catalog == boot.catalog
    }

    pub(crate) fn matches_owner(
        &self,
        history: GroupHistory,
        boot: &BootBinding,
        member: MemberId,
    ) -> bool {
        self.0.group.matches_scope(&history)
            && &self.0.boot == boot
            && self.0.member == member.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagedParkPhase {
    Negotiating,
    Delivered,
    Superseded,
}

pub(crate) struct ManagedParkOperation {
    pub(crate) permit: ManagedParkPermit,
    pub(crate) phase: ManagedParkPhase,
    pub(crate) revision: u64,
}

pub(crate) enum ManagedParkSettlement<'a> {
    Delivered(&'a ParkDeliveryReceipt),
    Advanced(&'a CellGroupQuorumMembership),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
enum SettlementEvidence {
    Delivered,
    Advanced { index: u64, term: u64 },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settlement {
    admission: [u8; 32],
    request: FleetRequestIdentity,
    evidence: SettlementEvidence,
}

impl Settlement {
    fn mutation(&self) -> Result<FleetMutation> {
        Ok(history_mutation(
            &self.request,
            vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: key(SETTLEMENT_PREFIX, self.request.operation),
                    value: Some(encode(SETTLEMENT_KIND, self, RECORD_LIMIT)?),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: SLOT_KEY.into(),
                    value: None,
                },
            ],
        ))
    }
}

impl FleetCatalog {
    pub(crate) async fn admit_managed_park(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        observed: &CellGroupQuorumMembership,
        timeout: Duration,
    ) -> Result<ManagedParkPermit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        if self
            .active_topology_operation(snapshot, deadline.remaining()?)
            .await?
            .is_some()
        {
            return Err(FleetCatalogError::Conflict);
        }
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                self.require_transition_membership_idle(snapshot).await?;
                if let Some(active) = self.active_disruption(snapshot).await? {
                    return Err(FleetCatalogError::DisruptionInProgress {
                        operation: active.operation(),
                    });
                }
                let replica = observed.replica();
                let group = self.binding(&replica.scope)?;
                let group_key = group.key().map_err(|_| FleetCatalogError::Conflict)?;
                let authorization = self
                    .authorize_live_group(snapshot, group.clone(), &group_key)
                    .await?;
                let host = self.host_from(snapshot, replica.node).await?;
                let boot = host.boot_binding()?;
                let root = observed.root();
                let at = root.position();
                let conf = observed.configuration();
                if host.state() != FleetNodeState::Active
                    || host.is_isolated()
                    || boot.disk != replica.disk.bytes()
                    || boot.process != replica.process.bytes()
                    || boot.engine != observed.engine().bytes()
                    || authorization.history_digests()
                        != (root.route().birth_digest, root.route().schema_digest)
                    || !authorization.matches_replica(replica, observed.membership())
                    || observed
                        .membership()
                        .fleet_status()
                        .is_none_or(|status| status.binding != group)
                    || !root.is_active()
                    || !observed.status().ready
                    || observed.status().leader != Some(replica.member.get())
                    || observed.status().committed != at.index
                    || !conf.voters.contains(&replica.member.get())
                    || !conf.voters_outgoing.is_empty()
                    || !conf.learners.is_empty()
                    || !conf.learners_next.is_empty()
                    || conf.auto_leave
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let record = Record {
                    request: request.clone(),
                    input_root: root_digest(snapshot)?,
                    group,
                    birth: root.route().birth_digest,
                    schema: root.route().schema_digest,
                    boot,
                    member: replica.member.get(),
                    base: at.index.get(),
                    term: at.term.get(),
                    membership: observed.membership().revision().get(),
                };
                record.validate(self)?;
                record.check_basis(snapshot)?;
                if self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &record.key(),
                    )
                    .await?
                    .is_some()
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let mutation = record.mutation()?;
                mutation.validate()?;
                self.commit_with_disruption(
                    snapshot,
                    &mutation,
                    None,
                    Authority::ManagedPark,
                    || Ok(()),
                )
                .await?;
                Ok(ManagedParkPermit(record))
            }),
        )
        .await
    }

    pub(crate) async fn reopen_managed_park(
        &self,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<ManagedParkPermit>> {
        self.run(timeout, async {
            let current = self.inner.current().await?;
            match self.active_disruption(&current).await? {
                Some(Disruption::Park(record)) if record.operation() == operation.bytes() => {
                    Ok(Some(ManagedParkPermit(record)))
                }
                _ => Ok(None),
            }
        })
        .await
    }

    pub(crate) async fn observe_managed_park(
        &self,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<ManagedParkOperation>> {
        self.run(timeout, Box::pin(async {
            let current = self.inner.current().await?;
            let operations = current.head.tables[FleetTable::Operations.slot()];
            let Some(bytes) = self.inner.lookup(operations, &key(OPERATION_PREFIX, operation.bytes())).await? else {
                return Ok(None);
            };
            let record: Record = decode(KIND, &bytes, RECORD_LIMIT)?;
            record.validate(self)?;
            self.confirm_managed_park(&current, &record).await?;
            if record.operation() != operation.bytes() {
                return Err(FleetCatalogError::Corrupt("park operation index"));
            }
            let terminal = self.inner.lookup(operations, &key(SETTLEMENT_PREFIX, operation.bytes())).await?;
            let phase = if let Some(terminal) = terminal {
                let terminal: Settlement = decode(SETTLEMENT_KIND, &terminal, RECORD_LIMIT)?;
                validate_request(&terminal.request)?;
                let mutation = terminal.mutation()?;
                let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
                if terminal.admission != hash(&bytes)
                    || terminal.request.operation != operation.bytes()
                    || terminal.request.principal != record.request.principal
                    || terminal.request.id == record.request.id
                    || terminal.request.expected_revision <= record.request.expected_revision
                    || self.inner.committed(&current.head, &hex::encode(terminal.request.id), digest).await?.is_none()
                {
                    return Err(FleetCatalogError::Corrupt("park settlement index"));
                }
                match terminal.evidence {
                    SettlementEvidence::Delivered => ManagedParkPhase::Delivered,
                    SettlementEvidence::Advanced { index, term } if fences_park(
                        record.base, record.term,
                        LogPosition { index: LogIndex::new(index), term: Term::new(term) },
                    ) => ManagedParkPhase::Superseded,
                    SettlementEvidence::Advanced { .. } => return Err(FleetCatalogError::Corrupt("park settlement cut")),
                }
            } else {
                if current.head.disruption != Some(hash(&bytes))
                    || !matches!(self.active_disruption(&current).await?, Some(Disruption::Park(ref active)) if *active == record)
                {
                    return Err(FleetCatalogError::Corrupt("unsettled park missing disruption"));
                }
                ManagedParkPhase::Negotiating
            };
            Ok(Some(ManagedParkOperation { permit: ManagedParkPermit(record), phase, revision: current.revision() }))
        })).await
    }

    pub(crate) async fn settle_managed_park(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        permit: &ManagedParkPermit,
        proof: ManagedParkSettlement<'_>,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let record = &permit.0;
        record.validate(self)?;
        if request.operation != record.operation()
            || request.principal != record.request.principal
            || request.id == record.request.id
            || request.expected_revision <= record.request.expected_revision
        {
            return Err(FleetCatalogError::Invalid("park settlement request"));
        }
        let settlement = Settlement {
            admission: hash(&encode(KIND, record, RECORD_LIMIT)?),
            request: request.clone(),
            evidence: record.settlement(proof)?,
        };
        let mutation = settlement.mutation()?;
        mutation.validate()?;
        self.run(timeout, Box::pin(async {
            let current = self.inner.current().await?;
            let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
            if let Some(applied_revision) = self.inner.committed(&current.head, &hex::encode(request.id), digest).await? {
                return Ok(FleetCommit { snapshot: current, applied_revision });
            }
            if current.token != snapshot.token || request.expected_revision != current.revision() {
                return Err(FleetCatalogError::Conflict);
            }
            if !matches!(self.active_disruption(&current).await?, Some(Disruption::Park(ref active)) if active == record)
                || current.head.disruption != Some(settlement.admission)
            {
                return Err(FleetCatalogError::Conflict);
            }
            self.commit_with_disruption(
                snapshot, &mutation, Some(settlement.admission), Authority::ManagedPark, || Ok(()),
            ).await
        })).await
    }

    pub(super) async fn confirm_managed_park(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
    ) -> Result<()> {
        let digest = hash(&encode(REQUEST_KIND, &record.mutation()?, REQUEST_LIMIT)?);
        if self
            .inner
            .committed(&snapshot.head, &hex::encode(record.request.id), digest)
            .await?
            .is_none()
        {
            return Err(FleetCatalogError::Corrupt("managed park journal"));
        }
        Ok(())
    }

    pub(super) async fn check_park_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if authority == Authority::ManagedPark {
            return Ok(());
        }
        if mutation.changes.iter().any(|change| {
            change.table == FleetTable::Operations
                && (change.key.starts_with(OPERATION_PREFIX)
                    || change.key.starts_with(SETTLEMENT_PREFIX))
        }) {
            return Err(FleetCatalogError::Invalid(
                "managed park operation authority",
            ));
        }
        if snapshot.head.disruption.is_some()
            && (matches!(
                authority,
                Authority::Topology | Authority::TopologyAdmission
            ) || mutation.changes.iter().any(|change| match change.table {
                FleetTable::Histories | FleetTable::Admissions => true,
                FleetTable::Policy => change.key == SLOT_KEY || change.key == namespaces::KEY,
                _ => false,
            }))
        {
            if let Some(Disruption::Park(record)) = self.active_disruption(snapshot).await? {
                return Err(FleetCatalogError::DisruptionInProgress {
                    operation: record.operation(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "managed_park_tests.rs"]
mod tests;
