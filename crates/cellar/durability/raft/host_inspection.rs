//! Capture bounded registered-history metadata through reserved control work.
use super::*;
use crate::raft::inspection::{
    CellInspection, InspectionSnapshot, MemberInspection, NodeInspection, ReplicaOccupancy,
    RESPONSE_RESERVATION,
};

pub(super) struct Completed {
    request: Box<Request>,
    started: u64,
    result: Result<GroupStatus, RuntimeError>,
}

impl Driver {
    pub(super) fn start_inspection(&mut self, request: Request) {
        if self.options.node_profile.is_none() {
            let mut request = request;
            reply(&mut request, Err(CommandFailure::UnsupportedCapability));
            return;
        }
        let scope = request.scope.clone();
        if self
            .cells
            .get(&scope)
            .is_some_and(|cell| !cell.failed && !cell.retired)
        {
            let runtime = self.runtime.clone();
            let domain = self.log.domain();
            self.jobs.push(Box::pin(async move {
                let (started, result) = match domain.mono_ms() {
                    Ok(started) => (started, runtime.status(&scope).await),
                    Err(_) => (0, Err(RuntimeError::Closed)),
                };
                Job::Inspection(Box::new(Completed {
                    request: Box::new(request),
                    started,
                    result,
                }))
            }));
        } else {
            self.reply_inspection(request, None);
        }
    }

    pub(super) fn finish_inspection(&mut self, completed: Completed) {
        let Completed {
            request,
            started,
            result,
        } = completed;
        let mut request = *request;
        if request.control.cancelled() {
            return;
        }
        match result {
            Ok(status) => self.reply_inspection(request, Some((started, status))),
            Err(error) => reply(
                &mut request,
                Err(match error {
                    RuntimeError::Overloaded | RuntimeError::NotReady => CommandFailure::Overloaded,
                    _ => CommandFailure::Unavailable,
                }),
            ),
        }
    }

    fn reply_inspection(&self, mut request: Request, status: Option<(u64, GroupStatus)>) {
        let Some(bytes) = self.control_budget.acquire(RESPONSE_RESERVATION) else {
            reply(&mut request, Err(CommandFailure::Overloaded));
            return;
        };
        let result = self
            .capture_inspection(&request.scope, status)
            .map(|snapshot| Response::Inspection(Box::new(snapshot)));
        reply_charged(&mut request, result, Some(bytes));
    }

    fn capture_inspection(
        &self,
        scope: &GroupScope,
        status: Option<(u64, GroupStatus)>,
    ) -> Result<InspectionSnapshot, CommandFailure> {
        let profile = self
            .options
            .node_profile
            .as_ref()
            .ok_or(CommandFailure::UnsupportedCapability)?
            .clone();
        let groups = self
            .log
            .group_count()
            .map_err(|_| CommandFailure::Unavailable)?
            .checked_add(usize::from(self.activating.is_some()))
            .ok_or(CommandFailure::Overloaded)?;
        let groups = u32::try_from(groups).map_err(|_| CommandFailure::Overloaded)?;
        let reserved_bytes = profile
            .bytes_per_group
            .checked_mul(u64::from(groups))
            .ok_or(CommandFailure::Overloaded)?;
        let local = self.log.limits();
        let node = NodeInspection {
            identity: NodeIdentity {
                node: local.node,
                disk: local.disk,
                process: local.process,
            },
            storage_instance: self
                .log
                .storage_instance()
                .map_err(|_| CommandFailure::Unavailable)?,
            profile,
            groups,
            reserved_bytes,
        };
        let mut snapshot = InspectionSnapshot {
            view: CellInspection {
                scope: scope.clone(),
                schema_digest: [0; 32],
                birth_digest: [0; 32],
                node,
                occupancy: ReplicaOccupancy::Failed,
                member: None,
                leader_load: None,
            },
            membership: None,
            configuration: None,
        };
        if let Some(cell) = self.cells.get(scope) {
            snapshot.view.schema_digest = cell.schema_digest;
            snapshot.view.birth_digest = cell.birth.digest();
            let member = cell.group.identity().member;
            let cache = cell.group.read().map_err(|_| CommandFailure::Unavailable)?;
            if cell.failed {
                return Ok(snapshot);
            }
            if cell.retired
                || !crate::raft::member_state::members(&cache.conf_state).contains(&member.get())
            {
                snapshot.view.occupancy = ReplicaOccupancy::Retired(member);
                return Ok(snapshot);
            }
            let (started, status) = status.ok_or(CommandFailure::Overloaded)?;
            if status.replica != *cell.group.identity()
                || status.term.get() != cache.hard_state.term
                || status.membership_revision != cache.membership.revision()
                || status.committed.get() != cache.hard_state.commit
                || status.applied != cache.applied.position
            {
                return Err(CommandFailure::Overloaded);
            }
            let published = cell.image.as_ref().map(|image| image.position);
            if published.is_some_and(|position| {
                position.index > cache.applied.position.index
                    || cache.term(position.index.get()) != Some(position.term.get())
            }) {
                return Err(CommandFailure::Overloaded);
            }
            let sample_age_ms = self
                .log
                .domain()
                .mono_ms()
                .map_err(|_| CommandFailure::Unavailable)?
                .checked_sub(started)
                .ok_or(CommandFailure::Unavailable)?;
            snapshot.view.occupancy = ReplicaOccupancy::Member(member);
            snapshot.view.member = Some(MemberInspection {
                member,
                term: status.term,
                leader: status.leader.and_then(|id| MemberId::new(id).ok()),
                is_leader: status.is_leader,
                membership_revision: cache.membership.revision(),
                member_high_water: cache.membership.high_water(),
                committed: status.committed,
                applied: status.applied,
                published,
                repair: cache
                    .membership
                    .repair_status()
                    .map_err(|_| CommandFailure::CorruptState)?,
                sample_age_ms,
            });
            snapshot.membership = Some(cache.membership.clone());
            snapshot.configuration = Some(cache.conf_state.clone());
            return Ok(snapshot);
        }
        if let Some(dormant) = self.standbys.get(scope) {
            snapshot.view.schema_digest = dormant.class.schema_digest();
            snapshot.view.birth_digest = dormant.birth.digest();
            snapshot.view.occupancy = if dormant.failed {
                ReplicaOccupancy::Failed
            } else if self.activating.as_ref() == Some(scope) {
                ReplicaOccupancy::Initializing
            } else {
                match self.log.group(scope) {
                    Ok(group) => ReplicaOccupancy::Occupied(group.identity().member),
                    Err(super::super::storage::LogError::NotFound) => ReplicaOccupancy::Standby,
                    Err(_) => return Err(CommandFailure::Unavailable),
                }
            };
            return Ok(snapshot);
        }
        Err(CommandFailure::UnsupportedCapability)
    }
}
