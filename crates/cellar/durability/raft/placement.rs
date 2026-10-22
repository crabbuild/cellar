//! Observations choose ordinary guarded operations. They never install state,
//! manufacture a member or bypass the runtime's configuration/progress checks.
use super::*;
use crate::raft::inspection::{CellInspection, ReplicaOccupancy};
use crate::raft::member_state::JoiningMember;
use cellar_logic::raft_leadership::{self, LeaderSample, LeaderView};
use cellar_logic::raft_placement::{self, NodeCapacity, NodeHealth, PlacedMember, PlacementView};

pub(super) struct Context<'a> {
    pub cells: &'a BTreeMap<GroupScope, Cell>,
    pub peers: &'a BTreeMap<NodeId, RaftPeerNode>,
    pub local: NodeId,
    pub rng: &'a mut crate::asyncrt::rng::Stream,
}

struct Seen<'a> {
    view: &'a CellInspection,
    sampled: u64,
    load_sampled: Option<u64>,
}

fn sampled(started: u64, view: &CellInspection) -> Option<Seen<'_>> {
    let sampled = started.checked_sub(
        view.member
            .as_ref()
            .map_or(0, |member| member.sample_age_ms),
    )?;
    let load_sampled = view
        .leader_load
        .as_ref()
        .and_then(|load| started.checked_sub(load.sample_age_ms))
        .map(|time| time.min(sampled));
    Some(Seen {
        view,
        sampled,
        load_sampled,
    })
}

fn placed(
    id: u64,
    ledger: &crate::raft::member_state::GroupMembership,
    birth: &GroupBirth,
    peers: &BTreeMap<NodeId, RaftPeerNode>,
) -> Result<PlacedMember, CommandFailure> {
    let member = MemberId::new(id).map_err(|_| CommandFailure::CorruptState)?;
    if let Some((binding, _)) = ledger
        .joined(member)
        .map_err(|_| CommandFailure::CorruptState)?
    {
        return Ok(PlacedMember {
            member,
            node: binding.node,
            disk: binding.disk,
        });
    }
    let node = birth
        .voters()
        .iter()
        .chain(birth.learners())
        .find(|initial| initial.member == member)
        .ok_or(CommandFailure::CorruptState)?
        .node;
    let peer = peers.get(&node).ok_or(CommandFailure::Unavailable)?;
    Ok(PlacedMember {
        member,
        node,
        disk: peer.disk,
    })
}

fn failed(history: &mut History, peer: &RaftPeerNode, started: u64, now: u64, grace: u64) -> bool {
    let first = history
        .failures
        .get(&peer.node)
        .is_none_or(|old| old.peer != *peer);
    if first {
        history.failures.insert(
            peer.node,
            FailedProbe {
                peer: peer.clone(),
                since: started,
                last: started,
            },
        );
        return false;
    }
    let Some(state) = history.failures.get_mut(&peer.node) else {
        return false;
    };
    let repeated = state.last < started;
    state.last = started;
    repeated
        && now
            .checked_sub(state.since)
            .is_some_and(|elapsed| elapsed >= grace)
}

fn unavailable(binding: &PlacedMember, started: u64) -> NodeCapacity {
    // These labels describe no observed domain and are never a diversity
    // proof. The pure selector drops the victim before checking the final
    // healthy remaining voters plus a fresh replacement candidate.
    NodeCapacity {
        node: binding.node,
        disk: binding.disk,
        pool: "unknown".into(),
        region: "unknown".into(),
        zone: "unknown".into(),
        sampled_ms: started,
        health: NodeHealth::Unavailable,
        compatible: false,
        draining: false,
        groups: 0,
        group_capacity: 0,
        bytes: 0,
        byte_capacity: 0,
    }
}

fn capacity(sample: &Seen<'_>, bindings: &[PlacedMember], required: u64) -> NodeCapacity {
    let view = sample.view;
    let profile = &view.node.profile;
    let matching = bindings.iter().find(|binding| {
        binding.node == view.node.identity.node && binding.disk == view.node.identity.disk
    });
    let compatible = profile.bytes_per_group == required
        && match (matching, view.occupancy) {
            (Some(binding), ReplicaOccupancy::Member(member)) => binding.member == member,
            (None, ReplicaOccupancy::Standby) => true,
            _ => false,
        };
    NodeCapacity {
        node: view.node.identity.node,
        disk: view.node.identity.disk,
        pool: profile.pool.clone(),
        region: profile.region.clone(),
        zone: profile.zone.clone(),
        sampled_ms: sample.sampled,
        health: NodeHealth::Healthy,
        compatible,
        draining: profile.draining,
        groups: view.node.groups,
        group_capacity: profile.group_capacity,
        bytes: view.node.reserved_bytes,
        byte_capacity: profile.byte_capacity,
    }
}

fn activation(
    observed: &probe::Observed,
    options: &RaftControllerOptions,
    now: u64,
    peers: &BTreeMap<NodeId, RaftPeerNode>,
) -> Option<action::Planned> {
    let local = &observed.local.view;
    if !matches!(
        local.occupancy,
        ReplicaOccupancy::Standby | ReplicaOccupancy::Occupied(_)
    ) {
        return None;
    }
    for probe in &observed.peers {
        if peers.get(&probe.peer.node) != Some(&probe.peer) {
            continue;
        }
        let Ok(remote) = &probe.result else {
            continue;
        };
        let Some(source) = remote.view.member.as_ref() else {
            continue;
        };
        if !source.is_leader {
            continue;
        }
        let Some(repair) = source.repair.as_ref() else {
            continue;
        };
        if repair.phase != RepairPhase::Seeding
            || repair.plan.replacement.node != local.node.identity.node
            || repair.plan.replacement.disk != local.node.identity.disk
            || matches!(local.occupancy, ReplicaOccupancy::Occupied(member) if member != repair.plan.replacement.member)
            || !probe
                .started
                .checked_sub(source.sample_age_ms)
                .and_then(|time| now.checked_sub(time))
                .is_some_and(|age| age <= options.placement.max_sample_age_ms)
        {
            continue;
        }
        return Some(action::Planned::Activate(client::LearnerActivation {
            member: repair.plan.replacement.member,
            source: source.member,
            source_node: remote.view.node.identity.node,
            from_s3: false,
            timeout: std::time::Duration::from_millis(options.seed_timeout_ms),
        }));
    }
    None
}

pub(super) fn decide(
    controller: &mut Controller,
    index: usize,
    observed: probe::Observed,
    now: u64,
    context: Context<'_>,
) -> Result<Option<action::Planned>, CommandFailure> {
    let scope = controller
        .histories
        .get(index)
        .ok_or(CommandFailure::Unavailable)?
        .binding
        .scope
        .clone();
    if observed.local.view.scope != scope || observed.local.view.node.identity.node != context.local
    {
        return Err(CommandFailure::CorruptState);
    }
    let options = controller.options.clone();
    let cell = context.cells.get(&scope);
    let durable = cell
        .map(|cell| {
            let cache = cell
                .group
                .read()
                .map_err(|_| CommandFailure::CorruptState)?;
            Ok::<_, CommandFailure>((
                cache.membership.revision(),
                cache
                    .membership
                    .repair_status()
                    .map_err(|_| CommandFailure::CorruptState)?,
            ))
        })
        .transpose()?;
    if let Some((revision, actual)) = &durable {
        if actual.as_ref().is_some_and(|repair| repair.phase.active()) {
            controller.active.insert(index);
        } else {
            controller.active.remove(&index);
        }
        if let Some((pending_index, pending)) = controller.intent.as_ref() {
            if *pending_index == index
                && (actual
                    .as_ref()
                    .is_some_and(|actual| actual.plan.id == pending.id && !actual.phase.active())
                    || (*revision > pending.expected_revision
                        && actual
                            .as_ref()
                            .is_none_or(|actual| actual.plan.id != pending.id)))
            {
                controller.intent = None;
            }
        }
        if let Some(actual) = actual
            .as_ref()
            .filter(|actual| actual.phase == RepairPhase::Complete)
        {
            let history = controller
                .histories
                .get_mut(index)
                .ok_or(CommandFailure::Unavailable)?;
            if history.last_completed != Some(actual.plan.id) {
                history.last_completed = Some(actual.plan.id);
                controller.status.completed_repairs = controller
                    .status
                    .completed_repairs
                    .checked_add(1)
                    .ok_or(CommandFailure::Unavailable)?;
            }
        }
    }
    let leader = observed
        .local
        .view
        .member
        .as_ref()
        .is_some_and(|member| member.is_leader);
    let Some(member) = observed.local.view.member.as_ref().filter(|_| leader) else {
        // Another leader owns a follower's plan. Keep an ambiguous plan we
        // proposed reserved, but do not let passive (possibly retired) mirrors
        // prevent coordination for unrelated local leaders indefinitely.
        if controller
            .intent
            .as_ref()
            .is_none_or(|(pending, _)| *pending != index)
        {
            controller.active.remove(&index);
        }
        if let Some(history) = controller.history(index, now) {
            history.leader_term = None;
        }
        return Ok(activation(&observed, &options, now, context.peers));
    };
    let Some(cell) = cell else {
        return Err(CommandFailure::CorruptState);
    };
    let Some(Ok(progress)) = observed.progress.as_ref() else {
        return Ok(None);
    };
    let ledger = observed
        .local
        .membership
        .as_ref()
        .ok_or(CommandFailure::CorruptState)?;
    let conf = observed
        .local
        .configuration
        .as_ref()
        .ok_or(CommandFailure::CorruptState)?;
    let empty = {
        let cache = cell
            .group
            .read()
            .map_err(|_| CommandFailure::CorruptState)?;
        if cell.failed
            || cell.retired
            || cache.hard_state.term != member.term.get()
            || cache.membership.revision() != member.membership_revision
            || cache.hard_state.commit != member.committed.get()
            || cache.applied.position != member.applied
            || progress.replica != *cell.group.identity()
            || progress.term != member.term
            || progress.membership_revision != member.membership_revision
            || progress.committed != member.committed
        {
            return Ok(None);
        }
        cache.applied.image.is_none()
    };
    let voters = conf
        .voters
        .iter()
        .map(|id| placed(*id, ledger, &cell.birth, context.peers))
        .collect::<Result<Vec<_>, _>>()?;
    let learners = conf
        .learners
        .iter()
        .map(|id| placed(*id, ledger, &cell.birth, context.peers))
        .collect::<Result<Vec<_>, _>>()?;
    let bindings = crate::raft::member_state::members(conf)
        .into_iter()
        .map(|id| placed(id, ledger, &cell.birth, context.peers))
        .collect::<Result<Vec<_>, _>>()?;
    let history = controller
        .history(index, now)
        .ok_or(CommandFailure::Unavailable)?;
    if history.leader_term != Some(member.term) {
        history.leader_term = Some(member.term);
        history.gained_at = now;
    }
    let gained_at = history.gained_at;
    history
        .failures
        .retain(|node, _| bindings.iter().any(|binding| binding.node == *node));
    let mut seen = BTreeMap::new();
    if let Some(local) = sampled(observed.started, &observed.local.view) {
        seen.insert(context.local, local);
    }
    let mut failed_nodes = Vec::new();
    for probe in &observed.peers {
        if context.peers.get(&probe.peer.node) != Some(&probe.peer) {
            history.failures.remove(&probe.peer.node);
            continue;
        }
        let failed_scope = matches!(&probe.result, Ok(snapshot) if snapshot.view.occupancy == ReplicaOccupancy::Failed);
        if matches!(&probe.result, Err(InspectionError::Unreachable)) || failed_scope {
            if let Some(binding) = bindings
                .iter()
                .find(|binding| binding.node == probe.peer.node && binding.disk == probe.peer.disk)
            {
                if failed(
                    history,
                    &probe.peer,
                    probe.started,
                    now,
                    options.unavailable_after_ms,
                ) {
                    failed_nodes.push(unavailable(binding, probe.started));
                }
            }
            continue;
        }
        history.failures.remove(&probe.peer.node);
        if let Ok(snapshot) = &probe.result {
            if let Some(sample) = sampled(probe.started, &snapshot.view) {
                seen.insert(probe.peer.node, sample);
            }
        }
    }
    let repair = member.repair.as_ref();
    if let Some(repair) = repair.filter(|repair| repair.phase.active()) {
        if repair.phase == RepairPhase::Joint {
            return Ok(Some(action::Planned::Advance {
                id: repair.plan.id,
                step: RepairStep::FinishJoint,
                revision: member.membership_revision,
            }));
        }
        let elapsed = match history.seeding {
            Some((id, started)) if id == repair.plan.id => now.checked_sub(started),
            _ => {
                history.seeding = Some((repair.plan.id, now));
                Some(0)
            }
        };
        let target = &repair.plan.replacement;
        let ready = seen.get(&target.node).is_some_and(|sample| {
            sample.view.node.identity.disk == target.disk
                && sample.view.occupancy == ReplicaOccupancy::Member(target.member)
                && now
                    .checked_sub(sample.sampled)
                    .is_some_and(|age| age <= options.placement.max_sample_age_ms)
                && sample.view.member.as_ref().is_some_and(|sample| {
                    sample.applied.index >= member.committed
                        && (sample
                            .published
                            .is_some_and(|cut| cut.index >= member.committed)
                            || (empty && sample.published.is_none()))
                })
                && progress.members.iter().any(|actual| {
                    actual.member == target.member
                        && !actual.voter
                        && actual.recent_active
                        && actual.matched >= member.committed
                })
        });
        if ready {
            return Ok(Some(action::Planned::Advance {
                id: repair.plan.id,
                step: RepairStep::PromoteAndRemove,
                revision: member.membership_revision,
            }));
        }
        if failed_nodes.iter().any(|node| node.node == target.node)
            || elapsed.is_some_and(|elapsed| elapsed >= options.learner_timeout_ms)
        {
            return Ok(Some(action::Planned::Advance {
                id: repair.plan.id,
                step: RepairStep::AbortLearner,
                revision: member.membership_revision,
            }));
        }
        return Ok(None);
    }
    history.seeding = None;
    if let Some((pending_index, pending)) = controller.intent.as_ref() {
        return Ok((*pending_index == index).then(|| action::Planned::Begin(pending.clone())));
    }
    let nodes = seen
        .values()
        .map(|sample| capacity(sample, &bindings, options.placement.required_bytes))
        .chain(failed_nodes)
        .collect::<Vec<_>>();
    let placement = PlacementView {
        scope: scope.clone(),
        revision: member.membership_revision,
        high_water: member.member_high_water,
        voters: voters.clone(),
        learners,
        joint: !conf.voters_outgoing.is_empty(),
        repairing: !conf.learners.is_empty(),
    };
    if controller.active.is_empty() {
        if let Ok(Some(placement)) =
            raft_placement::select_repair(&placement, &nodes, &options.placement, now)
        {
            let mut bytes = [0; 16];
            context.rng.fill_bytes(&mut bytes);
            let id = ProposalId::new(bytes).map_err(|_| CommandFailure::Unavailable)?;
            let plan = RepairPlan {
                id,
                policy_revision: placement.policy_revision,
                expected_revision: placement.expected_revision,
                obsolete: placement.obsolete,
                replacement: JoiningMember {
                    member: placement.replacement.member,
                    node: placement.replacement.node,
                    disk: placement.replacement.disk,
                },
            };
            controller.intent = Some((index, plan.clone()));
            controller.active.insert(index);
            return Ok(Some(action::Planned::Begin(plan)));
        }
    }
    let Some(policy) = options.leadership else {
        return Ok(None);
    };
    let last_transfer = controller.last_transfer.max(gained_at);
    let samples = voters
        .iter()
        .filter_map(|binding| {
            let sample = seen.get(&binding.node)?;
            if sample.view.occupancy != ReplicaOccupancy::Member(binding.member)
                || sample.view.node.identity.disk != binding.disk
            {
                return None;
            }
            let load = sample.view.leader_load.as_ref()?;
            let progress = progress.members.iter().find(|progress| {
                progress.member == binding.member
                    && progress.voter
                    && (progress.member == member.member || progress.recent_active)
            })?;
            Some(LeaderSample {
                binding: binding.clone(),
                sampled_ms: sample.load_sampled?,
                health: NodeHealth::Healthy,
                compatible: sample.view.node.profile.bytes_per_group
                    == options.placement.required_bytes,
                draining: sample.view.node.profile.draining,
                leaders: load.leaders,
                leader_capacity: load.capacity,
                matched: progress.matched,
                applied: match sample.view.member.as_ref()?.published {
                    Some(published) => published.index,
                    None if empty => sample.view.member.as_ref()?.applied.index,
                    None => return None,
                },
            })
        })
        .collect::<Vec<_>>();
    let view = LeaderView {
        scope,
        term: member.term,
        revision: member.membership_revision,
        current: member.member,
        committed: member.committed,
        voters,
        joint: placement.joint,
        repairing: placement.repairing,
        last_transfer_ms: last_transfer,
    };
    Ok(raft_leadership::select_leader(&view, &samples, policy, now)
        .map_err(|_| CommandFailure::Unavailable)?
        .map(|movement| {
            action::Planned::Transfer(crate::raft::completion::LeadershipTransfer {
                target: movement.target.member,
                expected_term: movement.expected_term,
                expected_revision: movement.expected_revision,
            })
        }))
}
