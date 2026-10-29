// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded iterative search over direct replacements. Source occupancy never
//! decreases, including while backtracking between alternative schedules.

use super::*;

#[derive(Clone, Copy)]
struct Action {
    group: usize,
    source: usize,
    target: usize,
}

pub(super) struct Scheduled {
    pub moves: Vec<ReplicaMove>,
    pub peaks: Vec<TopologyPeak>,
    pub retained: Vec<Resources>,
}

pub(super) fn find(
    model: &Model<'_>,
    initial: &[[usize; 3]],
    desired: &[[usize; 3]],
    base: &[Resources],
    failed: Option<usize>,
    work: &mut Work,
) -> Result<Option<Scheduled>, TopologyError> {
    let mut actions = Vec::new();
    let mut required = 0;
    for (group, (old, new)) in initial.iter().zip(desired).enumerate() {
        let incoming: Vec<_> = new.iter().filter(|n| !old.contains(n)).copied().collect();
        required += incoming.len();
        for source in old.iter().filter(|n| !new.contains(n)) {
            for target in &incoming {
                actions.push(Action {
                    group,
                    source: *source,
                    target: *target,
                });
            }
        }
    }
    actions.sort_by(|a, b| {
        let size = |a: &Action| {
            let p = model.groups[a.group].peak;
            (p.durable_bytes, p.memory_bytes, p.engine_slots)
        };
        size(b)
            .cmp(&size(a))
            .then_with(|| {
                model.groups[a.group]
                    .scope
                    .cmp(&model.groups[b.group].scope)
            })
            .then_with(|| {
                model.view.nodes[a.source]
                    .ledger
                    .scope
                    .node
                    .node
                    .cmp(&model.view.nodes[b.source].ledger.scope.node.node)
            })
            .then_with(|| {
                model.view.nodes[a.target]
                    .ledger
                    .scope
                    .node
                    .node
                    .cmp(&model.view.nodes[b.target].ledger.scope.node.node)
            })
    });
    // Explicit frames keep the stack bounded independently of the cell count.
    let mut current = initial.to_vec();
    let mut growth = vec![Resources::default(); base.len()];
    let mut next = vec![0_usize];
    let mut chosen = Vec::new();
    loop {
        if chosen.len() == required {
            return Ok(Some(materialize(model, base, &actions, &chosen)?));
        }
        let depth = chosen.len();
        if next[depth] == actions.len() {
            next.pop();
            let Some(index) = chosen.pop() else {
                return Ok(None);
            };
            let action: Action = actions[index];
            replace(&mut current[action.group], action.target, action.source);
            growth[action.target] =
                growth[action.target].subtract(model.groups[action.group].resident);
            continue;
        }
        let index = next[depth];
        next[depth] += 1;
        work.step()?;
        let a = actions[index];
        let old = current[a.group];
        if !old.contains(&a.source) || old.contains(&a.target) {
            continue;
        }
        let mut new = old;
        replace(&mut new, a.source, a.target);
        if !model.domains(new)
            || old.iter().filter(|n| Some(**n) != failed).count() < 2
            || new.iter().filter(|n| Some(**n) != failed).count() < 2
            || !model.fits(
                a.target,
                add(
                    add(base[a.target], growth[a.target])?,
                    model.groups[a.group].peak,
                )?,
            )
        {
            continue;
        }
        growth[a.target] = add(growth[a.target], model.groups[a.group].resident)?;
        current[a.group] = new;
        chosen.push(index);
        next.push(0);
    }
}

fn replace(voters: &mut [usize; 3], source: usize, target: usize) {
    for n in voters {
        if *n == source {
            *n = target;
            return;
        }
    }
}

fn materialize(
    model: &Model<'_>,
    base: &[Resources],
    actions: &[Action],
    chosen: &[usize],
) -> Result<Scheduled, TopologyError> {
    let mut retained = base.to_vec();
    let mut peaks = base.to_vec();
    let mut moves = Vec::new();
    for (i, index) in chosen.iter().enumerate() {
        let a = actions[*index];
        peaks[a.target] =
            peaks[a.target].high_water(add(retained[a.target], model.groups[a.group].peak)?);
        retained[a.target] = add(retained[a.target], model.groups[a.group].resident)?;
        moves.push(ReplicaMove {
            group: model.groups[a.group].scope.clone(),
            source: model.view.nodes[a.source].ledger.scope,
            target: model.view.nodes[a.target].ledger.scope,
            after: i.checked_sub(1),
        });
    }
    let mut peaks: Vec<_> = peaks
        .into_iter()
        .enumerate()
        .map(|(n, resources)| TopologyPeak {
            scope: model.view.nodes[n].ledger.scope,
            resources,
        })
        .collect();
    peaks.sort_by_key(|p| p.scope.node.node);
    Ok(Scheduled {
        moves,
        peaks,
        retained,
    })
}
