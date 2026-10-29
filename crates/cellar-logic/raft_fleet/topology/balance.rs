// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Generate an equal-copy layout using direct moves from surplus to deficit.
//! Retained source histories never fund either balancing or failure recovery.

use super::*;

const MAX_ACTIONS: usize = 1_000_000;

#[derive(Clone, Copy)]
struct Action {
    group: usize,
    source: usize,
    target: usize,
}

#[derive(Clone, Copy)]
enum Choice {
    Complete,
    Blocked,
    Target(usize),
}

struct Frame {
    choice: Choice,
    next: usize,
}

/// Generate a balanced RF3 assignment for the explicit final keep-set and verify
/// its complete staged path and every kept-node loss with `preview_topology`'s
/// verifier. Kept nodes receive either floor(3 * groups / kept) or ceil copies;
/// departing nodes receive zero. Existing copies on an excluded kept node may
/// remain, but neither balancing nor recovery may send it new copies.
///
/// Search first gives remainder copies to nodes with the most existing copies,
/// breaking ties by node ID. It tries alternate remainder allocations and move
/// choices when resources, domains or recovery headroom block the first choice.
/// Every move reduces a surplus and a deficit, so a successful result has the
/// minimum number of replacements for its chosen per-node counts. It does not
/// optimize workload, leadership, heterogeneous capacity shares or TPS.
///
/// This bounded search considers only direct surplus-to-deficit replacements,
/// without neutral reshuffles or temporary copies on intermediate nodes. A
/// `BalanceUnproven` result is not a global infeasibility proof. One work limit
/// covers candidate enumeration, all assignments, staged paths and recovery
/// searches; exhaustion returns no partial plan. Candidate storage is capped at
/// one million actions and search frames at three times the history count.
///
/// Like an explicit preview, this pure result grants no admission authority.
/// Adapters must authenticate a complete stable view, retain physical charges,
/// reserve each exact move and revalidate current quorum and eligibility.
pub fn balance_topology(
    view: &TopologyView,
    policy: TopologyPolicy,
    now_ms: u64,
) -> Result<TopologyPreview, TopologyError> {
    let mut model = Model::new(view, policy, None, now_ms)?;
    let mut work = Work {
        used: 0,
        limit: policy.max_work,
    };
    let mut counts = vec![0_usize; view.nodes.len()];
    for voters in &model.initial {
        for n in voters {
            counts[*n] += 1;
        }
    }
    let mut kept: Vec<_> = model
        .nodes
        .values()
        .copied()
        .filter(|n| view.nodes[*n].keep)
        .collect();
    if kept.is_empty() {
        return Err(TopologyError::InvalidInput);
    }
    // Prefer the remainder allocation that retains the most serving copies.
    // All alternatives are still searched when that first choice is blocked.
    kept.sort_by(|a, b| {
        counts[*b].cmp(&counts[*a]).then_with(|| {
            view.nodes[*a]
                .ledger
                .scope
                .node
                .node
                .cmp(&view.nodes[*b].ledger.scope.node.node)
        })
    });
    let copies = model.groups.len() * 3;
    let floor = copies / kept.len();
    let remainder = copies % kept.len();
    let mut extra: Vec<_> = (0..remainder).collect();
    loop {
        work.step()?;
        let mut quotas = vec![0; view.nodes.len()];
        for n in &kept {
            quotas[*n] = floor;
        }
        for i in &extra {
            quotas[kept[*i]] += 1;
        }
        if let Some(result) = assign(&mut model, &counts, &quotas, &mut work)? {
            return Ok(result);
        }
        if !next_combination(&mut extra, kept.len()) {
            return Err(TopologyError::BalanceUnproven);
        }
    }
}

fn next_combination(indices: &mut [usize], count: usize) -> bool {
    for i in (0..indices.len()).rev() {
        if indices[i] < count - indices.len() + i {
            indices[i] += 1;
            for j in i + 1..indices.len() {
                indices[j] = indices[j - 1] + 1;
            }
            return true;
        }
    }
    false
}

fn assign(
    model: &mut Model<'_>,
    initial_counts: &[usize],
    quotas: &[usize],
    work: &mut Work,
) -> Result<Option<TopologyPreview>, TopologyError> {
    let nodes: Vec<_> = model.nodes.values().copied().collect();
    let mut actions = Vec::new();
    for (group, voters) in model.initial.iter().enumerate() {
        for source in voters {
            if initial_counts[*source] <= quotas[*source] {
                continue;
            }
            for target in &nodes {
                work.step()?;
                if initial_counts[*target] >= quotas[*target]
                    || !model.view.nodes[*target].receives_replicas
                    || model.occupied[group].contains(target)
                {
                    continue;
                }
                if actions.len() == MAX_ACTIONS {
                    return Err(TopologyError::Limit);
                }
                actions.push(Action {
                    group,
                    source: *source,
                    target: *target,
                });
            }
        }
    }
    let mut counts = initial_counts.to_vec();
    let mut current = model.initial.clone();
    let mut growth = vec![Resources::default(); nodes.len()];
    let mut frames = vec![Frame {
        choice: choose(model, &actions, &counts, quotas, &current, &growth, work)?,
        next: 0,
    }];
    let mut chosen = Vec::<usize>::new();
    loop {
        let depth = chosen.len();
        let choice = frames[depth].choice;
        if matches!(choice, Choice::Complete) {
            model.desired = current.clone();
            match verify(model, work) {
                Ok(result) => return Ok(Some(result)),
                Err(TopologyError::NoStagedPath | TopologyError::RecoveryUnproven(_)) => (),
                Err(error) => return Err(error),
            }
        }
        if !matches!(choice, Choice::Target(_)) || frames[depth].next == actions.len() {
            frames.pop();
            let Some(index) = chosen.pop() else {
                return Ok(None);
            };
            let action = actions[index];
            replace(&mut current[action.group], action.target, action.source);
            growth[action.target] =
                growth[action.target].subtract(model.groups[action.group].resident);
            counts[action.source] += 1;
            counts[action.target] -= 1;
            continue;
        }
        let index = frames[depth].next;
        frames[depth].next += 1;
        work.step()?;
        let action = actions[index];
        if !matches!(choice, Choice::Target(target) if target == action.target)
            || counts[action.source] <= quotas[action.source]
            || !current[action.group].contains(&action.source)
            || current[action.group].contains(&action.target)
        {
            continue;
        }
        let mut trial = current[action.group];
        replace(&mut trial, action.source, action.target);
        if !model.domains(trial)
            || !model.fits(
                action.target,
                add(
                    add(model.base[action.target], growth[action.target])?,
                    model.groups[action.group].peak,
                )?,
            )
        {
            continue;
        }
        growth[action.target] = add(growth[action.target], model.groups[action.group].resident)?;
        current[action.group] = trial;
        counts[action.source] -= 1;
        counts[action.target] += 1;
        chosen.push(index);
        frames.push(Frame {
            choice: choose(model, &actions, &counts, quotas, &current, &growth, work)?,
            next: 0,
        });
    }
}

/// Count an upper bound of the remaining distinct histories at each deficit
/// node before exploring another branch. Domains may become legal after a
/// different copy leaves, so they must not reduce that upper bound. They only
/// determine which deficit can receive the next move now.
fn choose(
    model: &Model<'_>,
    actions: &[Action],
    counts: &[usize],
    quotas: &[usize],
    current: &[[usize; 3]],
    growth: &[Resources],
    work: &mut Work,
) -> Result<Choice, TopologyError> {
    let mut available = vec![0_usize; counts.len()];
    let mut last_group = vec![None; counts.len()];
    let mut legal = vec![false; counts.len()];
    for action in actions {
        work.step()?;
        if counts[action.target] >= quotas[action.target]
            || counts[action.source] <= quotas[action.source]
            || !current[action.group].contains(&action.source)
            || current[action.group].contains(&action.target)
            || !model.fits(
                action.target,
                add(
                    add(model.base[action.target], growth[action.target])?,
                    model.groups[action.group].peak,
                )?,
            )
        {
            continue;
        }
        if last_group[action.target] != Some(action.group) {
            last_group[action.target] = Some(action.group);
            available[action.target] += 1;
        }
        let mut trial = current[action.group];
        replace(&mut trial, action.source, action.target);
        legal[action.target] |= model.domains(trial);
    }
    let mut best = None;
    let mut complete = true;
    for n in model.nodes.values().copied() {
        let deficit = quotas[n].saturating_sub(counts[n]);
        if deficit == 0 {
            continue;
        }
        complete = false;
        if available[n] < deficit {
            return Ok(Choice::Blocked);
        }
        let slack = available[n] - deficit;
        if legal[n] && best.is_none_or(|(_, old_slack)| slack < old_slack) {
            best = Some((n, slack));
        }
    }
    Ok(if complete {
        Choice::Complete
    } else if let Some((target, _)) = best {
        Choice::Target(target)
    } else {
        Choice::Blocked
    })
}

fn replace(voters: &mut [usize; 3], source: usize, target: usize) {
    for n in voters {
        if *n == source {
            *n = target;
            return;
        }
    }
}
