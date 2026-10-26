// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Find an entire direct RF3 restoration assignment before returning a witness.
//! A failed search cannot authorize a partial capacity reduction.

use super::*;

pub(super) fn find(
    model: &Model<'_>,
    failed: usize,
    occupied: &[BTreeSet<usize>],
    base: &[Resources],
    work: &mut Work,
) -> Result<NodeRecovery, TopologyError> {
    let mut rows = Vec::new();
    for (group, voters) in model.desired.iter().enumerate() {
        if !voters.contains(&failed) {
            continue;
        }
        let mut candidates = Vec::new();
        for (node, profile) in model.view.nodes.iter().enumerate() {
            work.step()?;
            if node == failed || !profile.keep || occupied[group].contains(&node) {
                continue;
            }
            let mut trial = *voters;
            for v in &mut trial {
                if *v == failed {
                    *v = node;
                }
            }
            if model.domains(trial) {
                candidates.push(node);
            }
        }
        candidates.sort_by_key(|n| model.view.nodes[*n].ledger.scope.node.node);
        rows.push((group, candidates));
    }
    rows.sort_by(|(a, ac), (b, bc)| {
        ac.len()
            .cmp(&bc.len())
            .then_with(|| {
                model.groups[*b]
                    .resident
                    .durable_bytes
                    .cmp(&model.groups[*a].resident.durable_bytes)
            })
            .then_with(|| model.groups[*a].scope.cmp(&model.groups[*b].scope))
    });
    let mut desired = model.desired.clone();
    let mut growth = vec![Resources::default(); base.len()];
    let mut next = vec![0_usize];
    let mut chosen = Vec::<usize>::new();
    loop {
        let depth = chosen.len();
        if depth == rows.len() {
            if let Some(scheduled) =
                schedule::find(model, &model.desired, &desired, base, Some(failed), work)?
            {
                return Ok(NodeRecovery {
                    failed: model.view.nodes[failed].ledger.scope,
                    moves: scheduled.moves,
                    peaks: scheduled.peaks,
                });
            }
        }
        if depth == rows.len() || next[depth] == rows[depth].1.len() {
            next.pop();
            let Some(node) = chosen.pop() else {
                return Err(TopologyError::RecoveryUnproven(
                    model.view.nodes[failed].ledger.scope.node.node,
                ));
            };
            let group = rows[chosen.len()].0;
            desired[group] = model.desired[group];
            growth[node] = growth[node].subtract(model.groups[group].resident);
            continue;
        }
        work.step()?;
        let (group, candidates) = &rows[depth];
        let node = candidates[next[depth]];
        next[depth] += 1;
        let projected = add(growth[node], model.groups[*group].resident)?;
        if !model.fits(node, add(base[node], projected)?) {
            continue;
        }
        growth[node] = projected;
        desired[*group] = model.desired[*group];
        for n in &mut desired[*group] {
            if *n == failed {
                *n = node;
            }
        }
        chosen.push(node);
        next.push(0);
    }
}
