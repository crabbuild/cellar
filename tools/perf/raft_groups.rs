// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A frozen population is partitioned by complete canonical routing keys. Cell
//! identity, physical group identity and placement remain separate quantities.
use super::*;
use cellar_logic::cell_group::CellKey;
use sha2::{Digest, Sha256};

pub const DEFAULT_OCCUPANCY: usize = 32;
pub const MAX_OCCUPANCY: usize = 128;

pub struct Group {
    pub history: GroupHistory,
    pub range: CellRange,
    pub cells: usize,
}

pub struct Layout {
    pub total: usize,
    pub occupancy: usize,
    pub class: String,
    pub generation: u8,
    pub groups: Vec<Group>,
    assignment: Vec<usize>,
    ordered_cells: Vec<usize>,
}

impl Layout {
    pub fn new(total: usize, occupancy: usize, class: &str, generation: u8) -> Result<Self> {
        ensure!((1..=1_000_000).contains(&total), "invalid cell population");
        ensure!(
            (1..=MAX_OCCUPANCY).contains(&occupancy),
            "invalid group occupancy"
        );
        ensure!(
            matches!(class, "Counter" | "Workload") && generation <= 1,
            "invalid population history"
        );
        let namespace = NamespaceId::new([2; 16])?;
        let mut keys = (0..total)
            .map(|ordinal| -> Result<_> {
                let cell = CellId::new(format!("{class}:{ordinal}"))?;
                let digest = Sha256::digest(cell.routing_preimage(namespace)).into();
                Ok((CellKey::from_digest(digest, cell), ordinal))
            })
            .collect::<Result<Vec<_>>>()?;
        keys.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut assignment = vec![0; total];
        let mut groups = Vec::new();
        for (ordinal, chunk) in keys.chunks(occupancy).enumerate() {
            for (_, cell) in chunk {
                assignment[*cell] = ordinal;
            }
            let lower = if ordinal == 0 {
                CellBound::NegativeInfinity
            } else {
                CellBound::Key(chunk[0].0.clone())
            };
            let upper = keys
                .get((ordinal + 1) * occupancy)
                .map_or(CellBound::PositiveInfinity, |(key, _)| {
                    CellBound::Key(key.clone())
                });
            groups.push(Group {
                history: Self::physical_history(ordinal, generation)?,
                range: CellRange::new(lower, upper)?,
                cells: chunk.len(),
            });
        }
        Ok(Self {
            total,
            occupancy,
            class: class.into(),
            generation,
            groups,
            assignment,
            ordered_cells: keys.into_iter().map(|(_, cell)| cell).collect(),
        })
    }

    pub fn physical_history(ordinal: usize, generation: u8) -> Result<GroupHistory> {
        ensure!(
            ordinal < 1_000_000 && generation <= 1,
            "invalid physical group"
        );
        let mut id = [5 + generation; 16];
        id[..8].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
        Ok(GroupHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            group: GroupId::new(id)?,
            incarnation: IncarnationId::new([4 + generation; 16])?,
        })
    }

    pub fn history(&self, cell: usize) -> Result<CellHistory> {
        ensure!(cell < self.total, "cell outside configured population");
        Ok(CellHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: CellId::new(format!("{}:{cell}", self.class))?,
            incarnation: IncarnationId::new([4 + self.generation; 16])?,
        })
    }

    pub fn named_history(&self, name: &str) -> Result<CellHistory> {
        let ordinal: usize = name.parse()?;
        ensure!(ordinal.to_string() == name, "noncanonical cell ordinal");
        self.history(ordinal)
    }

    pub fn group_ordinal(&self, cell: usize) -> Result<usize> {
        self.assignment
            .get(cell)
            .copied()
            .context("cell outside configured population")
    }

    pub fn group(&self, cell: usize) -> Result<&Group> {
        Ok(&self.groups[self.group_ordinal(cell)?])
    }

    /// Canonical cells in this range, without rescanning the full population.
    pub fn cells_in_group(&self, ordinal: usize) -> Result<&[usize]> {
        let group = self
            .groups
            .get(ordinal)
            .context("group outside configured population")?;
        let start = ordinal * self.occupancy;
        Ok(&self.ordered_cells[start..start + group.cells])
    }

    pub fn spec(&self, ordinal: usize, voters: Vec<InitialMember>) -> Result<CellGroupBirthSpec> {
        let group = self
            .groups
            .get(ordinal)
            .context("group outside configured population")?;
        let mut creation = [81 + self.generation; 16];
        creation[..8].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
        Ok(CellGroupBirthSpec {
            history: group.history,
            range: group.range.clone(),
            epoch: RouteEpoch::new(u64::from(self.generation) + 1)?,
            creation: ProposalId::new(creation)?,
            voters,
            learners: vec![],
        })
    }

    pub fn enrollment(&self, cell: usize) -> Result<CellEnrollmentRequest> {
        let history = self.history(cell)?;
        let mut digest = Sha256::new();
        digest.update(b"cellar-perf-cell-enrollment-1\0");
        digest.update(history.cell.routing_preimage(history.namespace));
        digest.update(history.incarnation.bytes());
        Ok(CellEnrollmentRequest {
            creation: ProposalId::new(digest.finalize()[..16].try_into()?)?,
            durability: Durability::Quorum,
        })
    }

    pub fn json(&self, placement: Placement) -> serde_json::Value {
        serde_json::json!({"cells":self.total, "cells_per_group":self.occupancy,
            "groups":self.groups.iter().enumerate().map(|(ordinal,group)| serde_json::json!({
                "ordinal":ordinal, "group":hex::encode(group.history.group.bytes()),
                "incarnation":hex::encode(group.history.incarnation.bytes()), "cells":group.cells,
                "voters":placement.voters(ordinal)
            })).collect::<Vec<_>>(), "cell_groups":self.assignment})
    }
}

#[cfg(test)]
#[path = "raft_groups_tests.rs"]
mod tests;
