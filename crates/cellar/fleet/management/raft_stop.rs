//! Bounded stop observations, without per-group identities or deletion authority.
use crate::application::consensus::RaftNodeShutdownReport;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

/// Required even for a Fleet-only node. Missing data cannot mean no host.
/// These observations do not certify deployment fencing or S3 recoverability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum RaftStopProof {
    NotRegistered,
    Closed {
        startup_incomplete: bool,
        owners: RaftStopOwners,
        handoff: RaftStopHandoff,
    },
    Failed {
        startup_incomplete: bool,
    },
}

/// Exact physical engine identity and complete aggregate counts. Archive lag
/// and unknown parked coverage remain obligations after a complete local stop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftStopOwners {
    pub node: [u8; 16],
    pub disk: [u8; 16],
    pub process: [u8; 16],
    pub engine: [u8; 16],
    pub groups: usize,
    pub omitted_group_details: usize,
    pub groups_with_archive_lag: usize,
    pub groups_with_unknown_archive: usize,
    pub unfinished_commands: usize,
}

/// Accepted transfer requests and observed old-leader releases are separate.
/// Neither establishes a target election or a fence against an old deployment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum RaftStopHandoff {
    Missing,
    Failed,
    Observed {
        groups_examined: usize,
        attempts: usize,
        accepted: usize,
        leadership_released: usize,
        unavailable_groups: usize,
        deadline_elapsed: bool,
    },
}

impl From<&RaftNodeShutdownReport> for RaftStopProof {
    fn from(report: &RaftNodeShutdownReport) -> Self {
        let Ok(owners) = &report.result else {
            return Self::Failed {
                startup_incomplete: report.startup_incomplete,
            };
        };
        let handoff = match &report.drain {
            None => RaftStopHandoff::Missing,
            Some(Err(_)) => RaftStopHandoff::Failed,
            Some(Ok(drain)) => RaftStopHandoff::Observed {
                groups_examined: drain.groups_examined,
                attempts: drain.handoff_attempts,
                accepted: drain.handoffs_accepted,
                leadership_released: drain.leadership_released,
                unavailable_groups: drain.unavailable_groups,
                deadline_elapsed: drain.deadline_elapsed,
            },
        };
        Self::Closed {
            startup_incomplete: report.startup_incomplete,
            owners: RaftStopOwners {
                node: owners.node.node.bytes(),
                disk: owners.node.disk.bytes(),
                process: owners.node.process.bytes(),
                engine: owners.storage_instance.bytes(),
                groups: owners.groups_total,
                omitted_group_details: owners.omitted_groups,
                groups_with_archive_lag: owners.groups_with_archive_lag,
                groups_with_unknown_archive: owners.groups_with_unknown_archive,
                unfinished_commands: owners.unfinished.len(),
            },
            handoff,
        }
    }
}

impl RaftStopProof {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Self::Closed {
            owners, handoff, ..
        } = self
        {
            ensure!(
                [owners.node, owners.disk, owners.process, owners.engine]
                    .into_iter()
                    .all(|id| id != [0; 16]),
                "invalid Raft stop engine identity"
            );
            ensure!(
                owners.omitted_group_details <= owners.groups
                    && owners.groups_with_unknown_archive <= owners.omitted_group_details
                    && owners.unfinished_commands
                        <= owners
                            .groups
                            .saturating_sub(owners.groups_with_unknown_archive)
                    && owners
                        .groups_with_archive_lag
                        .checked_add(owners.groups_with_unknown_archive)
                        .is_some_and(|count| count <= owners.groups),
                "inconsistent Raft stop group counts"
            );
            if let RaftStopHandoff::Observed {
                groups_examined,
                attempts,
                accepted,
                leadership_released,
                unavailable_groups,
                ..
            } = handoff
            {
                ensure!(
                    *groups_examined
                        <= owners
                            .groups
                            .saturating_sub(owners.groups_with_unknown_archive)
                        && *unavailable_groups <= *groups_examined
                        && *accepted <= *groups_examined
                        && *accepted <= *attempts
                        && *leadership_released <= *accepted
                        && groups_examined
                            .checked_mul(8)
                            .is_some_and(|bound| *attempts <= bound),
                    "inconsistent Raft stop handoff counts"
                );
            }
        }
        Ok(())
    }

    pub(crate) fn complete(&self) -> bool {
        self.validate().is_ok()
            && match self {
                Self::NotRegistered => true,
                Self::Closed {
                    startup_incomplete: false,
                    owners,
                    handoff:
                        RaftStopHandoff::Observed {
                            deadline_elapsed: false,
                            ..
                        },
                } => owners.unfinished_commands == 0,
                _ => false,
            }
    }
}

#[cfg(test)]
#[path = "raft_stop_tests.rs"]
mod tests;
