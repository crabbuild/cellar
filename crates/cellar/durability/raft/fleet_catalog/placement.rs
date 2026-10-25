//! The rooted separation contract is independent of advisory deployment labels.
use serde::{Deserialize, Serialize};

/// Required separation for each serving copy and newly admitted learner.
/// `Nodes` is useful for an explicitly colocated development fleet; it grants
/// no independence from loss of a shared physical host.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FleetReplicaSeparation {
    Nodes,
    #[default]
    PhysicalHosts,
}
