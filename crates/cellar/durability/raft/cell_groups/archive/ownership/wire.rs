// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit host-side route codec; the dependency-free decision types do not
//! acquire serde or I/O dependencies. Shares canonical range-bound validation.
use super::*;
use crate::raft::cell_groups::root::Bound;
use cellar_logic::{cell_group::*, consensus::*};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireCreation {
    domain: String,
    id: [u8; 32],
    cluster: [u8; 16],
    namespace: [u8; 16],
    group: [u8; 16],
    incarnation: [u8; 16],
    lower: Bound,
    upper: Bound,
    epoch: u64,
    birth: [u8; 32],
    schema: [u8; 32],
}

impl From<CellGroupArchiveCreation> for WireCreation {
    fn from(value: CellGroupArchiveCreation) -> Self {
        let route = value.route;
        Self {
            domain: value.domain,
            id: value.id,
            cluster: route.history.cluster.bytes(),
            namespace: route.history.namespace.bytes(),
            group: route.history.group.bytes(),
            incarnation: route.history.incarnation.bytes(),
            lower: Bound::encode(route.range.lower()),
            upper: Bound::encode(route.range.upper()),
            epoch: route.epoch.get(),
            birth: route.birth_digest,
            schema: route.schema_digest,
        }
    }
}

impl TryFrom<WireCreation> for CellGroupArchiveCreation {
    type Error = CellGroupArchiveError;

    fn try_from(value: WireCreation) -> Result<Self> {
        let invalid = |_| CellGroupArchiveError::Invalid("archive registry route identity");
        let namespace = NamespaceId::new(value.namespace).map_err(invalid)?;
        let route = GroupRoute {
            history: GroupHistory {
                cluster: ClusterId::new(value.cluster).map_err(invalid)?,
                namespace,
                group: GroupId::new(value.group).map_err(invalid)?,
                incarnation: IncarnationId::new(value.incarnation).map_err(invalid)?,
            },
            range: CellRange::new(
                value
                    .lower
                    .decode(namespace)
                    .map_err(CellGroupCheckpointError::from)?,
                value
                    .upper
                    .decode(namespace)
                    .map_err(CellGroupCheckpointError::from)?,
            )
            .map_err(invalid)?,
            epoch: RouteEpoch::new(value.epoch).map_err(invalid)?,
            birth_digest: value.birth,
            schema_digest: value.schema,
        };
        let result = Self {
            domain: value.domain,
            id: value.id,
            route,
        };
        result.check(result.route.history)?;
        Ok(result)
    }
}
