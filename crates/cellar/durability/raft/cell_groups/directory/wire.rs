// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde::{Deserialize, Serialize};

pub(super) const PAGE: u8 = 81;
pub(super) const HEAD: u8 = 82;
pub(super) const JOURNAL: u8 = 83;
pub(super) const PIN: u8 = 84;
pub(super) const REQUEST: u8 = 85;
// Fleet maintenance already uses 86 in the shared catalog envelope.
pub(super) const GROUP_BIRTH: u8 = 186;
pub(super) const LIMIT: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "bound", content = "cell", deny_unknown_fields)]
pub(super) enum Bound {
    NegativeInfinity,
    Key(String),
    PositiveInfinity,
}

impl Bound {
    pub fn from_bound(value: &CellBound) -> Self {
        match value {
            CellBound::NegativeInfinity => Self::NegativeInfinity,
            CellBound::Key(k) => Self::Key(k.cell().as_str().into()),
            CellBound::PositiveInfinity => Self::PositiveInfinity,
        }
    }

    pub fn decode(&self, ns: NamespaceId) -> Result<CellBound, CatalogError> {
        Ok(match self {
            Self::NegativeInfinity => CellBound::NegativeInfinity,
            Self::PositiveInfinity => CellBound::PositiveInfinity,
            Self::Key(cell) => CellBound::Key(cell_routing_key(
                ns,
                &CellId::new(cell).map_err(|_| CatalogError::Corrupt("range cell ID"))?,
            )),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Route {
    pub group: [u8; 16],
    pub incarnation: [u8; 16],
    pub lower: Bound,
    pub upper: Bound,
    pub epoch: u64,
    pub birth: [u8; 32],
}

impl Route {
    pub fn encode(value: &GroupRoute, owner: &RangeDirectory) -> Result<Self, CatalogError> {
        let route = Self {
            group: value.history.group.bytes(),
            incarnation: value.history.incarnation.bytes(),
            lower: Bound::from_bound(value.range.lower()),
            upper: Bound::from_bound(value.range.upper()),
            epoch: value.epoch.get(),
            birth: value.birth_digest,
        };
        if route.decode(owner)? != *value {
            return Err(CatalogError::Invalid("range route binding"));
        }
        Ok(route)
    }

    pub fn decode(&self, owner: &RangeDirectory) -> Result<GroupRoute, CatalogError> {
        let config = owner.namespace.config();
        if self.birth == [0; 32] {
            return Err(CatalogError::Corrupt("range birth digest"));
        }
        Ok(GroupRoute {
            history: GroupHistory {
                cluster: config.cluster,
                namespace: config.namespace,
                group: GroupId::new(self.group)
                    .map_err(|_| CatalogError::Corrupt("range group"))?,
                incarnation: IncarnationId::new(self.incarnation)
                    .map_err(|_| CatalogError::Corrupt("range incarnation"))?,
            },
            range: CellRange::new(
                self.lower.decode(config.namespace)?,
                self.upper.decode(config.namespace)?,
            )
            .map_err(|_| CatalogError::Corrupt("range bounds"))?,
            epoch: RouteEpoch::new(self.epoch).map_err(|_| CatalogError::Corrupt("range epoch"))?,
            birth_digest: self.birth,
            schema_digest: config.schema_digest,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reference {
    pub digest: [u8; 32],
    pub height: u8,
    pub groups: u32,
    pub lower: Bound,
    pub upper: Bound,
    pub complete: bool,
}

impl Reference {
    pub fn range(&self, owner: &RangeDirectory) -> Result<CellRange, CatalogError> {
        if self.height == 0
            || self.height > 32
            || self.groups == 0
            || self.groups > owner.limits.max_groups
        {
            return Err(CatalogError::Limit);
        }
        CellRange::new(
            self.lower.decode(owner.namespace.config.namespace)?,
            self.upper.decode(owner.namespace.config.namespace)?,
        )
        .map_err(|_| CatalogError::Corrupt("page bounds"))
    }

    pub fn require_complete(&self, owner: &RangeDirectory) -> Result<(), CatalogError> {
        if self.range(owner)? != CellRange::all() || !self.complete {
            return Err(CatalogError::Corrupt("incomplete range directory"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Page {
    pub namespace: [u8; 32],
    pub route: Route,
    pub left: Option<Reference>,
    pub right: Option<Reference>,
}

impl Page {
    pub fn reference(
        &self,
        bytes: &[u8],
        owner: &RangeDirectory,
    ) -> Result<Reference, CatalogError> {
        if self.namespace != owner.namespace.root_digest {
            return Err(CatalogError::Corrupt("range page namespace"));
        }
        let route = self.route.decode(owner)?;
        let height = |r: &Option<Reference>| r.as_ref().map_or(0, |r| r.height);
        if height(&self.left).abs_diff(height(&self.right)) > 1 {
            return Err(CatalogError::Corrupt("unbalanced range page"));
        }
        let mut groups = 1_u32;
        let mut lower = route.range.lower().clone();
        let mut upper = route.range.upper().clone();
        let mut complete = true;
        if let Some(left) = &self.left {
            let range = left.range(owner)?;
            if range.upper() > route.range.lower() {
                return Err(CatalogError::Corrupt("overlapping left ranges"));
            }
            complete &= left.complete && range.upper() == route.range.lower();
            lower = range.lower().clone();
            groups = groups.checked_add(left.groups).ok_or(CatalogError::Limit)?;
        }
        if let Some(right) = &self.right {
            let range = right.range(owner)?;
            if range.lower() < route.range.upper() {
                return Err(CatalogError::Corrupt("overlapping right ranges"));
            }
            complete &= right.complete && range.lower() == route.range.upper();
            upper = range.upper().clone();
            groups = groups
                .checked_add(right.groups)
                .ok_or(CatalogError::Limit)?;
        }
        let result = Reference {
            digest: record::digest(bytes),
            height: height(&self.left)
                .max(height(&self.right))
                .checked_add(1)
                .ok_or(CatalogError::Limit)?,
            groups,
            lower: Bound::from_bound(&lower),
            upper: Bound::from_bound(&upper),
            complete,
        };
        result.range(owner)?;
        Ok(result)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub namespace: [u8; 32],
    pub genesis: [u8; 32],
    pub revision: u64,
    pub tree: Reference,
    pub operations: Option<[u8; 32]>,
    pub groups: Option<[u8; 32]>,
    pub journal: [u8; 32],
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Pin {
    pub namespace: [u8; 32],
    pub genesis: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub namespace: [u8; 32],
    pub operation: [u8; 16],
    pub expected: Option<[u8; 32]>,
    pub parents: Vec<Route>,
    pub children: Vec<Route>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Journal {
    pub namespace: [u8; 32],
    pub operation: [u8; 16],
    pub request: [u8; 32],
    pub revision: u64,
    pub previous: Option<[u8; 32]>,
    pub tree: Reference,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Birth {
    pub namespace: [u8; 32],
    pub group: [u8; 16],
    pub incarnation: [u8; 16],
    pub lower: Bound,
    pub upper: Bound,
    pub epoch: u64,
    pub creation: [u8; 16],
    pub voters: Vec<record::Member>,
    pub learners: Vec<record::Member>,
}
