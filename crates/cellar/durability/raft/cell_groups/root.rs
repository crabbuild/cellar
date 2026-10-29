// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete-root preparation and engine metadata. Neither a marker observation
//! nor physical image seals establish current-term quorum/serving authority.

use super::{
    CellImagePin, CellManifestLeaf, ManifestError, ManifestLimits, ManifestPageStore, ManifestPins,
    ManifestRoot, ManifestTree,
};
use crate::application::consensus::cell_routing_key;
use cellar_logic::{cell_group::*, consensus::*};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireRoot", into = "WireRoot")]
pub struct GroupManifest {
    pub(super) route: GroupRoute,
    pub(super) position: LogPosition,
    pub(super) membership_revision: u64,
    pub(super) root: ManifestRoot,
}

impl GroupManifest {
    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    pub fn root(&self) -> ManifestRoot {
        self.root
    }

    pub(crate) fn check(&self) -> Result<(), ManifestError> {
        self.position
            .validate()
            .map_err(|_| ManifestError::Invalid("root position"))?;
        if self.position.index.get() == 0
            || self.membership_revision == 0
            || self.membership_revision > self.position.index.get()
        {
            return Err(ManifestError::Invalid("root membership"));
        }
        ManifestTree::new(self.route.clone(), ManifestLimits::default())?;
        if let Some(root) = self.root.0 {
            if root.cells == 0 || root.cells > 1_000_000 || root.height == 0 || root.height > 32 {
                return Err(ManifestError::Invalid("root bounds"));
            }
        }
        Ok(())
    }

    pub(crate) fn matches_group(&self, scope: &GroupScope, birth: [u8; 32]) -> bool {
        self.route.history
            == GroupHistory {
                cluster: scope.cluster,
                namespace: scope.namespace,
                group: scope.group,
                incarnation: scope.incarnation,
            }
            && self.route.birth_digest == birth
    }
}

/// A complete, synced closure. Image pins remain alive while the engine request
/// is queued or syncing. Durable-root retention must also survive uncertain
/// outcomes; physical stores must reconcile engine roots before any reclamation.
#[derive(Clone)]
pub struct PreparedGroupManifest {
    pub(crate) marker: GroupManifest,
    // The pins are deliberately inaccessible to mutation/publication callers.
    pub(crate) images: ManifestPins,
    pub(crate) engine_owner: Arc<()>,
    pub(crate) base: Option<GroupManifest>,
    page_owner: Arc<()>,
}

impl PreparedGroupManifest {
    pub fn marker(&self) -> &GroupManifest {
        &self.marker
    }

    pub fn pinned_images(&self) -> usize {
        self.images.len()
    }

    /// Pin the exact image/version selected by this coherent root. This is a
    /// physical read view; the caller still needs the requested read authority.
    pub fn pin_cell(&self, cell: &CellId) -> Option<(&CellManifestLeaf, CellImagePin)> {
        self.images
            .get(&cell_routing_key(self.marker.route.history.namespace, cell))
    }

    pub(crate) fn reserved_bytes(&self) -> usize {
        self.images.reserved_bytes()
    }
}

impl ManifestTree {
    /// Full closure verification for bootstrap, recovery and checkpoint install.
    /// Ordinary cell mutations use `prepare_update` instead.
    /// The callback returns actual sealed images, never merely accepted markers.
    pub fn prepare_complete(
        &self,
        group: &crate::raft::cache::GroupLog,
        store: &mut ManifestPageStore,
        root: ManifestRoot,
        position: LogPosition,
        membership_revision: u64,
        mut image: impl FnMut(&CellManifestLeaf) -> Result<CellImagePin, ManifestError>,
    ) -> Result<PreparedGroupManifest, ManifestError> {
        let marker = GroupManifest {
            route: self.route.clone(),
            position,
            membership_revision,
            root,
        };
        marker.check()?;
        group
            .cache_revision()
            .map_err(|_| ManifestError::Invalid("closed engine"))?;
        if !marker.matches_group(&group.identity().scope, group.birth_digest) {
            return Err(ManifestError::Invalid("root/engine identity"));
        }
        self.verify(store, root, position, |_| Ok(()))?;
        let pins = ManifestPins::complete(self, store, root, &mut image)?;
        store.sync_directory()?;
        Ok(PreparedGroupManifest {
            marker,
            images: pins,
            engine_owner: group.owner.clone(),
            base: None,
            page_owner: store.owner.clone(),
        })
    }

    /// Prepare one cell mutation/enrollment at the next contiguous group index.
    /// The previous complete capability supplies unchanged physical image pins;
    /// only the changed image and O(log cells) page path need preparation. It
    /// may be chained for staging, but engine publication still checks order.
    pub fn prepare_update(
        &self,
        store: &mut ManifestPageStore,
        previous: &PreparedGroupManifest,
        leaf: CellManifestLeaf,
        expected: Option<&CellVersion>,
        image: CellImagePin,
    ) -> Result<PreparedGroupManifest, ManifestError> {
        if previous.marker.route != self.route || !Arc::ptr_eq(&previous.page_owner, &store.owner) {
            return Err(ManifestError::Invalid("root/page owner"));
        }
        if previous.marker.position.index.get().checked_add(1)
            != Some(leaf.last_mutation.index.get())
            || leaf.last_mutation.term < previous.marker.position.term
        {
            return Err(ManifestError::Invalid("noncontiguous mutation"));
        }
        image.check(&leaf, self.route.schema_digest)?;
        let root = self.update(store, previous.marker.root, leaf.clone(), expected)?;
        // update synchronizes every newly referenced page before returning. All
        // reused subtrees belong to a complete preparation on this store owner.
        let pins = previous.images.updated(self, store, root, &leaf, &image)?;
        let marker = GroupManifest {
            route: self.route.clone(),
            position: leaf.last_mutation,
            membership_revision: previous.marker.membership_revision,
            root,
        };
        marker.check()?;
        Ok(PreparedGroupManifest {
            marker,
            images: pins,
            engine_owner: previous.engine_owner.clone(),
            base: Some(previous.marker.clone()),
            page_owner: store.owner.clone(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRoot {
    domain: String,
    cluster: [u8; 16],
    namespace: [u8; 16],
    group: [u8; 16],
    incarnation: [u8; 16],
    lower: Bound,
    upper: Bound,
    route_epoch: u64,
    birth: [u8; 32],
    schema: [u8; 32],
    index: u64,
    term: u64,
    membership_revision: u64,
    #[serde(deserialize_with = "required_root")]
    root: Option<super::page_wire::PageRef>,
}

fn required_root<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<super::page_wire::PageRef>, D::Error> {
    Option::deserialize(decoder)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "bound", content = "cell", deny_unknown_fields)]
enum Bound {
    NegativeInfinity,
    Key(String),
    PositiveInfinity,
}

impl Bound {
    fn encode(value: &CellBound) -> Self {
        match value {
            CellBound::NegativeInfinity => Self::NegativeInfinity,
            CellBound::Key(k) => Self::Key(k.cell().as_str().into()),
            CellBound::PositiveInfinity => Self::PositiveInfinity,
        }
    }

    fn decode(self, namespace: NamespaceId) -> Result<CellBound, ManifestError> {
        Ok(match self {
            Self::NegativeInfinity => CellBound::NegativeInfinity,
            Self::PositiveInfinity => CellBound::PositiveInfinity,
            Self::Key(s) => CellBound::Key(cell_routing_key(
                namespace,
                &CellId::new(s).map_err(|_| ManifestError::Invalid("range ID"))?,
            )),
        })
    }
}

impl From<GroupManifest> for WireRoot {
    fn from(m: GroupManifest) -> Self {
        Self {
            domain: "cellar-group-root-1".into(),
            cluster: m.route.history.cluster.bytes(),
            namespace: m.route.history.namespace.bytes(),
            group: m.route.history.group.bytes(),
            incarnation: m.route.history.incarnation.bytes(),
            lower: Bound::encode(m.route.range.lower()),
            upper: Bound::encode(m.route.range.upper()),
            route_epoch: m.route.epoch.get(),
            birth: m.route.birth_digest,
            schema: m.route.schema_digest,
            index: m.position.index.get(),
            term: m.position.term.get(),
            membership_revision: m.membership_revision,
            root: m.root.0,
        }
    }
}

impl TryFrom<WireRoot> for GroupManifest {
    type Error = ManifestError;

    fn try_from(w: WireRoot) -> Result<Self, Self::Error> {
        if w.domain != "cellar-group-root-1" {
            return Err(ManifestError::Invalid("root domain"));
        }
        let invalid = |_| ManifestError::Invalid("root identity");
        let namespace = NamespaceId::new(w.namespace).map_err(invalid)?;
        let result = Self {
            route: GroupRoute {
                history: GroupHistory {
                    cluster: ClusterId::new(w.cluster).map_err(invalid)?,
                    namespace,
                    group: GroupId::new(w.group).map_err(invalid)?,
                    incarnation: IncarnationId::new(w.incarnation).map_err(invalid)?,
                },
                range: CellRange::new(w.lower.decode(namespace)?, w.upper.decode(namespace)?)
                    .map_err(invalid)?,
                epoch: RouteEpoch::new(w.route_epoch).map_err(invalid)?,
                birth_digest: w.birth,
                schema_digest: w.schema,
            },
            position: LogPosition {
                index: LogIndex::new(w.index),
                term: Term::new(w.term),
            },
            membership_revision: w.membership_revision,
            root: ManifestRoot(w.root),
        };
        result.check()?;
        Ok(result)
    }
}
