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
    pub(super) recovery: Option<Box<super::recovery::RecoveryState>>,
    seal: Option<Box<GroupSourceSeal>>,
}

/// Exact, irreversible cut selected by committed source apply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupSourceSeal {
    transition: [u8; 16],
    authorization: [u8; 32],
    payload: [u8; 32],
    index: u64,
    term: u64,
}

impl GroupSourceSeal {
    pub fn position(&self) -> LogPosition {
        LogPosition {
            index: LogIndex::new(self.index),
            term: Term::new(self.term),
        }
    }

    pub fn authorization(&self) -> [u8; 32] {
        self.authorization
    }

    pub(crate) fn matches(
        &self,
        transition: TransitionId,
        authorization: [u8; 32],
        payload: [u8; 32],
    ) -> bool {
        self.transition == transition.bytes()
            && self.authorization == authorization
            && self.payload == payload
    }
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

    /// Data imported into a fresh history remains unavailable to application
    /// reads/writes until its replicated activation decision is selected.
    pub fn is_active(&self) -> bool {
        self.seal.is_none() && self.recovery.as_ref().is_none_or(|state| state.active())
    }

    pub fn source_seal(&self) -> Option<&GroupSourceSeal> {
        self.seal.as_deref()
    }

    pub(super) fn seal_source(
        &mut self,
        transition: TransitionId,
        authorization: [u8; 32],
        payload: [u8; 32],
    ) -> Result<(), ManifestError> {
        if !self.is_active() {
            return Err(ManifestError::Invalid("source is not active"));
        }
        self.seal = Some(Box::new(GroupSourceSeal {
            transition: transition.bytes(),
            authorization,
            payload,
            index: self.position.index.get(),
            term: self.position.term.get(),
        }));
        self.check()
    }

    pub(crate) fn preserves_source_seal(&self, previous: &Self) -> bool {
        match (previous.source_seal(), self.source_seal()) {
            (Some(previous_seal), Some(next_seal)) => {
                previous_seal == next_seal
                    && previous.root == self.root
                    && previous.membership_revision == self.membership_revision
            }
            (Some(_), None) => false,
            (None, Some(next_seal)) => {
                next_seal.index > previous.position.index.get()
                    && next_seal.term >= previous.position.term.get()
            }
            (None, None) => true,
        }
    }

    /// Commitment to the immutable recovery intent and retained source archive.
    /// This is provenance, never a fence, membership or serving permission.
    pub fn recovery_request(&self) -> Option<[u8; 32]> {
        self.recovery.as_ref().map(|state| state.request())
    }

    pub(crate) fn is_recovery_genesis(&self) -> bool {
        self.recovery.is_some()
            && !self.is_active()
            && self.position.index.get() == 1
            && self.position.term.get() == 1
            && self.membership_revision == 1
    }

    pub(crate) fn recovery_commitment(&self) -> Result<Option<[u8; 32]>, ManifestError> {
        self.recovery
            .as_deref()
            .map(|state| match state.activation_manifest() {
                Some(commitment) => Ok(commitment),
                None => state.seed_commitment(self),
            })
            .transpose()
    }

    pub(crate) fn recovery_activation(&self) -> Option<([u8; 32], [u8; 32], LogPosition)> {
        self.recovery.as_ref().and_then(|state| state.activation())
    }

    /// A retained history cannot forget or replace its recovery lineage. Only
    /// an authenticated empty learner may acquire that lineage via a snapshot.
    pub(crate) fn preserves_recovery(&self, previous: &Self) -> bool {
        match (&previous.recovery, &self.recovery) {
            (None, None) => true,
            (Some(previous), Some(next)) => next.succeeds(previous),
            _ => false,
        }
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
        if let Some(recovery) = &self.recovery {
            recovery.validate(self.position)?;
            if !recovery.active() && self.membership_revision != 1 {
                return Err(ManifestError::Invalid(
                    "pending recovery membership changed",
                ));
            }
        }
        if let Some(seal) = self.source_seal() {
            if TransitionId::new(seal.transition).is_err()
                || seal.authorization == [0; 32]
                || seal.payload == [0; 32]
                || seal.index < 2
                || seal.term == 0
                || seal.index > self.position.index.get()
                || seal.term > self.position.term.get()
                || (seal.index == self.position.index.get()
                    && seal.term != self.position.term.get())
                || self.membership_revision >= seal.index
                || self.recovery.as_ref().is_some_and(|state| !state.active())
            {
                return Err(ManifestError::Invalid("source seal"));
            }
        }
        if let Some(root) = self.root.0 {
            if root.cells == 0
                || root.cells > 1_000_000
                || root.height == 0
                || root.height > 32
                || root.last_mutation_index == 0
                || root.last_mutation_term == 0
                || root.last_mutation_index > self.position.index.get()
                || root.last_mutation_term > self.position.term.get()
                || root.last_mutation_index == self.position.index.get()
                    && root.last_mutation_term != self.position.term.get()
                || root
                    .next_due_unix_millis
                    .is_some_and(|due| due > i64::MAX as u64)
            {
                return Err(ManifestError::Invalid("root bounds"));
            }
        }
        Ok(())
    }

    pub(crate) fn matches_group<S>(&self, scope: &S, birth: [u8; 32]) -> bool
    where
        for<'a> GroupHistory: From<&'a S>,
    {
        self.route.history == GroupHistory::from(scope) && self.route.birth_digest == birth
    }

    pub(crate) fn genesis(route: GroupRoute) -> Result<Self, ManifestError> {
        let marker = Self {
            route,
            position: LogPosition {
                index: LogIndex::new(1),
                term: Term::new(1),
            },
            membership_revision: 1,
            root: ManifestRoot::default(),
            recovery: None,
            seal: None,
        };
        marker.check()?;
        Ok(marker)
    }

    pub(crate) fn recovery_genesis(
        route: GroupRoute,
        root: ManifestRoot,
        request: [u8; 32],
        operation: ProposalId,
    ) -> Result<Self, ManifestError> {
        let marker = Self {
            root,
            recovery: Some(Box::new(super::recovery::RecoveryState::pending(
                request, operation,
            )?)),
            ..Self::genesis(route)?
        };
        marker.check()?;
        Ok(marker)
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
    /// Import admission follows every clone of this private physical closure.
    pub(super) recovery_admission: Option<Arc<crate::raft::budget::Permit>>,
    pub(super) page_owner: Arc<super::page_store::PageOwner>,
    retention: Arc<super::retention::RootRetentions>,
}

/// Private storage identity for archive preparation, with no Raft engine,
/// member, disk lineage or publication permission. Never accept a caller's token.
pub(super) struct ArchiveManifestOwner {
    identity: Arc<()>,
    closed: std::sync::atomic::AtomicBool,
}

impl ArchiveManifestOwner {
    pub(super) fn new() -> Self {
        Self {
            identity: Arc::new(()),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(super) fn require_live(&self) -> Result<(), ManifestError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ManifestError::Invalid("closed archive store"));
        }
        Ok(())
    }

    pub(super) fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn owns(&self, root: &PreparedGroupManifest) -> bool {
        Arc::ptr_eq(&self.identity, &root.engine_owner)
    }
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

    /// Select one due cell from this immutable physical closure without I/O,
    /// wrapping after the routing-key cursor. Like `pin_cell`, this is a data
    /// view only; it grants no execution, leadership or delivery permission.
    pub fn next_due(
        &self,
        now_unix_millis: u64,
        after: Option<&CellId>,
    ) -> Option<(&CellManifestLeaf, CellImagePin)> {
        let after = after.map(|cell| cell_routing_key(self.marker.route.history.namespace, cell));
        self.images.next_due(now_unix_millis, after.as_ref())
    }

    pub(super) fn require_selected<S>(
        &self,
        group: &crate::raft::cache::GroupLog<S>,
    ) -> Result<(), ManifestError> {
        self.retention.require_selected(group, &self.marker)
    }

    pub(crate) fn require_checkpoint<S>(
        &self,
        group: &crate::raft::cache::GroupLog<S>,
    ) -> Result<(), ManifestError> {
        self.retention.require_checkpoint(group)
    }

    pub(crate) fn retained_checkpoint<S>(
        &self,
        group: &crate::raft::cache::GroupLog<S>,
    ) -> Result<Self, ManifestError> {
        if !Arc::ptr_eq(&self.engine_owner, &group.owner) {
            return Err(ManifestError::Conflict);
        }
        let marker = group
            .checkpoint_manifest()
            .map_err(|_| ManifestError::Invalid("closed engine"))?
            .ok_or(ManifestError::Invalid("checkpoint not published"))?;
        let images = self.retention.checkpoint(group, &marker)?;
        Ok(Self {
            marker,
            images,
            base: None,
            ..self.clone()
        })
    }

    pub(super) fn checkpoint_from_store<S>(
        group: &crate::raft::cache::GroupLog<S>,
        store: &ManifestPageStore,
    ) -> Result<Self, ManifestError> {
        store.require_live()?;
        let marker = group
            .checkpoint_manifest()
            .map_err(|_| ManifestError::Invalid("closed engine"))?
            .ok_or(ManifestError::Invalid("checkpoint not published"))?;
        let images = store.retention.checkpoint(group, &marker)?;
        Ok(Self {
            marker,
            images,
            engine_owner: group.owner.clone(),
            base: None,
            recovery_admission: None,
            page_owner: store.owner.clone(),
            retention: store.retention.clone(),
        })
    }

    pub(crate) fn reserved_bytes(&self) -> usize {
        self.images.reserved_bytes()
    }

    pub(crate) fn begin_publication<S>(
        &self,
        group: &crate::raft::cache::GroupLog<S>,
        previous: Option<&GroupManifest>,
        checkpoint: bool,
    ) -> Result<super::retention::PublicationPin, ManifestError> {
        self.retention.begin(group, self, previous, checkpoint)
    }

    pub(super) fn begin_genesis(
        &self,
        permit: &crate::raft::catalog::RecoverySeedPermit,
        closed: &Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<super::retention::PublicationPin, ManifestError> {
        self.retention.begin_genesis(permit, closed, self)
    }
}

impl ManifestTree {
    /// Advance one committed no-op or duplicate enrollment without changing any
    /// cell version. The owner must supply the actual next committed position;
    /// this preparation alone proves neither commitment nor serving authority.
    pub fn prepare_unchanged(
        &self,
        store: &ManifestPageStore,
        previous: &PreparedGroupManifest,
        position: LogPosition,
    ) -> Result<PreparedGroupManifest, ManifestError> {
        store.require_live()?;
        if previous.marker.route != self.route || !Arc::ptr_eq(&previous.page_owner, &store.owner) {
            return Err(ManifestError::Invalid("root/page owner"));
        }
        if previous.marker.position.index.get().checked_add(1) != Some(position.index.get())
            || position.term < previous.marker.position.term
        {
            return Err(ManifestError::Invalid("noncontiguous no-op"));
        }
        let mut prepared = previous.clone();
        prepared.base = Some(previous.marker.clone());
        prepared.marker.position = position;
        prepared.marker.check()?;
        Ok(prepared)
    }

    /// Full closure verification for bootstrap, recovery and checkpoint install.
    /// Ordinary cell mutations use `prepare_update` instead.
    /// The callback returns actual sealed images, never merely accepted markers.
    pub fn prepare_complete<S>(
        &self,
        group: &crate::raft::cache::GroupLog<S>,
        store: &mut ManifestPageStore,
        root: ManifestRoot,
        position: LogPosition,
        membership_revision: u64,
        mut image: impl FnMut(&CellManifestLeaf) -> Result<CellImagePin, ManifestError>,
    ) -> Result<PreparedGroupManifest, ManifestError>
    where
        for<'a> GroupHistory: From<&'a S>,
    {
        let mut marker = GroupManifest {
            route: self.route.clone(),
            position,
            membership_revision,
            root,
            recovery: None,
            seal: None,
        };
        // Reopening must retain the selected recovery lifecycle, rather than
        // silently turning a pending imported root into an ordinary active one.
        let mut lifecycle_selected = false;
        let mut matched = false;
        for selected in [group.group_manifest(), group.checkpoint_manifest()] {
            if let Some(selected) = selected.map_err(|_| ManifestError::Invalid("closed engine"))? {
                lifecycle_selected |= selected.recovery.is_some() || selected.seal.is_some();
                if selected.route == marker.route
                    && selected.position == marker.position
                    && selected.membership_revision == marker.membership_revision
                    && selected.root == marker.root
                {
                    marker = selected;
                    matched = true;
                    break;
                }
            }
        }
        if lifecycle_selected && !matched {
            return Err(ManifestError::Invalid("complete recovery marker required"));
        }
        self.prepare_bound_complete(group, store, marker, &mut image)
    }

    pub(super) fn prepare_bound_complete<S>(
        &self,
        group: &crate::raft::cache::GroupLog<S>,
        store: &mut ManifestPageStore,
        marker: GroupManifest,
        image: &mut dyn FnMut(&CellManifestLeaf) -> Result<CellImagePin, ManifestError>,
    ) -> Result<PreparedGroupManifest, ManifestError>
    where
        for<'a> GroupHistory: From<&'a S>,
    {
        if marker.route != self.route {
            return Err(ManifestError::Conflict);
        }
        marker.check()?;
        group
            .cache_revision()
            .map_err(|_| ManifestError::Invalid("closed engine"))?;
        if !marker.matches_group(&group.identity().scope, group.birth_digest) {
            return Err(ManifestError::Invalid("root/engine identity"));
        }
        let prepared = self.prepare_physical(store, marker.clone(), group.owner.clone(), image)?;
        // Recover physical ownership only from the engine's selected marker.
        // A prospective checkpoint or mutation remains a preparation capability.
        if group
            .group_manifest()
            .map_err(|_| ManifestError::Invalid("closed engine"))?
            .as_ref()
            == Some(&marker)
        {
            store.retention.recover(group, &marker, &prepared.images)?;
        }
        if group
            .checkpoint_manifest()
            .map_err(|_| ManifestError::Invalid("closed engine"))?
            .as_ref()
            == Some(&marker)
        {
            store
                .retention
                .recover_checkpoint(group, &marker, &prepared.images)?;
        }
        Ok(prepared)
    }

    pub(super) fn prepare_archive_complete(
        &self,
        owner: &ArchiveManifestOwner,
        store: &mut ManifestPageStore,
        marker: GroupManifest,
        image: &mut dyn FnMut(&CellManifestLeaf) -> Result<CellImagePin, ManifestError>,
    ) -> Result<PreparedGroupManifest, ManifestError> {
        owner.require_live()?;
        if marker.route() != self.route() {
            return Err(ManifestError::Invalid("archive root route"));
        }
        self.prepare_physical(store, marker, owner.identity.clone(), image)
    }

    /// Called only with the catalog's process-local engine grant. No engine
    /// selection/retention is recovered before sync.
    pub(super) fn prepare_recovery_genesis(
        &self,
        permit: &crate::raft::catalog::RecoverySeedPermit,
        store: &mut ManifestPageStore,
        source: &PreparedGroupManifest,
    ) -> Result<PreparedGroupManifest, ManifestError> {
        if !source.marker.is_recovery_genesis()
            || source.marker.route != self.route
            || !Arc::ptr_eq(&source.page_owner, &store.owner)
            || source.marker != *permit.genesis()
        {
            return Err(ManifestError::Invalid("recovery genesis physical binding"));
        }
        self.prepare_physical(
            store,
            source.marker.clone(),
            permit.owner().clone(),
            &mut |leaf| {
                source
                    .pin_cell(&leaf.version.history.cell)
                    .filter(|(selected, _)| *selected == leaf)
                    .map(|(_, pin)| pin)
                    .ok_or(ManifestError::Invalid("recovery genesis closure"))
            },
        )
    }

    fn prepare_physical(
        &self,
        store: &mut ManifestPageStore,
        marker: GroupManifest,
        owner: Arc<()>,
        image: &mut dyn FnMut(&CellManifestLeaf) -> Result<CellImagePin, ManifestError>,
    ) -> Result<PreparedGroupManifest, ManifestError> {
        marker.check()?;
        store.require_live()?;
        self.verify(store, marker.root(), marker.position(), |_| Ok(()))?;
        let pins = ManifestPins::complete(self, store, marker.root(), &mut |leaf| image(leaf))?;
        store.sync_directory()?;
        Ok(PreparedGroupManifest {
            marker,
            images: pins,
            engine_owner: owner,
            base: None,
            recovery_admission: None,
            page_owner: store.owner.clone(),
            retention: store.retention.clone(),
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
        if !previous.marker.is_active() {
            return Err(ManifestError::Invalid("recovery is not activated"));
        }
        if previous.marker.position.index.get().checked_add(1)
            != Some(leaf.last_mutation.index.get())
            || leaf.last_mutation.term < previous.marker.position.term
            || leaf.witness.group != self.route.history
            || leaf.witness.position != leaf.last_mutation
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
            recovery: previous.marker.recovery.clone(),
            seal: None,
        };
        marker.check()?;
        Ok(PreparedGroupManifest {
            marker,
            images: pins,
            engine_owner: previous.engine_owner.clone(),
            base: Some(previous.marker.clone()),
            recovery_admission: previous.recovery_admission.clone(),
            page_owner: store.owner.clone(),
            retention: store.retention.clone(),
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
    #[serde(deserialize_with = "required_recovery")]
    recovery: Option<Box<super::recovery::RecoveryState>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seal: Option<Box<GroupSourceSeal>>,
}

fn required_recovery<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Box<super::recovery::RecoveryState>>, D::Error> {
    Option::deserialize(decoder)
}

fn required_root<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<super::page_wire::PageRef>, D::Error> {
    Option::deserialize(decoder)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "bound", content = "cell", deny_unknown_fields)]
pub(super) enum Bound {
    NegativeInfinity,
    Key(String),
    PositiveInfinity,
}

impl Bound {
    pub(super) fn encode(value: &CellBound) -> Self {
        match value {
            CellBound::NegativeInfinity => Self::NegativeInfinity,
            CellBound::Key(k) => Self::Key(k.cell().as_str().into()),
            CellBound::PositiveInfinity => Self::PositiveInfinity,
        }
    }

    pub(super) fn decode(self, namespace: NamespaceId) -> Result<CellBound, ManifestError> {
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
            recovery: m.recovery,
            seal: m.seal,
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
            recovery: w.recovery,
            seal: w.seal,
        };
        result.check()?;
        Ok(result)
    }
}
