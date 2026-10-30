// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Range discovery and conditional topology metadata. Neither a route nor a
//! directory publication grants Raft, transfer, birth or serving authority.

use super::{CatalogError, NamespaceCatalog};
use crate::application::consensus::cell_routing_key;
use crate::raft::{catalog_wire as record, coordination_index::Index};
use cellar_logic::{
    cell_group::*,
    consensus::{GroupId, IncarnationId, NamespaceId, ProposalId},
};
use object_store::{path::Path, PutMode, PutOptions, UpdateVersion};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

mod birth;
mod bootstrap;
mod route_cache;
mod routing;
pub use route_cache::CellRouteCacheUsage;
pub(crate) use route_cache::RouteCache;
mod store;
mod tree;
mod wire;

pub use birth::{CellGroupBirth, CellGroupBirthRequest, CellGroupBirthSpec};
pub use bootstrap::CellGroupBootstrapPermit;
pub use routing::{
    CellRouteAttempt, CellRouteOperation, CellRouteRequest, CellRoutingError, CellRoutingFailure,
    CellRoutingLimits, CellRoutingSession, ResolvedCellRoute,
};

#[derive(Clone, Copy, Debug)]
pub struct DirectoryLimits {
    pub max_groups: u32,
    pub max_mutations: u64,
    pub max_cached_pages: usize,
    pub max_cache_bytes: usize,
    pub operation_timeout_ms: u64,
}

impl Default for DirectoryLimits {
    fn default() -> Self {
        Self {
            max_groups: 1_000_000,
            max_mutations: 1_000_000,
            max_cached_pages: 64,
            max_cache_bytes: 4 * 1024 * 1024,
            operation_timeout_ms: 5000,
        }
    }
}

#[derive(Clone)]
pub struct RangeDirectory {
    namespace: NamespaceCatalog,
    limits: DirectoryLimits,
    cache: Arc<Mutex<store::Cache>>,
}

/// A pinned catalog observation, not authority or an automatic freshness claim.
#[derive(Clone)]
pub struct DirectoryView {
    head: wire::Head,
    digest: [u8; 32],
    owner: Arc<()>,
}

impl DirectoryView {
    pub fn revision(&self) -> u64 {
        self.head.revision
    }

    pub fn groups(&self) -> u32 {
        self.head.tree.groups
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Persist these canonical bytes before issuing a topology mutation. Resuming
/// decodes the same expected root and operation, rather than rebasing a retry.
pub struct DirectoryMutation {
    request: wire::Request,
    bytes: Vec<u8>,
    digest: [u8; 32],
    operation: ProposalId,
}

impl DirectoryMutation {
    pub fn encode(&self) -> &[u8] {
        &self.bytes
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn operation(&self) -> ProposalId {
        self.operation
    }
}

/// Exact historical catalog decision. A later directory head may supersede its
/// ranges; destination admission still needs the replicated transition protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryPublication {
    operation: ProposalId,
    revision: u64,
    tree: [u8; 32],
    journal: [u8; 32],
}

impl DirectoryPublication {
    pub fn operation(&self) -> ProposalId {
        self.operation
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn tree_digest(&self) -> [u8; 32] {
        self.tree
    }

    pub fn journal_digest(&self) -> [u8; 32] {
        self.journal
    }
}

impl RangeDirectory {
    pub(crate) fn namespace_config(&self) -> &crate::raft::catalog::NamespaceConfig {
        self.namespace.config()
    }

    pub fn new(namespace: NamespaceCatalog, limits: DirectoryLimits) -> Result<Self, CatalogError> {
        namespace.require_mode(cellar_logic::NamespaceMode::Raft)?;
        if !(1..=1_000_000).contains(&limits.max_groups)
            || !(1..=1_000_000).contains(&limits.max_mutations)
            || limits.max_cached_pages > 1024
            || limits.max_cache_bytes > 64 * 1024 * 1024
            || (limits.max_cached_pages == 0) != (limits.max_cache_bytes == 0)
            || !(1..=60_000).contains(&limits.operation_timeout_ms)
        {
            return Err(CatalogError::Limit);
        }
        Ok(Self {
            namespace,
            limits,
            cache: Arc::new(Mutex::new(store::Cache::default())),
        })
    }

    fn check_view(&self, view: &DirectoryView) -> Result<(), CatalogError> {
        if !Arc::ptr_eq(&view.owner, &self.namespace.inner.owner)
            || view.head.namespace != self.namespace.root_digest
        {
            return Err(CatalogError::Conflict("foreign directory view"));
        }
        Ok(())
    }

    fn request(
        &self,
        operation: ProposalId,
        expected: Option<[u8; 32]>,
        parents: &[GroupRoute],
        children: &[GroupRoute],
    ) -> Result<DirectoryMutation, CatalogError> {
        if parents.len() > 64 || children.len() > if expected.is_none() { 128 } else { 64 } {
            return Err(CatalogError::Limit);
        }
        let request = wire::Request {
            namespace: self.namespace.root_digest,
            operation: operation.bytes(),
            expected,
            parents: parents
                .iter()
                .map(|r| wire::Route::encode(r, self))
                .collect::<Result<_, _>>()?,
            children: children
                .iter()
                .map(|r| wire::Route::encode(r, self))
                .collect::<Result<_, _>>()?,
        };
        self.decode_mutation(&record::encode(wire::REQUEST, &request, wire::LIMIT)?)
    }

    pub fn prepare_initialization(
        &self,
        operation: ProposalId,
        ranges: &[GroupRoute],
    ) -> Result<DirectoryMutation, CatalogError> {
        self.request(operation, None, &[], ranges)
    }

    /// Prepare metadata for one exact contiguous replacement. The topology
    /// owner must separately obtain seal/install/archive/activation proofs before
    /// using this publication; this staging API does not perform a transfer.
    pub fn prepare_replacement(
        &self,
        view: &DirectoryView,
        operation: ProposalId,
        parents: &[GroupRoute],
        children: &[GroupRoute],
    ) -> Result<DirectoryMutation, CatalogError> {
        self.check_view(view)?;
        self.request(operation, Some(view.digest), parents, children)
    }

    pub fn decode_mutation(&self, bytes: &[u8]) -> Result<DirectoryMutation, CatalogError> {
        let request: wire::Request = record::decode(wire::REQUEST, bytes, wire::LIMIT)?;
        self.check_request(&request)?;
        let operation = ProposalId::new(request.operation)
            .map_err(|_| CatalogError::Invalid("directory operation"))?;
        Ok(DirectoryMutation {
            request,
            operation,
            digest: record::digest(bytes),
            bytes: bytes.to_vec(),
        })
    }

    fn union(&self, routes: &[wire::Route]) -> Result<CellRange, CatalogError> {
        let first = routes
            .first()
            .ok_or(CatalogError::Invalid("empty range set"))?
            .decode(self)?;
        let mut range = first.range;
        let mut ids = BTreeSet::from([first.history.group]);
        for route in &routes[1..] {
            let route = route.decode(self)?;
            if !ids.insert(route.history.group) {
                return Err(CatalogError::Conflict("duplicate range group"));
            }
            range = range
                .merge(&route.range)
                .map_err(|_| CatalogError::Conflict("overlap or gap in range set"))?;
        }
        Ok(range)
    }

    fn check_request(&self, request: &wire::Request) -> Result<(), CatalogError> {
        if request.namespace != self.namespace.root_digest {
            return Err(CatalogError::Conflict("directory namespace"));
        }
        if request.children.is_empty()
            || request.children.len() > if request.expected.is_none() { 128 } else { 64 }
            || request.parents.len() > 64
        {
            return Err(CatalogError::Limit);
        }
        let children = self.union(&request.children)?;
        let epoch = if request.expected.is_none() {
            if !request.parents.is_empty() || children != CellRange::all() {
                return Err(CatalogError::Invalid("initial range coverage"));
            }
            1
        } else {
            if self.union(&request.parents)? != children {
                return Err(CatalogError::Conflict("changed range union"));
            }
            request
                .parents
                .iter()
                .map(|r| r.epoch)
                .max()
                .ok_or(CatalogError::Invalid("parent ranges"))?
                .checked_add(1)
                .ok_or(CatalogError::Limit)?
        };
        if request.children.iter().any(|r| r.epoch != epoch) {
            return Err(CatalogError::Conflict("range epoch successor"));
        }
        Ok(())
    }

    fn check_head(&self, head: &wire::Head) -> Result<(), CatalogError> {
        if head.namespace != self.namespace.root_digest
            || head.revision == 0
            || head.revision > self.limits.max_mutations
            || head.genesis == [0; 32]
            || head.journal == [0; 32]
            || head.operations.is_none()
            || head.groups.is_none()
        {
            return Err(CatalogError::Corrupt("directory head"));
        }
        head.tree.require_complete(self)
    }

    async fn head(&self) -> Result<Option<(DirectoryView, UpdateVersion)>, CatalogError> {
        self.namespace
            .inner
            .read_versioned_bounded(&self.key("head"), wire::LIMIT)
            .await?
            .map(|(bytes, version)| {
                let head: wire::Head = record::decode(wire::HEAD, &bytes, wire::LIMIT)?;
                self.check_head(&head)?;
                if !version.e_tag.as_ref().is_some_and(|s| !s.is_empty())
                    && !version.version.as_ref().is_some_and(|s| !s.is_empty())
                {
                    return Err(CatalogError::Corrupt("directory CAS token"));
                }
                Ok((
                    DirectoryView {
                        head,
                        digest: record::digest(&bytes),
                        owner: self.namespace.inner.owner.clone(),
                    },
                    version,
                ))
            })
            .transpose()
    }

    async fn current(&self) -> Result<(DirectoryView, UpdateVersion), CatalogError> {
        let pin: wire::Pin = record::decode(
            wire::PIN,
            &self
                .read("pin")
                .await?
                .ok_or(CatalogError::Corrupt("missing directory pin"))?,
            wire::LIMIT,
        )?;
        let (view, version) = self
            .head()
            .await?
            .ok_or(CatalogError::Corrupt("missing directory head"))?;
        if pin.namespace != self.namespace.root_digest || pin.genesis != view.head.genesis {
            return Err(CatalogError::Corrupt("directory genesis pin"));
        }
        let journal = self.journal(view.head.journal).await?;
        if journal.revision != view.head.revision
            || journal.tree != view.head.tree
            || (journal.revision == 1 && view.head.genesis != view.head.journal)
            || Index::new(self)
                .lookup(view.head.operations, &hex::encode(journal.operation))
                .await?
                != Some(view.head.journal.to_vec())
        {
            return Err(CatalogError::Corrupt("directory head/journal disagreement"));
        }
        Ok((view, version))
    }

    async fn bounded<T>(
        &self,
        work: impl std::future::Future<Output = Result<T, CatalogError>>,
    ) -> Result<T, CatalogError> {
        crate::asyncrt::timeout(
            Duration::from_millis(self.limits.operation_timeout_ms),
            work,
        )
        .await
        .map_err(|_| CatalogError::Unconfirmed)?
    }

    pub async fn observe(&self) -> Result<DirectoryView, CatalogError> {
        self.bounded(async { Ok(self.current().await?.0) }).await
    }

    /// Looks up only an immutable tree path. No head refresh, namespace listing
    /// or object-store call is required when that path is already cached.
    pub async fn lookup(
        &self,
        view: &DirectoryView,
        cell: &CellId,
    ) -> Result<GroupRoute, CatalogError> {
        self.check_view(view)?;
        self.bounded(self.lookup_tree(&view.head.tree, cell)).await
    }

    async fn confirmed(
        &self,
        head: &wire::Head,
        mutation: &DirectoryMutation,
    ) -> Result<Option<DirectoryPublication>, CatalogError> {
        let key = hex::encode(mutation.request.operation);
        let Some(value) = Index::new(self).lookup(head.operations, &key).await? else {
            return Ok(None);
        };
        let id: [u8; 32] = value
            .try_into()
            .map_err(|_| CatalogError::Corrupt("directory journal pointer"))?;
        let journal = self.journal(id).await?;
        if journal.operation != mutation.request.operation || journal.revision > head.revision {
            return Err(CatalogError::Corrupt("directory journal identity"));
        }
        if journal.request != mutation.digest {
            return Err(CatalogError::Conflict("directory operation reused"));
        }
        Ok(Some(DirectoryPublication {
            operation: mutation.operation,
            revision: journal.revision,
            tree: journal.tree.digest,
            journal: id,
        }))
    }

    async fn journal(&self, id: [u8; 32]) -> Result<wire::Journal, CatalogError> {
        let bytes = self
            .read(&format!("journal/{}", hex::encode(id)))
            .await?
            .ok_or(CatalogError::Corrupt("missing directory journal"))?;
        if record::digest(&bytes) != id {
            return Err(CatalogError::Corrupt("directory journal hash"));
        }
        let journal: wire::Journal = record::decode(wire::JOURNAL, &bytes, wire::LIMIT)?;
        journal.tree.require_complete(self)?;
        if journal.namespace != self.namespace.root_digest
            || journal.revision == 0
            || journal.revision > self.limits.max_mutations
            || ProposalId::new(journal.operation).is_err()
            || journal.previous.is_none() != (journal.revision == 1)
        {
            return Err(CatalogError::Corrupt("directory journal identity"));
        }
        Ok(journal)
    }

    /// `None` is an observation, never permission to replace the operation ID
    /// of a potentially submitted mutation. Re-submit the identical bytes.
    pub async fn publication(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<Option<DirectoryPublication>, CatalogError> {
        self.check_request(&mutation.request)?;
        self.bounded(async {
            self.confirmed(&self.current().await?.0.head, mutation)
                .await
        })
        .await
    }

    pub async fn publish(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<DirectoryPublication, CatalogError> {
        self.check_request(&mutation.request)?;
        self.bounded(self.publish_inner(mutation)).await
    }

    async fn publish_inner(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<DirectoryPublication, CatalogError> {
        let request = &mutation.request;
        let current = if request.expected.is_some() {
            Some(self.current().await?)
        } else {
            let pin = self.read("pin").await?;
            let head = self.head().await?;
            if pin.is_some() {
                let (view, _) = self.current().await?;
                return self
                    .confirmed(&view.head, mutation)
                    .await?
                    .ok_or(CatalogError::Conflict("directory already initialized"));
            }
            if head.as_ref().is_some_and(|(v, _)| v.head.revision != 1) {
                return Err(CatalogError::Corrupt("missing active directory pin"));
            }
            head
        };
        if let Some((view, _)) = &current {
            if let Some(publication) = self.confirmed(&view.head, mutation).await? {
                if request.expected.is_some() {
                    return Ok(publication);
                }
                // An interrupted genesis resumes only its identical first head.
            }
            if request
                .expected
                .is_some_and(|expected| expected != view.digest)
            {
                return Err(CatalogError::Conflict("stale directory root"));
            }
        }
        let base = if request.expected.is_some() {
            current.as_ref().map(|(v, _)| &v.head)
        } else {
            None
        };
        let revision = base
            .map_or(Some(1), |h| h.revision.checked_add(1))
            .ok_or(CatalogError::Limit)?;
        if revision > self.limits.max_mutations {
            return Err(CatalogError::Limit);
        }
        let mut tree = base.map(|h| h.tree.clone());
        // A route can be lazy, but its exact cell-independent birth and global
        // group identity must be reserved before it can enter the directory.
        for child in &request.children {
            self.verify_birth_reservation(&child.decode(self)?).await?;
        }
        for parent in &request.parents {
            tree = self.remove(tree, parent.clone(), 0).await?;
        }
        let mut groups = base.and_then(|h| h.groups);
        for child in &request.children {
            let key = hex::encode(child.group);
            if Index::new(self).lookup(groups, &key).await?.is_some() {
                return Err(CatalogError::Conflict("group identity already assigned"));
            }
            groups = Index::new(self)
                .update(groups, &key, Some(child.incarnation.to_vec()), 0)
                .await?;
            tree = Some(self.insert(tree, child.clone(), 0).await?);
        }
        let tree = tree.ok_or(CatalogError::Invalid("empty directory"))?;
        tree.require_complete(self)?;
        let journal = wire::Journal {
            namespace: self.namespace.root_digest,
            operation: request.operation,
            request: mutation.digest,
            revision,
            previous: base.map(|h| h.journal),
            tree: tree.clone(),
        };
        let bytes = record::encode(wire::JOURNAL, &journal, wire::LIMIT)?;
        let id = record::digest(&bytes);
        self.immutable("journal", id, &bytes).await?;
        let operations = Index::new(self)
            .update(
                base.and_then(|h| h.operations),
                &hex::encode(request.operation),
                Some(id.to_vec()),
                0,
            )
            .await?;
        let head = wire::Head {
            namespace: self.namespace.root_digest,
            genesis: base.map_or(id, |h| h.genesis),
            revision,
            tree,
            operations,
            groups,
            journal: id,
        };
        self.check_head(&head)?;
        let bytes = record::encode(wire::HEAD, &head, wire::LIMIT)?;
        if request.expected.is_none() {
            // Unlike an immutable page, this object may advance after Create
            // succeeds and before its read-back. Confirm via its pinned journal.
            let _ = self
                .namespace
                .inner
                .store
                .put_opts(
                    &self.key("head"),
                    bytes.into(),
                    PutOptions::from(PutMode::Create),
                )
                .await;
            let (observed, _) = self.head().await?.ok_or(CatalogError::Unconfirmed)?;
            if observed.head != head {
                if observed.head.genesis != head.genesis {
                    return Err(CatalogError::Conflict("directory genesis winner"));
                }
                let (selected, _) = self.current().await?;
                return self
                    .confirmed(&selected.head, mutation)
                    .await?
                    .ok_or(CatalogError::Corrupt("missing genesis operation"));
            }
            let pin = record::encode(
                wire::PIN,
                &wire::Pin {
                    namespace: head.namespace,
                    genesis: head.genesis,
                },
                wire::LIMIT,
            )?;
            if self
                .namespace
                .inner
                .create_and_observe_bounded(&self.key("pin"), &pin, wire::LIMIT)
                .await?
                != pin
            {
                return Err(CatalogError::Corrupt("directory pin collision"));
            }
        } else {
            let (_, version) = current.ok_or(CatalogError::Conflict("missing directory base"))?;
            // A transport error can follow a committed CAS. Reconcile by the
            // root-selected operation index, including after subsequent writes.
            let _ = self
                .namespace
                .inner
                .store
                .put_opts(
                    &self.key("head"),
                    bytes.into(),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await;
        }
        let (after, _) = self
            .current()
            .await
            .map_err(|_| CatalogError::Unconfirmed)?;
        match self.confirmed(&after.head, mutation).await {
            Ok(Some(publication)) => Ok(publication),
            Ok(None) if request.expected.is_some_and(|base| base != after.digest) => {
                Err(CatalogError::Conflict("directory CAS lost"))
            }
            _ => Err(CatalogError::Unconfirmed),
        }
    }
}
