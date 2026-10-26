// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned complete topology inputs. Historical group observations describe a
//! proposal; each actual movement still requires fresh guarded admission.
mod inputs;
mod membership;

use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::host::RaftPeerNodeObservation;
use crate::raft::member_state::{JoiningMember, RepairPlan};
use cellar_logic::consensus::{MemberId, ProposalId};
use cellar_logic::raft_fleet::{balance_topology, TopologyGroup, TopologyPolicy, TopologyView};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub use cellar_logic::raft_fleet::TopologyPreview as FleetTopologyPlan;

/// Explicit topology constraints, independent of optional workload policy.
/// Host separation is always taken from the rooted fleet contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetTopologyPolicy {
    pub revision: u64,
    pub distinct_zones: bool,
    pub distinct_regions: bool,
    pub max_work: u64,
}

/// Trusted deployment occupancy plus explicit reserved headroom. The adapter
/// independently owns authenticated physical probes and retains rooted floors.
#[derive(Clone, Debug)]
pub struct FleetTopologyNodeResources {
    pub attestation: FleetResourceAttestation,
    pub reserve: FleetResourceBounds,
}

/// Deployment-supplied conservative per-history copy and seed bounds. These
/// values are not measurements of image size or estimated command throughput.
#[derive(Clone, Debug)]
pub struct FleetTopologyHistoryResources {
    pub group: GroupScope,
    pub resident: FleetResourceBounds,
    pub peak: FleetResourceBounds,
}

/// Exact explicit intent for this read-only collection. There must be one
/// resource input per participating node and live rooted history. Retired local
/// bindings remain occupied. Missing input never denotes zero usage.
#[derive(Clone, Debug)]
pub struct FleetTopologyRequest {
    pub keep: Vec<NodeId>,
    pub policy: FleetTopologyPolicy,
    pub nodes: Vec<FleetTopologyNodeResources>,
    pub histories: Vec<FleetTopologyHistoryResources>,
}

struct NodeInput {
    host: FleetHostAuthorization,
    capacity: FleetCapacityAttestation,
    resources: FleetTopologyNodeResources,
    physical: RaftPeerNodeObservation,
}

#[derive(Clone)]
struct GroupInput {
    resources: FleetTopologyHistoryResources,
    voters: BTreeMap<NodeId, MemberId>,
    revision: cellar_logic::consensus::LogIndex,
    high_water: u64,
}

/// Incremental complete-inventory collection at one immutable root. Each call
/// consumes at most one history and owns its three safe-ReadIndex RPCs. The
/// object retains at most 64 bounded physical inventories and 10,000 compact
/// group records, never a membership ledger for every business cell.
pub struct FleetTopologyVerification {
    catalog: FleetCatalog,
    snapshot: FleetSnapshot,
    observer: RaftCells,
    policy: TopologyPolicy,
    keep: BTreeSet<NodeId>,
    nodes: Vec<NodeInput>,
    bounds: BTreeMap<String, FleetTopologyHistoryResources>,
    groups: Vec<GroupInput>,
    cursor: Option<FleetCursor>,
    exhausted: bool,
    seen: u64,
    transcript: Sha256,
}

/// Opaque generated proposal, not a future health lease or admission grant.
/// Its first move can be submitted to reserve_movement, which independently
/// rechecks the exact root, fresh target occupancy and source quorum.
pub struct FleetTopologyPreview {
    plan: FleetTopologyPlan,
    revision: u64,
    first: Option<(GroupInput, FleetResourceAttestation)>,
    pub(super) snapshot: FleetSnapshot,
    pub(super) keep: BTreeSet<NodeId>,
    pub(super) policy: FleetTopologyPolicy,
    pub(super) oldest: u64,
}

impl FleetTopologyPreview {
    pub fn plan(&self) -> &FleetTopologyPlan {
        &self.plan
    }

    pub fn inventory_revision(&self) -> u64 {
        self.revision
    }

    /// Describe only the first serial move. A successor requires its predecessor
    /// to settle and a new complete collection; this method cannot authorize an
    /// entire stale schedule. Retain the exact request to retry an unknown CAS.
    pub fn first_movement(
        &self,
        identity: FleetRequestIdentity,
    ) -> Result<Option<FleetMovementRequest>> {
        super::super::histories::validate_request(&identity)?;
        if identity.expected_revision != self.revision {
            return Err(FleetCatalogError::Conflict);
        }
        let Some((group, attestation)) = &self.first else {
            return Ok(None);
        };
        let selected = self
            .plan
            .moves
            .first()
            .ok_or(FleetCatalogError::Corrupt("topology first move"))?;
        let obsolete = *group
            .voters
            .get(&selected.source.node.node)
            .ok_or(FleetCatalogError::Corrupt("topology source binding"))?;
        let next = group
            .high_water
            .checked_add(1)
            .filter(|n| *n < u64::MAX - 2)
            .ok_or(FleetCatalogError::Limit)?;
        let mut digest = Sha256::new();
        digest.update(b"cellar-topology-movement-1");
        digest.update(self.plan.input_revision);
        digest.update(identity.id);
        let digest: [u8; 32] = digest.finalize().into();
        let plan = RepairPlan {
            id: ProposalId::new(
                digest[..16]
                    .try_into()
                    .map_err(|_| FleetCatalogError::Limit)?,
            )
            .map_err(|_| FleetCatalogError::Invalid("topology movement identity"))?,
            policy_revision: self.plan.policy_revision,
            expected_revision: group.revision,
            obsolete,
            replacement: JoiningMember {
                member: MemberId::new(next).map_err(|_| FleetCatalogError::Limit)?,
                node: selected.target.node.node,
                disk: selected.target.node.disk,
            },
        };
        Ok(Some(FleetMovementRequest {
            resources: FleetResourceRequest {
                identity,
                group: selected.group.clone(),
                attestation: attestation.clone(),
                peak: group.resources.peak,
            },
            source: selected.source.node.node,
            plan,
        }))
    }
}

impl FleetCatalog {
    /// Own complete namespace inventory, empty settled movement-book checks,
    /// current quantified capacity and authenticated physical node inventories.
    /// A changed root invalidates collection. No mutation or exclusion occurs.
    pub async fn prepare_topology_verification(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetTopologyRequest,
        observer: &RaftCells,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetTopologyVerification> {
        self.check_snapshot(snapshot)?;
        let deadline = super::super::deadline::Deadline::new(self, timeout)?;
        let (policy, keep, bounds) = inputs::validate(self, snapshot, request)?;
        if namespaces.len() > super::super::namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        let hosts = self
            .run(deadline.remaining()?, async {
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                self.inner
                    .domain
                    .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)??;
                let book = self.resource_book(snapshot).await?;
                if !book.rows.is_empty() {
                    return Err(resource_error(LedgerError::MovementLimit));
                }
                self.check_resource_permits(snapshot, &[]).await?;
                let mut result = Vec::new();
                for record in self.node_records(snapshot).await? {
                    if record.state == FleetNodeState::Removed || record.isolation.is_some() {
                        continue;
                    }
                    let host = self.host_record(snapshot, record)?;
                    let input = request
                        .nodes
                        .iter()
                        .find(|input| input.attestation.boot.node == host.node().bytes())
                        .ok_or(FleetCatalogError::InventoryIncomplete)?;
                    let capacity = self.capacity_from(snapshot, host.node()).await?;
                    if input.attestation.boot != capacity.attestation().boot
                        || input.attestation.profile_revision
                            != capacity.attestation().profile_revision
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    result.push((host, capacity.attestation().clone(), input.clone()));
                }
                if result.len() != request.nodes.len()
                    || keep
                        .iter()
                        .any(|n| !result.iter().any(|(h, _, _)| h.node() == *n))
                {
                    return Err(FleetCatalogError::InventoryIncomplete);
                }
                Ok(result)
            })
            .await?;
        let mut transcript = Sha256::new();
        transcript.update(b"cellar-topology-input-1");
        transcript.update(hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?));
        transcript.update(policy.revision.to_be_bytes());
        transcript.update([
            u8::from(policy.distinct_hosts),
            u8::from(policy.distinct_zones),
            u8::from(policy.distinct_regions),
        ]);
        transcript.update(policy.max_work.to_be_bytes());
        transcript.update((keep.len() as u32).to_be_bytes());
        for node in &keep {
            transcript.update(node.bytes());
        }
        transcript.update((hosts.len() as u32).to_be_bytes());
        transcript.update((bounds.len() as u64).to_be_bytes());
        let mut nodes = Vec::with_capacity(hosts.len());
        for (host, capacity, resources) in hosts {
            let physical = observer
                .inspect_peer_physical_inventory(
                    host.node(),
                    snapshot.revision(),
                    deadline.remaining()?,
                )
                .await
                .map_err(FleetCatalogError::Observation)?;
            inputs::check_node(&host, &capacity, &physical)?;
            transcript.update(
                serde_json::to_vec(&resources.attestation)
                    .map_err(|_| FleetCatalogError::Invalid("topology attestation"))?,
            );
            transcript.update(
                serde_json::to_vec(&resources.reserve)
                    .map_err(|_| FleetCatalogError::Invalid("topology reserve"))?,
            );
            transcript.update(inputs::identity_digest(&physical)?);
            nodes.push(NodeInput {
                host,
                capacity,
                resources,
                physical,
            });
        }
        let value = FleetTopologyVerification {
            catalog: self.clone(),
            snapshot: snapshot.clone(),
            observer: observer.clone(),
            policy,
            keep,
            nodes,
            bounds,
            groups: Vec::new(),
            cursor: None,
            exhausted: false,
            seen: 0,
            transcript,
        };
        value.current(deadline.remaining()?).await?;
        Ok(value)
    }
}

impl FleetTopologyVerification {
    pub fn histories_checked(&self) -> u64 {
        self.seen
    }

    /// Refresh deployment evidence after a long walk without replaying stable
    /// membership observations. Copy bounds may only grow within this walk.
    pub(super) async fn refresh_resources(
        &mut self,
        request: &FleetTopologyRequest,
        timeout: Duration,
    ) -> Result<()> {
        self.current(timeout).await?;
        let (policy, keep, mut bounds) = inputs::validate(&self.catalog, &self.snapshot, request)?;
        if keep != self.keep
            || policy.revision != self.policy.revision
            || policy.distinct_hosts != self.policy.distinct_hosts
            || policy.distinct_zones != self.policy.distinct_zones
            || policy.distinct_regions != self.policy.distinct_regions
            || policy.max_work != self.policy.max_work
            || request.nodes.len() != self.nodes.len()
            || bounds.keys().ne(self.bounds.keys())
        {
            return Err(FleetCatalogError::Conflict);
        }
        for node in &self.nodes {
            let fresh = request
                .nodes
                .iter()
                .find(|n| n.attestation.boot.node == node.host.node().bytes())
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if fresh.attestation.boot != node.resources.attestation.boot
                || fresh.attestation.profile_revision != node.resources.attestation.profile_revision
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        for (key, bound) in &mut bounds {
            let original = self
                .bounds
                .get(key)
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            bound.resident = bound.resident.high_water(original.resident);
            bound.peak = bound
                .peak
                .high_water(original.peak)
                .high_water(bound.resident);
        }
        // Commit no partial update if validation above fails. These are trusted
        // deployment inputs; final authenticated node probes remain mandatory.
        for node in &mut self.nodes {
            node.resources = request
                .nodes
                .iter()
                .find(|n| n.attestation.boot.node == node.host.node().bytes())
                .ok_or(FleetCatalogError::InventoryIncomplete)?
                .clone();
        }
        for group in &mut self.groups {
            let key = self
                .catalog
                .binding(&group.resources.group)?
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("topology history key"))?;
            group.resources = bounds
                .get(&key)
                .ok_or(FleetCatalogError::InventoryIncomplete)?
                .clone();
        }
        self.bounds = bounds;
        Ok(())
    }

    async fn current(&self, timeout: Duration) -> Result<()> {
        self.catalog
            .run(timeout, async {
                if self.catalog.inner.current().await?.token != self.snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(())
            })
            .await
    }

    /// Consume exactly one rooted history. False means the complete walk is
    /// exhausted. Failure preserves the cursor; Reserved births never count as
    /// cancelled or live work. Each group supplies actual quorum observations.
    pub async fn verify_next(&mut self, timeout: Duration) -> Result<bool> {
        let catalog = self.catalog.clone();
        let deadline = super::super::deadline::Deadline::new(&catalog, timeout)?;
        self.current(deadline.remaining()?).await?;
        if self.exhausted {
            return Ok(false);
        }
        let page = catalog
            .run(
                deadline.remaining()?,
                catalog.inner.scan(
                    self.snapshot.head.tables[FleetTable::Histories.slot()],
                    self.cursor.as_ref(),
                    1,
                ),
            )
            .await?;
        if page.entries.len() > 1 || page.entries.is_empty() && page.next.is_some() {
            return Err(FleetCatalogError::Corrupt("topology inventory page"));
        }
        let mut group = None;
        let mut transcript = self.transcript.clone();
        if let Some(row) = page.entries.first() {
            let history = FleetHistory::decode(&row.value)?;
            if history
                .binding
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("topology history"))?
                != row.key
            {
                return Err(FleetCatalogError::Corrupt("topology history key"));
            }
            transcript.update(hash(&row.value));
            match *history.state() {
                FleetHistoryState::Cancelled { .. } => (),
                FleetHistoryState::Live => {
                    let bounds = self
                        .bounds
                        .get(&row.key)
                        .ok_or(FleetCatalogError::InventoryIncomplete)?;
                    let input = self
                        .verify_group(history, bounds.clone(), &deadline)
                        .await?;
                    transcript.update(input.revision.get().to_be_bytes());
                    transcript.update(input.high_water.to_be_bytes());
                    for (node, member) in &input.voters {
                        transcript.update(node.bytes());
                        transcript.update(member.get().to_be_bytes());
                    }
                    transcript.update(
                        serde_json::to_vec(&input.resources.resident)
                            .map_err(|_| FleetCatalogError::Limit)?,
                    );
                    transcript.update(
                        serde_json::to_vec(&input.resources.peak)
                            .map_err(|_| FleetCatalogError::Limit)?,
                    );
                    group = Some(input);
                }
                _ => return Err(FleetCatalogError::InventoryIncomplete),
            }
        }
        self.current(deadline.remaining()?).await?;
        deadline.remaining()?;
        if let Some(group) = group {
            self.groups.push(group);
        }
        if !page.entries.is_empty() {
            self.seen = self.seen.checked_add(1).ok_or(FleetCatalogError::Limit)?;
        }
        self.cursor = page.next;
        self.exhausted = self.cursor.is_none();
        self.transcript = transcript;
        Ok(!page.entries.is_empty())
    }

    /// Refresh node occupancy/profile inputs after the incremental group walk.
    /// Binding identity must still match; applied/completed progress may advance.
    /// No cross-cell five-second ReadIndex lease is required: the immutable root
    /// and settled permits pin configuration, and real admission rechecks quorum.
    pub async fn preview(&self, timeout: Duration) -> Result<FleetTopologyPreview> {
        self.preview_with_memory(timeout, None).await
    }

    /// A node-owned controller retains its planning reservation inside physical
    /// blocking work too, including when a response deadline drops the waiter.
    pub(super) async fn preview_with_memory(
        &self,
        timeout: Duration,
        memory: Option<Arc<Vec<crate::raft::budget::Permit>>>,
    ) -> Result<FleetTopologyPreview> {
        let catalog = self.catalog.clone();
        let deadline = super::super::deadline::Deadline::new(&catalog, timeout)?;
        self.current(deadline.remaining()?).await?;
        if !self.exhausted
            || self.seen != self.snapshot.head.history_count
            || self.groups.len() != self.bounds.len()
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let (nodes, oldest, digest) = self.refresh_nodes(&deadline).await?;
        let groups = self
            .groups
            .iter()
            .map(|group| {
                let scope = group.resources.group.clone();
                let mut occupied = Vec::new();
                for node in &self.nodes {
                    if node
                        .physical
                        .inventory
                        .as_ref()
                        .ok_or(FleetCatalogError::InventoryIncomplete)?
                        .history(&scope)
                        .map_err(|_| FleetCatalogError::Corrupt("topology physical scope"))?
                        .is_some()
                    {
                        occupied.push(node.host.node());
                    }
                }
                Ok(TopologyGroup {
                    scope,
                    revision: group.revision,
                    voters: group
                        .voters
                        .keys()
                        .copied()
                        .collect::<Vec<_>>()
                        .try_into()
                        .map_err(|_| FleetCatalogError::InventoryIncomplete)?,
                    occupied,
                    resident: group.resources.resident.resources(),
                    peak: group.resources.peak.resources(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let view = TopologyView {
            revision: digest,
            complete: true,
            nodes,
            groups,
        };
        let now = catalog.resource_now()?;
        let policy = self.policy;
        let task = catalog
            .inner
            .domain
            .blocking(move || {
                let _memory = memory;
                balance_topology(&view, policy, now)
            })
            .map_err(|_| FleetCatalogError::Closed)?;
        let plan = catalog
            .inner
            .domain
            .run(crate::asyncrt::timeout(deadline.remaining()?, task))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(FleetCatalogError::Topology)?;
        self.current(deadline.remaining()?).await?;
        catalog.require_resource_age(oldest)?;
        let first = plan
            .moves
            .first()
            .map(|selected| -> Result<_> {
                let group = self
                    .groups
                    .iter()
                    .find(|g| g.resources.group == selected.group)
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                let target = self
                    .nodes
                    .iter()
                    .find(|n| n.host.node() == selected.target.node.node)
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                Ok((group.clone(), target.resources.attestation.clone()))
            })
            .transpose()?;
        Ok(FleetTopologyPreview {
            plan,
            revision: self.snapshot.revision(),
            first,
            snapshot: self.snapshot.clone(),
            keep: self.keep.clone(),
            policy: FleetTopologyPolicy {
                revision: self.policy.revision,
                distinct_zones: self.policy.distinct_zones,
                distinct_regions: self.policy.distinct_regions,
                max_work: self.policy.max_work,
            },
            oldest,
        })
    }
}
