//! Rooted host enrollment is independent of any business-cell birth.
use super::*;
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, NodeId, ProposalId};
use serde::Deserialize;

const NODE_KIND: u8 = 47;
const MAX_NODES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FleetNodeState {
    Registered,
    Ready,
    Active,
    RestartPending,
    ResizePending,
    Draining,
    Evacuated,
    Removed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetNodeEligibility {
    pub receive_replicas: bool,
    pub receive_leadership: bool,
    pub evacuate_memberships: bool,
}

impl FleetNodeState {
    pub fn eligibility(self) -> FleetNodeEligibility {
        FleetNodeEligibility {
            receive_replicas: self == Self::Active,
            receive_leadership: self == Self::Active,
            evacuate_memberships: self == Self::Draining,
        }
    }

    fn permits_boot(self) -> bool {
        !matches!(self, Self::Evacuated | Self::Removed)
    }
}

/// Trusted deployment identity; registration alone grants no capacity or votes.
#[derive(Clone, Debug)]
pub struct FleetNodeSpec {
    pub node: NodeId,
    pub disk: DiskLineage,
    pub physical_host: IncarnationId,
    pub label: String,
    pub server_name: String,
    pub capability_digest: [u8; 32],
    /// Pin an already known engine on import. Fresh engines are permanently
    /// pinned during startup before their endpoint can be published.
    pub storage_instance: Option<EngineInstanceId>,
}

#[derive(Clone, Debug)]
pub struct FleetRequestIdentity {
    pub id: [u8; 16],
    pub operation: [u8; 16],
    pub principal: String,
    pub expected_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NodeRecord {
    fleet: [u8; 16],
    cluster: [u8; 16],
    pub(super) node: [u8; 16],
    disk: [u8; 16],
    pub(super) physical_host: [u8; 16],
    label: String,
    server_name: String,
    capability_digest: [u8; 32],
    registration: [u8; 16],
    registration_operation: [u8; 16],
    pub(super) state: FleetNodeState,
    epoch: u64,
    #[serde(deserialize_with = "required_nullable")]
    operation: Option<[u8; 16]>,
    #[serde(deserialize_with = "required_nullable")]
    pub(super) storage_instance: Option<[u8; 16]>,
}

fn required_nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(decoder)
}

impl NodeRecord {
    fn validate(&self, inner: &Inner) -> Result<()> {
        let identity = || FleetCatalogError::Corrupt("node identity");
        NodeId::new(self.node).map_err(|_| identity())?;
        DiskLineage::new(self.disk).map_err(|_| identity())?;
        IncarnationId::new(self.physical_host).map_err(|_| identity())?;
        ProposalId::new(self.registration).map_err(|_| identity())?;
        ProposalId::new(self.registration_operation).map_err(|_| identity())?;
        if let Some(id) = self.operation {
            ProposalId::new(id).map_err(|_| identity())?;
        }
        if let Some(id) = self.storage_instance {
            EngineInstanceId::new(id).map_err(|_| identity())?;
        }
        let operating = matches!(
            self.state,
            FleetNodeState::RestartPending
                | FleetNodeState::ResizePending
                | FleetNodeState::Draining
                | FleetNodeState::Evacuated
                | FleetNodeState::Removed
        );
        if self.fleet != inner.incarnation
            || self.cluster != inner.cluster
            || self.capability_digest == [0; 32]
            || self.epoch == 0
            || self.epoch >= u64::MAX - 2
            || operating != self.operation.is_some()
            || self.state != FleetNodeState::Registered && self.storage_instance.is_none()
            || crate::machine::validate_node_name(&self.label).is_err()
            || self.server_name.is_empty()
            || self.server_name.len() > 255
            || rustls::pki_types::ServerName::try_from(self.server_name.clone()).is_err()
        {
            return Err(FleetCatalogError::Corrupt("node descriptor"));
        }
        Ok(())
    }
}

/// Opaque authorization from a rooted node record. Host startup refreshes the
/// record; a saved capability is not a lease or placement-ready certificate.
#[derive(Clone)]
pub struct FleetHostAuthorization {
    catalog: FleetCatalog,
    record: NodeRecord,
    revision: u64,
    node: NodeId,
    disk: DiskLineage,
    physical_host: IncarnationId,
    cluster: ClusterId,
    storage_instance: Option<EngineInstanceId>,
}

impl fmt::Debug for FleetHostAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FleetHostAuthorization")
            .field("node", &self.node)
            .field("revision", &self.revision)
            .field("state", &self.record.state)
            .finish()
    }
}

impl FleetHostAuthorization {
    pub(crate) fn check_namespace(
        &self,
        config: &crate::raft::catalog::NamespaceConfig,
    ) -> Result<()> {
        if config.cluster != self.cluster()
            || config.managed_fleet.is_some()
                && self.catalog.bind_namespace(config.clone())? != *config
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    /// The one owned discovery worker has its own bounded read turn. Sharing
    /// the mutation adapter's turn would let a periodic read reject foreground
    /// history admission while waiting on storage. Its caller retains the
    /// separate physical metadata reservation through this reader's completion.
    pub(crate) fn trust_reader(&self) -> FleetTrustReader {
        let source = &self.catalog.inner;
        let mut authorization = self.clone();
        authorization.catalog = FleetCatalog {
            inner: Arc::new(Inner {
                bucket: source.bucket.clone(),
                domain: source.domain.clone(),
                prefix: source.prefix.clone(),
                cluster: source.cluster,
                incarnation: source.incarnation,
                turn: Mutex::new(()),
            }),
        };
        FleetTrustReader { authorization }
    }

    /// One current, complete rooted node cut. Process-view objects cannot add
    /// entries, and removed identities remain in this cut as tombstones.
    pub(crate) async fn trust_snapshot(&self, timeout: Duration) -> Result<FleetTrustSnapshot> {
        self.refresh_trust(None, timeout).await
    }

    pub(crate) async fn refresh_trust(
        &self,
        previous: Option<&FleetTrustSnapshot>,
        timeout: Duration,
    ) -> Result<FleetTrustSnapshot> {
        self.catalog
            .run(timeout, async {
                let snapshot = self.catalog.inner.current().await?;
                let root_digest = hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?);
                if let Some(previous) = previous {
                    if previous.fleet != self.fleet() || previous.cluster != self.cluster() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    if previous.revision == snapshot.revision() {
                        if previous.root_digest != root_digest {
                            return Err(FleetCatalogError::Conflict);
                        }
                        return Ok(previous.clone());
                    }
                }
                let records = self.catalog.node_records(&snapshot).await?;
                let nodes = records
                    .into_iter()
                    .map(|record| self.catalog.host_record(&snapshot, record))
                    .collect::<Result<Vec<_>>>()?;
                let local = nodes
                    .iter()
                    .find(|node| node.node == self.node)
                    .ok_or(FleetCatalogError::Conflict)?;
                self.require_successor(local)?;
                Ok(FleetTrustSnapshot {
                    revision: snapshot.revision(),
                    fleet: self.fleet(),
                    cluster: self.cluster(),
                    root_digest,
                    nodes: nodes.into(),
                })
            })
            .await
    }

    pub(crate) fn permits_transport(&self) -> bool {
        self.state() != FleetNodeState::Removed && self.storage_instance.is_some()
    }

    pub(crate) fn matches_peer(&self, peer: &crate::raft::transport_options::RaftPeerNode) -> bool {
        self.permits_transport()
            && peer.node == self.node
            && peer.disk == self.disk
            && peer.label == self.record.label
            && peer.server_name == self.record.server_name
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    pub fn disk(&self) -> DiskLineage {
        self.disk
    }

    pub fn physical_host(&self) -> IncarnationId {
        self.physical_host
    }

    pub fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub fn fleet(&self) -> [u8; 16] {
        self.record.fleet
    }

    pub fn storage_instance(&self) -> Option<EngineInstanceId> {
        self.storage_instance
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn state(&self) -> FleetNodeState {
        self.record.state
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.record.epoch
    }

    pub fn eligibility(&self) -> FleetNodeEligibility {
        self.state().eligibility()
    }

    pub(crate) fn label(&self) -> &str {
        &self.record.label
    }

    pub(crate) fn server_name(&self) -> &str {
        &self.record.server_name
    }

    pub(crate) fn capability_digest(&self) -> [u8; 32] {
        self.record.capability_digest
    }

    pub(crate) fn instance_matches(&self, instance: EngineInstanceId) -> bool {
        self.record
            .storage_instance
            .is_none_or(|expected| expected == instance.bytes())
    }

    pub(crate) async fn group_authorization(
        &self,
        scope: &cellar_logic::consensus::GroupScope,
        timeout: Duration,
    ) -> Result<FleetGroupAuthorization> {
        let start = self
            .catalog
            .inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        let snapshot = self.catalog.read(timeout).await?;
        let elapsed = self
            .catalog
            .inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?
            .saturating_sub(start);
        let remaining = timeout
            .checked_sub(Duration::from_millis(elapsed))
            .filter(|d| !d.is_zero())
            .ok_or(FleetCatalogError::Unconfirmed)?;
        self.catalog
            .authorize_group(&snapshot, scope, remaining)
            .await
    }

    pub(crate) async fn refresh(&self, timeout: Duration) -> Result<Self> {
        self.catalog
            .run(timeout, async {
                let snapshot = self.catalog.inner.current().await?;
                let current = self.catalog.host_from(&snapshot, self.node).await?;
                self.require_successor(&current)?;
                Ok(current)
            })
            .await
    }

    fn require_successor(&self, current: &Self) -> Result<()> {
        let a = &self.record;
        let b = &current.record;
        if a.node != b.node
            || a.disk != b.disk
            || a.physical_host != b.physical_host
            || a.label != b.label
            || a.server_name != b.server_name
            || a.capability_digest != b.capability_digest
            || a.registration != b.registration
            || a.registration_operation != b.registration_operation
            || a.storage_instance.is_some() && a.storage_instance != b.storage_instance
            || b.epoch < a.epoch
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    pub(crate) async fn bind_storage(
        &self,
        instance: EngineInstanceId,
        timeout: Duration,
    ) -> Result<Self> {
        self.catalog
            .run(timeout, async {
                let snapshot = self.catalog.inner.current().await?;
                let mut current = self.catalog.host_from(&snapshot, self.node).await?;
                self.require_successor(&current)?;
                if let Some(pinned) = current.record.storage_instance {
                    if pinned != instance.bytes() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    return Ok(current);
                }
                current.record.storage_instance = Some(instance.bytes());
                current.record.epoch = current
                    .record
                    .epoch
                    .checked_add(1)
                    .ok_or(FleetCatalogError::Limit)?;
                current.record.validate(&self.catalog.inner)?;
                let value = encode(NODE_KIND, &current.record, 4096)?;
                // Include the input revision. After an ambiguous reply the pinned
                // field reconciles success; a newer unpinned root fences the old
                // CAS and allows a distinct attempt without changing its payload.
                let mut identity = b"cellar-fleet-host-storage-1".to_vec();
                identity.extend_from_slice(&snapshot.revision().to_be_bytes());
                identity.extend_from_slice(&value);
                let digest = hash(&identity);
                let mut id = [0; 16];
                id.copy_from_slice(&digest[..16]);
                let mutation = FleetMutation {
                    id,
                    operation: current.record.registration_operation,
                    principal: "fleet-host-storage".into(),
                    expected_revision: snapshot.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Nodes,
                        key: hex::encode(current.record.node),
                        value: Some(value),
                    }],
                };
                mutation.validate()?;
                let committed = self.catalog.commit_inner(&snapshot, &mutation).await?;
                let observed = self
                    .catalog
                    .host_from(&committed.snapshot, self.node)
                    .await?;
                self.require_successor(&observed)?;
                if observed.storage_instance() != Some(instance) {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(observed)
            })
            .await
    }
}

/// Read-only surface of the single separately budgeted directory adapter.
#[derive(Clone)]
pub(crate) struct FleetTrustReader {
    authorization: FleetHostAuthorization,
}

impl FleetTrustReader {
    pub(crate) async fn refresh(
        &self,
        previous: Option<&FleetTrustSnapshot>,
        timeout: Duration,
    ) -> Result<FleetTrustSnapshot> {
        self.authorization.refresh_trust(previous, timeout).await
    }
}

/// Opaque complete node-table cut, produced only by a bounded current-root read.
/// Eligibility is deliberately separate: restart, resize and evacuation keep
/// transport available until final removal has resolved membership work.
#[derive(Clone, Debug)]
pub(crate) struct FleetTrustSnapshot {
    revision: u64,
    fleet: [u8; 16],
    cluster: ClusterId,
    root_digest: [u8; 32],
    nodes: Arc<[FleetHostAuthorization]>,
}

impl FleetTrustSnapshot {
    pub(crate) fn fleet(&self) -> [u8; 16] {
        self.fleet
    }

    pub(crate) fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn nodes(&self) -> &[FleetHostAuthorization] {
        &self.nodes
    }

    pub(crate) fn node(&self, node: NodeId) -> Option<&FleetHostAuthorization> {
        self.nodes.iter().find(|entry| entry.node == node)
    }

    pub(crate) fn check_successor(&self, previous: &Self) -> Result<()> {
        if self.fleet != previous.fleet
            || self.cluster != previous.cluster
            || self.revision < previous.revision
        {
            return Err(FleetCatalogError::Conflict);
        }
        for old in previous.nodes.iter() {
            let current = self.node(old.node).ok_or(FleetCatalogError::Conflict)?;
            old.require_successor(current)?;
            if old.record.epoch == current.record.epoch && old.record != current.record
                || old.state() == FleetNodeState::Removed
                    && current.state() != FleetNodeState::Removed
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        if self.revision == previous.revision
            && (self.root_digest != previous.root_digest
                || self.nodes.len() != previous.nodes.len()
                || self.nodes.iter().any(|current| {
                    previous
                        .node(current.node)
                        .is_none_or(|old| old.record != current.record)
                }))
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }
}

impl FleetCatalog {
    pub async fn register_node(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        spec: &FleetNodeSpec,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        if spec.label.len() > 128 || spec.server_name.len() > 255 || request.principal.len() > 256 {
            return Err(FleetCatalogError::Limit);
        }
        let record = NodeRecord {
            fleet: self.inner.incarnation,
            cluster: self.inner.cluster,
            node: spec.node.bytes(),
            disk: spec.disk.bytes(),
            physical_host: spec.physical_host.bytes(),
            label: spec.label.clone(),
            server_name: spec.server_name.clone(),
            capability_digest: spec.capability_digest,
            registration: request.id,
            registration_operation: request.operation,
            state: FleetNodeState::Registered,
            epoch: 1,
            operation: None,
            storage_instance: spec.storage_instance.map(|id| id.bytes()),
        };
        record
            .validate(&self.inner)
            .map_err(|_| FleetCatalogError::Invalid("node descriptor"))?;
        let mutation = FleetMutation {
            id: request.id,
            operation: request.operation,
            expected_revision: request.expected_revision,
            principal: request.principal.clone(),
            changes: vec![FleetChange {
                table: FleetTable::Nodes,
                key: hex::encode(record.node),
                value: Some(encode(NODE_KIND, &record, 4096)?),
            }],
        };
        mutation.validate()?;
        self.run(timeout, async {
            // Reconciliation precedes uniqueness checks: an exact retry can
            // observe its own registered node even after later lifecycle work.
            let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
            if self
                .inner
                .committed(&snapshot.head, &hex::encode(mutation.id), digest)
                .await?
                .is_none()
            {
                let nodes = self.node_records(snapshot).await?;
                if nodes.len() >= MAX_NODES {
                    return Err(FleetCatalogError::Limit);
                }
                if nodes.iter().any(|n| {
                    n.node == record.node
                        || n.disk == record.disk
                        || n.label == record.label && n.state != FleetNodeState::Removed
                }) {
                    return Err(FleetCatalogError::Conflict);
                }
            }
            self.commit_inner(snapshot, &mutation).await
        })
        .await
    }

    pub async fn authorize_host(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
        timeout: Duration,
    ) -> Result<FleetHostAuthorization> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, self.host_from(snapshot, node)).await
    }

    pub(super) async fn node_records(&self, snapshot: &FleetSnapshot) -> Result<Vec<NodeRecord>> {
        let page = self
            .inner
            .scan(
                snapshot.head.tables[FleetTable::Nodes.slot()],
                None,
                MAX_NODES + 1,
            )
            .await?;
        if page.next.is_some() || page.entries.len() > MAX_NODES {
            return Err(FleetCatalogError::Limit);
        }
        let mut nodes: Vec<NodeRecord> = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let record: NodeRecord = decode(NODE_KIND, &entry.value, 4096)?;
            record.validate(&self.inner)?;
            if entry.key != hex::encode(record.node)
                || nodes.iter().any(|n| {
                    n.node == record.node
                        || n.disk == record.disk
                        || n.label == record.label
                            && n.state != FleetNodeState::Removed
                            && record.state != FleetNodeState::Removed
                })
            {
                return Err(FleetCatalogError::Corrupt("duplicate node identity"));
            }
            nodes.push(record);
        }
        Ok(nodes)
    }

    pub(super) async fn host_from(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
    ) -> Result<FleetHostAuthorization> {
        let record = self
            .node_records(snapshot)
            .await?
            .into_iter()
            .find(|n| n.node == node.bytes())
            .ok_or(FleetCatalogError::Conflict)?;
        if !record.state.permits_boot() {
            return Err(FleetCatalogError::Conflict);
        }
        self.host_record(snapshot, record)
    }

    fn host_record(
        &self,
        snapshot: &FleetSnapshot,
        record: NodeRecord,
    ) -> Result<FleetHostAuthorization> {
        Ok(FleetHostAuthorization {
            node: NodeId::new(record.node).map_err(|_| FleetCatalogError::Corrupt("node"))?,
            disk: DiskLineage::new(record.disk).map_err(|_| FleetCatalogError::Corrupt("disk"))?,
            physical_host: IncarnationId::new(record.physical_host)
                .map_err(|_| FleetCatalogError::Corrupt("physical host"))?,
            cluster: ClusterId::new(record.cluster)
                .map_err(|_| FleetCatalogError::Corrupt("cluster"))?,
            storage_instance: record
                .storage_instance
                .map(EngineInstanceId::new)
                .transpose()
                .map_err(|_| FleetCatalogError::Corrupt("storage instance"))?,
            catalog: self.clone(),
            record,
            revision: snapshot.revision(),
        })
    }
}
