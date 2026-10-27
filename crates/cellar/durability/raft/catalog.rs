// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable namespace/birth selection and permanent initial-member claims.
//! Discovery is not mutation authority. Start runtimes parked; only a verified
//! local-state/claim pair may attach an initial voting member.

use super::cache::GroupLog;
use super::catalog_wire as wire;
use super::runtime::RaftRuntime;
use super::storage::{GroupBootstrap, LogError, ReplicaLog};
use crate::application::consensus::protocol::WireScope;
use cellar_logic::consensus::{
    BootstrapClaim, ClusterId, DiskLineage, Durability, GroupScope, MemberId, NamespaceId, NodeId,
    ReplicaIdentity,
};
use cellar_logic::NamespaceMode;
use futures_util::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode, PutOptions, UpdateVersion};
use std::{collections::BTreeSet, fmt, sync::Arc};

#[path = "node_directory.rs"]
mod directory;
pub use directory::{NodePublication, NodeRegistration, NodeView};

#[derive(Debug)]
pub enum CatalogError {
    Invalid(&'static str),
    Conflict(&'static str),
    Corrupt(&'static str),
    Limit,
    WrongMode,
    NotInitialMember,
    LostStableState,
    Unconfirmed,
    Storage(object_store::Error),
    Log(LogError),
    Runtime(super::completion::RuntimeError),
    Version(super::version::VersionError),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft catalog: {self:?}")
    }
}

impl std::error::Error for CatalogError {}

#[derive(Clone, Debug)]
pub struct CatalogOptions {
    pub prefix: String,
    pub max_record_bytes: usize,
    /// Whole-operation bound for mandatory physical storage registration.
    pub node_timeout_ms: u64,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            max_record_bytes: 64 * 1024,
            node_timeout_ms: 5000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoterPolicy {
    ThreeVoters,
    SingleVoterDevelopment,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceConfig {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub mode: NamespaceMode,
    pub schema_digest: [u8; 32],
    pub minimum_durability: Durability,
    pub voters: VoterPolicy,
    pub image_storage: RaftImageStorage,
}

/// Immutable physical representation for every cell in one Raft namespace.
/// Changing it requires a fresh namespace; images are never converted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RaftImageStorage {
    Materialized,
    Extents,
}

impl NamespaceConfig {
    pub fn new(
        cluster: ClusterId,
        namespace: NamespaceId,
        mode: NamespaceMode,
        schema_digest: [u8; 32],
    ) -> Self {
        Self {
            cluster,
            namespace,
            mode,
            schema_digest,
            minimum_durability: Durability::Quorum,
            voters: VoterPolicy::ThreeVoters,
            image_storage: RaftImageStorage::Materialized,
        }
    }
}

struct Inner {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    limit: usize,
    owner: Arc<()>,
    node_timeout_ms: u64,
}

#[derive(Clone)]
pub struct Catalog {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub struct NamespaceCatalog {
    inner: Arc<Inner>,
    config: NamespaceConfig,
    root_digest: [u8; 32],
    data_prefix: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitialMember {
    pub member: MemberId,
    pub node: NodeId,
}

#[derive(Clone, Debug)]
pub struct BirthProposal {
    pub scope: GroupScope,
    pub voters: Vec<InitialMember>,
    pub learners: Vec<InitialMember>,
}

/// Constructed only after a canonical immutable record and its reservation have
/// been read back from the selected namespace. Its placement never overrides
/// committed Raft configuration once a member has initialized.
#[derive(Clone, Debug)]
pub struct GroupBirth {
    scope: GroupScope,
    voters: Vec<InitialMember>,
    learners: Vec<InitialMember>,
    digest: [u8; 32],
    root_digest: [u8; 32],
    catalog_owner: Arc<()>,
}

impl GroupBirth {
    pub fn scope(&self) -> &GroupScope {
        &self.scope
    }

    pub fn voters(&self) -> &[InitialMember] {
        &self.voters
    }

    pub fn learners(&self) -> &[InitialMember] {
        &self.learners
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Process-local capability tied to the exact engine owner that synced the
/// initial state and observed its matching permanent object-store claim.
pub struct BootstrapPermit {
    local: GroupLog,
    birth: GroupBirth,
}

impl BootstrapPermit {
    pub fn replica(&self) -> &ReplicaIdentity {
        self.local.identity()
    }

    pub async fn attach(&self, runtime: &RaftRuntime) -> Result<(), CatalogError> {
        let local = runtime
            .log()
            .group(&self.birth.scope)
            .map_err(CatalogError::Log)?;
        if local.identity() != self.local.identity()
            || !Arc::ptr_eq(&local.owner, &self.local.owner)
        {
            return Err(CatalogError::Conflict("bootstrap engine owner"));
        }
        runtime
            .attach(&self.birth.scope)
            .await
            .map_err(CatalogError::Runtime)
    }
}

impl Catalog {
    pub fn new(store: Arc<dyn ObjectStore>, options: CatalogOptions) -> Result<Self, CatalogError> {
        if !(1024..=64 * 1024).contains(&options.max_record_bytes) {
            return Err(CatalogError::Limit);
        }
        if !(1..=60_000).contains(&options.node_timeout_ms) {
            return Err(CatalogError::Invalid("node registration timeout"));
        }
        let prefix = crate::replica_storage::normalize_prefix(&options.prefix)
            .map_err(|_| CatalogError::Invalid("catalog prefix"))?;
        if prefix.len() > 1024 {
            return Err(CatalogError::Limit);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                prefix,
                limit: options.max_record_bytes,
                owner: Arc::new(()),
                node_timeout_ms: options.node_timeout_ms,
            }),
        })
    }

    pub async fn open_namespace(
        &self,
        config: NamespaceConfig,
    ) -> Result<NamespaceCatalog, CatalogError> {
        self.namespace(config, true).await
    }

    /// Read the exact existing root without creating namespace metadata.
    pub async fn existing_namespace(
        &self,
        config: NamespaceConfig,
    ) -> Result<NamespaceCatalog, CatalogError> {
        self.namespace(config, false).await
    }

    async fn namespace(
        &self,
        config: NamespaceConfig,
        create: bool,
    ) -> Result<NamespaceCatalog, CatalogError> {
        if config.schema_digest == [0; 32] {
            return Err(CatalogError::Invalid("namespace schema digest"));
        }
        let data_prefix = match config.mode {
            NamespaceMode::Raft => format!(
                "{}raft/{}/{}/",
                self.inner.prefix,
                hex::encode(config.cluster.bytes()),
                hex::encode(config.namespace.bytes())
            ),
            NamespaceMode::Fleet => format!("{}cells/", self.inner.prefix),
        };
        let expected = wire::Root::new(&config, data_prefix.clone());
        expected.validate()?;
        let key = Path::from(format!(
            "{}namespaces/{}/root",
            self.inner.prefix,
            hex::encode(config.namespace.bytes())
        ));
        let bytes = wire::encode(wire::ROOT, &expected, self.inner.limit)?;
        let observed = if create {
            self.inner.create_and_observe(&key, &bytes).await?
        } else {
            self.inner
                .read(&key)
                .await?
                .ok_or(CatalogError::Conflict("namespace missing"))?
        };
        let root: wire::Root = wire::decode(wire::ROOT, &observed, self.inner.limit)?;
        root.validate()?;
        if root != expected {
            return Err(CatalogError::Conflict("immutable namespace root"));
        }
        Ok(NamespaceCatalog {
            inner: self.inner.clone(),
            config,
            root_digest: wire::digest(&observed),
            data_prefix,
        })
    }
}

impl NamespaceCatalog {
    pub fn mode(&self) -> NamespaceMode {
        self.config.mode
    }

    pub fn data_prefix(&self) -> &str {
        &self.data_prefix
    }

    pub fn minimum_durability(&self) -> Durability {
        self.config.minimum_durability
    }

    pub fn config(&self) -> &NamespaceConfig {
        &self.config
    }

    /// Bind archive reads/writes to a selected immutable namespace and birth.
    /// This capability supplies recovery material, never member authorization.
    pub fn cell_archive(
        &self,
        birth: &GroupBirth,
        host: cellar_ltx::LtxHost,
        limits: super::archive::ArchiveLimits,
    ) -> Result<super::archive::CellArchive, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        self.check_birth(birth)?;
        limits.validate().map_err(|_| CatalogError::Limit)?;
        Ok(super::archive::CellArchive::new(
            self.inner.store.clone(),
            format!("{}/archives", self.data_prefix),
            birth,
            self.root_digest,
            self.config.schema_digest,
            limits,
            host,
        )
        .with_image_storage(self.config.image_storage))
    }

    pub fn require_mode(&self, mode: NamespaceMode) -> Result<(), CatalogError> {
        if mode != self.config.mode {
            return Err(CatalogError::WrongMode);
        }
        Ok(())
    }

    /// Persist only public verification metadata. Rolling rotation distributes
    /// new verifiers before switching signers. Any unexpired registered key
    /// must remain configured, including after a restart. The catalog cannot
    /// authenticate a token by supplying an otherwise untrusted public key.
    pub async fn require_version_keys(
        &self,
        keys: &super::version::VersionKeys,
        now_ms: u64,
    ) -> Result<(), CatalogError> {
        use super::version::VersionVerificationKey;
        self.require_mode(NamespaceMode::Raft)?;
        let path = Path::from(format!("{}version-keys", self.data_prefix));
        let record = wire::VersionKeys {
            root_digest: self.root_digest,
            keys: keys
                .verification_keys()
                .into_iter()
                .map(|key| wire::VersionKey {
                    public: key.public_bytes(),
                    verify_until_ms: key.verify_until_ms(),
                })
                .collect(),
        };
        let desired = wire::encode(wire::VERSION_KEYS, &record, self.inner.limit)?;
        for _ in 0..8 {
            let previous = self.inner.read_versioned(&path).await?;
            let mode = if let Some((bytes, version)) = previous {
                let record: wire::VersionKeys =
                    wire::decode(wire::VERSION_KEYS, &bytes, self.inner.limit)?;
                if record.root_digest != self.root_digest
                    || record.keys.is_empty()
                    || record.keys.len() > 64
                {
                    return Err(CatalogError::Corrupt("version key registry"));
                }
                let registered = record
                    .keys
                    .into_iter()
                    .map(|key| {
                        VersionVerificationKey::new(key.public, key.verify_until_ms)
                            .map_err(CatalogError::Version)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if registered
                    .windows(2)
                    .any(|pair| pair[0].id() >= pair[1].id())
                {
                    return Err(CatalogError::Corrupt("version key ordering"));
                }
                keys.validate_retention(&registered, now_ms)
                    .map_err(CatalogError::Version)?;
                if bytes == desired {
                    return Ok(());
                }
                if version.e_tag.is_none() && version.version.is_none() {
                    return Err(CatalogError::Unconfirmed);
                }
                PutMode::Update(version)
            } else {
                keys.validate_retention(&[], now_ms)
                    .map_err(CatalogError::Version)?;
                PutMode::Create
            };
            // A lost conditional-write response never establishes completion.
            // Confirm the exact durable record; otherwise re-read and validate
            // retention before another bounded conditional attempt.
            let _ = self
                .inner
                .store
                .put_opts(&path, desired.clone().into(), PutOptions::from(mode))
                .await;
            if self.inner.read(&path).await?.as_ref() == Some(&desired) {
                return Ok(());
            }
        }
        Err(CatalogError::Unconfirmed)
    }

    /// Concurrent creators follow the winning birth, including its placement;
    /// retry never overwrites that record or substitutes another incarnation.
    pub async fn resolve_birth(
        &self,
        mut proposal: BirthProposal,
    ) -> Result<GroupBirth, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        self.check_scope(&proposal.scope)?;
        proposal.voters.sort_by_key(|m| m.member);
        proposal.learners.sort_by_key(|m| m.member);
        self.check_members(&proposal.voters, &proposal.learners)?;
        let key = self.birth_key(&proposal.scope.cell);
        if let Some(bytes) = self.inner.read(&key).await? {
            return self.decode_birth(&proposal.scope.cell, &bytes).await;
        }
        let reservation = wire::Reservation {
            root_digest: self.root_digest,
            scope: WireScope::from(&proposal.scope),
        };
        let reserved = wire::encode(wire::RESERVATION, &reservation, self.inner.limit)?;
        let observed = self
            .inner
            .create_and_observe(&self.reservation_key(&proposal.scope), &reserved)
            .await?;
        if observed != reserved {
            return Err(CatalogError::Conflict("group identity already reserved"));
        }
        let birth = wire::Birth {
            root_digest: self.root_digest,
            scope: WireScope::from(&proposal.scope),
            voters: proposal
                .voters
                .into_iter()
                .map(wire::Member::encode)
                .collect(),
            learners: proposal
                .learners
                .into_iter()
                .map(wire::Member::encode)
                .collect(),
            empty_digest: wire::digest(&[]),
        };
        let bytes = wire::encode(wire::BIRTH, &birth, self.inner.limit)?;
        let observed = self.inner.create_and_observe(&key, &bytes).await?;
        self.decode_birth(&proposal.scope.cell, &observed).await
    }

    pub async fn birth(&self, cell: &str) -> Result<Option<GroupBirth>, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        if cell.is_empty() || cell.len() > 1024 || cell.chars().any(char::is_control) {
            return Err(CatalogError::Invalid("cell identity"));
        }
        match self.inner.read(&self.birth_key(cell)).await? {
            Some(bytes) => Ok(Some(self.decode_birth(cell, &bytes).await?)),
            None => Ok(None),
        }
    }

    async fn decode_birth(&self, cell: &str, bytes: &[u8]) -> Result<GroupBirth, CatalogError> {
        let birth: wire::Birth = wire::decode(wire::BIRTH, bytes, self.inner.limit)?;
        let scope = birth
            .scope
            .clone()
            .decode()
            .map_err(|_| CatalogError::Corrupt("birth scope"))?;
        self.check_scope(&scope)?;
        if scope.cell != cell
            || birth.root_digest != self.root_digest
            || birth.empty_digest != wire::digest(&[])
        {
            return Err(CatalogError::Conflict("birth namespace/cell/empty state"));
        }
        let voters = birth
            .voters
            .iter()
            .map(wire::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let learners = birth
            .learners
            .iter()
            .map(wire::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        self.check_members(&voters, &learners)?;
        let reservation = self
            .inner
            .read(&self.reservation_key(&scope))
            .await?
            .ok_or(CatalogError::Corrupt("missing group reservation"))?;
        let value: wire::Reservation =
            wire::decode(wire::RESERVATION, &reservation, self.inner.limit)?;
        if value.root_digest != self.root_digest
            || value
                .scope
                .decode()
                .map_err(|_| CatalogError::Corrupt("reservation scope"))?
                != scope
        {
            return Err(CatalogError::Conflict("group reservation"));
        }
        Ok(GroupBirth {
            scope,
            voters,
            learners,
            digest: wire::digest(bytes),
            root_digest: self.root_digest,
            catalog_owner: self.inner.owner.clone(),
        })
    }

    /// Use only with a parked runtime. Read the remote claim before deciding
    /// whether any blank initialization is legal, then sync local genesis before
    /// conditionally creating and reading back the one-time member claim.
    pub async fn bootstrap_member(
        &self,
        birth: &GroupBirth,
        log: &ReplicaLog,
    ) -> Result<BootstrapPermit, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        self.check_birth(birth)?;
        let selected = birth
            .voters
            .iter()
            .chain(&birth.learners)
            .find(|m| m.node == log.limits().node)
            .ok_or(CatalogError::NotInitialMember)?;
        self.require_node_storage(log).await?;
        let expected = BootstrapClaim {
            scope: birth.scope.clone(),
            member: selected.member,
            disk: log.limits().disk,
            birth_digest: birth.digest,
        };
        let key = self.claim_key(birth, selected.member);
        let remote = self.read_claim(&key, birth).await?;
        let local = match log.group(&birth.scope) {
            Ok(local) => Some(local),
            Err(LogError::NotFound) => None,
            Err(error) => return Err(CatalogError::Log(error)),
        };
        if remote.as_ref().is_some_and(|remote| remote != &expected) {
            return Err(CatalogError::Conflict("permanent bootstrap claim"));
        }
        if remote.is_some() && local.is_none() {
            return Err(CatalogError::LostStableState);
        }
        // This call either durably creates genesis or verifies the existing
        // mapping's exact member, birth digest and initial ConfState.
        let local = log
            .create_group(GroupBootstrap {
                scope: birth.scope.clone(),
                member: selected.member,
                birth_digest: birth.digest,
                voters: birth.voters.iter().map(|m| m.member).collect(),
                learners: birth.learners.iter().map(|m| m.member).collect(),
            })
            .await
            .map_err(CatalogError::Log)?;
        let payload = wire::Claim {
            root_digest: self.root_digest,
            scope: WireScope::from(&birth.scope),
            member: selected.member.get(),
            disk: expected.disk.bytes(),
            birth_digest: birth.digest,
        };
        let bytes = wire::encode(wire::CLAIM, &payload, self.inner.limit)?;
        let observed = self.inner.create_and_observe(&key, &bytes).await?;
        if observed != bytes {
            return Err(CatalogError::Conflict("permanent bootstrap claim"));
        }
        // Compare through the pure reconciliation contract too: lost stable
        // state may not be recovered by assigning a different local directory.
        let remote = self
            .read_claim(&key, birth)
            .await?
            .ok_or(CatalogError::Unconfirmed)?;
        expected
            .reconcile(Some(&expected), Some(&remote))
            .map_err(|_| CatalogError::LostStableState)?;
        Ok(BootstrapPermit {
            local,
            birth: birth.clone(),
        })
    }

    /// Resume a joined member or establish that a first seed is still legal.
    /// A permanent claim with missing local stable state is never reset.
    pub(crate) async fn resume_joined(
        &self,
        birth: &GroupBirth,
        log: &ReplicaLog,
        member: MemberId,
    ) -> Result<Option<BootstrapPermit>, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        self.check_birth(birth)?;
        if birth
            .voters
            .iter()
            .chain(&birth.learners)
            .any(|m| m.member == member)
        {
            return Err(CatalogError::Conflict("joined member is initial"));
        }
        self.require_node_storage(log).await?;
        let expected = BootstrapClaim {
            scope: birth.scope.clone(),
            member,
            disk: log.limits().disk,
            birth_digest: birth.digest,
        };
        let key = self.claim_key(birth, member);
        let remote = self.read_claim(&key, birth).await?;
        if remote.as_ref().is_some_and(|claim| claim != &expected) {
            return Err(CatalogError::Conflict("permanent joined claim"));
        }
        let local = match log.group(&birth.scope) {
            Ok(local) => local,
            Err(LogError::NotFound) if remote.is_none() => return Ok(None),
            Err(LogError::NotFound) => return Err(CatalogError::LostStableState),
            Err(error) => return Err(CatalogError::Log(error)),
        };
        let mut initial = ::raft::prelude::ConfState::default();
        initial.set_voters(birth.voters.iter().map(|m| m.member.get()).collect());
        initial.set_learners(birth.learners.iter().map(|m| m.member.get()).collect());
        let valid = {
            let cache = local.read().map_err(CatalogError::Log)?;
            local.birth_digest == birth.digest
                && local.identity().member == member
                && cache
                    .membership
                    .same_birth(&super::member_state::GroupMembership::initial(&initial))
                && cache.membership.permits(local.identity())
                && cache
                    .membership
                    .joined(member)
                    .map_err(CatalogError::Log)?
                    .is_some()
                && cache
                    .conf_state
                    .voters
                    .iter()
                    .chain(&cache.conf_state.voters_outgoing)
                    .chain(&cache.conf_state.learners)
                    .chain(&cache.conf_state.learners_next)
                    .any(|id| *id == member.get())
        };
        if !valid {
            return Err(CatalogError::Conflict("joined stable state"));
        }
        let payload = wire::Claim {
            root_digest: self.root_digest,
            scope: WireScope::from(&birth.scope),
            member: member.get(),
            disk: expected.disk.bytes(),
            birth_digest: birth.digest,
        };
        let bytes = wire::encode(wire::CLAIM, &payload, self.inner.limit)?;
        let observed = self.inner.create_and_observe(&key, &bytes).await?;
        if observed != bytes {
            return Err(CatalogError::Conflict("permanent joined claim"));
        }
        let confirmed = self
            .read_claim(&key, birth)
            .await?
            .ok_or(CatalogError::Unconfirmed)?;
        expected
            .reconcile(Some(&expected), Some(&confirmed))
            .map_err(|_| CatalogError::LostStableState)?;
        Ok(Some(BootstrapPermit {
            local,
            birth: birth.clone(),
        }))
    }

    async fn read_claim(
        &self,
        key: &Path,
        birth: &GroupBirth,
    ) -> Result<Option<BootstrapClaim>, CatalogError> {
        let Some(bytes) = self.inner.read(key).await? else {
            return Ok(None);
        };
        let claim: wire::Claim = wire::decode(wire::CLAIM, &bytes, self.inner.limit)?;
        let scope = claim
            .scope
            .decode()
            .map_err(|_| CatalogError::Corrupt("claim scope"))?;
        if claim.root_digest != self.root_digest
            || scope != birth.scope
            || claim.birth_digest != birth.digest
        {
            return Err(CatalogError::Conflict("claim birth/history"));
        }
        Ok(Some(BootstrapClaim {
            scope,
            member: MemberId::new(claim.member)
                .map_err(|_| CatalogError::Corrupt("claim member"))?,
            disk: DiskLineage::new(claim.disk).map_err(|_| CatalogError::Corrupt("claim disk"))?,
            birth_digest: claim.birth_digest,
        }))
    }

    fn check_birth(&self, birth: &GroupBirth) -> Result<(), CatalogError> {
        self.check_scope(&birth.scope)?;
        if birth.root_digest != self.root_digest
            || !Arc::ptr_eq(&birth.catalog_owner, &self.inner.owner)
        {
            return Err(CatalogError::Conflict("birth catalog binding"));
        }
        Ok(())
    }

    fn check_scope(&self, scope: &GroupScope) -> Result<(), CatalogError> {
        scope
            .validate()
            .map_err(|_| CatalogError::Invalid("group scope"))?;
        if scope.cluster != self.config.cluster || scope.namespace != self.config.namespace {
            return Err(CatalogError::Conflict("namespace scope"));
        }
        Ok(())
    }

    fn check_members(
        &self,
        voters: &[InitialMember],
        learners: &[InitialMember],
    ) -> Result<(), CatalogError> {
        let expected = match self.config.voters {
            VoterPolicy::ThreeVoters => 3,
            VoterPolicy::SingleVoterDevelopment => 1,
        };
        if voters.len() != expected || voters.len() + learners.len() > 32 {
            return Err(CatalogError::Invalid("initial member count"));
        }
        if voters.windows(2).any(|m| m[0].member >= m[1].member)
            || learners.windows(2).any(|m| m[0].member >= m[1].member)
        {
            return Err(CatalogError::Invalid("canonical member order"));
        }
        let mut members = BTreeSet::new();
        let mut nodes = BTreeSet::new();
        for member in voters.iter().chain(learners) {
            if !members.insert(member.member) || !nodes.insert(member.node) {
                return Err(CatalogError::Invalid("duplicate member/node"));
            }
        }
        Ok(())
    }

    fn namespace_key(&self) -> String {
        format!(
            "{}namespaces/{}/",
            self.inner.prefix,
            hex::encode(self.config.namespace.bytes())
        )
    }

    fn cell_key(&self, cell: &str) -> String {
        format!(
            "{}cells/{}/",
            self.namespace_key(),
            hex::encode(wire::digest(cell.as_bytes()))
        )
    }

    fn birth_key(&self, cell: &str) -> Path {
        Path::from(format!("{}birth", self.cell_key(cell)))
    }

    fn reservation_key(&self, scope: &GroupScope) -> Path {
        Path::from(format!(
            "{}groups/{}",
            self.inner.prefix,
            hex::encode(scope.group.bytes())
        ))
    }

    fn claim_key(&self, birth: &GroupBirth, member: MemberId) -> Path {
        Path::from(format!(
            "{}claims/{:016x}",
            self.cell_key(&birth.scope.cell),
            member.get()
        ))
    }
}

impl Inner {
    async fn read(&self, key: &Path) -> Result<Option<Vec<u8>>, CatalogError> {
        Ok(self.read_versioned(key).await?.map(|(bytes, _)| bytes))
    }

    async fn read_versioned(
        &self,
        key: &Path,
    ) -> Result<Option<(Vec<u8>, UpdateVersion)>, CatalogError> {
        let object = match self.store.get(key).await {
            Ok(object) => object,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(CatalogError::Storage(error)),
        };
        let length = usize::try_from(object.meta.size).map_err(|_| CatalogError::Limit)?;
        if length > self.limit || object.range.start != 0 || object.range.end != object.meta.size {
            return Err(CatalogError::Limit);
        }
        let version = UpdateVersion {
            e_tag: object.meta.e_tag.clone(),
            version: object.meta.version.clone(),
        };
        let mut bytes = Vec::with_capacity(length);
        let mut stream = object.into_stream();
        let mut chunks = 0;
        while let Some(chunk) = stream.try_next().await.map_err(CatalogError::Storage)? {
            chunks += 1;
            if chunks > 4096
                || bytes
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|n| n > length)
            {
                return Err(CatalogError::Limit);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != length {
            return Err(CatalogError::Corrupt("short catalog object"));
        }
        Ok(Some((bytes, version)))
    }

    async fn create_and_observe(&self, key: &Path, bytes: &[u8]) -> Result<Vec<u8>, CatalogError> {
        // All failed/ambiguous creates are reconciled by exact-key reads. Never
        // fall back to overwrite, unconditional PUT or a different identity.
        let result = self
            .store
            .put_opts(
                key,
                bytes.to_vec().into(),
                PutOptions::from(PutMode::Create),
            )
            .await;
        match self.read(key).await? {
            Some(observed) => {
                if result.is_ok() && observed != bytes {
                    return Err(CatalogError::Conflict(
                        "conditional create/read disagreement",
                    ));
                }
                Ok(observed)
            }
            None => match result {
                Err(error) => Err(CatalogError::Storage(error)),
                Ok(_) => Err(CatalogError::Unconfirmed),
            },
        }
    }
}
