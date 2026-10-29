//! Explicit compiled capabilities let a rooted enrolled host boot with no cells.
use super::super::fleet_catalog::FleetHostAuthorization;
use super::*;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub struct RaftNamespaceRegistration {
    pub namespace: NamespaceConfig,
    pub class: String,
}

/// Node enrollment only. This does not declare inventory complete, activate
/// capacity or replace each group's independent membership authorization.
#[derive(Clone, Debug)]
pub struct RaftManagedHostOptions {
    pub authorization: FleetHostAuthorization,
    pub namespaces: Vec<RaftNamespaceRegistration>,
}

impl RaftManagedHostOptions {
    /// Canonical bounded declaration digest for the trusted node descriptor.
    pub fn capability_digest(namespaces: &[RaftNamespaceRegistration]) -> anyhow::Result<[u8; 32]> {
        anyhow::ensure!(
            (1..=64).contains(&namespaces.len()),
            "invalid managed namespace count"
        );
        for entry in namespaces {
            anyhow::ensure!(
                !entry.class.is_empty()
                    && entry.class.len() <= 128
                    && !entry.class.chars().any(|c| c.is_control() || c == ':')
                    && entry.namespace.mode == cellar_logic::NamespaceMode::Raft
                    && entry.namespace.voters == VoterPolicy::ThreeVoters
                    && entry.namespace.schema_digest != [0; 32],
                "invalid managed namespace declaration"
            );
        }
        let mut ordered: Vec<_> = namespaces.iter().collect();
        ordered.sort_by_key(|e| (e.namespace.cluster, e.namespace.namespace));
        let mut hasher = Sha256::new();
        hasher.update(b"cellar-managed-host-capabilities-1");
        hasher.update(crate::application::consensus::protocol::protocol_identity());
        hasher.update((ordered.len() as u64).to_be_bytes());
        let mut previous = None;
        for entry in ordered {
            let key = (entry.namespace.cluster, entry.namespace.namespace);
            anyhow::ensure!(previous != Some(key), "duplicate managed namespace");
            previous = Some(key);
            hasher.update(entry.namespace.cluster.bytes());
            hasher.update(entry.namespace.namespace.bytes());
            hasher.update(entry.namespace.schema_digest);
            match &entry.namespace.managed_fleet {
                Some(owner) => {
                    hasher.update([1]);
                    hasher.update(owner.fleet);
                    hasher.update(owner.catalog);
                }
                None => hasher.update([0]),
            }
            hasher.update([match entry.namespace.minimum_durability {
                Durability::Quorum => 1,
                Durability::S3 => 2,
            }]);
            hasher.update((entry.class.len() as u64).to_be_bytes());
            hasher.update(entry.class.as_bytes());
        }
        Ok(hasher.finalize().into())
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.namespaces
                .iter()
                .all(|e| self.authorization.check_namespace(&e.namespace).is_ok())
                && Self::capability_digest(&self.namespaces)?
                    == self.authorization.capability_digest(),
            "managed host declarations disagree with rooted authorization"
        );
        Ok(())
    }
}

pub(crate) struct NamespaceRegistration {
    pub namespace: NamespaceCatalog,
    pub class: Arc<RaftClass>,
}

pub(super) async fn prepare(
    options: &mut RaftHostOptions,
    service: &crate::daemon::NodeRaftService,
    cells: &[Registration],
    namespaces: &[NamespaceRegistration],
    peers: &BTreeMap<NodeId, RaftPeerNode>,
    label: &str,
) -> anyhow::Result<BTreeMap<GroupScope, crate::raft::fleet_catalog::FleetGroupAuthorization>> {
    let Some(managed) = &mut options.managed else {
        anyhow::ensure!(namespaces.is_empty(), "undeclared managed namespaces");
        return Ok(BTreeMap::new());
    };
    managed.authorization = managed
        .authorization
        .refresh(std::time::Duration::from_secs(5))
        .await?;
    managed.validate()?;
    anyhow::ensure!(
        namespaces.len() == managed.namespaces.len(),
        "incomplete managed namespace preparation"
    );
    for (prepared, declared) in namespaces.iter().zip(&managed.namespaces) {
        anyhow::ensure!(
            prepared.namespace.config() == &declared.namespace
                && prepared.class.name() == declared.class
                && prepared.class.schema_digest() == declared.namespace.schema_digest,
            "managed namespace preparation mismatch"
        );
    }
    let auth = &managed.authorization;
    let local = service.log.limits();
    let own = peers
        .get(&local.node)
        .ok_or_else(|| anyhow::anyhow!("managed host self endpoint missing"))?;
    anyhow::ensure!(
        auth.node() == local.node
            && auth.disk() == local.disk
            && auth.label() == label
            && own.label == auth.label()
            && own.server_name == auth.server_name()
            && auth.instance_matches(service.log.storage_instance()?),
        "managed host physical identity mismatch"
    );
    managed
        .authorization
        .check_declared_namespaces(namespaces, std::time::Duration::from_secs(5))
        .await?;

    let mut authorizations = BTreeMap::new();
    for cell in cells {
        let scope = cell.birth.scope();
        let authorization = auth
            .group_authorization(scope, std::time::Duration::from_secs(5))
            .await?;
        anyhow::ensure!(
            authorization.check_birth(&cell.namespace, &cell.birth),
            "managed history birth/schema mismatch"
        );
        match service.log.group(scope) {
            Ok(group) => anyhow::ensure!(
                group
                    .membership()?
                    .fleet_status()
                    .is_some_and(|status| status.binding == *authorization.binding()),
                "managed host requires committed group enrollment"
            ),
            Err(super::super::storage::LogError::NotFound) => anyhow::ensure!(
                cell.placement == RaftCellPlacement::Replica
                    && cell.birth.managed_owner().is_some()
                    && authorization.allows_managed_genesis()
                    || matches!(
                        cell.placement,
                        RaftCellPlacement::Standby | RaftCellPlacement::QueryRouter { .. }
                    ),
                "managed host cannot bootstrap an unguarded member"
            ),
            Err(error) => return Err(error.into()),
        }
        authorizations.insert(scope.clone(), authorization);
    }
    Ok(authorizations)
}
