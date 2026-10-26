// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded projections and permanent canonical/journal linkage.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Definition {
    keep: Vec<String>,
    pub policy: FleetTopologyPolicy,
}

impl Definition {
    pub fn new(keep: &BTreeSet<NodeId>, policy: &FleetTopologyPolicy) -> Result<Self> {
        if !(4..=64).contains(&keep.len())
            || policy.revision == 0
            || !(1..=50_000_000).contains(&policy.max_work)
        {
            return Err(FleetCatalogError::Invalid("desired topology"));
        }
        Ok(Self {
            keep: keep.iter().map(|n| hex::encode(n.bytes())).collect(),
            policy: policy.clone(),
        })
    }

    pub fn nodes(&self) -> Result<BTreeSet<NodeId>> {
        let mut nodes = BTreeSet::new();
        for text in &self.keep {
            let mut bytes = [0; 16];
            hex::decode_to_slice(text, &mut bytes)
                .map_err(|_| FleetCatalogError::Corrupt("topology keep node"))?;
            if hex::encode(bytes) != *text {
                return Err(FleetCatalogError::Corrupt("topology keep encoding"));
            }
            nodes.insert(NodeId::new(bytes).map_err(|_| FleetCatalogError::Corrupt("keep node"))?);
        }
        if Self::new(&nodes, &self.policy)? != *self {
            return Err(FleetCatalogError::Corrupt("topology keep ordering"));
        }
        Ok(nodes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Policy {
    pub format: u8,
    pub catalog: [u8; 32],
    pub operation: [u8; 16],
    pub definition: Definition,
}

impl Policy {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        encode(POLICY_KIND, self, 4096)
    }

    pub fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        if self.format != 1 || self.catalog != catalog.birth_owner()?.catalog {
            return Err(FleetCatalogError::Corrupt("topology policy identity"));
        }
        ProposalId::new(self.operation)
            .map_err(|_| FleetCatalogError::Corrupt("topology owner"))?;
        self.definition.nodes()?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Canonical {
    pub request: FleetRequestIdentity,
    pub desired: Policy,
}

impl Canonical {
    pub fn key(&self) -> String {
        format!("topology-requests/{}", hex::encode(self.request.operation))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IntentRecord {
    pub format: u8,
    pub catalog: [u8; 32],
    pub request: FleetRequestIdentity,
    pub input_root: [u8; 32],
    pub desired: [u8; 32],
    #[serde(deserialize_with = "required_nullable")]
    pub previous: Option<[u8; 32]>,
    pub phase: FleetTopologyPhase,
    pub transition: FleetRequestIdentity,
    #[serde(deserialize_with = "required_nullable")]
    pub moves: Option<[u8; 32]>,
    pub count: u64,
    #[serde(deserialize_with = "required_nullable")]
    pub last_superseded_admission: Option<[u8; 16]>,
}

fn required_nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(decoder)
}

impl IntentRecord {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        encode(ROW_KIND, self, 4096)
    }

    pub fn change(&self) -> Result<FleetChange> {
        Ok(FleetChange {
            table: FleetTable::Operations,
            key: row_key(self.request.operation),
            value: Some(self.bytes()?),
        })
    }

    pub fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        super::super::super::histories::validate_request(&self.request)?;
        super::super::super::histories::validate_request(&self.transition)?;
        if self.format != 1
            || self.catalog != catalog.birth_owner()?.catalog
            || self.input_root == [0; 32]
            || self.desired == [0; 32]
            || self.previous == Some([0; 32])
            || self.transition.operation != self.request.operation
            || self.transition.expected_revision < self.request.expected_revision
            || (self.count == 0) != self.moves.is_none()
            || self.count >= u64::MAX - 2
            || self.last_superseded_admission == Some([0; 16])
        {
            return Err(FleetCatalogError::Corrupt("topology operation projection"));
        }
        Ok(())
    }

    pub fn applied_revision(&self) -> Result<u64> {
        self.transition
            .expected_revision
            .checked_add(1)
            .ok_or(FleetCatalogError::Corrupt("topology transition revision"))
    }
}

impl FleetCatalog {
    pub(super) async fn read_intent_policy(&self, digest: [u8; 32]) -> Result<Policy> {
        let (policy, _) = self
            .inner
            .read::<Policy>(
                &format!("topology-policies/{}", hex::encode(digest)),
                POLICY_KIND,
                4096,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing topology policy"))?;
        policy.validate(self)?;
        if hash(&policy.bytes()?) != digest {
            return Err(FleetCatalogError::Corrupt("topology policy digest"));
        }
        Ok(policy)
    }

    pub(super) async fn placement_policy(
        &self,
        snapshot: &FleetSnapshot,
    ) -> Result<Option<Policy>> {
        let Some(bytes) = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], POLICY_KEY)
            .await?
        else {
            return Ok(None);
        };
        let policy: Policy = decode(POLICY_KIND, &bytes, 4096)?;
        policy.validate(self)?;
        if self.read_intent_policy(hash(&bytes)).await? != policy {
            return Err(FleetCatalogError::Corrupt("topology policy projection"));
        }
        Ok(Some(policy))
    }

    pub(super) async fn intent_record(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<IntentRecord> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &row_key(operation),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let record: IntentRecord = decode(ROW_KIND, &bytes, 4096)?;
        record.validate(self)?;
        let canonical = self
            .inner
            .read::<Canonical>(
                &format!("topology-requests/{}", hex::encode(operation)),
                REQUEST_KIND,
                4096,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing topology request"))?
            .0;
        if record.request.operation != operation
            || canonical.request != record.request
            || canonical.desired.operation != operation
            || hash(&canonical.desired.bytes()?) != record.desired
        {
            return Err(FleetCatalogError::Corrupt("topology canonical binding"));
        }
        canonical.desired.validate(self)?;
        let original = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(record.request.id)),
                super::super::super::REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?
            .0;
        let mut initial = record.clone();
        initial.phase = FleetTopologyPhase::Running;
        initial.transition = initial.request.clone();
        initial.moves = None;
        initial.count = 0;
        initial.last_superseded_admission = None;
        let expected = mutation(
            &record.request,
            vec![
                initial.change()?,
                FleetChange {
                    table: FleetTable::Policy,
                    key: POLICY_KEY.into(),
                    value: Some(canonical.desired.bytes()?),
                },
                active_change(Some(operation)),
            ],
        );
        if original != expected
            || self.resource_applied(snapshot, &original).await?
                != Some(initial.applied_revision()?)
        {
            return Err(FleetCatalogError::Corrupt("unadmitted topology origin"));
        }
        let (change, _) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(record.transition.id)),
                super::super::super::REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        change.validate()?;
        if change.id != record.transition.id
            || change.operation != operation
            || change.expected_revision != record.transition.expected_revision
            || change.principal != record.transition.principal
            || change
                .changes
                .iter()
                .find(|c| c.table == FleetTable::Operations && c.key == row_key(operation))
                != Some(&record.change()?)
            || self.resource_applied(snapshot, &change).await? != Some(record.applied_revision()?)
        {
            return Err(FleetCatalogError::Corrupt("unadmitted topology transition"));
        }
        Ok(record)
    }

    pub(super) async fn active_intent(
        &self,
        snapshot: &FleetSnapshot,
    ) -> Result<Option<IntentRecord>> {
        let Some(bytes) = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], ACTIVE_KEY)
            .await?
        else {
            return Ok(None);
        };
        let id: [u8; 16] = bytes
            .try_into()
            .map_err(|_| FleetCatalogError::Corrupt("active topology ID"))?;
        let record = self.intent_record(snapshot, id).await?;
        let policy = self
            .placement_policy(snapshot)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing active topology policy"))?;
        if !matches!(
            record.phase,
            FleetTopologyPhase::Running | FleetTopologyPhase::Cancelling
        ) || hash(&policy.bytes()?) != record.desired
        {
            return Err(FleetCatalogError::Corrupt("active topology projection"));
        }
        Ok(Some(record))
    }

    pub(super) async fn intent_observation(
        &self,
        snapshot: &FleetSnapshot,
        record: &IntentRecord,
    ) -> Result<FleetTopologyOperation> {
        let policy = self.read_intent_policy(record.desired).await?;
        // intent_record checked this exact transition against the permanent
        // journal before an observation can reach this helper.
        let applied_revision = record.applied_revision()?;
        Ok(FleetTopologyOperation {
            identity: record.request.clone(),
            keep: policy.definition.nodes()?.into_iter().collect(),
            policy: policy.definition.policy,
            phase: record.phase,
            admitted_movements: record.count,
            applied_revision,
            observed_revision: snapshot.revision(),
            last_superseded_admission: record.last_superseded_admission,
        })
    }
}
