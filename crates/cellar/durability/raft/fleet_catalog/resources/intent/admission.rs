// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Desired intent is admitted without changing node epochs or old permits.
use super::records::Canonical;
use super::*;

impl FleetCatalog {
    /// Persist the desired keep-set and exclude all other nodes from new
    /// placement in one CAS. Existing history/permit authority is preserved.
    /// This admits Planning work; it does not claim that the desired fleet fits.
    pub async fn begin_topology(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetTopologyIntentRequest,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetTopologyOperation> {
        self.check_snapshot(snapshot)?;
        super::super::super::histories::validate_request(&request.identity)?;
        if namespaces.len() > super::super::super::namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        let keep = request.keep.iter().copied().collect::<BTreeSet<_>>();
        if keep.len() != request.keep.len() {
            return Err(FleetCatalogError::Invalid("duplicate desired keep node"));
        }
        let desired = Policy {
            format: 1,
            catalog: self.birth_owner()?.catalog,
            operation: request.identity.operation,
            definition: Definition::new(&keep, &request.policy)?,
        };
        let canonical = Canonical {
            request: request.identity.clone(),
            desired,
        };
        let deadline = super::super::super::deadline::Deadline::new(self, timeout)?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                let current = self.inner.current().await?;
                if let Some((prior, _)) = self
                    .inner
                    .read::<Canonical>(&canonical.key(), REQUEST_KIND, 4096)
                    .await?
                {
                    if prior != canonical {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                }
                if let Some((original, _)) = self
                    .inner
                    .read::<FleetMutation>(
                        &format!("requests/{}", hex::encode(request.identity.id)),
                        super::super::super::REQUEST_KIND,
                        REQUEST_LIMIT,
                    )
                    .await?
                {
                    if original.operation != request.identity.operation
                        || original.id != request.identity.id
                        || original.principal != request.identity.principal
                        || original.expected_revision != request.identity.expected_revision
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    if self.resource_applied(&current, &original).await?.is_some() {
                        let record = self
                            .intent_record(&current, request.identity.operation)
                            .await?;
                        if record.request != request.identity
                            || record.desired != hash(&canonical.desired.bytes()?)
                        {
                            return Err(FleetCatalogError::PayloadConflict);
                        }
                        return self.intent_observation(&current, &record).await;
                    }
                }
                self.require_intent_current(snapshot).await?;
                if request.identity.expected_revision != snapshot.revision() {
                    return Err(FleetCatalogError::Conflict);
                }
                if let Some(record) = self.active_intent(snapshot).await? {
                    return Err(FleetCatalogError::DisruptionInProgress {
                        operation: record.request.operation,
                    });
                }
                if snapshot.head.disruption.is_some() {
                    return Err(FleetCatalogError::Invalid("active node disruption"));
                }
                if self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &row_key(request.identity.operation),
                    )
                    .await?
                    .is_some()
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                self.check_inventory_inner(snapshot, namespaces).await?;
                self.require_intent_settled(snapshot).await?;
                for node in keep {
                    if !self.capacity_from(snapshot, node).await?.receive_replicas() {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                let previous = self
                    .placement_policy(snapshot)
                    .await?
                    .map(|p| p.bytes())
                    .transpose()?
                    .map(|b| hash(&b));
                let policy_bytes = canonical.desired.bytes()?;
                let desired = hash(&policy_bytes);
                let record = IntentRecord {
                    format: 1,
                    catalog: self.birth_owner()?.catalog,
                    request: request.identity.clone(),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    desired,
                    previous,
                    phase: FleetTopologyPhase::Running,
                    transition: request.identity.clone(),
                    moves: None,
                    count: 0,
                    last_superseded_admission: None,
                };
                record.validate(self)?;
                self.inner
                    .create_exact(
                        &format!("topology-policies/{}", hex::encode(desired)),
                        &policy_bytes,
                        4096,
                    )
                    .await?;
                self.inner
                    .create_exact(
                        &canonical.key(),
                        &encode(REQUEST_KIND, &canonical, 4096)?,
                        4096,
                    )
                    .await?;
                let change = mutation(
                    &request.identity,
                    vec![
                        record.change()?,
                        FleetChange {
                            table: FleetTable::Policy,
                            key: POLICY_KEY.into(),
                            value: Some(policy_bytes),
                        },
                        active_change(Some(request.identity.operation)),
                    ],
                );
                let applied = self
                    .commit_intent(snapshot, &change, || deadline.remaining().map(|_| ()))
                    .await?;
                let record = self
                    .intent_record(&applied.snapshot, request.identity.operation)
                    .await?;
                self.intent_observation(&applied.snapshot, &record).await
            }),
        )
        .await
    }
}
