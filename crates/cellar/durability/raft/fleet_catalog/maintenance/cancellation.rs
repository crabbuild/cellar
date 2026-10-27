// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Only the unchanged preparing epoch can be cancelled without a stop fence.
use super::super::nodes::{NodeRecord, NODE_KIND};
use super::*;
use serde::Deserialize;

const KIND: u8 = 77;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancellation {
    format: u8,
    request: FleetRequestIdentity,
    input_root: [u8; 32],
    disruption: [u8; 32],
    origin: Record,
    node: NodeRecord,
}

impl Cancellation {
    fn key(operation: [u8; 16]) -> String {
        format!("restart-cancelled/{}", hex::encode(operation))
    }

    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.origin.validate(catalog)?;
        self.node.validate(&catalog.inner)?;
        let mut expected = self.origin.node.clone();
        expected.cancel_restart_preparation(self.request.operation)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.disruption != hash(&encode(super::records::KIND, &self.origin, 4096)?)
            || self.request.expected_revision <= self.origin.request.expected_revision
            || self.origin.request.operation != self.request.operation
            || self.node != expected
        {
            return Err(FleetCatalogError::Corrupt(
                "restart preparation cancellation",
            ));
        }
        Ok(())
    }

    fn matches(&self, request: &FleetRestartRequest) -> bool {
        self.request == request.identity
            && self.origin.node.node == request.node.bytes()
            && self.origin.node.boot.as_ref().map(|b| b.process)
                == Some(request.expected_process.bytes())
            && self.origin.grace_seconds == request.grace_seconds
    }

    fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(self.node.node),
                    value: Some(encode(NODE_KIND, &self.node, 4096)?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::key(self.request.operation),
                    value: Some(encode(KIND, self, 8192)?),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: super::super::disruption::SLOT_KEY.into(),
                    value: None,
                },
            ],
        })
    }
}

impl FleetCatalog {
    /// Cancel only the original preparation epoch, before readiness or stop
    /// authority exists. Later preparation steps must advance the node epoch;
    /// they cannot use this path to bypass a synced stop-cancellation fence.
    /// Ready has no current capacity; complete retained charges are preserved.
    pub async fn cancel_restart_preparation(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartRequest,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        request.validate()?;
        self.run(timeout, async {
            let current = self.inner.current().await?;
            let previous = self.restart_cancellation_candidate(request).await?;
            if let Some(record) = &previous {
                let mutation = record.mutation()?;
                if self
                    .inner
                    .committed(
                        &current.head,
                        &hex::encode(request.identity.id),
                        hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
                    )
                    .await?
                    .is_some()
                {
                    return self.commit_inner(snapshot, &mutation).await;
                }
            }
            if current.token != snapshot.token
                || request.identity.expected_revision != snapshot.revision()
            {
                return Err(FleetCatalogError::Conflict);
            }
            let active = self
                .active_disruption(snapshot)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let super::super::disruption::Disruption::Restart(origin) = active else {
                return Err(FleetCatalogError::DisruptionInProgress {
                    operation: active.operation(),
                });
            };
            if origin.request.operation != request.identity.operation
                || origin.node.node != request.node.bytes()
                || origin.node.boot.as_ref().map(|b| b.process)
                    != Some(request.expected_process.bytes())
                || origin.grace_seconds != request.grace_seconds
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let mut node = self.host_from_record(snapshot, request.node).await?.record;
            // Strict equality is intentional: a later readiness/rejoin epoch
            // cannot be treated as the original pre-capability preparation.
            if node != origin.node {
                return Err(FleetCatalogError::Conflict);
            }
            node.cancel_restart_preparation(request.identity.operation)?;
            let record = Cancellation {
                format: 1,
                request: request.identity.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                disruption: snapshot
                    .head
                    .disruption
                    .ok_or(FleetCatalogError::Conflict)?,
                origin,
                node,
            };
            record.validate(self)?;
            if previous.as_ref().is_some_and(|old| *old != record) {
                return Err(FleetCatalogError::PayloadConflict);
            }
            self.commit_with_disruption(
                snapshot,
                &record.mutation()?,
                Some(record.disruption),
                super::super::resources::Authority::None,
                || Ok(()),
            )
            .await
        })
        .await
    }

    async fn restart_cancellation_candidate(
        &self,
        request: &FleetRestartRequest,
    ) -> Result<Option<Cancellation>> {
        let Some((mutation, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(request.identity.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(None);
        };
        mutation.validate()?;
        let bytes = mutation
            .changes
            .iter()
            .find(|c| {
                c.table == FleetTable::Operations
                    && c.key == Cancellation::key(request.identity.operation)
            })
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Cancellation = decode(KIND, bytes, 8192)?;
        record.validate(self)?;
        if !record.matches(request) || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }
}
