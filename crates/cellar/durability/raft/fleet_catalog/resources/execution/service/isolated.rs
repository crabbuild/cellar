// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Walk immutable candidates once; renew current authority for every decision.
//! Exclusion prevents new references to the isolated node. Existing admitted
//! work always runs first; this cursor is not an evacuation/release certificate.
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_catalog::histories::{FleetHistory, FleetHistoryState};
use crate::raft::fleet_catalog::resources::repair::{Group, Planning};
use crate::raft::fleet_stop::BootBinding;
use cellar_logic::consensus::{NodeId, ProposalId};
use sha2::{Digest, Sha256};

struct Scan {
    snapshot: FleetSnapshot,
    after: Option<FleetCursor>,
    current: Option<(String, Option<FleetCursor>)>,
    exhausted: bool,
}

struct Candidate {
    revision: u64,
    id: [u8; 16],
    seen_at: u64,
}

pub(super) struct IsolatedRepairService {
    operation: ProposalId,
    evidence: Arc<dyn FleetIsolatedRepairEvidence>,
    boot: Option<BootBinding>,
    scan: Option<Scan>,
    candidate: Option<Candidate>,
    idle: bool,
    failed_passes: u32,
}

impl IsolatedRepairService {
    pub fn new(operation: ProposalId, evidence: Arc<dyn FleetIsolatedRepairEvidence>) -> Self {
        Self {
            operation,
            evidence,
            boot: None,
            scan: None,
            candidate: None,
            idle: false,
            failed_passes: 0,
        }
    }

    pub fn pass_delay(&self, options: &FleetMovementServiceOptions) -> Option<Duration> {
        if !self.scan.as_ref().is_some_and(|s| s.exhausted) || self.failed_passes == 0 {
            return None;
        }
        Some(Duration::from_millis(super::schedule::retry_delay(
            options,
            self.failed_passes,
            crate::asyncrt::rng("fleet-isolated-repair-pass-backoff").next_u32(),
        )))
    }

    fn advance(&mut self, status: &watch::Sender<FleetMovementServiceStatus>) -> Result<()> {
        let scan = self.scan.as_mut().ok_or(FleetCatalogError::Conflict)?;
        let (_, next) = scan.current.take().ok_or(FleetCatalogError::Conflict)?;
        scan.exhausted = next.is_none();
        scan.after = next;
        self.candidate = None;
        let exhausted = scan.exhausted;
        if exhausted {
            let blocked = status
                .borrow()
                .isolated_repair
                .as_ref()
                .is_some_and(|r| r.blocked_histories > 0);
            self.failed_passes = if blocked {
                self.failed_passes.saturating_add(1)
            } else {
                0
            };
        }
        status.send_modify(|s| {
            if let Some(r) = &mut s.isolated_repair {
                r.histories_checked = r.histories_checked.saturating_add(1);
                r.inventory_exhausted = exhausted;
            }
        });
        Ok(())
    }

    pub async fn round(
        &mut self,
        catalog: &FleetCatalog,
        observer: &RaftCells,
        cut: &FleetSnapshot,
        options: &FleetMovementServiceOptions,
        deadline: &Deadline<'_>,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) -> Result<()> {
        let (boot, nodes) = catalog
            .run(deadline.remaining()?, async {
                if catalog.inner.current().await?.token != cut.token {
                    return Err(FleetCatalogError::Conflict);
                }
                let active = catalog
                    .checked_active_evacuation(cut, self.operation)
                    .await?;
                let host = catalog
                    .host_from_record(cut, active.admission().node())
                    .await?;
                let isolated = catalog.confirm_node_isolation(cut, &host.record).await?;
                if catalog.resource_book(cut).await?.rows.len() >= super::super::super::MAX_MOVES {
                    return Err(FleetCatalogError::Resource(
                        cellar_logic::raft_fleet::LedgerError::MovementLimit,
                    ));
                }
                Ok((isolated.boot, catalog.node_records(cut).await?.len()))
            })
            .await?;
        if self.boot.as_ref().is_some_and(|old| *old != boot) {
            return Err(FleetCatalogError::Conflict);
        }
        self.boot = Some(boot.clone());
        if self.idle {
            return Ok(());
        }
        if self.scan.as_ref().is_some_and(|s| s.exhausted) {
            let blocked = status
                .borrow()
                .isolated_repair
                .as_ref()
                .is_none_or(|r| r.blocked_histories > 0);
            if !blocked {
                self.idle = true;
                return Ok(());
            }
            self.scan = None;
        }
        if self.scan.is_none() {
            self.scan = Some(Scan {
                snapshot: cut.clone(),
                after: None,
                current: None,
                exhausted: false,
            });
            status.send_modify(|s| {
                s.isolated_repair = Some(FleetIsolatedRepairServiceStatus {
                    operation: self.operation,
                    failed_boot: boot.clone(),
                    scan_revision: cut.revision(),
                    histories_checked: 0,
                    blocked_histories: 0,
                    inventory_exhausted: false,
                    last_blocked_group: None,
                    last_blocked_failure: None,
                })
            });
        }
        let scan = self.scan.as_mut().ok_or(FleetCatalogError::Conflict)?;
        if scan.current.is_none() {
            let page = catalog
                .run(
                    deadline.remaining()?,
                    catalog.inner.scan(
                        scan.snapshot.head.tables[FleetTable::Histories.slot()],
                        scan.after.as_ref(),
                        1,
                    ),
                )
                .await?;
            if page.entries.len() > 1 || page.entries.is_empty() && page.next.is_some() {
                return Err(FleetCatalogError::Corrupt("isolated repair inventory page"));
            }
            let Some(row) = page.entries.first() else {
                scan.exhausted = true;
                status.send_modify(|s| {
                    if let Some(r) = &mut s.isolated_repair {
                        r.inventory_exhausted = true;
                    }
                });
                return Ok(());
            };
            let original = FleetHistory::decode(&row.value)?;
            if original
                .binding()
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("isolated repair inventory binding"))?
                != row.key
            {
                return Err(FleetCatalogError::Corrupt("isolated repair inventory key"));
            }
            scan.current = Some((row.key.clone(), page.next));
        }
        let key = scan
            .current
            .as_ref()
            .ok_or(FleetCatalogError::Conflict)?
            .0
            .clone();
        let history = catalog
            .run(deadline.remaining()?, async {
                if catalog.inner.current().await?.token != cut.token {
                    return Err(FleetCatalogError::Conflict);
                }
                catalog.history_inner(cut, &key).await
            })
            .await?;
        let failed = NodeId::new(boot.node)
            .map_err(|_| FleetCatalogError::Corrupt("isolated repair boot"))?;
        match history.state() {
            FleetHistoryState::Cancelled { .. } => return self.advance(status),
            FleetHistoryState::Live => (),
            _ if !history
                .voters
                .iter()
                .chain(&history.learners)
                .any(|v| v.node == boot.node) =>
            {
                return self.advance(status)
            }
            _ => {
                let scope = history
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("isolated repair reserved scope"))?;
                self.block(scope, &FleetCatalogError::InventoryIncomplete, status);
                return self.advance(status);
            }
        }
        let (voters, latest) = match catalog
            .run(
                deadline.remaining()?,
                catalog.settled_group_voters(cut, &history),
            )
            .await
        {
            Ok(group) => group,
            Err(
                error @ (FleetCatalogError::InventoryIncomplete
                | FleetCatalogError::Unconfirmed
                | FleetCatalogError::Conflict),
            ) => {
                let scope = history
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("isolated repair blocked scope"))?;
                self.block(scope, &error, status);
                return self.advance(status);
            }
            Err(error) => return Err(error),
        };
        if !voters.contains_key(&failed) {
            return self.advance(status);
        }
        let context = FleetIsolatedRepairContext {
            operation: self.operation,
            failed_boot: boot,
            group: history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("isolated repair scope"))?,
        };
        let request = identity(catalog, self.operation, cut, &key, b"movement")?;
        if catalog
            .run(
                deadline.remaining()?,
                candidate_exists(catalog, cut, &request),
            )
            .await?
        {
            let now = catalog.resource_now()?;
            let pending = self.candidate.get_or_insert(Candidate {
                revision: cut.revision(),
                id: request.id,
                seen_at: now,
            });
            if pending.revision != cut.revision() || pending.id != request.id {
                self.candidate = Some(Candidate {
                    revision: cut.revision(),
                    id: request.id,
                    seen_at: now,
                });
                return Ok(());
            }
            if now.saturating_sub(pending.seen_at) < options.turn_timeout.as_millis() as u64 {
                return Ok(());
            }
            // This descriptive fence has no membership or release authority.
            // Its expected-root CAS and the old prospective admission cannot
            // both win. An admitted move remains in the ordinary durable book.
            let fence = identity(catalog, self.operation, cut, &key, b"fence")?;
            let mutation = FleetMutation {
                id: fence.id,
                operation: fence.operation,
                principal: fence.principal,
                expected_revision: fence.expected_revision,
                changes: vec![FleetChange {
                    table: FleetTable::Operations,
                    key: format!(
                        "isolated-repair-fence/{}",
                        hex::encode(self.operation.bytes())
                    ),
                    value: Some(
                        serde_json::to_vec(&(1_u8, cut.revision(), request.id))
                            .map_err(|_| FleetCatalogError::Limit)?,
                    ),
                }],
            };
            catalog
                .commit(cut, &mutation, deadline.remaining()?)
                .await?;
            self.candidate = None;
            return Ok(());
        }
        self.candidate = None;
        let bytes = ROOT_LIMIT
            .checked_mul(2)
            .and_then(|n| n.checked_add(nodes.checked_mul(512 * 1024)?))
            .ok_or(FleetCatalogError::Limit)?;
        let _memory =
            super::topology::reserve_collection_memory(&observer.handle.control_budget, bytes, 0)?;
        let group = Group {
            history,
            voters,
            latest,
        };
        let result = async {
            let inputs = self
                .evidence
                .refresh(&context, deadline.remaining()?)
                .await?;
            if inputs.nodes.len() > nodes || inputs.namespaces.len() > 64 {
                return Err(FleetCatalogError::Limit);
            }
            let selected = Planning {
                catalog,
                cut,
                context: &context,
                group: &group,
                observer,
                identity: request,
                deadline,
            }
            .plan(&inputs)
            .await?;
            catalog
                .reserve_isolated_repair(
                    cut,
                    &selected,
                    observer,
                    &inputs.namespaces,
                    deadline.remaining()?,
                )
                .await?;
            Ok::<_, FleetCatalogError>(())
        }
        .await;
        match result {
            Ok(()) => Ok(()), // Revisit this group after the owned worker settles it.
            Err(FleetCatalogError::Closed) => Err(FleetCatalogError::Closed),
            Err(error @ FleetCatalogError::Corrupt(_)) => Err(error),
            Err(error) => {
                self.block(context.group, &error, status);
                self.advance(status)
            }
        }
    }

    fn block(
        &self,
        group: GroupScope,
        error: &FleetCatalogError,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) {
        status.send_modify(|s| {
            if let Some(r) = &mut s.isolated_repair {
                r.blocked_histories = r.blocked_histories.saturating_add(1);
                r.last_blocked_group = Some(group);
                r.last_blocked_failure = Some(super::schedule::classify(error));
            }
        });
    }
}

fn identity(
    catalog: &FleetCatalog,
    operation: ProposalId,
    cut: &FleetSnapshot,
    key: &str,
    kind: &[u8],
) -> Result<FleetRequestIdentity> {
    let mut digest = Sha256::new();
    digest.update(b"cellar-isolated-repair-service-1");
    digest.update(catalog.birth_owner()?.catalog);
    digest.update(catalog.inner.incarnation);
    digest.update(operation.bytes());
    digest.update(cut.revision().to_be_bytes());
    digest.update((kind.len() as u64).to_be_bytes());
    digest.update(kind);
    digest.update(key.as_bytes());
    let hash: [u8; 32] = digest.finalize().into();
    Ok(FleetRequestIdentity {
        id: hash[..16]
            .try_into()
            .map_err(|_| FleetCatalogError::Limit)?,
        operation: operation.bytes(),
        principal: "fleet-isolated-repair-service".into(),
        expected_revision: cut.revision(),
    })
}

async fn candidate_exists(
    catalog: &FleetCatalog,
    cut: &FleetSnapshot,
    request: &FleetRequestIdentity,
) -> Result<bool> {
    let Some((change, _)) = catalog
        .inner
        .read::<FleetMutation>(
            &format!("requests/{}", hex::encode(request.id)),
            super::super::super::super::REQUEST_KIND,
            REQUEST_LIMIT,
        )
        .await?
    else {
        return Ok(false);
    };
    change.validate()?;
    let value = change
        .changes
        .first()
        .and_then(|c| c.value.as_deref())
        .ok_or(FleetCatalogError::Corrupt(
            "isolated repair prospective admission",
        ))?;
    let row = super::super::super::records::Record::decode(catalog, value)?;
    if row.request.identity != *request
        || row.request.movement.is_none()
        || change.changes.len() != 4
    {
        return Err(FleetCatalogError::PayloadConflict);
    }
    let (canonical, original) = catalog
        .resource_request(&row.request)
        .await?
        .ok_or(FleetCatalogError::Unconfirmed)?;
    if canonical != change || original.encode()? != row.encode()? {
        return Err(FleetCatalogError::Corrupt(
            "isolated repair prospective projection",
        ));
    }
    if catalog.resource_applied(cut, &change).await?.is_some() {
        return Err(FleetCatalogError::Conflict);
    }
    Ok(true)
}
