// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded authenticated process protocol. Wire IDs never replace SDK proofs.
use super::*;
use axum::{
    extract::{DefaultBodyLimit, State as AxumState},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use subtle::ConstantTimeEq;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Observe,
    Status {
        group: [u8; 16],
        incarnation: [u8; 16],
    },
    Stop,
    QualifyNode {
        request: RaftFleetNodeCapacity,
    },
    ActivateNode {
        request: RaftFleetNodeCapacity,
    },
    AuthorizeSeal {
        request: RaftFleetTransitionSealRequest,
    },
    SealSource {
        request: RaftFleetTransitionSourceSealRequest,
    },
    SetupChild {
        request: RaftFleetTransitionChildSetupRequest,
    },
    GrantOrigin {
        revision: u64,
        node: [u8; 16],
        disk: [u8; 16],
        process: [u8; 16],
        label: String,
        server_name: String,
        addresses: [std::net::SocketAddr; 3],
        read: bool,
        write: bool,
    },
    Start {
        group: [u8; 16],
        incarnation: [u8; 16],
    },
    ReopenRecovery {
        selection: RaftFleetTransitionRecoverySelection,
        plan_digest: String,
    },
    PrepareSeed {
        group: [u8; 16],
        incarnation: [u8; 16],
    },
    Seed {
        group: [u8; 16],
        incarnation: [u8; 16],
    },
    Attach {
        group: [u8; 16],
        incarnation: [u8; 16],
        operation: [u8; 16],
        plan_digest: [u8; 32],
        seed_digest: [u8; 32],
    },
    Activate {
        group: [u8; 16],
        incarnation: [u8; 16],
        operation: [u8; 16],
        plan_digest: [u8; 32],
        seed_digest: [u8; 32],
    },
    Enroll {
        cell: String,
        incarnation: [u8; 16],
        creation: [u8; 16],
        s3: bool,
    },
    Write {
        cell: String,
        incarnation: [u8; 16],
        session: [u8; 16],
        sequence: u64,
        retry_until_ms: u64,
        delta: i64,
        s3: bool,
    },
    Read {
        cell: String,
        incarnation: [u8; 16],
        invocation: [u8; 16],
        minimum: Option<String>,
    },
}

impl Action {
    fn timeout(&self) -> Duration {
        if matches!(self, Self::ReopenRecovery { .. }) {
            bounds().timeout
        } else {
            TIMEOUT
        }
    }
}

pub(super) fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"live":true})) }))
        .route("/control", post(control))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(state)
}

async fn control(
    AxumState(state): AxumState<Arc<State>>,
    headers: HeaderMap,
    Json(action): Json<Action>,
) -> (StatusCode, Json<Value>) {
    let authorized = single_header(&headers, "authorization")
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|v| bool::from(v.as_bytes().ct_eq(state.config.token.as_bytes())));
    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        );
    }
    let target = [
        ("x-cellar-node", hex::encode(state.config.node)),
        ("x-cellar-disk", hex::encode(state.config.disk)),
        ("x-cellar-process", hex::encode(state.config.process)),
    ];
    if target
        .iter()
        .any(|(name, expected)| single_header(&headers, name) != Some(expected.as_str()))
    {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error":"different voter process", "outcome":"not_submitted"})),
        );
    }
    let Ok(_lane) = state.lane.try_lock() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"control lane busy", "outcome":"not_submitted"})),
        );
    };
    let result = tokio::time::timeout(action.timeout(), state.domain.run(state.call(action))).await;
    match result {
        Ok(Ok(Ok(value))) => (StatusCode::OK, Json(value)),
        Ok(Ok(Err(error))) => {
            let message = format!("{error:#}")
                .replace(&state.config.access, "[REDACTED]")
                .replace(&state.config.secret, "[REDACTED]")
                .replace(&state.config.token, "[REDACTED]");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":message, "outcome":"unknown"})),
            )
        }
        Ok(Err(_)) | Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"domain closed or control deadline expired", "outcome":"unknown"})),
        ),
    }
}

impl State {
    async fn call(&self, action: Action) -> Result<Value> {
        let turn = Budget::new(action.timeout());
        match action {
            Action::Observe => {
                let view = self.cells.observe_node(turn.remaining()?).await?;
                let revision = self.catalog.read(turn.remaining()?).await?.revision();
                Ok(
                    json!({"node":self.config.node, "process":self.config.process,
                    "boot":view.fleet_boot, "groups":view.node.groups,
                    "catalog_id":self.control.catalog_id(),
                    "namespace_digest":hex::encode(self.control.namespace_digest(&self.config.namespace().namespace_id)?),
                    "fleet_revision":revision, "installed_revision":view.fleet_trust_revision,
                    "registered_histories":view.registered_histories,
                    "unattached_engine_histories":view.unattached_engine_histories,
                    "reserved_bytes":view.node.reserved_bytes,
                    "profile_digest":hex::encode(fleet_capacity_profile_digest(&view.node.profile)?),
                    "group_capacity":view.node.profile.group_capacity,
                    "byte_capacity":view.node.profile.byte_capacity,
                    "bytes_per_group":view.node.profile.bytes_per_group}),
                )
            }
            Action::Stop => {
                self.stop.send(true)?;
                Ok(json!({"stopping":true}))
            }
            Action::QualifyNode { request } => {
                let admission = self
                    .control
                    .qualify_node(
                        &self.config.principal,
                        &hex::encode(self.config.node),
                        request,
                        turn.remaining()?,
                    )
                    .await?;
                Ok(json!({"admission":admission}))
            }
            Action::ActivateNode { request } => {
                let admission = self
                    .control
                    .activate_node(
                        &self.config.principal,
                        &hex::encode(self.config.node),
                        request,
                        turn.remaining()?,
                    )
                    .await?;
                Ok(json!({"admission":admission}))
            }
            Action::Status { group, incarnation } => {
                let status = self
                    .cells
                    .group(self.history(group, incarnation)?)?
                    .status()
                    .await?;
                Ok(
                    json!({"leader":status.leader, "ready":status.ready, "term":status.term.get(),
                    "applied":status.applied.index.get(), "committed":status.committed.get()}),
                )
            }
            Action::AuthorizeSeal { request } => Ok(serde_json::to_value(
                self.control
                    .authorize_transition_seal(
                        &self.config.principal,
                        &self.config.namespace().namespace_id,
                        &request.operation_id,
                        &request,
                        turn.remaining()?,
                    )
                    .await?,
            )?),
            Action::SealSource { request } => Ok(serde_json::to_value(
                self.control
                    .seal_transition_source(
                        &self.config.principal,
                        &self.config.namespace().namespace_id,
                        &request.operation_id,
                        &request,
                        turn.remaining()?,
                    )
                    .await?,
            )?),
            Action::SetupChild { request } => Ok(serde_json::to_value(
                self.control
                    .setup_transition_child(
                        &self.config.principal,
                        &self.config.namespace().namespace_id,
                        &request.operation_id,
                        &request,
                        turn.remaining()?,
                    )
                    .await?,
            )?),
            Action::GrantOrigin {
                revision,
                node,
                disk,
                process,
                label,
                server_name,
                addresses,
                read,
                write,
            } => {
                ensure!(
                    read || write,
                    "explicit namespace operation permission required"
                );
                self.cells
                    .inspect_peer_node(NodeId::new(self.config.node)?, revision, turn.remaining()?)
                    .await?;
                let observed = self
                    .cells
                    .inspect_peer_node(NodeId::new(node)?, revision, turn.remaining()?)
                    .await?;
                let boot = observed
                    .view
                    .fleet_boot
                    .context("missing authenticated origin boot")?;
                ensure!(
                    boot.node == node && boot.disk == disk && boot.process == process,
                    "namespace origin differs from its authenticated current boot"
                );
                let peer = RaftPeerNode {
                    node: NodeId::new(node)?,
                    disk: DiskLineage::new(disk)?,
                    process: ProcessGeneration::new(process)?,
                    label,
                    server_name,
                    control_address: addresses[0],
                    data_address: addresses[1],
                    query_address: addresses[2],
                };
                let cluster = ClusterId::new(id(&self.config.connection.cluster_id)?)?;
                let namespace = NamespaceId::new(id(&self.config.namespace().namespace_id)?)?;
                if read {
                    self.cells.grant_query_origin(NamespaceQueryOrigin {
                        cluster,
                        namespace,
                        revision,
                        peer: peer.clone(),
                    })?;
                }
                if write {
                    self.cells.grant_command_origin(NamespaceCommandOrigin {
                        cluster,
                        namespace,
                        revision,
                        peer,
                    })?;
                }
                Ok(json!({"read":read,"write":write}))
            }
            Action::Start { group, incarnation } => {
                let history = self.history(group, incarnation)?;
                self.cells
                    .start_managed_first_group(history, turn.remaining()?)
                    .await?;
                Ok(json!({"attached":true}))
            }
            Action::ReopenRecovery {
                selection,
                plan_digest,
            } => {
                ensure!(
                    selection.namespace_id == self.config.namespace().namespace_id,
                    "different configured namespace"
                );
                if let Some(previous) = self.recovery.lock().unwrap().as_ref() {
                    ensure!(
                        previous.selection == selection
                            && previous.plan.digest() == digest(&plan_digest)?,
                        "different active recovery selection"
                    );
                }
                let authority = CellGroupPermanentFenceAuthority::new(self.config.fence_public)?;
                let fenced = Arc::new(
                    self.control
                        .reopen_fenced_transition_recovery(
                            &self.config.principal,
                            &selection,
                            &authority,
                            turn.remaining()?,
                        )
                        .await?,
                );
                let plan = Arc::new(
                    self.control
                        .reopen_transition_recovery_replacement(
                            &selection.namespace_id,
                            &fenced,
                            turn.remaining()?,
                        )
                        .await?
                        .context("controller must stage the exact replacement plan first")?,
                );
                ensure!(
                    plan.digest() == digest(&plan_digest)?,
                    "different replacement plan"
                );
                let value = json!({"fence_digest":hex::encode(fenced.request().digest()),
                    "receipt_digest":hex::encode(fenced.receipt_digest()),
                    "plan_digest":hex::encode(plan.digest()), "ranges":plan.births().len()});
                *self.recovery.lock().unwrap() = Some(Recovery {
                    selection,
                    fenced,
                    plan,
                });
                Ok(value)
            }
            Action::PrepareSeed { group, incarnation } => {
                let history = self.history(group, incarnation)?;
                let selected = self.selected_recovery()?;
                let reservation = Arc::new(
                    self.control
                        .reopen_transition_recovery_range(
                            &selected.selection.namespace_id,
                            &selected.fenced,
                            &selected.plan,
                            history,
                            turn.remaining()?,
                        )
                        .await?
                        .context("controller must reserve this range first")?,
                );
                // Exactly one preparation is retained. Entered SDK workers own
                // their clones after cancellation; replacing this slot cannot
                // release their physical pins or bypass their resource bank.
                self.seed.lock().unwrap().take();
                let prepared = self
                    .cells
                    .prepare_managed_recovery_seed(
                        &selected.fenced,
                        &selected.plan,
                        &reservation,
                        &self.recovery_pool,
                        turn.remaining()?,
                    )
                    .await?;
                let value = json!({"prepared":true, "plan_digest":hex::encode(selected.plan.digest()),
                    "commitment":hex::encode(prepared.commitment())});
                *self.seed.lock().unwrap() = Some(Seed {
                    history,
                    reservation,
                    prepared,
                });
                Ok(value)
            }
            Action::Seed { group, incarnation } => {
                let history = self.history(group, incarnation)?;
                let selected = self.selected_recovery()?;
                let (reservation, prepared) = {
                    let prepared = self.seed.lock().unwrap();
                    let seed = prepared
                        .as_ref()
                        .context("prepare this range in this voter process first")?;
                    ensure!(seed.history == history, "different prepared range");
                    (seed.reservation.clone(), seed.prepared.clone())
                };
                let seeded = self
                    .cells
                    .seed_managed_recovery_range(
                        selected.fenced,
                        selected.plan,
                        reservation,
                        prepared,
                        turn.remaining()?,
                    )
                    .await?;
                Ok(
                    json!({"seeded":true, "group":seeded.route.history.group.bytes(),
                    "incarnation":seeded.route.history.incarnation.bytes(), "member":seeded.member.get(),
                    "plan_digest":hex::encode(seeded.plan_digest),
                    "commitment":hex::encode(seeded.genesis_commitment), "boot":seeded.boot}),
                )
            }
            Action::Attach {
                group,
                incarnation,
                operation,
                plan_digest,
                seed_digest,
            } => {
                let history = self.history(group, incarnation)?;
                self.cells
                    .attach_managed_recovery_voter(
                        self.attach_request(operation, plan_digest, seed_digest)?,
                        history,
                        turn.remaining()?,
                    )
                    .await?;
                Ok(json!({"attached":true}))
            }
            Action::Activate {
                group,
                incarnation,
                operation,
                plan_digest,
                seed_digest,
            } => {
                let group = self.cells.group(self.history(group, incarnation)?)?;
                let position = group
                    .activate_managed_recovery(
                        self.attach_request(operation, plan_digest, seed_digest)?,
                        turn.remaining()?,
                    )
                    .await?;
                Ok(
                    json!({"applied":position.map(|p| json!({"index":p.index.get(), "term":p.term.get()}))}),
                )
            }
            Action::Enroll {
                cell,
                incarnation,
                creation,
                s3,
            } => {
                let enrolled = self
                    .cell(cell, incarnation)?
                    .enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new(creation)?,
                            durability: durability(s3),
                        },
                        turn.remaining()?,
                    )
                    .await?;
                Ok(json!({"enrolled":true, "durability":durability_name(enrolled.durability())}))
            }
            Action::Write {
                cell,
                incarnation,
                session,
                sequence,
                retry_until_ms,
                delta,
                s3,
            } => {
                let command = CommandId::new(ClientId::new(session)?, sequence)?;
                let receipt = self
                    .cell(cell, incarnation)?
                    .command(
                        &self.add,
                        delta.to_be_bytes().to_vec(),
                        CommandOptions {
                            id: command,
                            durability: durability(s3),
                            retry_until_ms,
                            timeout: turn.remaining()?,
                        },
                    )
                    .await?;
                Ok(
                    json!({"result":hex::encode(receipt.result), "version":receipt.version.sequence.get(),
                    "token":receipt.version_token.as_str(), "durability":durability_name(receipt.durability),
                    "group":receipt.witness.group.group.bytes(), "incarnation":receipt.witness.group.incarnation.bytes()}),
                )
            }
            Action::Read {
                cell,
                incarnation,
                invocation,
                minimum,
            } => {
                let options = QueryOptions {
                    consistency: match minimum {
                        Some(token) => {
                            QueryConsistency::AtLeast(CellVersionToken::from_encoded(&token)?)
                        }
                        None => QueryConsistency::Linearizable,
                    },
                    timeout: turn.remaining()?,
                    ..Default::default()
                };
                let result = self
                    .cell(cell, incarnation)?
                    .query(ProposalId::new(invocation)?, &self.get, vec![], options)
                    .await?;
                Ok(
                    json!({"result":hex::encode(result.result), "version":result.version.sequence.get(),
                    "token":result.version_token.as_str(), "served_by":result.served_by.get()}),
                )
            }
        }
    }

    fn attach_request(
        &self,
        operation: [u8; 16],
        plan_digest: [u8; 32],
        seed_digest: [u8; 32],
    ) -> Result<FleetRecoveryAttachRequest> {
        ensure!(
            plan_digest != [0; 32] && seed_digest != [0; 32],
            "nonzero recovery commitments required"
        );
        Ok(FleetRecoveryAttachRequest {
            operation: ProposalId::new(operation)?,
            principal: self.config.principal.clone(),
            plan_digest,
            seed_digest,
        })
    }
}

fn durability(s3: bool) -> Durability {
    if s3 {
        Durability::S3
    } else {
        Durability::Quorum
    }
}

fn durability_name(value: Durability) -> &'static str {
    match value {
        Durability::Quorum => "quorum",
        Durability::S3 => "s3",
    }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value)
}

struct Budget(tokio::time::Instant);

impl Budget {
    fn new(timeout: Duration) -> Self {
        Self(tokio::time::Instant::now() + timeout)
    }

    fn remaining(&self) -> Result<Duration> {
        let remaining = self
            .0
            .saturating_duration_since(tokio::time::Instant::now());
        ensure!(
            remaining.as_millis() > 0,
            "control deadline expired; reconcile the original operation"
        );
        Ok(remaining)
    }
}
