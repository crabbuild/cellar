// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded controller step per process. Descriptive IDs reopen SDK proofs;
//! no serialized receipt, observation or local cache grants serving authority.
use super::*;
use cellar::sdk::{consensus::*, fleet_operations::*};
use serde::Deserialize;
use std::io::Read as _;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    endpoint: String,
    bucket: String,
    prefix: String,
    access: String,
    secret: String,
    bucket_prefix: String,
    connection: RaftFleetConnection,
    principal: String,
    namespace_id: String,
    fence_public: [u8; 32],
    command: Command,
}

#[derive(Deserialize)]
#[serde(tag = "step", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Describe,
    FleetStatus,
    RegisterNode {
        request: RaftFleetRegisterNode,
    },
    FirstStandby,
    FirstReserve {
        plan: RaftFleetFirstGroupPlan,
    },
    FirstPublish {
        request: RaftFleetFirstGroupPublish,
    },
    FirstArchive {
        request: RaftFleetFirstGroupMutation,
    },
    TransitionBasis,
    TransitionStatus {
        operation: String,
    },
    TransitionAdmit {
        request: RaftFleetTransitionAdmissionRequest,
    },
    TransitionPrepare {
        request: RaftFleetTransitionPrepareRequest,
    },
    TransitionCapture {
        request: RaftFleetTransitionSourceSealRequest,
    },
    TransitionVerifyArchive {
        request: RaftFleetTransitionPrepareRequest,
    },
    TransitionPublish {
        request: RaftFleetTransitionPublishRequest,
    },
    RecoveryBasis {
        operation: String,
    },
    HoldPrepare {
        basis: RaftFleetTransitionRecoveryBasis,
        source: RaftFleetTransitionGroup,
    },
    HoldAcquire {
        basis: RaftFleetTransitionRecoveryBasis,
        source: RaftFleetTransitionGroup,
        intent: Box<CellGroupArchiveIntent>,
    },
    Intent {
        basis: RaftFleetTransitionRecoveryBasis,
        operation: String,
        holds: Vec<CellGroupArchiveHold>,
    },
    IntentStatus {
        operation: String,
    },
    Challenge {
        operation: String,
        intent_digest: String,
    },
    Fence {
        selection: RaftFleetTransitionRecoverySelection,
        certificate: Vec<u8>,
    },
    Stage {
        selection: RaftFleetTransitionRecoverySelection,
        replacements: Vec<Replacement>,
    },
    Plan {
        basis: Selected,
    },
    Reserve {
        basis: Selected,
        history: History,
    },
    Reservation {
        basis: Selected,
        history: History,
    },
    SelectSeeds {
        basis: Selected,
    },
    Seeds {
        basis: Selected,
    },
    Publish {
        basis: Selected,
        seed_digest: String,
    },
    Publication {
        request: Published,
    },
    Activation {
        request: Published,
        history: History,
    },
    Settle {
        request: Published,
    },
    Complete {
        request: Published,
    },
    Release {
        request: Published,
        completion: String,
    },
    Retire {
        request: Published,
        completion: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selected {
    selection: RaftFleetTransitionRecoverySelection,
    plan_digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Published {
    operation: String,
    plan_digest: String,
    seed_digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct History {
    group: String,
    incarnation: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    node: String,
    member: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Replacement {
    history: History,
    creation: String,
    epoch: u64,
    voters: Vec<Member>,
    learners: Vec<Member>,
}

pub(super) async fn run(domain: &ExecutionDomain, options: &Options) -> Result<()> {
    let path = options.config.clone();
    let bytes = domain
        .blocking(move || -> Result<Vec<u8>> {
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(256 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            Ok(bytes)
        })?
        .await??;
    ensure!(
        bytes.len() <= 256 * 1024,
        "recovery configuration too large"
    );
    let config: Configuration = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("invalid recovery configuration"))?;
    config.validate()?;
    let turn = Budget(tokio::time::Instant::now() + bounds().timeout);
    let result = tokio::time::timeout(turn.remaining()?, execute(domain, options, &config, &turn))
        .await
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "recovery command deadline expired; reconcile the original command"
            ))
        });
    reply(result, &config.access, &config.secret)
}

pub(super) fn reply(result: Result<Value>, access: &str, secret: &str) -> Result<()> {
    match result {
        Ok(value) => output(json!({"managed_recovery":"ok", "result":value})),
        Err(error) => {
            let typed = error.downcast_ref::<RaftFleetFailure>();
            let message = format!("{error:#}")
                .replace(access, "[REDACTED]")
                .replace(secret, "[REDACTED]");
            output(json!({"managed_recovery":"error",
                "code":typed.map(|failure| failure.code).unwrap_or(RaftFleetFailureCode::Unavailable),
                "outcome":typed.map(|failure| failure.outcome).unwrap_or(RaftFleetOutcome::Unknown),
                "message":message}))?;
            anyhow::bail!("managed recovery refused; retain and reconcile the original command")
        }
    }
}

async fn execute(
    domain: &ExecutionDomain,
    options: &Options,
    config: &Configuration,
    turn: &Budget,
) -> Result<Value> {
    let bucket = crate::managed_store::open(
        &config.endpoint,
        &config.bucket,
        &config.prefix,
        &config.access,
        &config.secret,
        &config.bucket_prefix,
    )?;
    let control =
        RaftFleetControl::connect(domain, bucket, config.connection.clone(), turn.remaining()?)
            .await?
            .with_transition_preparer(
                crate::managed_operator::verifier(domain, options.data.clone(), config.rooted())
                    .await?,
                CellGroupRecoveryPool::new(Default::default())?,
            );
    let namespace = &config.namespace_id;
    match &config.command {
        Command::Describe => Ok(json!({"catalog_id":control.catalog_id(),
            "namespace_id":namespace, "namespace_digest":hex::encode(control.namespace_digest(namespace)?),
            "image_storage":config.connection.namespaces[0].image_storage})),
        Command::FleetStatus => Ok(serde_json::to_value(
            control.status(turn.remaining()?).await?,
        )?),
        Command::RegisterNode { request } => {
            let admission = control
                .register_node(&config.principal, request.clone(), turn.remaining()?)
                .await?;
            Ok(json!({"admission":admission}))
        }
        Command::FirstStandby => Ok(serde_json::to_value(
            control
                .first_group_standby(namespace, turn.remaining()?)
                .await?,
        )?),
        Command::FirstReserve { plan } => {
            let admission = control
                .reserve_first_group(&config.principal, namespace, plan, turn.remaining()?)
                .await?;
            Ok(serde_json::to_value(admission)?)
        }
        Command::FirstPublish { request } => {
            let admission = control
                .publish_first_group(&config.principal, namespace, request, turn.remaining()?)
                .await?;
            Ok(serde_json::to_value(admission)?)
        }
        Command::FirstArchive { request } => {
            let admission = control
                .initialize_first_group_archive(
                    &config.principal,
                    namespace,
                    request,
                    turn.remaining()?,
                )
                .await?;
            Ok(serde_json::to_value(admission)?)
        }
        Command::TransitionBasis => Ok(serde_json::to_value(
            control
                .transition_basis(namespace, turn.remaining()?)
                .await?,
        )?),
        Command::TransitionStatus { operation } => Ok(serde_json::to_value(
            control
                .transition_status(namespace, operation, turn.remaining()?)
                .await?,
        )?),
        Command::TransitionAdmit { request } => Ok(serde_json::to_value(
            control
                .admit_transition(
                    &config.principal,
                    namespace,
                    &request.operation_id,
                    request,
                    turn.remaining()?,
                )
                .await?,
        )?),
        Command::TransitionPrepare { request } => Ok(serde_json::to_value(
            control
                .prepare_transition(
                    &config.principal,
                    namespace,
                    &request.operation_id,
                    request,
                    turn.remaining()?,
                )
                .await?,
        )?),
        Command::TransitionCapture { request } => Ok(serde_json::to_value(
            control
                .capture_transition_source(
                    &config.principal,
                    namespace,
                    &request.operation_id,
                    request,
                    turn.remaining()?,
                )
                .await?,
        )?),
        Command::TransitionVerifyArchive { request } => Ok(serde_json::to_value(
            control
                .verify_transition_archive(
                    &config.principal,
                    namespace,
                    &request.operation_id,
                    request,
                    turn.remaining()?,
                )
                .await?,
        )?),
        Command::TransitionPublish { request } => Ok(serde_json::to_value(
            control
                .publish_transition(
                    &config.principal,
                    namespace,
                    &request.operation_id,
                    request,
                    turn.remaining()?,
                )
                .await?,
        )?),
        Command::RecoveryBasis { operation } => Ok(serde_json::to_value(
            control
                .transition_recovery_basis(namespace, operation, turn.remaining()?)
                .await?,
        )?),
        Command::HoldPrepare { basis, source } => Ok(serde_json::to_value(
            control
                .prepare_transition_recovery_hold(
                    &config.principal,
                    basis,
                    source,
                    turn.remaining()?,
                )
                .await?,
        )?),
        Command::HoldAcquire {
            basis,
            source,
            intent,
        } => Ok(serde_json::to_value(
            control
                .acquire_transition_recovery_hold(
                    &config.principal,
                    basis,
                    source,
                    intent,
                    turn.remaining()?,
                )
                .await?,
        )?),
        Command::Intent {
            basis,
            operation,
            holds,
        } => {
            let verified = control
                .verify_transition_recovery_archives(
                    &config.principal,
                    basis,
                    holds.clone(),
                    turn.remaining()?,
                )
                .await?;
            let intent = control
                .stage_transition_recovery_intent(
                    &config.principal,
                    operation,
                    &verified,
                    turn.remaining()?,
                )
                .await?;
            Ok(
                json!({"operation":hex::encode(intent.operation()),"intent_digest":hex::encode(intent.digest()),"holds":intent.archive_holds().len()}),
            )
        }
        Command::IntentStatus { operation } => {
            let intent = control
                .reopen_transition_recovery_intent(
                    &config.principal,
                    namespace,
                    operation,
                    turn.remaining()?,
                )
                .await?;
            Ok(
                json!({"intent":intent.map(|intent|json!({"operation":hex::encode(intent.operation()),"intent_digest":hex::encode(intent.digest()),"holds":intent.archive_holds().len()}))}),
            )
        }
        Command::Challenge {
            operation,
            intent_digest,
        } => {
            let intent = control
                .reopen_transition_recovery_intent(
                    &config.principal,
                    namespace,
                    operation,
                    turn.remaining()?,
                )
                .await?
                .context("stage recovery intent first")?;
            ensure!(
                hex::encode(intent.digest()) == *intent_digest,
                "different recovery intent"
            );
            let (selection, challenge) = control
                .transition_recovery_challenge(
                    &config.principal,
                    namespace,
                    &intent,
                    turn.remaining()?,
                )
                .await?;
            Ok(
                json!({"selection":selection,"challenge":serde_json::from_slice::<Value>(&challenge)?}),
            )
        }
        Command::Fence {
            selection,
            certificate,
        } => {
            ensure!(
                selection.catalog_id == control.catalog_id()
                    && selection.namespace_id == *namespace,
                "different recovery scope"
            );
            let authority = CellGroupPermanentFenceAuthority::new(config.fence_public)?
                .with_certificate(certificate.clone())?;
            let fenced = control
                .fence_selected_transition_recovery(
                    &config.principal,
                    selection,
                    &authority,
                    turn.remaining()?,
                )
                .await?;
            Ok(
                json!({"fence_digest":hex::encode(fenced.request().digest()),"receipt_digest":hex::encode(fenced.receipt_digest()),"ranges":fenced.archives().len(),
                "cuts":fenced.archives().iter().map(|archive|json!({"group":hex::encode(archive.marker().route().history.group.bytes()),
                    "incarnation":hex::encode(archive.marker().route().history.incarnation.bytes()),"epoch":archive.marker().route().epoch.get(),
                    "position":archive.marker().position().index.get()})).collect::<Vec<_>>()}),
            )
        }
        Command::Stage {
            selection,
            replacements,
        } => {
            let fenced = config.fenced(&control, selection, turn).await?;
            ensure!(
                replacements.len() == fenced.archives().len(),
                "one replacement per selected cut required"
            );
            let members = |rows: &[Member]| -> Result<Vec<InitialMember>> {
                rows.iter()
                    .map(|row| {
                        Ok(InitialMember {
                            node: NodeId::new(id(&row.node)?)?,
                            member: MemberId::new(row.member)?,
                        })
                    })
                    .collect()
            };
            let births = replacements
                .iter()
                .zip(fenced.archives())
                .map(|(replacement, cut)| {
                    Ok(CellGroupBirthSpec {
                        history: config.history(&replacement.history)?,
                        range: cut.marker().route().range.clone(),
                        epoch: RouteEpoch::new(replacement.epoch)?,
                        creation: ProposalId::new(id(&replacement.creation)?)?,
                        voters: members(&replacement.voters)?,
                        learners: members(&replacement.learners)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let plan = control
                .stage_transition_recovery_replacement(
                    namespace,
                    &fenced,
                    births,
                    turn.remaining()?,
                )
                .await?;
            Ok(plan_view(&plan))
        }
        Command::Plan { basis } => {
            let (_, plan) = config.plan(&control, basis, turn).await?;
            Ok(plan_view(&plan))
        }
        Command::Reserve { basis, history } | Command::Reservation { basis, history } => {
            let (fenced, plan) = config.plan(&control, basis, turn).await?;
            let history = config.history(history)?;
            let reservation = if matches!(config.command, Command::Reserve { .. }) {
                Some(
                    control
                        .reserve_transition_recovery_range(
                            namespace,
                            &fenced,
                            &plan,
                            history,
                            turn.remaining()?,
                        )
                        .await?,
                )
            } else {
                control
                    .reopen_transition_recovery_range(
                        namespace,
                        &fenced,
                        &plan,
                        history,
                        turn.remaining()?,
                    )
                    .await?
            };
            Ok(match reservation {
                None => json!({"reservation":null}),
                Some(value) => json!({"reservation":{
                    "group":hex::encode(value.route().history.group.bytes()),
                    "incarnation":hex::encode(value.route().history.incarnation.bytes()),
                    "plan_digest":hex::encode(value.plan_digest()),
                    "applied_revision":value.applied_revision(), "voter_boots":value.voter_boots()}}),
            })
        }
        Command::SelectSeeds { basis } | Command::Seeds { basis } => {
            let (fenced, plan) = config.plan(&control, basis, turn).await?;
            let seeds = if matches!(config.command, Command::SelectSeeds { .. }) {
                Some(
                    control
                        .select_transition_recovery_seed_quorums(
                            namespace,
                            &fenced,
                            &plan,
                            turn.remaining()?,
                        )
                        .await?,
                )
            } else {
                control
                    .reopen_transition_recovery_seed_quorums(
                        namespace,
                        &fenced,
                        &plan,
                        turn.remaining()?,
                    )
                    .await?
            };
            Ok(match seeds {
                None => json!({"seeds":null}),
                Some(value) => json!({"seeds":{"plan_digest":hex::encode(value.plan_digest()),
                    "seed_digest":hex::encode(value.digest()), "ranges":value.ranges().len()}}),
            })
        }
        Command::Publish { basis, seed_digest } => {
            let expected = id::<32>(seed_digest)?;
            let (fenced, plan) = config.plan(&control, basis, turn).await?;
            let seeds = control
                .reopen_transition_recovery_seed_quorums(
                    namespace,
                    &fenced,
                    &plan,
                    turn.remaining()?,
                )
                .await?
                .context("select seed quorums before publication")?;
            ensure!(seeds.digest() == expected, "different selected seed set");
            let published = control
                .publish_transition_recovery(namespace, &fenced, &plan, &seeds, turn.remaining()?)
                .await?;
            Ok(publication_view(Some(&published)))
        }
        Command::Publication { request } => {
            let selected = config.request(request)?;
            let publication = control
                .reopen_transition_recovery_publication(
                    namespace,
                    &hex::encode(selected.operation.bytes()),
                    &selected.principal,
                    (selected.plan_digest, selected.seed_digest),
                    turn.remaining()?,
                )
                .await?;
            Ok(publication_view(publication.as_ref()))
        }
        Command::Activation { request, history } => {
            let position = control
                .reopen_transition_recovery_activation(
                    namespace,
                    &config.request(request)?,
                    config.history(history)?,
                    turn.remaining()?,
                )
                .await?;
            Ok(
                json!({"activation":position.map(|p| json!({"index":p.index.get(), "term":p.term.get()}))}),
            )
        }
        Command::Settle { request } => {
            let progress = control
                .advance_transition_recovery_settlement(
                    namespace,
                    &config.request(request)?,
                    turn.remaining()?,
                )
                .await?;
            Ok(
                json!({"promoted_ranges":progress.promoted_ranges, "total_ranges":progress.total_ranges,
                "applied_revision":progress.applied_revision, "publication_digest":hex::encode(progress.publication_digest)}),
            )
        }
        Command::Complete { request } => {
            let completion = control
                .record_transition_recovery_directory_completion(
                    namespace,
                    &config.request(request)?,
                    turn.remaining()?,
                )
                .await?;
            Ok(json!({"completion":hex::encode(completion)}))
        }
        Command::Release {
            request,
            completion,
        } => {
            let completion = control
                .release_transition_recovery_serving(
                    namespace,
                    &config.request(request)?,
                    id(completion)?,
                    turn.remaining()?,
                )
                .await?;
            Ok(json!({"completion":hex::encode(completion)}))
        }
        Command::Retire {
            request,
            completion,
        } => {
            let authority = CellGroupPermanentFenceAuthority::new(config.fence_public)?;
            let progress = control
                .advance_transition_recovery_retirement(
                    namespace,
                    &config.request(request)?,
                    id(completion)?,
                    &authority,
                    turn.remaining()?,
                )
                .await?;
            Ok(
                json!({"retired_histories":progress.retired_histories, "total_histories":progress.total_histories,
                "applied_revision":progress.applied_revision, "fleet_slot_released":progress.fleet_slot_released}),
            )
        }
    }
}

impl Configuration {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.access.is_empty() && !self.secret.is_empty(),
            "explicit S3 credentials required"
        );
        ensure!(
            !self.principal.is_empty()
                && self.principal.len() <= 128
                && !self.principal.chars().any(char::is_control),
            "explicit operator principal required"
        );
        ensure!(
            self.connection.namespaces.len() == 1
                && self.connection.namespaces[0].managed
                && self.connection.namespaces[0].namespace_id == self.namespace_id,
            "one exact existing managed namespace required"
        );
        id::<16>(&self.namespace_id)?;
        id::<16>(&self.connection.cluster_id)?;
        CellGroupPermanentFenceAuthority::new(self.fence_public)?;
        Ok(())
    }

    fn rooted(&self) -> bool {
        self.connection.namespaces[0].image_storage == RaftImageStorage::Extents
    }

    fn history(&self, history: &History) -> Result<GroupHistory> {
        Ok(GroupHistory {
            cluster: ClusterId::new(id(&self.connection.cluster_id)?)?,
            namespace: NamespaceId::new(id(&self.namespace_id)?)?,
            group: GroupId::new(id(&history.group)?)?,
            incarnation: IncarnationId::new(id(&history.incarnation)?)?,
        })
    }

    fn request(&self, request: &Published) -> Result<FleetRecoveryAttachRequest> {
        Ok(FleetRecoveryAttachRequest {
            operation: ProposalId::new(id(&request.operation)?)?,
            principal: self.principal.clone(),
            plan_digest: id(&request.plan_digest)?,
            seed_digest: id(&request.seed_digest)?,
        })
    }

    async fn fenced(
        &self,
        control: &RaftFleetControl,
        selection: &RaftFleetTransitionRecoverySelection,
        turn: &Budget,
    ) -> Result<FleetFencedTransitionRecovery> {
        selection.validate()?;
        ensure!(
            selection.namespace_id == self.namespace_id
                && selection.catalog_id == control.catalog_id(),
            "different recovery scope"
        );
        Ok(control
            .reopen_fenced_transition_recovery(
                &self.principal,
                selection,
                &CellGroupPermanentFenceAuthority::new(self.fence_public)?,
                turn.remaining()?,
            )
            .await?)
    }

    async fn plan(
        &self,
        control: &RaftFleetControl,
        basis: &Selected,
        turn: &Budget,
    ) -> Result<(
        FleetFencedTransitionRecovery,
        FleetTransitionRecoveryReplacement,
    )> {
        let expected = id::<32>(&basis.plan_digest)?;
        let fenced = self.fenced(control, &basis.selection, turn).await?;
        let plan = control
            .reopen_transition_recovery_replacement(&self.namespace_id, &fenced, turn.remaining()?)
            .await?
            .context("stage the selected replacement first")?;
        ensure!(plan.digest() == expected, "different replacement plan");
        Ok((fenced, plan))
    }
}

fn plan_view(plan: &FleetTransitionRecoveryReplacement) -> Value {
    json!({"plan_digest":hex::encode(plan.digest()), "ranges":plan.births().iter().map(|birth| {
        json!({"group":hex::encode(birth.route().history.group.bytes()),
            "incarnation":hex::encode(birth.route().history.incarnation.bytes()),
            "creation":hex::encode(birth.creation().bytes()), "epoch":birth.route().epoch.get()})
    }).collect::<Vec<_>>()})
}

fn publication_view(publication: Option<&DirectoryPublication>) -> Value {
    json!({"publication":publication.map(|value| json!({"operation":hex::encode(value.operation().bytes()),
        "revision":value.revision(), "tree_digest":value.tree_digest().map(hex::encode),
        "journal_digest":hex::encode(value.journal_digest())}))})
}

fn id<const N: usize>(value: &str) -> Result<[u8; N]> {
    ensure!(
        value.len() == 2 * N
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "canonical hexadecimal identity required"
    );
    let mut bytes = [0; N];
    hex::decode_to_slice(value, &mut bytes)?;
    ensure!(bytes != [0; N], "nonzero identity required");
    Ok(bytes)
}

struct Budget(tokio::time::Instant);

impl Budget {
    fn remaining(&self) -> Result<Duration> {
        let remaining = self
            .0
            .saturating_duration_since(tokio::time::Instant::now());
        ensure!(
            remaining.as_millis() > 0,
            "recovery deadline expired; reconcile the original command"
        );
        Ok(remaining)
    }
}
