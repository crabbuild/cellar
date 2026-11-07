// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Execute at most one local step and one coordination step per rooted round.
use super::*;
use crate::raft::fleet_catalog::{ReopenedManagedCellGroupTransition, TransitionOperatorStep};
use cellar_logic::cell_group::GroupRoute;
use sha2::{Digest, Sha256};

pub(super) struct Context<'a> {
    pub(super) control: &'a RaftFleetControl,
    pub(super) directory: &'a RangeDirectory,
    pub(super) consent: &'a ManagedTransitionExecution,
    pub(super) transition: &'a ReopenedManagedCellGroupTransition,
    pub(super) parents: &'a [GroupRoute],
    pub(super) children: &'a [GroupRoute],
}

impl Context<'_> {
    fn namespace(&self) -> String {
        hex::encode(self.directory.namespace_config().namespace.bytes())
    }

    fn operation(&self) -> String {
        hex::encode(self.consent.identity.operation)
    }

    fn principal(&self) -> &str {
        &self.consent.identity.principal
    }

    fn plan(&self) -> String {
        hex::encode(self.consent.plan)
    }

    fn phase(&self) -> Option<TransitionPhase> {
        self.transition.journal.as_ref().map(|j| j.phase())
    }

    fn request(&self, purpose: &[u8], group: Option<GroupHistory>) -> String {
        let mut digest = Sha256::new();
        digest.update(b"cellar-transition-execution-step-1");
        digest.update(self.consent.identity.id);
        digest.update(self.consent.plan);
        digest.update(purpose);
        if let Some(group) = group {
            digest.update(group.group.bytes());
            digest.update(group.incarnation.bytes());
        }
        let bytes = digest.finalize();
        let mut id = [0; 16];
        id.copy_from_slice(&bytes[..16]);
        id[0] |= 1;
        hex::encode(id)
    }

    fn prepare(&self, purpose: &[u8]) -> RaftFleetTransitionPrepareRequest {
        RaftFleetTransitionPrepareRequest {
            catalog_id: self.control.catalog_id.clone(),
            namespace_id: self.namespace(),
            operation_id: self.operation(),
            request_id: self.request(purpose, None),
            plan_digest: self.plan(),
        }
    }

    fn source(&self, route: &GroupRoute, purpose: &[u8]) -> RaftFleetTransitionSourceSealRequest {
        RaftFleetTransitionSourceSealRequest {
            catalog_id: self.control.catalog_id.clone(),
            namespace_id: self.namespace(),
            operation_id: self.operation(),
            request_id: self.request(purpose, Some(route.history)),
            plan_digest: self.plan(),
            source: group(route.history),
        }
    }

    fn child(
        &self,
        route: &GroupRoute,
        step: RaftFleetTransitionChildStep,
    ) -> RaftFleetTransitionChildSetupRequest {
        RaftFleetTransitionChildSetupRequest {
            catalog_id: self.control.catalog_id.clone(),
            namespace_id: self.namespace(),
            operation_id: self.operation(),
            request_id: self.request(
                match step {
                    RaftFleetTransitionChildStep::Install => b"install-child",
                    RaftFleetTransitionChildStep::Attach => b"attach-child",
                    RaftFleetTransitionChildStep::Quorum => b"child-quorum",
                    RaftFleetTransitionChildStep::InitializeArchive => b"initialize-child-archive",
                },
                Some(route.history),
            ),
            plan_digest: self.plan(),
            child: group(route.history),
            step,
        }
    }
}

fn group(history: GroupHistory) -> RaftFleetTransitionGroup {
    RaftFleetTransitionGroup {
        group_id: hex::encode(history.group.bytes()),
        incarnation_id: hex::encode(history.incarnation.bytes()),
    }
}

pub(super) async fn local(
    cx: &Context<'_>,
    state: &mut worker::Work,
    index: usize,
    turn: &Turn<'_>,
) -> Result<()> {
    let observer = cx
        .control
        .node_observer
        .as_ref()
        .ok_or_else(RaftFleetFailure::unsupported)?;
    let phase = cx.phase();
    if phase == Some(TransitionPhase::Retired)
        && state.options.reclaim_retired_sources
        && cx
            .control
            .catalog
            .source_cleanup_status(cx.transition, cx.directory, turn.remaining()?)
            .await?
            .is_some()
    {
        state.action("collect-and-settle-source");
        cx.control
            .catalog
            .settle_local_source_cleanup(cx.transition, cx.directory, observer, turn.remaining()?)
            .await?;
        return Ok(());
    }
    if phase == Some(TransitionPhase::Retired) && state.options.quiesce_retired_sources {
        let route = &cx.parents[index % cx.parents.len()];
        if observer.group(route.history).is_ok() {
            state.action("quiesce-retired-source");
            cx.control
                .quiesce_transition_source(
                    cx.principal(),
                    &cx.namespace(),
                    &cx.operation(),
                    &cx.source(route, b"quiesce-source"),
                    turn.remaining()?,
                )
                .await?;
            return Ok(());
        }
    }
    let sources = match phase {
        Some(TransitionPhase::SealAuthorized) => {
            index % (cx.parents.len() + cx.children.len()) < cx.parents.len()
        }
        Some(TransitionPhase::Active) => {
            cx.transition
                .journal
                .as_ref()
                .is_some_and(|j| j.child_archives().is_some())
                && index % (cx.parents.len() + cx.children.len()) < cx.parents.len()
        }
        _ => false,
    };
    if sources {
        let route = &cx.parents[index % cx.parents.len()];
        if observer.group(route.history).is_ok() {
            let request = cx.source(
                route,
                if phase == Some(TransitionPhase::SealAuthorized) {
                    b"seal-source"
                } else {
                    b"retire-source"
                },
            );
            if phase == Some(TransitionPhase::SealAuthorized) {
                state.action("seal-source");
                cx.control
                    .seal_transition_source(
                        cx.principal(),
                        &cx.namespace(),
                        &cx.operation(),
                        &request,
                        turn.remaining()?,
                    )
                    .await?;
            } else {
                state.action("retire-source");
                cx.control
                    .retire_transition_source(
                        cx.principal(),
                        &cx.namespace(),
                        &cx.operation(),
                        &request,
                        turn.remaining()?,
                    )
                    .await?;
            }
            return Ok(());
        }
    }
    match phase {
        Some(
            TransitionPhase::SealAuthorized
            | TransitionPhase::Sealed
            | TransitionPhase::Installed
            | TransitionPhase::Archived,
        ) => {
            let route = &cx.children[index % cx.children.len()];
            let snapshot = cx.control.catalog.read(turn.remaining()?).await?;
            let birth = cx
                .control
                .catalog
                .history(&snapshot, &route.history, turn.remaining()?)
                .await?
                .initial_placement()?;
            if !birth
                .voters
                .iter()
                .any(|member| member.node == observer.identity().node)
            {
                return Ok(());
            }
            let step = state
                .local
                .get(&route.history)
                .copied()
                .unwrap_or(RaftFleetTransitionChildStep::Install);
            state.action(match step {
                RaftFleetTransitionChildStep::Install => "install-child",
                RaftFleetTransitionChildStep::Attach => "attach-child",
                _ => "child-quorum",
            });
            cx.control
                .setup_transition_child(
                    cx.principal(),
                    &cx.namespace(),
                    &cx.operation(),
                    &cx.child(route, step),
                    turn.remaining()?,
                )
                .await?;
            state.local.insert(
                route.history,
                match step {
                    RaftFleetTransitionChildStep::Install => RaftFleetTransitionChildStep::Attach,
                    _ => RaftFleetTransitionChildStep::Quorum,
                },
            );
        }
        Some(TransitionPhase::Published) => {
            let route = &cx.children[index % cx.children.len()];
            if observer.group(route.history).is_err() {
                return Ok(());
            }
            let request = RaftFleetTransitionActivationRequest {
                catalog_id: cx.control.catalog_id.clone(),
                namespace_id: cx.namespace(),
                operation_id: cx.operation(),
                request_id: cx.request(b"activate", Some(route.history)),
                plan_digest: cx.plan(),
                group_id: hex::encode(route.history.group.bytes()),
                incarnation_id: hex::encode(route.history.incarnation.bytes()),
            };
            state.action("activate-child");
            cx.control
                .activate_transition_child(
                    cx.principal(),
                    &cx.namespace(),
                    &cx.operation(),
                    &request,
                    turn.remaining()?,
                )
                .await?;
        }
        Some(TransitionPhase::Active) => {
            let route = &cx.children[index % cx.children.len()];
            if let Ok(group) = observer.group(route.history) {
                state.action("attach-child-archive");
                group
                    .ensure_existing_archive(turn.remaining()?)
                    .await
                    .map_err(|error| match error {
                        crate::raft::cell_groups::CellDispatchError::Runtime(
                            crate::raft::completion::RuntimeError::Overloaded,
                        ) => FleetCatalogError::Overloaded.into(),
                        _ => RaftFleetFailure::from(FleetCatalogError::Unconfirmed),
                    })?;
            }
        }
        _ => (),
    }
    Ok(())
}

pub(super) async fn coordinate(
    cx: &Context<'_>,
    state: &mut worker::Work,
    index: usize,
    turn: &Turn<'_>,
) -> Result<()> {
    let c = cx.control;
    let namespace = cx.namespace();
    let operation = cx.operation();
    let principal = cx.principal();
    match cx.phase() {
        None => {
            c.catalog
                .plan_managed_transition(
                    &cx.transition.request,
                    cx.directory,
                    &cx.transition.plan,
                    turn.remaining()?,
                )
                .await?;
        }
        Some(TransitionPhase::Planned) => {
            state.action("prepare");
            c.prepare_transition(
                principal,
                &namespace,
                &operation,
                &cx.prepare(b"prepare"),
                turn.remaining()?,
            )
            .await?;
        }
        Some(TransitionPhase::Prepared) => {
            let request = RaftFleetTransitionSealRequest {
                catalog_id: c.catalog_id.clone(),
                namespace_id: namespace.clone(),
                operation_id: operation.clone(),
                request_id: cx.request(b"authorize-seal", None),
                plan_digest: cx.plan(),
                preparation_digest: hex::encode(
                    cx.transition
                        .journal
                        .as_ref()
                        .and_then(|j| j.preparation())
                        .ok_or(FleetCatalogError::Conflict)?,
                ),
            };
            state.action("authorize-seal");
            c.authorize_transition_seal(
                principal,
                &namespace,
                &operation,
                &request,
                turn.remaining()?,
            )
            .await?;
        }
        Some(TransitionPhase::SealAuthorized) => {
            state.action("capture-source");
            c.capture_transition_source(
                principal,
                &namespace,
                &operation,
                &cx.source(&cx.parents[index % cx.parents.len()], b"capture-source"),
                turn.remaining()?,
            )
            .await?;
        }
        Some(TransitionPhase::Installed) => {
            state.action("verify-transfer");
            c.verify_transition_archive(
                principal,
                &namespace,
                &operation,
                &cx.prepare(b"verify-transfer"),
                turn.remaining()?,
            )
            .await?;
        }
        Some(TransitionPhase::Archived) => {
            let request = RaftFleetTransitionPublishRequest {
                catalog_id: c.catalog_id.clone(),
                namespace_id: namespace.clone(),
                operation_id: operation.clone(),
                request_id: cx.request(b"publish", None),
                plan_digest: cx.plan(),
                archive_digest: hex::encode(
                    cx.transition
                        .journal
                        .as_ref()
                        .and_then(|j| j.transfer_archive())
                        .ok_or(FleetCatalogError::Conflict)?,
                ),
            };
            state.action("publish");
            c.publish_transition(
                principal,
                &namespace,
                &operation,
                &request,
                turn.remaining()?,
            )
            .await?;
        }
        Some(TransitionPhase::Active) => {
            if let Some(child) = cx
                .children
                .iter()
                .find(|child| !state.initialized.contains(&child.history))
            {
                state.action("initialize-child-archive");
                c.setup_transition_child(
                    principal,
                    &namespace,
                    &operation,
                    &cx.child(child, RaftFleetTransitionChildStep::InitializeArchive),
                    turn.remaining()?,
                )
                .await?;
                state.initialized.push(child.history);
            } else {
                state.action("verify-child-archives");
                c.verify_transition_child_archives(
                    principal,
                    &namespace,
                    &operation,
                    &cx.prepare(b"verify-child-archives"),
                    turn.remaining()?,
                )
                .await?;
            }
        }
        Some(TransitionPhase::Retired) => {
            let snapshot = c.catalog.read(turn.remaining()?).await?;
            let mut reserved = false;
            for child in cx.children {
                reserved |= matches!(
                    c.catalog
                        .history(&snapshot, &child.history, turn.remaining()?)
                        .await?
                        .state(),
                    crate::raft::fleet_catalog::FleetHistoryState::Reserved
                );
            }
            if !reserved && state.options.reclaim_retired_sources {
                if !cx
                    .transition
                    .journal
                    .as_ref()
                    .is_some_and(|j| j.parent_holds_released())
                {
                    state.action("release-parent-holds");
                    c.catalog
                        .release_managed_transfer_parent_holds(
                            cx.transition,
                            cx.directory,
                            &c.archive_pool,
                            c.node_observer
                                .as_ref()
                                .ok_or_else(RaftFleetFailure::unsupported)?,
                            turn.remaining()?,
                        )
                        .await?;
                    return Ok(());
                }
                match c
                    .catalog
                    .source_cleanup_status(cx.transition, cx.directory, turn.remaining()?)
                    .await?
                {
                    None => {
                        state.action("select-source-cleanup");
                        c.catalog
                            .begin_source_cleanup(
                                cx.transition,
                                cx.directory,
                                c.node_observer
                                    .as_ref()
                                    .ok_or_else(RaftFleetFailure::unsupported)?,
                                turn.remaining()?,
                            )
                            .await?;
                        return Ok(());
                    }
                    Some(false) => {
                        state.action("await-source-settlements");
                        return Ok(());
                    }
                    Some(true) => (),
                }
            }
            let step = if reserved {
                RaftFleetTransitionResumeStep::ConfirmChildren
            } else {
                RaftFleetTransitionResumeStep::Complete
            };
            let identity = TransitionOperatorStep {
                operation: cx.transition.plan.operation(),
                principal: principal.into(),
                request_digest: cx.consent.plan,
                step: records::id(
                    &cx.request(if reserved { b"confirm" } else { b"complete" }, None),
                )?,
            }
            .identity(snapshot.revision());
            let request = RaftFleetTransitionResume {
                catalog_id: c.catalog_id.clone(),
                namespace_id: namespace.clone(),
                operation_id: operation.clone(),
                request_id: hex::encode(identity.id),
                expected_revision: snapshot.revision(),
                plan_digest: cx.plan(),
                step,
            };
            state.action(if reserved {
                "confirm-children"
            } else {
                "complete"
            });
            c.resume_transition(
                principal,
                &namespace,
                &operation,
                &request,
                turn.remaining()?,
            )
            .await?;
        }
        _ => (),
    }
    Ok(())
}
