// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed learner startup belongs to the node's retained provisioning owner.
use super::startup::{admission_error, catalog_error};
use super::*;
use crate::application::consensus::client::LearnerActivation;
use crate::raft::{
    catalog::CatalogError,
    completion::RuntimeError,
    fleet_catalog::{FleetGroupAuthorization, FleetPermitAuthorization},
    member_state::{FleetMembershipStatus, FleetPermitPhase},
};

impl RaftCells {
    /// Install and attach one catalog-selected physical learner. Success is a
    /// local durable receipt; it cannot promote the learner or grant serving.
    /// Once submitted, cancellation retains preparation and joins entered I/O.
    pub fn activate_managed_learner_group(
        &self,
        group: FleetGroupAuthorization,
        authorization: FleetPermitAuthorization,
        activation: LearnerActivation,
    ) -> impl Future<Output = Result<FleetMembershipStatus, CellGroupStartupError>> + Send + 'static
    {
        let admitted = (|| {
            let (deadline, memory) = self
                .metadata_admission(activation.timeout, 4 * 1024 * 1024)
                .map_err(admission_error)?;
            let host = self
                .handle
                .core
                .managed
                .clone()
                .ok_or(CellGroupControllerError::Invalid("managed learner owner"))?;
            let history = group
                .binding()
                .scope()
                .map_err(|e| catalog_error(CatalogError::Log(e)))?;
            if group.binding() != authorization.permit().binding()
                || group.binding().fleet() != host.fleet()
                || activation.from_s3
            {
                return Err(CellGroupControllerError::Invalid(
                    "managed learner capability",
                ));
            }
            let directory =
                self.handle
                    .core
                    .host
                    .directory(history)
                    .map_err(|error| match error {
                        CellDispatchError::Runtime(error) => error.into(),
                        _ => CellGroupControllerError::Invalid("learner namespace capability"),
                    })?;
            Ok((deadline, memory, host, history, directory))
        })();
        let owner = self.internal_observer();
        async move {
            use CellGroupStartupError::{Refused, Unconfirmed};
            let (deadline, _memory, host, history, directory) = admitted.map_err(Refused)?;
            let mut submitted = false;
            let work = async {
                owner.startup_live(&host, deadline).map_err(Refused)?;
                let birth = directory
                    .materialize_selected_history(history)
                    .await
                    .map_err(catalog_error)
                    .map_err(Refused)?;
                if !group.check_birth(&directory, &birth) {
                    return Err(Refused(CellGroupControllerError::Invalid("learner birth")));
                }
                let fresh = host
                    .group_authorization(
                        &history,
                        owner.startup_remaining(deadline).map_err(Refused)?,
                    )
                    .await
                    .map_err(CatalogError::from)
                    .map_err(catalog_error)
                    .map_err(Refused)?;
                if fresh.binding() != group.binding() || !fresh.check_birth(&directory, &birth) {
                    return Err(Refused(CellGroupControllerError::Invalid(
                        "learner history changed",
                    )));
                }
                owner.startup_live(&host, deadline).map_err(Refused)?;
                let member = activation.member;
                let call = owner
                    .handle
                    .core
                    .controller
                    .provision_managed_learner(
                        directory.clone(),
                        birth.route().clone(),
                        host.clone(),
                        authorization.clone(),
                        activation,
                        deadline,
                    )
                    .map_err(Refused)?;
                submitted = true;
                call.response()
                    .await
                    .map_err(|e| Unconfirmed((*e).clone()))?;
                owner.startup_live(&host, deadline).map_err(Unconfirmed)?;
                let fresh = host
                    .group_authorization(
                        &history,
                        owner.startup_remaining(deadline).map_err(Unconfirmed)?,
                    )
                    .await
                    .map_err(CatalogError::from)
                    .map_err(catalog_error)
                    .map_err(Unconfirmed)?;
                if fresh.binding() != group.binding() || !fresh.check_birth(&directory, &birth) {
                    return Err(Unconfirmed(CellGroupControllerError::Invalid(
                        "learner history changed",
                    )));
                }
                let core = &owner.handle.core;
                if core
                    .controller
                    .group_failure(history)
                    .map_err(CellGroupControllerError::from)
                    .map_err(Unconfirmed)?
                    .is_some()
                {
                    return Err(Unconfirmed(RuntimeError::GroupQuarantined.into()));
                }
                let local = core
                    .log
                    .cell_group(&history)
                    .map_err(|e| Unconfirmed(RuntimeError::from(e).into()))?;
                let status = {
                    let cache = local
                        .read()
                        .map_err(|e| Unconfirmed(RuntimeError::from(e).into()))?;
                    let joined = cache
                        .membership
                        .joined(member)
                        .map_err(|e| Unconfirmed(RuntimeError::from(e).into()))?;
                    let status = cache.membership.fleet_status().ok_or(Unconfirmed(
                        CellGroupControllerError::Invalid("learner fleet guard"),
                    ))?;
                    if local.identity().member != member
                        || !fresh.matches_replica(local.identity(), &cache.membership)
                        || cache
                            .group_manifest
                            .as_ref()
                            .is_none_or(|m| m.route() != birth.route() || !m.is_active())
                        || joined.is_none_or(|(binding, at)| {
                            binding.node != core.node.node
                                || binding.disk != core.node.disk
                                || cache.group_seed.is_none_or(|seed| seed < at.get())
                        })
                        || status.permit.as_ref() != Some(authorization.permit())
                        || &status.binding != authorization.permit().binding()
                        || status.consumed_sequence != authorization.permit().sequence()
                        || !matches!(
                            status.phase,
                            Some(
                                FleetPermitPhase::Seeding
                                    | FleetPermitPhase::Joint
                                    | FleetPermitPhase::Complete
                            )
                        )
                    {
                        return Err(Unconfirmed(CellGroupControllerError::Invalid(
                            "learner installed receipt",
                        )));
                    }
                    status
                };
                owner.startup_live(&host, deadline).map_err(Unconfirmed)?;
                Ok(status)
            };
            match owner
                .handle
                .core
                .transport
                .seed_control_until(deadline, async {
                    owner
                        .handle
                        .domain
                        .run(work)
                        .await
                        .map_err(|_| crate::raft::transport::TransportError::Closed)
                })
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    let error = CellGroupControllerError::Transport(error);
                    Err(if submitted {
                        Unconfirmed(error)
                    } else {
                        Refused(error)
                    })
                }
            }
        }
    }
}
