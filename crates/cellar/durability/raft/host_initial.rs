//! Initial voters use the same owned SQL/attachment pipeline as learners, but
//! their authority is the admitted immutable birth, never a recovery snapshot.
use super::*;
use crate::raft::fleet_catalog::{FleetCatalogError, FleetGroupAuthorization};
use crate::raft::member_state::FleetMembershipStatus;
use std::time::Duration;

impl Driver {
    pub(super) fn initial_receipt(
        &self,
        scope: &GroupScope,
        member: MemberId,
    ) -> Result<FleetMembershipStatus, CommandFailure> {
        let cell = self.cells.get(scope).ok_or(CommandFailure::Unavailable)?;
        if cell.failed || cell.retired || cell.group.identity().member != member {
            return Err(CommandFailure::Unavailable);
        }
        let cache = cell
            .group
            .read()
            .map_err(|_| CommandFailure::CorruptState)?;
        if !super::super::member_state::members(&cache.conf_state).contains(&member.get())
            || !cache.membership.permits(cell.group.identity())
        {
            return Err(CommandFailure::CommandConflict);
        }
        cache
            .membership
            .fleet_status()
            .ok_or(CommandFailure::CorruptState)
    }

    pub(super) fn activate_initial(
        &mut self,
        mut request: Request,
        authorization: FleetGroupAuthorization,
    ) {
        let setup = (|| -> Result<_, CommandFailure> {
            let managed = self
                .options
                .managed
                .as_ref()
                .ok_or(CommandFailure::UnsupportedCapability)?;
            if authorization.binding().fleet() != managed.authorization.fleet()
                || !authorization.matches_scope(&request.scope)
            {
                return Err(CommandFailure::CommandConflict);
            }
            let dormant = self.standbys.get(&request.scope).cloned();
            if dormant.as_ref().is_some_and(|entry| entry.failed) {
                return Err(CommandFailure::CommandConflict);
            }
            // Startup recovery installs active cells directly. Retrying an
            // initializer must observe their retained guard without requiring
            // a dormant entry or reinitializing the engine/SQL image.
            let (namespace, birth) = if let Some(dormant) = &dormant {
                (&dormant.namespace, &dormant.birth)
            } else {
                let cell = self
                    .cells
                    .get(&request.scope)
                    .ok_or(CommandFailure::UnsupportedCapability)?;
                let declared = self
                    .declared_namespaces
                    .iter()
                    .find(|entry| {
                        let config = entry.namespace.config();
                        config.cluster == request.scope.cluster
                            && config.namespace == request.scope.namespace
                    })
                    .ok_or(CommandFailure::UnsupportedCapability)?;
                (&declared.namespace, &cell.birth)
            };
            if namespace.config().managed_fleet.is_none()
                || !authorization.check_birth(namespace, birth)
            {
                return Err(CommandFailure::CommandConflict);
            }
            managed
                .authorization
                .check_namespace(namespace.config())
                .map_err(|_| CommandFailure::CommandConflict)?;
            let local = self.log.limits();
            let member = birth
                .voters()
                .iter()
                .find(|member| member.node == local.node)
                .ok_or(CommandFailure::CommandConflict)?
                .member;
            if self.activating.is_some() {
                return Err(CommandFailure::Overloaded);
            }
            if self.cells.contains_key(&request.scope) {
                return Ok((member, None));
            }
            let dormant = dormant.ok_or(CommandFailure::UnsupportedCapability)?;
            self.admit_new_history(&request.scope)?;
            // Every required genesis endpoint must be known before durable
            // initialization; an unavailable route cannot poison a new slot.
            if dormant
                .birth
                .voters()
                .iter()
                .any(|entry| !self.peers.contains_key(&entry.node))
            {
                return Err(CommandFailure::Unavailable);
            }
            let memory = self
                .control_budget
                .acquire(4 * 1024 * 1024)
                .ok_or(CommandFailure::Overloaded)?;
            Ok((
                member,
                Some((dormant, managed.authorization.clone(), memory)),
            ))
        })();
        let (member, prepared) = match setup {
            Ok(value) => value,
            Err(error) => {
                reply(&mut request, Err(error));
                return;
            }
        };
        let Some((dormant, host, memory)) = prepared else {
            let result = self
                .initial_receipt(&request.scope, member)
                .map(|status| Response::Fleet(Some(Box::new(status))));
            reply(&mut request, result);
            return;
        };
        if !request.control.begin_submission() {
            return;
        }
        self.activating = Some(request.scope.clone());
        let log = self.log.clone();
        let transport = self.transport.clone();
        self.jobs.push(Box::pin(async move {
            let _memory = memory;
            let deadline = request.control.deadline();
            let result = transport
                .seed_control_until(deadline, async {
                    let result = async {
                        let remaining = || -> anyhow::Result<Duration> {
                            let now = log.domain().mono_ms()?;
                            let ms = deadline
                                .checked_sub(now)
                                .filter(|ms| *ms > 0 && *ms <= 60_000)
                                .ok_or_else(|| {
                                    anyhow::anyhow!("initial member deadline expired")
                                })?;
                            Ok(Duration::from_millis(ms))
                        };
                        let current = host.refresh(remaining()?).await?;
                        current.check_namespace(dormant.namespace.config())?;
                        if !(current.node() == log.limits().node
                            && current.disk() == log.limits().disk
                            && current.instance_matches(log.storage_instance()?))
                        {
                            return Err(CatalogError::Conflict(
                                "initial member physical identity changed",
                            )
                            .into());
                        }
                        let fresh = current
                            .group_authorization(&request.scope, remaining()?)
                            .await?;
                        if fresh.binding() != authorization.binding()
                            || !fresh.check_birth(&dormant.namespace, &dormant.birth)
                        {
                            return Err(CatalogError::Conflict(
                                "initial history authorization changed",
                            )
                            .into());
                        }
                        Ok::<_, anyhow::Error>(
                            dormant
                                .namespace
                                .bootstrap_managed_member(&dormant.birth, &log, &fresh)
                                .await?,
                        )
                    }
                    .await;
                    Ok(result)
                })
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error) => Err(error.into()),
            };
            Job::Standby(Box::new(standby::Completed::Seeded {
                request: Box::new(request),
                member: standby::StartedMember::Initial(member),
                result,
            }))
        }));
    }

    pub(super) fn admit_new_history(&self, scope: &GroupScope) -> Result<(), CommandFailure> {
        match self.log.group(scope) {
            Ok(_) => Ok(()),
            Err(super::super::storage::LogError::NotFound) => self
                .options
                .node_profile
                .as_ref()
                .map_or(Ok(()), |profile| {
                    let groups = self
                        .log
                        .group_count()
                        .map_err(|_| CommandFailure::Unavailable)?;
                    let bytes = u64::try_from(groups)
                        .ok()
                        .and_then(|n| n.checked_add(1))
                        .and_then(|n| n.checked_mul(profile.bytes_per_group));
                    if profile.draining
                        || groups >= profile.group_capacity as usize
                        || !bytes.is_some_and(|bytes| bytes <= profile.byte_capacity)
                    {
                        Err(CommandFailure::Overloaded)
                    } else {
                        Ok(())
                    }
                }),
            Err(_) => Err(CommandFailure::Unavailable),
        }
    }
}

pub(super) fn failure(error: &anyhow::Error) -> (CommandFailure, bool) {
    if let Some(error) = error.downcast_ref::<CatalogError>() {
        return match error {
            CatalogError::Corrupt(_) => (CommandFailure::CorruptState, true),
            CatalogError::Conflict(_)
            | CatalogError::ManagedAdmissionRequired
            | CatalogError::LostStableState
            | CatalogError::Log(super::super::storage::LogError::WrongIdentity) => {
                (CommandFailure::CommandConflict, true)
            }
            _ => (CommandFailure::Unavailable, false),
        };
    }
    if let Some(error) = error.downcast_ref::<FleetCatalogError>() {
        return match error {
            FleetCatalogError::Corrupt(_) => (CommandFailure::CorruptState, true),
            FleetCatalogError::Conflict | FleetCatalogError::PayloadConflict => {
                (CommandFailure::CommandConflict, false)
            }
            FleetCatalogError::Overloaded | FleetCatalogError::Limit => {
                (CommandFailure::Overloaded, false)
            }
            _ => (CommandFailure::Unavailable, false),
        };
    }
    (CommandFailure::Unavailable, false)
}
