//! Lazy metadata registration is serialized with activation and shutdown.
//! Neither a rooted history descriptor nor registration grants membership.
use super::*;
use crate::raft::fleet_catalog::{FleetCatalogError, FleetGroupAuthorization, FleetNodeState};
use std::time::Duration;

const METADATA_RESERVATION: usize = 4 * 1024 * 1024;

pub(super) struct Completed {
    request: NodeRequest,
    prepared: Result<Prepared, CommandFailure>,
    _memory: Permit,
}

struct Prepared {
    scope: GroupScope,
    namespace: NamespaceCatalog,
    birth: GroupBirth,
    class: Arc<RaftClass>,
}

fn catalog_failure(error: FleetCatalogError) -> CommandFailure {
    match error {
        FleetCatalogError::Conflict | FleetCatalogError::PayloadConflict => {
            CommandFailure::CommandConflict
        }
        FleetCatalogError::Limit | FleetCatalogError::Overloaded => CommandFailure::Overloaded,
        FleetCatalogError::Corrupt(_) => CommandFailure::CorruptState,
        _ => CommandFailure::Unavailable,
    }
}

impl Driver {
    pub(super) fn start_history_registration(
        &mut self,
        mut request: NodeRequest,
        authorization: FleetGroupAuthorization,
    ) {
        let setup = (|| {
            if self.history_registration.is_some() {
                return Err(CommandFailure::Overloaded);
            }
            let managed = self
                .options
                .managed
                .as_ref()
                .ok_or(CommandFailure::UnsupportedCapability)?;
            let scope = authorization
                .binding()
                .scope()
                .map_err(|_| CommandFailure::CommandConflict)?;
            if authorization.binding().fleet() != managed.authorization.fleet()
                || scope.cluster != managed.authorization.cluster()
            {
                return Err(CommandFailure::CommandConflict);
            }
            let declared = self
                .declared_namespaces
                .iter()
                .find(|n| {
                    n.namespace.config().namespace == scope.namespace
                        && n.namespace.config().cluster == scope.cluster
                        && scope
                            .cell
                            .split_once(':')
                            .is_some_and(|(name, _)| name == n.class.name())
                })
                .ok_or(CommandFailure::UnsupportedCapability)?;
            let memory = self
                .control_budget
                .acquire(METADATA_RESERVATION)
                .ok_or(CommandFailure::Overloaded)?;
            Ok((
                managed.authorization.clone(),
                scope,
                declared.namespace.clone(),
                declared.class.clone(),
                memory,
            ))
        })();
        let (host, scope, namespace, class, memory) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                reply_node(&mut request, Err(error));
                return;
            }
        };
        let domain = self.log.domain();
        self.history_registration = Some(request.control.clone());
        self.jobs.push(Box::pin(async move {
            let result = async {
                let remaining = || -> Result<Duration, CommandFailure> {
                    let now = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
                    request.control.deadline().checked_sub(now).filter(|ms| *ms > 0 && *ms <= 60_000)
                        .map(Duration::from_millis).ok_or(CommandFailure::Unavailable)
                };
                let current = host.refresh(remaining()?).await.map_err(catalog_failure)?;
                current.check_namespace(namespace.config()).map_err(catalog_failure)?;
                let fresh = current.group_authorization(&scope, remaining()?).await.map_err(catalog_failure)?;
                if fresh.binding() != authorization.binding() { return Err(CommandFailure::CommandConflict); }
                let birth = namespace.birth(&scope.cell).await.map_err(|error| match error {
                    CatalogError::BirthCancelled | CatalogError::Conflict(_) => CommandFailure::CommandConflict,
                    CatalogError::Corrupt(_) => CommandFailure::CorruptState,
                    _ => CommandFailure::Unavailable,
                })?.ok_or(CommandFailure::CommandConflict)?;
                if birth.scope() != &scope || !fresh.check_birth(&namespace, &birth) {
                    return Err(CommandFailure::CommandConflict);
                }
                // The root admitted this original-voter reference before
                // target exclusion. Settling it grants no new placement:
                // reserve_birth orders new intent against that same root CAS.
                let admitted_initial = birth.managed_owner().is_some()
                    && fresh.allows_managed_genesis()
                    && birth.voters().iter().any(|member| member.node == current.node());
                let settling = matches!(current.state(),
                    FleetNodeState::Draining | FleetNodeState::RestartPending | FleetNodeState::ResizePending);
                let admitted_learner = if settling && !admitted_initial {
                    current.admitted_learner(&scope, remaining()?).await.map_err(catalog_failure)?;
                    true
                } else { false };
                if !matches!(current.state(), FleetNodeState::Registered | FleetNodeState::Ready | FleetNodeState::Active)
                    && !(settling && (admitted_initial || admitted_learner))
                {
                    return Err(CommandFailure::Unavailable);
                }
                Ok(Prepared { scope, namespace, birth, class })
            };
            let prepared = crate::asyncrt::select_biased! { "registration cancellation precedes metadata publication";
                _=request.control.cancellation()=>Err(CommandFailure::Unavailable),
                prepared=result=>prepared,
            };
            Job::HistoryRegistered(Box::new(Completed { request, prepared, _memory: memory }))
        }));
    }

    pub(super) fn finish_history_registration(&mut self, mut completed: Completed) {
        self.history_registration = None;
        let result = (|| {
            if self.closing.load(Ordering::Acquire)
                || self.local.admission.is_draining()
                || completed.request.control.cancelled()
            {
                return Err(CommandFailure::Unavailable);
            }
            let prepared = completed.prepared?;
            let scope = &prepared.scope;
            if self.local.bindings.contains_scope(scope)? {
                if let Some(dormant) = self.standbys.get(scope) {
                    if dormant.failed {
                        return Err(CommandFailure::CommandConflict);
                    }
                } else if !self.cells.contains_key(scope) {
                    return Err(CommandFailure::CommandConflict);
                }
                if let Some(cell) = self.cells.get(scope) {
                    if cell.retired || cell.failed {
                        return Err(CommandFailure::CommandConflict);
                    }
                }
                match self.log.group(scope) {
                    Ok(group) => {
                        let cache = group.read().map_err(|_| CommandFailure::CorruptState)?;
                        if !cache.membership.permits(group.identity())
                            || !super::super::member_state::members(&cache.conf_state)
                                .contains(&group.identity().member.get())
                        {
                            return Err(CommandFailure::CommandConflict);
                        }
                    }
                    Err(super::super::storage::LogError::NotFound) => (),
                    Err(_) => return Err(CommandFailure::Unavailable),
                }
                return Ok(Response::HistoryRegistered);
            }
            // An occupied or retired engine slot can never be reset by adding
            // another metadata binding, even when aggregate capacity is free.
            match self.log.group(scope) {
                Err(super::super::storage::LogError::NotFound) => (),
                Ok(_) => return Err(CommandFailure::CommandConflict),
                Err(_) => return Err(CommandFailure::Unavailable),
            }
            if prepared.birth.managed_owner().is_none()
                && prepared
                    .birth
                    .voters()
                    .iter()
                    .chain(prepared.birth.learners())
                    .any(|m| m.node == self.log.limits().node)
            {
                return Err(CommandFailure::CommandConflict);
            }
            let mut occupied = self.local.bindings.len()?;
            for group in self.log.groups().map_err(|_| CommandFailure::Unavailable)? {
                if !self
                    .local
                    .bindings
                    .contains_scope(&group.identity().scope)?
                {
                    occupied = occupied.checked_add(1).ok_or(CommandFailure::Overloaded)?;
                }
            }
            if occupied >= self.options.max_groups {
                return Err(CommandFailure::Overloaded);
            }
            let worker = self.local.bindings.len()? % self.options.blocking_workers;
            let storage = sql::CellStorage {
                directory: self
                    .directory_path
                    .join(hex::encode(scope.namespace.bytes()))
                    .join(hex::encode(scope.group.bytes()))
                    .join(hex::encode(scope.incarnation.bytes())),
                host: ltx_host(),
                services: read_services(),
                images: self.options.images,
                rooted: None,
                snapshot_scratch: self.log.snapshot_scratch(),
                execution: self.options.execution,
            };
            self.local.bindings.insert(Binding {
                scope: scope.clone(),
                class: prepared.class.clone(),
                minimum: prepared.namespace.minimum_durability(),
                member: client::LocalMember::new(None),
                leader_hint: None,
            })?;
            if self.effect_scheduler.is_none()
                && (prepared.class.alarm_limits().is_some()
                    || prepared.class.outbox_definition().is_some())
            {
                self.effect_scheduler = self.options.effects.map(effect_scheduler::Scheduler::new);
            }
            self.standbys.insert(
                prepared.scope,
                standby::Dormant {
                    namespace: prepared.namespace,
                    birth: prepared.birth,
                    class: prepared.class,
                    storage,
                    worker,
                    failed: false,
                },
            );
            Ok(Response::HistoryRegistered)
        })();
        reply_node(&mut completed.request, result);
    }
}
