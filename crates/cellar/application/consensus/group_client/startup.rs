// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Initial voters use the controller's retained whole-group preparation owner.
//! Attachment receipts grant neither a fresh quorum nor application admission.
use super::*;
use crate::raft::{
    catalog::CatalogError,
    completion::RuntimeError,
    fleet_catalog::{FleetGroupAuthorization, FleetHostAuthorization},
    member_state::FleetMembershipStatus,
};
use cellar_logic::consensus::CommandFailure;

const STARTUP_BYTES: usize = 4 * 1024 * 1024;

/// A refusal submitted no preparation in this invocation. An unconfirmed result
/// can follow durable genesis or attachment; retry the same physical history.
#[derive(Clone, Debug)]
pub enum CellGroupStartupError {
    Refused(CellGroupControllerError),
    Unconfirmed(CellGroupControllerError),
}

impl std::fmt::Display for CellGroupStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(f, "group startup refused: {error}"),
            Self::Unconfirmed(error) => write!(f, "group startup unconfirmed: {error}"),
        }
    }
}

impl std::error::Error for CellGroupStartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Refused(error) | Self::Unconfirmed(error) => error,
        })
    }
}

pub(super) fn admission_error(error: CommandFailure) -> CellGroupControllerError {
    match error {
        CommandFailure::Overloaded => RuntimeError::Overloaded.into(),
        CommandFailure::VersionUnavailable => RuntimeError::DeadlineExceeded.into(),
        CommandFailure::UnsupportedCapability => {
            CellGroupControllerError::Invalid("group startup callback context")
        }
        _ => CellGroupControllerError::Closed,
    }
}

pub(super) fn catalog_error(error: CatalogError) -> CellGroupControllerError {
    CellGroupControllerError::Catalog(Arc::new(error))
}

fn startup_directory_error(error: CellDispatchError) -> CellGroupControllerError {
    match error {
        CellDispatchError::Runtime(error) => error.into(),
        _ => CellGroupControllerError::Invalid("group startup namespace capability"),
    }
}

impl RaftCells {
    /// Start the exact operator-provisioned first group on this initial voter.
    /// Verify the rooted archive, attach or validate the retained local replica,
    /// and wait for a fresh quorum behind one startup deadline. Call this from
    /// daemon readiness before opening application ingress. The result is a
    /// point-in-time barrier and supplies no ongoing serving lease.
    pub fn start_managed_first_group(
        &self,
        history: GroupHistory,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CellGroupStartupError>> + Send + 'static {
        use CellGroupStartupError::{Refused, Unconfirmed};
        let admitted = (|| {
            let (deadline, memory) = self
                .metadata_admission(timeout, STARTUP_BYTES)
                .map_err(admission_error)?;
            let core = &self.handle.core;
            let host = core
                .managed
                .clone()
                .ok_or(CellGroupControllerError::Invalid(
                    "managed group startup owner",
                ))?;
            let archives = core
                .archive_io
                .clone()
                .ok_or(CellGroupControllerError::Invalid(
                    "managed first group archive pool",
                ))?;
            let directory = core
                .host
                .directory(history)
                .map_err(startup_directory_error)?;
            Ok((deadline, memory, host, directory, archives))
        })();
        let owner = self.internal_observer();
        async move {
            let (deadline, memory, host, directory, archives) = admitted.map_err(Refused)?;
            owner.startup_live(&host, deadline).map_err(Refused)?;
            let remaining = owner.startup_remaining(deadline).map_err(Refused)?;
            let authorization = owner
                .handle
                .domain
                .run(host.verify_first_group_archive(directory, archives, history, remaining))
                .await
                .map_err(|_| Refused(CellGroupControllerError::Closed))?
                .map_err(|error| Refused(catalog_error(CatalogError::from(error))))?;
            drop(memory);
            let remaining = owner.startup_remaining(deadline).map_err(Refused)?;
            let attachment = owner
                .activate_managed_initial_group(authorization, remaining)
                .await?;
            let core = &owner.handle.core;
            let observed = core
                .runtime
                .quorum_membership(history, deadline)
                .await
                .map_err(|error| Unconfirmed(error.into()))?;
            let remaining = owner.startup_remaining(deadline).map_err(Unconfirmed)?;
            let fresh = owner
                .handle
                .domain
                .run(host.group_authorization(&history, remaining))
                .await
                .map_err(|_| Unconfirmed(CellGroupControllerError::Closed))?
                .map_err(|error| Unconfirmed(catalog_error(CatalogError::from(error))))?;
            let local = observed.replica();
            if fresh.binding() != &attachment.binding
                || observed.engine() != core.engine
                || local.node_identity() != core.node
                || !observed
                    .configuration()
                    .voters
                    .contains(&local.member.get())
                || !fresh.matches_replica(local, observed.membership())
                || fresh.history_digests()
                    != (
                        observed.root().route().birth_digest,
                        observed.root().route().schema_digest,
                    )
            {
                return Err(Unconfirmed(CellGroupControllerError::Invalid(
                    "managed first group quorum membership",
                )));
            }
            owner.startup_live(&host, deadline).map_err(Unconfirmed)
        }
    }

    /// Recover or initialize this exact admitted initial voter. Namespace lookup
    /// allocates no cell/group identity; the rooted directory selects its birth.
    /// Success describes a durable local attachment, never quorum readiness.
    pub fn activate_managed_initial_group(
        &self,
        authorization: FleetGroupAuthorization,
        timeout: Duration,
    ) -> impl Future<Output = Result<FleetMembershipStatus, CellGroupStartupError>> + Send + 'static
    {
        let admitted = (|| {
            let (deadline, memory) = self
                .metadata_admission(timeout, STARTUP_BYTES)
                .map_err(admission_error)?;
            let host =
                self.handle
                    .core
                    .managed
                    .clone()
                    .ok_or(CellGroupControllerError::Invalid(
                        "managed group startup owner",
                    ))?;
            let history = authorization
                .binding()
                .scope()
                .map_err(|e| catalog_error(CatalogError::Log(e)))?;
            if !authorization.allows_managed_genesis()
                || authorization.binding().fleet() != host.fleet()
            {
                return Err(CellGroupControllerError::Invalid(
                    "managed genesis authority",
                ));
            }
            let directory = self
                .handle
                .core
                .host
                .directory(history)
                .map_err(startup_directory_error)?;
            Ok((deadline, memory, host, history, directory))
        })();
        let owner = self.internal_observer();
        async move {
            use CellGroupStartupError::{Refused, Unconfirmed};
            let (deadline, _memory, host, history, directory) = admitted.map_err(Refused)?;
            let mut submitted = false;
            let work = async {
                owner.startup_live(&host, deadline).map_err(Refused)?;
                let boot = host
                    .boot_binding()
                    .map_err(CatalogError::from)
                    .map_err(catalog_error)
                    .map_err(Refused)?;
                let namespace_owner = directory
                    .namespace()
                    .cell_group_owner()
                    .await
                    .map_err(catalog_error)
                    .map_err(Refused)?;
                if namespace_owner
                    .is_none_or(|o| o.fleet != host.fleet() || o.catalog != boot.catalog)
                {
                    return Err(Refused(catalog_error(
                        CatalogError::ManagedAdmissionRequired,
                    )));
                }
                let birth = directory
                    .materialize_selected_history(history)
                    .await
                    .map_err(catalog_error)
                    .map_err(Refused)?;
                if !authorization.check_birth(&directory, &birth)
                    || !birth
                        .voters()
                        .iter()
                        .any(|m| m.node == owner.handle.core.node.node)
                {
                    return Err(Refused(CellGroupControllerError::Invalid(
                        "initial group voter birth",
                    )));
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
                if fresh.binding() != authorization.binding()
                    || !fresh.check_birth(&directory, &birth)
                    || !fresh.allows_managed_genesis()
                {
                    return Err(Refused(CellGroupControllerError::Invalid(
                        "initial group authorization changed",
                    )));
                }
                owner.startup_live(&host, deadline).map_err(Refused)?;
                let call = owner
                    .handle
                    .core
                    .controller
                    .provision_managed_initial(
                        directory.clone(),
                        birth.route().clone(),
                        host.clone(),
                        deadline,
                    )
                    .map_err(Refused)?;
                submitted = true;
                call.response()
                    .await
                    .map_err(|error| Unconfirmed((*error).clone()))?;
                owner.startup_live(&host, deadline).map_err(Unconfirmed)?;
                // Reauthorize after attachment. The saved admission and a successful
                // controller reply cannot substitute for this current root check.
                let fresh = host
                    .group_authorization(
                        &history,
                        owner.startup_remaining(deadline).map_err(Unconfirmed)?,
                    )
                    .await
                    .map_err(CatalogError::from)
                    .map_err(catalog_error)
                    .map_err(Unconfirmed)?;
                if fresh.binding() != authorization.binding()
                    || !fresh.check_birth(&directory, &birth)
                    || !fresh.allows_managed_genesis()
                {
                    return Err(Unconfirmed(CellGroupControllerError::Invalid(
                        "initial group authorization changed",
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
                let group = core
                    .log
                    .cell_group(&history)
                    .map_err(|e| Unconfirmed(RuntimeError::from(e).into()))?;
                let marker = group
                    .group_manifest()
                    .map_err(|e| Unconfirmed(RuntimeError::from(e).into()))?
                    .ok_or(Unconfirmed(CellGroupControllerError::Invalid(
                        "missing attached group root",
                    )))?;
                if marker.route() != birth.route() || !marker.is_active() {
                    return Err(Unconfirmed(CellGroupControllerError::Invalid(
                        "initial group root",
                    )));
                }
                let status = {
                    let cache = group
                        .read()
                        .map_err(|e| Unconfirmed(RuntimeError::from(e).into()))?;
                    if !crate::raft::member_state::members(&cache.conf_state)
                        .contains(&group.identity().member.get())
                        || !fresh.matches_replica(group.identity(), &cache.membership)
                    {
                        return Err(Unconfirmed(CellGroupControllerError::Invalid(
                            "initial group membership",
                        )));
                    }
                    cache.membership.fleet_status().ok_or(Unconfirmed(
                        CellGroupControllerError::Invalid("missing initial fleet guard"),
                    ))?
                };
                if &status.binding != authorization.binding() {
                    return Err(Unconfirmed(CellGroupControllerError::Invalid(
                        "initial fleet binding",
                    )));
                }
                owner.startup_live(&host, deadline).map_err(Unconfirmed)?;
                Ok(status)
            };
            match owner
                .handle
                .core
                .transport
                .seed_control_until(deadline, async {
                    // Public callers may poll on another runtime. Catalog
                    // deadlines and I/O must still enter this installed owner.
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

    pub(super) fn startup_remaining(
        &self,
        deadline: u64,
    ) -> Result<Duration, CellGroupControllerError> {
        let now = self
            .handle
            .domain
            .mono_ms()
            .map_err(|_| CellGroupControllerError::Closed)?;
        let remaining = deadline
            .checked_sub(now)
            .filter(|ms| *ms > 0 && *ms <= 60_000)
            .ok_or(CellGroupControllerError::Runtime(
                RuntimeError::DeadlineExceeded,
            ))?;
        Ok(Duration::from_millis(remaining))
    }

    pub(super) fn startup_live(
        &self,
        host: &FleetHostAuthorization,
        deadline: u64,
    ) -> Result<(), CellGroupControllerError> {
        self.metadata_live(deadline).map_err(admission_error)?;
        let boot = host
            .boot_binding()
            .map_err(CatalogError::from)
            .map_err(catalog_error)?;
        let core = &self.handle.core;
        if boot.node != core.node.node.bytes()
            || boot.disk != core.node.disk.bytes()
            || boot.process != core.node.process.bytes()
            || boot.engine != core.engine.bytes()
            || core
                .log
                .lifecycle_guard()
                .map_err(RuntimeError::from)?
                .is_none_or(|guard| !guard.admits_group_start(&boot))
        {
            return Err(catalog_error(CatalogError::Fenced));
        }
        Ok(())
    }
}
