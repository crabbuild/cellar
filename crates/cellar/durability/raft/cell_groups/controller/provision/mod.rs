// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One preparation per physical group, owned and joined by the node controller.
mod config;
mod learner;
mod manager;
mod prepare;
mod transfer;
pub(super) use config::Config;
pub use config::{CellGroupProvisionLimits, CellGroupProvisionNamespace, CellGroupProvisioning};
pub(super) use manager::Manager;

use super::{calls::Delivery, *};
use crate::raft::{budget::Permit, catalog::CatalogError, cell_groups::RangeDirectory};
use cellar_logic::cell_group::GroupRoute;
use tokio::sync::oneshot;

// Logical routes have fixed-width bounded keys. Directory handles share their
// separately bounded cache; no tree or cell list is copied into a request.
const WAITER_BYTES: usize = 4096;

fn apply_error(error: ExecutionError) -> CellGroupControllerError {
    CellGroupControllerError::Apply(Arc::new(error))
}

fn catalog_error(error: CatalogError) -> CellGroupControllerError {
    CellGroupControllerError::Catalog(Arc::new(error))
}

/// Response loss never cancels physical recovery or attachment. Success means
/// locally attached, not elected, caught up, or authorized to serve a request.
pub struct CellGroupProvisionCall {
    receiver: oneshot::Receiver<Delivery<(), CellGroupControllerError>>,
    deadline: Option<(crate::asyncrt::DomainToken, u64)>,
}

impl CellGroupProvisionCall {
    pub async fn response(self) -> Result<(), Arc<CellGroupControllerError>> {
        let delivery = self
            .receiver
            .await
            .map_err(|_| Arc::new(CellGroupControllerError::Closed))?;
        delivery.result?;
        check_delivery_deadline(&self.deadline).map_err(Arc::new)
    }
}

pub(super) struct Waiter {
    reply: oneshot::Sender<Delivery<(), CellGroupControllerError>>,
    permit: Arc<Permit>,
    deadline: Option<(crate::asyncrt::DomainToken, u64)>,
}

impl Waiter {
    fn finish(self, result: Result<(), CellGroupControllerError>) {
        let result = result.and_then(|()| check_delivery_deadline(&self.deadline));
        let _ = self.reply.send(Delivery {
            result: result.map_err(Arc::new),
            _permit: self.permit,
        });
    }
}

fn check_delivery_deadline(
    deadline: &Option<(crate::asyncrt::DomainToken, u64)>,
) -> Result<(), CellGroupControllerError> {
    if let Some((domain, deadline)) = deadline {
        if domain
            .mono_ms()
            .map_err(|_| CellGroupControllerError::Closed)?
            >= *deadline
        {
            return Err(RuntimeError::DeadlineExceeded.into());
        }
    }
    Ok(())
}

pub(super) struct Request {
    directory: RangeDirectory,
    route: GroupRoute,
    config: Arc<Config>,
    authority: Authority,
    waiter: Waiter,
}

#[derive(Clone)]
enum Authority {
    Initial,
    Managed(Box<ManagedAuthority>),
}

#[derive(Clone)]
struct ManagedAuthority {
    learner: Option<learner::Admission>,
    host: crate::raft::fleet_catalog::FleetHostAuthorization,
    deadline: u64,
}

impl Authority {
    fn managed(&self) -> Option<&ManagedAuthority> {
        match self {
            Self::Initial => None,
            Self::Managed(authority) => Some(authority),
        }
    }

    fn same_owner(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Initial, Self::Initial) => true,
            (Self::Managed(a), Self::Managed(b)) => {
                a.host.same_boot_owner(&b.host)
                    && learner::same_admission(a.learner.as_ref(), b.learner.as_ref())
            }
            _ => false,
        }
    }

    fn check_live(&self, runtime: &CellGroupRuntime) -> Result<(), CellGroupControllerError> {
        if let Some(ManagedAuthority { host, deadline, .. }) = self.managed() {
            let now = runtime
                .log()
                .domain()
                .mono_ms()
                .map_err(|_| CellGroupControllerError::Closed)?;
            let remaining = deadline
                .checked_sub(now)
                .filter(|ms| *ms > 0)
                .ok_or(RuntimeError::DeadlineExceeded)?;
            if remaining > 60_000 {
                return Err(CellGroupControllerError::Invalid(
                    "group startup deadline bound",
                ));
            }
            let boot = host
                .boot_binding()
                .map_err(CatalogError::from)
                .map_err(catalog_error)?;
            let log = runtime.log();
            if boot.node != log.limits().node.bytes()
                || boot.disk != log.limits().disk.bytes()
                || boot.process != log.limits().process.bytes()
                || boot.engine
                    != log
                        .storage_instance()
                        .map_err(|e| catalog_error(CatalogError::Log(e)))?
                        .bytes()
                || log
                    .lifecycle_guard()
                    .map_err(|e| catalog_error(CatalogError::Log(e)))?
                    .is_none_or(|guard| !guard.admits_group_start(&boot))
            {
                return Err(catalog_error(CatalogError::Fenced));
            }
        }
        Ok(())
    }
}

impl CellGroupController {
    /// Install immutable node resources once. Reconfiguration requires a joined
    /// restart so pending recovery cannot change filesystem or image owners.
    pub fn configure_provisioning(
        &self,
        settings: CellGroupProvisioning,
    ) -> Result<(), CellGroupControllerError> {
        self.configure_provisioning_with_archives(settings, None)
    }

    pub(crate) fn configure_provisioning_with_archives(
        &self,
        settings: CellGroupProvisioning,
        archives: Option<archive::Provisioning>,
    ) -> Result<(), CellGroupControllerError> {
        let shared = &self.handle.0;
        let config = Arc::new(Config::new(settings, shared, archives)?);
        let mut slot = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
        if slot.is_some() {
            return Err(CellGroupControllerError::Invalid(
                "provisioning already configured",
            ));
        }
        *slot = Some(config);
        Ok(())
    }

    /// Materialize and recover an already selected initial-member assignment.
    /// Concurrent requests coalesce only for the same directory owner and exact
    /// route. This never allocates a range or a Raft group for an individual cell.
    pub fn provision(
        &self,
        directory: RangeDirectory,
        route: GroupRoute,
    ) -> Result<CellGroupProvisionCall, CellGroupControllerError> {
        self.provision_authorized(directory, route, Authority::Initial)
    }

    /// Initial members require their exact managed boot and fresh rooted birth.
    /// The caller's deadline controls admission and delivery; accepted physical
    /// work remains owned and joined after response loss or deadline expiry.
    pub(crate) fn provision_managed_initial(
        &self,
        directory: RangeDirectory,
        route: GroupRoute,
        host: crate::raft::fleet_catalog::FleetHostAuthorization,
        deadline: u64,
    ) -> Result<CellGroupProvisionCall, CellGroupControllerError> {
        self.provision_authorized(
            directory,
            route,
            Authority::Managed(Box::new(ManagedAuthority {
                host,
                deadline,
                learner: None,
            })),
        )
    }

    /// A learner must retain the exact repair admission and complete authenticated
    /// seed. This path never creates genesis from a placement reservation.
    pub(crate) fn provision_managed_learner(
        &self,
        directory: RangeDirectory,
        route: GroupRoute,
        host: crate::raft::fleet_catalog::FleetHostAuthorization,
        authorization: crate::raft::fleet_catalog::FleetPermitAuthorization,
        activation: crate::application::consensus::client::LearnerActivation,
        deadline: u64,
    ) -> Result<CellGroupProvisionCall, CellGroupControllerError> {
        if activation.from_s3 {
            let configured = self
                .handle
                .0
                .provisioning
                .lock()
                .map_err(|_| CellGroupControllerError::Closed)?;
            if configured.as_ref().is_none_or(|c| c.archives.is_none()) {
                return Err(CellGroupControllerError::Invalid(
                    "S3 learner resources unavailable",
                ));
            }
        }
        let learner = learner::Admission::new(&route, &host, authorization, activation)?;
        self.provision_authorized(
            directory,
            route,
            Authority::Managed(Box::new(ManagedAuthority {
                host,
                deadline,
                learner: Some(learner),
            })),
        )
    }

    fn provision_authorized(
        &self,
        directory: RangeDirectory,
        route: GroupRoute,
        authority: Authority,
    ) -> Result<CellGroupProvisionCall, CellGroupControllerError> {
        let shared = &self.handle.0;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
        authority.check_live(&shared.runtime)?;
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "provisioning not configured",
            ))?;
        let permit = config
            .waiters
            .acquire(WAITER_BYTES)
            .ok_or(RuntimeError::Overloaded)?;
        let (reply, receiver) = oneshot::channel();
        let deadline = match &authority {
            Authority::Initial => None,
            Authority::Managed(authority) => Some((shared.domain.clone(), authority.deadline)),
        };
        shared
            .requests
            .try_send(requests::Request::Provision(Box::new(Request {
                directory,
                route,
                config,
                authority,
                waiter: Waiter {
                    reply,
                    permit: Arc::new(permit),
                    deadline: deadline.clone(),
                },
            })))
            .map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    CellGroupControllerError::Closed
                }
                tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded.into(),
            })?;
        Ok(CellGroupProvisionCall { receiver, deadline })
    }
}
