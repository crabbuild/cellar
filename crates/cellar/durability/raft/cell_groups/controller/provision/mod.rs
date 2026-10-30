// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One preparation per physical group, owned and joined by the node controller.
mod config;
mod manager;
mod prepare;
pub use config::CellGroupProvisioning;
pub(super) use config::Config;
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
}

impl CellGroupProvisionCall {
    pub async fn response(self) -> Result<(), Arc<CellGroupControllerError>> {
        self.receiver
            .await
            .map_err(|_| Arc::new(CellGroupControllerError::Closed))?
            .result
    }
}

struct Waiter {
    reply: oneshot::Sender<Delivery<(), CellGroupControllerError>>,
    permit: Arc<Permit>,
}

impl Waiter {
    fn finish(self, result: Result<(), CellGroupControllerError>) {
        let _ = self.reply.send(Delivery {
            result: result.map_err(Arc::new),
            _permit: self.permit,
        });
    }
}

pub(super) struct Request {
    directory: RangeDirectory,
    route: GroupRoute,
    config: Arc<Config>,
    waiter: Waiter,
}

impl CellGroupController {
    /// Install immutable node resources once. Reconfiguration requires a joined
    /// restart so pending recovery cannot change filesystem or image owners.
    pub fn configure_provisioning(
        &self,
        settings: CellGroupProvisioning,
    ) -> Result<(), CellGroupControllerError> {
        let shared = &self.handle.0;
        let config = Arc::new(Config::new(settings, shared)?);
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
        let shared = &self.handle.0;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
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
        shared
            .requests
            .try_send(requests::Request::Provision(Box::new(Request {
                directory,
                route,
                config,
                waiter: Waiter {
                    reply,
                    permit: Arc::new(permit),
                },
            })))
            .map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    CellGroupControllerError::Closed
                }
                tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded.into(),
            })?;
        Ok(CellGroupProvisionCall { receiver })
    }
}
