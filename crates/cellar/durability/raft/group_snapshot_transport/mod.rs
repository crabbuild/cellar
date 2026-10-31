// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete checkpoints on dedicated authenticated data connections. A stream
//! reply proves admission only; Raft emits its own response after persistence.
use super::*;
use crate::raft::cell_groups::{
    CellGroupApplier, CellGroupCheckpointError, CellGroupCheckpointPool,
};
use std::sync::Weak;
mod codec;
mod receive;
pub(crate) mod seed;
mod send;
mod workers;
pub(super) use codec::START_MAGIC;
use codec::*;
pub use seed::{CellGroupSeedError, CellGroupSeedSession};

pub(super) struct Endpoint {
    applier: CellGroupApplier,
    route: Arc<GroupRoute<GroupHistory>>,
    active: AtomicBool,
}

/// Retain with the group's controller registration. Dropping this handle revokes
/// new and in-flight streams; already admitted Raft work keeps its own owners.
pub struct CellGroupCheckpointRegistration {
    state: Weak<State>,
    endpoint: Arc<Endpoint>,
}

impl Drop for CellGroupCheckpointRegistration {
    fn drop(&mut self) {
        self.endpoint.active.store(false, Ordering::Release);
        let Some(state) = self.state.upgrade() else {
            return;
        };
        if let Ok(mut registry) = state.registry.lock() {
            let history = self.endpoint.applier.history();
            if registry
                .group_checkpoints
                .get(&history)
                .is_some_and(|old| old.ptr_eq(&Arc::downgrade(&self.endpoint)))
            {
                registry.group_checkpoints.remove(&history);
            }
        };
    }
}

impl RaftTransport {
    /// Configure once per node, before accepting snapshots. Share this same pool
    /// with local checkpoint publication. Scratch comes from the owning log.
    pub fn enable_group_checkpoints(
        &self,
        pool: CellGroupCheckpointPool,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        state.runtime.shared()?;
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if registry.group_checkpoint_pool.is_some() {
            return Err(TransportError::Identity);
        }
        registry.group_checkpoint_pool = Some(pool);
        Ok(())
    }

    pub(crate) fn group_checkpoint_pool(&self) -> Result<CellGroupCheckpointPool, TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry
            .group_checkpoint_pool
            .clone()
            .ok_or(TransportError::Refused)
    }

    pub fn register_group_checkpoint(
        &self,
        applier: CellGroupApplier,
    ) -> Result<CellGroupCheckpointRegistration, TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        if !applier.belongs_to_engine(&runtime.log().owner()) {
            return Err(TransportError::Identity);
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let history = applier.history();
        let route = registry
            .physical_groups
            .get(&history)
            .ok_or(TransportError::UnknownGroup)?
            .clone();
        if registry
            .group_checkpoints
            .get(&history)
            .and_then(Weak::upgrade)
            .is_some()
        {
            return Err(TransportError::Identity);
        }
        if registry.group_checkpoints.len() >= state.options.max_groups {
            return Err(TransportError::Overloaded);
        }
        let endpoint = Arc::new(Endpoint {
            applier,
            route,
            active: AtomicBool::new(true),
        });
        registry
            .group_checkpoints
            .insert(history, Arc::downgrade(&endpoint));
        Ok(CellGroupCheckpointRegistration {
            state: Arc::downgrade(state),
            endpoint,
        })
    }
}

impl State {
    fn checkpoint_endpoint(
        &self,
        history: &GroupHistory,
    ) -> Result<(Arc<Endpoint>, CellGroupCheckpointPool), TransportError> {
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let pool = registry
            .group_checkpoint_pool
            .clone()
            .ok_or(TransportError::Refused)?;
        let endpoint = registry
            .group_checkpoints
            .get(history)
            .and_then(Weak::upgrade)
            .ok_or(TransportError::Refused)?;
        if !endpoint.active.load(Ordering::Acquire)
            || !registry
                .physical_groups
                .get(history)
                .is_some_and(|route| Arc::ptr_eq(route, &endpoint.route))
        {
            return Err(TransportError::StalePeer);
        }
        Ok((endpoint, pool))
    }

    fn checkpoint_current(
        &self,
        endpoint: &Arc<Endpoint>,
        wire: &Route,
    ) -> Result<Arc<Pool>, TransportError> {
        let (_, target) = wire.decode_group()?;
        let (current, _) = self.checkpoint_endpoint(&target.scope)?;
        if !Arc::ptr_eq(&current, endpoint) {
            return Err(TransportError::StalePeer);
        }
        self.authorize_physical(wire)
    }
}

fn checkpoint_error(error: CellGroupCheckpointError) -> TransportError {
    match error {
        CellGroupCheckpointError::Busy => TransportError::Overloaded,
        CellGroupCheckpointError::Authority(error) => TransportError::Runtime(error),
        CellGroupCheckpointError::Limit => TransportError::Limit,
        other => TransportError::Invalid(other.to_string()),
    }
}

// Covers two in-flight chunk windows, the producer's copy, protobuf decoding,
// authentication/header copies and reply framing. Physical metadata and image
// workspaces are separately charged by the node-wide checkpoint pool.
fn wire_charge(options: &TransportOptions) -> usize {
    4 * CHUNK_BYTES + 4 * MESSAGE_BYTES + 4 * options.max_header_bytes
}

struct Admission {
    _node: Permit,
    _peer: Permit,
    _transfer: Permit,
}

pub(super) fn revoke(registry: &mut Registry, history: &GroupHistory) {
    if let Some(endpoint) = registry
        .group_checkpoints
        .remove(history)
        .and_then(|weak| weak.upgrade())
    {
        endpoint.active.store(false, Ordering::Release);
    }
}
