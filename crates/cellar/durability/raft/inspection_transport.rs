// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Admission and error classification for peer inspection.
use super::*;
use crate::raft::inspection::InspectionError;
use crate::raft::transport_options::NamespaceInspectionOrigin;
use std::sync::atomic::AtomicU8;

pub(super) const SLOTS: usize = 16;
pub(super) const META_BYTES: usize = 1024 * 1024;

#[cfg(test)]
#[path = "inspection_connect_tests.rs"]
mod connect_tests;

pub(super) async fn dial<T>(
    domain: &crate::asyncrt::DomainToken,
    deadline: u64,
    stage: &AtomicU8,
    connect: impl Future<Output = std::io::Result<T>>,
) -> Result<T, TransportError> {
    let now = domain.mono_ms().map_err(|_| TransportError::Closed)?;
    let remaining = deadline
        .checked_sub(now)
        .filter(|ms| *ms >= 2)
        .ok_or(TransportError::Timeout)?;
    // A dial that consumes the outer deadline cannot report Unreachable: the
    // host has already cancelled its waiter. Reserve half the remaining call
    // budget for classification and delivery. Local expiry before an attempt
    // keeps stage zero, and TLS/metadata retain stage two and the full deadline.
    let timer = domain
        .sleep_until(now + remaining / 2)
        .map_err(|_| TransportError::Closed)?;
    stage.store(1, Ordering::Release);
    let socket = crate::asyncrt::select_biased! { "the connection phase expires before a late socket is accepted";
        _ = timer => Err(TransportError::Timeout),
        result = connect => result.map_err(TransportError::from),
    }?;
    stage.store(2, Ordering::Release);
    Ok(socket)
}

pub(crate) struct Delivery<T> {
    pub(super) reply: Option<oneshot::Receiver<Result<T, InspectionError>>>,
    pub(super) cancel: watch::Sender<bool>,
}

impl<T> Delivery<T> {
    pub(crate) async fn receive(mut self) -> Result<T, InspectionError> {
        self.reply
            .take()
            .ok_or(InspectionError::Unavailable)?
            .await
            .map_err(|_| InspectionError::Unavailable)?
    }
}

impl<T> Drop for Delivery<T> {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

pub(super) fn classify(error: TransportError, stage: u8) -> InspectionError {
    match error {
        TransportError::Overloaded => InspectionError::Overloaded,
        TransportError::Refused | TransportError::UnknownGroup | TransportError::Identity => {
            InspectionError::Refused
        }
        TransportError::StalePeer => InspectionError::StalePeer,
        TransportError::Io(_) | TransportError::Timeout if stage == 1 => {
            InspectionError::Unreachable
        }
        _ => InspectionError::Unavailable,
    }
}

impl Registry {
    pub(super) fn inspection_peer_budget(
        &mut self,
        node: NodeId,
        limit: usize,
    ) -> Result<Arc<Budget>, TransportError> {
        if let Some(budget) = self.inspection_peers.get(&node) {
            return Ok(budget.clone());
        }
        if self.inspection_peers.len() >= limit {
            return Err(TransportError::Overloaded);
        }
        let budget = Budget::new(META_BYTES * 2, 2);
        self.inspection_peers.insert(node, budget.clone());
        Ok(budget)
    }
}

impl RaftTransport {
    pub fn grant_inspection_origin(
        &self,
        grant: NamespaceInspectionOrigin,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        grant.peer.validate_endpoint()?;
        if grant.revision == 0 || grant.peer.node == state.runtime.log().limits().node {
            return Err(TransportError::Identity);
        }
        let key = (grant.cluster, grant.namespace, grant.peer.node);
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry.check_node_view(&grant.peer)?;
        registry.check_physical_endpoint(&grant.peer)?;
        if let Some(old) = registry.inspection_origins.get(&key) {
            if old.revision == grant.revision && old.peer.as_ref() == Some(&grant.peer) {
                return Ok(());
            }
            if old.revision >= grant.revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.inspection_origins.len() >= state.options.max_query_origins {
            return Err(TransportError::Overloaded);
        }
        if registry
            .groups
            .values()
            .chain(registry.query_groups.values())
            .flat_map(|route| &route.peers)
            .any(|peer| peer.replica.node == grant.peer.node && !grant.peer.same_host(peer))
            || registry
                .origins
                .values()
                .filter_map(|old| old.peer.as_ref())
                .chain(
                    registry
                        .inspection_origins
                        .iter()
                        .filter(|(other, _)| **other != key)
                        .filter_map(|(_, old)| old.peer.as_ref()),
                )
                .any(|peer| peer.node == grant.peer.node && peer != &grant.peer)
        {
            return Err(TransportError::Identity);
        }
        let (stop, _) = watch::channel(false);
        if let Some(old) = registry.inspection_origins.insert(
            key,
            Arc::new(queries::OriginGrant {
                revision: grant.revision,
                peer: Some(grant.peer),
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        if !state.inspection_enabled.swap(true, Ordering::AcqRel) {
            state.control_connections.add_permits(SLOTS);
        }
        drop(registry);
        Ok(())
    }

    pub fn revoke_inspection_origin(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        node: NodeId,
        revision: u64,
    ) -> Result<(), TransportError> {
        if revision == 0 {
            return Err(TransportError::Identity);
        }
        let state = &self.handle.0;
        let key = (cluster, namespace, node);
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if let Some(old) = registry.inspection_origins.get(&key) {
            if old.revision == revision && old.peer.is_none() {
                return Ok(());
            }
            if old.revision >= revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.inspection_origins.len() >= state.options.max_query_origins {
            return Err(TransportError::Overloaded);
        }
        let (stop, _) = watch::channel(false);
        if let Some(old) = registry.inspection_origins.insert(
            key,
            Arc::new(queries::OriginGrant {
                revision,
                peer: None,
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        Ok(())
    }
}
