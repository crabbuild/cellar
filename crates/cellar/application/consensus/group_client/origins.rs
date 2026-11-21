// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit namespace origin grants after managed peers publish their boots.
use super::*;
use crate::raft::transport_options::{NamespaceCommandOrigin, NamespaceQueryOrigin};
use cellar_logic::consensus::{ClusterId, NamespaceId, NodeId};

impl RaftCells {
    /// Permit an explicitly selected managed node to originate namespace reads.
    /// The owned directory must already have accepted its current physical boot.
    /// Reapply host policy after restarting this transport. Subsequent directory
    /// refreshes fence superseded processes using the existing grant lifecycle.
    pub fn grant_managed_query_origin(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        node: NodeId,
        revision: u64,
    ) -> Result<(), CellDispatchError> {
        let peer = self.managed_origin_peer(cluster, namespace, node)?;
        self.grant_query_origin(NamespaceQueryOrigin {
            cluster,
            namespace,
            revision,
            peer,
        })
    }

    /// Permit an explicitly selected managed node to originate namespace writes.
    /// This is trusted host policy, independent of voting membership. The grant
    /// uses only the currently installed directory process and its endpoints;
    /// missing discovery refuses admission. Revisions retain revocation fences.
    pub fn grant_managed_command_origin(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        node: NodeId,
        revision: u64,
    ) -> Result<(), CellDispatchError> {
        let peer = self.managed_origin_peer(cluster, namespace, node)?;
        self.grant_command_origin(NamespaceCommandOrigin {
            cluster,
            namespace,
            revision,
            peer,
        })
    }

    fn managed_origin_peer(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        node: NodeId,
    ) -> Result<crate::raft::transport_options::RaftPeerNode, CellDispatchError> {
        self.check_open()?;
        self.handle
            .core
            .host
            .namespace_directory(cluster, namespace)?;
        if self.handle.core.managed.is_none() {
            return Err(CellDispatchError::Invalid(
                "origin requires managed discovery",
            ));
        }
        self.handle
            .core
            .transport
            .observed_peer(node)
            .map_err(CellDispatchError::Transport)?
            .ok_or(CellDispatchError::Runtime(
                crate::raft::completion::RuntimeError::NotReady,
            ))
    }

    /// Permit this exact peer process to originate reads for an owned
    /// namespace. The destination still proves the requested consistency.
    /// Managed discovery must verify the current boot and endpoints first.
    pub fn grant_query_origin(&self, grant: NamespaceQueryOrigin) -> Result<(), CellDispatchError> {
        self.check_open()?;
        self.handle
            .core
            .host
            .namespace_directory(grant.cluster, grant.namespace)?;
        self.handle
            .core
            .transport
            .grant_query_origin(grant)
            .map_err(CellDispatchError::Transport)
    }

    /// Permit this exact peer process to originate commands for an owned
    /// namespace. Managed discovery must already have verified its current boot
    /// and endpoints. Membership supplies no permission. Repeating the exact
    /// grant is idempotent; replacing or restoring one requires a newer revision.
    /// This is trusted host configuration, not a client-facing authorization API.
    pub fn grant_command_origin(
        &self,
        grant: NamespaceCommandOrigin,
    ) -> Result<(), CellDispatchError> {
        self.check_open()?;
        self.handle
            .core
            .host
            .namespace_directory(grant.cluster, grant.namespace)?;
        self.handle
            .core
            .transport
            .grant_command_origin(grant)
            .map_err(CellDispatchError::Transport)
    }

    /// Revoke namespace write permission while retaining its revision tombstone.
    /// Already admitted commands remain owned; revocation is not rollback proof.
    pub fn revoke_command_origin(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        node: NodeId,
        revision: u64,
    ) -> Result<(), CellDispatchError> {
        self.check_open()?;
        self.handle
            .core
            .host
            .namespace_directory(cluster, namespace)?;
        self.handle
            .core
            .transport
            .revoke_command_origin(cluster, namespace, node, revision)
            .map_err(CellDispatchError::Transport)
    }
}
