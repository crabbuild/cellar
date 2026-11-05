// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit namespace origin grants after managed peers publish their boots.
use super::*;
use crate::raft::transport_options::{NamespaceCommandOrigin, NamespaceQueryOrigin};
use cellar_logic::consensus::{ClusterId, NamespaceId, NodeId};

impl RaftCells {
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
