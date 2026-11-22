// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated-controller metadata codec, never a quorum or bearer proof.
use super::container_execution::ExecutionClaim;
use super::containers::{BindingCheckpoint, BindingRecord};
use anyhow::{ensure, Result};
use cellar_logic::{
    cell_group::{CellHistory, CellId},
    consensus::{ClusterId, IncarnationId, NamespaceId},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Wire {
    format: u8,
    cluster: [u8; 16],
    namespace: [u8; 16],
    cell: String,
    incarnation: [u8; 16],
    binding: String,
    key: [u8; 32],
    sequence: u64,
    owner: String,
    checkpoint: Checkpoint,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    revision: u64,
    activation_epoch: u64,
    record: BindingRecord,
}

impl From<ExecutionClaim> for Wire {
    fn from(claim: ExecutionClaim) -> Self {
        Self {
            format: 1,
            cluster: claim.history.cluster.bytes(),
            namespace: claim.history.namespace.bytes(),
            cell: claim.history.cell.as_str().into(),
            incarnation: claim.history.incarnation.bytes(),
            binding: claim.binding,
            key: claim.key,
            sequence: claim.claim.sequence,
            owner: claim.claim.owner,
            checkpoint: Checkpoint {
                revision: claim.checkpoint.revision,
                activation_epoch: claim.checkpoint.activation_epoch,
                record: claim.checkpoint.record,
            },
        }
    }
}

impl TryFrom<Wire> for ExecutionClaim {
    type Error = anyhow::Error;

    fn try_from(wire: Wire) -> Result<Self> {
        ensure!(wire.format == 1, "unsupported execution claim format");
        let history = CellHistory {
            cluster: ClusterId::new(wire.cluster)?,
            namespace: NamespaceId::new(wire.namespace)?,
            cell: CellId::new(&wire.cell)?,
            incarnation: IncarnationId::new(wire.incarnation)?,
        };
        crate::storage::container_bindings::validate_name(&wire.binding)?;
        wire.checkpoint.record.validate()?;
        ensure!(
            wire.checkpoint.revision > 0
                && wire.checkpoint.activation_epoch > 0
                && (1..=i64::MAX as u64).contains(&wire.sequence)
                && (wire.owner.len() == 64
                    && wire
                        .owner
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))),
            "invalid execution claim metadata"
        );
        let mut key = Sha256::new();
        key.update(b"cellar-provider-execution-resource-1");
        key.update(crate::raft::cell_groups::cell_image_history_digest(
            &history,
        ));
        key.update((wire.binding.len() as u16).to_be_bytes());
        key.update(wire.binding.as_bytes());
        let key: [u8; 32] = key.finalize().into();
        ensure!(key == wire.key, "execution claim history/key mismatch");
        Ok(Self {
            history,
            binding: wire.binding,
            key,
            claim: crate::storage::container_execution::Claim {
                sequence: wire.sequence,
                generation: wire.checkpoint.record.generation,
                binding_revision: wire.checkpoint.revision,
                owner: wire.owner,
            },
            checkpoint: BindingCheckpoint {
                revision: wire.checkpoint.revision,
                activation_epoch: wire.checkpoint.activation_epoch,
                record: wire.checkpoint.record,
            },
        })
    }
}
