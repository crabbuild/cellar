//! Committed business-feature adapters. Immutable values carry their actual
//! bounded bytes in the command transaction and in the archive dependency set.
//! Alarm callbacks use private replicated commands and an owned host scheduler.
//! Outbox delivery remains separate integration work.

#[path = "effect_alarm.rs"]
pub(crate) mod alarm;
#[path = "effect_blob.rs"]
mod blob;
pub use alarm::{RaftAlarmLimits, RaftAlarmOptions};
pub(crate) use blob::{capture_dependencies, get_blob, put_blob};

/// One stable private command client per history, independent of node boot and
/// leadership. Public commands cannot consume it in a class with system work.
pub(crate) fn system_client(
    scope: &cellar_logic::consensus::GroupScope,
) -> Result<cellar_logic::consensus::ClientId, cellar_logic::consensus::CommandFailure> {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-system-client-1");
    for identity in [
        scope.cluster.bytes(),
        scope.namespace.bytes(),
        scope.group.bytes(),
        scope.incarnation.bytes(),
    ] {
        hash.update(identity);
    }
    hash.update((scope.cell.len() as u64).to_be_bytes());
    hash.update(scope.cell.as_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    cellar_logic::consensus::ClientId::new(id)
        .map_err(|_| cellar_logic::consensus::CommandFailure::CorruptState)
}
pub use blob::{RaftBlobLimits, RaftBlobReference};
