//! A fresh safe ReadIndex precedes each seed capture. A cached configuration or
//! archive is never a substitute for this request's live-quorum authorization.

use super::*;
use crate::raft::query::ReadOperation;
use crate::raft::transport::seeds::SourceSeed;

pub(super) fn valid_target(
    cell: &Cell,
    target: &ReplicaIdentity,
    peers: &BTreeMap<NodeId, RaftPeerNode>,
) -> bool {
    if target.scope != cell.group.identity().scope || target.member == cell.group.identity().member
    {
        return false;
    }
    cell.group.read().is_ok_and(|cache| {
        cache.conf_state.learners.contains(&target.member.get())
            && !cache.conf_state.voters.contains(&target.member.get())
            && !cache
                .conf_state
                .voters_outgoing
                .contains(&target.member.get())
            && cache.membership.permits(target)
            && cache
                .membership
                .joined(target.member)
                .is_ok_and(|binding| binding.is_some())
            && peers
                .get(&target.node)
                .is_some_and(|peer| peer.disk == target.disk && peer.process == target.process)
    })
}

impl Driver {
    pub(super) fn next_read_id(&mut self) -> Option<ReadId> {
        self.read_sequence = self.read_sequence.checked_add(1)?;
        let mut id = [0; 32];
        id[..8].copy_from_slice(b"CLRRD001");
        id[8..24].copy_from_slice(&self.read_nonce);
        id[24..].copy_from_slice(&self.read_sequence.to_be_bytes());
        Some(id)
    }

    pub(super) fn start_seed(&mut self, mut request: Request, target: ReplicaIdentity) {
        if !self.options.enable_snapshots
            || self.cells.get(&request.scope).is_none_or(|cell| {
                cell.failed || cell.retired || !valid_target(cell, &target, &self.peers)
            })
        {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        // A fresh authenticated request supersedes an earlier transfer to the
        // same physical learner. Old stream owners retain their pins until
        // their bounded I/O exits; no unverified progress escapes either one.
        if self
            .snapshot_source
            .as_ref()
            .is_some_and(|source| source.targets(&request.scope, target.member))
        {
            if let Some(source) = self.snapshot_source.take() {
                source.cancel();
            }
        }
        let Some(id) = self.next_read_id() else {
            reply(&mut request, Err(CommandFailure::Overloaded));
            return;
        };
        self.reads.insert(id, PendingRead::seed(request, target));
    }

    pub(super) fn seed_sql(
        &mut self,
        scope: GroupScope,
        id: ReadId,
        result: Result<sql::Output, ExecutionError>,
    ) {
        if let Some(cell) = self.cells.get_mut(&scope) {
            cell.sql_busy = false;
        }
        let Some(mut read) = self.reads.remove(&id) else {
            return;
        };
        let ReadOperation::Seed { target } = &read.operation else {
            reply(&mut read.request, Err(CommandFailure::CorruptState));
            return;
        };
        let Some(Minimum::ReadIndex(barrier)) = read.minimum.clone() else {
            reply(&mut read.request, Err(CommandFailure::CorruptState));
            return;
        };
        if read.expired()
            || self
                .cells
                .get(&scope)
                .is_none_or(|cell| !valid_target(cell, target, &self.peers))
        {
            reply(&mut read.request, Err(CommandFailure::VersionUnavailable));
            return;
        }
        let output = match result {
            Ok(sql::Output {
                value:
                    Value::SnapshotSource {
                        package,
                        reservation,
                    },
                ..
            }) => {
                let snapshot = package.snapshot();
                if snapshot.get_metadata().index < barrier.get() {
                    Err(CommandFailure::VersionUnavailable)
                } else if package.encoded_length() == 0 {
                    drop(reservation);
                    Ok(SourceSeed {
                        snapshot,
                        barrier,
                        lease: 0,
                    })
                } else {
                    match self.transport.serve_snapshot(
                        &scope,
                        target.member.get(),
                        *package,
                        reservation,
                    ) {
                        Ok(source) => {
                            let lease = source.id();
                            self.snapshot_source = Some(source);
                            Ok(SourceSeed {
                                snapshot,
                                barrier,
                                lease,
                            })
                        }
                        Err(_) => Err(CommandFailure::Unavailable),
                    }
                }
            }
            Err(error) => Err(failure(error)),
            _ => Err(CommandFailure::CorruptState),
        };
        reply(
            &mut read.request,
            output.map(|seed| Response::Seed(Box::new(seed))),
        );
    }
}
