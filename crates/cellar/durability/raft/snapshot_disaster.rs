// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replay a pinned old history into a fresh initial configuration. Old votes,
//! membership changes, Raft terms and version authority never cross this boundary.
use super::*;

pub(crate) struct DisasterSeed {
    pub(crate) permit: crate::raft::catalog::DisasterSeedPermit,
    pub(crate) set: crate::raft::archive::ArchiveRecoverySet,
    pub(crate) schema: [u8; 32],
    pub(crate) limits: SnapshotLimits,
    pub(crate) reservation: std::sync::Arc<crate::raft::budget::Permit>,
}

impl DisasterSeed {
    pub(crate) fn scope(&self) -> &GroupScope {
        &self.permit.local.scope
    }

    pub(crate) fn finish(
        self,
        images: &GenerationStore,
        services: cellar_ltx::generation::ReadServices,
    ) -> Result<PreparedSeed, SnapshotError> {
        self.limits.validate()?;
        self.permit
            .validate_set(&self.set)
            .map_err(|_| SnapshotError::Invalid("disaster archive authority"))?;
        if self.set.schema_digest() != self.schema {
            return Err(SnapshotError::Invalid("disaster archive schema"));
        }
        let mut hash = Sha256::new();
        hash.update(b"cellar-disaster-image-1");
        hash.update(self.permit.birth.digest());
        hash.update(self.permit.local.node.bytes());
        hash.update(self.permit.local.disk.bytes());
        hash.update(self.permit.local.process.bytes());
        let digest: [u8; 32] = hash.finalize().into();
        let nonce = cellar_ltx::generation::ImageId::new(
            digest[..16]
                .try_into()
                .map_err(|_| SnapshotError::Invalid("disaster nonce"))?,
        )?;
        let pin = self.set.restore(images, nonce)?;
        // An old delivery may have reached its destination after the selected
        // archive cut. Never rewrite its origin or send it from a new history.
        // Reconciliation is deliberately a separate operator/application step.
        {
            let database = crate::storage::open_immutable_generation(
                pin.clone(),
                crate::storage::ImmutableQueryOptions {
                    services,
                    sqlite_vec: false,
                },
            )
            .map_err(|_| SnapshotError::Invalid("disaster SQLite image"))?;
            require_reconciled_outbox(database.connection())?;
        }
        let birth = &self.permit.birth;
        let mut initial = ::raft::prelude::ConfState::default();
        initial.set_voters(birth.voters().iter().map(|m| m.member.get()).collect());
        initial.set_learners(birth.learners().iter().map(|m| m.member.get()).collect());
        let bytes = encode_image(&pin, self.limits, stream_snapshot::IoProfile::Compact)?;
        let descriptor = Descriptor {
            format: 1,
            scope: WireScope::from(birth.scope()),
            birth: birth.digest(),
            schema: self.schema,
            // Index 1 is reserved for empty genesis. The certified restore is
            // the new history's initial materialized checkpoint at index 2.
            index: 2,
            term: 1,
            conf: manifest::encode_conf(&initial)
                .map_err(|_| SnapshotError::Invalid("disaster configuration"))?,
            membership: crate::raft::member_state::GroupMembership::initial(&initial),
            image: Some(marker(&pin)),
            encoded_length: bytes.metadata().encoded_length as usize,
            encoded_digest: bytes.metadata().encoded_digest,
            transfer_length: bytes.metadata().encoded_length as usize,
        };
        Descriptor::decode_bound(
            birth.scope(),
            birth.digest(),
            &descriptor.membership,
            self.schema,
            &descriptor.snapshot()?,
            self.limits,
        )?;
        Ok(PreparedSeed {
            grant: SeedPermit::Disaster(self.permit),
            descriptor,
            initial,
            pin: Some(SnapshotPin::Materialized(pin)),
            _reservation: self.reservation,
        })
    }
}

fn require_reconciled_outbox(connection: &rusqlite::Connection) -> Result<(), SnapshotError> {
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_outbox')", [], |row| row.get(0))
        .map_err(|_| SnapshotError::Invalid("disaster outbox schema"))?;
    if exists {
        let pending: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM __sys_raft_outbox)",
                [],
                |row| row.get(0),
            )
            .map_err(|_| SnapshotError::Invalid("disaster outbox state"))?;
        if pending {
            return Err(SnapshotError::Invalid(
                "disaster outbox requires reconciliation",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::disallowed_methods)]
    fn pending_old_origin_delivery_refuses_admission_without_rewriting_it() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        require_reconciled_outbox(&connection).unwrap();
        connection.execute_batch("CREATE TABLE __sys_raft_outbox(request BLOB); INSERT INTO __sys_raft_outbox VALUES(X'010203');").unwrap();
        assert!(matches!(
            require_reconciled_outbox(&connection),
            Err(SnapshotError::Invalid(
                "disaster outbox requires reconciliation"
            ))
        ));
        let bytes: Vec<u8> = connection
            .query_row("SELECT request FROM __sys_raft_outbox", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(bytes, [1, 2, 3]);
        connection
            .execute("DELETE FROM __sys_raft_outbox", [])
            .unwrap();
        require_reconciled_outbox(&connection).unwrap();
    }
}
