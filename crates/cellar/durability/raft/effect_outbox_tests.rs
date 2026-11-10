// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{cell_group::*, consensus::*};
use futures_util::future::BoxFuture;

struct Unavailable;

impl RaftEffectSink for Unavailable {
    fn deliver(
        &self,
        _: RaftDeliveryRequest,
    ) -> BoxFuture<'static, Result<Vec<u8>, CommandFailure>> {
        Box::pin(async { Err(CommandFailure::Unavailable) })
    }
}

struct Fixture {
    connection: Connection,
    old: CellHistory,
    current: CellHistory,
    outbox: RaftOutbox,
    schema: [u8; 32],
}

impl Fixture {
    fn new() -> Result<Self> {
        let old = CellHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: CellId::new("Counter:one")?,
            incarnation: IncarnationId::new([3; 16])?,
        };
        let current = CellHistory {
            incarnation: IncarnationId::new([4; 16])?,
            ..old.clone()
        };
        let signer = Arc::new(RaftEffectSigningKey::new([5; 32])?);
        let target = RaftEffectTarget::new(
            old.clone(),
            [6; 32],
            crate::application::consensus::OperationContract {
                operation: "Receive".into(),
                kind: crate::application::consensus::OperationKind::Command,
                digest: [7; 32],
            },
            signer.verification_key(),
            Durability::S3,
        )?;
        let outbox = RaftOutbox::new(signer, Default::default())?
            .binding("Events", [8; 32], target.clone(), Arc::new(Unavailable))?
            .binding("Fresh", [9; 32], target, Arc::new(Unavailable))?;
        let connection = Connection::open_in_memory()?;
        install(&connection)?;
        Ok(Self {
            connection,
            old,
            current,
            outbox,
            schema: [10; 32],
        })
    }

    fn pending(&self, lane: &str, scope: &CellHistory, next_at: u64) -> Result<Vec<u8>> {
        let binding = &self.outbox.bindings[lane];
        let request = RaftDeliveryRequest::new(
            DeliveryOrigin {
                scope,
                schema: self.schema,
                binding: lane,
                digest: binding.digest,
                sequence: 1,
            },
            b"uncertain",
            binding.target.clone(),
            &self.outbox.signer,
        )?;
        self.connection.execute(
            "INSERT INTO __sys_raft_outbox_lanes VALUES(?1,?2,?3)",
            params![
                lane,
                1u64.to_be_bytes().as_slice(),
                0u64.to_be_bytes().as_slice()
            ],
        )?;
        self.connection.execute(
            "INSERT INTO __sys_raft_outbox VALUES(?1,?2,?3,1,?4,?5)",
            params![
                lane,
                1u64.to_be_bytes().as_slice(),
                request.encoded(),
                [11u8; 32].as_slice(),
                next_at
            ],
        )?;
        Ok(request.encoded().to_vec())
    }
}

#[test]
fn uncertain_old_origin_does_not_starve_fresh_lane() -> Result<()> {
    let f = Fixture::new()?;
    let original = f.pending("Events", &f.old, 0)?;
    f.pending("Fresh", &f.current, 1)?;
    let selected = due(&f.connection, &f.outbox, &f.current, f.schema, 2)?
        .expect("fresh lane must remain runnable");
    assert_eq!(selected.0.lane, "Fresh");
    let retained = load(&f.connection, "Events", 1)?.expect("uncertain intent must remain");
    assert_eq!(retained.bytes, original);
    assert_eq!(retained.generation, [11; 32]);
    Ok(())
}

#[test]
fn old_origin_status_is_explicit_and_cannot_reissue() -> Result<()> {
    let f = Fixture::new()?;
    let original = f.pending("Events", &f.old, 0)?;
    let blocked = progress(&f.connection, &f.outbox, &f.current, f.schema, "Events")?;
    assert_eq!((blocked.emitted, blocked.settled), (1, 0));
    assert!(blocked.requires_reconciliation);
    let old = progress(&f.connection, &f.outbox, &f.old, f.schema, "Events")?;
    assert!(!old.requires_reconciliation);
    let failure = f
        .outbox
        .request(&f.current, f.schema, "Events", 1, &original)
        .unwrap_err();
    assert!(matches!(
        failure.downcast_ref::<CommandFailure>(),
        Some(CommandFailure::WrongIncarnation)
    ));
    assert!(due(&f.connection, &f.outbox, &f.current, f.schema, 2)?.is_none());
    let retained = load(&f.connection, "Events", 1)?.unwrap();
    assert_eq!(retained.bytes, original);
    assert_eq!(retained.generation, [11; 32]);
    assert_eq!(retained.next_at, 0);
    Ok(())
}

#[test]
fn corrupted_or_changed_requests_cannot_be_hidden_as_old_origins() -> Result<()> {
    let mut f = Fixture::new()?;
    let original = f.pending("Events", &f.old, 0)?;
    f.pending("Fresh", &f.current, 1)?;
    let wrong = CellHistory {
        cell: CellId::new("Counter:foreign")?,
        ..f.current.clone()
    };
    let failure = f
        .outbox
        .request(&wrong, f.schema, "Events", 1, &original)
        .unwrap_err();
    assert!(matches!(
        failure.downcast_ref::<CommandFailure>(),
        Some(CommandFailure::CorruptState)
    ));
    assert!(progress(&f.connection, &f.outbox, &f.current, [12; 32], "Events").is_err());
    f.outbox.bindings.get_mut("Events").unwrap().digest = [13; 32];
    assert!(progress(&f.connection, &f.outbox, &f.current, f.schema, "Events").is_err());
    assert!(due(&f.connection, &f.outbox, &f.current, f.schema, 2).is_err());
    f.outbox.bindings.get_mut("Events").unwrap().digest = [8; 32];
    let mut signed: serde_json::Value = serde_json::from_slice(&original)?;
    signed["signature"] = serde_json::Value::String("A".repeat(86));
    let corrupt = serde_json::to_vec(&signed)?;
    f.connection.execute(
        "UPDATE __sys_raft_outbox SET request=?1 WHERE lane='Events'",
        [corrupt],
    )?;
    assert!(progress(&f.connection, &f.outbox, &f.current, f.schema, "Events").is_err());
    assert!(due(&f.connection, &f.outbox, &f.current, f.schema, 2).is_err());
    Ok(())
}

#[test]
fn completed_lane_needs_no_reconciliation_in_a_fresh_history() -> Result<()> {
    let f = Fixture::new()?;
    f.connection.execute(
        "INSERT INTO __sys_raft_outbox_lanes VALUES('Events',?1,?1)",
        [1u64.to_be_bytes().as_slice()],
    )?;
    let completed = progress(&f.connection, &f.outbox, &f.current, f.schema, "Events")?;
    assert_eq!((completed.emitted, completed.settled), (1, 1));
    assert!(!completed.requires_reconciliation);
    assert!(due(&f.connection, &f.outbox, &f.current, f.schema, 2)?.is_none());
    Ok(())
}
