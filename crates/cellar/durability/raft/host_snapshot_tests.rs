// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn ticket() -> ApplyTicket {
    ApplyTicket {
        owner: Arc::new(()),
        engine_owner: Arc::new(()),
        replica: ReplicaIdentity {
            scope: GroupScope {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace: NamespaceId::new([2; 16]).unwrap(),
                cell: "Counter:snapshot-order".into(),
                group: GroupId::new([3; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
            },
            node: NodeId::new([5; 16]).unwrap(),
            member: MemberId::new(1).unwrap(),
            disk: DiskLineage::new([6; 16]).unwrap(),
            process: ProcessGeneration::new([7; 16]).unwrap(),
        },
        operation: OperationId::new(1),
        position: LogPosition {
            index: LogIndex::new(4),
            term: Term::new(2),
        },
        digest: [8; 32],
    }
}

fn incoming(phase: Phase) -> Incoming {
    Incoming {
        ticket: ticket(),
        phase,
        received: None,
        completion: None,
        retry_at: 0,
        next: None,
    }
}

fn event(budget: &Arc<Budget>, ticket: ApplyTicket, source: u64) -> RuntimeEvent {
    let mut snapshot = ::raft::prelude::Snapshot::default();
    snapshot.mut_metadata().index = ticket.position().index.get();
    snapshot.mut_metadata().term = ticket.position().term.get();
    RuntimeEvent {
        kind: RuntimeEventKind::Snapshot {
            ticket,
            snapshot,
            source,
        },
        _permit: Arc::new(budget.acquire(4096).unwrap()),
    }
}

#[test]
fn next_snapshot_retains_exact_ticket_and_admission_until_publication_owner_releases_it() {
    for phase in [Phase::Publish, Phase::Publishing] {
        let budget = Budget::new(8192, 2);
        let mut incoming = incoming(phase);
        let mut next = incoming.ticket.clone();
        next.operation = OperationId::new(2);
        next.position.index = LogIndex::new(5);
        next.digest = [9; 32];
        assert!(incoming.defer(event(&budget, next.clone(), 3)));
        assert_eq!(budget.usage(), (4096, 1));

        // Refusing another event must neither overwrite the admitted ticket
        // nor refund its retained event bytes prematurely.
        let mut later = next.clone();
        later.operation = OperationId::new(3);
        later.position.index = LogIndex::new(6);
        assert!(!incoming.defer(event(&budget, later, 2)));
        assert_eq!(budget.usage(), (4096, 1));
        let deferred = incoming.next.as_ref().unwrap();
        let RuntimeEventKind::Snapshot {
            ticket,
            snapshot,
            source,
        } = &deferred.kind
        else {
            panic!("deferred snapshot changed kind")
        };
        assert_eq!(ticket, &next);
        assert_eq!(snapshot.get_metadata().index, 5);
        assert_eq!(*source, 3);

        let deferred = incoming.next.take().unwrap();
        drop(incoming);
        assert_eq!(budget.usage(), (4096, 1));
        drop(deferred);
        assert_eq!(budget.usage(), (0, 0));
    }
}

#[test]
fn overlapping_snapshot_before_durable_install_remains_refused() {
    for phase in [
        Phase::Fetching,
        Phase::Downloaded,
        Phase::Preparing,
        Phase::Prepared,
        Phase::Authorizing,
        Phase::Installing,
        Phase::NeedAbort,
        Phase::Aborting,
    ] {
        let budget = Budget::new(4096, 1);
        let mut incoming = incoming(phase);
        assert!(!incoming.defer(event(&budget, ticket(), 3)));
        assert!(incoming.next.is_none());
        assert_eq!(budget.usage(), (0, 0));
    }
}
