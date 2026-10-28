// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{
    ClusterId, DiskLineage, GroupId, IncarnationId, LogIndex, LogPosition, MemberId, NamespaceId,
    NodeId, OperationId, ProcessGeneration, ReplicaIdentity, Term,
};

fn scope(id: u8) -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: format!("Counter:{id}"),
        group: GroupId::new([id; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

fn request(
    id: u8,
    archive: bool,
    slots: &Arc<Semaphore>,
    performance: &Arc<super::super::performance::Registry>,
) -> Request {
    let (reply, _receive) = oneshot::channel();
    Request {
        queued: performance.timer(super::super::performance::Operation::SqlQueue),
        scope: scope(id),
        action: if archive {
            Action::CaptureArchive {
                limits: ArchiveLimits::default(),
            }
        } else {
            Action::ScanEffects {
                now: 0,
                prefer_delivery: false,
                reservation: Budget::new(1, 1).acquire(0).unwrap(),
            }
        },
        reply,
        _work: None,
        cancel: None,
        _slot: slots.clone().try_acquire_owned().unwrap(),
    }
}

#[tokio::test]
async fn archive_capture_advances_past_unrelated_queued_sql_work() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let performance = domain
        .run(async {
            super::super::performance::Registry::new(
                crate::asyncrt::services().domain_token().unwrap(),
            )
        })
        .await
        .unwrap();
    let slots = Arc::new(Semaphore::new(4));
    let (sender, receiver) = mpsc::sync_channel(4);
    sender
        .send(request(1, false, &slots, &performance))
        .unwrap();
    sender
        .send(request(2, false, &slots, &performance))
        .unwrap();
    sender.send(request(3, true, &slots, &performance)).unwrap();
    let mut queue = RequestQueue::new(receiver, 4);
    assert_eq!(
        queue.next().unwrap().scope,
        scope(3),
        "archive progress waited behind unrelated queued SQL work"
    );
    assert_eq!(queue.next().unwrap().scope, scope(1));
    assert_eq!(queue.next().unwrap().scope, scope(2));
    domain.shutdown().await;
}

#[tokio::test]
async fn committed_publication_advances_without_releasing_its_reservation() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let performance = domain
        .run(async {
            super::super::performance::Registry::new(
                crate::asyncrt::services().domain_token().unwrap(),
            )
        })
        .await
        .unwrap();
    let slots = Arc::new(Semaphore::new(2));
    let (sender, receiver) = mpsc::sync_channel(2);
    sender
        .send(request(1, false, &slots, &performance))
        .unwrap();
    let mut publish = request(2, true, &slots, &performance);
    publish.action = Action::Publish {
        ticket: ApplyTicket {
            owner: Arc::new(()),
            engine_owner: Arc::new(()),
            replica: ReplicaIdentity {
                scope: scope(2),
                node: NodeId::new([4; 16]).unwrap(),
                member: MemberId::new(1).unwrap(),
                disk: DiskLineage::new([5; 16]).unwrap(),
                process: ProcessGeneration::new([6; 16]).unwrap(),
            },
            operation: OperationId::new(1),
            position: LogPosition {
                term: Term::new(1),
                index: LogIndex::new(2),
            },
            digest: [7; 32],
        },
    };
    sender.send(publish).unwrap();
    let mut queue = RequestQueue::new(receiver, 2);
    let selected = queue.next().unwrap();
    assert_eq!(selected.scope, scope(2));
    assert_eq!(slots.available_permits(), 0);
    assert!(slots.clone().try_acquire_owned().is_err());
    drop(selected);
    assert_eq!(slots.available_permits(), 1);
    drop(queue);
    assert_eq!(slots.available_permits(), 2);
    domain.shutdown().await;
}

#[tokio::test]
async fn progress_never_passes_an_earlier_request_for_the_same_cell() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let performance = domain
        .run(async {
            super::super::performance::Registry::new(
                crate::asyncrt::services().domain_token().unwrap(),
            )
        })
        .await
        .unwrap();
    let slots = Arc::new(Semaphore::new(3));
    let (sender, receiver) = mpsc::sync_channel(3);
    sender
        .send(request(1, false, &slots, &performance))
        .unwrap();
    sender.send(request(1, true, &slots, &performance)).unwrap();
    sender.send(request(2, true, &slots, &performance)).unwrap();
    let mut queue = RequestQueue::new(receiver, 3);
    assert_eq!(queue.next().unwrap().scope, scope(2));
    let ordinary = queue.next().unwrap();
    assert_eq!(ordinary.scope, scope(1));
    assert!(matches!(ordinary.action, Action::ScanEffects { .. }));
    let capture = queue.next().unwrap();
    assert_eq!(capture.scope, scope(1));
    assert!(matches!(capture.action, Action::CaptureArchive { .. }));
    domain.shutdown().await;
}

#[tokio::test]
async fn continuous_progress_cannot_starve_older_ordinary_work() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let performance = domain
        .run(async {
            super::super::performance::Registry::new(
                crate::asyncrt::services().domain_token().unwrap(),
            )
        })
        .await
        .unwrap();
    let slots = Arc::new(Semaphore::new(3));
    let (sender, receiver) = mpsc::sync_channel(3);
    sender
        .send(request(1, false, &slots, &performance))
        .unwrap();
    sender.send(request(2, true, &slots, &performance)).unwrap();
    let mut queue = RequestQueue::new(receiver, 3);
    for id in 2..=4 {
        assert_eq!(queue.next().unwrap().scope, scope(id));
        sender
            .send(request(id + 1, true, &slots, &performance))
            .unwrap();
    }
    assert_eq!(queue.next().unwrap().scope, scope(1));
    assert_eq!(queue.next().unwrap().scope, scope(5));
    domain.shutdown().await;
}

#[tokio::test]
async fn disconnected_sender_does_not_discard_already_queued_work() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let performance = domain
        .run(async {
            super::super::performance::Registry::new(
                crate::asyncrt::services().domain_token().unwrap(),
            )
        })
        .await
        .unwrap();
    let slots = Arc::new(Semaphore::new(3));
    let (sender, receiver) = mpsc::sync_channel(3);
    for id in 1..=3 {
        sender
            .send(request(id, false, &slots, &performance))
            .unwrap();
    }
    drop(sender);
    let mut queue = RequestQueue::new(receiver, 3);
    for id in 1..=3 {
        assert_eq!(queue.next().unwrap().scope, scope(id));
    }
    assert!(matches!(
        queue.next(),
        Err(mpsc::RecvTimeoutError::Disconnected)
    ));
    assert_eq!(slots.available_permits(), 3);
    domain.shutdown().await;
}
