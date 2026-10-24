// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact mailbox boundaries retain and release every owned reservation.
use super::{channel, Budget, Event};
use tokio::sync::mpsc::error::TrySendError;

#[test]
fn mailbox_boundaries_release_every_reservation() {
    let (sender, mut receiver) = channel(2);
    let budget = Budget::new(3);
    let event = || Event::Complete {
        queued_at: std::time::Instant::now(),
        payload: "x".into(),
        body: None,
        failed: false,
        reservation: budget.reserve(1).unwrap(),
    };
    assert!(sender.try_send(event()).is_ok());
    assert!(sender.try_send(event()).is_ok());
    assert!(matches!(
        sender.try_send(event()),
        Err(TrySendError::Full(_))
    ));
    assert_eq!(budget.report().queued_bytes, 2);
    assert_eq!(budget.report().pending_reservations, 2);
    drop(receiver.try_recv().expect("accepted event missing"));
    assert!(sender.try_send(event()).is_ok());
    drop(receiver);
    assert_eq!(budget.report().queued_bytes, 0);
    assert_eq!(budget.report().pending_reservations, 0);
    assert!(matches!(
        sender.try_send(event()),
        Err(TrySendError::Closed(_))
    ));
    assert_eq!(budget.report().queued_bytes, 0);
    assert_eq!(budget.report().pending_reservations, 0);
}
