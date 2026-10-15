//! Enumerate core decisions across delayed replies and stale cleanup.
//! These tests do not simulate SQLite commits or object-store durability.
use cellar_logic::event_stream::{self, Acknowledgment, Failure, Lease, Receipt};
use cellar_logic::wake::{self, AlarmSnapshot, AlarmSource, PublicationId, Retirement, WakeCore};
use std::collections::BTreeSet;

fn permutations<const N: usize>(
    mut order: [usize; N],
    mut check: impl FnMut(&[usize; N]),
) -> usize {
    fn visit<const N: usize>(
        order: &mut [usize; N],
        index: usize,
        check: &mut impl FnMut(&[usize; N]),
        count: &mut usize,
    ) {
        if index == N {
            check(order);
            *count += 1;
            return;
        }
        for next in index..N {
            order.swap(index, next);
            visit(order, index + 1, check, count);
            order.swap(index, next);
        }
    }
    let mut count = 0;
    visit(&mut order, 0, &mut check, &mut count);
    count
}

#[test]
fn late_wake_confirmations_and_old_owner_cleanup_preserve_the_successor() {
    let cell = "Order:a";
    let snapshots = [
        AlarmSnapshot::committed(
            Some(50),
            AlarmSource {
                id: PublicationId {
                    epoch: 1,
                    sequence: 1,
                },
                position: 1,
            },
        ),
        AlarmSnapshot::committed(
            Some(50),
            AlarmSource {
                id: PublicationId {
                    epoch: 1,
                    sequence: 2,
                },
                position: 2,
            },
        ),
        AlarmSnapshot::committed(
            Some(50),
            AlarmSource {
                id: PublicationId {
                    epoch: 2,
                    sequence: 3,
                },
                position: 3,
            },
        ),
    ];
    let keys: Vec<_> = snapshots
        .iter()
        .map(|s| wake::entry_key(50, cell, s.source().unwrap().id))
        .collect();
    let count = permutations([0, 1, 2, 3, 4, 5], |order| {
        let mut core = WakeCore::new();
        // All PUTs are already durable. Their callbacks can arrive later than
        // ownership replacement, retirement, or a cache forget/recovery.
        let mut persisted: BTreeSet<_> = keys.iter().cloned().collect();
        for &step in order {
            match step {
                0..=2 => core.confirm(cell, snapshots[step]),
                3 | 4 => {
                    let certificate = Retirement {
                        id: snapshots[1].source().unwrap().id,
                        at_ms: if step == 3 { Some(50) } else { None },
                    };
                    let eligible: BTreeSet<_> = if step == 3 {
                        [keys[0].clone()].into_iter().collect()
                    } else {
                        [keys[0].clone(), keys[1].clone()].into_iter().collect()
                    };
                    for key in core.take_retired(cell, certificate) {
                        assert!(eligible.contains(&key), "unsafe cleanup in {order:?}");
                        persisted.remove(&key);
                    }
                }
                5 => core.forget(cell),
                _ => unreachable!(),
            }
            assert!(persisted.contains(&keys[2]), "successor lost in {order:?}");
            // Cache loss may require republishing, never a different identity.
            if let Some(publication) = core.publication(cell, snapshots[2]) {
                assert_eq!(publication.key, keys[2]);
            }
        }
        core.confirm(cell, snapshots[2]);
        assert!(core.covered(cell, snapshots[2]));
    });
    assert_eq!(count, 720);
}

#[test]
fn lease_settlement_expiry_extension_and_retention_orders_never_skip_progress() {
    let receipt = Receipt {
        subscription: [1; 16],
        incarnation: [2; 16],
        generation: 1,
        token: [3; 32],
        first: 1,
        last: 2,
    };
    let count = permutations([0, 1, 2, 3, 4, 5], |order| {
        let mut checkpoint = 0;
        let mut lease = Some(Lease {
            receipt: receipt.clone(),
            issued_at_ms: 0,
            expires_at_ms: 10,
        });
        let mut last_ack = None;
        let mut now = 0;
        let mut incarnation = [2; 16];
        let mut retained = BTreeSet::from([1, 2]);
        let mut advanced = false;
        for &step in order {
            match step {
                0 | 1 => {
                    let before = checkpoint;
                    let outcome = event_stream::acknowledge(
                        [1; 16],
                        incarnation,
                        checkpoint,
                        lease.as_ref(),
                        last_ack.as_ref(),
                        &receipt,
                        now,
                    );
                    // Independent decision table for the one issued receipt.
                    if incarnation != [2; 16] {
                        assert_eq!(outcome, Err(Failure::SubscriptionReset));
                    } else if advanced {
                        assert_eq!(outcome, Ok(Acknowledgment::AlreadyAcknowledged));
                    } else if now >= lease.as_ref().unwrap().expires_at_ms {
                        assert_eq!(outcome, Err(Failure::LeaseExpired));
                    } else {
                        assert_eq!(outcome, Ok(Acknowledgment::Advance { through: 2 }));
                        checkpoint = 2;
                        advanced = true;
                        last_ack = Some(receipt.clone());
                        lease = None;
                    }
                    if !matches!(outcome, Ok(Acknowledgment::Advance { .. })) {
                        assert_eq!(checkpoint, before);
                    }
                }
                2 => now = 10,
                3 => {
                    if let Some(current) = &mut lease {
                        let outcome = event_stream::extend(current, &receipt, now, 20, 30);
                        if now >= current.expires_at_ms {
                            assert_eq!(outcome, Err(Failure::LeaseExpired));
                        } else {
                            assert_eq!(outcome, Ok(20));
                            current.expires_at_ms = outcome.unwrap();
                        }
                    }
                }
                4 => {
                    let starts = lease
                        .as_ref()
                        .filter(|l| l.expires_at_ms > now)
                        .map(|l| l.receipt.first);
                    let frontier =
                        event_stream::deletion_frontier(2, [checkpoint], starts).unwrap();
                    assert!(frontier <= checkpoint);
                    retained.retain(|offset| *offset > frontier);
                }
                // A committed incarnation reset invalidates the old receipt.
                // This models the post-commit identity, not reset persistence.
                5 => {
                    incarnation = [4; 16];
                    lease = None;
                    last_ack = None;
                }
                _ => unreachable!(),
            }
            assert!(checkpoint == 0 || checkpoint == 2);
            assert!(advanced || retained == BTreeSet::from([1, 2]));
        }
    });
    assert_eq!(count, 720);
}
