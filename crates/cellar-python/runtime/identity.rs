// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One non-reusing identity space for cell proxies and invocation mailboxes.
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) fn reserve(next: &AtomicU64) -> Result<u64> {
    next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    })
    .map_err(|_| anyhow::anyhow!("Python runtime identity space exhausted"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhaustion_refuses_without_reusing_an_identity() {
        let next = AtomicU64::new(u64::MAX - 1);
        assert_eq!(reserve(&next).unwrap(), u64::MAX - 1);
        for _ in 0..2 {
            assert!(reserve(&next).is_err());
            assert_eq!(next.load(Ordering::Relaxed), u64::MAX);
        }
    }

    #[test]
    fn simultaneous_factory_and_invocation_reservations_are_distinct() {
        let next = AtomicU64::new(1);
        let mut ids = std::thread::scope(|scope| {
            let workers = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        (0..1024)
                            .map(|_| reserve(&next).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        ids.sort_unstable();
        assert_eq!(ids, (1..=4096).collect::<Vec<_>>());
        assert_eq!(next.load(Ordering::Relaxed), 4097);
    }
}
