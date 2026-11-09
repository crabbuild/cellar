// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Quiet control turns must not compete with the work they are waiting for.
use std::time::Duration;

#[derive(Default)]
pub(super) struct PollBackoff {
    unchanged: u32,
}

impl PollBackoff {
    pub(super) fn delay(
        &mut self,
        progressed: bool,
        failures: u32,
        interval: Duration,
        maximum: Duration,
    ) -> Duration {
        self.unchanged = if progressed {
            0
        } else {
            self.unchanged.saturating_add(1)
        };
        interval
            .saturating_mul(1u32 << self.unchanged.max(failures).min(8))
            .min(maximum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_unchanged_turns_do_not_spin_at_the_base_interval() {
        let mut backoff = PollBackoff::default();
        let base = Duration::from_millis(50);
        let maximum = Duration::from_millis(250);
        let elapsed: Duration = (0..20)
            .map(|_| {
                let delay = backoff.delay(false, 0, base, maximum);
                assert!((base..=maximum).contains(&delay));
                delay
            })
            .sum();
        assert!(elapsed >= Duration::from_secs(4));
        assert_eq!(backoff.delay(true, 0, base, maximum), base);
        assert!(backoff.delay(false, 0, base, maximum) > base);
    }

    #[test]
    fn progress_does_not_erase_failure_backoff_or_overflow_the_bound() {
        let mut backoff = PollBackoff::default();
        let base = Duration::from_millis(50);
        let maximum = Duration::from_millis(250);
        assert_eq!(backoff.delay(true, u32::MAX, base, maximum), maximum);
        for _ in 0..1000 {
            assert!(backoff.delay(false, u32::MAX, base, maximum) <= maximum);
        }
        assert_eq!(backoff.delay(true, 0, base, maximum), base);
    }
}
