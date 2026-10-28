// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Payload-free observations of an existing engine budget. These clocks never
//! decide admission, change deadlines or grant additional execution time.
use std::{
    fmt,
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Startup,
    Replies,
    Admission,
    Microtasks,
    Completion,
    Cancellation,
    Retirement,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Replies => "replies",
            Self::Admission => "admission",
            Self::Microtasks => "microtasks",
            Self::Completion => "completion",
            Self::Cancellation => "cancellation",
            Self::Retirement => "retirement",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Progress {
    started: Instant,
    phase: Phase,
    phase_started: Instant,
    cpu_started: Option<Duration>,
}

impl Progress {
    pub(super) fn new(phase: Phase, now: Instant, cpu: Option<Duration>) -> Self {
        Self {
            started: now,
            phase,
            phase_started: now,
            cpu_started: cpu,
        }
    }

    pub(super) fn phase(&mut self, phase: Phase, now: Instant) {
        self.phase = phase;
        self.phase_started = now;
    }

    pub(super) fn observe(self, now: Instant) -> Observation {
        Observation {
            progress: self,
            observed: now,
        }
    }
}

/// Copy the first watchdog observation before the engine clears its current
/// pass. CPU is sampled later on the engine thread, never on the watchdog or
/// error-reporting thread; it therefore includes engine-side unwind work.
#[derive(Clone, Copy)]
pub(super) struct Observation {
    progress: Progress,
    observed: Instant,
}

impl Observation {
    pub(super) fn report(self, now: Instant, cpu: Option<Duration>) -> Report {
        Report {
            phase: self.progress.phase,
            observed_wall_us: self
                .observed
                .saturating_duration_since(self.progress.started)
                .as_micros(),
            phase_wall_us: self
                .observed
                .saturating_duration_since(self.progress.phase_started)
                .as_micros(),
            report_wall_us: now
                .saturating_duration_since(self.progress.started)
                .as_micros(),
            engine_thread_cpu_us: self
                .progress
                .cpu_started
                .zip(cpu)
                .and_then(|(start, end)| end.checked_sub(start))
                .map(|cpu| cpu.as_micros()),
        }
    }
}

pub(super) struct Report {
    phase: Phase,
    observed_wall_us: u128,
    phase_wall_us: u128,
    report_wall_us: u128,
    engine_thread_cpu_us: Option<u128>,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JavaScript engine turn exceeded execution budget: phase={} observed_wall_us={} phase_wall_us={} report_wall_us={} engine_thread_cpu_us=",
            self.phase.name(), self.observed_wall_us, self.phase_wall_us, self.report_wall_us)?;
        match self.engine_thread_cpu_us {
            Some(cpu) => write!(f, "{cpu}"),
            None => f.write_str("unavailable"),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn thread_cpu() -> Option<Duration> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes to a valid initialized timespec. This is the
    // calling engine thread's CPU clock; no foreign-thread handle is retained.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) } != 0 {
        return None;
    }
    let seconds = u64::try_from(time.tv_sec).ok()?;
    let nanos = u32::try_from(time.tv_nsec).ok()?;
    (nanos < 1_000_000_000).then(|| Duration::new(seconds, nanos))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn thread_cpu() -> Option<Duration> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_phase_and_wall_observation_survive_later_progress() {
        let start = Instant::now();
        let mut progress = Progress::new(Phase::Replies, start, Some(Duration::from_millis(3)));
        progress.phase(Phase::Microtasks, start + Duration::from_millis(20));
        let observed = progress.observe(start + Duration::from_millis(80));
        progress.phase(Phase::Retirement, start + Duration::from_millis(81));
        let report = observed.report(
            start + Duration::from_millis(90),
            Some(Duration::from_millis(15)),
        );
        assert_eq!(report.to_string(), "JavaScript engine turn exceeded execution budget: phase=microtasks observed_wall_us=80000 phase_wall_us=60000 report_wall_us=90000 engine_thread_cpu_us=12000");
    }

    #[test]
    fn missing_or_reversed_cpu_samples_are_not_fabricated() {
        let now = Instant::now();
        for initial in [None, Some(Duration::from_secs(2))] {
            let observed = Progress::new(Phase::Startup, now, initial).observe(now);
            assert!(observed
                .report(now, Some(Duration::from_secs(1)))
                .to_string()
                .ends_with("unavailable"));
        }
    }
}
