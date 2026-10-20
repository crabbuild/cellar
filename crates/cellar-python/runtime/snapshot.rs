//! The managed loop publishes owned observations; Rust readers never enter Python.
use super::{InterpreterReport, InterpreterWork};
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct Sample {
    pub interpreter: InterpreterReport,
    pub work: InterpreterWork,
}

/// Last managed-loop observation. Age increases while Python is blocked; this
/// is sampled telemetry, never a fresh liveness check or invocation authority.
#[derive(Clone, Debug, serde::Serialize)]
pub struct InterpreterSnapshot {
    pub generation: u64,
    pub age_ns: u64,
    pub interpreter: InterpreterReport,
    pub work: InterpreterWork,
}

struct Stamped {
    at: Duration,
    generation: u64,
    sample: Sample,
}

pub(crate) struct Samples {
    origin: Instant,
    current: Mutex<Option<Stamped>>,
}

impl Samples {
    /// Process bootstrap calls this outside any node execution domain.
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            current: Mutex::new(None),
        }
    }

    pub fn publish(&self, sample: Sample) {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let generation = current
            .as_ref()
            .map_or(1, |previous| previous.generation.saturating_add(1));
        *current = Some(Stamped {
            at: self.origin.elapsed(),
            generation,
            sample,
        });
    }

    /// Busy publisher and no initial observation both return `None`.
    pub fn read(&self) -> Option<InterpreterSnapshot> {
        let current = self.current.try_lock().ok()?;
        let current = current.as_ref()?;
        Some(InterpreterSnapshot {
            generation: current.generation,
            age_ns: self
                .origin
                .elapsed()
                .saturating_sub(current.at)
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            interpreter: current.sample.interpreter.clone(),
            work: current.sample.work.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_observe_age_and_skip_a_busy_publisher() {
        let mut samples = Samples::new();
        assert!(samples.read().is_none());
        samples.publish(Sample::default());
        assert_eq!(samples.read().unwrap().generation, 1);
        // Move only the test's observation clock, without a wall-clock wait.
        samples.origin -= Duration::from_secs(1);
        assert!(samples.read().unwrap().age_ns >= 1_000_000_000);
        let publishing = samples.current.lock().unwrap();
        assert!(samples.read().is_none());
        drop(publishing);
        samples.publish(Sample::default());
        assert_eq!(samples.read().unwrap().generation, 2);
    }
}
