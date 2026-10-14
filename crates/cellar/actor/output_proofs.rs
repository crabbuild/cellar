//! Observations of real adapter barriers. These are not acknowledgement counts
//! or authority: the serial core still validates position, epoch and ownership.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(super) struct OutputProofs {
    started: AtomicU64,
    bucket: AtomicU64,
    fleet: AtomicU64,
    errors: AtomicU64,
    elapsed_us: AtomicU64,
    max_us: AtomicU64,
}

impl OutputProofs {
    pub fn start(&self) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn finish(&self, source: Option<cellar_logic::ProofSource>, elapsed_us: u64) {
        match source {
            Some(cellar_logic::ProofSource::Bucket) => &self.bucket,
            Some(cellar_logic::ProofSource::Fleet) => &self.fleet,
            None => &self.errors,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.elapsed_us.fetch_add(elapsed_us, Ordering::Relaxed);
        self.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({"started":get(&self.started), "bucket":get(&self.bucket),
            "fleet":get(&self.fleet), "errors":get(&self.errors),
            "elapsed_us":get(&self.elapsed_us), "max_us":get(&self.max_us)})
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_adapter_sources_and_errors_without_inventing_cancelled_proofs() {
        let stats = OutputProofs::default();
        for _ in 0..4 {
            stats.start();
        }
        stats.finish(Some(cellar_logic::ProofSource::Fleet), 7);
        stats.finish(Some(cellar_logic::ProofSource::Bucket), 19);
        stats.finish(None, 3);
        assert_eq!(
            stats.snapshot(),
            serde_json::json!({"started":4,"fleet":1,
            "bucket":1,"errors":1,"elapsed_us":29,"max_us":19})
        );
    }
}
