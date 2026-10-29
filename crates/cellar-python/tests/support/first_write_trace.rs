// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, memory-only diagnostics for the synthetic Python reliability probes.
use anyhow::Result;
use std::{
    collections::VecDeque,
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
pub(super) struct Trace(Arc<Mutex<VecDeque<Vec<u8>>>>);

impl Trace {
    pub(super) fn install() -> Result<Self> {
        let trace = Self::default();
        if std::env::var_os("RUST_LOG").is_some() {
            let writer = trace.clone();
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()?)
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .try_init()
                .map_err(|error| anyhow::anyhow!("Python reliability tracing: {error}"))?;
        }
        Ok(trace)
    }

    pub(super) fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    pub(super) fn save(&self, path: &Path) -> Result<()> {
        let mut file = std::fs::File::create(path)?;
        for line in self.0.lock().unwrap().iter() {
            file.write_all(line)?;
        }
        Ok(())
    }
}

impl Write for Trace {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut lines = self.0.lock().unwrap();
        // The newest 2,048 records, at most 8 KiB each. This is a diagnostic
        // tail, not a qualification transcript; old/long records are truncated.
        if lines.len() == 2048 {
            lines.pop_front();
        }
        lines.push_back(bytes[..bytes.len().min(8192)].to_vec());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
