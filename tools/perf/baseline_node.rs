// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reference workload hosted by the existing Fleet/bucket native output gate.
use anyhow::Result;
use cellar::sdk::Application;
use std::sync::Arc;

mod baseline_workload;
mod deployment;
mod embedded;
mod reference_workload;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn application(node: Arc<str>) -> Result<Arc<dyn Application>> {
    baseline_workload::application(
        node,
        baseline_workload::Options {
            secret: std::env::var("CELLAR_PERF_TOKEN")?,
            run_id: std::env::var("CELLAR_PERF_RUN_ID")?,
            policy: std::env::var("CELLAR_PERF_DURABILITY")?,
        },
    )
}

fn main() -> Result<()> {
    embedded::run(deployment::graph(), application)
}

#[cfg(test)]
#[path = "baseline_tests.rs"]
mod tests;
