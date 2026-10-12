//! Closed-loop HTTP write measurements, with per-cell acknowledgement accounting.

// The load generator runs outside Cellar's execution boundary. Its clock and
// accounting files belong to the measuring client, not to a Cellar node.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, Context, Result};
use rand::{RngCore, SeedableRng};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Default, Serialize, Deserialize)]
struct Accounting {
    writes: Vec<u64>,
}

#[derive(Default)]
struct Samples {
    latency_us: Vec<u64>,
    errors: BTreeMap<String, u64>,
    writes: Vec<u64>,
    owners: BTreeMap<String, u64>,
}

#[derive(Deserialize)]
struct Reply {
    writes: u64,
    payload_bytes: usize,
    node: String,
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((sorted.len() as f64 * p).ceil() as usize).max(1) - 1;
    sorted[rank.min(sorted.len() - 1)] as f64 / 1000.0
}

fn save(path: &Path, accounting: &Accounting) -> Result<()> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(accounting)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn report(value: serde_json::Value) -> Result<()> {
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("{value}"))
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    anyhow::ensure!(
        args.len() == 8,
        "usage: cellar-perf-load write|verify URLS CELLS CONCURRENCY SECONDS BYTES ACCOUNTING.json"
    );
    let urls: Vec<_> = args[2].split(',').map(str::to_owned).collect();
    let cells: usize = args[3].parse()?;
    let concurrency: usize = args[4].parse()?;
    let seconds: u64 = args[5].parse()?;
    let payload_bytes: usize = args[6].parse()?;
    anyhow::ensure!(
        cells > 0 && concurrency > 0 && concurrency <= cells && payload_bytes > 0,
        "invalid workload"
    );
    let accounting_path = Path::new(&args[7]);
    let mut accounting: Accounting = if accounting_path.exists() {
        serde_json::from_slice(&std::fs::read(accounting_path)?)?
    } else {
        Accounting {
            writes: vec![0; cells],
        }
    };
    anyhow::ensure!(accounting.writes.len() == cells, "cell count changed");
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(concurrency)
        .build()?;
    if args[1] == "verify" {
        let started = Instant::now();
        let mut owners = BTreeMap::<String, usize>::new();
        let mut failures = Vec::new();
        let mut read_retries = BTreeMap::<String, u64>::new();
        let mut max_cell_read_ms = 0.0_f64;
        for (cell, expected) in accounting.writes.iter().enumerate() {
            let read_started = Instant::now();
            let reply: Reply = loop {
                let result = async {
                    client
                        .get(format!("{}/cells/{cell:04}", urls[cell % urls.len()]))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<Reply>()
                        .await
                }
                .await;
                match result {
                    Ok(reply) => break reply,
                    Err(error) => {
                        let retryable = error.is_connect()
                            || error.is_timeout()
                            || error.status().is_some_and(|status| {
                                matches!(status.as_u16(), 500 | 502 | 503 | 504)
                            });
                        if !retryable || read_started.elapsed() >= Duration::from_secs(120) {
                            return Err(error)
                                .context(format!("verification failed for cell {cell}"));
                        }
                        *read_retries
                            .entry(
                                error
                                    .status()
                                    .map_or_else(|| "transport".into(), |s| s.to_string()),
                            )
                            .or_default() += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            };
            max_cell_read_ms = max_cell_read_ms.max(read_started.elapsed().as_secs_f64() * 1000.0);
            *owners.entry(reply.node).or_default() += 1;
            if reply.writes != *expected || (*expected > 0 && reply.payload_bytes != payload_bytes)
            {
                failures.push(serde_json::json!({"cell": cell, "expected": expected, "actual": reply.writes, "payload_bytes": reply.payload_bytes}));
            }
        }
        report(
            serde_json::json!({"operation": "verify", "cells": cells, "expected_writes": accounting.writes.iter().sum::<u64>(), "owners": owners, "elapsed_s": started.elapsed().as_secs_f64(), "read_retries": read_retries, "max_cell_read_ms": max_cell_read_ms, "mismatches": failures}),
        )?;
        anyhow::ensure!(failures.is_empty(), "durable state mismatch");
        return Ok(());
    }
    if args[1] != "write" {
        bail!("expected write or verify");
    }
    anyhow::ensure!(seconds > 0, "duration must be positive");
    let started = Instant::now();
    let deadline = started + Duration::from_secs(seconds);
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..concurrency {
        let client = client.clone();
        let urls = urls.clone();
        tasks.spawn(async move {
            let mut random = rand::rngs::StdRng::seed_from_u64(0xCE11_A000 + worker as u64);
            let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            let mut payload = vec![0_u8; payload_bytes];
            let assigned: Vec<_> = (worker..cells).step_by(concurrency).collect();
            let mut samples = Samples {
                writes: vec![0; cells],
                ..Samples::default()
            };
            let mut iteration = 0;
            while Instant::now() < deadline {
                let cell = assigned[iteration % assigned.len()];
                // Rotate ingress independently of ownership to exercise remote routing.
                let ingress = (cell + iteration / assigned.len()) % urls.len();
                iteration += 1;
                random.fill_bytes(&mut payload);
                for byte in &mut payload {
                    *byte = alphabet[(*byte & 63) as usize];
                }
                let start = Instant::now();
                let result = async {
                    let response = client
                        .post(format!("{}/cells/{cell:04}", urls[ingress]))
                        .body(payload.clone())
                        .send()
                        .await?;
                    let status = response.status();
                    if !status.is_success() {
                        bail!(
                            "HTTP {status}: {}",
                            response.text().await?.chars().take(160).collect::<String>()
                        );
                    }
                    let reply: Reply = response.json().await?;
                    anyhow::ensure!(
                        reply.payload_bytes == payload_bytes,
                        "payload length mismatch"
                    );
                    Ok::<_, anyhow::Error>(reply)
                }
                .await;
                let elapsed = start.elapsed().as_micros().min(u64::MAX as u128) as u64;
                match result {
                    Ok(reply) => {
                        samples.latency_us.push(elapsed);
                        samples.writes[cell] += 1;
                        *samples.owners.entry(reply.node).or_default() += 1;
                    }
                    Err(error) => {
                        *samples.errors.entry(error.to_string()).or_default() += 1;
                    }
                }
            }
            samples
        });
    }
    let mut samples = Samples::default();
    while let Some(result) = tasks.join_next().await {
        let result = result.context("load worker failed")?;
        samples.latency_us.extend(result.latency_us);
        for (cell, writes) in result.writes.iter().enumerate() {
            accounting.writes[cell] += writes;
        }
        for (error, count) in result.errors {
            *samples.errors.entry(error).or_default() += count;
        }
        for (owner, count) in result.owners {
            *samples.owners.entry(owner).or_default() += count;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    save(accounting_path, &accounting)?;
    samples.latency_us.sort_unstable();
    let successful = samples.latency_us.len();
    report(serde_json::json!({
            "operation": "write", "cells": cells, "concurrency": concurrency, "payload_bytes": payload_bytes,
            "payload_pattern": "changing seeded random ASCII (64-symbol alphabet)",
        "requested_seconds": seconds, "elapsed_s": elapsed, "successful_writes": successful,
        "errors": samples.errors, "owners": samples.owners, "writes_per_s": successful as f64 / elapsed,
        "logical_mib_per_s": successful as f64 * payload_bytes as f64 / elapsed / 1048576.0,
        "latency_ms": {"p50": percentile(&samples.latency_us, 0.5), "p95": percentile(&samples.latency_us, 0.95),
            "p99": percentile(&samples.latency_us, 0.99), "max": percentile(&samples.latency_us, 1.0)},
    }))?;
    anyhow::ensure!(
        samples.errors.is_empty(),
        "write errors occurred; ambiguous commits require investigation"
    );
    Ok(())
}
