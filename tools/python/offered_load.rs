//! Bounded, independent-arrival HTTP echo client for the native/Python hosts.
//! Client scheduling, overload and transport failures remain in raw evidence.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context, Result};
use reqwest::{Client, Url};
use serde::Serialize;
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};
use tokio::{task::JoinSet, time::Instant};

#[derive(Clone, Serialize)]
struct Workload {
    url: String,
    rate: u64,
    duration_ms: u64,
    max_inflight: usize,
    body_bytes: usize,
    timeout_ms: u64,
    max_lag_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Success,
    ClientCapacity,
    ClientLate,
    Timeout,
    Transport,
    HttpStatus,
    BodyMismatch,
}

#[derive(Serialize)]
struct Sample {
    id: u64,
    scheduled_ns: u64,
    observed_ns: u64,
    dispatched_ns: Option<u64>,
    completed_ns: Option<u64>,
    outcome: Outcome,
    status: Option<u16>,
}

#[derive(Serialize)]
struct Report {
    format: u8,
    debug_assertions: bool,
    workload: Workload,
    planned: u64,
    elapsed_ns: u64,
    peak_inflight: usize,
    samples: Vec<Sample>,
}

fn offset(id: u64, rate: u64) -> u64 {
    // Limits below keep the integer product within u64. Each arrival is
    // anchored independently; slow responses never move subsequent arrivals.
    id * 1_000_000_000 / rate
}

fn elapsed(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

impl Workload {
    fn parse(args: &[String]) -> Result<(Self, PathBuf)> {
        ensure!(args.len() == 8,
            "usage: python-offered-load URL RATE DURATION_MS MAX_INFLIGHT BODY_BYTES TIMEOUT_MS MAX_LAG_MS OUTPUT.json");
        let mut url = Url::parse(&args[0])?;
        ensure!(
            url.scheme() == "http"
                && url.host_str() == Some("127.0.0.1")
                && url.port().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none(),
            "a private loopback origin is required"
        );
        url.set_path("/echo");
        let workload = Self {
            url: url.into(),
            rate: args[1].parse()?,
            duration_ms: args[2].parse()?,
            max_inflight: args[3].parse()?,
            body_bytes: args[4].parse()?,
            timeout_ms: args[5].parse()?,
            max_lag_ms: args[6].parse()?,
        };
        ensure!(
            (1..=100_000).contains(&workload.rate)
                && (1..=120_000).contains(&workload.duration_ms)
                && (1..=512).contains(&workload.max_inflight)
                && workload.body_bytes <= 65_536
                && (1..=10_000).contains(&workload.timeout_ms)
                && (1..=1_000).contains(&workload.max_lag_ms),
            "invalid finite workload limits"
        );
        let planned = workload.rate * workload.duration_ms / 1_000;
        ensure!((1..=1_000_000).contains(&planned), "invalid arrival count");
        Ok((workload, PathBuf::from(&args[7])))
    }
}

fn payload(id: u64, size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| (index as u64).wrapping_add(id) as u8)
        .collect()
}

async fn request(client: Client, workload: Workload, start: Instant, mut sample: Sample) -> Sample {
    let body = payload(sample.id, workload.body_bytes);
    // Task startup can lag behind its spawn. Charge that delay to the client,
    // and never send a late task merely because it previously reserved a slot.
    sample.observed_ns = elapsed(start);
    if sample.observed_ns >= workload.duration_ms * 1_000_000
        || sample.observed_ns.saturating_sub(sample.scheduled_ns) > workload.max_lag_ms * 1_000_000
    {
        sample.outcome = Outcome::ClientLate;
        return sample;
    }
    sample.dispatched_ns = Some(sample.observed_ns);
    let result = async {
        let mut response = client
            .post(&workload.url)
            .header("content-type", "application/octet-stream")
            .body(body.clone())
            .send()
            .await?;
        sample.status = Some(response.status().as_u16());
        if response.status().as_u16() != 200 {
            return Ok(Outcome::HttpStatus);
        }
        // A broken host must not turn the bounded client into an unbounded
        // response accumulator. Compare chunks directly, including EOF.
        let mut cursor = 0;
        while let Some(chunk) = response.chunk().await? {
            let end = cursor + chunk.len();
            if body.get(cursor..end) != Some(chunk.as_ref()) {
                return Ok(Outcome::BodyMismatch);
            }
            cursor = end;
        }
        Ok::<_, reqwest::Error>(if cursor == body.len() {
            Outcome::Success
        } else {
            Outcome::BodyMismatch
        })
    }
    .await;
    sample.completed_ns = Some(elapsed(start));
    sample.outcome = match result {
        Ok(outcome) => outcome,
        Err(error) if error.is_timeout() => Outcome::Timeout,
        Err(_) => Outcome::Transport,
    };
    sample
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let (workload, output) = Workload::parse(&std::env::args().skip(1).collect::<Vec<_>>())?;
    // Refuse evidence replacement before sending any traffic. A partial file
    // from a failed client cannot be mistaken for a complete report.
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    let client = Client::builder()
        .timeout(Duration::from_millis(workload.timeout_ms))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .pool_max_idle_per_host(workload.max_inflight)
        .build()?;
    let planned = workload.rate * workload.duration_ms / 1_000;
    let mut samples = Vec::with_capacity(planned as usize);
    let mut tasks = JoinSet::new();
    let mut peak_inflight = 0;
    let start = Instant::now();
    for id in 0..planned {
        let scheduled_ns = offset(id, workload.rate);
        tokio::time::sleep_until(start + Duration::from_nanos(scheduled_ns)).await;
        // Completed tasks do not occupy capacity. Keep this bounded by the
        // admission limit rather than accumulating one task per arrival.
        while let Some(result) = tasks.try_join_next() {
            samples.push(result.context("client request task failed")?);
        }
        let observed_ns = elapsed(start);
        let mut sample = Sample {
            id,
            scheduled_ns,
            observed_ns,
            dispatched_ns: None,
            completed_ns: None,
            outcome: Outcome::ClientCapacity,
            status: None,
        };
        if observed_ns >= workload.duration_ms * 1_000_000
            || observed_ns.saturating_sub(scheduled_ns) > workload.max_lag_ms * 1_000_000
        {
            sample.outcome = Outcome::ClientLate;
            samples.push(sample);
        } else if tasks.len() >= workload.max_inflight {
            samples.push(sample);
        } else {
            tasks.spawn(request(client.clone(), workload.clone(), start, sample));
            peak_inflight = peak_inflight.max(tasks.len());
        }
    }
    // The measurement window is fixed even when the last request finishes
    // early. Drain all admitted requests, retaining their actual completion.
    tokio::time::sleep_until(start + Duration::from_millis(workload.duration_ms)).await;
    while let Some(result) = tasks.join_next().await {
        samples.push(result.context("client request task failed")?);
    }
    let elapsed_ns = elapsed(start);
    ensure!(samples.len() as u64 == planned, "missing arrival evidence");
    samples.sort_unstable_by_key(|sample| sample.id);
    let report = Report {
        format: 1,
        debug_assertions: cfg!(debug_assertions),
        workload,
        planned,
        elapsed_ns,
        peak_inflight,
        samples,
    };
    // Serialization is outside the measured interval. Buffer it so large
    // reports do not spend millions of syscalls keeping an idle host alive.
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, &report)?;
    writer.flush()?;
    Ok(())
}
