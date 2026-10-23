//! Bounded, independent-arrival HTTP client for the native/Python hosts.
//! Client scheduling, overload and transport failures remain in raw evidence.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context, Result};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{task::JoinSet, time::Instant};

#[derive(Serialize)]
struct Workload {
    url: String,
    rate: u64,
    duration_ms: u64,
    max_inflight: usize,
    body_bytes: usize,
    timeout_ms: u64,
    max_lag_ms: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    reads: Vec<ReadTarget>,
    #[serde(skip)]
    failure_details: AtomicUsize,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadTarget {
    path: String,
    expected_body: Vec<u8>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    response_prefix: Option<String>,
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
        ensure!(args.len() == 8 || args.len() == 9,
            "usage: python-offered-load URL RATE DURATION_MS MAX_INFLIGHT BODY_BYTES TIMEOUT_MS MAX_LAG_MS OUTPUT.json [READ_TARGETS.json]");
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
        let reads: Vec<ReadTarget> = if let Some(path) = args.get(8) {
            let mut bytes = Vec::new();
            File::open(path)?
                .take(5 * 1_048_576 + 1)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= 5 * 1_048_576,
                "read manifest exceeds byte limit"
            );
            serde_json::from_slice(&bytes)?
        } else {
            url.set_path("/echo");
            Vec::new()
        };
        if args.len() == 9 {
            ensure!((1..=64).contains(&reads.len()), "invalid read target count");
            let mut paths = HashSet::new();
            for target in &reads {
                ensure!(
                    target.path.starts_with('/')
                        && !target.path.starts_with("//")
                        && target.path.len() <= 256
                        && target.path.bytes().all(|byte| byte.is_ascii_alphanumeric()
                            || matches!(byte, b'/' | b'-' | b'_'))
                        && paths.insert(&target.path)
                        && target.expected_body.len() <= 65_536,
                    "invalid or duplicate read target"
                );
            }
            ensure!(
                reads
                    .iter()
                    .map(|target| target.expected_body.len())
                    .sum::<usize>()
                    <= 1_048_576,
                "read targets exceed total body limit"
            );
        }
        let workload = Self {
            url: url.into(),
            rate: args[1].parse()?,
            duration_ms: args[2].parse()?,
            max_inflight: args[3].parse()?,
            body_bytes: args[4].parse()?,
            timeout_ms: args[5].parse()?,
            max_lag_ms: args[6].parse()?,
            reads,
            failure_details: AtomicUsize::new(0),
        };
        ensure!(
            (1..=100_000).contains(&workload.rate)
                && (1..=120_000).contains(&workload.duration_ms)
                && (1..=512).contains(&workload.max_inflight)
                && workload.body_bytes <= 65_536
                && (workload.reads.is_empty() || workload.body_bytes == 0)
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

async fn request(
    client: Client,
    workload: Arc<Workload>,
    start: Instant,
    mut sample: Sample,
) -> Sample {
    let echo_body = payload(sample.id, workload.body_bytes);
    let (request, expected) = if workload.reads.is_empty() {
        (
            client
                .post(&workload.url)
                .header("content-type", "application/octet-stream")
                .body(echo_body.clone()),
            echo_body.as_slice(),
        )
    } else {
        let target = &workload.reads[sample.id as usize % workload.reads.len()];
        (
            client.get(format!(
                "{}{}",
                workload.url.trim_end_matches('/'),
                target.path
            )),
            target.expected_body.as_slice(),
        )
    };
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
        let mut response = request.send().await?;
        sample.status = Some(response.status().as_u16());
        if response.status().as_u16() != 200 {
            // Keep enough typed failure evidence to distinguish admission from
            // storage/lease failures, without retaining every failed body.
            if workload.failure_details.fetch_add(1, Ordering::Relaxed) < 16 {
                let mut prefix = Vec::new();
                while prefix.len() < 1024 {
                    match response.chunk().await {
                        Ok(Some(chunk)) => {
                            let count = chunk.len().min(1024 - prefix.len());
                            prefix.extend_from_slice(&chunk[..count]);
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                sample.response_prefix = Some(String::from_utf8_lossy(&prefix).into_owned());
            }
            return Ok(Outcome::HttpStatus);
        }
        // A broken host must not turn the bounded client into an unbounded
        // response accumulator. Compare chunks directly, including EOF.
        let mut cursor = 0;
        while let Some(chunk) = response.chunk().await? {
            let end = cursor + chunk.len();
            if expected.get(cursor..end) != Some(chunk.as_ref()) {
                return Ok(Outcome::BodyMismatch);
            }
            cursor = end;
        }
        Ok::<_, reqwest::Error>(if cursor == expected.len() {
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
    let workload = Arc::new(workload);
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
            response_prefix: None,
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
        workload: Arc::try_unwrap(workload)
            .map_err(|_| anyhow::anyhow!("client workload still in use"))?,
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
