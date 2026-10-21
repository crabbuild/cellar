//! Finite operator policy, parsed before starting an owned process.
use anyhow::{bail, ensure, Context as _, Result};
use std::{path::PathBuf, time::Duration};

pub(super) const HELP: &str = "cellar supervise --health-url URL [OPTIONS] -- PROGRAM [ARG...]\n\
    --startup-ms 30000   Deadline for the first instance-matched healthy response\n\
    --probe-ms 2000      Timeout of each HTTP probe (redirects/proxies disabled)\n\
    --interval-ms 1000   Time between probes; missed ticks are skipped\n\
    --failures 3         Consecutive failures after readiness before restarting\n\
    --shutdown-ms 35000  TERM grace before KILL; a second signal forces KILL\n\
    --restart-ms 1000    Delay before restarting an unhealthy/exited process\n\
    --max-restarts 3     Restarts allowed across this supervisor's lifetime\n\
    --json              Structured lifecycle records on stdout\n\n\
    Child stdout/stderr go to stderr. The health handler must echo the child-only\n\
    CELLAR_SUPERVISOR_INSTANCE value in x-cellar-supervisor-instance. Use a handler\n\
    dispatched by the runtime being monitored. Exit status is not a drain proof.";

pub(super) struct Options {
    pub url: reqwest::Url,
    pub startup: Duration,
    pub probe: Duration,
    pub interval: Duration,
    pub failures: u32,
    pub shutdown: Duration,
    pub restart: Duration,
    pub max_restarts: u32,
    pub json: bool,
    pub program: PathBuf,
    pub arguments: Vec<String>,
}

impl Options {
    pub fn parse(arguments: Vec<String>) -> Result<Option<Self>> {
        let mut arguments = arguments.into_iter();
        let mut url = None;
        let (mut startup, mut probe, mut interval, mut shutdown, mut restart) =
            (30000, 2000, 1000, 35000, 1000);
        let (mut failures, mut max_restarts, mut json) = (3, 3, false);
        let mut program = None;
        let mut child_arguments = Vec::new();
        while let Some(flag) = arguments.next() {
            match flag.as_str() {
                "--help" | "-h" => return Ok(None),
                "--json" => {
                    json = true;
                    continue;
                }
                "--" => {
                    program = Some(PathBuf::from(
                        arguments
                            .next()
                            .context("supervise requires a program after --")?,
                    ));
                    child_arguments = arguments.collect();
                    break;
                }
                _ => {}
            }
            let value = arguments
                .next()
                .with_context(|| format!("{flag} requires a value"))?;
            match flag.as_str() {
                "--health-url" => url = Some(value),
                "--startup-ms" => startup = duration(&value)?,
                "--probe-ms" => probe = duration(&value)?,
                "--interval-ms" => interval = duration(&value)?,
                "--shutdown-ms" => shutdown = duration(&value)?,
                "--restart-ms" => restart = duration(&value)?,
                "--failures" => {
                    failures = value
                        .parse::<u32>()
                        .context("invalid probe failure count")?;
                    ensure!(
                        (1..=1000).contains(&failures),
                        "probe failures must be 1..1000"
                    );
                }
                "--max-restarts" => {
                    max_restarts = value.parse::<u32>().context("invalid restart count")?;
                    ensure!(max_restarts <= 1000, "max restarts must be 0..1000");
                }
                _ => bail!("unknown supervise option {flag}"),
            }
        }
        let url = reqwest::Url::parse(&url.context("supervise requires --health-url")?)
            .context("invalid health URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "health URL must be HTTP(S) without credentials, query or fragment"
        );
        let program = program.context("supervise requires -- PROGRAM [ARG...]")?;
        ensure!(
            !program.as_os_str().is_empty(),
            "supervised program must not be empty"
        );
        Ok(Some(Self {
            url,
            startup: Duration::from_millis(startup),
            probe: Duration::from_millis(probe),
            interval: Duration::from_millis(interval),
            failures,
            shutdown: Duration::from_millis(shutdown),
            restart: Duration::from_millis(restart),
            max_restarts,
            json,
            program,
            arguments: child_arguments,
        }))
    }
}

fn duration(value: &str) -> Result<u64> {
    let value = value.parse::<u64>().context("invalid supervise duration")?;
    ensure!(
        (1..=86_400_000).contains(&value),
        "supervise durations must be 1..86400000 milliseconds"
    );
    Ok(value)
}
