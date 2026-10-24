// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! External startup/liveness watchdog for process-owned runtimes.
#![allow(clippy::disallowed_methods)] // This CLI owns processes, probes and real timers.
mod child;
mod options;

use crate::cli_output::{Format, Output, Record};
use anyhow::{bail, ensure, Result};
use options::Options;
use std::os::unix::process::ExitStatusExt as _;
use std::{borrow::Cow, process::ExitStatus};
use tokio::time::{Instant, MissedTickBehavior};

const INSTANCE_HEADER: &str = "x-cellar-supervisor-instance";

struct Lifecycle {
    event: &'static str,
    pid: u32,
    attempt: u32,
    reason: Option<&'static str>,
    forced: bool,
    exit_code: Option<i32>,
    exit_signal: Option<i32>,
}

impl Record for Lifecycle {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({"event":self.event,"pid":self.pid,"attempt":self.attempt,"reason":self.reason,"forced":self.forced,"exit_code":self.exit_code,"exit_signal":self.exit_signal})
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "{} pid={} attempt={} reason={} forced={} exit_code={:?} exit_signal={:?}",
            self.event,
            self.pid,
            self.attempt,
            self.reason.unwrap_or("none"),
            self.forced,
            self.exit_code,
            self.exit_signal
        ))
    }
}

enum End {
    Exited(ExitStatus),
    Requested(&'static str),
    Startup,
    Unhealthy,
}

fn publish(output: &mut Output, record: &Lifecycle) -> Result<()> {
    output.row(record)?;
    output.flush()
}

pub(crate) async fn run(arguments: Vec<String>) -> Result<()> {
    let Some(options) = Options::parse(arguments)? else {
        return Output::new(Format::Text).help(options::HELP);
    };
    let mut signals = crate::dev::ShutdownSignals::install()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(options.probe)
        .timeout(options.probe)
        .pool_max_idle_per_host(1)
        .build()?;
    let mut output = Output::new(if options.json {
        Format::Json
    } else {
        Format::Text
    });
    for attempt in 0..=options.max_restarts {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes)?;
        let instance = format!("{:032x}", u128::from_ne_bytes(bytes));
        let mut owned = child::Owned::spawn(&options, &instance)?;
        let end = match publish(
            &mut output,
            &Lifecycle {
                event: "supervisor_started",
                pid: owned.pid,
                attempt,
                reason: None,
                forced: false,
                exit_code: None,
                exit_signal: None,
            },
        ) {
            Ok(()) => {
                watch(
                    &mut owned,
                    &options,
                    &client,
                    &instance,
                    &mut signals,
                    &mut output,
                    attempt,
                )
                .await
            }
            Err(error) => Err(error),
        };
        let reason = match &end {
            Ok(End::Exited(_)) => "child_exit",
            Ok(End::Requested(reason)) => *reason,
            Ok(End::Startup) => "startup_timeout",
            Ok(End::Unhealthy) => "health_failed",
            Err(_) => "observation_error",
        };
        let transition = publish(
            &mut output,
            &Lifecycle {
                event: "supervisor_stopping",
                pid: owned.pid,
                attempt,
                reason: Some(reason),
                forced: false,
                exit_code: None,
                exit_signal: None,
            },
        );
        // Even an observation/output error must terminate and join the child.
        let stopped = owned.stop(options.shutdown, &mut signals).await;
        let (status, forced) = stopped?;
        let end = end?;
        transition?;
        publish(
            &mut output,
            &Lifecycle {
                event: "supervisor_stopped",
                pid: owned.pid,
                attempt,
                reason: Some(reason),
                forced,
                exit_code: status.code(),
                exit_signal: status.signal(),
            },
        )?;
        match end {
            End::Requested(_) => {
                ensure!(
                    !forced && status.success(),
                    "supervised process did not exit successfully within its shutdown grace"
                );
                return Ok(());
            }
            End::Exited(observed) => {
                ensure!(
                    observed == status,
                    "supervised process exit changed while joining"
                );
            }
            _ => {}
        }
        if attempt == options.max_restarts {
            bail!("supervised process exhausted its restart limit");
        }
        crate::asyncrt::select_biased! { "operator stop prevents a simultaneous restart";
        _ = signals.received() => return Ok(()), _ = tokio::time::sleep(options.restart) => {} }
    }
    unreachable!("bounded restart loop always returns")
}

async fn watch(
    owned: &mut child::Owned,
    options: &Options,
    client: &reqwest::Client,
    instance: &str,
    signals: &mut crate::dev::ShutdownSignals,
    output: &mut Output,
    attempt: u32,
) -> Result<End> {
    let deadline = Instant::now() + options.startup;
    let mut interval = tokio::time::interval(options.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let (mut ready, mut failures) = (false, 0u32);
    loop {
        if output.closed() {
            return Ok(End::Requested("output_closed"));
        }
        crate::asyncrt::select_biased! {
            "exit, operator stop and startup deadline take precedence over another health probe";
            status = owned.child.wait() => return Ok(End::Exited(status?)),
            _ = signals.received() => return Ok(End::Requested("signal")),
            _ = tokio::time::sleep_until(deadline), if !ready => return Ok(End::Startup),
            _ = interval.tick() => {}
        }
        let response = crate::asyncrt::select_biased! {
            "exit, operator stop and startup deadline take precedence over a health response";
            status = owned.child.wait() => return Ok(End::Exited(status?)),
            _ = signals.received() => return Ok(End::Requested("signal")),
            _ = tokio::time::sleep_until(deadline), if !ready => return Ok(End::Startup),
            response = client.get(options.url.clone()).send() => response,
        };
        let healthy = response.is_ok_and(|response| {
            response.status().is_success()
                && response
                    .headers()
                    .get(INSTANCE_HEADER)
                    .is_some_and(|value| value.as_bytes() == instance.as_bytes())
        });
        if healthy {
            failures = 0;
            if !ready {
                ready = true;
                publish(
                    output,
                    &Lifecycle {
                        event: "supervisor_ready",
                        pid: owned.pid,
                        attempt,
                        reason: None,
                        forced: false,
                        exit_code: None,
                        exit_signal: None,
                    },
                )?;
            }
        } else if ready {
            failures += 1;
            if failures >= options.failures {
                return Ok(End::Unhealthy);
            }
        }
    }
}
