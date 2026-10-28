// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, memory-only lease observations for failed actor fixtures.
#![allow(clippy::disallowed_methods)]
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

#[derive(Debug)]
pub struct Observation {
    elapsed_ms: u128,
    thread: String,
    fields: Vec<(String, String)>,
}

pub struct Trace {
    started: Instant,
    observations: Mutex<VecDeque<Observation>>,
}

impl Trace {
    pub fn mark(&self, phase: &str) {
        self.record(vec![("phase".into(), phase.to_owned())]);
    }

    pub fn applied_renewal(&self, thread: &str) -> bool {
        self.observations.lock().unwrap().iter().any(|event| {
            event.thread == thread
                && event
                    .fields
                    .iter()
                    .any(|(key, value)| key == "attempt" && value.trim_matches('"') == "renew")
                && event
                    .fields
                    .iter()
                    .any(|(key, value)| key == "outcome" && value.trim_matches('"') == "applied")
        })
    }

    fn record(&self, fields: Vec<(String, String)>) {
        let mut observations = self.observations.lock().unwrap();
        if observations.len() == 64 {
            observations.pop_front();
        }
        observations.push_back(Observation {
            elapsed_ms: self.started.elapsed().as_millis(),
            thread: std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_owned(),
            fields,
        });
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.observations
            .lock()
            .unwrap()
            .iter()
            .map(|event| format!("{}ms {} {:?}", event.elapsed_ms, event.thread, event.fields))
            .collect()
    }
}

struct LeaseLayer(Arc<Trace>);

impl<S: Subscriber> Layer<S> for LeaseLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields(Vec::new());
        event.record(&mut fields);
        if !fields
            .0
            .iter()
            .any(|(key, value)| key == "event" && value.contains("node_lease_"))
        {
            return;
        }
        self.0.record(fields.0);
    }
}

struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}

/// One subscriber owns both bounded proof observations and optional stderr
/// diagnostics. Only the executable boundary supplies an environment filter;
/// actor fixtures pass None and retain their memory-only behavior.
pub fn install(logging: Option<tracing_subscriber::EnvFilter>) -> &'static Arc<Trace> {
    install_with_writer(logging, std::io::stderr)
}

pub fn install_with_writer<W>(
    logging: Option<tracing_subscriber::EnvFilter>,
    writer: W,
) -> &'static Arc<Trace>
where
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    static TRACE: OnceLock<Arc<Trace>> = OnceLock::new();
    TRACE.get_or_init(|| {
        let trace = Arc::new(Trace {
            started: Instant::now(),
            observations: Mutex::new(VecDeque::new()),
        });
        let filter = tracing_subscriber::filter::filter_fn(|metadata| {
            matches!(
                metadata.target(),
                "cellar::actor::effects" | "cellar::actor"
            ) && *metadata.level() <= tracing::Level::INFO
        });
        let subscriber = tracing_subscriber::registry()
            .with(LeaseLayer(trace.clone()).with_filter(filter))
            // The log filter is per layer: suppressing stderr must not suppress
            // the lease events used by the proof fixture's authority checks.
            .with(logging.map(|filter| {
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(writer)
                    .with_filter(filter)
            }));
        tracing::subscriber::set_global_default(subscriber).expect("actor fixture owns tracing");
        trace
    })
}
