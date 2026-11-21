// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A bounded allowlist for one operator server's seal failure observations.
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

const MAX_EVENTS: usize = 16;
const PHASES: &[&str] = &[
    "operator_authorization",
    "request",
    "catalog",
    "catalog_permit",
    "children",
    "retention",
    "physical_inventory",
    "physical_validation",
    "catalog_revalidate",
    "birth_accounting",
    "capacity",
    "select",
];
const CLASSES: &[&str] = &[
    "operator_error",
    "cancelled",
    "invalid",
    "corrupt",
    "conflict",
    "payload_conflict",
    "missing_root",
    "inventory_incomplete",
    "disruption_in_progress",
    "settlement_in_progress",
    "limit",
    "overloaded",
    "unconfirmed",
    "closed",
    "storage",
    "resource",
    "topology",
    "peer_unreachable",
    "peer_overloaded",
    "peer_unavailable",
    "peer_refused",
    "peer_stale",
];

#[derive(Clone, Debug, Serialize)]
pub struct Observation {
    phase: String,
    error_class: String,
    clock_available: bool,
    elapsed_ms: u64,
}

#[derive(Default)]
pub struct Trace(Mutex<VecDeque<Observation>>);

impl Trace {
    pub fn dispatch(self: &Arc<Self>) -> tracing::Dispatch {
        tracing::Dispatch::new(tracing_subscriber::registry().with(Capture(self.clone())))
    }

    pub fn snapshot(&self) -> Vec<Observation> {
        self.0.lock().unwrap().iter().cloned().collect()
    }
}

struct Capture(Arc<Trace>);

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != "cellar::raft::managed_transition" {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if !fields.event {
            return;
        }
        let (Some(phase), Some(error_class), Some(clock_available), Some(elapsed_ms)) = (
            fields.phase,
            fields.error_class,
            fields.clock_available,
            fields.elapsed_ms,
        ) else {
            return;
        };
        let mut rows = self.0 .0.lock().unwrap();
        if rows.len() == MAX_EVENTS {
            rows.pop_front();
        }
        rows.push_back(Observation {
            phase,
            error_class,
            clock_available,
            elapsed_ms,
        });
    }
}

#[derive(Default)]
struct Fields {
    event: bool,
    phase: Option<String>,
    error_class: Option<String>,
    clock_available: Option<bool>,
    elapsed_ms: Option<u64>,
}

impl Visit for Fields {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "event" => self.event = value == "seal_authorization_observation",
            "phase" if PHASES.contains(&value) => self.phase = Some(value.into()),
            "error_class" if CLASSES.contains(&value) => self.error_class = Some(value.into()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "elapsed_ms" {
            self.elapsed_ms = Some(value);
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "clock_available" {
            self.clock_available = Some(value);
        }
    }
}

#[test]
fn seal_capture_bounds_rows_and_refuses_private_fields() {
    let trace = Arc::new(Trace::default());
    let dispatch = trace.dispatch();
    let private = "private-signed-url-or-store-error";
    tracing::dispatcher::with_default(&dispatch, || {
        for elapsed_ms in 0u64..32 {
            tracing::warn!(target: "cellar::raft::managed_transition",
                event = "seal_authorization_observation", phase = "physical_inventory",
                error_class = "storage", clock_available = true, elapsed_ms,
                raw_error = private, "{private}");
        }
        tracing::warn!(target: "cellar::raft::managed_transition",
            event = "seal_authorization_observation", phase = private,
            error_class = "storage", clock_available = true, elapsed_ms = 99u64);
        tracing::warn!(target: "cellar::raft::managed_transition",
            event = "seal_authorization_observation", phase = "select",
            error_class = private, clock_available = true, elapsed_ms = 99u64);
    });
    let rows = trace.snapshot();
    assert_eq!(rows.len(), MAX_EVENTS);
    assert_eq!(rows[0].elapsed_ms, 16);
    assert_eq!(rows[MAX_EVENTS - 1].elapsed_ms, 31);
    let bytes = serde_json::to_vec(&rows).unwrap();
    assert!(!bytes
        .windows(private.len())
        .any(|part| part == private.as_bytes()));
    let rows: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for row in rows.as_array().unwrap() {
        let fields = row.as_object().unwrap();
        assert_eq!(fields.len(), 4);
        assert!(fields.keys().all(|key| matches!(
            key.as_str(),
            "phase" | "error_class" | "clock_available" | "elapsed_ms"
        )));
    }
}
