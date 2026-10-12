// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

//! Effect adapters for the clean-sheet core.
//!
//! The executable owns one serial actor which is the only caller of
//! `cellar_logic::on_event`. Adapter futures never borrow core state; they send
//! versioned completion events back through its mailbox.

#[path = "execution/select.rs"]
mod tokio_select;
// The retained externally supplied daemon conformance sources previously
// ran in the binary and may name its library dependency explicitly.
#[cfg(cellar_internal_tests)]
extern crate self as cellar;
#[doc(hidden)]
pub use tokio_select::__asyncrt_select_support;

#[path = "actor/mod.rs"]
pub mod actor;
#[path = "application/mod.rs"]
pub mod application;
pub mod sdk;
#[cfg(all(test, cellar_internal_tests))]
mod conformance_core_loop_tests {
    include!(env!("CELLAR_CONFORMANCE_CORE_LOOP_TESTS"));
}
#[cfg(all(test, cellar_internal_tests))]
mod conformance_facet_failure_tests {
    include!(env!("CELLAR_CONFORMANCE_FACET_FAILURE_TESTS"));
}
pub mod assets;
#[cfg(not(cellar_internal_tests))]
#[path = "execution/domain.rs"]
pub mod asyncrt;
// The internal test flag alone selects the simulated asyncrt, so an external
// test harness built with the flag sees the same simulated world the
// in-crate suites see. `test` must not be part of the gate:
// a dependency never has it, and the harness binary builds unflagged,
// so the shipped and harness builds keep the real asyncrt.
#[cfg(cellar_internal_tests)]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
pub mod asyncrt {
    include!(env!("CELLAR_INTERNAL_ASYNCRT"));
}
#[cfg(all(test, cellar_internal_tests))]
mod asyncrt_contract_tests {
    include!(env!("CELLAR_INTERNAL_ASYNCRT_TESTS"));
}
pub mod bucket;
#[path = "cli/cell_cli.rs"]
pub mod cell_cli;
#[path = "runtime/cells/dispatch.rs"]
pub mod cell_dispatch;
#[path = "runtime/cells/host.rs"]
mod cell_host;
#[path = "runtime/cells/state.rs"]
pub mod cell_runtime;
#[path = "node/reload_certificate.rs"]
pub mod clean_reload;
#[path = "cli/cli_options.rs"]
pub mod cli_options;
#[path = "cli/cli_output.rs"]
pub mod cli_output;
pub mod container;
#[path = "node/managed.rs"]
pub mod control_plane;
#[path = "node/mod.rs"]
pub mod daemon;
pub mod dead_node_gc;
#[path = "cli/deploy/mod.rs"]
pub mod deploy;
#[path = "cli/dev.rs"]
pub mod dev;
pub mod docker;
pub mod drain_token;
#[path = "runtime/contracts.rs"]
pub mod engine_api;
pub mod env_vars;
#[path = "cli/sql_cli.rs"]
pub mod sql_cli;
#[cfg(cellar_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod fault {
    include!(env!("CELLAR_INTERNAL_SQLITE_FAULT"));
}
#[path = "runtime/cells/facet_streams.rs"]
pub(crate) mod facet_streams;
pub mod fleet;
#[path = "runtime/generation.rs"]
pub mod generation;
#[path = "runtime/invocation/channels.rs"]
pub mod host_channels;
#[path = "runtime/cells/facets/mod.rs"]
pub mod host_facets;
#[path = "transport/http/fetch.rs"]
pub(crate) mod host_fetch;
#[path = "runtime/invocation/gates.rs"]
pub mod host_gates;
// Retain host interfaces while their remaining native frontends are ported.
#[allow(dead_code)]
#[path = "runtime/invocation/context/mod.rs"]
pub mod host_io;
#[path = "runtime/workers/loader/mod.rs"]
pub mod host_loader;
#[path = "runtime/invocation/rpc.rs"]
pub(crate) mod host_rpc;
#[path = "runtime/invocation/service.rs"]
pub mod host_service;
#[path = "execution/services.rs"]
pub mod host_services;
// Retain host interfaces while their remaining native frontends are ported.
#[allow(dead_code)]
#[path = "transport/tcp/mod.rs"]
pub(crate) mod host_tcp;
#[path = "runtime/invocation/turn.rs"]
pub mod host_turn;
#[path = "transport/websocket/bindings.rs"]
pub(crate) mod host_websocket;
pub(crate) mod html_rewriter;
// Retain host interfaces while their remaining native frontends are ported.
#[allow(dead_code)]
#[path = "transport/http/streams.rs"]
pub mod http_streams;
pub mod kv_blob;
#[path = "cli/kv_cli.rs"]
pub mod kv_cli;
#[path = "runtime/workers/loader_policy.rs"]
pub mod loader_policy;
pub(crate) mod local_store;
pub mod log_options;
pub mod ltx_repl;
pub mod ltx_replication;
pub mod machine;
pub mod memory;
#[path = "cli/native_deploy/mod.rs"]
pub mod native_deploy;
#[path = "runtime/workers/native/mod.rs"]
pub(crate) mod native_worker;
#[path = "runtime/workers/worker.rs"]
pub mod worker;
// Retain host interfaces while their remaining native frontends are ported.
#[allow(dead_code)]
#[path = "runtime/workers/config/mod.rs"]
pub mod worker_config;
/// Engine-independent Rust application primitives.
pub use cellar_native as native;
#[path = "cli/blob_cli.rs"]
pub mod blob_cli;
pub(crate) mod blob_store;
#[path = "node/lifecycle.rs"]
pub mod node_lifecycle;
pub mod node_log;
#[path = "cli/operator_cell.rs"]
pub(crate) mod operator_cell;
#[doc(hidden)]
pub mod otlp;
pub mod ownership_store;
pub mod peer_auth;
pub mod peer_probe;
#[path = "runtime/workers/pool.rs"]
pub mod pool;
pub mod protocol;
pub(crate) mod queue_batching;
#[path = "cli/queue_cli.rs"]
pub mod queue_cli;
pub mod queue_policy;
pub mod replica_storage;
pub mod replication;
pub mod replication_options;
#[path = "runtime/mod.rs"]
pub mod runtime;
#[path = "runtime/policy.rs"]
pub mod runtime_policy;
#[path = "node/listeners.rs"]
pub mod startup;
pub mod storage;
pub mod telemetry;
pub mod wake;
pub mod wake_entry;
pub mod wake_format;
#[path = "transport/websocket/client.rs"]
pub mod ws_client;
#[path = "transport/websocket/registry.rs"]
pub mod ws_registry;
#[path = "transport/websocket/upgrades.rs"]
pub(crate) mod ws_tunnel;

#[cfg(all(test, cellar_internal_tests))]
mod composed_simulation {
    include!(env!("CELLAR_INTERNAL_COMPOSED_SIMULATION"));
}

#[cfg(all(test, cellar_internal_tests))]
mod simulation_reproduction {
    include!(env!("CELLAR_INTERNAL_SIMULATION_REPRODUCTION"));
}

#[cfg(all(test, cellar_internal_tests))]
mod token_lifecycle_tests {
    include!(env!("CELLAR_INTERNAL_TOKEN_PROBE"));
}

#[cfg(all(test, cellar_internal_tests))]
mod conformance_world_tests {
    include!(env!("CELLAR_CONFORMANCE_WORLD_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s2_tests {
    include!(env!("CELLAR_CONFORMANCE_WORLD_S2_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_o3_oracle {
    include!(env!("CELLAR_CONFORMANCE_O3_ORACLE_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5a_tests {
    include!(env!("CELLAR_CONFORMANCE_WORLD_S5A_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5c_tests {
    include!(env!("CELLAR_CONFORMANCE_WORLD_S5C_TESTS"));
}

#[cfg(cellar_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod conformance_sim_store {
    include!(env!("CELLAR_CONFORMANCE_SIM_STORE_TESTS"));
}

#[cfg(cellar_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod conformance_sim_cell_host {
    include!(env!("CELLAR_CONFORMANCE_SIM_CELL_HOST_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_world_s1_tests {
    include!(env!("CELLAR_CONFORMANCE_WORLD_S1_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_world_coverage {
    include!(env!("CELLAR_CONFORMANCE_WORLD_COVERAGE_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s3_tests {
    include!(env!("CELLAR_CONFORMANCE_WORLD_S3_TESTS"));
}

#[cfg(all(test, cellar_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5b_tests {
    include!(env!("CELLAR_CONFORMANCE_WORLD_S5B_TESTS"));
}

/// Completion token for a resident-isolate reservation made by the decision
/// core. Dropping the queued/running job reports that the cell is idle again;
/// the token contains no selection or lifecycle policy of its own.
pub struct CellActivityGuard {
    finish: Option<Box<dyn FnOnce() + Send>>,
}

impl CellActivityGuard {
    pub fn new(finish: impl FnOnce() + Send + 'static) -> Self {
        Self {
            finish: Some(Box::new(finish)),
        }
    }
}

impl Drop for CellActivityGuard {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish();
        }
    }
}

/// A stateless entrypoint selected for one fetch request.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerFetchEntrypoint {
    pub name: String,
    /// The caller's `ctx.props` as structured-clone bytes. An empty vector
    /// represents an omitted or `undefined` props value.
    pub props: Vec<u8>,
}

/// Resource limits selected by a Worker Loader stub for one invocation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInvocationLimits {
    pub cpu_ms: Option<u32>,
    pub sub_requests: Option<u32>,
}

/// One operation on a Worker entrypoint. A property read has no arguments,
/// while a call owns its complete receiver path and structured-clone payload.
pub enum WorkerRpcOperation {
    Get { path: Vec<String> },
    Call { path: Vec<String>, args: Vec<u8> },
}

pub enum WorkerJob {
    Fetch {
        queued_at: std::time::Instant,
        /// An entrypoint dispatcher target, or the direct default export when
        /// absent.
        entrypoint: Option<WorkerFetchEntrypoint>,
        invocation_limits: Option<WorkerInvocationLimits>,
        url: String,
        method: String,
        body: engine_api::RequestBody,
        headers: Vec<(String, String)>,
        request_id: Option<engine_api::RequestId>,
        /// Receives one completed invocation report for Dynamic Worker tails.
        /// Ordinary fetches leave this empty.
        tail_report: Option<tokio::sync::oneshot::Sender<String>>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<engine_api::HttpResponse>>,
    },
    Rpc {
        entrypoint: String,
        operation: WorkerRpcOperation,
        /// Identifies the caller's cancellation lifetime, as for fetch.
        request_id: Option<engine_api::RequestId>,
        /// The caller's `ctx.props` as structured-clone bytes, the same
        /// encoding as `args`. It is empty when the caller sent none, which no
        /// encoded value can be. A Worker Loader entrypoint or a transferred
        /// Service Binding can set it.
        props: Vec<u8>,
        invocation_limits: Option<WorkerInvocationLimits>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },
    Queue {
        queued_at: std::time::Instant,
        batch: engine_api::QueueBatch,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<engine_api::QueueDispatchResult>>,
    },
}

/// Temporary host seam required by the verbatim JS adapter. The runtime
/// adapter will construct the real shared Worker queue; lifecycle policy does
/// not move into this type.
/// Compatibility switches copied with the JS adapter. These are runtime
/// semantics, not lifecycle decisions.
pub fn worker_compat(metadata: &serde_json::Value) -> engine_api::Compat {
    let flags = metadata
        .get("compatibility_flags")
        .and_then(serde_json::Value::as_array);
    let has_flag = |name: &str| {
        flags.is_some_and(|flags| {
            flags
                .iter()
                .any(|flag| flag.as_str().is_some_and(|flag| flag == name))
        })
    };
    let date = metadata
        .get("compatibility_date")
        .and_then(serde_json::Value::as_str);
    let switch = |enable: &str, disable: &str, since: &str| {
        if has_flag(enable) {
            return true;
        }
        if has_flag(disable) {
            return false;
        }
        date.is_some_and(|date| date >= since)
    };
    engine_api::Compat {
        delete_all_deletes_alarm: switch(
            "delete_all_deletes_alarm",
            "delete_all_preserves_alarm",
            "2026-02-24",
        ),
        js_rpc: has_flag("js_rpc"),
        fetcher_get_put_delete: !switch(
            "fetcher_no_get_put_delete",
            "fetcher_has_get_put_delete",
            "2024-03-26",
        ),
        sqlite_vec: has_flag("sqlite_vec"),
        websocket_standard_binary_type: has_flag("websocket_standard_binary_type"),
        queue_json_messages: switch(
            "queue_json_messages",
            "queue_structured_messages",
            "2024-03-18",
        ),
    }
}
