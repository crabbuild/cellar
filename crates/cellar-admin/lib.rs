// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Browser management service. Authentication and fleet configuration are
//! explicit inputs; application execution remains in the existing nodes.

mod api;
mod audit;
mod auth;
mod cron;
mod events;
mod nodes;
mod raft;
mod raft_lifecycle;
mod raft_nodes;

pub use audit::{AuditStore, Operation};
pub use auth::{Permission, Principal};

use anyhow::{ensure, Result};
use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
    Router,
};
use cellar::management::FleetClient;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

#[derive(Clone, Serialize, Deserialize)]
pub struct FleetInfo {
    pub id: String,
    pub name: String,
    pub environment: String,
}

pub struct Fleet {
    pub info: FleetInfo,
    pub client: FleetClient,
}

pub struct Console {
    /// Caller-owned domain for management I/O. Keep its executor alive through shutdown.
    pub domain: Arc<cellar::sdk::ExecutionDomain>,
    pub fleets: BTreeMap<String, Fleet>,
    pub principals: Vec<Principal>,
    pub audit: AuditStore,
    pub assets: PathBuf,
    /// Explicit origins, including the external URL when behind a proxy.
    pub origins: Vec<String>,
}

pub fn router(console: Console) -> Result<Router> {
    ensure!(
        !console.principals.is_empty(),
        "configure at least one dashboard principal"
    );
    for (id, fleet) in &console.fleets {
        ensure!(
            id == &fleet.info.id && valid_id(id),
            "invalid or mismatched fleet ID"
        );
    }
    let state = Arc::new(console);
    let api = Router::new()
        .route("/fleets/{fleet}/raft/status", get(raft::status))
        .route(
            "/fleets/{fleet}/raft/evacuations/{operation}",
            get(raft_lifecycle::status),
        )
        .route(
            "/fleets/{fleet}/raft/nodes/{node}/evacuate",
            post(raft_lifecycle::evacuate).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/fleets/{fleet}/raft/nodes/{node}/prepare-removal",
            post(raft_lifecycle::prepare).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/fleets/{fleet}/raft/nodes/{node}/stop",
            post(raft_lifecycle::stop).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/fleets/{fleet}/raft/nodes/{node}/remove",
            post(raft_lifecycle::remove).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/fleets/{fleet}/raft/nodes",
            post(raft_nodes::register).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/fleets/{fleet}/raft/nodes/{node}", get(raft_nodes::status))
        .route(
            "/fleets/{fleet}/raft/nodes/{node}/qualify",
            post(raft_nodes::qualify).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/fleets/{fleet}/raft/nodes/{node}/activate",
            post(raft_nodes::activate).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/fleets/{fleet}/raft/operations",
            post(raft::begin).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/fleets/{fleet}/raft/operations/{operation}",
            get(raft::operation),
        )
        .route(
            "/fleets/{fleet}/raft/operations/{operation}/movements",
            get(raft::movements),
        )
        .route(
            "/fleets/{fleet}/raft/operations/{operation}/cancel",
            post(raft::cancel).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/session", get(api::session))
        .route(
            "/fleets/{fleet}/events/retirement-proof",
            post(events::retirement_proof),
        )
        .route(
            "/fleets/{fleet}/events/inventory",
            get(events::inventory_page),
        )
        .route(
            "/fleets/{fleet}/cells/{scope}/event-inventory",
            post(events::inventory),
        )
        .route("/fleets/{fleet}/cells/{scope}/outbox", post(events::outbox))
        .route(
            "/fleets/{fleet}/cells/{scope}/event-state",
            get(events::state),
        )
        .route(
            "/fleets/{fleet}/nodes/{node}/event-metrics",
            get(events::metrics),
        )
        .route(
            "/fleets/{fleet}/streams/{application}/{binding}/resources",
            get(events::resources),
        )
        .route(
            "/fleets/{fleet}/streams/{application}/{binding}/action",
            post(events::stream),
        )
        .route("/fleets/{fleet}/cron/preview", post(cron::preview))
        .route(
            "/fleets/{fleet}/applications/{application}/cron",
            get(cron::configuration).post(cron::update),
        )
        .route(
            "/fleets/{fleet}/applications/{application}/cron/nodes",
            get(cron::propagation),
        )
        .route(
            "/fleets/{fleet}/applications/{application}/cron/executions",
            get(cron::executions),
        )
        .route("/fleets/{fleet}/nodes", get(api::nodes))
        .route("/fleets/{fleet}/nodes/{node}", get(api::node))
        .route("/fleets/{fleet}/nodes/{node}/probe", post(api::probe))
        .route("/fleets/{fleet}/nodes/{node}/action", post(nodes::action))
        .route(
            "/fleets/{fleet}/nodes/{node}/stops/{generation}/{actor}/{operation}",
            get(nodes::receipt),
        )
        .route("/fleets/{fleet}/cells", get(api::cells))
        .route("/fleets/{fleet}/cells/{scope}", get(api::cell))
        .route("/fleets/{fleet}/applications", get(api::applications))
        .route(
            "/fleets/{fleet}/applications/{application}",
            get(api::application),
        )
        .route("/fleets/{fleet}/operations", get(api::operations))
        .route(
            "/fleets/{fleet}/resources/action",
            post(api::resource_action),
        )
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::authenticate,
        ));
    Ok(Router::new()
        .nest("/api/v1", api)
        .fallback(api::asset)
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, enter_domain)))
}

pub(crate) fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
}

async fn enter_domain(
    axum::extract::State(console): axum::extract::State<Arc<Console>>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match console.domain.run(next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({"error": "Dashboard is shutting down"})),
        )
            .into_response(),
    }
}
