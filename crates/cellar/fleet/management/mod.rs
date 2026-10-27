// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Management observations and owner-routed operations shared by HTTP hosts.
//!
//! This client observes fleet metadata; it never acquires cell authority or
//! opens a cell's SQLite files. Authentication of the human operator belongs
//! to the embedding management service.

mod crons;
mod event_inventory;
mod events;
pub use crons::CronConfiguration;
pub use events::{EventStreamAction, OutboxAction};
mod inventory;
mod kv;
pub use kv::{EditExpiration, KvEdit, RevisionCondition};
mod nodes;
mod operations;
mod raft;
mod raft_stop;
pub use nodes::NodeAction;
pub(crate) use nodes::NodeRequest;
pub(crate) use operations::receipt_key;
pub use operations::{StopMode, StopProof, StopReceipt};
pub use raft::{
    RaftFleetAcceptance, RaftFleetBegin, RaftFleetCancel, RaftFleetCapacityAttestation,
    RaftFleetConnection, RaftFleetControl, RaftFleetEvacuationAcceptance, RaftFleetEvacuationDebt,
    RaftFleetEvacuationPhase, RaftFleetEvacuationProgress, RaftFleetEvacuationStatus,
    RaftFleetFailure, RaftFleetFailureCode, RaftFleetMovement, RaftFleetMovements,
    RaftFleetNamespace, RaftFleetNamespaceScope, RaftFleetNodeAcceptance, RaftFleetNodeAction,
    RaftFleetNodeAdmission, RaftFleetNodeCancel, RaftFleetNodeCapacity, RaftFleetNodeDescriptor,
    RaftFleetNodeOperation, RaftFleetNodeRemove, RaftFleetNodeState, RaftFleetNodeStatus,
    RaftFleetNodeStop, RaftFleetOperation, RaftFleetOutcome, RaftFleetRegisterNode,
    RaftFleetStatus, RaftFleetVerificationBlocker, RaftFleetVerificationReason,
    RaftFleetVerificationStage,
};
pub use raft_stop::{RaftStopHandoff, RaftStopOwners, RaftStopProof};
mod routing;
mod workflows;
pub use crate::application::workflow::OperatorAction as WorkflowAction;

pub use inventory::{
    ApplicationView, BindingView, CellView, EventBindingView, NodeView, Page, PageRequest,
};

use crate::bucket::Bucket;
use crate::operator_cell::{Reachable, Subject};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

/// Explicit transport policy. Library construction never reads CLI options.
#[derive(Clone)]
pub struct FleetClient {
    bucket: Bucket,
    allow_public_peers: bool,
    raft: Option<RaftFleetControl>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueAction {
    Info,
    Peek,
    Pause,
    Resume,
    Redrive,
    Purge,
}

impl FleetClient {
    pub fn new(bucket: Bucket, allow_public_peers: bool) -> Self {
        Self {
            bucket,
            allow_public_peers,
            raft: None,
        }
    }

    /// Install trusted Raft coordination and namespace handles explicitly.
    /// Ordinary management clients retain no Raft mutation capability.
    pub fn with_raft_fleet(mut self, control: RaftFleetControl) -> Self {
        self.raft = Some(control);
        self
    }

    pub fn raft_fleet(&self) -> Option<&RaftFleetControl> {
        self.raft.as_ref()
    }

    async fn call(&self, scope: String, name: Option<&str>, body: Value) -> Result<Value> {
        Reachable::from_entrances(
            &self.bucket,
            Subject {
                noun: "resource",
                source: "dashboard",
                timeout: Duration::from_secs(30),
            },
            scope.clone(),
            name,
            self.routing_entrances(&scope).await?,
        )
        .await?
        .with_response_limit(8 * 1024 * 1024)
        .call(body)
        .await
    }

    /// SQL is authorized by the host before calling this method. Database
    /// identity comes from a declared binding, not a browser-supplied scope.
    pub async fn sql(&self, database: &str, sql: &str, readonly: bool) -> Result<Value> {
        ensure!(
            !sql.trim().is_empty() && sql.len() <= 64 * 1024,
            "SQL must contain 1–65536 bytes"
        );
        let result = self
            .call(
                crate::engine_api::sql_cell_scope(database),
                None,
                json!({"console": {"sql": sql, "readonly": readonly}}),
            )
            .await?;
        ensure!(result["count"].is_u64() && result["results"].is_array(), "node does not support the dashboard SQL console response; inspect the operation before retrying");
        Ok(result)
    }

    pub async fn sql_migrations(&self, database: &str, table: &str, after: u64) -> Result<Value> {
        ensure!(
            cellar_native::sql::valid_migrations_table(table),
            "invalid migration table"
        );
        let result = self
            .call(
                crate::engine_api::sql_cell_scope(database),
                None,
                json!({"console_migrations":{"table":table,"after":after}}),
            )
            .await?;
        ensure!(
            result["items"].is_array() && result["exists"].is_boolean() && result["table"] == table,
            "node does not support dashboard migration history"
        );
        Ok(result)
    }

    pub async fn kv_list(&self, namespace: &str, prefix: &str, after: &str) -> Result<Value> {
        ensure!(
            prefix.len() <= 512 && after.len() <= 512,
            "key filter is too long"
        );
        self.call(
            crate::engine_api::kv_cell_scope(namespace, 0),
            None,
            json!({"op": "list", "prefix": prefix, "after": after, "limit": 100}),
        )
        .await
    }

    pub async fn kv_get(&self, namespace: &str, key: &str) -> Result<Value> {
        self.call(
            crate::engine_api::kv_cell_scope(
                namespace,
                cellar_logic::kv::shard_of(key, cellar_logic::kv::SHARDS),
            ),
            None,
            json!({"op": "get", "key": key}),
        )
        .await
    }

    pub async fn kv_put(
        &self,
        namespace: &str,
        key: &str,
        bytes: Vec<u8>,
        metadata: Option<Value>,
        expiration_ttl: Option<u64>,
    ) -> Result<Value> {
        use base64::Engine;
        ensure!(
            !key.is_empty() && key.len() <= 512,
            "key must contain 1–512 bytes"
        );
        ensure!(
            bytes.len() <= 1024 * 1024,
            "dashboard values are limited to 1 MiB"
        );
        let large = bytes.len() > cellar_logic::kv::MAX_INLINE_VALUE_BYTES;
        self.call(
            crate::engine_api::kv_cell_scope(namespace, cellar_logic::kv::shard_of(key, cellar_logic::kv::SHARDS)),
            None,
            json!({
                "op": if large { "put-base64" } else { "put" },
                "key": key,
                "value": if large { Value::String(base64::engine::general_purpose::STANDARD.encode(&bytes)) } else { json!(bytes) },
                "tag": "bytes", "metadata": metadata.map(|v| v.to_string()),
                "expirationTtl": expiration_ttl,
            }),
        )
        .await
    }

    pub async fn kv_delete(&self, namespace: &str, key: &str) -> Result<Value> {
        self.call(
            crate::engine_api::kv_cell_scope(
                namespace,
                cellar_logic::kv::shard_of(key, cellar_logic::kv::SHARDS),
            ),
            None,
            json!({"op": "delete", "keys": [key]}),
        )
        .await
    }

    pub async fn queue(&self, queue: &str, action: QueueAction, limit: u32) -> Result<Value> {
        ensure!(
            (1..=100).contains(&limit),
            "limit must be between 1 and 100"
        );
        self.call(
            crate::engine_api::queue_cell_scope(queue),
            Some(queue),
            json!({"op": action, "limit": limit, "console": true}),
        )
        .await
    }

    /// Only a signed probe establishes current reachability. A live lease is
    /// advisory observation and must not be presented as that proof.
    pub async fn probe(&self, node: &str) -> Result<Value> {
        ensure!(valid_node_id(node), "invalid node ID");
        let lease = crate::fleet::live_node_lease(&self.bucket, node)
            .await?
            .ok_or_else(|| anyhow::anyhow!("node lease expired"))?;
        let address = crate::startup::parse_advertise(&lease.addr)?;
        ensure!(
            self.allow_public_peers || !address.is_public_ip(),
            "public peer addresses are disabled"
        );
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let auth = crate::peer_auth::PeerAuth::new(
            crate::peer_auth::load_existing(&self.bucket).await?,
            "dashboard",
        )?;
        crate::peer_probe::probe(&http, &lease, &auth).await?;
        Ok(
            json!({"node": node, "generation": lease.generation, "reachable": true, "observed_at": crate::ownership_store::now_ms()}),
        )
    }
}

fn valid_node_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}
