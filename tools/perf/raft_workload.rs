// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Noncommutative reference workload for the MR-14 runner. All state changes,
//! payload bytes and conditional results cross the normal receipt/LTX boundary.
use crate::reference_workload::{apply, decode, Change, State, Transition, READ_SQL};
use crate::{counter, raft_groups::Layout};
use anyhow::{ensure, Result};
use cellar::sdk::{consensus::*, Application, Output, RaftClass, Registry};
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use subtle::ConstantTimeEq;

pub fn definition() -> Result<RaftClass> {
    Ok(RaftClass::new("Workload", 1, [71; 32])?
        // Business state is populated by the explicitly journaled Initialize
        // command. Enrollment still needs a replicated empty SQLite history.
        .initializer([74; 32], |_, _| Ok(()))?
        .command("Change", [72; 32], |context, input| {
            let change = serde_json::from_slice(input)?;
            Ok(serde_json::to_vec(&apply(context.sql(), change)?)?)
        })?
        .query("Read", [73; 32], |sql, _| {
            Ok(serde_json::to_vec(&sql.query_row(READ_SQL, [], decode)?)?)
        })?)
}

pub struct Api {
    pub cells: Mutex<Option<RaftCells>>,
    layout: Arc<Layout>,
    next_query: AtomicU64,
    command: OperationContract,
    query: OperationContract,
    secret: String,
    durability: Durability,
}

impl Api {
    pub fn new(
        secret: String,
        layout: Arc<Layout>,
        class: &RaftClass,
        durability: Durability,
    ) -> Result<Arc<Self>> {
        ensure!(secret.len() >= 16, "short workload bearer");
        Ok(Arc::new(Self {
            cells: Mutex::new(None),
            layout,
            next_query: AtomicU64::new(1),
            command: class
                .contract("Change")
                .ok_or_else(|| anyhow::anyhow!("missing change"))?,
            query: class
                .contract("Read")
                .ok_or_else(|| anyhow::anyhow!("missing read"))?,
            secret,
            durability,
        }))
    }

    async fn request(
        &self,
        id: usize,
        method: axum::http::Method,
        headers: axum::http::HeaderMap,
        body: axum::body::Bytes,
    ) -> Result<(u16, serde_json::Value)> {
        for name in [
            "authorization",
            "x-command-id",
            "x-retry-until-ms",
            "x-read-consistency",
            "x-min-version",
            "x-read-policy",
            "x-query-timeout-ms",
            "x-query-members",
            "x-query-attempt-ms",
            "x-leader-fallback",
        ] {
            ensure!(
                headers.get_all(name).iter().count() <= 1,
                "duplicate header"
            );
        }
        let field = |name: &str| -> Result<&str> {
            headers
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("missing header"))?
                .to_str()
                .map_err(Into::into)
        };
        let token = field("authorization")?
            .strip_prefix("Bearer ")
            .unwrap_or("");
        ensure!(
            bool::from(token.as_bytes().ct_eq(self.secret.as_bytes())),
            "unauthorized"
        );
        let cells = self
            .cells
            .lock()
            .map_err(|_| anyhow::anyhow!("binding poisoned"))?
            .clone()
            .ok_or_else(|| anyhow::anyhow!("not ready"))?;
        let cell = cells.cell(self.layout.history(id)?)?;
        if method == axum::http::Method::POST {
            let change: Change = serde_json::from_slice(&body)?;
            let (client, sequence) = field("x-command-id")?
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid command id"))?;
            let command = CommandId::new(
                ClientId::new(
                    hex::decode(client)?
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("client length"))?,
                )?,
                sequence.parse()?,
            )?;
            let mut options = CommandOptions::new(command, field("x-retry-until-ms")?.parse()?);
            options.durability = self.durability;
            return match cell
                .command(&self.command, serde_json::to_vec(&change)?, options)
                .await
            {
                Ok(reply) => {
                    let value: Transition = serde_json::from_slice(&reply.result)?;
                    Self::complete(
                        serde_json::json!({"state":value.state,"changed":value.changed}),
                        reply.version,
                        reply.witness,
                        reply.version_token,
                        None,
                        None,
                        Some(reply.durability),
                    )
                }
                Err(error) => Ok((
                    503,
                    serde_json::json!({"error":format!("{:?}",error.cause),
                    "ambiguous":error.ambiguous, "command":format!("{:?}",error.command)}),
                )),
            };
        }
        ensure!(
            method == axum::http::Method::GET && body.is_empty(),
            "unsupported request"
        );
        // Reference traces must state their read contract. Every optional read
        // control uses the same strict normalization as the native/Axum counter.
        field("x-read-consistency")?;
        let headers = headers
            .iter()
            .map(|(k, v)| Ok((k.as_str().to_owned(), v.to_str()?.to_owned())))
            .collect::<Result<Vec<_>>>()?;
        let options = counter::read_options(&headers)?;
        let sequence = self
            .next_query
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map_err(|_| anyhow::anyhow!("query identity exhausted"))?;
        let mut digest = Sha256::new();
        digest.update(b"cellar-perf-reference-query-1\0");
        digest.update(cells.identity().process.bytes());
        digest.update(sequence.to_be_bytes());
        let invocation = ProposalId::new(digest.finalize()[..16].try_into()?)?;
        match cell.query(invocation, &self.query, vec![], options).await {
            Ok(reply) => Self::complete(
                serde_json::json!({"state":serde_json::from_slice::<State>(&reply.result)?}),
                reply.version,
                reply.witness,
                reply.version_token,
                Some(reply.served_by.get()),
                Some(reply.served_by_node.node),
                None,
            ),
            Err(error) => Ok((
                503,
                serde_json::json!({"error":format!("{error:?}"),"ambiguous":false}),
            )),
        }
    }

    fn complete(
        mut body: serde_json::Value,
        version: cellar_logic::cell_group::CellVersion,
        witness: cellar_logic::cell_group::CommitWitness,
        token: CellVersionToken,
        served_by: Option<u64>,
        served_by_node: Option<NodeId>,
        durability: Option<Durability>,
    ) -> Result<(u16, serde_json::Value)> {
        let object = body
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("invalid result"))?;
        let metadata = serde_json::json!({"index":witness.position.index.get(),"term":witness.position.term.get(),
            "group":hex::encode(witness.group.group.bytes()),"group_incarnation":hex::encode(witness.group.incarnation.bytes()),
            "incarnation":hex::encode(version.history.incarnation.bytes()),"cell":version.history.cell.as_str(),
            "sequence":version.sequence.get(),"version_token":token.as_str(),"served_by":served_by,
            "served_by_node":served_by_node.map(|n|hex::encode(n.bytes())),
            "durability":durability.map(|d|match d { Durability::Quorum=>"quorum",Durability::S3=>"s3" })});
        object.extend(
            metadata
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("invalid metadata"))?
                .clone(),
        );
        Ok((200, body))
    }
}

pub fn application(class: RaftClass) -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
    ))
}

pub fn router(api: Arc<Api>) -> axum::Router {
    axum::Router::new()
        .route(
            "/cells/{id}",
            axum::routing::any(
                move |axum::extract::Path(id): axum::extract::Path<usize>,
                      method: axum::http::Method,
                      headers: axum::http::HeaderMap,
                      body: axum::body::Bytes| {
                    let api = api.clone();
                    async move {
                        let (status, body) = api
                            .request(id, method, headers, body)
                            .await
                            .unwrap_or_else(|error| {
                                (400, serde_json::json!({"error":error.to_string()}))
                            });
                        (
                            axum::http::StatusCode::from_u16(status).unwrap(),
                            axum::Json(body),
                        )
                    }
                },
            ),
        )
        .layer(axum::extract::DefaultBodyLimit::max(128 * 1024))
}
