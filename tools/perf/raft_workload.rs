// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Noncommutative reference workload for the MR-14 runner. All state changes,
//! payload bytes and conditional results cross the normal receipt/LTX boundary.
use crate::reference_workload::{apply, decode, Change, State, Transition, READ_SQL};
use anyhow::{ensure, Result};
use cellar::sdk::{consensus::*, Application, Output, RaftClass, Registry};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use subtle::ConstantTimeEq;

pub fn definition() -> Result<RaftClass> {
    Ok(RaftClass::new("Workload", 1, [71; 32])?
        .command("Change", [72; 32], |context, input| {
            let change = serde_json::from_slice(input)?;
            Ok(serde_json::to_vec(&apply(context.sql(), change)?)?)
        })?
        .query("Read", [73; 32], |sql, _| {
            Ok(serde_json::to_vec(&sql.query_row(READ_SQL, [], decode)?)?)
        })?
        .query("DenseAudit", [74; 32], |sql, input| {
            let block: Option<usize> = serde_json::from_slice(input)?;
            let (blocks, bytes): (u64, u64) = sql.query_row(
                "SELECT count(*),coalesce(sum(length(bytes)),0) FROM dense_padding",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let digest = if let Some(block) = block {
                use sha2::{Digest, Sha256};
                let bytes: Vec<u8> = sql.query_row(
                    "SELECT bytes FROM dense_padding WHERE block=?1",
                    [block as i64],
                    |row| row.get(0),
                )?;
                Some(hex::encode(Sha256::digest(&bytes)))
            } else {
                None
            };
            Ok(serde_json::to_vec(
                &serde_json::json!({"blocks":blocks,"bytes":bytes,"block":block,"sha256":digest}),
            )?)
        })?)
}

pub struct Api {
    pub cells: Mutex<Option<RaftCells>>,
    namespace: NamespaceId,
    command: OperationContract,
    query: OperationContract,
    audit: OperationContract,
    secret: String,
    durability: Durability,
}

impl Api {
    pub fn new(
        secret: String,
        namespace: NamespaceId,
        class: &RaftClass,
        durability: Durability,
    ) -> Result<Arc<Self>> {
        ensure!(secret.len() >= 16, "short workload bearer");
        Ok(Arc::new(Self {
            cells: Mutex::new(None),
            namespace,
            command: class
                .contract("Change")
                .ok_or_else(|| anyhow::anyhow!("missing change"))?,
            query: class
                .contract("Read")
                .ok_or_else(|| anyhow::anyhow!("missing read"))?,
            audit: class
                .contract("DenseAudit")
                .ok_or_else(|| anyhow::anyhow!("missing dense audit"))?,
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
        let cell = cells.cell(self.namespace, &format!("Workload:{id}"))?;
        let result = if method == axum::http::Method::POST {
            let change: Change = serde_json::from_slice(&body)?;
            let (client, sequence) = field("x-command-id")?
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid command id"))?;
            let id = CommandId::new(
                ClientId::new(
                    hex::decode(client)?
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("client length"))?,
                )?,
                sequence.parse()?,
            )?;
            let mut options = CommandOptions::new(id, field("x-retry-until-ms")?.parse()?);
            options.durability = self.durability;
            // Bulk population is setup work, separate from the five-second
            // foreground command budget used by timed request windows.
            if matches!(&change, Change::Populate { .. })
                || headers
                    .get("x-cellar-setup")
                    .and_then(|value| value.to_str().ok())
                    == Some("preloaded-warmup")
            {
                options.timeout = Duration::from_secs(120);
            }
            cell.command(self.command.clone(), serde_json::to_vec(&change)?, options).await.map(|reply| {
                let result = serde_json::from_slice::<Transition>(&reply.result);
                (result.map(|value| serde_json::json!({"state":value.state, "changed":value.changed})), reply.version, reply.version_token, None,
                 Some(match reply.durability { Durability::Quorum => "quorum", Durability::S3 => "s3" }))
            })
        } else {
            ensure!(
                method == axum::http::Method::GET && body.is_empty(),
                "unsupported request"
            );
            let consistency = match field("x-read-consistency")? {
                "at-least" => {
                    QueryConsistency::AtLeast(VersionToken::from_encoded(field("x-min-version")?)?)
                }
                "linearizable" => QueryConsistency::Linearizable,
                "stale" => QueryConsistency::Stale,
                _ => anyhow::bail!("unknown consistency"),
            };
            let audit = headers.get("x-cellar-dense-audit");
            let (contract, input) = if let Some(value) = audit {
                let block: Option<usize> = serde_json::from_str(value.to_str()?)?;
                (self.audit.clone(), serde_json::to_vec(&block)?)
            } else {
                (self.query.clone(), vec![])
            };
            cell.query(
                contract,
                input,
                QueryOptions {
                    consistency,
                    timeout: Duration::from_secs(5),
                    ..Default::default()
                },
            )
            .await
            .map(|reply| {
                (
                    if audit.is_some() {
                        serde_json::from_slice::<serde_json::Value>(&reply.result)
                    } else {
                        serde_json::from_slice::<State>(&reply.result)
                            .map(|state| serde_json::json!({"state":state}))
                    },
                    reply.version,
                    reply.version_token,
                    Some(reply.served_by.get()),
                    None,
                )
            })
        };
        match result {
            Ok((body, version, token, served_by, durability)) => {
                let mut body = body?;
                let object = body
                    .as_object_mut()
                    .ok_or_else(|| anyhow::anyhow!("invalid result"))?;
                object.extend(serde_json::json!({"index":version.position.index.get(), "term":version.position.term.get(),
                    "group":hex::encode(version.scope.group.bytes()), "incarnation":hex::encode(version.scope.incarnation.bytes()),
                    "version_token":token.as_str(), "served_by":served_by, "durability":durability}).as_object().unwrap().clone());
                Ok((200, body))
            }
            Err(error) => Ok((
                503,
                serde_json::json!({"error":format!("{error:?}"), "ambiguous":matches!(error, CommandFailure::Ambiguous { .. })}),
            )),
        }
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
