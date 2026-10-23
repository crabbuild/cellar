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
        })?)
}

pub struct Api {
    pub cells: Mutex<Option<RaftCells>>,
    namespace: NamespaceId,
    command: OperationContract,
    query: OperationContract,
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
            cell.query(
                self.query.clone(),
                vec![],
                QueryOptions {
                    consistency,
                    timeout: Duration::from_secs(5),
                    ..Default::default()
                },
            )
            .await
            .map(|reply| {
                (
                    serde_json::from_slice::<State>(&reply.result)
                        .map(|state| serde_json::json!({"state":state})),
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
