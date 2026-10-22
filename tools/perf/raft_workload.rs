//! Noncommutative reference workload for the MR-14 runner. All state changes,
//! payload bytes and conditional results cross the normal receipt/LTX boundary.
use anyhow::{ensure, Result};
use cellar::sdk::{consensus::*, Application, Output, RaftClass, Registry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use subtle::ConstantTimeEq;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct State {
    pub revision: u64,
    pub left: i64,
    pub right: i64,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Change {
    Initialize {
        database_bytes: usize,
    },
    Transfer {
        expected_revision: u64,
        value: i64,
        payload: String,
    },
    Set {
        expected_revision: u64,
        value: i64,
        payload: String,
    },
}

#[derive(Serialize, Deserialize)]
struct Transition {
    state: State,
    changed: bool,
}

const READ_SQL: &str =
    "SELECT revision, left_value, right_value, payload_sha256 FROM workload WHERE id=1";

fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<State> {
    Ok(State {
        revision: row.get(0)?,
        left: row.get(1)?,
        right: row.get(2)?,
        payload_sha256: row.get(3)?,
    })
}

// The workload uses the restricted SDK transaction in production. The SQLite
// adapter runs the identical statements in the focused close/reopen test.
trait Sql {
    fn execute(&self, statement: &str, params: impl rusqlite::Params) -> Result<usize>;

    fn execute_batch(&self, statements: &str) -> Result<()>;

    fn state(&self) -> Result<State>;
}

impl Sql for CommandTransaction<'_> {
    fn execute(&self, statement: &str, params: impl rusqlite::Params) -> Result<usize> {
        self.execute(statement, params)
    }

    fn execute_batch(&self, statements: &str) -> Result<()> {
        self.execute_batch(statements)
    }

    fn state(&self) -> Result<State> {
        self.query_row(READ_SQL, [], decode)
    }
}

#[cfg(test)]
impl Sql for rusqlite::Connection {
    fn execute(&self, statement: &str, params: impl rusqlite::Params) -> Result<usize> {
        Ok(self.execute(statement, params)?)
    }

    fn execute_batch(&self, statements: &str) -> Result<()> {
        Ok(self.execute_batch(statements)?)
    }

    fn state(&self) -> Result<State> {
        Ok(self.query_row(READ_SQL, [], decode)?)
    }
}

fn apply(sql: &impl Sql, change: Change) -> Result<Transition> {
    if let Change::Initialize { database_bytes } = change {
        ensure!(database_bytes <= 8 * 1024 * 1024, "database seed bound");
        sql.execute_batch("CREATE TABLE workload(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, left_value INTEGER NOT NULL, right_value INTEGER NOT NULL, payload_sha256 TEXT NOT NULL, payload BLOB NOT NULL); CREATE TABLE padding(bytes BLOB NOT NULL)")?;
        sql.execute(
            "INSERT INTO workload VALUES(1,0,1000000,0,?1,X'')",
            [hex::encode(Sha256::digest([]))],
        )?;
        sql.execute("INSERT INTO padding VALUES(zeroblob(?1))", [database_bytes])?;
        return Ok(Transition {
            state: sql.state()?,
            changed: true,
        });
    }
    let state = sql.state()?;
    let (expected, left, payload) = match change {
        Change::Transfer {
            expected_revision,
            value,
            payload,
        } => (expected_revision, state.left.checked_sub(value), payload),
        Change::Set {
            expected_revision,
            value,
            payload,
        } => (expected_revision, Some(value), payload),
        Change::Initialize { .. } => unreachable!(),
    };
    ensure!(payload.len() <= 64 * 1024, "payload bound");
    let Some(left) = left.filter(|left| (0..=1000000).contains(left)) else {
        return Ok(Transition {
            state,
            changed: false,
        });
    };
    if expected != state.revision {
        return Ok(Transition {
            state,
            changed: false,
        });
    }
    let revision = state
        .revision
        .checked_add(1)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("revision overflow"))?;
    sql.execute("UPDATE workload SET revision=?1,left_value=?2,right_value=?3,payload_sha256=?4,payload=?5 WHERE id=1",
        rusqlite::params![revision, left, 1000000-left, hex::encode(Sha256::digest(payload.as_bytes())), payload.as_bytes()])?;
    Ok(Transition {
        state: sql.state()?,
        changed: true,
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditional_results_and_payloads_survive_sqlite_reopen() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("state.sqlite");
        let sql = rusqlite::Connection::open(&path)?;
        apply(
            &sql,
            Change::Initialize {
                database_bytes: 4096,
            },
        )?;
        let first = apply(
            &sql,
            Change::Set {
                expected_revision: 0,
                value: 100,
                payload: "first".into(),
            },
        )?;
        assert_eq!(first.state.left, 100);
        let refused = apply(
            &sql,
            Change::Transfer {
                expected_revision: 0,
                value: 3,
                payload: "wrong".into(),
            },
        )?;
        assert!(!refused.changed);
        assert_eq!(refused.state, first.state);
        let second = apply(
            &sql,
            Change::Transfer {
                expected_revision: 1,
                value: 3,
                payload: "second".into(),
            },
        )?;
        assert_eq!(
            (second.state.left, second.state.right, second.state.revision),
            (97, 999903, 2)
        );
        drop(sql);
        let sql = rusqlite::Connection::open(&path)?;
        assert_eq!(sql.state()?, second.state);
        assert_eq!(
            sql.query_row("SELECT payload FROM workload", [], |row| row
                .get::<_, Vec<u8>>(0))?,
            b"second"
        );
        assert_eq!(
            sql.query_row("SELECT length(bytes) FROM padding", [], |row| row
                .get::<_, u64>(0))?,
            4096
        );
        Ok(())
    }
}
