//! Fleet/bucket adapter for the same SQL workload. The application receipt is
//! committed with the business mutation; the native host owns the output gate.
use crate::reference_workload::{apply, Change, Sql, State, READ_SQL};
use anyhow::{bail, ensure, Context as _, Result};
use cellar::application::storage::SqlValue;
use cellar::sdk::http::{Response, Url};
use cellar::sdk::{
    Application, Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;

impl Sql for Storage {
    fn execute(&self, statement: &str, params: &[rusqlite::types::Value]) -> Result<usize> {
        let params = params
            .iter()
            .map(|value| match value {
                rusqlite::types::Value::Null => SqlValue::Null,
                rusqlite::types::Value::Integer(v) => SqlValue::Integer(*v),
                rusqlite::types::Value::Real(v) => SqlValue::Real(*v),
                rusqlite::types::Value::Text(v) => SqlValue::Text(v.clone()),
                rusqlite::types::Value::Blob(v) => SqlValue::Blob(v.clone()),
            })
            .collect::<Vec<_>>();
        let mut cursor = self.sql_cursor(statement, &params)?;
        ensure!(cursor.next_raw()?.is_none(), "unexpected mutation result");
        Ok(cursor.rows_written().try_into()?)
    }

    fn state(&self) -> Result<State> {
        let rows = self.sql_cursor(READ_SQL, &[])?.to_raw_array()?;
        let [row] = rows.as_slice() else {
            bail!("missing workload row")
        };
        let [SqlValue::Integer(revision), SqlValue::Integer(left), SqlValue::Integer(right), SqlValue::Text(digest)] =
            row.as_slice()
        else {
            bail!("invalid workload row")
        };
        Ok(State {
            revision: (*revision).try_into()?,
            left: *left,
            right: *right,
            payload_sha256: digest.clone(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct Options {
    pub secret: String,
    pub run_id: String,
    /// Configured native output policy; Fleet may use a bucket proof first.
    pub policy: String,
}

impl Options {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.secret.len() >= 16
                && self.run_id.len() == 24
                && self.run_id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid benchmark identity"
        );
        ensure!(
            matches!(self.policy.as_str(), "fleet" | "bucket"),
            "invalid output policy"
        );
        Ok(())
    }

    fn contract(&self) -> &'static str {
        if self.policy == "fleet" {
            "fleet-or-bucket"
        } else {
            "bucket"
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    digest: String,
    retry_until_ms: i64,
    response: serde_json::Value,
}

struct Workload {
    node: Arc<str>,
    options: Options,
}

impl Workload {
    fn change(
        &self,
        storage: &Storage,
        body: &[u8],
        id: &str,
        retry_until_ms: i64,
        scope: &str,
    ) -> Result<serde_json::Value> {
        let change: Change = serde_json::from_slice(body)?;
        let (client, sequence) = id.split_once(':').context("invalid command identity")?;
        ensure!(
            client.len() == 32 && client.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid client identity"
        );
        let sequence: u64 = sequence.parse()?;
        ensure!(
            (1..=4096).contains(&sequence),
            "baseline receipt bound: 4096 commands per cell"
        );
        let now = cellar::asyncrt::wall_ms();
        ensure!(
            retry_until_ms > now && retry_until_ms <= now.saturating_add(300_000),
            "expired or excessive retry window"
        );
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(&change)?));
        storage.transaction_sync(|tx| {
            let key = format!("receipt:{id}");
            if let Some(value) = tx.get_json(&key)? {
                let receipt: Receipt = serde_json::from_value(value)?;
                ensure!(receipt.digest == digest && receipt.retry_until_ms == retry_until_ms, "command identity conflicts with retained receipt");
                return Ok(receipt.response);
            }
            // One client per cell and contiguous sequences bound both the
            // client floor and retained receipts. Expired IDs never execute
            // again; the fixture does not prune or replace their client floor.
            let previous = tx.get_json("client")?;
            let next = if let Some(previous) = previous {
                ensure!(previous["id"] == client, "baseline admits one client per cell");
                previous["sequence"].as_u64().filter(|value| *value <= 4096).and_then(|value| value.checked_add(1)).context("corrupt client floor")?
            } else { 1 };
            ensure!(sequence == next, "command sequence gap or expired receipt");
            let transition = apply(tx, change)?;
            let response = serde_json::json!({"state":transition.state, "changed":transition.changed,
                "durability":self.options.contract(), "scope":scope, "command_id":id,
                "sequence":sequence, "owner":self.node.as_ref(), "receipt_kind":"application-transaction"});
            tx.put_json(&key, &serde_json::to_value(Receipt { digest, retry_until_ms, response: response.clone() })?)?;
            tx.put_json("client", &serde_json::json!({"id":client,"sequence":sequence}))?;
            Ok(response)
        })
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Result<&'a str> {
    let mut values = headers
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(name));
    let value = values.next().context("missing header")?.1.as_str();
    ensure!(values.next().is_none(), "duplicate header");
    Ok(value)
}

impl Cell for Workload {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                bail!("expected HTTP")
            };
            let storage = context.storage()?;
            let path = Url::new(&request.url, None)?.pathname();
            let scope = format!("{}:{}", self.options.run_id, path);
            let value = match request.method.as_str() {
                "POST" => {
                    let id = header(&request.headers, "x-command-id")?.to_owned();
                    let retry_until_ms = header(&request.headers, "x-retry-until-ms")?.parse()?;
                    let body = request.into_body_reader(&context)?.bytes(&context).await?;
                    ensure!(body.len() <= 128 * 1024, "request body bound");
                    self.change(&storage, &body, &id, retry_until_ms, &scope)?
                }
                "GET" => {
                    // All reads route to the authoritative owner. This is not
                    // a follower read or a Raft ReadIndex/token implementation.
                    ensure!(
                        header(&request.headers, "x-read-consistency")? == "owner",
                        "baseline supports owner reads only"
                    );
                    let client = storage.get_json("client")?.context("uninitialized cell")?;
                    serde_json::json!({"state":storage.state()?, "scope":scope,
                        "sequence":client["sequence"], "owner":self.node.as_ref(), "read_contract":"owner"})
                }
                _ => bail!("expected GET or POST"),
            };
            Ok(Output::Http(
                Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

pub(crate) fn application(node: Arc<str>, options: Options) -> Result<Arc<dyn Application>> {
    options.validate()?;
    let secret = options.secret.clone();
    let registry = Registry::new(move |context, event| {
        let secret = secret.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                bail!("expected HTTP")
            };
            let token = header(&request.headers, "authorization")?
                .strip_prefix("Bearer ")
                .unwrap_or("");
            ensure!(
                bool::from(token.as_bytes().ct_eq(secret.as_bytes())),
                "unauthorized"
            );
            let path = Url::new(&request.url, None)?.pathname();
            let name = path.strip_prefix("/cells/").context("expected /cells/ID")?;
            let id: usize = name.parse()?;
            ensure!(id < 4096 && name == id.to_string(), "invalid cell id");
            let cell = context.namespace("ENTITIES")?.get_by_name(name);
            Ok(Output::Http(cell.fetch(&context, request).await?))
        })
    })
    .cell("Entity", move |_, _| {
        Ok(Arc::new(Workload {
            node: node.clone(),
            options: options.clone(),
        }))
    })?;
    Ok(Arc::new(registry))
}
