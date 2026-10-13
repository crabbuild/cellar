//! Rust KV bindings and the runtime-supplied namespace cell. Identity, schema,
//! RPC, expiry and blob commit order match the existing service. Fleet routing,
//! ownership and durability still belong to the production host.

mod wire;

use super::storage::{SqlRow, SqlValue};
use super::{
    decode_rpc, decode_rpc_result, encode_rpc, Cell, Context, HandlerFuture, Invocation, Output,
    Storage,
};
use crate::engine_api::{HttpChunkStream, HttpResponse, RpcData};
use anyhow::{anyhow, bail, ensure, Result};
use base64::Engine;
use cellar_logic::kv as policy;
use cellar_native::clone::{StructuredValue, Value as CloneValue};
use futures_util::TryStreamExt;
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Clone, Debug)]
pub struct Namespace {
    identity: Arc<str>,
}

#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    /// Unix seconds, as in the original binding. TTL takes precedence.
    pub expiration: Option<f64>,
    pub expiration_ttl: Option<f64>,
    pub metadata: Option<Value>,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum ReadType {
    #[default]
    Text,
    Json,
    Bytes,
    Stream,
}

pub enum ReadValue {
    Text(String),
    Json(Value),
    Bytes(Vec<u8>),
    Stream(HttpChunkStream),
}

pub struct WithMetadata {
    pub value: Option<ReadValue>,
    pub metadata: Option<Value>,
    /// The service currently reads strongly and has no read cache.
    pub cache_status: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    pub prefix: String,
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListedKey {
    pub name: String,
    pub metadata: Option<Value>,
    pub expiration: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListResult {
    pub keys: Vec<ListedKey>,
    pub list_complete: bool,
    pub cursor: Option<String>,
    pub cache_status: Option<String>,
}

fn now() -> i64 {
    crate::ownership_store::now_ms() as i64
}
fn error(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("KV_ERROR: {message}")
}
fn list_limit(requested: Option<i64>) -> usize {
    let maximum = crate::application::limits::current().kv.max_list_limit;
    requested
        .filter(|n| *n > 0)
        .map_or(policy::MAX_LIST_LIMIT, |n| n as usize)
        .min(maximum)
}
fn check_key(key: &str) -> Result<()> {
    ensure!(!key.is_empty(), "KV_ERROR: a key must not be empty");
    ensure!(
        key.len() <= crate::application::limits::current().kv.max_key_bytes,
        "KV_ERROR: a key is at most {} bytes, got {}",
        crate::application::limits::current().kv.max_key_bytes,
        key.len()
    );
    Ok(())
}
fn check_value(size: usize) -> Result<()> {
    ensure!(
        size <= crate::application::limits::current().kv.max_value_bytes,
        "KV_ERROR: a value is at most {} bytes, got {size}",
        crate::application::limits::current().kv.max_value_bytes
    );
    Ok(())
}
fn check_metadata(metadata: &str) -> Result<()> {
    ensure!(
        metadata.len() <= crate::application::limits::current().kv.max_metadata_bytes,
        "KV_ERROR: metadata is at most {} bytes, got {}",
        crate::application::limits::current().kv.max_metadata_bytes,
        metadata.len()
    );
    Ok(())
}
fn check_bulk(keys: &[String]) -> Result<()> {
    ensure!(
        !keys.is_empty(),
        "KV_ERROR: a bulk get needs at least one key"
    );
    ensure!(
        keys.len() <= crate::application::limits::current().kv.max_bulk_keys,
        "KV_ERROR: a bulk get takes at most {} keys, got {}",
        crate::application::limits::current().kv.max_bulk_keys,
        keys.len()
    );
    for key in keys {
        check_key(key)?;
    }
    Ok(())
}
fn expiry(now: i64, expiration: Option<f64>, ttl: Option<f64>) -> Result<Option<i64>> {
    let floor = crate::kv_blob::limits()["minExpirationTtlMs"]
        .as_i64()
        .expect("KV limits");
    if let Some(ttl) = ttl {
        let ms = (ttl * 1000.0).floor();
        ensure!(
            ms.is_finite() && ms >= floor as f64 && ms < i64::MAX as f64,
            "KV_ERROR: expirationTtl is at least {} seconds",
            floor as f64 / 1000.0
        );
        return Ok(Some(
            now.checked_add(ms as i64)
                .ok_or_else(|| error("expiration is outside timestamp range"))?,
        ));
    }
    if let Some(expiration) = expiration {
        let at = (expiration * 1000.0).floor();
        ensure!(
            at.is_finite() && at > now as f64 && at < i64::MAX as f64,
            "KV_ERROR: expiration {expiration} is in the past"
        );
        return Ok(Some(at as i64));
    }
    Ok(None)
}

fn cursor_encode(key: &str) -> String {
    hex_encode(key.as_bytes())
}
fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                DIGITS[(byte >> 4) as usize] as char,
                DIGITS[(byte & 15) as usize] as char,
            ]
        })
        .collect()
}
fn cursor_decode(cursor: &str) -> Result<String> {
    ensure!(
        cursor.len().is_multiple_of(2)
            && cursor
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "KV_ERROR: the list cursor is malformed"
    );
    let bytes = cursor
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |byte: u8| {
                if byte <= b'9' {
                    byte - b'0'
                } else {
                    byte - b'a' + 10
                }
            };
            (digit(pair[0]) << 4) | digit(pair[1])
        })
        .collect::<Vec<_>>();
    decode_text(&bytes)
}
fn decode_text(bytes: &[u8]) -> Result<String> {
    Ok(
        cellar_native::encoding::TextDecoder::new("utf-8", Default::default())?
            .decode(bytes, false)?,
    )
}
fn decode_value(bytes: Vec<u8>, kind: ReadType) -> Result<ReadValue> {
    Ok(match kind {
        ReadType::Text => ReadValue::Text(decode_text(&bytes)?),
        ReadType::Json => ReadValue::Json(serde_json::from_str(&decode_text(&bytes)?)?),
        ReadType::Bytes => ReadValue::Bytes(bytes),
        ReadType::Stream => {
            ReadValue::Stream(Box::pin(futures_util::stream::once(
                async move { Ok(bytes) },
            )))
        }
    })
}

impl Namespace {
    pub(crate) fn new(identity: String) -> Self {
        Self {
            identity: identity.into(),
        }
    }

    async fn call(
        &self,
        context: &Context,
        method: &str,
        args: StructuredValue,
    ) -> Result<StructuredValue> {
        let reply = context
            .rpc_cell(
                policy_scope(&self.identity),
                Some(policy::cell_name(&self.identity, 0)),
                method.into(),
                encode_rpc(&args)?,
            )
            .await?;
        decode_rpc_result(&reply)
    }

    async fn read(
        &self,
        context: &Context,
        keys: &[String],
        kind: ReadType,
        metadata: bool,
    ) -> Result<Vec<(String, WithMetadata)>> {
        for key in keys {
            check_key(key)?;
        }
        let reply = self
            .call(
                context,
                "__kvGet",
                StructuredValue::from_json(
                    json!([{ "keys":keys, "withMetadata":metadata, "now":now() }]),
                )?,
            )
            .await?;
        let rows = wire::array(&reply, &reply.root)?;
        ensure!(
            rows.len() == keys.len(),
            "KV_ERROR: namespace returned an invalid result count"
        );
        rows.iter()
            .zip(keys)
            .map(|(row, key)| {
                ensure!(
                    wire::string(wire::field(&reply, row, "key"))? == *key,
                    "KV_ERROR: namespace returned a different key"
                );
                let found = matches!(wire::field(&reply, row, "found"), CloneValue::Boolean(true));
                let value = if found {
                    Some(decode_value(
                        wire::bytes(&reply, wire::field(&reply, row, "value"))?,
                        kind,
                    )?)
                } else {
                    None
                };
                let metadata = wire::optional_string(wire::field(&reply, row, "metadata"))?
                    .map(|text| serde_json::from_str(&text))
                    .transpose()?;
                Ok((
                    key.clone(),
                    WithMetadata {
                        value,
                        metadata,
                        cache_status: None,
                    },
                ))
            })
            .collect()
    }

    pub async fn get(
        &self,
        context: &Context,
        key: &str,
        kind: ReadType,
    ) -> Result<Option<ReadValue>> {
        Ok(self
            .read(context, &[key.into()], kind, false)
            .await?
            .remove(0)
            .1
            .value)
    }
    pub async fn get_with_metadata(
        &self,
        context: &Context,
        key: &str,
        kind: ReadType,
    ) -> Result<WithMetadata> {
        Ok(self
            .read(context, &[key.into()], kind, true)
            .await?
            .remove(0)
            .1)
    }
    pub async fn get_many(
        &self,
        context: &Context,
        keys: &[String],
        kind: ReadType,
    ) -> Result<IndexMap<String, Option<ReadValue>>> {
        check_bulk(keys)?;
        Ok(self
            .read(context, keys, kind, false)
            .await?
            .into_iter()
            .map(|(key, value)| (key, value.value))
            .collect())
    }
    pub async fn get_many_with_metadata(
        &self,
        context: &Context,
        keys: &[String],
        kind: ReadType,
    ) -> Result<IndexMap<String, WithMetadata>> {
        check_bulk(keys)?;
        Ok(self
            .read(context, keys, kind, true)
            .await?
            .into_iter()
            .collect())
    }

    async fn put(
        &self,
        context: &Context,
        key: &str,
        bytes: Vec<u8>,
        tag: &str,
        options: PutOptions,
    ) -> Result<()> {
        check_key(key)?;
        check_value(bytes.len())?;
        let metadata = options
            .metadata
            .map(|value| serde_json::to_string(&value))
            .transpose()?;
        if let Some(metadata) = &metadata {
            check_metadata(metadata)?;
        }
        let expires = expiry(now(), options.expiration, options.expiration_ttl)?;
        let mut args = StructuredValue::from_json(
            json!([{ "entries":[{ "key":key, "value":null, "tag":tag, "metadata":metadata, "expiresAt":expires }] }]),
        )?;
        let body = wire::array(&args, &args.root)?[0].clone();
        let entry = wire::array(&args, wire::field(&args, &body, "entries"))?[0].clone();
        wire::set_bytes(&mut args, &entry, "value", bytes)?;
        self.call(context, "__kvPut", args).await?;
        Ok(())
    }
    pub async fn put_text(
        &self,
        context: &Context,
        key: &str,
        text: &str,
        options: PutOptions,
    ) -> Result<()> {
        self.put(context, key, text.as_bytes().to_vec(), "text", options)
            .await
    }
    pub async fn put_bytes(
        &self,
        context: &Context,
        key: &str,
        bytes: Vec<u8>,
        options: PutOptions,
    ) -> Result<()> {
        self.put(context, key, bytes, "bytes", options).await
    }
    /// Own and drain the source with a bound checked on every chunk. Dropping
    /// the source on refusal propagates cancellation through its host claim.
    pub async fn put_stream(
        &self,
        context: &Context,
        key: &str,
        mut stream: HttpChunkStream,
        options: PutOptions,
    ) -> Result<()> {
        check_key(key)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.try_next().await.map_err(anyhow::Error::msg)? {
            ensure!(
                chunk.len()
                    <= crate::application::limits::current()
                        .kv
                        .max_value_bytes
                        .saturating_sub(bytes.len()),
                "KV_ERROR: a value is at most {} bytes, and the stream is larger",
                crate::application::limits::current().kv.max_value_bytes
            );
            bytes.extend(chunk);
        }
        self.put(context, key, bytes, "bytes", options).await
    }
    pub async fn delete(&self, context: &Context, key: &str) -> Result<()> {
        check_key(key)?;
        self.call(
            context,
            "__kvDelete",
            StructuredValue::from_json(json!([{ "keys":[key] }]))?,
        )
        .await?;
        Ok(())
    }
    pub async fn delete_many(&self, context: &Context, keys: &[String]) -> Result<()> {
        check_bulk(keys)?;
        self.call(
            context,
            "__kvDelete",
            StructuredValue::from_json(json!([{ "keys":keys }]))?,
        )
        .await?;
        Ok(())
    }
    pub async fn list(&self, context: &Context, options: ListOptions) -> Result<ListResult> {
        let after = options
            .cursor
            .as_deref()
            .map(cursor_decode)
            .transpose()?
            .unwrap_or_default();
        let reply = self.call(context, "__kvList", StructuredValue::from_json(json!([{ "prefix":options.prefix, "limit":list_limit(options.limit), "after":after, "now":now() }]))?).await?.to_json()?;
        let complete = reply["complete"]
            .as_bool()
            .ok_or_else(|| error("invalid list response"))?;
        let keys = reply["keys"]
            .as_array()
            .ok_or_else(|| error("invalid list response"))?
            .iter()
            .map(|row| {
                Ok(ListedKey {
                    name: row["name"]
                        .as_str()
                        .ok_or_else(|| error("invalid listed key"))?
                        .into(),
                    metadata: row["metadata"]
                        .as_str()
                        .map(serde_json::from_str)
                        .transpose()?,
                    expiration: row["expiration"].as_i64().map(|ms| ms.div_euclid(1000)),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let cursor = if complete {
            None
        } else {
            Some(cursor_encode(
                &keys
                    .last()
                    .ok_or_else(|| error("incomplete list returned no keys"))?
                    .name,
            ))
        };
        Ok(ListResult {
            keys,
            list_complete: complete,
            cursor,
            cache_status: None,
        })
    }
}

fn policy_scope(identity: &str) -> String {
    crate::engine_api::kv_cell_scope(identity, 0)
}

struct Entry {
    key: String,
    value: Vec<u8>,
    tag: String,
    metadata: Option<String>,
    expires: Option<i64>,
}
struct Stored {
    entry: Entry,
    value: Option<Vec<u8>>,
    reference: Option<String>,
}
struct NamespaceCell {
    storage: Storage,
    ready: AtomicBool,
    protocol: tokio::sync::Mutex<()>,
    test_sweep_marked: AtomicBool,
    test_sweep_finished: AtomicBool,
    test_put_attempted: tokio::sync::Notify,
}

pub(crate) fn create_cell(context: &Context) -> Result<Arc<dyn Cell>> {
    Ok(Arc::new(NamespaceCell {
        storage: context.storage()?,
        ready: AtomicBool::new(false),
        protocol: tokio::sync::Mutex::new(()),
        test_sweep_marked: AtomicBool::new(false),
        test_sweep_finished: AtomicBool::new(false),
        test_put_attempted: tokio::sync::Notify::new(),
    }))
}

fn text(row: &SqlRow, key: &str) -> Result<String> {
    match row.get(key) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}
fn optional_text(row: &SqlRow, key: &str) -> Result<Option<String>> {
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        _ => text(row, key).map(Some),
    }
}
fn integer(row: &SqlRow, key: &str) -> Result<i64> {
    match row.get(key) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}
fn optional_integer(row: &SqlRow, key: &str) -> Result<Option<i64>> {
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        _ => integer(row, key).map(Some),
    }
}
fn sql_text(value: Option<String>) -> SqlValue {
    value.map(SqlValue::Text).unwrap_or(SqlValue::Null)
}
fn sql_integer(value: Option<i64>) -> SqlValue {
    value.map(SqlValue::Integer).unwrap_or(SqlValue::Null)
}

impl NamespaceCell {
    fn exec(&self, query: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        self.storage.system_sql(query, params)
    }
    fn open(&self) -> Result<()> {
        if self.ready.load(Ordering::Acquire) {
            return Ok(());
        }
        self.storage.system_sql_schema(
            "CREATE TABLE IF NOT EXISTS __sys_kv (name TEXT PRIMARY KEY,value BLOB,blob_id TEXT,size INTEGER NOT NULL,tag TEXT NOT NULL,metadata TEXT,expires_at INTEGER) WITHOUT ROWID;
             CREATE INDEX IF NOT EXISTS __sys_kv_expires ON __sys_kv(expires_at) WHERE expires_at IS NOT NULL;
             CREATE TABLE IF NOT EXISTS __sys_kv_meta(name TEXT PRIMARY KEY,value INTEGER NOT NULL) WITHOUT ROWID;"
        )?;
        self.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn blob_due(&self) -> Result<bool> {
        Ok(!self
            .exec(
                "SELECT 1 FROM __sys_kv_meta WHERE name='blob-sweep-due' AND value=1",
                &[],
            )?
            .is_empty())
    }
    fn arm_blob(&self) -> Result<()> {
        self.open()?;
        self.exec("INSERT INTO __sys_kv_meta(name,value) VALUES ('blob-sweep-due',1) ON CONFLICT(name) DO UPDATE SET value=excluded.value", &[])?;
        self.arm_before(
            now().saturating_add(
                crate::kv_blob::limits()["blobSweepMs"]
                    .as_i64()
                    .expect("KV limits"),
            ),
        )
    }
    fn arm_before(&self, deadline: i64) -> Result<()> {
        let at = self.storage.get_alarm()?;
        if at.is_none_or(|at| at <= now() || at > deadline) {
            self.storage.set_alarm(deadline)?;
        }
        Ok(())
    }
    fn rearm(&self) -> Result<()> {
        if crate::kv_blob::limits()["sweepDisabled"] == true {
            return Ok(());
        }
        let row = self
            .exec(
                "SELECT MIN(expires_at) AS next FROM __sys_kv WHERE expires_at IS NOT NULL",
                &[],
            )?
            .remove(0);
        let next = optional_integer(&row, "next")?;
        let due = self.blob_due()?;
        if next.is_none() && !due {
            return self.storage.delete_alarm();
        }
        let current = now();
        let mut deadline = next
            .map(|at| at.max(current.saturating_add(1)))
            .unwrap_or(i64::MAX);
        if due {
            deadline = deadline.min(
                current.saturating_add(
                    crate::kv_blob::limits()["blobSweepMs"]
                        .as_i64()
                        .expect("KV limits"),
                ),
            );
        }
        self.arm_before(deadline)
    }

    async fn get(
        &self,
        context: &Context,
        keys: &[String],
        metadata: bool,
        expiration: bool,
        at: i64,
    ) -> Result<StructuredValue> {
        check_bulk(keys)?;
        self.open()?;
        let mut rows = Vec::with_capacity(keys.len());
        let mut bytes = Vec::with_capacity(keys.len());
        for key in keys {
            let row = self.exec("SELECT value,blob_id,tag,metadata,expires_at FROM __sys_kv WHERE name=? AND (expires_at IS NULL OR expires_at>?)", &[key.clone().into(), at.into()])?.into_iter().next();
            let Some(row) = row else {
                rows.push(json!({ "key":key,"found":false }));
                bytes.push(None);
                continue;
            };
            let reference = optional_text(&row, "blob_id")?;
            let inline = reference.is_none();
            let value = if let Some(reference) = reference {
                match crate::native_worker::kv_blob(
                    context,
                    json!({"mode":"get","reference":reference}),
                    None,
                )
                .await?
                {
                    crate::kv_blob::BlobReply::Bytes(bytes) => bytes,
                    crate::kv_blob::BlobReply::Json(_) => {
                        return Err(error(format!(
                            "the value for {} is missing from the fleet bucket (blob {reference})",
                            serde_json::to_string(key)?
                        )))
                    }
                }
            } else {
                match row.get("value") {
                    Some(SqlValue::Blob(bytes)) => bytes.clone(),
                    _ => return Err(error("stored inline value is not bytes")),
                }
            };
            let mut entry = json!({"key":key,"found":true,"tag":text(&row,"tag")?,"value":null});
            if metadata {
                entry["metadata"] = json!(optional_text(&row, "metadata")?);
            }
            if expiration {
                entry["expiration"] = json!(optional_integer(&row, "expires_at")?);
            }
            rows.push(entry);
            bytes.push(Some((value, inline)));
        }
        let mut graph = StructuredValue::from_json(Value::Array(rows))?;
        let rows = wire::array(&graph, &graph.root)?.to_vec();
        for (row, bytes) in rows.iter().zip(bytes) {
            if let Some((bytes, inline)) = bytes {
                if inline {
                    wire::set_buffer(&mut graph, row, "value", bytes)?;
                } else {
                    wire::set_bytes(&mut graph, row, "value", bytes)?;
                }
            }
        }
        Ok(graph)
    }

    async fn put(&self, context: &Context, entries: Vec<Entry>) -> Result<()> {
        for entry in &entries {
            check_key(&entry.key)?;
            check_value(entry.value.len())?;
            if let Some(metadata) = &entry.metadata {
                check_metadata(metadata)?;
            }
        }
        if crate::kv_blob::limits()["raceSweepPut"] == true
            && entries
                .iter()
                .any(|entry| entry.value.len() > policy::MAX_INLINE_VALUE_BYTES)
        {
            ensure!(
                self.test_sweep_marked.load(Ordering::Acquire)
                    && !self.test_sweep_finished.load(Ordering::Acquire),
                "KV_ERROR: the test blob sweep did not reach its mark snapshot"
            );
            self.test_put_attempted.notify_one();
        }
        let _protocol = self.protocol.lock().await;
        self.open()?;
        for entry in &entries {
            if !self
                .exec(
                    "SELECT 1 FROM __sys_kv WHERE name=? AND blob_id IS NOT NULL",
                    &[entry.key.clone().into()],
                )?
                .is_empty()
            {
                self.arm_blob()?;
                break;
            }
        }
        let mut stored = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.value.len() <= policy::MAX_INLINE_VALUE_BYTES {
                stored.push(Stored {
                    value: Some(entry.value.clone()),
                    reference: None,
                    entry,
                });
            } else {
                let digest = hex_encode(&cellar_native::crypto::digest(
                    cellar_native::crypto::DigestAlgorithm::Hash(
                        cellar_native::crypto::HashAlgorithm::Sha256,
                    ),
                    &entry.value,
                ));
                self.arm_blob()?;
                let crate::kv_blob::BlobReply::Json(reply) = crate::native_worker::kv_blob(
                    context,
                    json!({"mode":"prepare","digest":digest}),
                    None,
                )
                .await?
                else {
                    bail!("KV_ERROR: invalid prepared blob reply")
                };
                let reference = reply["reference"]
                    .as_str()
                    .ok_or_else(|| error("invalid prepared blob reply"))?
                    .to_string();
                crate::native_worker::kv_blob(
                    context,
                    json!({"mode":"put","reference":reference}),
                    Some(entry.value.clone()),
                )
                .await?;
                if crate::kv_blob::limits()["failAfterBlobWrite"] == true {
                    bail!("KV_ERROR: CELLAR_TEST_KV_FAIL_AFTER_BLOB_WRITE");
                }
                stored.push(Stored {
                    entry,
                    value: None,
                    reference: Some(reference),
                });
            }
        }
        self.storage.transaction_sync(|_| {
            for item in &stored {
                self.exec("INSERT INTO __sys_kv(name,value,blob_id,size,tag,metadata,expires_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value,blob_id=excluded.blob_id,size=excluded.size,tag=excluded.tag,metadata=excluded.metadata,expires_at=excluded.expires_at", &[
                    item.entry.key.clone().into(), item.value.clone().map(SqlValue::Blob).unwrap_or(SqlValue::Null), sql_text(item.reference.clone()), (item.entry.value.len() as i64).into(), item.entry.tag.clone().into(), sql_text(item.entry.metadata.clone()), sql_integer(item.entry.expires)
                ])?;
            }
            Ok(())
        })?;
        if stored.iter().any(|item| item.entry.expires.is_some()) {
            self.rearm()?;
        }
        Ok(())
    }
    async fn delete(&self, keys: Vec<String>) -> Result<()> {
        let _protocol = self.protocol.lock().await;
        self.open()?;
        for key in &keys {
            if !self
                .exec(
                    "SELECT 1 FROM __sys_kv WHERE name=? AND blob_id IS NOT NULL",
                    &[key.clone().into()],
                )?
                .is_empty()
            {
                self.arm_blob()?;
                break;
            }
        }
        self.storage.transaction_sync(|_| {
            for key in &keys {
                self.exec("DELETE FROM __sys_kv WHERE name=?", &[key.clone().into()])?;
            }
            Ok(())
        })
    }
    fn list(&self, prefix: String, limit: i64, after: String, at: i64) -> Result<Value> {
        self.open()?;
        ensure!(
            (0..i64::MAX).contains(&limit)
                && limit as usize <= crate::application::limits::current().kv.max_list_limit,
            "KV_ERROR: invalid list limit"
        );
        let upper = prefix_upper(&prefix);
        let mut query = String::from("SELECT name,metadata,expires_at FROM __sys_kv WHERE ");
        query.push_str(if after.is_empty() {
            "name>=?"
        } else {
            "name>?"
        });
        let mut params = vec![if after.is_empty() {
            prefix.clone()
        } else {
            after
        }
        .into()];
        if let Some(upper) = upper {
            query.push_str(" AND name<?");
            params.push(upper.into());
        }
        if !prefix.is_empty() {
            query.push_str(" AND substr(name,1,?)=?");
            params.push((prefix.chars().count() as i64).into());
            params.push(prefix.into());
        }
        query.push_str(" AND (expires_at IS NULL OR expires_at>?) ORDER BY name LIMIT ?");
        params.push(at.into());
        params.push((limit + 1).into());
        let rows = self.exec(&query, &params)?;
        let complete = rows.len() <= limit as usize;
        let keys = rows.into_iter().take(limit as usize).map(|row| Ok(json!({"name":text(&row,"name")?,"metadata":optional_text(&row,"metadata")?,"expiration":optional_integer(&row,"expires_at")?}))).collect::<Result<Vec<_>>>()?;
        Ok(json!({"keys":keys,"complete":complete}))
    }
    fn metrics(&self, at: i64) -> Result<Value> {
        self.open()?;
        let row = self.exec("SELECT COUNT(*) AS count,COALESCE(SUM(size),0) AS bytes FROM __sys_kv WHERE expires_at IS NULL OR expires_at>?", &[at.into()])?.remove(0);
        Ok(json!({"count":integer(&row,"count")?,"bytes":integer(&row,"bytes")?}))
    }
    fn sweep(&self, at: i64, limit: i64) -> Result<usize> {
        self.open()?;
        ensure!(limit >= 0, "KV_ERROR: invalid sweep bound");
        self.storage.transaction_sync(|_| {
            let rows = self.exec("SELECT name FROM __sys_kv WHERE expires_at IS NOT NULL AND expires_at<=? ORDER BY expires_at LIMIT ?", &[at.into(),limit.into()])?;
            for row in &rows { self.exec("DELETE FROM __sys_kv WHERE name=?", &[text(row,"name")?.into()])?; }
            Ok(rows.len())
        })
    }
    async fn alarm(&self, context: &Context) -> Result<()> {
        let removed = {
            let _protocol = self.protocol.lock().await;
            self.open()?;
            let at = now();
            if !self.exec("SELECT 1 FROM __sys_kv WHERE expires_at IS NOT NULL AND expires_at<=? AND blob_id IS NOT NULL LIMIT ?", &[at.into(),(cellar_logic::sweep::BATCH_ROWS as i64).into()])?.is_empty() { self.arm_blob()?; }
            let removed = self.sweep(at, cellar_logic::sweep::BATCH_ROWS as i64)?;
            let test_race = crate::kv_blob::limits()["raceSweepPut"] == true;
            if self.blob_due()? || test_race {
                let live = self
                    .exec(
                        "SELECT blob_id FROM __sys_kv WHERE blob_id IS NOT NULL",
                        &[],
                    )?
                    .iter()
                    .map(|row| text(row, "blob_id"))
                    .collect::<Result<HashSet<_>>>()?;
                if test_race && !self.test_sweep_finished.load(Ordering::Acquire) {
                    self.test_sweep_marked.store(true, Ordering::Release);
                    self.test_put_attempted.notified().await;
                    crate::asyncrt::sleep(std::time::Duration::from_millis(100)).await;
                }
                match crate::native_worker::kv_blob(
                    context,
                    json!({"mode":"sweep","live":live}),
                    None,
                )
                .await
                {
                    Ok(_) => {
                        self.exec("DELETE FROM __sys_kv_meta WHERE name='blob-sweep-due'", &[])?;
                    }
                    Err(error) => tracing::error!(%error,"kv blob sweep failed"),
                }
                if test_race {
                    self.test_sweep_finished.store(true, Ordering::Release);
                }
            }
            removed
        };
        if removed >= cellar_logic::sweep::BATCH_ROWS {
            self.storage.set_alarm(now().saturating_add(1))
        } else {
            self.rearm()
        }
    }

    async fn rpc(&self, context: &Context, method: &str, args: &RpcData) -> Result<RpcData> {
        let graph = decode_rpc(args)?;
        let arguments = wire::array(&graph, &graph.root)?;
        let body = arguments
            .first()
            .ok_or_else(|| error("RPC has no arguments"))?;
        let field = |name| wire::field(&graph, body, name);
        let result = match method {
            "__kvGet" => {
                self.get(
                    context,
                    &names(&graph, field("keys"))?,
                    matches!(field("withMetadata"), CloneValue::Boolean(true)),
                    matches!(field("withExpiration"), CloneValue::Boolean(true)),
                    wire::number(field("now"))?,
                )
                .await?
            }
            "__kvPut" => {
                let entries = wire::array(&graph, field("entries"))?
                    .iter()
                    .map(|entry| {
                        let field = |name| wire::field(&graph, entry, name);
                        Ok(Entry {
                            key: wire::string(field("key"))?,
                            value: wire::bytes(&graph, field("value"))?,
                            tag: wire::string(field("tag"))?,
                            metadata: wire::optional_string(field("metadata"))?,
                            expires: wire::optional_number(field("expiresAt"))?,
                        })
                    })
                    .collect::<Result<_>>()?;
                self.put(context, entries).await?;
                StructuredValue {
                    root: CloneValue::Undefined,
                    objects: Vec::new(),
                }
            }
            "__kvDelete" => {
                self.delete(names(&graph, field("keys"))?).await?;
                StructuredValue {
                    root: CloneValue::Undefined,
                    objects: Vec::new(),
                }
            }
            "__kvList" => StructuredValue::from_json(self.list(
                wire::string(field("prefix"))?,
                wire::number(field("limit"))?,
                wire::string(field("after"))?,
                wire::number(field("now"))?,
            )?)?,
            "__kvMetrics" => {
                StructuredValue::from_json(self.metrics(wire::number(field("now"))?)?)?
            }
            "__kvSweep" => StructuredValue::from_json(
                json!({"removed":self.sweep(wire::number(field("now"))?,wire::number(field("limit"))?)?}),
            )?,
            _ => return Err(error(format!("namespace has no RPC method {method}"))),
        };
        match args {
            RpcData::Structured(_) => encode_rpc(&result),
            RpcData::Json(_) if matches!(result.root, CloneValue::Undefined) => {
                Ok(RpcData::Json("null".into()))
            }
            RpcData::Json(_) if method == "__kvGet" => Ok(RpcData::Json(wire::json_get(&result)?)),
            RpcData::Json(_) => Ok(RpcData::Json(serde_json::to_string(&result.to_json()?)?)),
        }
    }

    async fn operator(&self, context: &Context, body: Value) -> Result<Value> {
        let at = now();
        match body["op"].as_str() {
            Some("get") => {
                let key = js_string(body.get("key"));
                let graph = self.get(context, &[key], true, true, at).await?;
                let row = &wire::array(&graph, &graph.root)?[0];
                if !matches!(wire::field(&graph, row, "found"), CloneValue::Boolean(true)) {
                    return Ok(json!({"found":false}));
                }
                let bytes = wire::bytes(&graph, wire::field(&graph, row, "value"))?;
                let mut value = json!({"found":true,"tag":wire::string(wire::field(&graph,row,"tag"))?,"metadata":wire::optional_string(wire::field(&graph,row,"metadata"))?,"expiration":wire::optional_number(wire::field(&graph,row,"expiration"))?});
                if bytes.len() > policy::MAX_INLINE_VALUE_BYTES {
                    value["value"] = json!(base64::engine::general_purpose::STANDARD.encode(bytes));
                    value["valueEncoding"] = json!("base64");
                } else {
                    value["value"] = json!(bytes);
                }
                Ok(value)
            }
            Some(op @ ("put" | "put-base64")) => {
                let key = js_string(body.get("key"));
                check_key(&key)?;
                let bytes = if op == "put-base64" {
                    base64::engine::general_purpose::STANDARD
                        .decode(js_string(body.get("value")))
                        .map_err(error)?
                } else {
                    body["value"]
                        .as_array()
                        .ok_or_else(|| error("expected an array of bytes"))?
                        .iter()
                        .map(|value| value.as_f64().unwrap_or(0.0) as i64 as u8)
                        .collect()
                };
                check_value(bytes.len())?;
                let metadata = body
                    .get("metadata")
                    .filter(|value| !value.is_null())
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_string)
                            .ok_or_else(|| error("operator metadata must be JSON text"))
                    })
                    .transpose()?;
                if let Some(metadata) = &metadata {
                    check_metadata(metadata)?;
                }
                let expires = expiry(
                    at,
                    body["expiration"].as_f64(),
                    body["expirationTtl"].as_f64(),
                )?;
                self.put(
                    context,
                    vec![Entry {
                        key,
                        value: bytes,
                        tag: body
                            .get("tag")
                            .filter(|value| !value.is_null())
                            .map(|value| js_string(Some(value)))
                            .unwrap_or_else(|| "bytes".into()),
                        metadata,
                        expires,
                    }],
                )
                .await?;
                Ok(json!({"ok":true}))
            }
            Some("delete") => {
                let keys = body["keys"]
                    .as_array()
                    .ok_or_else(|| error("keys must be an array"))?
                    .iter()
                    .map(|value| js_string(Some(value)))
                    .collect::<Vec<_>>();
                for key in &keys {
                    check_key(key)?;
                }
                self.delete(keys).await?;
                Ok(json!({"ok":true}))
            }
            Some("list") => self.list(
                body["prefix"].as_str().unwrap_or_default().into(),
                body["limit"]
                    .as_i64()
                    .ok_or_else(|| error("invalid list limit"))?,
                body["after"].as_str().unwrap_or_default().into(),
                at,
            ),
            Some("info") => {
                let live = self.metrics(at)?;
                let total = self
                    .exec("SELECT COUNT(*) AS count FROM __sys_kv", &[])?
                    .remove(0);
                Ok(
                    json!({"live":live["count"],"bytes":live["bytes"],"stored":integer(&total,"count")?}),
                )
            }
            _ => Err(error(format!(
                "unknown operation {}",
                body.get("op")
                    .map(Value::to_string)
                    .unwrap_or_else(|| "undefined".into())
            ))),
        }
    }
    async fn fetch(&self, context: &Context, body: crate::engine_api::RequestBody) -> HttpResponse {
        let body = async {
            let mut stream = context.take_body(body)?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await.map_err(anyhow::Error::msg)? {
                bytes.extend(chunk);
            }
            serde_json::from_slice::<Value>(&bytes).map_err(anyhow::Error::from)
        }
        .await;
        let result = match body {
            Ok(body) => self.operator(context, body).await,
            Err(_) => Err(error("invalid request body")),
        };
        let (status, body) = match result {
            Ok(result) => (200, json!({"result":result})),
            Err(error) => (400, json!({"error":error.to_string()})),
        };
        HttpResponse {
            status,
            body: serde_json::to_vec(&body).expect("KV operator JSON"),
            stream: None,
            headers: vec![("content-type".into(), "application/json".into())],
            websocket: None,
            write_position: None,
            observed_position: None,
        }
    }
}

fn names(graph: &StructuredValue, value: &CloneValue) -> Result<Vec<String>> {
    wire::array(graph, value)?
        .iter()
        .map(wire::string)
        .collect()
}
fn prefix_upper(prefix: &str) -> Option<String> {
    let mut points = prefix.chars().collect::<Vec<_>>();
    while let Some(point) = points.pop() {
        if point == '\u{10ffff}' {
            continue;
        }
        let next = if point == '\u{d7ff}' {
            '\u{e000}'
        } else {
            char::from_u32(point as u32 + 1)?
        };
        points.push(next);
        return Some(points.into_iter().collect());
    }
    None
}
fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Object(_)) => "[object Object]".into(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(Some(value))
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(value) => value.to_string(),
    }
}
impl Cell for NamespaceCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Fetch(request) => {
                    Ok(Output::Http(self.fetch(&context, request.body).await))
                }
                Invocation::Alarm { .. } => {
                    self.alarm(&context).await?;
                    Ok(Output::Done)
                }
                Invocation::CellRpc { method, args } => match self
                    .rpc(&context, &method, &args)
                    .await
                {
                    Ok(reply) => Ok(Output::Rpc(reply)),
                    Err(error) if matches!(args, RpcData::Structured(_)) => {
                        Ok(Output::Rpc(RpcData::Structured(
                            cellar_native::rpc::encode_error(&error.to_string(), "Error", None)?
                                .into(),
                        )))
                    }
                    Err(error) => Err(error),
                },
                _ => bail!("KV namespace received an unsupported event"),
            }
        })
    }
}
