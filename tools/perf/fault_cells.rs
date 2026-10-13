//! Test application: atomic operation receipts distinguish ambiguous commits.
//! Idempotency is application policy, not a framework delivery guarantee.

use anyhow::{ensure, Result};
use cellar::sdk::http::{Response, Url};
use cellar::sdk::{Application, Cell, Context, HandlerFuture, Invocation, Output, Registry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

mod deployment;
mod embedded;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Receipt {
    sha256: String,
    ordinal: u64,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct State {
    writes: u64,
    receipts: BTreeMap<String, Receipt>,
}

impl State {
    fn apply(&mut self, operation_id: &str, payload: &str) -> Result<(Receipt, bool)> {
        ensure!(
            !operation_id.is_empty()
                && operation_id.len() <= 128
                && operation_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
            "invalid operation ID"
        );
        ensure!(payload.len() <= 65_536, "payload too large");
        let sha256 = format!("{:x}", Sha256::digest(payload.as_bytes()));
        if let Some(receipt) = self.receipts.get(operation_id) {
            ensure!(
                receipt.sha256 == sha256,
                "operation ID reused with different payload"
            );
            return Ok((receipt.clone(), false));
        }
        ensure!(
            self.receipts.len() < 4_096,
            "test receipt capacity exceeded"
        );
        let ordinal = self
            .writes
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("counter overflow"))?;
        let receipt = Receipt { sha256, ordinal };
        self.receipts
            .insert(operation_id.to_owned(), receipt.clone());
        self.writes = ordinal;
        Ok((receipt, true))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    operation_id: String,
    payload: String,
    #[serde(default)]
    hold_ms: u64,
}

struct Entity {
    node: Arc<str>,
}

impl Cell for Entity {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("fault cells accept HTTP requests");
            };
            let storage = context.storage()?;
            let value = match request.method.as_str() {
                "POST" => {
                    let body = request.into_body_reader(&context)?.bytes(&context).await?;
                    let write: Write = serde_json::from_slice(&body)?;
                    ensure!(write.hold_ms <= 30_000, "hold exceeds fixture budget");
                    let (receipt, inserted) = storage.transaction_sync(|transaction| {
                        let mut state = transaction
                            .get_json("state")?
                            .map(serde_json::from_value)
                            .transpose()?
                            .unwrap_or_default();
                        let result = State::apply(&mut state, &write.operation_id, &write.payload)?;
                        transaction.put_json("state", &serde_json::to_value(&state)?)?;
                        Ok(result)
                    })?;
                    if write.hold_ms > 0 {
                        // This is diagnostic evidence of a local transaction,
                        // deliberately NOT an output-gate acknowledgement.
                        cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(
                            format_args!("{}", serde_json::json!({
                                "event": "fault_write_applied", "operation_id": write.operation_id,
                                "node": self.node.as_ref(), "inserted": inserted,
                                "hold_ms": write.hold_ms,
                            })),
                        )?;
                        cellar::asyncrt::sleep(Duration::from_millis(write.hold_ms)).await;
                    }
                    serde_json::json!({"operation_id": write.operation_id, "receipt": receipt,
                                      "node": self.node.as_ref()})
                }
                "GET" => {
                    let state: State = storage
                        .get_json("state")?
                        .map(serde_json::from_value)
                        .transpose()?
                        .unwrap_or_default();
                    serde_json::json!({"writes": state.writes, "receipts": state.receipts,
                                      "node": self.node.as_ref(),
                                      "epoch": context.identity().ok_or_else(|| anyhow::anyhow!("missing cell identity"))?.epoch})
                }
                _ => anyhow::bail!("expected GET or POST"),
            };
            Ok(Output::Http(
                Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

fn application(node: Arc<str>) -> Result<Arc<dyn Application>> {
    let registry = Registry::new(|context, event| {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("expected HTTP")
            };
            let url = Url::new(&request.url, None)?;
            let name = url
                .pathname()
                .strip_prefix("/cells/")
                .filter(|name| !name.is_empty())
                .ok_or_else(|| anyhow::anyhow!("expected /cells/NAME"))?
                .to_owned();
            let entity = context.namespace("ENTITIES")?.get_by_name(&name);
            Ok(Output::Http(entity.fetch(&context, request).await?))
        })
    })
    .cell("Entity", move |_, _| {
        Ok(Arc::new(Entity { node: node.clone() }))
    })?;
    Ok(Arc::new(registry))
}

fn main() -> Result<()> {
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
        "{}",
        serde_json::json!({"event": "fault_receipt_protocol", "version": 1})
    ))?;
    embedded::run(deployment::graph(), application)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_preserves_original_receipt_and_counter_after_serialization() {
        let mut state = State::default();
        let (first, inserted) = state.apply("operation-1", "first payload").unwrap();
        assert!(inserted);
        state.apply("operation-2", "second payload").unwrap();
        let mut state: State =
            serde_json::from_value(serde_json::to_value(state).unwrap()).unwrap();
        let (replayed, inserted) = state.apply("operation-1", "first payload").unwrap();
        assert!(!inserted);
        assert_eq!(replayed, first);
        assert_eq!(state.writes, 2);
        assert_eq!(state.receipts.len(), 2);
    }

    #[test]
    fn conflicting_retry_cannot_mutate_receipt_or_counter() {
        let mut state = State::default();
        state.apply("operation-1", "first payload").unwrap();
        let before = serde_json::to_value(&state).unwrap();
        assert!(state.apply("operation-1", "different payload").is_err());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
    }

    #[test]
    fn invalid_ids_and_excess_payloads_do_not_create_operations() {
        let mut state = State::default();
        for id in ["", "a b", "../id", "日本", &"x".repeat(129)] {
            assert!(state.apply(id, "payload").is_err());
        }
        assert!(state.apply("valid", &"x".repeat(65_537)).is_err());
        assert_eq!(state.writes, 0);
        assert!(state.receipts.is_empty());
    }
}
