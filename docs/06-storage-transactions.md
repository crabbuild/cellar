# Storage, transactions and schema changes

A cell's storage belongs to its activation. Obtain it with `context.storage()`.
The SDK exposes JSON and structured values, multi-key operations, bounded lists,
lazy cursors, SQL, transactions, alarms and an explicit `sync` durability wait.
The SQL binding is a separate bound database surface; do not confuse it with
direct access to the current cell's SQLite state.

![Local atomicity and distributed coordination](diagrams/cell-boundaries.svg)

## Local atomic updates

This handler fragment atomically records a command receipt and mutation. It
uses actual `Storage` methods; integrate it into your cell's authenticated
command handling and validate the command ID before use.

```rust
let storage = context.storage()?;
let receipt_key = format!("commands/{command_id}");
let result = storage.transaction_sync(|tx| {
    if let Some(result) = tx.get_json(&receipt_key)? {
        return Ok(result);
    }
    let current = tx.get_json("count")?
        .and_then(|v| v.as_u64()).unwrap_or(0);
    let next = current.checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("counter overflow"))?;
    let result = serde_json::json!({"count": next});
    tx.put_json("count", &serde_json::json!(next))?;
    tx.put_json(&receipt_key, &result)?;
    Ok(result)
})?;
```

Store the request digest with the receipt in a real application. Reuse of one
ID with a different command must fail rather than return another command's
result. Define receipt retention longer than the supported client retry window.
Deleting deduplication state changes the replay guarantee.

Asynchronous transactions reserve root slots in call order; nested children
settle before the boundary commits or rolls back. Errors/panics roll back,
and escaped views refuse access after termination. Synchronous helpers cannot
start asynchronous transaction blocks. Use parameterized SQL and bound values
for untrusted inputs. Close or drop cursors promptly and cap scans.

## Durability and external side effects

A local SQLite commit and an externally acknowledged durable result are
different events. LTX captures committed writes, and output gates prove the
required write position and ownership. Never bypass framework egress or return
a fabricated success when a gate/proof fails. Explore
[write durability](diagrams/write-flow.svg).

For an external payment or message, propose a transactional outbox: persist a
uniquely identified intent and the state mutation together; dispatch it using
an alarm/workflow/queue; use the same idempotency key at the receiver; record
completion and reconcile uncertain outcomes. No atomic transaction spanning
SQLite and an arbitrary external service is established by this framework.
An outbox is application code, not an automatically created Cellar table.

## Migrations and data retention

Reserve `__sys_` for framework tables. Version application schemas explicitly.
For rolling releases, prefer expand → deploy compatible readers/writers →
backfill in bounded batches → observe → contract after rollback eligibility ends.
An old executable cannot safely roll back an incompatible data transformation.
Use the SQL binding's migration API where appropriate; cell-local migrations
need a tested application convention.

Bound payload sizes, command receipts, history and pending outbox rows. Record
growth rates and retention in the service contract. A small counter benchmark
does not establish large-database restore behavior. Never mutate persisted
SQLite behind a live owner: use SDK or authenticated owner-routed operator
commands. See [backup and restore](18-recovery-backup.md).

Sources: [storage](../crates/cellar/application/storage.rs),
[transactions](../crates/cellar/application/storage/transaction.rs),
[SQL binding](../crates/cellar/application/sql.rs), [LTX](../crates/cellar-ltx/README.md).
