# Outbox, streams and subscriptions

Use a transactional outbox when a cell mutation must create a recoverable event
obligation. Use a durable stream for ordered history and independent consumers.
Use a subscription when a consumer needs a durable checkpoint and leased work.
These primitives share identities and proof boundaries, but each has a distinct
job. Their implementation is under development; the
[qualification ledger](../plans/outbox-implementation-status.md) records open gates.

## Choose the right surface

| Surface | Owns | Success establishes |
| --- | --- | --- |
| `db.tx` and `tx.emit` | Business state and a source outbox row | One database transaction; successful output proves a recoverable obligation |
| Framework dispatch | Delivery of the stored obligation to its bound stream | Proven append lets the source settle its accepted prefix |
| `Stream::read` | Bounded history inspection with an opaque cursor | A page; no lease or consumer checkpoint change |
| `Subscription::next` | One named consumer's processing authority | A leased batch or `None` after an empty poll; no checkpoint change |
| `tx.inbox(...).accept` | Deduplication of local effects | Receipt and local effect commit together |
| `Batch::ack` | Settlement of the exact leased batch | Durable checkpoint advancement or an explicit settlement result |

The outbox destination is a bound Cellar stream. Connect email, webhooks or
external brokers through a subscription consumer. External delivery needs the
receiver's idempotency key or a reconciliation protocol.

## Declare and resolve once

Define an event with a stable application schema family and version:

```rust
use cellar::sdk::event_stream::Event;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderConfirmed {
    order_id: String,
    amount_cents: i64,
}

impl Event for OrderConfirmed {
    const FAMILY: &'static str = "order-confirmed";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        &self.order_id
    }
}
```

Declare `streams` and `stream_bindings` in native application configuration.
The [runnable example](../examples/durable-streams/application.rs) shows a
`Definition`, separate business/administrative grants, and a required
`projection` subscription. Definitions provision a specific resource UUID.
A binding can select that UUID or resolve an existing catalog name at activation.
Resolved handles and stored outbox rows pin the UUID; name recreation never
redirects old work. Each receiver independently checks current durable grants.

Resolve the application binding with
`context.stream::<OrderConfirmed>("EVENTS")?`. Handles belong to their execution
domain. A declared binding, an active catalog entry and successful readiness
checks are required. Bucket-backed storage, replication and durable wake
discovery currently are required even for a single deployed node.

`Node::start` waits for resource activation while supervising the node. If the
node exits first, startup fails with its error or outcome. A completed supervisor
also fails startup when resource readiness is already ready. A reported activation
failure closes and joins the node before returning.

Application event versions identify payload schemas. All Cellar-owned protocol,
format and schema markers stay at **1** during internal development. Layout
changes require rebuilt participants and recreated development state.

## Commit the business command and obligation

This handler fragment uses the source cell's database:

```rust
let db = context.storage()?;
let stream = context.stream::<OrderConfirmed>("EVENTS")?;
db.tx(|tx| {
    tx.put_json("order", &serde_json::to_value(&order)?)?;
    tx.emit(&stream, order)?;
    Ok(())
})?;
```

Emission performs no network I/O. Rollback removes the business change and event
together; nested transactions follow the existing savepoint rules. Returning a
successful response passes the source output proof gates. The stream or consumer
may still be unavailable. An operation that updates a separate SQL binding must
emit inside that database's owning cell to share the atomic boundary.

Retained storage handles stay bound to their execution domain and activation.
Storage access and transaction commit recheck admission, including when forced
domain closure happens inside an already-entered synchronous callback. A refused
commit rolls back business changes and emitted events together. Rollback cleanup
can still use the original connection after closure; it grants no new business
or delivery authority. Local writes already executed before closure remain
subject to the ordinary durability and discovery gates.

Application commands still need stable command IDs and digest-checked receipts.
A retried business request can otherwise emit twice. Stream append deduplication
protects retries of the same stored obligation, not repeated business commands.

## Process, prove, acknowledge

Run consumers through bounded invocations, cron, workflow activities or explicitly
supervised tasks. A subscription does not install a function trigger or create a
background consumer. This handler fragment processes one leased batch:

```rust
use cellar::sdk::event_stream::BatchOptions;

let db = context.storage()?;
let stream = context.stream::<OrderConfirmed>("EVENTS")?;
let subscription = stream.subscription("projection")?;
if let Some(batch) = subscription.next(&context, BatchOptions {
    max_records: 100,
    max_bytes: 1 << 20,
    wait_ms: 0,
}).await? {
    for record in &batch.records {
        db.tx(|tx| {
            if tx.inbox("order-projection-v1")?.accept(record)? {
                let event = record.event.decode::<OrderConfirmed>()?;
                tx.put_json(&format!("orders/{}", event.order_id),
                    &serde_json::to_value(event)?)?;
            }
            Ok(())
        })?;
    }
    db.sync().await?;
    batch.ack(&context).await?;
}
```

If decoding or applying an effect fails, that transaction rolls back its inbox
receipt too. Earlier records may already have committed; retained receipts make
replaying the batch safe for those local effects. Inbox pruning ends that
protection. Sync before acknowledgment so the remote checkpoint cannot outrun
proven effects. Acknowledgment is for the entire exact batch, not a partial prefix.

Use `next_with_id` with the same fetch ID to resolve an ambiguous fetch. Fetch
IDs are 32 lowercase hexadecimal digits; save the ID before starting the call.
While its lease remains live, replay recovers the same receipt, expiry and batch;
a different fetch ID receives `LeaseBusy`. This identity is not a permanent batch
receipt: after acknowledgment or lease expiry, the same fetch ID can obtain a
fresh lease. Retained inbox receipts protect local effects when records repeat.
Keep the returned receipt unchanged when retrying an ambiguous acknowledgment. `retry`
releases work for later processing without advancing the checkpoint.
`extend_until` uses a bounded absolute expiry so a repeated extension cannot
continually add time. Expired/reset/obsolete authority must be handled explicitly.

Several workers sharing a subscription compete for one live batch. Expiry allows
another worker to receive the same records with a new receipt; it does not stop
the old worker's effects. A delayed acknowledgment or extension cannot settle or
extend that replacement lease. Reset changes the subscription incarnation even
when replay starts at the same offset, so old receipts return `SubscriptionReset`.
A replayed successful acknowledgment may return `AlreadyAcknowledged` while a
later batch is leased; that result describes only the original receipt. Keep
inbox receipts and business effects in the same stable consumer database and
processing scope across redelivery. Separate worker databases do not share inbox
deduplication; external effects still require destination idempotency.

Stream deadline decisions use a captured, persisted logical clock. Authorized
append, configuration and retirement attempts retain their time observation even
when the requested change is refused; duplicate appends retain it too. Scheduling
uses that captured time. Once a mutation observes expiry, rolling the host clock
backward cannot make that receipt live again, including after owner handoff.
Read-only status clamps its observation without advancing that clock; denied
callers cannot advance it. Exact configuration-receipt replay returns the recorded
result without a new deadline decision. Live leases protect their
batch from age and capacity reclamation; after expiry, required checkpoints
continue protecting unacknowledged history. Bounded-history subscriptions can
instead become expired and require an explicit reset after reclamation.

The SDK returns `Acknowledgment::Obsolete` for an older receipt whose range is
already behind the checkpoint. Like `AlreadyAcknowledged`, this is settled
progress, not an instruction to apply the events again.

`BatchOptions` defaults to 100 records, 1 MiB and a 30-second poll. Host policy
caps the requested budgets. Polling stops when work is available or its wait
elapses; caller cancellation can stop it earlier. The pending RPC deadline
implementation reserves the validated poll wait in addition to the host transport
budget, then uses one captured deadline across routing, retries, execution and
response collection. Deadline failure after possible submission is ambiguous.

## Handle outcomes by operation

Inspect `anyhow::Error::downcast_ref::<DeliveryError>()` even when an application
adds error context. Match `error.code`; use `code.as_str()` for the same canonical
name used in JSON. Display text is diagnostic, not a retry decision.

| Outcome | Application response |
| --- | --- |
| `InvalidRequest`, `TransactionRequired`, `WrongDomain`, `EventFamilyMismatch`, `PayloadTooLarge` | Correct input, transaction scope, handle ownership or payload |
| `OutboxFull`, `StreamFull`, `InboxFull`, `ProducerLimit`, `TooManyWaiters` | Apply backpressure and inspect the constrained resource; do not discard obligations |
| `Unavailable` | This attempt was refused before submission or never connected; follow the operation's retry contract |
| `OwnershipLost` | Resolve current authority; a prior commit may still exist |
| `Ambiguous` | Reconcile using the saved command ID, operation ID, fetch ID or exact receipt before repeating effects |
| `LeaseBusy`, `SubscriptionPaused` | Wait for eligible work or an authorized lifecycle change |
| `LeaseExpired`, `StaleReceipt`, `SubscriptionReset` | Stop treating the old receipt as current processing authority |
| `CursorExpired` | Inspect the earliest retained offset and explicitly choose whether to restart or rebuild |
| `InvalidCursor` | Use a cursor for the same resource and inspect its retained range before selecting another position |
| `BatchTooSmall` | Increase record/byte budgets within host policy; preserve the fetch ID when retrying a leased batch |
| `NotInitialized` | Verify declared bindings, resource provisioning and readiness; an identifier alone cannot provision or authorize a resource |
| `SubscriptionLimit` | Inspect live and retained subscription/lease identities and captured limits; use an explicit administrative lifecycle rather than discarding checkpoints |
| `OperationLimit` | Inspect retained operation-receipt capacity and the explicit source/resource lifecycle; preserve saved operation identities |
| `OperationUnknown` | Check the resource/incarnation and saved operation ID; follow the original guarded operation's replay contract rather than assuming business state is absent |
| `PendingDelivery` | Drain/settle obligations or follow the guarded retirement/destruction contract; ordinary reset cannot discard pending work |
| `UnsupportedProtocol` | Rebuild matching participants and recreate unsupported development state; owned markers stay at 1 with no compatibility mode |
| `Forbidden`, `ConfigConflict`, `DestinationDeleted`, `SequenceGap`, `ProducerSealed` | Inspect grants, lifecycle and producer state; operator repair may be needed |

An invalid success body after a submitted fetch or mutation is `Ambiguous`,
even if the underlying metadata validator detects an invalid field or receipt.
It cannot establish that a lease, acknowledgment, configuration or lifecycle
change failed to commit. Valid broker error branches keep their exact typed
code and metadata. Local preflight errors occur before this acceptance boundary.
An invalid configuration definition or subscription name is `InvalidRequest`;
a well-formed operation denied by the local binding is `Forbidden`. These local
refusals do not contact the destination, and a local permission check never
replaces the destination's current durable-grant check.

`DeliveryError::is_permanent()` describes whether automatic outbox delivery blocks
that lane. It is not a general SDK retry policy, and it does not mean that an
operator cannot repair the cause. No helper automatically replays ambiguous
business calls.

## Operate the whole path

Required subscriptions protect their unacknowledged history and can backpressure
producers. Bounded history can expire lagging readers. Choose this policy before
deployment, monitor backlog and lag, and define how to rebuild a consumer whose
history has expired. Correctness receipts and watermarks do not expire
automatically. Retiring resources and destroying sources require explicit controls.

The [CLI reference](23-cli-reference.md) documents authenticated, owner-routed
operations. Inspect source backlog with `cellar outbox inspect`, resource state
with `cellar stream describe`, and consumer checkpoint/lease state with
`cellar subscription describe`. These are live observations, not snapshot or
drain proofs. Save operation IDs and expected revisions for mutations; query
status after a lost response. Diagnostics omit payloads and lease tokens by
default. Fleet root metrics report coverage failures rather than inventing totals.

After repairing a blocked lane, retry against its freshly inspected revision.
Retry fences attempts already in flight: their later success or refusal cannot
clear or block the retried work. Reuse the same operation ID, reason and expected
revision when resolving an ambiguous retry response; replay returns its retained
receipt and does not schedule another attempt, even after delivery completes.
That receipt proves the retry obligation, not destination acceptance.

Start with the [executable walkthrough](../examples/durable-streams/README.md).
Its smoke test checks publication, retry, preserving restart, inbox suppression
and acknowledgment. Its process-fault harnesses retain independent ledgers and
audit before replay. Those cases do not establish the full follower, expired
owner, power, store-outage, physical-capacity or release failure matrix.
