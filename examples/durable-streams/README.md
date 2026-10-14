# Transactional outbox, durable stream and consumer inbox

Confirm an order and retain its delivery obligation in one business transaction.
Process the event in a separate projection cell, prove the inbox receipt and
effect, and then acknowledge its subscription batch. This example uses the new
framework stream primitive; the older
[queue-backed outbox recipe](../transactional-outbox/README.md) implements its
own publication loop instead.

The event primitive remains under implementation. This example demonstrates the
implemented SDK and one private bucket-backed node. It does not certify a
production fleet or the complete crash/ownership/response-loss matrix; see the
[implementation status](../../plans/outbox-implementation-status.md).

## How it works

[application.rs](application.rs) defines `OrderConfirmed`, a typed `Event` whose
family/version identify its payload schema and whose key groups this order's
events. The resource has an explicit incarnation UUID. Definition and binding
objects provision a required `projection` subscription and separate business
and management grants. Deployment bindings can supply an explicit UUID or omit
`resource.id` to resolve the current catalog name during activation.

For example, a native `stream_bindings` entry can select the provisioned name:

```json
{
  "environment": "EVENTS",
  "resource": {
    "tenant": "example",
    "application": "stream-example",
    "name": "orders",
    "family": "order-confirmed"
  },
  "principal": "stream-example",
  "grant": "business",
  "publish": true,
  "read": true,
  "subscriptions": ["projection"]
}
```

`BindingConfig` is the deployment selector; `Binding` and `Resource` remain
resolved runtime identities. Rust hosts convert an existing `Binding` with
`.into()` and can set the resulting `resource.id` to `None` for name lookup.
The catalog must already contain an active resource and independently authorize
the grant. Name lookup does not provision a resource. Activation also checks the
exact UUID at the catalog and partition before admitting work. Each generation
pins that UUID; recreating `orders` cannot redirect a retained stream handle or
stored outbox lane. A new generation may resolve its successor, and emission with
an old handle is refused. Resource definitions still declare their creation UUID.

Each order has a named `Order` cell. Its `db.tx` stores the business record and
calls `tx.emit(&events, event)` together. A repeated request with the same amount
returns the existing order; changed input conflicts. No network request occurs
inside the transaction. Framework dispatch proves the source obligation and
appends it asynchronously. A successful business response establishes durable
state and a recoverable obligation, not completed delivery.

The `Projection` cell calls `subscription.next` with explicit batch/byte budgets
and zero wait. For every record, one transaction records
`tx.inbox("order-projection-v1").accept(record)` and increments local totals.
Matching retained receipts suppress repeated effects. `db.sync().await` proves
those effects before `batch.ack` advances the remote checkpoint. This protects
local effects while receipts remain retained; external side effects need their
own idempotency. No automatic inbox pruning runs here.

`POST /projection/retry` commits/proves effects but releases the batch for
redelivery without acknowledging it. The smoke test stops the process at that
point and checks that the reopened inbox suppresses those effects. This is
explicit retry plus clean restart, not simulated crash or actual response loss.
The projection is driven by bounded HTTP requests; this recipe does not install
a background consumer. Required retention can eventually backpressure producers
if the projection stops making progress.

[main.rs](main.rs) owns host/core executors, listeners, clients and an injected
`NodeFleet`. Its private SQLite-backed object store retains replicated data, logs,
ownership and durable wakes. It persists a randomly generated fleet secret through
the framework bootstrap API and reuses it after restart. The node directory lock
guards the process. This differs from an SQLite-only node without a bucket,
which currently refuses deployed stream bindings.

Both listeners bind loopback. Public routes have no authentication in this local
recipe. The host requests preserving shutdown and requires complete ingress,
application, cell and pending-dispatch drain reports before returning. Both
caller-owned executors remain alive through that join.

## Run end to end

Prerequisites: the repository's Rust toolchain, native C/C++ build tools and curl.
Run from the repository root and follow [AGENTS.md](../../AGENTS.md) for a private
Cargo target directory. Start with a new tutorial data directory:

```sh
cargo run -p cellar --bin cellar-example-streams --locked -- \
  examples/.cellar/streams-walkthrough 9876
```

The host prints `{"ready":"http://127.0.0.1:9876"}` when resource activation is
complete. Stop another example occupying that port or supply another port;
`0` chooses an available port and prints its actual address. In another terminal:

```sh
curl -sS -w '\n' -X POST 'http://127.0.0.1:9876/orders/a?amount_cents=1200'
curl -sS -w '\n' http://127.0.0.1:9876/orders/a
curl -sS -w '\n' http://127.0.0.1:9876/events
curl -sS -w '\n' -X POST http://127.0.0.1:9876/projection/retry
curl -sS -w '\n' http://127.0.0.1:9876/subscription
```

The first POST returns status `created` and the stored order with HTTP 201.
Repeating it returns `duplicate` with HTTP 200; another amount for `a` returns
`conflict` with HTTP 409. Negative/oversized amounts and invalid names return
HTTP 400. The order inspection's `outbox.pending_rows` reaches `"0"` once delivery
settles. Poll `/events` until it contains the order before requesting processing.
The stream read includes an opaque `cursor`; supply it as `/events?cursor=TOKEN`
to resume. History expiration is a typed `CursorExpired` result.

The retry response reports `fresh_effects: 1` and
`projection: {"orders":1,"total_cents":1200}`. Its subscription checkpoint remains
`"0"`. Stop with Ctrl-C or SIGTERM, check the final stopped/drain report, and run
the same command with the same directory again. Then:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/projection
curl -sS -w '\n' -X POST http://127.0.0.1:9876/projection/process
curl -sS -w '\n' http://127.0.0.1:9876/subscription
```

The projection retains the same totals. Processing reports `acknowledged` with
`fresh_effects: 0`: delivery repeats, but its committed inbox receipt suppresses
the local effect. The checkpoint advances to `"1"`. Another process request
reports `empty`. Additional orders produce additional effects normally.

## Verify and clean up

Build and run the actual executable smoke test, which uses temporary data and
available ports and verifies both complete shutdown reports:

```sh
cargo build -p cellar --bin cellar-example-streams --locked
python3 examples/durable-streams/smoke.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams"
```

It checks duplicate/conflicting producer requests, two-event delivery, resumable
reads, effect-before-retry, a preserving restart, duplicate suppression before
acknowledgment, empty fetch and a new event after restart. Expected final output
starts `PASS durable streams`. Temporary smoke state is removed automatically.
For the walkthrough, stop the host first and remove only its tutorial data
directory when its retained orders, projection and fleet state are no longer needed.

## Process-failure qualification

The separate [faults.py](faults.py) harness runs the actual compiled executable
with `--fault-harness` against a fresh private directory. This opt-in mode uses
a one-record required stream, stores immutable source/effect receipts, and holds
one consumer request after `db.sync` has proven effects but before acknowledgment.
All ordinary ownership, discovery, replication, lease and output gates still run.

The measuring client fsyncs its own command plan, payload digests, acknowledgments,
accepted envelope digests and snapshots. A loopback proxy consumes a real source
response and closes the client socket. The harness then observes two actual
SIGKILL exits: one with an acknowledged source obligation pending behind stream
capacity, and one with proven consumer effects before acknowledgment. Each
restart is audited before ambiguous source retry or consumer redelivery. The
second restart must recover the existing lease and checkpoint; redelivery waits
for that lease's original expiry and must add no repeated effect. After progress
reclaims acknowledged history, archived effect/acceptance receipts continue to
account for the intentionally removed event. Final totals must equal distinct
receipts and the final process must report complete preserving shutdown.

```sh
python3 -m unittest discover -s examples/durable-streams -p 'test_*.py'
python3 examples/durable-streams/faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --output /tmp/cellar-stream-fault-evidence
python3 examples/durable-streams/faults.py \
  --audit /tmp/cellar-stream-fault-evidence/ledger.jsonl
```

The output directory must not exist. It retains logs, private bucket/state,
the measuring ledger and a summary with the executable digest. The audit-only
command rechecks all four original snapshots, distinct kill stages, response
loss, proven consumer boundary and replay ordering without sending requests.

The separate [before_send.py](before_send.py) case holds the source's real input
gate before committing the command. After `db.sync`, it exposes a proven command
receipt with one pending obligation and zero dispatch attempts/generation. The
independent client records that boundary, observes SIGKILL and copies the killed
source's database/WAL family before restarting any framework process. A read of
only that forensic copy verifies zero attempts after the kill and archives the
original envelope digest. Recovery is audited before ambiguous command replay
or consumer processing; replay returns `duplicate`, the accepted envelope matches
the killed image and the consumer applies one effect with checkpoint 1.

```sh
python3 examples/durable-streams/before_send.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --output /tmp/cellar-stream-before-send-evidence
python3 examples/durable-streams/before_send.py \
  --audit /tmp/cellar-stream-before-send-evidence/ledger.jsonl
```

Audit-only mode rechecks the original boundary, actual kill, forensic SQLite
image/WAL hashes, both saved runtime snapshots and replay ordering. SQLite's
mutable SHM read marks are not authenticated durable bytes. No client source
acknowledgment is invented: this command was proven by the held handler but its
HTTP response was interrupted. Ordinary handler/input-gate deadlines remain in
force; the harness must reach the boundary and kill promptly.

These qualify single-node process loss (including before first dispatch) and
source HTTP-response loss with a persistent SQLite-backed bucket. They do not
qualify destination append/ack response loss,
separate producer/destination/consumer hosts, follower or network/bucket faults,
power loss, the complete cut-point matrix, or capacity soaks. Those release
requirements remain in the [design](../../plans/002-outbox-streams-subscriptions.md).


## Independent-process destination recovery

[fleet_faults.py](fleet_faults.py) starts destination, producer and consumer as
three independent loopback processes with separate node directories and one
private SQLite-backed bucket. The opt-in `--fleet-fixture FILE` host mode captures
bounded node/bucket/peer/advertisement/definition settings. It preserves the
ordinary actor, handler, lease, replication and shutdown policies. Test-only
source/effect diagnostics identify the serving process; normal routes retain
the walkthrough behavior.

A peer proxy forwards the original authenticated tunnel and exact framing. It
holds a complete successful append reply through an independent command/source/
destination audit, then the harness SIGKILLs the destination. Automatic append
retries are blocked while the destination restarts; a saved recovery audit must
prove that the accepted envelope still exists before retry is released. Retried
request and acceptance bytes match exactly. A consumer with a stale pooled
connection is audited before another processing attempt, and an installed lease
keeps its original expiry. Inbox redelivery adds no repeated effect; the final
checkpoint is 1. The replacement destination and both surviving processes must
report complete preserving shutdown. Saved audits require the original process
IDs, kill/restart/retry order, command acknowledgments, captured byte digests and
final shutdown reports.

Supply `--rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc` to run the same boundary
against a fresh private Docker RustFS S3 server. The driver creates a random
bucket and credentials, publishes only a random loopback port and removes only
its own container after all nodes have stopped. Credentials reach explicitly
constructed host clients through the fixture environment and are omitted from
configuration and ledgers. Conditional ownership writes use a separate no-retry
client. Saved evidence records the actual store image, backend and cleanup order.
The SQLite mode remains the default. Use a pinned image digest for reproducible
qualification and preserve the binary digest with the ledger.

```sh
python3 examples/durable-streams/fleet_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --output /tmp/cellar-independent-stream-evidence
python3 examples/durable-streams/fleet_faults.py \
  --audit /tmp/cellar-independent-stream-evidence/ledger.jsonl
```

This proves one independent-process destination append/response-loss cut point
on a single machine, with the selected SQLite or RustFS S3 backend. It does not
qualify separate physical hosts, follower loss, destination ack SIGKILL, object-
store loss, power loss or the complete failure
and capacity matrix.

## Whole-fleet recovery from the bucket

[whole_fleet.py](whole_fleet.py) starts three independent processes with explicit
bucket durability (`fleet_durability: false` in the opt-in fixture). The first
command is accepted and its inbox effect is proven but retried without ack.
A second acknowledged source command remains pending behind the one-record
required stream. The client audits both commands, event identity, retained
effect and checkpoint before SIGKILLing every process. Only after all three
kill exits are observed does it inventory and discard their private node data
directories; the bucket, measuring ledger and logs remain.

Replacement processes restore from that bucket. Before any source POST or
consumer replay, the client checks the original source receipts, pending row,
accepted event, consumer effect and checkpoint 0. Consumer replay must add zero
effects. Its ack releases capacity, the second obligation dispatches without
source-command replay, and the final projection has two distinct effects with
checkpoint 2. Saved audit-only checks require all original/replacement process
IDs, file-loss inventories, kill/discard/restart order and complete final shutdown.

```sh
python3 examples/durable-streams/whole_fleet.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc \
  --output /tmp/cellar-whole-fleet-evidence
python3 examples/durable-streams/whole_fleet.py \
  --audit /tmp/cellar-whole-fleet-evidence/ledger.jsonl
```

Omit `--rustfs-image` for a private SQLite bucket. These runs prove bucket-mode
recovery at this selected boundary on one machine. They do not qualify fleet
follower-only proof, power or object-store loss, every commit/proof cut point,
physical hosts, capacity or soaks.
