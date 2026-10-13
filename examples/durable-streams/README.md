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
and management grants. Name-only resource resolution is still under development.

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
