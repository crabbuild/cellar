<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

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

The [event delivery guide](../../docs/26-event-delivery.md) explains the SDK
contract, error handling, replay identities and operating model.

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

Storage access and commit retain the original activation/domain authority.
Forced domain closure inside the callback refuses commit and rolls back state
and the emitted obligation together; retaining a storage handle cannot bypass it.

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
Required checkpoints can keep events beyond their age target. Age maintenance
waits for checkpoint or policy progress when the whole prefix is protected;
live leases keep their expiry deadline. Acknowledgment releases capacity and
resumes eligible bounded cleanup.

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

Add `--follower-only` to qualify an append accepted solely on follower proof.
This mode uses 1000ms node leases, fleet durability on the original destination
and explicit bucket proofs for producer/consumer leader writes. It waits for
the destination ensemble, refuses destination replica uploads and requires an
actual refused stream LTX write plus unchanged stream LTX and original-session
log objects in the bucket. After SIGKILL it deletes the destination's private
data and observes actual lease expiry. Followers keep their retained data.

The original append reply stays held through the recovery audit, so the source
cannot retry through a new owner and repair the prefix being measured. Only
after the original envelope, pending source, empty inbox and checkpoint zero
are audited does the harness drop the reply and release identical retries.
One effect/checkpoint and complete shutdown are required. The extra saved-ledger
checks live in [destination_proof.py](destination_proof.py) and reject bucket
bypass, missing proof, early retry, local-data survival and stale authority.

```sh
python3 examples/durable-streams/fleet_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --follower-only --output /tmp/cellar-destination-follower-evidence
python3 examples/durable-streams/fleet_faults.py \
  --audit /tmp/cellar-destination-follower-evidence/ledger.jsonl
```

The same mode accepts `--rustfs-image` with a pinned digest. It covers the
selected destination append cut with live followers; follower disk loss,
follower-only fetch/ack, physical hosts, power/store loss and the full matrix
remain unqualified. The ordinary mode retains its existing proof/lease defaults.

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

## Independent-process acknowledgment loss

[ack_faults.py](ack_faults.py) starts the same three independent roles and holds
a complete successful `__streamAck` reply on the real authenticated peer tunnel.
The opt-in `/projection/lost-ack` route stores the exact batch receipt in the
same transaction as its inbox effect, then syncs before acknowledgment. A saved
audit requires checkpoint 1, no live lease, the original effect and the retained
receipt digest before SIGKILL of the destination and actual socket reply loss.

After destination restart, another audit checks that state before any replay.
`/projection/ack-stored` decodes the persisted receipt and calls `Subscription::ack`
without fetching another batch. Original request bytes must match and the SDK
must return `AlreadyAcknowledged`; effects and checkpoint remain unchanged.
If a stale pooled connection interrupts replay, the driver records that ambiguity
and audits state/receipt again before another attempt. It preserves original
handler, operation and lease deadlines. All final processes must report complete
preserving shutdown.

```sh
python3 examples/durable-streams/ack_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc \
  --output /tmp/cellar-ack-fault-evidence
python3 examples/durable-streams/ack_faults.py \
  --audit /tmp/cellar-ack-fault-evidence/ledger.jsonl
```

Omit `--rustfs-image` for SQLite. Ordinary mode adds independent-process ack-loss
evidence on one machine. It does not qualify consumer process loss during exact
receipt replay, follower-only proof, power/store loss or all lease/ack cut points.

Add `--follower-only` to refuse destination replica uploads after the accepted-
event audit and before processing/acknowledgment. The shared destination proof
gate requires an actual refused stream upload, an installed fleet-ack marker
and unchanged stream LTX/session-log bucket bytes. After SIGKILL the harness
deletes destination-local data, observes old authority expiry and requires a
higher owner epoch/new process generation. Recovery must retain checkpoint 1,
the original consumer effect and the exact saved receipt before replay returns
`AlreadyAcknowledged`. This mode uses 1000ms node leases and explicit bucket
proofs for producer/consumer leader writes; ordinary mode keeps its defaults.
It covers this cut with live retained followers, not follower disk destruction,
consumer loss during replay, physical hosts, power/store loss or every ack cut.

## Independent-process fetch loss

[fetch_faults.py](fetch_faults.py) holds a complete proven `__streamNext` reply
while an independent measuring client audits the accepted event, checkpoint zero
and live lease. It then observes destination SIGKILL and actual response loss.
The consumer must report `Ambiguous` without applying an effect. Every recovered
snapshot is validated before the proxy permits another fetch attempt.

Two explicit modes keep the stream lease at its original 30 seconds:

- `--lease-state live` records a 1-second node-authority policy for this fixture.
  Recovery must retain the exact original live lease before replay, and replay
  request/response bytes must match the held exchange exactly.
- `--lease-state expired` retains the existing 30-second node-authority policy.
  Recovery must observe original lease expiry before any fetch retry. The same
  fetch ID must obtain a higher lease generation and a different response;
  subsequent replay retains that fresh lease and identical response bytes.

The opt-in `/projection/fetch?fetch_id=ID` route uses `next_with_id`, saves the
receipt atomically with inbox/effects and syncs without acknowledging. Two
successful fetch replays must apply one effect total. Only the saved receipt is
then acknowledged, advancing checkpoint one. Saved evidence verifies identities,
wire digests, lease times, effect receipts, SIGKILL/replacement PIDs and complete
shutdown. An audit-only invocation requires complete evidence; internal prefix
audits cannot stand in for that final gate.

```sh
python3 examples/durable-streams/fetch_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --lease-state live --output /tmp/cellar-fetch-live-evidence
python3 examples/durable-streams/fetch_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --lease-state expired --output /tmp/cellar-fetch-expired-evidence
python3 examples/durable-streams/fetch_faults.py \
  --audit /tmp/cellar-fetch-live-evidence/ledger.jsonl
```

Add `--rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc` to use a fresh private S3
server managed by the measuring client; omit it for a private SQLite bucket.
These are independent loopback processes on one machine. They do not qualify
power loss, failed object-store persistence, follower-only proof, physical host
isolation or an old owner still executing during takeover.

Add `--follower-only` to either lease mode to isolate fetch proof on retained
followers. It arms the destination replica refusal after the accepted-event
audit, checks unchanged stream LTX/session-log bucket objects and actual fleet
proof, then deletes destination-local data after SIGKILL. Actual node-authority
expiry must precede restart, and the recovered original event, checkpoint zero
and lease state must be audited before another fetch. The live case returns the
exact original receipt/response; the expired case obtains higher lease authority
before stable replay. Two successful replays produce one inbox effect, followed
by acknowledgment of the saved receipt. Producer/consumer leader writes use
explicit bucket proof. This selected fetch cut retains live follower disks;
other commit/proof cuts, follower disk loss, physical hosts, power/store loss
and live old-owner execution remain unqualified.

```sh
python3 examples/durable-streams/fetch_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --follower-only --lease-state live --output /tmp/cellar-fetch-follower-live
python3 examples/durable-streams/ack_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --follower-only --output /tmp/cellar-ack-follower-evidence
```

Add `--restart-followers` to those ack/fetch commands to recover from **cold
retained follower disks**. It requires `--follower-only`. After the proven reply
is lost, all original processes are SIGKILLed. The measuring client archives the
original destination ensemble's checksum-protected CLB1/CLE1 batches and checks
that neither stream LTX nor that destination session's log objects repaired the
bucket prefix. It deletes the destination's private data while preserving both
peer disks. All replacements start before readiness waits; old leases must expire
and peer process IDs/node generations must change.

The saved recovery audit must prove the original event and subscription state
before request replay. A returning witness can become stream owner, so every
advertised replacement route retains the paused exact-byte measuring proxy.
Ack recovers checkpoint one and replays the exact saved receipt without new
fetch/effect. Live fetch retains the exact lease/reply; expired fetch waits for
the original lease and obtains higher authority before stable replay. Offline
audit reopens both archived batch inventories and refuses changed bytes or
incomplete cleanup. Unit oracles use synthetic custody metadata; the process
runs supply the actual retained batch and state evidence.

These cases preserve follower disks on one machine. They do not qualify missing
witness disks, cold pending-append recovery, consumer-local replay, physical host
or power loss. The original live-follower modes remain separate cases.

For a **cold pending append**, use both flags on `fleet_faults.py`:

```sh
python3 examples/durable-streams/fleet_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --follower-only --restart-followers --output /tmp/cellar-cold-append
python3 examples/durable-streams/fleet_faults.py \
  --audit /tmp/cellar-cold-append/ledger.jsonl
```

The source still has an unsettled obligation when every original process dies.
To prevent automatic delivery from repairing the destination before inspection,
returning peers enable a private exact-scope ownership-write refusal. It denies
only puts to this stream's ownership object while `claim-block` exists; reads,
other claims/leases and follower-log replies retain their real behavior. The
driver must observe an actual refusal before starting the destination. The
replacement destination then claims and recovers through the ordinary proof
path. The private fault never returns success or supplies proof.

After both peers are ready, require a blocked retry with the original append
bytes. Audit the original event/envelope, pending source receipt, empty consumer
and checkpoint zero before dropping the held reply or resuming retries. The
source may recover on either returning peer. Final delivery must retain the
same acceptance and produce one inbox effect/checkpoint. Offline audit rechecks
the private claim configurations/markers and archived follower batches; complete
process/proxy/store cleanup remains mandatory. Add `--rustfs-image` with a pinned
digest for a private S3 store.

This qualifies recovery under an explicit placement gate. Unrestricted placement,
missing witness disks, consumer-local replay and physical host/store/power loss
retain separate qualification. Neither this cut nor a passing rerun explains
the earlier intermittent startup/shutdown failures.

## Resume an expired source owner

[resumed_owner.py](resumed_owner.py) uses four independent loopback processes
and explicit bucket proof. The original producer holds its real input gate after
proving one source obligation but before first dispatch. The measuring client
observes SIGSTOP, archives that stable SQLite family, reads actual node-authority
expiry from the private store, and starts a different successor process. Before
SIGCONT, the successor must own a higher epoch and settle the original event.

The old process must resume within the original 10-second invocation budget and
report `NodeLeaseExpired`, with no graceful-stop report and no successful original
response. Its archived old business/outbox image must still show the same receipt,
one pending record, zero dispatch attempts and zero settlement. A second successor
audit must precede command retry or consumer processing. The duplicate command
retains its receipt, and the consumer applies one effect with checkpoint one.

```sh
python3 examples/durable-streams/resumed_owner.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --output /tmp/cellar-resumed-owner-evidence
python3 examples/durable-streams/resumed_owner.py \
  --audit /tmp/cellar-resumed-owner-evidence/ledger.jsonl
```

Add `--rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc` for a fresh private S3 store.
Node leases are explicitly 1000ms; handler and stream lifetimes are unchanged.
Forensic queries use disposable copies: even read-only WAL connections can create
empty WAL/SHM files, so the measured archive itself is never opened by SQLite.
This case qualifies a resumed expired owner with a held invocation/first dispatch.
It does not qualify physical partitions, power/store loss, follower-only proof,
or every in-flight write/reply interleaving while an old owner is running.

## Keep a live source callback entered through takeover

[live_owner.py](live_owner.py) runs four independent processes with explicit
bucket proof and 1000ms node leases. It first accepts and delivers one original
order. A second, deliberately different probe stays inside the old source's
native poll at either `--cut before_tx` (retained handles before starting a
transaction) or `--cut before_commit` (business/event writes still uncommitted).
The ordinary 10-second handler budget stays unchanged.

Only the producer enables the private `owner_fault` fixture client. A private
marker makes its actual ordinary, conditional and listing operations fail;
the CAS wrapper retains the original no-retry client. The small fixture listing
is capped at 1000 objects. No adapter supplies proof or alters another node's
client. Callback phase files are observations, not authority or receipts.

The measuring client requires real refused lease operations, observed lease
expiry, a live/non-stopped old PID and higher-epoch successor ownership. It audits
the original business receipt, envelope, producer progress and empty consumer
state before releasing the callback. No source command is replayed. The stale
operation must refuse closed-domain authority; its caught-error success-shaped
response must not succeed. The old process must join with lease-expiry fencing.
Its archived DB/WAL is queried only through disposable copies and must retain
the original business state, next sequence 2, settlement 1 and zero pending rows.
Another audit precedes the single consumer effect and complete survivor cleanup.

```sh
python3 examples/durable-streams/live_owner.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --cut before_commit --output /tmp/cellar-live-owner-evidence
python3 examples/durable-streams/live_owner.py \
  --audit /tmp/cellar-live-owner-evidence/ledger.jsonl
```

Add `--rustfs-image` with a pinned RustFS digest for a private S3 bucket. These
cuts qualify independent loopback processes on one machine with an injected
node-client outage. Physical partitions, power/store loss, follower-only proofs
and other stale settlement/reply interleavings retain their own qualification.

## Keep a live consumer callback entered through takeover

[consumer_owner.py](consumer_owner.py) extends the consumer ownership cuts to
four independent processes with explicit Bucket proof. It commits/acknowledges
one original effect, then holds a second actual SDK delivery inside the old
consumer's native poll. `--cut before_accept` retains the inbox handle and enters
the transaction before fencing; `--cut before_commit` has already written the
receipt and effect. The normal 1000ms node lease, 10-second handler budget and
30-second delivery lease stay unchanged.

Only the consumer enables the private owner fault. The observer requires actual
refused lease operations, expiry, a live/non-stopped old PID and higher-epoch
successor ownership. Its first successor audit checks the original effect prefix
before callback release or fetch replay. Acceptance is reported separately from
transaction completion so a later write refusal cannot hide illegal acceptance.
Require closed-domain refusal, interrupted output and joined expiry fencing.

The old process's archived SQLite/WAL must retain exactly the first inbox digest
and effect. Only after that audit may the successor replay the same saved fetch
ID, receive the exact original receipt/batch, apply the pending effect once,
skip it on repeated fetch and acknowledge checkpoint 2. After complete survivor
joins, a successor archive must retain both exact inbox digests and effects.
[consumer_owner_audit.py](consumer_owner_audit.py) rechecks the saved ledger and
both archives through disposable copies; it never opens measured archives as
SQLite databases or repairs state by replay.

```sh
python3 examples/durable-streams/consumer_owner.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --cut before_commit --output /tmp/cellar-consumer-owner-evidence
python3 examples/durable-streams/consumer_owner.py \
  --audit /tmp/cellar-consumer-owner-evidence/ledger.jsonl
```

Add `--rustfs-image` with a pinned digest for a private S3 bucket. This qualifies
selected client-outage cuts on loopback processes. Follower-only consumer proof,
physical partitions, power/store loss and the full fault/capacity matrix remain
separate requirements.

## Recover consumer inbox and effects from follower-only proof

[consumer_recovery.py](consumer_recovery.py) uses the ordinary saved-fetch route
to commit and prove inbox receipts, effects and an acknowledgment receipt while
the subscription is still unacknowledged. After one seeded effect, the consumer's
private replica fault refuses actual per-cell and node-session publication. The
second fetch must return a proven effect using a real follower ensemble while
its selected bucket objects stay unchanged. Source/destination nodes retain
explicit Bucket proof. The consumer is SIGKILLed and its entire directory removed.

The successor's first audit must recover both original effects, exact inbox
identities and the saved receipt hash before any fetch replay or acknowledgment.
The fixture retains both original events. A bounded raw SDK read checks both
inbox acceptances in an always-rolled-back transaction before replay; both must
report already processed. A missing receipt cannot be repaired by that probe.
Replaying the same fetch ID must apply zero new effects. A live receipt remains
exact; if its normal 30-second lease actually expired, fresh authority must have
a higher generation and unchanged event bytes. The checkpoint advances only
when the recovered receipt is explicitly acknowledged. Joined old/recovered
SQLite/WAL archives must retain both inbox digests and the corresponding receipt.

`--restart-followers` also kills every original process, archives the committed
consumer witness batches, waits for real peer expiry and restarts peers from
those retained disks with fresh generations. A private proxy temporarily refuses
only seal requests for the original consumer session. A signed read-only
framework request for that consumer scope must return explicit `Unavailable`
before the unchanged operation deadline, without changing its owner, sealing the
log or inventing bucket coverage. After the real witness service returns, the
same successor PID/generation must recover the prefix on its first fresh read.
No source command or consumer processing is replayed to repair that audit.

```sh
python3 examples/durable-streams/consumer_recovery.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --restart-followers --output /tmp/cellar-consumer-recovery-evidence
python3 examples/durable-streams/consumer_recovery.py \
  --audit /tmp/cellar-consumer-recovery-evidence/ledger.jsonl
```

Add a pinned `--rustfs-image` for a private S3 bucket. Saved evidence independently
rechecks original/recovered SQLite/WAL and cold witness archives, exact commands,
fresh process generations and complete node/proxy/store joins. This covers
retained follower disks and transient witness-service unavailability.

Add `--witness-data missing` or `--witness-data corrupt` together with
`--restart-followers` to remove every committed consumer batch or flip a checksum
byte in each batch on all original witnesses. All original processes are already
killed, and exact valid archives remain separate. State/ownership metadata stays
intact. Real witness services return incomplete seal certificates; two framework
reads must return explicit `Unavailable`, with unchanged consumer ownership,
an unsealed recovering log and no new selected bucket coverage. The second read
must actually contact the witnesses despite the existing loss diagnostic.

`--witness-data oversize` instead keeps each original batch prefix and extends
its file with zeros to 64 MiB + 73 bytes, one byte above the format limit. State
metadata remains intact. The same two real refusals and incomplete seals are
required; the saved auditor checks exact lengths, original prefixes and zero
extension with bounded reads. The native reader regression separately requires
zero payload-read requests before rejection. Current group commits cap encoded
entry bytes at 64 MiB plus their 72-byte envelope; excess ordered frames carry
into consecutive commits, with no dropped records or reordered replies. A native
full-budget ordered-stream/cold-load case checks the accepted boundary too.

Both damaged peer processes must drain and join before original batch bytes
return. Their persisted seal state is preserved. The proxies propagate the
stopped upstreams' EOF before fresh peer generations start. The same successor's
first fresh recovery audit must expose both original effects and the saved
receipt, followed by two false inbox acceptances before any processing replay.
Offline audits inspect original, damaged and returned witness archives as well
as both consumer images. This covers batch loss/corruption with retained state,
not arbitrary metadata loss, physical host/power/store loss or the complete
failure matrix. A generic application GET may wrap a nested
routing failure as HTTP 500; that response does not qualify the explicit refusal
check, which uses the existing authenticated framework event route.

`--witness-data state-missing` removes only the selected consumer state metadata;
`--witness-data state-corrupt` changes its base to its end without updating the
checksum. `--witness-data state-oversize` pads the original valid record to
64 KiB without changing its fields or checksum. All preserve every committed
batch. Real witness services must refuse
before supplying a seal, and two framework calls must return explicit
`Unavailable` without advancing the consumer owner or sealing its log. These
metadata errors leave witness state inconclusive; they do not invent a loss
certificate. Proxy observations retain the actual peer status/body digest.
The oversized case requires the actual format-bound error and independently
checks the exact original-byte prefix and padding. Native filesystem probes
separately verify rejection before requesting any state payload bytes, not only
a decoder error after allocation.

After both peers drain and join, require their files to equal the damaged
inventory exactly before returning the original valid metadata. Returning bytes
must not overwrite a newer seal or other intervening state. Fresh peer
generations and the same successor then run the original-prefix/inbox/replay
audits. Archived state records have format 1, a checksum and both session and
follower identities; the independent archive reader validates all of them.
This is a hard development cutover: rebuild participants and recreate older
unchecksummed state, with no legacy reader or migration.

Cold destination ack/fetch replay fixtures also prevent the returning consumer
from claiming the selected stream, while preserving its projection and other
store operations. The recovered stream must live on a different node so exact
receipt/reply replay actually crosses the measuring proxy. Configuration and
marker custody are audited; normal application support for colocated consumers
does not change. A local AlreadyAcknowledged result alone cannot establish the
network replay boundary.

## Recover a follower-only source proof

[follower_faults.py](follower_faults.py) runs independent destination, producer
and consumer processes with real fleet durability. A required subscription holds
the first event and backpressures a second source. After observing an installed
follower ensemble, the measuring client arms an opt-in replica-client refusal on
the producer. Both single-object and multipart publication fail; lease, catalog
and wake clients remain healthy. The second source must still return success.

Before loss, the driver independently inventories exact source LTX bytes in the
private bucket, checks that they did not change across the transaction, observes
an actual refused source upload, and audits the original receipt and pending
obligation. It SIGKILLs the producer, archives its stable pending database/WAL
for the exact encoded envelope digest, then inventories and deletes its local data,
observes real node-authority expiry, and starts a new successor process. Reads
must recover the original receipt and pending obligation under a higher ownership
epoch before any consumer effect or acknowledgment. No source command is replayed.
The recovered accepted envelope must match the killed source's bytes exactly.
Two consumer deliveries then apply two distinct inbox effects and checkpoint two.
Saved evidence and all survivor shutdowns must pass the independent oracle.

```sh
python3 examples/durable-streams/follower_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --output /tmp/cellar-follower-evidence
python3 examples/durable-streams/follower_faults.py \
  --audit /tmp/cellar-follower-evidence/ledger.jsonl
```

Add `--rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc` for a private S3 server.
Node authority is explicitly 1000ms; existing fixtures retain their defaults.
The fault adapter supplies no durability credit and is absent unless explicitly
enabled in fleet-fixture configuration. This case covers follower-only source
acceptance and leader-local-data loss. Destination append/fetch/ack follower-only
cuts, loss of the follower disks, physical host/power loss and unsupported store
loss remain separate qualification requirements.

Add `--restart-followers --peer-proof bucket` to kill **all** original fleet
processes and restart destination/consumer from their retained disks. The source
still uses follower-only proof; the explicit peer policy keeps other leaders'
commits bucket-backed. Before any replacement starts, the driver rechecks that
source bucket bytes are unchanged, including producer-session node-log objects
as well as cell LTX, and archives the original ensemble's committed
batch files. Batch headers, ranges, entry identities and SHA-256 footers supply
the evidence; `state.json` may legitimately retain a zero watermark. It verifies
unchanged follower files before restart, actual old authority expiry, new PIDs
and new node generations before auditing recovery. Saved source and follower
archives are checked again at completion.

The broader `--restart-followers --peer-proof fleet` case has an open recovery
gap: deleting the producer's disk can also delete the destination's sole follower
witness. A private S3 run refused destination startup with `no complete true
witness`; consumer/successor activation then failed. This is fail-closed behavior,
not successful recovery. Passing the source-only proof case does not resolve
that combined failure or establish physical disk/power tolerance.

## Discover sources from the fleet

`cellar outbox census Order --limit 100 --json [FLEET OPTIONS]` lists root source
state through the bucket cell directory and signed owner inspection. Unlike
destination `stream sources`, it can find a producer whose first event is still
blocked. Continue with `--cursor` until `next` is null; filtered empty pages can
still continue. Failed owner probes return `unavailable`, retain the page/cursor
and cause a failed exit, rather than claiming an empty source. This is a live
root-cell view, not a snapshot, drain proof, facet inventory or aggregate metrics.
Inspection can restore a cold source and let existing durable work progress.

Run the actual CLI against a private RustFS fixture:

```sh
python3 examples/durable-streams/census_smoke.py \
  --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --output /tmp/cellar-census-evidence
```

The driver checks a settled producer, a blocked producer absent from destination
inventory and an empty cell across one-prefix pages. It rejects a foreign-class
cursor and verifies explicit failed probes after complete node shutdown.

## Offline source capacity and slow consumer

`capacity.py` uses an opt-in `Backlog` cell so all commands share one outbox and
producer lane. It pauses append traffic, fills the captured row quota, requires
`OutboxFull` for one extra command and audits rollback of that command's business
write and sequence. After a short offline soak, it SIGKILLs only the producer and
audits all original commands and obligations before allowing delivery to resume.
No source command is replayed to repair recovery.

The required stream retains one record. The driver follows its resumable cursor,
slows consumer processing and proves each inbox effect before redelivery and ack.
It checks FIFO acceptance, unchanged envelopes, one effect per command, checkpoint
progress and complete preserving shutdown. The proxy forwards capacity errors so
the producer can learn the destination's batch limit. A separate saved-ledger
oracle rejects lost state, partial quota writes, replay before audit and duplicate
effects; the run cannot report success without that oracle passing.

```sh
python3 examples/durable-streams/capacity.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --count 64 --soak-seconds 30 --consumer-delay .1 \
  --rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc \
  --output /tmp/cellar-capacity-evidence
python3 examples/durable-streams/capacity.py \
  --audit /tmp/cellar-capacity-evidence/ledger.jsonl
```

Omit `--rustfs-image` for SQLite. Counts are bounded to 1–256. Resource samples
record process RSS, actor residency/cache and live local file sizes, including
SQLite/WAL/LTX families. File allocation is an observation, not a consistent
snapshot, unique physical extent count or bucket storage total. This selected
single-source case does not qualify many cold sources, long soaks, memory SLOs,
compaction/restore capacity, follower-only proof or physical-host/power/store loss.

## Many cold sources

`many_sources.py` commits one event in each independent Order cell. Required
retention fills the captured stream window and backpressures every append path,
including local calls from a source that moves onto the destination node. It
archives the full accepted prefix and checks every business command, receipt and
remaining pending
obligation before SIGKILL of all three fleet processes. Owners may move during
pressure or wake recovery, so the audit accepts current owners within the fleet.
After restart it observes the hosts and
unchanged protected destination history, then starts consumption without reading or posting any source
until the entire backlog has been accepted and consumed. Durable wakes must drive
recovery. Final source inspection verifies original identities and settled lanes.

The default 144 sources exceed one host's 128-resident limit. The eight-record
required stream fills before each consumer retry/redelivery check so newly
appended records cannot change that batch. Cross-source ordering follows actual
destination offsets; no global command-order promise is assumed. The saved oracle
checks distinct sources/lanes, contiguous acceptance, exact inbox receipts and
effects, checkpoint progression, resource limits and complete shutdown.

```sh
python3 examples/durable-streams/many_sources.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --count 144 --stream-rows 8 --soak-seconds 30 \
  --rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc \
  --output /tmp/cellar-many-source-evidence
python3 examples/durable-streams/many_sources.py \
  --audit /tmp/cellar-many-source-evidence/ledger.jsonl
```

Counts are bounded to 2–192 and stream rows to 1–20, below the source count. Optional fixture limits
change only the explicit test graph; the ordinary example remains unchanged.
Host/local file observations have the same limits as `capacity.py`. This case
uses the original application class and surviving local state/store. Removed-
class recovery, compaction, follower-only proof, power/store/physical-host loss,
long soaks and operational memory SLOs retain their separate qualification scope.

For startup diagnosis, set `CELLAR_STREAM_FIXTURE_TRACE=1 RUST_BACKTRACE=1` on
the same command. The opt-in fixture writes activation method/stage timings and
the driver records host snapshots while replacement readiness is pending. These
probes never invoke source cells and preserve the original readiness deadline.
Keep diagnostic runs distinct from ordinary qualification: probes add traffic,
and a successful replay cannot explain an earlier intermittent failure.

The controlled cold-start case starts the destination before returning its
witnesses. It waits for an actual missing-witness recovery refusal, records the
unchanged predecessor/owner and old peer leases, then starts both retained peers.
This reproduces the startup backoff/activation race without artificial proof or
longer deadlines. The destination's trace is enabled for this case; its bounded
diagnostic prefix is checked again by the independent evidence auditor.

```sh
python3 examples/durable-streams/fetch_faults.py \
  --binary "$CARGO_TARGET_DIR/debug/cellar-example-streams" \
  --follower-only --restart-followers --lease-state expired --recovery-first \
  --output /tmp/cellar-cold-start-evidence
python3 examples/durable-streams/fetch_faults.py \
  --audit /tmp/cellar-cold-start-evidence/ledger.jsonl
```

Add `--rustfs-image` with the chosen pinned image to run against a private network
store. Successful startup must retain the original fetched state before replay,
then apply one effect and acknowledge with complete process/proxy/store joins.
The case keeps the original 30-second node/delivery leases and 10-second event
operation budget. Physical failures and other startup causes remain separate.
