# Field-service application performance design

Prepared 2026-10-12 against Cellar
`2f514334c193ee585c3edb90ba8e3ed2be327d41`.
Implementation: [FleetCare crate and executable suite](../../crates/cellar-fleetcare/README.md).
Run `python3 tools/perf/fleetcare.py`; the crate README describes implemented
cases, commands, resource budgets and coverage limits. This document retains
the broader qualification design: Poisson arrivals, peer partitions, rolling
code adoption and the full sizing/soak matrix are extensions beyond the
standard suite. The accompanying [blueprint](field-service-workload.json) is
reference documentation rather than executable runner configuration.

## What this experiment should answer

Build **FleetCare**, a multi-tenant equipment monitoring and field-service
application, as a native Rust executable embedding `cellar::sdk::Node`.
Devices report health, operators inspect state and history, service tickets
reserve spare parts, durable workflows coordinate technicians, queues build
reporting projections and deliver notifications, and Blob stores diagnostic
attachments. All authoritative state changes go through Cellar.

Use 10,000 fleet-owned cells across three nodes, each with **1 vCPU and 2 GiB
RAM**. Measure these questions independently:

1. How many authoritative reads and durably acknowledged commands per second
   can the fleet sustain, at what p50/p95/p99 latency?
2. What does a complete ticket/reservation/notification flow cost, including
   asynchronous completion and projection visibility?
3. How much performance changes with cold activation, remote forwarding,
   hot cells, larger databases, background work and each integration?
4. Does adding nodes improve throughput, or move the bottleneck to RustFS,
   the local disk, admission, a queue broker or one business cell?
5. What happens to latency and acknowledged state during crash, partition,
   recovery, rebalance, rolling adoption and shutdown?

Keep isolated primitive measurements, integrated application measurements and
failure measurements as separate result series. An aggregate requests/s number
cannot answer all five questions.

~~~mermaid
flowchart LR
    Client["Open-loop clients / operators"] --> Host["Application-owned HTTP host"]
    Host --> Cells["Three embedded Cellar nodes"]
    Cells --> Domain["Device / order / inventory state"]
    Cells --> Async["Workflow / Queue / SQL / KV cells"]
    Domain --> Async
    Cells --> Store["RustFS: ownership, LTX, logs, Blob"]
    Cells --> Simulator["Technician HTTP / TCP / TLS simulator"]
    Async --> Domain
~~~

## Existing evidence and the gap

The [10,000-cell reference](results/2026-10-12-embedded-10000-reference.md)
already used three 1-CPU/2-GiB embedded nodes with RustFS. It measured counter
overwrites under closed-loop traffic, checked every acknowledged counter and
returned 14 SDK shutdown proofs. It did not exercise this application, normal
read traffic, growing history, independent arrivals or all SDK capabilities.
Its older image also predates route-cache bounds and typed fencing outcomes.

Reuse the resource isolation and receipt verification patterns in
[run.py](run.py), [faults.py](faults.py) and
[peer_partition.py](peer_partition.py), and the host composition in
[embedded.rs](embedded.rs). Preserve those original workloads for comparison.
Do not relabel their numbers as FleetCare results.

## Application and cell accounting

Use 20 tenants with stable identities derived from tenant and entity IDs.
Authentication determines the tenant before identity construction. Generate
business scopes with the SDK namespace APIs and system-binding scopes with
their existing identity algorithms; node addresses never enter a durable name.

| Authority | Count | Per tenant | Durable responsibilities |
| --- | ---: | ---: | --- |
| Device | 5,999 | 299–300 | Latest telemetry, sequence, health, history, receipts, alarm and outbox |
| WorkOrderBook | 2,400 | 120 | A stable ticket partition containing orders, transitions and attachment references |
| Inventory | 1,000 | 50 | SKU/location stock, reservations, expiry and receipts |
| Dispatch workflow instance | 500 | 25 | Long-lived dispatcher journal, named steps, waits and participant results |
| SQL binding database | 60 | 3 | Tenant reporting partitions, consumer receipts and reconciliation state |
| KV namespace backing cell | 20 | 1 | Versioned tenant policies, TTL lookups and metadata |
| Queue broker | 20 | Shared as described below | 10 primary brokers and 10 corresponding dead-letter brokers |
| Cron authority | 1 | Shared | Bounded broker reconciliation |
| **Total fleet-owned cells** | **10,000** | | **9,399 business + 601 system/workflow cells** |

Two tenants share each primary queue and its dead-letter queue. Every envelope
contains an authorized tenant identity; the consumer checks it before routing
to that tenant's SQL partition. All 20 brokers are initialized and exercised,
including deliberate poison deliveries in the coverage phase. Declare 60 SQL
identities, 20 KV identities and 20 queue identities explicitly in the graph;
one binding with many keys is not many backing cells. The current KV binding
addresses shard zero.

New tickets hash into the existing WorkOrderBook partitions, rather than
creating a new cell per ticket. New telemetry and tickets add rows to existing
databases. Dispatcher slots also retain stable identities while coordinating
successive orders. This holds the fleet at 10,000 identities while allowing
real state growth; it does not hold the row count constant.

Facets are parent-owned child scopes, not independently placed fleet owners.
The coverage profile creates at most 50 diagnostic facets underneath existing
devices. Report their extra databases, bytes and lifetimes separately. Stateless
services, loaded workers, WebSocket connections, Blob objects and child
containers also have separate counts. Do not hide these resources in the
10,000-cell total or claim there are exactly 10,000 SQLite files.

### Ownership targets and residency

| Authority | Node A target | Node B target | Node C target |
| --- | ---: | ---: | ---: |
| Device | 1,999 | 2,000 | 2,000 |
| WorkOrderBook | 800 | 800 | 800 |
| Inventory | 334 | 333 | 333 |
| Workflow | 166 | 167 | 167 |
| SQL | 20 | 20 | 20 |
| KV | 7 | 7 | 6 |
| Queue | 7 | 6 | 7 |
| Cron | 1 | 0 | 0 |
| **Total** | **3,334** | **3,333** | **3,333** |

These are targets, not a node-pinning API. Set equal placement weights and
enable the existing paced rebalance. Seed through all three ingresses, allow
placement to settle, and record the actual authoritative owner/epoch of every
scope. Require every identity to have one live owner and each node's total to
be within 5% of one third before a balanced steady-state comparison. Also
record class distribution and traffic share: equal cell counts can conceal
unequal load. If placement does not settle, retain that failure as a result.

Start with Sticky ownership on eviction so 10,000 authorities can remain
assigned even when their handlers are not resident. Sweep Release ownership
separately; with Release, idle identities can be unowned and the instantaneous
owned-cell total need not be 10,000. Never edit bucket ownership records to
manufacture a distribution.

Start at 512 resident cells per node and a 2,048-entry idle route cache.
Owning roughly 3,333 cells does not require 3,333 resident handlers. Owned,
alarmed and unfinished records can survive beyond the idle cache bound.
Measure each category and whole-cgroup memory; the cache limit alone does not
bound process memory. The allocation targets are not guarantees of residency.

## State, commands and invariants

Use cell-local SQLite through Storage for authority; SQL binding databases
serve reporting. Keep remote calls outside synchronous transactions. A handler
can yield between awaits, so protect each read-modify-write invariant with a
transaction instead of assuming whole-handler serialization.

### Shared local schema

Each business cell has an application schema version and these tables:

~~~sql
CREATE TABLE command_receipts (
    command_id TEXT PRIMARY KEY,
    request_sha256 TEXT NOT NULL,
    version INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL
);
CREATE TABLE outbox (
    effect_id TEXT PRIMARY KEY,
    source_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','delivered','quarantined')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_ms INTEGER NOT NULL
);
CREATE INDEX outbox_due ON outbox(state, next_attempt_ms);
CREATE TABLE changes (
    version INTEGER PRIMARY KEY,
    command_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_ms INTEGER NOT NULL
);
~~~

Apply the domain mutation, version advance, receipt, change row and outbox
intent in one local transaction. The same command ID and digest return the
original result. The same ID with another digest fails. Deterministic business
rejections have replayable results too. Keep receipts for the entire experiment
and recovery audit; the operational retention target is at least 24 hours.
Retention tests use a separate profile and never delete still-retryable IDs.
Version the digest algorithm: SHA-256 over the command kind, tenant/cell
identity, expected version and canonical payload bytes. Use bounded command
IDs and validate the digest server-side. Reusing an ID with a changed target or
expected version is a different request, even if its JSON body looks similar.

The public acknowledgement passes through the normal output gate. A local
commit marker is not a durable acknowledgement. Do not add an unconditional
Storage.sync to every handler and double-count the normal response gate; test
explicit sync as its own variant.

An outbox dispatcher retries a stable effect ID using a persisted alarm.
wait_until may accelerate delivery after the response, but the persisted
outbox/alarm remains the crash-survival mechanism. Each cell has one alarm:
schedule the earliest health/reservation/outbox deadline, and process due work
in bounded batches before scheduling the next deadline.

### Domain schemas and state machines

| Cell | Additional data | States and transitions | Invariant |
| --- | --- | --- | --- |
| Device | device(id, last_seq, latest_json, health, version, last_seen_ms); telemetry(seq, command_id, payload, observed_ms) | Healthy → Alerting on threshold; Healthy/Alerting → Offline on deadline; Offline → Healthy/Alerting on newer telemetry; Maintenance entered/exited by operator command | Sequence never regresses; duplicate command never appends another sample; alarm cannot mark a recently updated device offline |
| WorkOrderBook | orders(order_id, device_id, status, version, inventory_id, reservation_id); order_events(order_id, version, kind); attachments(order_id, object_key, sha256, bytes, state) | Open → Reserving → Reserved → Dispatched → InProgress → Completed; failure → Compensating → Cancelled; unresolved failure → NeedsReview | Expected-version transition; terminal order never consumes or releases the same reservation twice |
| Inventory | stock(sku, location, on_hand, reserved, version); reservations(id, order_id, qty, state, expires_ms) | Held → Consumed / Released / Expired | on_hand ≥ reserved ≥ 0; reserve only if sufficient stock; consume decrements both on_hand and reserved exactly once |
| Workflow | Framework journal plus recorded participant receipts and order assignment | WaitingAssignment → Reserving → Notifying → WaitingTechnician → Finalizing → WaitingAssignment; errors enter explicit compensation or review | Stable versioned step names; replay uses the same participant IDs; compensation is application code |
| SQL projection | device_view; order_view; inventory_view; consumer_receipts(effect_id PRIMARY KEY); projection_progress(source_scope PRIMARY KEY, source_version) | Received → Applied / Duplicate / Quarantined | Receipt and projection update share the database transaction; older changes never overwrite newer versions |
| KV | policy/v1; routing hints; expiring operator sessions with metadata | Absent → Present → Replaced / Expired / Deleted | Policy version/digest in responses; current binding reads strongly; no invented eventual-read cache |
| Queue | Built-in message, delivery and settlement state; application envelope carries effect_id and source version | Enqueued → Delivered → Acked, or Retried → DeadLettered | Consumer commits its idempotent result before ack; attempt count differs from unique applied effects |

Emit full replacement snapshots in projection messages so out-of-order delivery
can safely compare source versions. If a later implementation uses deltas,
buffer gaps and reconcile them rather than simply discarding older deltas.
Projection read responses expose source version and as-of time.

Device storage also keeps small JSON preferences and a structured diagnostic
value, using JSON/clone get/put, multi-key operations and bounded list/cursor
paths. This exercises both current-cell key/value and SQL storage without
moving authoritative stock into a different database binding.

~~~mermaid
stateDiagram-v2
    [*] --> Open
    Open --> Reserving
    Reserving --> Reserved
    Reserved --> Dispatched
    Dispatched --> InProgress
    InProgress --> Completed
    Reserving --> Compensating: failure
    Reserved --> Compensating: cancellation
    Dispatched --> Compensating: deadline
    InProgress --> Compensating: failure
    Compensating --> Cancelled: effects reconciled
    Compensating --> NeedsReview: unresolved effect
    Completed --> [*]
    Cancelled --> [*]
~~~

### Ticket saga

1. Create an order and dispatch intent atomically in its WorkOrderBook.
   Return the durably gated accepted receipt.
2. Deliver the assignment to one of the tenant's existing dispatcher instances.
   Duplicate assignment events reconcile the order/command receipt.
3. Record a workflow step that reserves inventory using reservation ID
   derived from order ID and saga revision. If insufficient, cancel or record
   NeedsReview according to the persisted decision.
4. Transition the order to Reserved, then invoke the technician HTTP simulator
   through SDK egress with a stable notification ID. The simulator durably
   deduplicates notifications and can deliberately drop a response.
5. Wait for the technician event with a deadline; test durable sleep and event
   waits. Complete by consuming the reservation, or explicitly compensate by
   releasing it. Reconcile uncertain participant results before continuing.
6. Publish projection/notification intents; consumers update tenant SQL and
   ack. An operator reads the authoritative order or a labeled lagging view.

No transaction spans order, inventory, workflow, queue, SQL or Blob.
Recovery must be able to resume between any two steps. Keep step names and
their meanings compatible across Node.adopt generations. Capture clock,
random and policy decisions as recorded step results rather than recomputing
them during replay. Use the SDK's repeated-step journal counts or stable
iteration names to distinguish successive assignments.

## API and read/write traffic

All mutations take command ID, payload digest and, where appropriate,
expected version. Responses contain operation ID, result version, digest,
owner and epoch for benchmark diagnosis. Reads can specify a minimum version;
reject or report an unmet version rather than return fabricated freshness.
Authorization uses tenant identity and bounded payload/scan limits.

Set ingress body allowance to 32 MiB for attachment coverage, while domain
commands enforce a 64-KiB payload cap. Limit active Blob transfers to eight per
node, with at most two 128-KiB buffered chunks per stream; maintain an aggregate
transport byte budget rather than trusting the request-count bound. WebSocket
pending output is capped at 64 KiB per connection and 32 MiB per node; close
slow consumers with a replay-required result. Cancel abandoned streams and
record these rejections. Admission and buffering limits are application policy
and must be included in the captured manifest.

| Endpoint or operation | State exercised | Completion being timed |
| --- | --- | --- |
| POST device telemetry | Atomic append, latest JSON, sequence, receipt, outbox, alarm | Complete durably acknowledged response |
| GET device / history | Current state; indexed keyset scan capped at 100 rows | Complete response; distinguish local/remote and resident/activated |
| POST order / transition | Orders, receipts, version check, dispatch intent | Accepted command; saga completion is another metric |
| GET order / command receipt | Authoritative status, version and idempotency result | Complete response and verified content |
| POST inventory reserve / release / consume / replenish | Conditional stock and reservation transitions | Durable mutation or replayable business rejection |
| GET inventory | Available stock and paginated reservation state | Complete response |
| GET tenant dashboard | Three SQL partitions, prepared queries, bounded service/RPC fanout | All requested partitions complete; expose projection lag |
| PUT/GET/DELETE tenant config | KV text/JSON/bytes/stream, metadata, TTL and listing | Gated write or complete validated read |
| PUT attachment; GET attachment/range | Streaming Blob, checksum and cell reference | Object transfer and reference acknowledgement, separately |
| WebSocket subscribe / replay | Device/order event versions and session attachment | First visible event, per-update delivery and reconnect catch-up |

For Blob upload, use an immutable unique object key, verify length/hash, then
commit its cell reference. Reconcile orphan objects with cron. A response
claims success only for the documented object/reference stage. SQLite does
not atomically commit with Blob. Multipart uses valid part sizes, abort tests
and conditional outcomes; store partial-upload state in the existing book.

Authoritative read-after-write checks inspect the same owner-routed cell and
require a version at least as new as the acknowledged command. Concurrent
writes can advance it further. To verify an exact previous payload, read its
receipt/history row. SQL reporting freshness is measured separately and is
not inferred from an authority read or SQL session bookmark.

### Traffic profiles

Percentages below refer to client requests. Queue deliveries, workflow steps,
RPCs, alarms and projection updates are additional measured work.

| Client request | Core mixed | Full application |
| --- | ---: | ---: |
| Device point read | 30% | 25% |
| Device history read | 0% | 10% |
| Order read | 10% | 10% |
| Inventory read | 10% | 5% |
| SQL projection read | 10% | 10% |
| KV read | 0% | 5% |
| Blob read | 0% | 5% |
| Device telemetry write | 20% | 15% |
| Order command | 10% | 8% |
| Inventory command | 5% | 3% |
| KV write | 5% | 1% |
| Blob upload | 0% | 3% |
| **Total** | **100%** | **100%** |

The core mix is 60% reads / 40% writes; full application is 70% / 30%.
Also run pure reads and pure writes. For the core mix disable optional
notification, attachment and live-session work and label it as such. For full
application enable sagas on 25% of order commands, projections on committed
changes, randomized alarms, cron and 200 hibernating client connections across
devices. Measure accepted orders/s and completed orders/s separately.

Seed reproducibly, record the random algorithm and seed, and use existing IDs
for every request. Default telemetry is 1 KiB; test 128 B, 16 KiB and 64 KiB
separately. Full-application Blob is 256 KiB by default, with separate 1-MiB
and 16-MiB/multipart cases. Report bytes/s as well as requests/s.

## Primitive coverage contract

Cover every SDK capability family at this revision. The coverage phase asserts
the behavior before timing it; the full mix need not invoke every method on
every command. Maintain method-level coverage against source and existing
native tests as implementation develops.

| SDK family | FleetCare use and dedicated probe | Evidence / limitation |
| --- | --- | --- |
| Cell identity, namespaces, fetch/RPC, services | Entity commands, nested dashboard calls, structured errors, local/remote paths; named service props and cancellation | [cells](../../crates/cellar/application/cells.rs), [service](../../crates/cellar/application/service.rs) |
| Storage JSON/clone, multi-key get/put/delete, list/cursors | Device preferences, diagnostic graph, receipts and bounded reconciliation scans | [storage](../../crates/cellar/application/storage.rs) |
| Cell SQL and transactions | Stock invariants, atomic receipt/outbox; sync/async, nested rollback, cursor cancellation and explicit sync variants | [transactions](../../crates/cellar/application/storage/transaction.rs); local transactions only |
| SQL binding | Prepared/indexed dashboard reads, batch updates, migration/history and sessions | [SQL](../../crates/cellar/application/sql.rs); dump() returns an unsupported error |
| KV binding | Policy text/JSON/bytes/stream, metadata, multi-get, TTL, delete and cursor listing; large-value Blob path | [KV](../../crates/cellar/application/kv.rs); reads are strong and cache_status has no read-cache hit |
| Queue | Single/batch and text/bytes/structured sends, delayed send, consumer batches, per-message/all ack/retry, metrics and configured DLQ | [Queue](../../crates/cellar/application/queue.rs); verify policy, duplicates, poison and backpressure |
| Workflow | Create/batch/get/status; do_step retries, sleeps and events; pause/resume/restart/terminate/delete in separate control cases | [Workflow](../../crates/cellar/application/workflow.rs), [replay](../../crates/cellar/application/workflow/replay.rs); the current SDK also supports sensitive/rollback, batch/configuration, history and byte streams, beyond the implemented suite's coverage |
| Alarm, durable wake, cron, wait_until | Offline detection, reservation expiry, durable outbox retry, paginated reconciliation; bounded background work and shutdown drain | [cron](../../crates/cellar/application/cron.rs), [wake](../../crates/cellar/fleet/wake); wait_until alone is not durable |
| Blob | Attachments: head/get/put/stream/delete/list, metadata/checksums, conditionals/ranges, multipart/resume/complete/abort | [Blob](../../crates/cellar/application/blob.rs); object and SQLite reference have distinct commits |
| HTTP, bodies and streams | JSON commands, streamed upload/download, slow clients, cancellation, bounded form/multipart data and streaming compression | [HTTP](../../crates/cellar/application/http.rs), [streams](../../crates/cellar/application/streams.rs) |
| Assets and HTML rewriting | Packaged technician console and streaming tenant-specific status markup | [assets](../../crates/cellar/application/assets.rs), [rewriter](../../crates/cellar/application/html_rewriter.rs) |
| Cache | Console invokes put/match/delete in compatibility probes | [Cache](../../crates/cellar/application/cache.rs) always misses and drains put bodies; no hit-throughput claim |
| WebSocket | Ordinary and hibernating sessions, pairs, upgrades, text/binary, tags/attachments, enumeration/auto-response, outbound mock connection | [WebSocket](../../crates/cellar/application/websocket.rs); durable sequence/replay belongs to the application |
| TCP/TLS | Diagnostic adapter sends a framed request to a controlled plain/TLS simulator; test reads, writes, close and abort | [sockets](../../crates/cellar/application/sockets.rs); dependency timings are separately recorded |
| Facets | Up to 50 device diagnostic child scopes: startup props, fetch/RPC, abort/delete/recreate and parent retirement | [facets](../../crates/cellar/application/facets.rs); extra state is parent-owned |
| Compiled loader | Registered native tenant policy modules: load/get, readiness, entrypoint props, fetch/RPC/tail, dispose and quota failures | [loader](../../crates/cellar/application/loader.rs); no JavaScript or source sandbox |
| Cell-owned containers | Three selected existing device cells run diagnostic executables: start/readiness, port forwarding, monitor, exec, signals, inactivity and destroy | [containers](../../crates/cellar/application/containers.rs); isolated profile and aggregate resource accounting below |
| Owned native values/codecs | URLs/patterns, headers, forms/files, JSON/structured clone, RPC envelopes, events, cancellation, bytes, encoding/compression and cryptography for receipts/auth/checksums | [native modules](../../crates/cellar-native/lib.rs), [web values](../../crates/cellar-native/web.rs); value-codec costs accompany byte sizes |
| Embedding, policy, deployment and lifecycle | Explicit host/core executors, listeners, clients, auth and signing; generation adoption/failure, loader/runtime quotas, shutdown and returned outcomes | [SDK](../../crates/cellar/sdk.rs), [Node](../../crates/cellar/node/embedded.rs); run the production path |
| Ownership, LTX, logs, recovery, fencing, rebalance and observations | Bucket/fleet proof comparison and fault matrix; restored state, wake coverage and joined lifetimes | [replication](../../crates/cellar/durability/replication), [logic](../../crates/cellar-logic), [services](../../crates/cellar/execution/services.rs) |

Unsupported surfaces are tested for their documented failure and listed as
unsupported, never reported as working business features. Implementing caching
or automatic rollback is outside this benchmark. Native codec/value probes
report CPU time and bytes, rather than suggesting every owned value needs its
own network service.

## Reproducible Colima / Docker / RustFS environment

Use a private run ID, Docker network, credentials, buckets, image tags and node
directories. Reuse the existing Colima VM only after recording its resources
and ensuring other work is idle. The current default VM has 8 vCPUs and 16 GiB;
no VM resize is needed for the proposed baseline.

| Component | CPU cap | Memory cap | Local storage |
| --- | ---: | ---: | --- |
| Cellar node A/B/C, each | 1 | 2 GiB | Private 10-GiB ext4 image inside the Linux VM |
| RustFS | 2 | 4 GiB | Dedicated VM-local durable volume; budget at least 30 GiB |
| Rust load generator | 2 | 2 GiB | Attempt ledger and raw histograms |
| HTTP/TCP/TLS technician simulator | 1 | 512 MiB | Durable deduplication/effect ledger |
| Collector | Host-side, bounded overhead | Record actual footprint | Results and one-second resource samples |

Application resource flags are `--cpus=1 --memory=2g --memory-swap=2g`.
Inspect applied Docker/cgroup limits, rather than trusting Compose intent.
Optionally pin nodes to distinct virtual CPUs and supporting services to the
remaining CPUs; record that variant. A CPU quota is a time allowance, not a
guarantee of a dedicated physical CPU. Build release images before timing,
retain target caches private to this checkout, and pin all image digests,
including RustFS instead of an unrecorded latest tag.

Keep SQLite and RustFS data inside the VM, avoiding macOS file-sharing mounts
for measured persistence. Validate fsync, exclusive-directory locks, free
space and relevant mount options. Three 10-GiB images do not fit all growth
profiles automatically: preflight actual VM/host free space, provision larger
volumes when needed, and stop at 70% disk occupancy. Observe background upload
and compaction instead of allowing a short run to conceal storage debt.

Use ordinary, conditional and paginated S3 clients as the existing embedded
host does. The ownership CAS client has no automatic retries that hide
ambiguous writes. Before workload seeding, verify RustFS conditional-create
and conditional-update winner/loser behavior, read/list visibility, ranged reads
and multipart operations using the exact pinned version.

The container coverage profile reserves one `dev` child per node: the
current engine caps each child at 1/16 vCPU and 256 MiB. Reduce each application
process to 15/16 vCPU and 1,792 MiB while its child is enabled, so their summed
limits remain 1 vCPU and 2 GiB per logical node. Include fence/proxy containers
and daemon work in observations; if auxiliary container resources are not
bounded/accounted, label that profile separately and make no strict node-budget
claim. Confirm the embedded container engine can reach its Docker socket and
the workload network without modifying unrelated daemon configuration. Do not
start 10,000 child containers or use a 4/6-GiB instance type.

All three nodes share a Colima VM, host disk and RustFS server. This setup
measures a local embedded application, process/network fault behavior and
resource quotas. It does not establish multi-host/multi-zone performance or
power-loss durability. Record host model, architecture, VM/runtime versions,
thermal/load conditions, container inspect output, disk and network topology.

Infrastructure references: [Colima configuration](https://github.com/abiosoft/colima/blob/main/README.md),
[Docker resource constraints](https://docs.docker.com/engine/containers/resource_constraints/),
[RustFS container deployment](https://docs.rustfs.com/en/installation/container).

### Captured host policy

Starting values are hypotheses to sweep, not tuned recommendations:

| Policy | Baseline | Separate sweep |
| --- | --- | --- |
| placement_weight | 1 on all nodes | Unequal weights only in placement test |
| max_resident | 512/node | 128, 1,024, 4,096 if measured memory permits |
| max_cached_routes | 2,048/node | 0, 32; ownership records counted independently |
| ownership_on_evict | Sticky | Release |
| max_activations / max_evictions / max_releases | 64 / 4 / 32 | Change one at a time after measuring activation pressure |
| max_stateless_requests | Some(128), per script pool | 64, 512; include old generations and background work |
| max_cell_requests | 16 | 1, 32 for hot-cell contention |
| max_stateless_workers / max_cells_per_worker | 2 / 32 | Capture actual worker counts |
| handler_budget / admission_wait | 30 s / 100 ms | 0-ms refusal versus bounded waiting |
| lease_ttl / operation deadline | 10 s / 30 s | Capture actual expiry/fence timing |
| max_request_body_bytes / domain payload cap | 32 MiB / 64 KiB | Streaming memory has independent byte/admission limits |
| durable wake polling | 1 s for accelerated workload | Also measure unchanged 60-s default |
| shutdown deadline | 120 s | Deliberately exhausted-budget fault case |
| replica/log/loader policy | Explicitly serialized values from chosen SDK policy | Upload/restore/log-window/worker limits changed individually |

Do not treat these as a single global concurrency bound. Nested events, script
pools and generations can add slots; persist every captured startup policy and
fail the run if the application reports another value. Keep both caller-owned
executors alive until Cellar joins its owned tasks and prove they remain usable.

## Measurement method

Use a Rust generator with **open-loop** independent arrivals and bounded
in-flight work. Start offered client rates at 10, 25, 50, 100, 200, 400, 800 and
1,600 requests/s; stop after a sustained overload or refine between the last
passing and first failing rate. These are offered rates, not throughput
predictions. Record a fixed-rate and a seeded Poisson variant.

At the in-flight limit (initially 1,024), count dropped scheduled arrivals as
generator/admission failures; never silently reduce arrivals. Persist planned
command IDs/digests before issuing writes. Batch the durable journal safely,
and independently check generator CPU/scheduler lag so journaling does not
become the bottleneck. A generator unable to offer the requested rate invalidates
that comparison, while its partial evidence remains available.

Measure request latency from scheduled arrival to complete response, and also
record actual-dispatch-to-response latency and scheduler delay. This captures
client scheduling/backlog without relying on a closed-loop percentile
correction. Use monotonic time within each process; fleet timestamps need an
explicit clock-offset observation. Single-generator end-to-end times do not
require subtracting unsynchronized node clocks.

Seed every manifest scope and validate it before timing. Warm for at least
180 seconds and until residency, upload backlog and resource samples settle.
For each selected rate measure at least five minutes, repeat three times and
record time-series windows. Repetitions start from the same seeded data for
controlled comparisons. A growth/soak run deliberately keeps its accumulated
state and identifies its starting checkpoint.

Use mergeable HDR histograms or an equivalent fixed documented precision, with
sample count, bounds, timeout/overflow counts and p50/p95/p99/max per operation,
per node and locality/activation class. Obtain pooled percentiles by merging
histograms, never by averaging percentile values. Preserve per-repetition
results too. Treat p99 with fewer than 10,000 samples per class as low-confidence;
extend sampling for important rare classes. Only publish p99.9 when sample
counts support it.

Separate successes, business conflicts/rejections, definite infrastructure
failures, timeouts/ambiguous mutations and generator drops. Do not remove slow
failures from the availability or latency accounting. Timed-out requests are
censored observations: report their count and deadline, and report unresolved
work after a bounded drain. Baseline performance traffic has no automatic
mutation retries. The fault phase uses the original command IDs and reports
attempt and logical-operation latency separately.

### Required metrics

| Layer | Required observations |
| --- | --- |
| Client | Offered, sent, completed, successful and rejected requests/s; unique acknowledged commands/s; bytes/s; latency by operation and outcome |
| Business flow | Accepted/completed/cancelled orders/s; outstanding saga count/age; projection visibility delay; alarm due-to-handler delay; outbox oldest age |
| Application work | Transactions/s and rows changed; receipt replays/conflicts; nested SDK calls per root; queue unique effects/delivery attempts/batches/retries/DLQ; workflow steps/replays |
| Cell lifecycle | Manifest identities, authoritative owners/epochs, residents, cached/owned/alarmed/inflight records; activation/restore/eviction/movement counts and duration |
| Durability | Gated write delay, actual Fleet versus Bucket proof source and fallback share; LTX/log upload bytes, sync operations, backlog and compaction/hydration |
| Resources | CPU usage and cgroup throttling; RSS and cgroup current/peak/cache; memory.events; FDs; disk bytes/IOPS/fsync; network bytes and peer/store request counts |
| Dependencies | RustFS GET/PUT/HEAD/LIST/CAS/range latency and errors, bytes and CPU/disk; simulator latency and distinct effects; generator scheduling debt |
| Stop/recovery | Typed NodeOutcome; returned ShutdownReport; both executors alive; readiness, first valid read/write, full receipt audit and workload SLO recovery times |

Instrument stages around admission, route/forward, activation/hydration, local
transaction, durable proof and response completion. Correlate by request/effect
ID and epoch. The framework internally knows ProofSource; the existing counter
report does not expose per-command proof provenance. Adding low-overhead
proof-source/stage telemetry is an implementation prerequisite for mechanism
claims. If unavailable, publish only configured-posture results and mark
provenance unknown. Do not infer follower proof from a fleet-mode flag.

Aggregate metrics use bounded labels (operation/class/node/outcome); store
cell/command IDs in ledgers or sampled traces, not 10,000 metric-label series.
Sample traces initially at 1%, collect aggregate counters/histograms for all
requests, and compare tracing off/on to quantify observer overhead.
Stage percentiles are diagnostic distributions, not quantities to add to p99.

## Experiment matrix and order

Run this staged program; avoid an uncontrolled Cartesian product.

| Stage | Selected experiment | What it resolves |
| --- | --- | --- |
| 0 | Exact-image coverage, invariant and store conformance checks on 100 cells, then all 10,000 | Trust the operations before measuring them |
| 1 | No-op HTTP/service-RPC, raw simulator/storage controls, one-cell transaction and codec probes | Generator, transport, SQLite and dependency costs; controls are clearly labeled |
| 2 | One node, then three nodes; identical 10,000-cell data, resources per node and traffic | Scale-out gain and per-node efficiency; run fleet posture only where valid |
| 3 | Pure reads, pure writes, core mix; bucket-proof versus fleet-enabled posture on three nodes | Capacity curves and proof/fallback attribution |
| 4 | Owner-directed versus random ingress; resident working set versus cycling all cells | Forwarding and activation/hydration cost |
| 5 | Uniform, 80% traffic to 20% of IDs, Zipf exponent 1.1, and 20% traffic to one inventory cell | Skew, per-cell ceiling and isolation of cold tenants |
| 6 | JSON sizes; per-cell target data 4 KiB, 64 KiB, 1 MiB; indexed history and bounded scans | Data volume, row growth, query and restore sensitivity |
| 7 | Full application; then add one feature at a time: projections, saga, Blob, WebSocket, cron/alarms, background work | Business amplification and attributable integration overhead |
| 8 | Queue backlog/poison/DLQ; alarms on 1%, 10%, 100% of devices with spread versus coincident deadlines | Durable scheduling, backlog drain and fairness under storms |
| 9 | 200 then 2,000 live clients; slow consumers, reconnect/replay; Blob ranges/multipart; TCP/TLS | Long-lived IO, buffers, delivery lag and cancellation |
| 10 | Facet/loader lifecycle, quota failures and three bounded child containers | Less common capability costs and resource ownership |
| 11 | 30-minute pilot then six-hour full-application soak at 70% of measured sustainable rate | Growth, retained work, compaction, leaks and storage debt |
| 12 | Crash/partition/store-outage and rolling-generation matrix under continuing load | Correctness and degraded-service/recovery envelope |

The 100-cell smoke fixture uses two tenants: 51 Device, 24 WorkOrderBook,
10 Inventory, four Workflow, six SQL and two KV cells, plus one shared primary
and one shared dead-letter broker. Generate its own manifest rather than
keeping the full 600 binding/workflow authorities behind a "100-cell" label.

Owner-directed traffic still invokes the same application route and SDK path;
use observed owners and record any change. Never bypass Cellar via local
SQLite or a private direct-DB read. "Resident" means a measured hot working set
that fits the configured limits. A second pass over all 10,000 cells with 512
residents/node is not automatically warm. Cold-start runs restart/evict with
documented cache state; do not drop VM-wide caches during another trial.

For large state, inventory manifests and scope counts remain fixed. Preflight
10,000 × 1 MiB ≈ 9.77 GiB of logical data before SQLite/WAL/LTX/log/store
overhead; the baseline volumes may need expansion. Cap/paginate history reads
and compare indexed to an explicitly labeled scan control. Keep changing
payloads so compression/deduplication do not create an unrealistically easy
workload; record a separate compressible-data variant.

## Failure and correctness verification

The client ledger records planned IDs/digests, attempts, acknowledgement
receipts and read observations. The external simulator retains a separate
synced ledger. Save immutable stage-specific snapshots before retry/replay.

| Fault under traffic | Required verification |
| --- | --- |
| Kill actual owner before local commit, after commit but before response, and immediately after acknowledgement | Every acknowledged receipt exists; ambiguous IDs may be absent or present once; original ordinals, versions and digests reconcile |
| Kill one node; restart with and without its original local directory | Survivors recover authority; acknowledged state and workflow/outbox progress survive; predecessor recovery precedes replacement lease |
| Lose all application nodes/local directories | Restore bucket posture from RustFS and audit every scope; for fleet posture wait for independently observed bucket coverage or explicitly test loss before coverage as a different loss model |
| Peer-only partition retaining bucket access, for at least two lease lifetimes | Probe both directions; observe actual proof fallback, retained owner/epoch and renewals; do not demand a majority fence when bucket authority remains valid |
| Isolate owner from peers and RustFS | Capture typed lease fence or distinct replication failure, joined work and surviving executors; survivors audit saved receipts before replay |
| Delay/fail RustFS while peer paths remain healthy | Record available proof paths, lease renewal loss/fence and write failures; no unproven fabricated success |
| Drop technician response after durable external effect | Retry/reconcile the same notification ID; one logical external effect; attempt count and recovery delay retained |
| Redeliver queue message, poison it, pause/resume workflow, fail compensation | One projection/participant effect; configured DLQ route and bounded retries; unresolved saga enters NeedsReview |
| Evict while alarms/streams/background work are active; disconnect slow clients | Wake/outbox coverage survives; cancellation releases resources; no stale-handle success |
| Handoff/restart A, B, C with continuing traffic; adopt compatible generation and deliberately failed factory | Complete SDK proof where expected; old workflows remain compatible; failed adoption preserves serving generation |
| Exhaust shutdown budget, then separately test normal Handoff and Preserve/reload | Incomplete report remains incomplete; normal reports prove event/cell/log/local drain and usable executors; preserving reload retains receipts |

Audit all 10,000 root scopes through owner-routed APIs after traffic drains:

- Domain mutations and their receipts agree; IDs with conflicting digests
  never acquire another result; no acknowledged command is lost.
- Device sequence/history and order transition versions are valid.
- Stock balance equals initial stock + replenishments − consumed reservations;
  reserved equals the sum of held reservations and never exceeds on_hand.
- Projection consumer receipts are unique; source-version watermarks converge
  and snapshot content agrees with authoritative cells.
- Every delivered outbox effect has matching participant/receiver evidence;
  pending/review obligations remain discoverable rather than disappearing.
- Referenced Blob objects have matching length/hash; injected orphans and
  partial multipart uploads reconcile according to retention policy.
- Workflow, queue, alarm and facet obligations survive their required
  recovery boundary; sockets can reconnect and replay from a durable version.

Verify acknowledged receipts **before** replaying ambiguous operations.
Then reconcile issued IDs and repeat the full audit after final preserving
reload. Retry must not conceal a lost acknowledged mutation. Correctness
failure invalidates that capacity claim, even if throughput looks good.

## Capacity criteria and report

Provisional discovery targets below are hypotheses, not Cellar guarantees or
user-approved production SLOs. Freeze the chosen target set before comparison
and retain its version with results:

| Operation class | Proposed p99 target |
| --- | ---: |
| Resident authoritative point read | 100 ms |
| Resident durable command | 500 ms |
| Activated/hydrated point read | 2 s |
| Activated/hydrated durable command | 3 s |
| Automated ticket saga, without deliberate human-delay simulation | 5 s |
| Projection visibility after accepted source command | 2 s |

Report full saga wall-clock latency including injected technician waits as
well as active processing time. Report streaming transfer latency/bytes for
each size instead of applying the point-read target to a 16-MiB object.
Alarm/wake deadlines have their own due-to-start distribution and do not
promise real-time execution.

Define a passing sustainable interval as meeting its frozen latency targets,
at least 99.9% successful expected-valid operations, no correctness failures,
no OOM, no steadily increasing queue/outbox/upload/scheduler backlog, and
adequate resource headroom. Count business conflicts separately but do not
remove unexpected rejection or ambiguity from availability. Target peak
memory below 80% of the node budget and disk below 70%; report CPU throttling
and FD/connection headroom. Apply these criteria through the soak and a
separately reported failure envelope, not just a five-minute peak.

Publish:

1. Offered-load curves: completed reads/s, unique durable commands/s,
   completed business flows/s and bytes/s, with latency and outcome breakdowns.
2. Per-operation/node/locality/activation distributions and sample counts,
   per-repetition variability and time-series resource/backlog observations.
3. One-to-three-node throughput ratio at equal workload and SLO:
   efficiency = three-node throughput / (3 × one-node throughput).
   Compare the same durability posture; do not mix single-node bucket and
   three-node fleet proof as a scale-only claim.
4. Read/write fanout and storage/network amplification per accepted command
   and completed saga; actual proof-source/fallback share.
5. Recovery timelines, full correctness audits, returned node outcomes and
   shutdown proofs, including incomplete/failed cases.
6. A concrete operating envelope: node count/resources, active working set,
   dataset and growth, traffic mix/skew/payload, acceptable rate, dependency
   limits and what becomes saturated first.

Keep run manifest, source SHA plus dirty diff, executable/image digests,
captured policies, scope/owner inventory, seed/checkpoint identity, attempt and
stage ledgers, histograms, resource samples, sampled traces, dependency stats,
fault timestamps, shutdown reports and analysis version under a new private
run directory. Exclude credentials and auth material from exported artifacts.
Set completed=true only after all required measurements and audit stages pass;
always retain incomplete runs.

## Implementation slices and verification gates

Keep the new measuring application under tools/perf, beside the existing
counter/fault fixtures. Proposed layout:

~~~text
tools/perf/field_service/
  main.rs                  application-owned embedded Node composition
  deployment.rs            fixed scope/binding inventory and assets
  device.rs orders.rs inventory.rs
  dispatcher.rs consumers.rs integrations.rs
  schema/                  versioned SQL and seed data
  contracts.rs             commands, receipts, effect envelopes and read versions
  observe.rs               bounded metrics and sampled stage/proof tracing
tools/perf/field_load.rs    independent arrivals and durable client ledger
tools/perf/field_run.py     isolated environment, seeding, phases and faults
tools/perf/field_analyze.py merged histograms, full audits and report generation
~~~

1. **Domain slice:** schemas, deterministic commands, fixed inventory,
   idempotency/versions, read and write endpoints. Verify stock/ticket/device
   invariants with concurrent conflicting commands and actual SDK transactions.
2. **Async slice:** durable outbox/alarm, queue projection/DLQ, workflow saga and
   idempotent technician simulator. Inject crash at participant boundaries;
   demonstrate receipt verification before replay.
3. **Integration slice:** Blob/streaming console, WebSockets, TCP/TLS,
   facets/loader and bounded container profile; run the capability matrix and
   assert documented unsupported outcomes.
4. **Measuring slice:** open-loop generator, ledgers, proof/stage telemetry,
   run manifest and independent analysis. Check histogram merges, missing-data
   rejection, accounting conservation and generator overload behavior.
5. **Fleet slice:** 100-cell smoke, then the exact 10,000-cell ownership census,
   core capacity curves, full application, soak and fault/reload audits.

Use locked Cargo builds and a private checkout-specific target directory on
the mounted workspace volume. Run formatting, native dependency and docs
checks, relevant native integration tests, and existing Python harness proof
guards. For Rust/runtime changes complete the repository's appropriate
workspace check/test/clippy gates. Do not claim external conformance harnesses
ran unless their sources were supplied.

The deliverable from implementation is the working application, generator,
repeatable orchestrator and retained evidence report. This design pass does
not claim that application performance, store conformance, placement balance,
resource headroom or the provisional SLOs have been measured.
