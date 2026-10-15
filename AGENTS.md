# AGENTS.md

Repository guidance for Cellar. Check for a more specific `AGENTS.md` before
working in a subtree. Maintain this file when architecture or verification
commands change; `CLAUDE.md` links here so both tools use the same guidance.

## Framework

Cellar is a Rust framework for applications composed of durable business cells.
Each cell has its own SQLite state. A conditional object-store write assigns
ownership to one node; nodes sharing a bucket form a fleet. The framework routes
calls, captures SQLite writes as LTX, proves durability, and manages alarms,
durable wakes, replication, recovery, and fleet shutdown.

Applications implement contracts from `cellar::sdk` and compile into native
executables with their application factories embedded. The `cellar` CLI builds,
develops, publishes, installs, and operates those applications. `cellar` is the
single application CLI executable; the optional `cellar-dashboard` service serves
the management console. A generic CLI binary cannot serve an application whose
factories were not compiled into it.

Deployment `stream_bindings` use `BindingConfig` selectors. Their `resource.id`
may be omitted for catalog name lookup; runtime `Binding`/outbox lanes always
retain resolved UUIDs. A private namespace/method-bound lookup reads an existing
active resource and validates current grants; it cannot provision or mutate.
The declaration travels in a bounded 128 KiB RPC body with a 64 KiB JSON cap,
not in the authority header. Catalog and partition identity/grants are checked
again before generation-local bindings pin once and admission opens. Name reuse
requires a new generation; old handles cannot emit or seal against successors.
All owned markers remain 1.

Event RPC host outcomes use `node/event_failure.rs` and the invocation RPC gate.
Pre-submission refusal/never-connected transport is `Unavailable`; fenced or
exhausted stale ownership is `OwnershipLost`; lost submitted results and unproven
post-execution output are `Ambiguous`. Authenticated peer host verdicts carry a
bounded header, with unknown/missing verdicts failing conservatively. Existing
DeliveryError and activation transport tags remain in the error chain. Typed
outcomes do not authorize ordinary RPC replay or prove a prior commit absent.
The tunnel pool uses Hyper try_send_request and retries only a returned unsent
request. Buffered bytes alone cannot authorize retry after response loss. Reuse
that exact returned request/body/headers; serialized failures surface as ambiguous.

Event delivery is under implementation; track the complete remaining scope in
`plans/outbox-implementation-status.md`. Native `streams` declarations and
`stream_bindings` are validated in `deployment/event_resources.rs` and require
`event-delivery-v1`. `LoadedDeployment::event_streams` and
`Generation::event_streams` carry desired configuration, not activation proof.
Node startup and adoption reconcile them through `node/event_resources.rs`,
verify current catalog revision/digest and independently resolve every binding
before event admission. Failed adoption preserves the serving generation, but
does not roll back resource changes already committed. Deployed event bindings
require a bucket-backed node or fleet with replication and durable wake discovery;
SQLite-only embedded nodes reject activation until a local recovery inventory
is implemented and qualified.
Outbox dispatch uses immutable stored principal/grant identity after source
proof; current application bindings govern new emission, and receivers repeat
authorization against durable resource grants. Outbox and inbox schemas have
version-1 markers; activation rejects unsupported markers, inconsistent retained
accounting and outbox events exceeding the captured host policy. Cellar is in internal development: all Cellar-owned protocol, format and
schema markers stay at version 1. Hard cutovers are permitted. Do not add legacy
readers, layout migrations, version negotiation or fleet upgrade commands.
Recreate development state and rebuild all participants when layouts change;
mixed builds are unsupported. External codec versions (SQLite WAL and upstream
LTX), package versions and runtime revision/sequence counters keep their meaning.
Telemetry exports use schema marker `1`; KV large-value references and object
prefixes use `v1` with ownership epochs. Recreate earlier development blobs on
cutover; readers and collectors accept only the current layout.
Initial catalog/partition creation requires a host provisioning claim from an
exact captured deployment declaration; proposed grants cannot authorize their
own creation. Ordinary SDK configuration can only administer existing resources.
Keep the remaining qualification gates explicit before release.

Source diagnostics use `Storage::outbox` under the owning activation, with
bounded metadata pages and source-incarnation continuations. Lane retry requires
the captured producer revision and advances its dispatch generation to fence
old replies; the dependency-free core owns that decision. `cellar outbox`
inspect/retry/status/audit uses fleet-HMAC ingress, a distinct source RPC
capability and the original owner/output/discovery gates. Retry operation
receipts and bounded audit pages live in an independent version-1 source
control schema and survive business reset. `cellar stream` describe/read/sources/status/
audit and `cellar subscription` list/describe/pause/resume/reset/delete use signed
`/streams/` ingress and delegated durable grants. Operator authority is limited
to stream control methods, with optional provenance omitted from ordinary event
packets. Subscription receipts retain the operator label and enforce it during
replay/status; ordinary SDK receipts carry no operator. Reads exclude bodies and
encoded envelopes unless explicitly requested. `cellar stream retire` and
`retirement-status` route to the catalog; operator authority also permits only
the matching catalog retirement and partition preflight/closure methods.
Catalog and stream schemas use version 1 with nullable operator provenance.
Current markers require those columns before base schema mutation.
Terminal retirement receipts retain the affected payload range after
bounded cleanup. Subscription inventory uses a bounded primary-key range and includes retained
deleted incarnations without lease tokens or payloads. Its `subs1` cursor is
bound to the stream UUID; every page checks current administration authority.
Pages are live observations, not a snapshot. Destination source inventory uses
`Stream::sources`/`cellar stream sources`, a bounded producer primary-key range,
and a `sources1` cursor bound to tenant/application/stream identity. It shows
accepted watermarks and retained seals, including after payload expiry, without
payloads or tokens. It is not a census of undispatched sources or an all-producer
drain proof. Each page repeats current administration checks. Whole-fleet
resource/source inventories and complete event metrics remain unfinished.
Fixed-cardinality event observations belong to each host execution domain.
`HostServices::event_metrics()` and fleet-authenticated internal `/event-metrics`
export operation counters, histograms and live reservation gauges without
resource identities or payload labels. Timers retain their creating domain.
These observations are not a receipt ledger or cold-cell state census;
resident host aggregate gauges remain unfinished. Fleet root-state collection is
available through `cellar event metrics` and signed `POST /event-state/SCOPE`.
Subscription status and bounded inventory carry owner-sampled lag/lease
observations. Logical unread offsets include lost history; retained unread age
is null when no unread payload remains. A read clamps to the durable clock but
does not advance it or expire leases. Each returned row adds at most one primary-
key timestamp lookup; SDK/operator acceptance checks observation consistency.
This diagnostic coverage does not supply aggregate metric gauges.
Outbox inspection uses transactional per-lane row/byte counters and seven
fixed permanent failure counters, including blocked seal delivery. Explicit
covering age indexes bound source/lane timestamp reads; pages do not aggregate
event ranges. Current blocked reasons clear on retry or accepted delivery.
Restore validates accounting and requires its indexes/triggers. The schema
marker stays 1; prior development layouts require recreated state. SDK/operator
page acceptance checks scope, lane identity, totals, reason set and ages.
`Stream::stats`/stream describe expose current offsets, payload bytes, lease/age
and exact protected/age-blocked retention bytes under administration authority.
Byte-prefix positions are internal ring bookkeeping, not delivery identity;
a retained window stays smaller than its modulus and offsets never wrap.
Restore audits contiguous offsets, sizes, timestamp order and byte positions
before base-schema mutation. SQLite allocation observations use a fixed framework
helper after activation checks; public pragma permissions stay unchanged. Full
WAL/LTX/bucket physical usage and aggregate export remain unfinished.
Stream maintenance only schedules age reclamation for an eligible first record;
required checkpoint protection can quiesce age work entirely. Live/expired lease
cleanup retains its expiry wake. Checkpoint/lifecycle/configuration transitions
recompute deadlines in their transaction; configuration cannot leave newly
eligible age work dormant. Indexed minimum checkpoint, at most 129 lease probes
and one oldest record bound scheduling independently of retained payload size.
Use durable logical time, preserve overflow refusal, and keep bounded continuation
intervals plus independent application alarms/work reasons.
Catalog inventory uses `cellar stream list` with a separate
fleet-administration namespace capability and `/catalogs/` ingress; resource
grants cannot mint it. Owner pages select at most 101 scalar identity/lifecycle
rows, including tombstones. `catalog1` continuations bind the namespace and tail.
Catalog identity and accepted/desired revision columns commit with configuration
state; the schema marker stays 1 and older layouts require recreated state.
`cellar stream retirement-proof --resource RESOURCE.json` uses an exact-resource,
read-only platform capability through the same catalog owner path. It requires
a terminal identity and matching committed retirement receipt, returning scalar
identity metadata without grants or event bodies. Resource grant revocation does
not prevent platform verification; this capability cannot mutate resources.
Source destruction uses a durable source lifecycle and captured lane/work
snapshot under the source control schema, still marker 1. Private source RPC and
outbox work may continue after business admission closes; ordinary contexts,
retained storage/cursor/inbox/facet handles and suspended turns keep their captured
source generation. Restore and instance reset reconstruct the durable fence.
`cellar outbox destroy/recreate` require exact incarnation, operation, reason and
explicit CLI intent; destruction also requires a loss acknowledgment. Retirement
evidence commits before deletion; stale verification and dispatch replies cannot
advance a replacement. Event/inbox/lane cleanup is batched, business reset uses its
existing transaction, receipts/tombstones survive and recreation mints fresh
producer identity. Source roots register top-level physical facet names durably before opening.
Destruction retires child activations and reclaims registered subtrees in at most
100 replica objects and, after remote completion, at most 100 local files or
directories per host batch, keeping each registry row until completion. Local
cleanup uses `FileSystem::remove_dir_batch` on the owning blocking executor;
custom filesystems must implement that resumable, no-symlink-following contract.
Runtime-only facet fixtures must supply their per-domain host-gate adapter because
registration writes parent storage. Synthetic fixture responders test API/policy
contracts and are not fleet durability evidence.
Recreated sources qualify facet storage names by producer incarnation; logical
names and startup identity remain unchanged. Native opens carry a captured
admission check across host waits and restore, and serialize with subtree cleanup.
An authenticated unfinished destruction replay re-arms durable cleanup before proof;
terminal replays leave successors untouched. Injected lost replica-delete replies
and complete owner handoff preserve closed admission and exact discard receipts.
Large local-directory/SQL reset capacity and the complete source fault/lease/policy
matrix remain unfinished. Do not infer full release qualification from the base-database tests.

Cold source recovery can use a separate framework-only worker pool when its
application class is absent. It accepts only restored epochs with retained
source identity, repeats schema/host-limit validation and preserves the actor's
restore, VFS, ownership and proof path. It has no application bindings or
handlers. Internal work and source controls execute before rejecting business
calls; unavailable application alarms back off without consuming abandonment
budget. The pool participates in generation retirement, census and shutdown.

Library applications start `sdk::Node` with explicit `NodeConfig`,
`NodeServices`, a `DeploymentGraph`, and `CompiledApplications`. The caller
supplies listeners, executors, clients, authentication, and any fleet services.
Axum hosts use `Node::start_external` with `ExternalNodeServices` and mount
`cellar_axum::CellarService` on their already-bound public listener. Application
handlers and body producers retain SDK event authority; only the node driver
polls admitted ingress in its owning domain. Keep the private peer listener.
Each node owns an isolated execution domain; library defaults do not inspect
process environment. The CLI captures environment settings at startup.
Queue leases and actor settlement share the host's captured operation deadline.
Embedded bootstrap/adoption generation builds and metrics sampling use fallible
blocking admission. Closing a domain can race a later effect in an already-entered
poll; return `DomainClosed` without invoking the refused factory/sampler. Started
blocking work still joins before releasing the node directory. Metrics maintenance
ends on closed admission. Keep startup/directory deadlines and assertions intact;
cancellation diagnostics independently distinguish retained locks from pending
replacement bootstrap. A fixed admission panic does not by itself explain an
observed bootstrap deadline failure.
Lower-level library hosts pin a 15-second default on first use; configure actor
policy before constructing generations to choose another budget. CLI startup
captures its environment override before any generation can sample that budget.
SDK HTTP uses `NodeConfig::http` and an optional `NodeServices::http_client`
builder factory. Lower-level hosts configure HTTP through `HostServices` before
the first fetch. Keep required redirect behavior and invocation subrequest
accounting in Cellar; factories choose DNS, TLS, proxies, credentials and pools.
Library HTTP defaults ignore ambient proxies. Domain closure drops cached pools
and prevents their reconstruction.
Asset loading receives an explicit `AssetCache` with directory, cleanup target,
file-size policy and optional caller filesystem. Generations, cohosted scripts
and CLI reloads retain that capability. Asset streams, fallback refreshes and
maintenance belong to their serving execution domain; closure joins begun
blocking file work. CLI cache options are captured once under the node data
folder unless an explicit environment override chooses another directory.

The workspace must remain free of JavaScript engines, including with all
features enabled. Source JavaScript, Node.js modules, and the former Python/Wasm
wrappers are not executable application formats. Cellar starts with fresh
configuration and state; Celld configuration aliases and state migration are
unsupported. Framework SQLite tables reserve the `__sys_` prefix.

## Repository map

```text
crates/
  cellar/                 Production effect adapters, host, and application SDK
    cli/                  Entry points, commands, packaging, dev, environment parsing
    application/          SDK bindings, storage, transactions, and application APIs
    actor/                Serialized core driver and effect execution
    execution/            Execution domains, injected services, and task ownership
    node/                 Startup, supervision, managed integration, and shutdown
      network/            Node WebSocket ingress and authenticated peer tunnels
    runtime/              Generations and application execution
      cells/              Cell activation, dispatch, and facet lifetimes
      workers/            Compiled workers, pools, loading, and configuration
      invocation/         Event context, gates, turns, channels, and service calls
    transport/            HTTP, TCP/TLS, and WebSocket adapters and registries
    storage/              SQLite, object-store clients, and Blob storage
      sqlite/             Activation-bound state, cursors, transactions, alarms
      object/             Bucket clients and local object-store implementation
      blob/               Shared Blob backend, multipart, and streaming
    durability/           Replication, fleet logs, recovery, and proof task ownership
      replication/        LTX backend, handles, storage clients, and policy
      log/                Authenticated node logs, follower proof, and recovery
    fleet/                Leases, peer authentication, handoff, and durable wakes
    deployment/           Shared manifests, artifact protocol, deployment loading
    containers/           Cell-owned container service and Docker adapter
    observability/        Telemetry export and OTLP encoding
    platform/             Machine and process-memory adapters
    tests/                Native SDK and host integration tests
  cellar-axum/            Axum handlers, external ingress service, shared builder, OpenAPI
  cellar-admin/           Browser management API, authentication and operation audit
  cellar-logic/           Dependency-free decision core
  cellar-ltx/             SQLite WAL capture, LTX codecs, restore, and compaction
  cellar-native/          Owned values, structured clone, crypto, and web primitives
  cellar-fleetcare/        Field-service app, embedded/daemon hosts and open-loop client
examples/                 Compiled Rust applications and project configurations
dashboard/                Vite, React and shadcn/ui browser management console
docs/                     Builder/team guides, evidence, decisions, and SVG explorer
tools/                    Formatting, dependency checks, documentation, and fleet tooling
.github/workflows/        SDK checks and release builds
```

Cellar groups its implementation into component directories next to
`Cargo.toml`; `lib.rs` declares the existing Rust module names. The logic and
native crates retain flat crate roots, and `cellar-ltx` uses `src/`. Cargo
declares the CLI entry point as `crates/cellar/cli/main.rs`.

CLI files retain their existing Rust module paths through `#[path]` declarations
in `crates/cellar/lib.rs`. For example, `cellar::native_deploy` is implemented in
`crates/cellar/cli/native_deploy/mod.rs`. Shared package configuration and
publication helpers also serve the SDK and host. Keep those callers in view
when editing CLI tooling. Shared bucket, fleet, protocol, and runtime adapters
remain outside the CLI directory.

Physical folders describe component responsibilities, while Rust paths retain
their existing names through `#[path]` declarations. For example,
`cellar::daemon` is implemented in `node/mod.rs`, `cellar::runtime` in
`runtime/mod.rs`, and `cellar::asyncrt` in `execution/domain.rs`. Keep module
visibility, conditional-build selection, and exported macro paths stable when
moving files. Node ingress modules under `node/network/` remain logical
children of `daemon`.

Only `lib.rs`, `sdk.rs`, and Cargo's `build.rs` remain as Rust files at the
Cellar crate root. Place new implementation helpers beside their owning
component and use private child modules where appropriate. Name shared helpers
for their contract and owner rather than collecting unrelated code in `utils/`.

SQLite alarm records stay with `storage/sqlite/`; fleet wake discovery and
response gates live under `fleet/wake/`. KV large-value and Queue policy/batching
helpers sit beside their built-in cells under `application/kv/` and
`application/queue/`, retaining their original crate-root module names.

Workflow batch/configuration, compensation, history, and byte-stream adapters
live under `application/workflow/`. Their journals and chunks share the owning
instance's SQLite state and output gate. Workflow subscriptions and stream
readers retain their opening invocation; continuation reads do not consume a
new subrequest slot. Workflow binding definitions are bounded, non-executable
diagrams. Create/restart snapshots and content digests share the instance transaction and retention;
history events retain their execution generation. Inspection inputs are explicit
`StepConfig::input` values, redacted with sensitive outputs. Definition versioning
does not pin executable versions or change replay policy.
Native workflow packages retain the `workflows-v1` feature while Cellar is in
internal development.
Use `native_workflow`, `native_workflow_cell`, `native_cron`, and
`native_artifacts` when changing these contracts.
Dashboard cron overrides live under `config/cron/`, keyed by deployment prefix and
declared expressions. Conditional writes guard the observed schedule revision.
The loader and standalone watcher share resolution; embedded hosts explicitly
adopt a loaded graph. `LoadedDeployment::cron_revision` records an adopted override,
including an empty schedule that must retire an existing alarm. Do not infer a
successful scheduled occurrence from generation adoption. Bounded execution observations
live in `application/cron/history.rs` in the scheduling cell's own SQLite/output
gate; retain started attempts without terminal evidence after interruption. Only
the primary application owns that cell. The cron class remains inspectable after
schedule removal, while `Generation::cron_cell` determines active adoption.
Cron inspection uses the authenticated reserved-cell route, a fixed read-only
`__cronHistory` RPC, and `data_read`;
anonymous `/do/` requests must not expose its journal.

The layout preserves three mixed modules for later focused refactors:
`node/mod.rs` combines shared node composition with process startup;
`node/managed.rs` combines managed services with connection/credential commands;
`deployment/fleet.rs` combines deployment loading with operator orchestration.
Shared package configuration and publication helpers still live under `cli/`
and serve the SDK and managed host. Separate those responsibilities before
changing their public interfaces. `platform/machine.rs` also retains its
existing identity, default-policy, and explicit environment adapters.

## Ownership boundaries

- `cellar-logic::on_event` advances behavioral state and returns effects. The
  core has no async work, I/O, ambient clocks, randomness, locks, or dependencies.
  Put lifecycle, routing, ownership, scheduling, and durability decisions here.
- The serial actor owns the core. Adapter futures execute effects and return
  versioned completion events through its mailbox; they do not mutate core
  state or implement competing policy.
- The execution boundary uses `cellar::asyncrt` and injected host services for
  tasks, timers, clocks, randomness, and node filesystem operations. Read
  `clippy.toml` before touching these paths. Direct host operations in CLI/build
  tooling need an explicit, justified boundary exception.
- Node policy and services belong to their execution domain. Keep actor,
  runtime, cell API limits, loader, replication, log, and shutdown configuration
  explicit in library APIs; environment resolution belongs in CLI adapters. Capabilities
  and spawned work must retain the domain that created them.
  `CellLimits` is pinned before generation construction or SQLite activation;
  preserve host isolation and receiver-side budgets when extending API limits.
- Transport registries and binding adapters retain their execution-domain and
  event ownership. Their location under `transport/` does not make sockets,
  HTTP streams, or upgrade capabilities independent of invocation gates.
- `cellar::application` implements application contracts and binding adapters;
  `cellar::sdk` is the application-facing namespace. Storage and context handles
  carry event/activation authority. Retiring an activation must invalidate its
  handles and wake or cancel outstanding work.
- `cellar-native` owns engine-independent values and codecs. `cellar-ltx` owns
  SQLite replication mechanics; fleet ownership and output-gate policy belong
  to the framework, not to the LTX codec.
- `durability/` groups framework replication and node-log adapters. Keep one
  `DurabilityOwner` for the coupled replication, log, and follower tasks;
  shared storage clients do not acquire lifecycle or ownership authority.
- Operator SQL, KV, and Queue commands use the authenticated fleet route to
  reach the owning cell. Never mutate their persisted SQLite state behind the
  owner. Blob commands reuse the binding's `blob_store` implementation.

## Contracts to preserve

1. A cell has one authoritative owner and epoch. Stale completions, leases, and
   storage handles cannot authorize work for a later activation.
2. Externally visible writes and egress pass through the output gate. Prove the
   required write position and ownership before acknowledging success; a
   durability or fencing failure must fail the operation.
3. Recovery must account for the prior owner's replicated log before takeover.
   Eviction, handoff, and shutdown cannot discard unproven writes or required
   wake/alarm coverage.
4. Transactions, cursors, sockets, reservations, and background tasks retain
   their owning lifetime and release resources on cancellation and failure.
5. Persisted protocol fields, cell identities, bucket keys, LTX formats, and
   feature gates are shared contracts. Search readers, writers, and consumers
   before changing them. Read `crates/cellar-ltx/README.md` and
   `crates/cellar-ltx/reference/ltx-format.md` for format changes.
6. Native packages validate their execution format, host protocol, target,
   executable digest, and size. Installation verifies downloaded bytes and
   refuses to overwrite an existing executable.
7. CLI stdout carries command data; progress and diagnostics belong on stderr.
   Use `cellar::cli_output::Output` and `note!`. Preserve JSON output, bounded
   listings, continuation information, and successful closed-pipe handling.
8. Recover predecessor logs before replacing a node lease. Keep the node's
   directory lock and caller-owned executors alive until owned work joins.
   Both stop modes drain complete event drivers, including post-response
   background work and queued SDK calls, before changing cell lifecycle.
   Progress may extend the stall window, never the absolute stop deadline.
   Handoff progress counts terminal adoption completions, including failed
   acknowledgements after ownership release; retry attempts are not progress.
   Successful adoption telemetry remains separate from that completion count.
   Forced cancellation supplies no graceful durability proof; report incomplete
   drain phases through `ShutdownReport` and return `NodeOutcome` to library
   callers. Process exits belong to the CLI boundary.
   Domain closure can race a task's current poll. Production actor selectors
   use `try_start`/`try_step` and fallible effect admission; cancellation must
   not become an invariant panic or a fabricated completion event.
   Reserve tracked work before releasing the task registry lock to dispatch
   it to an executor. Executor rejection can synchronously destroy work;
   never dispatch while holding that lock. Rejected async/blocking captures
   retain their original domain, and a racing close must retain cancellation
   and join coverage until every reserved task has released its resources.
   Preserve the core's typed fencing reason in `FenceReport` and
   `NodeOutcome::Fenced`. A closed reporting channel is a supervision failure,
   never evidence of an authority loss.
9. Idle, unowned cell routing history uses the actor's captured
   `max_cached_routes` limit. Reclamation cannot discard ownership, alarms or
   unfinished work. Every cell-table replacement maintains the recency index;
   direct retirement paths must refresh it. Stale completions and queued timers
   cannot affect a new request after its old routing record was reclaimed.

## Build and validation

The manifests use Rust 2021 and declare Rust 1.94.1 as the minimum. CI selects
its toolchain in `.github/workflows/native-sdk.yml`; inspect that file rather
than assuming the local toolchain matches CI. Dependencies are centralized in
the root `Cargo.toml`; member crates use workspace dependencies. Keep builds
locked and review any intentional `Cargo.lock` change.

On the development machine, place compilation artifacts on the mounted
workspace volume. Assign a separate, stable target directory to each checkout
or worktree, set it for each shell invocation, and confirm the volume is
writable. Do not reuse or clean another checkout's artifacts. CI may use its
runner's default target directory.

Run these commands from the repository root, with `<checkout-name>` replaced
by the current checkout's identifier:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
cargo format --check
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo check --workspace --all-targets --all-features --locked
cargo test -p cellar --test native_cli --test native_artifacts --locked
```

`cargo format` (alias for `cargo xtask fmt`) applies stable rustfmt plus the
repository's declaration spacing. `cargo format --check` checks the same result
without writing source files; CI runs it before tests. The tool lives under
`tools/xtask` and covers tracked and unignored new Rust files, including examples.
Run `cargo test -p cellar-xtask --locked` when changing formatter behavior.
Plain `cargo fmt --all` remains available for rustfmt alone.

For Axum adapter changes also run both feature modes and the shared example:

```sh
cargo test -p cellar-axum --no-default-features --locked
cargo test -p cellar-axum --all-features --locked
cargo test -p cellar-axum-counter --all-features --locked
cargo check -p cellar-axum-counter --no-default-features --bins --locked
```

Read `crates/cellar-axum/README.md` for middleware, body and WebSocket lifetime
constraints. Cellar WebSocket handlers use `CellWebSocketUpgrade`; ambient
Axum upgrade tasks do not carry SDK authority. Each acknowledging body chunk
uses the existing output gate. OpenAPI is an optional immutable declaration
owned by the same application generation; export must not start a node.

Choose additional existing integration tests for the changed bindings, host
behavior, or storage paths. The full SDK CI gates are:

```sh
cargo test --workspace --all-features --lib --tests --locked
cargo test --workspace --all-features --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

`cellar_internal_tests` enables externally supplied simulation/conformance
sources through `CELLAR_INTERNAL_*` and `CELLAR_CONFORMANCE_*` paths. Those
sources are not all checked into this repository. Do not enable the flag
without the corresponding harness, or claim those tests ran in a normal build.

Exercise user-facing changes through the CLI or a compiled application when
practical. The local counter smoke test is:

```sh
cargo run -p cellar --bin cellar --locked -- dev examples/native-counter/project.json
# In another terminal:
curl -X POST http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/counters/alpha
```

`tools/perf/README.md` describes isolated Docker/Colima validation of the
embedded `sdk::Node` path, actual RustFS recovery, rolling handoffs and returned
shutdown proofs. `python3 tools/perf/fleetcare.py` builds the separate
`cellar-fleetcare` application and runs its 10,000-cell read/write matrix in
embedded and daemon modes with Bucket and Fleet durability. Use
`--profile smoke --suite smoke` for a short functional check; the default
standard suite additionally verifies crash, store outage and cold recovery.
Its separate `faults.py` driver exercises process failures
during writes with an application receipt ledger. Verify acknowledged receipts
before replaying ambiguous requests; otherwise retry can hide lost writes.
The driver's final audit rechecks saved ledgers from each verification stage.
Its isolation scenario disconnects a node from peers and its bucket; distinguish
the SDK's typed lease-expiry fence from a replication failure, and require joined
owned work with both caller executors alive before restarting that directory.
The peer-partition scenario changes only the owner's private network namespace:
peer paths fail in both directions, bucket paths and other peers stay reachable,
and the owner renews its lease without changing generation or cell epoch.
Keep image tags and compiled target caches private to the
checkout. Run the harness proof guards with
`python3 -m unittest discover -s tools/perf -p 'test_*.py'`.

The dev supervisor retains project state under `.cellar/<project-hash>` beside
the configuration, keyed by its canonical path. It does not migrate earlier
development-state layouts.
`--clean` intentionally resets that project's local state. Container examples
require Docker or Podman. See `examples/README.md` for application packaging and
other runnable examples.

`docs/README.md` indexes builder, platform, operations, and team documentation.
Keep source-backed behavior distinct from proposed practices and release-specific
qualification gates. `python3 tools/docs/build.py` regenerates the catalog,
offline explorer, and SVG sources; `python3 tools/docs/check.py` validates their
local references and coverage. Browser-only documentation scripts are independent
of the Rust application execution format. Use the optional preview renderer in
`tools/docs/render.py` only when generating PNG previews; it requires an existing
`agent-browser` installation and is not an application or CI dependency.

`cellar-native-examples` uses `examples/main.rs` to compile the focused modules
into registered applications; `examples/project.json` configures the combined
cookbook. Standalone native hosts keep `main.rs`, `project.json`, and their assets
in their recipe directories. Each recipe has a project config;
`examples/catalog.json` records sources, routes and prerequisites. Runtime smoke
suites use private temporary project state:

```sh
cargo build -p cellar --bin cellar --bin cellar-native-examples \
  --bin cellar-example-embedded --locked
python3 examples/focused_smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" --with-cron
python3 examples/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar"
python3 examples/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --project examples/team-services/project.json --suite team
python3 examples/platform_smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --embedded "$CARGO_TARGET_DIR/debug/cellar-example-embedded"
```

`cellar-example-embedded` runs `sdk::Node` with local SQLite and checks returned
drain phases. `cellar-example-container-peer` validates the dependency-free Rust
peer built inside the example container. Native pattern equivalents do not imply
third-party SDK compatibility; keep gaps in `examples/ROADMAP.md` explicit.

`cellar-example-streams` is a separate bucket-backed `sdk::Node` host. It provisions
required stream retention, commits order state with `tx.emit`, composes projection
effects with inbox receipts and syncs before remote acknowledgment. Its explicit
retry endpoint plus preserving process restart demonstrate redelivery without
duplicate local effects. The private bucket also retains the randomly generated
fleet secret. Verify the real binary with `python3 examples/durable-streams/smoke.py
--binary "$CARGO_TARGET_DIR/debug/cellar-example-streams"`; this is a local demo,
not production crash/response-loss or multi-node capacity qualification.
Its separate `examples/durable-streams/faults.py` opt-in harness records an
independent fsynced ledger, drops a real source HTTP response and observes
SIGKILL with a pending source obligation and proven consumer effects before
ack. It audits before replay, preserves lease expiry, archives accepted envelope
digests/effect receipts and re-audits saved stages. Run its oracle guards with
`python3 -m unittest discover -s examples/durable-streams -p 'test_*.py'`.
This is single-node SQLite-bucket process evidence; it does not complete the
destination/follower/network/power/cut-point/capacity release matrix.
The serial Node suite also places destination, producer and consumer in distinct
execution domains. A real loopback peer tunnel proxy captures a proven append
reply, holds it through an independent source/destination audit and closes both
sockets. Retry bytes and acceptance match exactly; one destination offset and
one inbox-protected effect survive. The same proxy can lose a proven acknowledgment
reply: the consumer stores the exact batch receipt atomically alongside its
retained inbox effect before proof, then replays unchanged receipt bytes after
independently auditing checkpoint 1 and no live lease. Replay returns
`AlreadyAcknowledged` without another effect or checkpoint change. This is actual
network reply loss between embedded nodes, not separate-process/host, SIGKILL or
power qualification.
`examples/durable-streams/before_send.py` holds the source's actual input gate,
proves its command/obligation and SIGKILLs before the first dispatch attempt.
Copy the killed SQLite family and audit zero attempts before restarting, then
audit recovery before replay. Audit-only mode checks saved database/WAL hashes,
envelope identity and replay ordering; SHM read marks are mutable bookkeeping.
Keep the original handler/gate deadlines and complete final shutdown assertions.
`examples/durable-streams/fleet_faults.py` uses opt-in bounded loopback fleet
configuration to start separate destination/producer/consumer processes sharing
a private SQLite bucket. The proxy holds a proven append reply through audit,
SIGKILLs the destination and blocks automatic retries until recovered acceptance
is audited. Consumer transport ambiguity is audited before retry and preserves
original lease expiry. Saved evidence validates exact peer byte digests, process
roles/IDs and complete surviving/replacement shutdown. This is separate-process
single-machine evidence, not physical-host/follower/power/capacity qualification.
The same driver accepts `--rustfs-image` to create a fresh private Docker S3
server and bucket. It captures image/backend identity, retries only startup
CreateBucket 503s, injects separate no-retry conditional-write clients and removes
its own server after complete node shutdown. Credentials stay outside saved
configuration and ledgers. This adds network-store destination recovery evidence;
it does not prove follower-only acknowledgment, store loss or physical-host loss.
`capacity.py` fills one opt-in Backlog source/lane to its captured row quota while
append traffic is paused. Overflow must roll back business state and sequence.
It audits before producer SIGKILL and before resumed retry, without source POST
replay, then uses resumable reads and a slow required consumer to verify every
acceptance, inbox effect and checkpoint. Unarmed peer errors must be forwarded
unchanged so advertised batch limits reach the dispatcher. Saved evidence must
pass the independent oracle and include complete final shutdown. RSS/residency
and live local file sizes are observations, not aggregate/bucket physical metrics,
memory SLOs, many-cold-source or long-soak qualification.
`many_sources.py` exercises independent Order sources beyond one host's resident
limit. Required retention, rather than only a network proxy, backpressures local
and remote appends even when source ownership moves. It archives the protected
prefix, audits remaining obligations, SIGKILLs every fleet process and starts all
replacements before waiting for readiness. No source requests occur before full
consumption. Its saved oracle and cleanup-failure guard are mandatory. Activation
retries only tagged network failures for its idempotent configuration or read-only
resolution/status calls, within the original activation deadline. It encodes the
authority/body once and uses a new dispatch identity per attempt to avoid prior
abort tombstones. General RPC still refuses ambiguous replay; protocol/authority/
codec refusals are not tagged for this loop. Preserve failed and passing evidence
in the implementation status: a passing replay alone cannot establish an earlier
failure's cause or justify relaxing readiness/durability/deadline/shutdown checks.
Startup boot/periodic wake discovery now waits for the captured generation's
event-resource activation before doing store reads or submitting cold hints.
Both embedded and CLI paths use `node/wake_discovery.rs`; predecessor recovery,
node authority and authenticated internal serving retain their existing order.
Failed/cancelled activation cannot start discovery; hosts without event activation
do not wait. Durable publications remain intact and normal discovery resumes
after readiness, including removed-class delivery. Gated reply task admission
uses try_spawn: refusal drops reply/value/gates in their owner and returns a closed
completion to the driver, without unproven output or a closure panic. The trace
identified startup source-wake pressure and the concrete alarm reply spawn caller;
untraced 144/192-source RustFS cases pass after these fixes. Wider maintenance
fairness and failed-startup/shutdown cut points retain their separate audits.
`whole_fleet.py` explicitly selects bucket proofs, audits one accepted event with
a proven unacked inbox effect plus one capacity-blocked source obligation, then
SIGKILLs all three processes. It inventories/discards only its private node data
after every kill is observed and audits bucket-restored receipts/state before
consumer replay. No source POST repairs recovery. Replay adds zero old effects;
ack releases capacity for the pending event and final shutdown is complete.
SQLite/RustFS results qualify this boundary, not follower-only/power/store loss.
`ack_faults.py` captures a proven `__streamAck` reply, audits checkpoint/effect/
receipt before destination SIGKILL and reply loss, then audits recovery before
exact stored-receipt replay. Opt-in consumer routes atomically save the receipt
with its effect and sync before ack; replay never fetches a new batch. A stale
pooled transport requires another saved audit before retry. Exact bytes and
`AlreadyAcknowledged` plus unchanged effect/checkpoint qualify this boundary;
consumer/follower/power/store loss and the full lease/ack matrix remain open.

`cellar outbox census [CLASS]` reads one bounded provider cell-directory page,
then performs up to four concurrent signed source-owner inspections, including
producers unseen by destinations. `census1` continuations bind fleet/prefix/class;
filtered pages still advance without an unbounded listing. Catalog/stream cells
are excluded, other framework roots stay candidates. Rows distinguish current
source, empty and unavailable; partial failure prints its cursor and returns a
failed exit. Metadata excludes lanes/payloads/tokens. Cold inspection can restore
and progress durable work. This is live root-cell census, not a snapshot/drain
proof, facet/historical-source inventory or aggregate gauges. The RustFS CLI
smoke is `census_smoke.py`; keep census scope distinct from destination watermarks.


## Change discipline

The dashboard is an optional management application, separate from native cell
execution. Browser JavaScript does not authorize adding a JavaScript engine to
the Rust workspace. `fleet/management/` and `fleet/operator.rs` supply shared
metadata observations and owner-routed requests. The console must not open cell
SQLite files or expose the private peer listener. `cellar-admin` owns its separate
audit database; authentication, permissions and fleet configuration are explicit.
Node stop acceptance is not shutdown proof. Signed stop requests bind the node,
generation, requested mode and operation identity. Final receipts in the fleet
bucket require the returned drain report and an explicit execution-domain join;
CLI hosts also join their dedicated core thread. A lost reply or failed receipt
publication remains unknown. Audit reconciliation must preserve a terminal proof
even when an earlier acceptance response arrives later.
KV console inspection and conditional edits live under `application/kv/inspection.rs`
and `fleet/management/kv.rs`. Every namespace put assigns a persisted revision;
compare it inside the owner transaction, including expiry after blob publication.
Conditional console requests also pin the inspected binding identity across deployments.
Console edits preserve exact expiry and storage tags, and distinguish absent
metadata from JSON null. Definite revision conflicts are rejected; lost write
replies remain unknown. Keep the existing unconditional SDK/CLI signatures.
See `plans/003-fleet-dashboard.md`, `plans/004-sql-kv-workspaces.md`, and
`plans/005-resource-inspectors.md` for
implementation progress and release gates.

For dashboard changes, also run:

```sh
cargo test -p cellar-admin --locked
npm --prefix dashboard ci
npm --prefix dashboard run build
npm --prefix dashboard run lint
npm --prefix dashboard run test:ui
```

Browser fixture tests do not prove live-fleet behavior. Operator and lifecycle
changes additionally need the corresponding SDK and isolated fleet tests.

- Read the changed module, its callers, adjacent implementations, and relevant
  tests before choosing a fix. Verify dependency contracts from their source or
  official documentation when the behavior depends on them.
- Make one clear implementation of the requested behavior. Keep unrelated
  cleanup out of the change, and avoid speculative options or extra adapters.
- Preserve public Rust APIs during file-only reorganizations. Update Cargo
  entry points, module paths, embedded-file paths, and documentation together.
- Follow surrounding Rust style. Propagate recoverable failures with `Result`
  and useful context; do not introduce input-triggered panics. Avoid blocking
  the actor or holding synchronous locks across `.await`.
- Separate import blocks, types, implementations, and functions (including
  methods) with a blank line. Keep documentation and attributes attached to
  their declaration. Expand dense configuration, closures, and macro payloads
  into readable lines. Use `cargo format` to apply standard layout and declaration
  spacing together.
- Use `asyncrt::select!` for fair polling. A biased select needs a reason tied
  to lifecycle ordering. Never bypass the forbidden macros in `clippy.toml`.
- Tests should exercise behavior and regressions. Do not weaken checks, alter
  expected results, or suppress lints merely to hide a failure.
- Keep credentials, deploy tokens, `.dev.vars`, and live fleet data out of
  commits and tool output. Use local test stores for destructive checks unless
  the task explicitly authorizes an external fleet operation.
- Preserve license and attribution notices when moving or adapting source.
- Before handoff, inspect the diff, run checks suited to the changed surface,
  and report the outcome plus any unverified behavior. Commit or push when
  requested, keeping the user's unrelated work intact.

This guide follows the organizational approach of
[Crab's AGENTS.md](https://raw.githubusercontent.com/crabbuild/crab/refs/heads/main/AGENTS.md),
with Cellar's architecture and commands derived from this repository.


Fleet root metrics use a separate scope/method-bound platform StateAuthority and
`runtime/event_state.rs`, under ordinary owner/output/discovery gates. Native
sampling bypasses application invocation, including removed-class recovery, and
reads indexed source counters and at most 129 live subscription metadata rows.
Deleted stream partitions remain observable and have a separate retired count.
Source/stream gauges and root SQLite allocation are canonical decimals; aggregate
sums use checked u128 and ages take maxima. All owned markers remain 1.
`cellar event metrics` scans at most 100 provider prefixes per page and probes four
owners concurrently, within its max-roots/page bound (default 10,000; cap 100,000).
Duplicate/invalid prefixes, failed probes, failed/repeated listing tokens or an
exhausted bound make collection incomplete. Incomplete Prometheus emits coverage
only; JSON puts partial values in observed_gauges with gauges=null and a failed
exit. Complete means a successful live root-directory traversal, never a snapshot
or drain proof. IDs are absent from global labels. It does not include facet
SQLite/WAL/LTX/bucket physical bytes, historical producer incarnations, or resident
host gauges. `census_smoke.py` also exercises actual RustFS CLI state collection,
protected/undispatched state, caps and unavailable owners. Keep these scopes explicit.


`cellar event inventory` discovers one bounded provider page of root descriptors,
with four signed owner probes and an inventory1 cursor bound to fleet bucket/
prefix/endpoint/region. Invalid, duplicate or unavailable roots remain explicit;
partial pages print their continuation and return failure. Catalog descriptors
recover namespace identity from scalar retained rows and validate its hashed scope;
empty catalogs cannot invent a namespace. Stream descriptors recover only resource
identity; ordinary roots expose current incarnation and receipt-history availability.
`event inventory SCOPE --page resources|producers|history` reads at most 101 scalar
rows for 100 returned records. A distinct exact-scope/method InventoryAuthority and
native framework handler precede application invocation and retain owner/output/
discovery gates. Resource/producer pages share existing validated range readers.
Source history projects retained operation identities, source incarnations,
recreation predecessor, lifecycle and loss counts without full receipts/operator
reasons/credentials. History1 binds root scope and exact sequence tail and remains
valid across supported recreation because the receipt ledger is retained.
Stream retirement deliberately purges producer metadata after terminal tombstones;
producer replies carry retired/cleanup_complete so an empty terminal set is not
mistaken for an active producer census. Pages are live, not snapshots/drain proofs.
Facet inventory, every historical restore/backend/ownership/capacity cut and
complete release qualification remain separate work. All owned markers stay 1.


Subscription fetch acceptance compares the receipt with the identity resolved for
that fetch, including subscription UUID and incarnation. Raw and leased reads
share the bounded envelope decoder: remaining caller bytes bound base64 allocation,
and family/source/incarnation/lane/event identities are independently checked.
Leased replies must match the exact contiguous receipt range and a nonnegative,
positive lease interval within captured lifetime limits. Unknown application event
schemas remain readable; processing authority never depends on application decoding.
Negative extension deadlines and retry delays outside host policy fail before RPC.
All owned markers remain 1; full codec, cancellation, lifecycle/fault qualification
remains tracked in plans/outbox-implementation-status.md.


Wake projection admission rejects previous alarm-only __sys_cell_wake_* triggers
through wake_record::check_schema before base-schema DDL, journal pragmas or writer
epoch updates. It never drops/replaces those triggers as an upgrade. Recreate that
development layout; all markers remain 1. Current projections preserve same-epoch
publication identity and consumed tombstones; a new owner advances the epoch while
retaining the monotonic sequence and independent alarm/work deadlines.


SDK event administration independently validates response metadata. Configuration
acceptance binds resource/operation and active definition revision/digest; pending
creation requires revision 1, while status may report later operations. Lifecycle
receipts bind action/reason/operator and identities, validate state/incarnation/
checkpoint/loss-range consistency, and retain exact decimal positions. Audit pages
allow monotonic sequence gaps and multiple authorized principals but enforce
bounded counts/receipt bytes, scope, unique operations and exact continuation.
Terminal retirement replies validate retained range/count/byte accounting; pending
intent can precede captured loss metadata. Retry responses require canonical,
nonnegative deadlines consistent with delay. All owned markers remain 1. These
checks do not establish full transport allocation/cancellation or failure-matrix
qualification; track remaining evidence in plans/outbox-implementation-status.md.


Shared event reply acceptance consumes exactly one ok/error branch; contradictory
or undecodable submitted results are Ambiguous. Wire bytes are capped at 64 MiB
before JSON/structured decode; encoded error metadata is capped at 16 KiB and
message text at 4096 bytes. Valid typed errors retain their exact metadata.
Do not clone a whole success/error object or reflect malformed body text in an
error. Peer event/control body collection uses that same wire ceiling and the
original route operation deadline. Coalesce frames into one bounded buffer, drop
unused trailers and yield bounded frame/byte work so ready frames cannot starve
cancellation or deadlines. Size/timeout refusals are Ambiguous without
RemoteRpcTransportFailure; genuine body I/O errors retain the activation-only
network tag. Cancellation wins undelivered body/deadline ties and releases the
body. The proven response-head abort disarm remains intact; collection failure
cannot kill the handler's later waitUntil work. Full request-phase allocation and
cancellation qualification remain in the implementation status.
