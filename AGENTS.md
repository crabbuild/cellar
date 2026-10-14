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
single CLI executable. A generic CLI binary cannot serve an application whose
factories were not compiled into it.

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
  cellar-logic/           Dependency-free decision core
  cellar-ltx/             SQLite WAL capture, LTX codecs, restore, and compaction
  cellar-native/          Owned values, structured clone, crypto, and web primitives
  cellar-fleetcare/        Field-service app, embedded/daemon hosts and open-loop client
examples/                 Compiled Rust applications and project configurations
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
new subrequest slot. Native workflow packages retain the `workflows-v1` feature
while Cellar is in internal development.
Use `native_workflow`, `native_workflow_cell`, `native_cron`, and
`native_artifacts` when changing these contracts.

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

## Change discipline

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
