# Plan 001: Group framework files by component

> **Executor instructions:** Read this plan fully, then implement the three
> batches in order. Run each verification gate before continuing. Preserve
> unrelated user work. Update the status in `plans/README.md` when work begins
> and when all done criteria pass.
>
> **Drift check:** Run
> `git diff --stat 0bbb2b11ffbcc2ccb3bd08f62a83c84c73c8c5f6..HEAD -- crates/cellar AGENTS.md README.md`
> before starting. If source changed, reconcile the move map and excerpts with
> live code before executing. Expected prior batches of this plan are allowed
> on a resumed execution; unrelated source drift requires a refreshed plan.
> Also inspect staged, unstaged, and untracked edits with
> `git diff --stat -- crates/cellar AGENTS.md README.md`,
> `git diff --cached --stat -- crates/cellar AGENTS.md README.md`, and
> `git status --short -- crates/cellar AGENTS.md README.md`.
> Reconcile existing edits before moving files; do not replace user work with
> the planned Git tree to make the baseline match.

## Status

- **Priority:** P2
- **Effort:** M, three reviewable batches
- **Risk:** MED overall; file relocation itself is LOW risk
- **Depends on:** None
- **Category:** tech-debt / dx
- **Planned at:** `0bbb2b11ffbcc2ccb3bd08f62a83c84c73c8c5f6`, 2026-10-12

## Why this matters

Cellar's production crate has 74 Rust files at its root. Related runtime,
invocation, network, storage, and fleet code is scattered across flat files
and directories whose names no longer describe the shared library host.
Grouping by responsibility makes the owning subsystem visible while preserving
the established SDK, public module paths, and lifecycle behavior.

This plan changes physical organization and necessary path wiring. The later
responsibility splits listed at the end are separate work.

## Current state and contracts

All paths in this plan are repository-relative unless explicitly stated.

The workspace contains four crates. Keep their boundaries:

- `cellar-logic`: dependency-free state transitions and returned effects.
  No I/O, asynchronous work, ambient clock, locks, or adapter policy.
- `cellar`: serial actor, production adapters, node composition, application
  SDK, and command-line tooling.
- `cellar-ltx`: SQLite WAL/LTX codecs, capture, restore, paging, compaction.
- `cellar-native`: engine-independent owned values and web/crypto primitives.

The Cellar crate uses `lib.rs` beside its manifest rather than `src/lib.rs`.
`crates/cellar/Cargo.toml` already points binaries to `cli/main.rs` and
`cli/native_main.rs`. Keep these entry points and all 43 direct
`crates/cellar/tests/*.rs` integration targets.

Existing relocation pattern, `crates/cellar/lib.rs:49`:

```rust
#[path = "cli/cell_cli.rs"]
pub mod cell_cli;
pub mod cell_dispatch;
mod cell_host;
pub mod cell_runtime;
```

Preserve the declaration names, visibility, attributes, cfg gates, order, and
root exports; add or update physical `#[path]` attributes. Folder structure
does not require a new logical Rust hierarchy.

The production domain and supplied simulation keep their existing selection,
`crates/cellar/lib.rs:32`:

```rust
#[cfg(not(cellar_internal_tests))]
pub mod asyncrt;
#[cfg(cellar_internal_tests)]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
pub mod asyncrt {
    include!(env!("CELLAR_INTERNAL_ASYNCRT"));
}
```

Apply the new path only to the production declaration. Keep the simulated
inline module and external observer/conformance includes in the same logical
scopes. Do not enable `cellar_internal_tests` without its external harness.

Domain ownership is explicit in `crates/cellar/host_services.rs:76`:

```rust
/// Services and resource registries owned by one execution domain.
pub struct HostServices {
    domain: OnceLock<crate::asyncrt::DomainToken>,
    application_work: Arc<ApplicationWork>,
    channels: crate::host_channels::HostChannels,
    gates: Arc<crate::host_gates::GateService>,
    tcp: Arc<crate::host_tcp::TcpService>,
    loader: Arc<dyn crate::host_loader::LoaderService>,
    facets: Arc<dyn crate::host_facets::FacetService>,
```

`host_services` belongs under execution. `host_service` instead dispatches
generation-scoped service binding calls and belongs under invocation. HTTP
streams and WebSocket registries remain owned by these services and event
lifetimes even when physically grouped under transport.

`crates/cellar/daemon.rs:2405` declares two logical siblings from `main/`:

```rust
#[path = "main/peer_tunnel.rs"]
mod peer_tunnel;
#[path = "main/websocket.rs"]
mod websocket;
```

They use `super::*` and sibling references. Move the files without introducing
a logical `network` module. `daemon/driver.rs` and `daemon/embedded.rs` supply
shared production dispatch and the library `sdk::Node`; keep the node intact.

`crates/cellar/node_log.rs:3997` owns the coupled durability stack:

```rust
pub struct DurabilityOwner {
    stack: Option<OwnedDurabilityStack>,
    follower_tasks: FollowerTaskOwner,
}
```

Log transport, follower proof, LTX registration, recovery, and task shutdown
belong under durability. Keep the owner and shutdown ordering unchanged.

`crates/cellar/fleet.rs:571` implements `run_deploy`, while `:652` implements
`load_current_worker` for runtime callers. Move this mixed module intact to
`deployment/fleet.rs`. Its existing `cellar::fleet` facade remains.

`crates/cellar/cell_runtime.rs:45` owns `FacetStreams` for resident root
activations. Put that registry beside cell runtime state, even though its
operations call replication. SQLite's `storage/wake_record.rs` stays with
SQLite; fleet wake discovery and response gates go under `fleet/wake/`.

Preserve the latest main contracts from `AGENTS.md:112` and `sdk.rs`:

- Drain full event drivers, post-response background work, alarms, previous
  generations, and queued SDK calls before changing cell lifecycle.
- Progress may extend the stall window, never the absolute stop deadline.
- Recover predecessor logs before replacing a lease. Retain the directory lock
  and caller-owned executors until all owned work joins.
- Domain closure races use fallible actor admission and `try_start`/`try_step`;
  cancellation cannot fabricate completion or cause an invariant panic.
- Retain typed `FenceReason`/`FenceReport`/`NodeOutcome::Fenced`. A closed
  reporting channel is a supervision failure.
- Keep idle unowned routing history bounded by captured `max_cached_routes`,
  without discarding ownership, alarms, or unfinished work.
- Preserve activation epochs, output gates, durable wake coverage, protocol
  fields, bucket keys, artifact validation, reserved `__sys_` tables, and
  dependency feature gates.
- Keep per-node configuration and services explicit. Relocating environment
  parsing does not change how or when existing callers read settings.

Read `AGENTS.md` and `clippy.toml` before implementation. Keep existing narrow
boundary exceptions and `asyncrt::select!` behavior. Do not widen lint allows.

### Validation context added by latest main

PR #9 changes the embedded measuring application, harness, analysis, proof-guard
tests, and documentation. Its production crates and module topology match the
previous review. The move map below has been revalidated at this revision.

`tools/perf/run.py` checks the node's captured resident/cache/request limits
before load and checks cache bounds in every required saved checkpoint.
`tools/perf/analyze.py` independently checks the saved observations. Preserve
the Cargo performance-binary entry points, harness files, and report formats.
The harness proof-guard unit tests use local temporary files and mocks and
require no Docker daemon.

The checked-in 10,000-cell reference image predates routing-cache bounds and
typed fencing; the newer zero-cache image covers a bounded single-node run.
Keep these historical reports as evidence for their recorded images. When
validating a later behavior refactor, rebuild its own image and follow
`tools/perf/README.md` for positive-cache, multi-node recovery and shutdown
checks. A file-only relocation uses the compilation, SDK, and proof-guard gates
listed here; it does not require repeating a long fleet workload.

## Target organization and move map

Retain `lib.rs`, `sdk.rs`, `build.rs`, `Cargo.toml`, existing `cli/` files,
all integration-test entry points, and the existing crate boundaries.

The following map is authoritative. Paths are relative to `crates/cellar/`.
A trailing slash means move every tracked file below that source prefix,
preserving its suffix. Expand directory rows from the original Git tree
before moving anything; some destinations are inside existing source
directories, so do not recursively move a directory into itself.

| Batch | Current path | Destination |
| --- | --- | --- |
| 1 | `actor.rs` | `actor/mod.rs` |
| 1 | `application.rs` | `application/mod.rs` |
| 1 | `asyncrt.rs` | `execution/domain.rs` |
| 1 | `host_services.rs` | `execution/services.rs` |
| 1 | `tokio_select.rs` | `execution/select.rs` |
| 1 | `daemon.rs` | `node/mod.rs` |
| 1 | `daemon/` | `node/` |
| 1 | `main/` | `node/network/` |
| 1 | `startup.rs` | `node/listeners.rs` |
| 1 | `node_lifecycle.rs` | `node/lifecycle.rs` |
| 1 | `clean_reload.rs` | `node/reload_certificate.rs` |
| 1 | `control_plane.rs` | `node/managed.rs` |
| 1 | `runtime.rs` | `runtime/mod.rs` |
| 1 | `runtime_policy.rs` | `runtime/policy.rs` |
| 1 | `engine_api.rs` | `runtime/contracts.rs` |
| 1 | `generation.rs` | `runtime/generation.rs` |
| 1 | `cell_runtime.rs` | `runtime/cells/state.rs` |
| 1 | `cell_host.rs` | `runtime/cells/host.rs` |
| 1 | `cell_dispatch.rs` | `runtime/cells/dispatch.rs` |
| 1 | `host_facets.rs` | `runtime/cells/facets/mod.rs` |
| 1 | `host_facets/` | `runtime/cells/facets/` |
| 1 | `facet_streams.rs` | `runtime/cells/facet_streams.rs` |
| 1 | `worker.rs` | `runtime/workers/worker.rs` |
| 1 | `native_worker.rs` | `runtime/workers/native/mod.rs` |
| 1 | `native_worker/` | `runtime/workers/native/` |
| 1 | `worker_config.rs` | `runtime/workers/config/mod.rs` |
| 1 | `worker_config/` | `runtime/workers/config/` |
| 1 | `pool.rs` | `runtime/workers/pool.rs` |
| 1 | `host_loader.rs` | `runtime/workers/loader/mod.rs` |
| 1 | `host_loader/` | `runtime/workers/loader/` |
| 1 | `loader_policy.rs` | `runtime/workers/loader_policy.rs` |
| 1 | `host_io.rs` | `runtime/invocation/context/mod.rs` |
| 1 | `host_io/` | `runtime/invocation/context/` |
| 1 | `host_gates.rs` | `runtime/invocation/gates.rs` |
| 1 | `host_turn.rs` | `runtime/invocation/turn.rs` |
| 1 | `host_channels.rs` | `runtime/invocation/channels.rs` |
| 1 | `host_rpc.rs` | `runtime/invocation/rpc.rs` |
| 1 | `host_service.rs` | `runtime/invocation/service.rs` |
| 1 | `host_fetch.rs` | `transport/http/fetch.rs` |
| 1 | `http_streams.rs` | `transport/http/streams.rs` |
| 1 | `host_tcp.rs` | `transport/tcp/mod.rs` |
| 1 | `host_tcp/` | `transport/tcp/` |
| 1 | `ws_client.rs` | `transport/websocket/client.rs` |
| 1 | `host_websocket.rs` | `transport/websocket/bindings.rs` |
| 1 | `ws_registry.rs` | `transport/websocket/registry.rs` |
| 1 | `ws_tunnel.rs` | `transport/websocket/upgrades.rs` |
| 2 | `storage.rs` | `storage/sqlite/mod.rs` |
| 2 | `storage/` | `storage/sqlite/` |
| 2 | `bucket.rs` | `storage/object/bucket.rs` |
| 2 | `local_store.rs` | `storage/object/local_store.rs` |
| 2 | `blob_store.rs` | `storage/blob/mod.rs` |
| 2 | `blob_store/` | `storage/blob/` |
| 2 | `replication.rs` | `durability/replication/contracts.rs` |
| 2 | `replication_options.rs` | `durability/replication/options.rs` |
| 2 | `replica_storage.rs` | `durability/replication/storage.rs` |
| 2 | `ltx_replication.rs` | `durability/replication/handle.rs` |
| 2 | `ltx_repl.rs` | `durability/replication/backend.rs` |
| 2 | `node_log.rs` | `durability/log/mod.rs` |
| 2 | `node_log/` | `durability/log/` |
| 2 | `log_options.rs` | `durability/log/options.rs` |
| 2 | `ownership_store.rs` | `fleet/ownership.rs` |
| 2 | `drain_token.rs` | `fleet/drain.rs` |
| 2 | `dead_node_gc.rs` | `fleet/retirement.rs` |
| 2 | `peer_auth.rs` | `fleet/peer/auth.rs` |
| 2 | `peer_probe.rs` | `fleet/peer/probe.rs` |
| 2 | `wake.rs` | `fleet/wake/mod.rs` |
| 2 | `wake_entry.rs` | `fleet/wake/entry.rs` |
| 2 | `wake_format.rs` | `fleet/wake/format.rs` |
| 2 | `kv_blob.rs` | `application/kv/blob_backend.rs` |
| 2 | `queue_policy.rs` | `application/queue/policy.rs` |
| 2 | `queue_batching.rs` | `application/queue/batching.rs` |
| 3 | `protocol.rs` | `deployment/protocol.rs` |
| 3 | `fleet.rs` | `deployment/fleet.rs` |
| 3 | `assets.rs` | `runtime/assets.rs` |
| 3 | `html_rewriter.rs` | `application/html_rewriter/backend.rs` |
| 3 | `container.rs` | `containers/mod.rs` |
| 3 | `docker.rs` | `containers/docker.rs` |
| 3 | `telemetry.rs` | `observability/telemetry.rs` |
| 3 | `otlp.rs` | `observability/otlp.rs` |
| 3 | `memory.rs` | `platform/memory.rs` |
| 3 | `machine.rs` | `platform/machine.rs` |
| 3 | `env_vars.rs` | `cli/environment.rs` |

Existing `actor/` and `application/` children stay at their current paths.
Parents moved to `mod.rs` continue to discover the same children.

Reasons for less obvious placements:

- `runtime/generation.rs` owns the full deployment graph, service bindings,
  asset resolvers, cron schedule, and pools; it is broader than worker loading.
- `runtime/assets.rs` implements generation-scoped serving and caching.
  The SDK binding remains `application/assets.rs`.
- KV large-value helpers and Queue policy/batching stay near their built-in
  cells under `application/kv/` and `application/queue/`. Queue's producer and
  fleet-log timing remain one captured policy; do not decouple them.
- `application/html_rewriter/backend.rs` sits beside its SDK adapter, while
  retaining the original crate-root `html_rewriter` module and boundary allow.
- `node/managed.rs` is a transitional home for managed-node integration.
  Its commands, environment access, re-exec, and deployment ingestion are
  documented mixed responsibilities for a later extraction.
- `deployment/fleet.rs` is a transitional intact module. Ownership and fleet
  discovery live under `fleet/`; deployment loading lives under `deployment/`.
- `platform/machine.rs` currently mixes observations, identity, defaults, and
  environment helpers. This move does not imply all of it is platform policy.
- `cli/environment.rs` remains accessible through `cellar::env_vars`. Preserve
  explicit library callers of its environment adapters.

## Scope

**In scope:**

- Every source and destination in the move map, including tracked children.
- `crates/cellar/lib.rs`: physical path attributes only.
- The relocated `daemon.rs`: its three existing path attributes only.
- The relocated `ws_client.rs`: TLS fixture include paths only.
- `crates/cellar/tests/native_sockets.rs`: two fixture include paths only.
- `AGENTS.md`: repository map, physical-path examples, boundary documentation.
- `README.md`: add a short component map if useful; preserve behavior docs.
- `plans/README.md`: update execution status.

**Out of scope:**

- API renaming, public component namespaces, visibility changes, new traits,
  new crate boundaries, or changes to crate-root helper types.
- Runtime algorithms, configuration values, environment timing, error handling,
  cancellation, task registration, recovery, shutdown, logging behavior.
- Removing old comments or dormant wrappers, unrelated formatting, dependency
  upgrades, features, manifest changes, `Cargo.lock` changes.
- Changes to `cellar-logic`, `cellar-ltx`, `cellar-native`, examples, CI, tools.
- Test relocation, new behavior tests for file moves, changed assertions,
  external conformance sources, live fleet operations.
- The follow-up refactors at the end of this plan.

## Commands and expected results

Run commands from the repository root. Use one stable target directory for
this checkout and set it in every shell invocation that compiles code:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-id>-component-layout"
test -d "$HOME/Workspace/crabbuild-target"
test -w "$HOME/Workspace/crabbuild-target"
mkdir -p "$CARGO_TARGET_DIR"
test -w "$CARGO_TARGET_DIR"
```

Replace `<checkout-id>` with this checkout's identifier, such as `5cf0`.
Never reuse or clean another checkout's target. CI may use its runner default.

| Purpose | Command | Expected result |
| --- | --- | --- |
| Target inventory | `cargo metadata --no-deps --format-version 1 --locked` | Exit 0; same packages, binaries, integration tests |
| Formatting | `cargo fmt --all -- --check` | Exit 0; no diff |
| Engine dependency guard | `python3 tools/check_native_dependencies.py` | Exit 0; no JavaScript engine |
| Harness proof guards | `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/perf -p 'test_*.py'` | Exit 0; existing guard tests pass (13 at this revision) |
| Compile every target/feature | `cargo check --workspace --all-targets --all-features --locked` | Exit 0 |
| SDK CI behavior checks | `cargo test --workspace --all-features --lib --tests --locked` | Exit 0; existing tests pass |
| Public examples/docs | `cargo test --workspace --all-features --doc --locked` | Exit 0 |
| Boundary/lint checks | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Exit 0; no warnings |
| Diff whitespace | `git diff --check` | Exit 0 |

Commands are verified from `AGENTS.md`, the manifests, and
`.github/workflows/native-sdk.yml`. They were not all run during planning.
The Cellar manifest requires Rust 1.94.1; SDK CI selects 1.98.1. Local recon used
1.97.0. Report the toolchain actually used; do not claim a local run matches CI
unless it does.

## Git workflow

Use `codex/organize-components` or a similarly named `codex/` branch. If
continuing an existing suitable branch, reuse it. Never force-switch or discard
unrelated edits.

Commit each verified batch with conventional intent, for example
`refactor: group node runtime and transport sources`. Batches can be separate
PRs; do not begin the next batch on an unverified prior batch. Do not push or
open a new PR unless requested for this implementation.

## Steps

### Step 1: Establish the baseline and expand the move inventory

1. Read the files to move, their logical parent declarations, relevant callers,
   `AGENTS.md`, and `clippy.toml`.
2. Run the drift check. Confirm there are no unaccounted source edits.
3. Capture Cargo metadata outside the working tree, and record all target
   names/kinds. Compare that inventory after the final batch.
4. Expand the map from the planned Git tree, then inspect every destination for
   collisions. Move tracked files individually when destination directories
   already exist. Do not let an iterative filesystem walk pick up files that
   were already moved during the same batch.
5. Run the compile gate before editing. A pre-existing failure must be reported,
   rather than fixed or hidden by this reorganization.

The following read-only inventory command must succeed before implementation:

```sh
python3 - <<'PY'
import re
import subprocess
from pathlib import Path

base = "0bbb2b11ffbcc2ccb3bd08f62a83c84c73c8c5f6"
root = "crates/cellar/"
plan = Path("plans/001-component-layout.md").read_text()
rows = re.findall(r"^\| ([123]) \| `([^`]+)` \| `([^`]+)` \|$", plan, re.M)
tracked = subprocess.check_output(
    ["git", "ls-tree", "-r", "--name-only", base, "--", root], text=True
).splitlines()
moves = {}
for batch, source, destination in rows:
    prefix = root + source
    files = [p for p in tracked if p.startswith(prefix)] if source.endswith("/") else [prefix]
    assert files and all(p in tracked for p in files), source
    for old in files:
        new = root + destination + old[len(prefix):] if source.endswith("/") else root + destination
        assert old not in moves, old
        moves[old] = (int(batch), new)
assert len({new for _, new in moves.values()}) == len(moves), "duplicate destination"
assert all(new not in tracked for _, new in moves.values()), "destination already tracked"
root_rs = {p for p in tracked if p.startswith(root) and "/" not in p[len(root):] and p.endswith(".rs")}
retained = {root + name for name in ("lib.rs", "sdk.rs", "build.rs")}
assert root_rs - set(moves) == retained, root_rs - set(moves)
assert len(root_rs) == 74
assert sum(old in root_rs for old in moves) == 71
print(f"Move inventory valid: {len(moves)} tracked files; 71 root Rust files relocate.")
PY
```

**Verify:** the inventory prints its success message; metadata and the compile
gate exit 0. Preserve the expanded inventory for later byte comparisons.
If the planned revision has drifted, refresh this inventory before any move.

### Step 2: Move batch 1 — execution, node, runtime, transport

1. Apply batch 1 moves, carrying every listed child tree and TCP fixture.
2. Update `lib.rs` paths with the same logical module names. Keep `sdk.rs` and
   all existing `crate::...` imports unchanged.
3. `actor/mod.rs` and `application/mod.rs` are standard parent files.
   Their existing child declarations remain unchanged.
4. In `node/mod.rs` replace:
   - `main/peer_tunnel.rs` with `network/peer_tunnel.rs`;
   - `main/websocket.rs` with `network/websocket.rs`;
   - `cli/daemon.rs` with `../cli/daemon.rs`.
   Keep these modules as logical `daemon` children, including the CLI parser.
5. Preserve `tokio_select` as the same private root module, the hidden
   `__asyncrt_select_support` root reexport, and exported macro definitions.
   They still refer to `$crate::asyncrt` and `$crate::__asyncrt_select_support`.
6. In `transport/websocket/client.rs` replace all three
   `host_tcp/fixtures/` include prefixes with `../tcp/fixtures/`.
   In `tests/native_sockets.rs` replace its two `../host_tcp/fixtures/` prefixes
   with `../transport/tcp/fixtures/`.
   `transport/tcp/tests.rs` retains its `fixtures/` references; fixture bytes
   remain identical.
7. Update the repository map in `AGENTS.md` for this batch. Remove the obsolete
   `main/` map entry once its two files have moved.

**Verify:** formatting and workspace compile gates exit 0, followed by:

```sh
cargo test -p cellar --locked \
  --test native_node --test native_daemon --test native_loopback \
  --test native_domains --test native_actor --test native_call_order \
  --test native_runtime --test native_runtime_policy --test native_events \
  --test native_concurrency --test native_egress \
  --test native_loader --test native_loader_policy \
  --test native_facets --test native_websocket --test native_outbound_websocket \
  --test native_websocket_opening --test native_bound_websocket \
  --test native_sockets --test native_body --test native_streams
```

Expected: exit 0, all existing tests pass. Inspect the rename diff; only module
path wiring, documented fixture paths, and architecture docs may differ.

### Step 3: Move batch 2 — storage, durability, fleet, KV/Queue helpers

1. Apply batch 2 moves and update the corresponding `lib.rs` paths.
2. SQLite's `wake_record` stays a logical child of `storage`. Blob's multipart
   and streaming modules stay logical children of `blob_store`. Log's
   `recovery_progress` stays a logical child of `node_log`.
3. Leave `application/queue/broker.rs` beside `schema.sql`; the existing
   `include_str!("schema.sql")` continues to resolve.
4. The KV and Queue helper files retain their original crate-root namespaces
   even though their physical paths are beside built-in binding code.
5. Keep replication handles, supplied clients, durability registration,
   follower ownership, and wake/lease formats unchanged. Do not extract the
   SQLite or log implementation while moving it.
6. Update `AGENTS.md` for storage, durability, and fleet placement.

**Verify:** formatting and workspace compile gates exit 0, followed by:

```sh
cargo test -p cellar --locked \
  --test native_storage --test native_sql --test native_transactions \
  --test native_kv --test native_queue --test native_blob \
  --test native_replication --test native_log --test native_log_protocol \
  --test native_node --test native_domains
```

Expected: exit 0, all existing tests pass. Byte comparison of relocated files
must show no changes in this batch.

### Step 4: Move batch 3 — deployment, remaining adapters, CLI environment

1. Apply batch 3 moves and update `lib.rs` paths.
2. Keep `cellar::fleet` and `cellar::protocol` declarations intact even though
   their files now sit under `deployment/`. Likewise preserve
   `cellar::container`, `cellar::env_vars`, and other existing root paths.
3. Keep mixed `deployment/fleet.rs` and `platform/machine.rs` intact. Do not
   move functions across logical modules or alter their boundary attributes.
4. Keep the SDK HTML adapter's logical nesting separate from the root
   `html_rewriter` backend. Physical colocation does not add a new child
   declaration or change visibility.
5. Finish the `AGENTS.md` component map and add a short map to `README.md` if
   useful. Document physical folders versus existing logical module names,
   domain/event ownership of transport registries, and the three transitional
   mixed modules. Keep `CLAUDE.md` as the existing link to `AGENTS.md`.

**Verify:** formatting, workspace compile, and dependency guard exit 0,
followed by:

```sh
cargo test -p cellar --locked \
  --test native_cli --test native_artifacts --test native_assets_cache \
  --test native_container_packages --test native_containers \
  --test native_html_rewriter
cargo run -p cellar --bin cellar --locked -- --help
cargo run -p cellar --bin cellar-native --locked -- --help
```

Expected: exit 0; all tests pass and both binaries show their existing help.

### Step 5: Verify the complete migration and hand off

1. Re-expand the original move inventory and confirm every destination exists.
   Compare each destination's bytes with `git show <planned-at>:<source>`.
   All moved files must match exactly except `node/mod.rs` and
   `transport/websocket/client.rs`, whose reviewed differences are restricted
   to the path substitutions in step 2 and formatting those affected lines.
2. Compare `tests/native_sockets.rs` against the base: only its two fixture
   literals and formatting of those lines may change. Inspect `lib.rs`:
   module names, visibility, cfgs, root types and exports must match.
3. Confirm the only direct Rust files in the crate root are `lib.rs`,
   `sdk.rs`, and `build.rs`:

```sh
python3 - <<'PY'
from pathlib import Path
assert {p.name for p in Path("crates/cellar").glob("*.rs")} == {"lib.rs", "sdk.rs", "build.rs"}
assert len(list(Path("crates/cellar/tests").glob("*.rs"))) == 43
print("Root layout and integration entry points preserved.")
PY
```

4. Regenerate Cargo metadata and compare package/target names, kinds, and source
   paths with the baseline. They must be unchanged; no Cargo entry point moves
   are included in this plan.
5. Run all SDK CI behavior, doc, Clippy, and harness proof-guard gates from the
   command table.
   Earlier targeted checks do not replace these final workspace gates.
6. Search for physical references to the old paths using `rg`. Historical
   references in this plan are intentional. Fix current documentation paths
   within scope; report any out-of-scope tool/CI path instead of silently
   expanding the migration.
7. Inspect `git diff --check`, `git diff --stat`, and
   `git diff --find-renames=50%`. Ensure every source change is covered by the
   move map or wiring/docs scope. Report tests/toolchain and any external
   harness or fleet-scale checks not run.
8. Update `plans/README.md` to DONE only when the criteria below hold.

**Verify:** both layout assertions pass; target inventories match; all final
gates exit 0; the diff contains only the permitted moves, paths, and docs.

## Test plan

Use the existing tests listed per batch. Do not add tests that merely mirror
module relocation. Existing SDK examples, integration targets, unit tests,
doctests, and Clippy check path resolution and the retained contracts.

The full workspace tests also cover `cellar-logic` routing-cache behavior without
changing that crate. External simulation/conformance includes require a separate
provided harness; a normal all-feature run does not exercise them.
`native_containers` uses a local mock Engine API, and `native_container_packages`
uses a deterministic Python engine stub. Neither needs Docker/Podman;
the packaging target requires Python 3. Real container examples or external
fleet harnesses may need Docker/Podman. Report actual results without weakening
tests or using a live fleet.

## Done criteria

- [ ] Every move-map destination exists and all moved source paths are retired.
- [ ] Relocated contents match the base apart from the specifically allowed paths.
- [ ] Direct crate-root Rust files are exactly `lib.rs`, `sdk.rs`, `build.rs`.
- [ ] All 43 integration-test entry points remain; Cargo target metadata matches.
- [ ] Existing module declarations, visibility, cfgs, exports, macros, and SDK
      facade are preserved.
- [ ] Formatting, dependency guard, locked all-target/all-feature check, full
      workspace library/integration tests, doctests, Clippy, and harness
      proof-guard tests exit 0.
- [ ] Both CLI help commands exit 0.
- [ ] `git diff --check` passes; no manifest, lockfile, other-crate, or
      out-of-scope behavior change.
- [ ] `AGENTS.md` describes the new physical layout and stable Rust namespaces.
- [ ] `plans/README.md` status records completion and verification limitations.

## Stop conditions

Stop and report if:

- A move-map source is missing, a destination collides, or unrelated source
  drift invalidates the baseline inventory.
- A move requires changing module privacy, public paths, macro export paths,
  cfg selection, externally supplied harness nesting, or runtime behavior.
- The peer-tunnel/WebSocket move requires inserting a new logical parent.
- A fixture or embedded schema can no longer resolve using only documented
  path corrections, or its contents need changing.
- Verification fails twice after correcting permitted path/format mistakes.
  Do not suppress a lint or modify test expectations to continue.
- A required fix touches an out-of-scope manifest, dependency, tool, CI file,
  other crate, or live fleet state.
- Any change appears to alter task ownership, recovery order, output gates,
  shutdown proof, typed fencing reports, or routing-cache reclamation.

## Maintenance notes and deferred work

Put new implementation files beside their owning component. Follow the existing
logical module topology until a separate public API proposal changes it.
Keep adapter services within their execution domain; physical transport folders
do not grant independent authority.

Separate later PRs should address:

1. CLI bootstrap in `node/mod.rs`, CLI commands/re-exec in `node/managed.rs`,
   and command orchestration in `deployment/fleet.rs`.
2. Shared deployment/package helpers currently under `cli/deploy/` and
   `cli/native_deploy/`, including types reexported by `sdk.rs`. Preserve queue
   attachment publication ordering and artifact validation when extracting.
3. Private subdivisions of runtime cancellation/event drivers, SQLite
   schema/cursor/transaction/alarm code, and node-log wire/follower/recovery
   code. Keep one state and lifecycle owner; characterize any new behavioral
   seam with focused existing and regression tests in that separate work.
4. Platform identity helpers and explicit CLI environment adapters currently
   mixed into `machine.rs` and component configuration modules.

These follow-ups are not completion criteria for the physical reorganization.
