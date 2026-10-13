# Native Rust application examples

Start with a focused recipe, then combine the building blocks into business
cells. Application handlers compile to native Rust. Recipes use the production
SDK, routing, SQLite, alarms and output gates, with a private local store under
`dev`.

The catalog borrows application patterns from the
[original celld examples](https://github.com/denoland/celld/tree/90b43017241f81189453d326d05948f388b34652/examples).
These are new Rust implementations. The pinned revision and every project's
source, route and prerequisites are in [catalog.json](catalog.json).
Every recipe has a README with purpose, code flow, prerequisites, end-to-end
commands, expected output, interpretation and cleanup. The catalog contains 34
entries, including two original standalone hosts.
Native pattern equivalents have explicit limits below. The catalog does not
claim Workers SDK compatibility or production fleet certification.

## Run the cookbook

From the repository root, using the Rust toolchain required by `Cargo.toml`:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/project.json
```

In another terminal:

```sh
curl http://127.0.0.1:9876/hello
curl -X POST http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/counters/alpha
```

On fresh state, `/hello` returns `{"hello":"Cellar","language":"Rust"}` and
the counter POST/GET both return `{"count":1}`. Existing state retains earlier
increments; each focused guide explains how to start with clean tutorial state.

Each recipe has its own README and independent project configuration:

```sh
cargo run -p cellar --bin cellar --locked -- dev examples/queues/project.json
```

The cookbook and focused projects share `cellar-native-examples`, with factories
registered for each script name. Focused projects enable only their required
bindings and use separate built-in resource identities.
[main.rs](main.rs) is the cookbook entry point; each focused recipe's
`main.rs` is a Rust module compiled into that executable, not a standalone
`rustc` input. To create your own application, copy the relevant handler,
register its factories in your host and give its project a new script name.

The combined cookbook uses [project.json](project.json). Standalone hosts under
`native-counter/`, `native-websocket/` and `native-container/` have their own
`main.rs` entry points and `project.json` files alongside their README and assets.

`dev` watches the project and preserves state under `.cellar/native-dev` beside
its configuration. Ctrl-C requests a graceful stop. `--no-watch` builds once;
`--port` allows multiple projects. `--clean` deletes that project's local state.
Fleet deployments still need deliberate bucket and resource names; local dev
isolation is not a deployment tenancy policy.

## celld patterns in native Rust

| Upstream example | Rust recipe | Working behavior and limits |
| --- | --- | --- |
| hello | [hello](hello/README.md) | Stateless HTTP and health response |
| webapi | [webapi](webapi/README.md) | Owned URLs, headers and SHA-256 |
| static-assets | [static-assets](static-assets/README.md) | Packaged website and explicit asset binding |
| counter | [counter](counter/README.md) | Durable named counter and atomic increments |
| vectordb | [vectordb](vectordb/README.md) | Per-cell sqlite-vec index and nearest-color query |
| d1 | [sql](sql/README.md) | Native SQL binding, parameterized inserts and guestbook |
| r2 | [blob](blob/README.md) | Native Blob binding, writes, streamed reads and deletion |
| kv | [kv](kv/README.md) | Bytes, TTL, metadata and bounded listing with cursor |
| queues | [queues](queues/README.md) | Producer, consumer, retries, DLQ and deduplicated effects |
| async | [async](async/README.md) | Event-owned timer, background storage and sync |
| body | [body](body/README.md) | Bounded buffering and backpressured echo |
| router | [router](router/README.md) | Business names route to the authoritative cell |
| wsecho | [wsecho](wsecho/README.md) | Hibernating text/binary echo and persisted message count |
| wsclient | [wsclient](wsclient/README.md) | Local upgrade consumption and configurable outbound peer |
| alarm | [alarm](alarm/README.md) | Persisted wake, cancellation and transactional occurrence effect |
| cron | [cron](cron/README.md) | Real minute trigger and deduplicated occurrence |
| workflow | [workflow](workflow/README.md) | Journaled steps, durable sleep and repeatable Blob publication |
| rpc | [rpc](rpc/README.md) | Structured Rust RPC through owner routing |
| dynamic-worker-tails | [dynamic-worker-tails](dynamic-worker-tails/README.md) | Compiled loader, copied environment and background trace; no Tail Worker event API |
| facets | [facets](facets/README.md) | Loaded class as a durable child facet |
| container | [container](container/README.md) | Cell-owned Rust process, readiness and forwarding; Docker/Podman required |
| sandbox | [sandbox](sandbox/README.md) | Fixed-tool exec, bounded stdout/stderr and exit status; no Sandbox SDK compatibility |
| pi | [pi](pi/README.md) | Durable Rust tool-loop pattern; no PiHarness/provider integration |
| opencode | [opencode](opencode/README.md) | Native tool-loop pattern; no OpenCode SDK integration |
| python | [python](python/README.md) | Native HTML processing equivalent; no Python execution |
| wasm | [wasm](wasm/README.md) | Native Rust computation; upstream Wasm counter also maps to counter |

## Application and team recipes

| Recipe | Application concern |
| --- | --- |
| [multi-tenant](multi-tenant/README.md) | Authenticated tenant identity, separate account cells, atomic receipts, conflict and rollback |
| [transactional-outbox](transactional-outbox/README.md) | Atomic business record plus wake, bounded publication and consumer deduplication |
| [durable-agent](durable-agent/README.md) | Bounded native tools, checkpoints, alarms and progress inspection |
| [team-services](team-services/README.md) | API/ledger manifests, service binding and explicit state ownership |
| [embedded-node](embedded-node/README.md) | Caller-owned executors, listeners, clients, explicit policy and checked shutdown |
| [distributed-fleet](distributed-fleet/README.md) | Existing three-node RustFS harness, recovery, handoff and acknowledged-write verification |

The [complete application-builder roadmap](ROADMAP.md) proposes the remaining
reference applications and their acceptance checks, distinguishing these
working recipes from capabilities that need implementation or validation.

## Verify behavior

Build the Rust hosts, then exercise fresh temporary project state:

```sh
cargo build -p cellar --bin cellar --bin cellar-native-examples \
  --bin cellar-example-embedded --locked
CELLAR_EXAMPLE_BIN_DIR="${CARGO_TARGET_DIR:-target}/debug"
python3 examples/focused_smoke.py --cellar "$CELLAR_EXAMPLE_BIN_DIR/cellar" --with-cron
python3 examples/smoke.py --cellar "$CELLAR_EXAMPLE_BIN_DIR/cellar"
python3 examples/smoke.py --cellar "$CELLAR_EXAMPLE_BIN_DIR/cellar" \
  --project examples/team-services/project.json --suite team
python3 examples/platform_smoke.py --cellar "$CELLAR_EXAMPLE_BIN_DIR/cellar" \
  --embedded "$CELLAR_EXAMPLE_BIN_DIR/cellar-example-embedded"
```

The commands use Cargo's selected target directory, falling back to `target`.
On the development machine, [AGENTS.md](../AGENTS.md) requires a checkout-specific target directory
on the workspace volume. CI runs the local suites automatically.

The focused runner boots every local HTTP project with fresh state, checks its
documented behavior and restarts projects with durable effects. Select one with
`--example counter` (repeat `--example` for several). Success prints a `PASS` line
per recipe and `PASS 30 focused Rust projects` for the complete local set.

The cookbook smoke runner checks HTTP status codes, binary streams, actual vector queries,
SQL/KV/Blob, alarms, background completion, retry/DLQ, deduplicated effects,
workflow output, WebSocket upgrades, compiled loads, facet storage, tenant
isolation, receipt conflicts, rollback, tool completion and state after a
restart. `--with-cron` waits for a real minute boundary. The team suite checks
service calls across two application manifests and restart. Container and fleet
failure checks have their own prerequisites and commands. The platform runner
checks a network WebSocket handshake between separate Rust hosts and the
standalone embedding host's drain proofs and restart. With a running engine
and `DOCKER_HOST` configured as described in the container guide, append
`--with-containers` to build the documented Dockerfile/fence package and verify
named isolation, HTTP forwarding and fixed-tool exec. It prints one `PASS` line
for each successful platform scenario and removes its temporary project state.
CI includes these container checks; the Colima fleet harness is a separate run.

## Package and deploy

```sh
cargo run -p cellar --bin cellar --locked -- \
  build examples/counter/project.json --output /tmp/cellar-counter-package
# Publish to the bucket selected for your application:
cargo run -p cellar --bin cellar --locked -- \
  deploy /tmp/cellar-counter-package --bucket YOUR_BUCKET
```

Install the verified package using `cellar install`, then run its compiled
executable with your node's bucket, listeners and peer settings. The generic
CLI cannot serve factories absent from its executable. The sample projects
use `profile: dev` for iteration; select `release` and your target for production.
Replace local teaching tokens and resource names; add your quotas and secret
injection before adapting a recipe.

The original standalone [counter host](native-counter/README.md),
[WebSocket ownership host](native-websocket/README.md) and
[container host](native-container/README.md) have complete guides as well.
Their source and configuration links are included in the catalog.
