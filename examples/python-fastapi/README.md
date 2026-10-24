# FastAPI orders with a packaged Python runtime

This runnable order-draft API embeds FastAPI in a native Cellar host. FastAPI
provides async routes, Pydantic validation, dependency injection and OpenAPI.
Each order is a durable Cellar cell with SQLite state, atomic creation receipts
and checked status changes. Cellar owns the listener, routing and durability;
Uvicorn and Docker are not required.

The native host routes canonical `GET`, `POST` and `PATCH /orders/{order_id}`
requests directly to the named order cell. FastAPI runs inside that cell, with
the original request, validation and dependency injection. This removes the
waiting Python gateway invocation for these routes. Other URLs, including
percent-encoded IDs, retain the Python gateway. Cell activation happens before
FastAPI body validation on the direct path, so an invalid body can activate a
cell without writing an order. Admission and authorization policies must account
for that ordering. This is an example-specific routing implementation; its
performance benefit still requires measurement.

## Build the application and its dependencies

Prerequisites: Rust 1.98.1 (the SDK CI toolchain), `uv`, and Python 3.11 or newer
for build/test scripts. Run from the repository root on the target OS and CPU
architecture. Linux capture requires binutils; macOS requires the SDK compiler
and `otool`. Follow the [native prerequisite contract](../../tools/python/README.md#native-prerequisites).

```sh
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.98.1
export PYTHONDONTWRITEBYTECODE=1
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-your-checkout"
mkdir -p "$HOME/Workspace"
export CELLAR_FASTAPI_ROOT="$(mktemp -d "$HOME/Workspace/cellar-fastapi-XXXXXX")"

python3 examples/python-fastapi/package.py \
  --prefix "$CELLAR_FASTAPI_ROOT/env-v1" \
  --output "$CELLAR_FASTAPI_ROOT/package-v1"
```

The helper provisions CPython 3.14.5 inside the new prefix, downloads hash-locked
wheels, installs them offline into `site/`, and builds the native host. It then
captures runtime/source/native prerequisites, creates the environment archive
and invokes `cellar build` with a `python_environment` project declaration.
Existing prefixes are refused. The helper defaults to the `dev` Cargo profile.
Use `--profile release` for deployment or `--profile lab` for optimized
measurements. The prerequisite capture host and final packaged host use the
selected profile; the build-time `cellar` CLI remains in `debug/`.

The package contains a manifest, executable and verified environment archive.
Python source is embedded in the executable. No package installer runs on a
request or when the deployed host starts. [requirements.lock](requirements.lock)
pins FastAPI 0.157.0, Pydantic 2.14.1 and their transitive dependencies. When
updating the lock, update the matching distribution pins in
[main.rs](main.rs) and [descriptor.json](descriptor.json).

Add `--runtime 3.14.5t` with a **new** prefix/output to package free-threaded
Python. Its wheels, compiled host and environment use the distinct `cp314t` ABI.

## Run and call the API

```sh
"$CARGO_TARGET_DIR/debug/cellar" dev "$CELLAR_FASTAPI_ROOT/env-v1/project.json" \
  --no-watch --port 8080
```

In a second terminal:

```sh
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/openapi.json
curl -X POST http://127.0.0.1:8080/orders/order-42 \
  -H 'content-type: application/json' \
  -d '{"customer_id":"customer-7","items":[{"sku":"coffee","quantity":2,"unit_price_cents":1250}]}'
curl http://127.0.0.1:8080/orders/order-42
curl -X PATCH http://127.0.0.1:8080/orders/order-42 \
  -H 'content-type: application/json' \
  -d '{"expected_version":1,"status":"confirmed"}'
```

Swagger UI is available at `/docs`; its default browser assets use a CDN.
`/openapi.json` remains available offline. Creation returns 201 with `total_cents: 2500`,
`status: "pending"` and `version: 1`. Prices are integer cents supplied in the
draft. A status change requires the current version and a pending order, then
returns version 2. Invalid inputs return 422; conflicting payloads/versions
return 409. Order IDs and SKUs are bounded ASCII identifiers.

The order ID is the creation idempotency key. The normalized request, order and
rendered creation response commit in one checked batch. Retrying the same order
returns its original response, including after a status change or process
restart. Reusing the ID for a different order refuses. Preserve receipts for the
full business reconciliation window. SDK failures expose conservative outcomes;
the example never automatically retries an ambiguous mutation.

The compiled host supplies each order invocation with an initial `order`
snapshot, declared in both `main.rs` and the package descriptor. Reads use this
snapshot without another Python-to-host storage call. Writes use the same
snapshot as a checked-batch precondition, so an intervening writer produces a
409 rather than overwriting newer state. The snapshot is captured before the
cell handler runs and does not refresh after an await. FastAPI routing,
validation and dependency injection still run for every public request.

## Test a fresh package installation locally

Stop the dev host first. The following drill temporarily moves the original
environment aside, publishes through the real native package API into a private
SQLite development bucket, and restores the runtime from that bucket. The
installed executable runs with a cleared environment, without import-path flags.

```sh
cargo run -p cellar --example python-fastapi-deployment --locked -- \
  "$CELLAR_FASTAPI_ROOT/package-v1" "$CELLAR_FASTAPI_ROOT" "$(command -v python3)"
```

Only use this command with the private scratch root created above. No process may
share its runtime while the drill runs. The test interpreter must be outside
that runtime. Add `--expect-free-threaded` for a `3.14.5t` package.

The drill checks real FastAPI validation/OpenAPI, native-extension-backed
Pydantic models, durable order creation, receipt replay, version conflicts,
preserving restart and complete combined native/Python shutdown reports. It
restores the original environment even on failure. Expected final output starts
with `PASS FastAPI deployment`. This is local deployment/recovery coverage;
it does not establish a remote object-store performance target.

The example's buffered ASGI boundary tests run with its installed dependencies:

```sh
export PYO3_PYTHON="$(UV_PYTHON_INSTALL_DIR="$CELLAR_FASTAPI_ROOT/env-v1/runtime" uv python find --managed-python 3.14.5)"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$CELLAR_FASTAPI_ROOT/env-v1/site" \
  "$PYO3_PYTHON" -B examples/python-fastapi/test_app.py
```

Use `3.14.5t` in the interpreter lookup for a free-threaded package.

## Publish and install on fleet nodes

Configure the normal bucket credentials/endpoint, then publish:

```sh
"$CARGO_TARGET_DIR/debug/cellar" deploy "$CELLAR_FASTAPI_ROOT/package-v1" \
  --bucket YOUR_BUCKET
```

On each matching target node, provide the generated `package.json` in a package
directory and install:

```sh
cellar install /path/to/package-v1 --bucket YOUR_BUCKET \
  --output /opt/cellar/releases/orders-v1 \
  --python-root /absolute/prefix/recorded/during/build
```

`--python-root` must equal the immutable prefix captured during packaging; it is
not a relocation option. Choose that path on the build machine with deployment
in mind and create its parent on each node. The installer downloads the matching
executable/archive, verifies their digests and restores runtime/package files
before publishing the executable. Nodes need matching attested OS libraries;
external system libraries are not copied into the archive. Normal and
free-threaded packages require separate matched hosts/environments.

Start the executable with the fleet's bucket, node, storage and listener
configuration. For process monitoring, use the shipped supervisor:

```sh
cellar supervise --health-url http://127.0.0.1:8080/health \
  --startup-ms 30000 --shutdown-ms 35000 --max-restarts 3 -- \
  /opt/cellar/releases/orders-v1 --listen 0.0.0.0:8080 \
  --internal-listen YOUR_PRIVATE_PEER_ADDRESS
```

The Python health route echoes the supervisor instance header. See the
[supervision recipe](../../tools/python/README.md#process-supervision) for the
remaining fleet configuration and systemd template. Installation does not start
or replace running processes. For an update, build with `--version v2`, a new
prefix and a new output directory, publish/install, then gracefully replace the
process using the same business namespace. Source/dependency changes require a
new executable; the live interpreter is never hot-reloaded.

## Measure read/write throughput and latency

Use an optimized host and write generated results outside the source tree.
Package a fresh environment with the `lab` profile so native dependency capture
and the deployed executable use the same compilation settings:

```sh
python3 examples/python-fastapi/package.py --profile lab \
  --prefix "$CELLAR_FASTAPI_ROOT/perf-environment" \
  --output "$CELLAR_FASTAPI_ROOT/perf-package"
python3 -B examples/python-fastapi/perf.py \
  --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --project "$CELLAR_FASTAPI_ROOT/perf-environment/project.json" \
  --output "$CELLAR_FASTAPI_ROOT/perf-results" \
  --concurrency 1 8 32 --repetitions 2 --read-seconds 8 --writes 512
```

Building, seeding and correctness verification are excluded from timing. Each
case starts a fresh server and development bucket, creates 32 read targets and
512 pending update targets, then measures:

- `read`: repeated GETs against the 32 resident orders for eight seconds.
- `create`: 512 POSTs with distinct new order IDs, including cell/SQLite creation
  and ownership acquisition. These are not repeated creation-receipt reads.
- `update`: 512 PATCHes confirming the pre-created orders with checked version 1.

The client uses persistent HTTP/1.1 connections, submits writes once, records
every attempt and reads back every acknowledged mutation. The report contains
successful requests/s, p50/p95/p99 latency, sample counts, errors, process CPU/RSS,
runtime/build identities and complete shutdown reports. All runs must pass
correctness and drain checks before `results.json` says `completed: true`.
If using a conventional Python load generator, check its CPU usage for a client
bottleneck; free-threaded Python 3.14t can also run this standard-library script.

`report.md` and raw JSON files are generated in the new output directory.
Add `--keep-serving` to start a fresh server afterward with `/orders/demo` and
print its URL/PID/stop command. That live server's shutdown is recorded separately
when stopped. This measures one local application node, four Rust worker threads
and a private SQLite-backed development bucket on the same machine as the
client. It does not measure remote S3 latency, replication across machines or
production capacity. The `lab` profile is optimized with thin LTO, distinct from
the repository's fat-LTO `release` profile.

### Independent-arrival reads

The fixed-concurrency benchmark above waits for replies before sending more
work. To expose queueing and overload, use the native arrival client with the
same prepared optimized project:

```sh
cargo build -p cellar-python --example python-offered-load --profile lab \
  --no-default-features --locked
python3 -B tools/python/test_offered.py "$CARGO_TARGET_DIR/lab/examples/python-offered-load"
python3 -B examples/python-fastapi/offered_perf.py \
  --client "$CARGO_TARGET_DIR/lab/examples/python-offered-load" --client-profile lab \
  --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --project "gil=$CELLAR_FASTAPI_ROOT/perf-environment/project.json" \
  --output "$CELLAR_FASTAPI_ROOT/offered-results" \
  --rates 1000 4000 8000 --duration-ms 5000 --repetitions 2
```

Add another `--project free-threaded=/absolute/path/other-project.json` to
compare a separately packaged free-threaded runtime. Runtime order rotates
between repetitions; each host uses fresh private storage. Preparation sends
32 order creations once and verifies their receipts through real reads. The
measurement rotates these 32 read paths, validating exact response bytes and
EOF. Host executable, inventory, source and client identities are retained;
runtime/GIL checks and complete combined shutdown are required.

On Linux, add `--host-cpus 0,1 --client-cpus 2,3` to assign disjoint logical
CPU pools, choosing CPUs available in `os.sched_getaffinity(0)`. Both options
must be supplied together. The harness applies the host mask before the CLI
starts, verifies that its native child and threads inherit it, and checks
native threads before and after each measured phase. Client launch masks and
host observations are retained with the raw results. Affinity restricts
eligible CPUs; it does not reserve exclusive cores or isolate shared memory
and storage. Omitting both options keeps the caller's placement on any platform.

Read `report.md` together with the raw JSON. Every planned arrival is retained,
including client lateness/capacity drops, timeouts, HTTP failures and corrupt
responses. Request latency starts at dispatch; scheduled latency also includes
dispatch delay. Both percentile sets describe successful responses only.
An experiment marked complete can contain overload and failures; it does not
mean the offered rate met an application SLO. Warmup and measurement have
separate client connection pools. The two native client workers share the host
machine, and timer granularity can group arrivals. These read measurements do
not qualify write capacity, long-running resource stability or fleet recovery.

### Size ingress for nested calls

Canonical order requests use one Python invocation inside the cell. Requests
that fall back to the Python gateway occupy a slot while awaiting their cell,
which needs another slot in the same interpreter. The default 128-slot Python
budget covers these calls, plus alarms, queues and other Python invocations.
A waiting parent keeps its slot. The CLI leaves native stateless admission unbounded when
`CELLAR_MAX_REQUESTS` is unset; the embedded library's default is 64 per script
pool. Configure the CLI explicitly when qualifying a deployment.

For this single-script example, compare bounded ingress with the same offered
rates and a fresh output directory:

```sh
CELLAR_MAX_REQUESTS=64 CELLAR_ADMISSION_WAIT_MS=10 \
python3 -B examples/python-fastapi/offered_perf.py \
  --client "$CARGO_TARGET_DIR/lab/examples/python-offered-load" --client-profile lab \
  --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --project "gil=$CELLAR_FASTAPI_ROOT/perf-environment/project.json" \
  --output "$CELLAR_FASTAPI_ROOT/offered-bounded-results" \
  --rates 1000 4000 8000 --duration-ms 5000 --repetitions 2
```

This is a tuning candidate, not a production default. Allow headroom for
background handlers, cohosted scripts and deeper or concurrent nested calls;
native request limits apply per script pool, while Python's budget is shared.
Increasing admission wait can replace refusals with longer latency and client
drops. Compare all outcomes and p99 together. The report records inherited native
policy settings; `null` means the CLI default applies. Apply the chosen settings
to the deployed host's environment as well as the benchmark.

Python invocation exhaustion returns HTTP 503 / SDK `Overloaded`. A parent may
have committed before a child was refused, so its mutation outcome remains
`unknown`. Reconcile writes with their receipt keys before considering replay.

## HTTP integration boundaries

[app.py](app.py) contains an example-only adapter for the
[ASGI HTTP protocol](https://asgi.readthedocs.io/en/stable/specs/www.html), which
[FastAPI implements](https://fastapi.tiangolo.com/advanced/middleware/). It passes
the original Cellar context in request-local ASGI state; an async dependency
retrieves it. On the direct path, async routes use the current order cell's
context after checking its identity; gateway routes call the named cell.
SDK operations still execute in the original Rust turn.

Responses are fully buffered up to 1 MiB, with at most 128 headers/32 KiB of
header data. Native bridge limits also apply. Binary bodies, repeated headers,
query strings, cancellation and concurrent request-context separation are tested.
The current native request does not expose the original HTTP version/client
address, so the ASGI scope uses HTTP/1.1 and `client=None`.

This example supports async HTTP routes, not WebSockets, live SSE/streaming,
ASGI lifespan hooks or file-send extensions. It has no lifespan resources.
Keep SDK dependencies/endpoints async: synchronous FastAPI functions run in a
thread pool outside the managed SDK loop. Background work remains nondurable;
use Cellar queues or alarms for persisted jobs. The adapter waits for the ASGI
call to finish and propagates cancellation without returning a partial response.
