# JavaScript and TypeScript bridge throughput

This opt-in benchmark runs the **same typechecked TypeScript application** on
embedded V8 and a Node.js child process. TypeScript is compiled before the test;
there is no separate TypeScript interpreter or runtime tax to measure.

The standalone Rust host uses the repository's shipped `release` profile and
normal `Host::local` startup, routing, ownership, storage and replication. The
benchmark creates a private SQLite development bucket, uses bucket durability,
and never reads credentials or connects to a production fleet. Both backends
use default bridge limits and two Tokio workers. The host binary includes both
optional adapters solely to permit identical builds/configuration in this probe.
Native-only Cellar binaries remain unchanged.

## Run

Use Node 24+, the repository Rust toolchain, and a target directory unique to
this checkout, as required by the root contributor guide. From the repository
root:

```sh
npm ci --prefix tools/javascript --ignore-scripts
node tools/perf/javascript/build.mjs
cargo build --locked --release -p cellar-javascript --features embed,node \
  --bin cellar-javascript-perf
node tools/perf/javascript/run.mjs
python3 tools/perf/javascript/summarize.py .cellar/javascript-perf-<timestamp>
```

`CARGO_TARGET_DIR` must point to the build directory, or set `CELLAR_PERF_HOST`
to the executable's absolute path. Optional environment settings:

| Setting | Default |
| --- | --- |
| `CELLAR_PERF_SECONDS` | 5 seconds per measured trial |
| `CELLAR_PERF_WARMUP` | 1 second before each trial |
| `CELLAR_PERF_REPEATS` | 3 |
| `CELLAR_PERF_CONCURRENCY` | `1,8,32` |
| `CELLAR_PERF_BACKENDS` | `v8,node` |
| `CELLAR_PERF_WORKLOADS` | `echo-1k,echo-64k,read,write-sharded,write-hot` |
| `CELLAR_PERF_APP` | `.cellar/javascript-perf-app` |
| `CELLAR_PERF_OUTPUT` | fresh timestamped `.cellar/javascript-perf-*` directory |

Each run requires a new output directory. A short harness check can use
`CELLAR_PERF_SECONDS=1 CELLAR_PERF_WARMUP=0 CELLAR_PERF_REPEATS=1`.
The default full matrix takes approximately ten minutes after compilation.
Do not compile or run other benchmarks concurrently with the measured campaign.

## Workloads and evidence

- `echo-1k` and `echo-64k`: stateless buffered HTTP echo. Validate every response
  byte. Includes HTTP, SDK byte encoding and bridge serialization; this is not a
  direct V8 function-call microbenchmark.
- `read`: route through the JS stateless handler into a cell and read its counter.
- `write-sharded`: one cell per concurrent client. Read, checked atomic apply,
  await `storage.sync()`, then acknowledge one increment.
- `write-hot`: all clients contend on one cell using the same checked apply.
  Proven `Conflict / not_committed` refusals return HTTP 409 and are counted as failures; no retries conceal contention or inflate TPS.

Cells are activated before the timed matrix. Each client maintains one outstanding
HTTP request over keep-alive connections. Duration includes the last request's
drain; TPS counts only validated successful write responses. Error counts and
samples accompany every trial. Warmup writes are included in the reconciliation
ledger but excluded from measured TPS. After every write trial, stored counts must
match all acknowledged increments, including warmup. After each backend, the host
shuts down gracefully and starts with a fresh node identity and empty local data
using the same bucket. Every counter must recover exactly. This recovery check
is not a SIGKILL or power-loss test.

`report.json` records source/build hashes, platform, settings, resource samples,
trial summaries and recovery results. `trials.jsonl` also retains warmup results.
Separate JSON files preserve every successful request's latency. Host logs and
private disposable state remain beside the report. On an exception the partial
report and diagnostics remain available, and the owned process is stopped.

Percentiles use nearest rank over successful full request/response latencies.
These are closed-loop capacity measurements, with no coordinated-omission
correction. Body MiB/s counts request plus response payload, excluding HTTP/TCP
headers, JSON/base64 expansion on internal channels and replication traffic.
Resource observations use `ps` on macOS/Linux: aggregate host/descendant CPU and
RSS, plus client CPU and system load. RSS sums shared pages and periodic samples
can miss short peaks. CPU cores are CPU seconds divided by wall seconds.

These local results do not establish cloud-object-store, multi-node, independent
arrival-rate SLOs, soak performance or a workload-independent maximum capacity.
A busy development machine is especially unsuitable for authoritative capacity
claims. Compare repeated trials and rerun on isolated deployment hardware.
