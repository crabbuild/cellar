<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# JS/TS bridge performance — 2026-10-22

## Result

Embedded V8 and Node.js both pass the small-payload throughput and durable-write probes. The same typed application compiles to JavaScript for each backend; TypeScript adds no runtime interpreter in this comparison.

**Embedded V8 failed the 64 KiB / 32-client stress case in two independent runs.** The first explicitly reports its default 100 ms turn watchdog expiring; the second reports `JavaScript dispatch failed`. The second diagnostic does not establish the same root cause. No runtime fix or timeout increase is included in this change.

These are short, closed-loop measurements on a **shared development machine**, not production capacity guarantees.

## Environment and method

- Apple M2 Max, 12 logical CPUs, 32 GiB RAM, macOS arm64. Significant unrelated VM/development activity was observed; no unrelated processes were stopped.
- Source baseline `64a4dfd801a5ab8e213ba9564f2f3406bbcca56d` plus the benchmark files in this change. Source and executable digests are in the raw reports and portable evidence.
- Rust 1.98.1, repository `release` profile (`opt-level=s`, fat LTO, one codegen unit). Two Tokio workers; default bridge options (64 invocations, 128 MiB JS heap, V8 100 ms turn budget).
- Embedded `v8` crate 152.2.0; Node v24.13.1 / V8 13.6.233.17-node.40. One runtime per host. Both adapters are compiled into the measurement executable.
- Real loopback HTTP, keep-alive, one outstanding request per client. Payloads validated byte for byte. Warm cells; one-second warmup and three five-second measured trials per workload/concurrency.
- Median RPS/TPS across repeats. Latencies pool successful responses using nearest rank. Timing includes response validation and final drain. No coordinated-omission correction.
- Durable writes: read snapshot → checked atomic apply → await `storage.sync()` → acknowledge. Private local SQLite development bucket, bucket durability, no cloud network or peers. No retries.

## Throughput and latency

| Backend | Workload | Clients | Median req/s or TPS | Repeat min–max | p50 ms | p95 ms | p99 ms | Errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v8 | echo-1k | 1 | 756.0 | 654.9–779.6 | 1.30 | 1.56 | 1.71 | 0 |
| v8 | echo-1k | 8 | 5202.9 | 4723.5–5275.5 | 1.53 | 1.88 | 2.15 | 0 |
| v8 | echo-1k | 32 | 14154.0 | 13208.1–14197.3 | 2.27 | 2.90 | 3.32 | 0 |
| v8 | read | 1 | 198.6 | 195.7–199.5 | 4.99 | 5.19 | 6.22 | 0 |
| v8 | read | 8 | 1549.3 | 1473.7–1551.6 | 5.12 | 5.45 | 6.79 | 0 |
| v8 | read | 32 | 5664.9 | 5558.2–5704.5 | 5.58 | 6.04 | 7.81 | 0 |
| v8 | write-sharded | 1 | 86.1 | 84.4–87.4 | 11.32 | 13.57 | 18.59 | 0 |
| v8 | write-sharded | 8 | 314.7 | 309.8–315.2 | 24.79 | 35.45 | 49.66 | 0 |
| v8 | write-sharded | 32 | 459.1 | 416.2–460.8 | 71.21 | 95.38 | 110.19 | 0 |
| v8 | write-hot | 1 | 83.7 | 79.8–87.4 | 11.46 | 14.62 | 24.10 | 0 |
| v8 | write-hot | 8 | 147.7 | 146.2–148.1 | 16.61 | 29.30 | 48.20 | 6,393 |
| v8 | write-hot | 32 | 193.3 | 192.4–196.7 | 24.49 | 33.91 | 40.81 | 24,217 |
| node | echo-1k | 1 | 398.0 | 395.8–398.2 | 2.52 | 2.59 | 2.72 | 0 |
| node | echo-1k | 8 | 3060.9 | 2815.8–3097.4 | 2.59 | 3.10 | 3.25 | 0 |
| node | echo-1k | 32 | 11088.3 | 9373.8–11671.7 | 2.81 | 3.74 | 5.16 | 0 |
| node | read | 1 | 97.4 | 75.6–99.0 | 10.08 | 13.69 | 27.20 | 0 |
| node | read | 8 | 675.2 | 646.0–774.9 | 10.33 | 13.21 | 36.41 | 0 |
| node | read | 32 | 2036.6 | 2034.2–2917.4 | 11.34 | 24.32 | 49.41 | 0 |
| node | write-sharded | 1 | 36.8 | 28.3–51.2 | 21.31 | 40.33 | 131.91 | 0 |
| node | write-sharded | 8 | 234.7 | 158.0–258.2 | 32.66 | 62.05 | 111.07 | 0 |
| node | write-sharded | 32 | 451.2 | 435.7–476.2 | 69.18 | 95.88 | 115.79 | 0 |
| node | write-hot | 1 | 51.6 | 43.2–52.0 | 19.68 | 26.14 | 33.34 | 0 |
| node | write-hot | 8 | 113.0 | 102.8–113.5 | 21.09 | 27.89 | 36.28 | 4,576 |
| node | write-hot | 32 | 166.2 | 153.6–174.0 | 28.87 | 40.10 | 46.90 | 18,417 |
| v8 | echo-64k | 1 | 259.9 | 225.8–292.2 | 3.44 | 6.14 | 10.24 | 0 |
| v8 | echo-64k | 8 | 372.7 | 315.0–388.7 | 20.38 | 32.97 | 40.61 | 0 |
| node | echo-64k | 1 | 204.8 | 184.9–214.0 | 4.59 | 6.90 | 10.69 | 0 |
| node | echo-64k | 8 | 436.1 | 428.4–441.4 | 17.43 | 24.42 | 44.92 | 0 |

At eight clients, 64 KiB combined-body throughput is **46.59 MiB/s for V8** and **54.51 MiB/s for Node.js**. This counts both request and response bodies; one-way payload throughput is half that figure. HTTP framing, internal base64/JSON expansion and replication bytes are excluded. Node is not universally slower: its concurrent durable TPS is similar here, and this large-body case favors Node.

For 32-client small echoes, median host-plus-child CPU was 1.02 cores (V8) and 1.16 cores (Node); load-generator CPU was 0.40 and 0.37 cores. Peak sampled aggregate RSS was approximately 89 MiB and 245 MiB. CPU/RSS are `ps` observations, sum shared pages, and can miss transient peaks. Periodic sampling also adds some measurement overhead.

## Failures and correctness

- The complete small-payload campaign made 754,439 measured requests. All 53,603 failures were explicit hot-cell HTTP 409 `Conflict / not_committed` refusals. All other measured requests succeeded. Warmup conflicts are recorded separately (10,440).
- At 32 clients contending on one cell, 89.2% of V8 attempts and 88.1% of Node attempts conflicted. Successful TPS alone would conceal this poor admission outcome. One-client hot-cell trials had no conflicts.
- All **41,344 acknowledged increments**, including warmup, reconciled exactly. Every write trial checked stored counts. After each backend, graceful shutdown and restart with a new identity and empty local node directory recovered every count from the same bucket. This is not a crash, power-loss, or external-side-effect proof.
- The lower-concurrency large-body campaign completed all 12 trials without errors. Node also completed three 32-client large-body trials without errors, but its rates ranged from 227.1 to 425.8 req/s and per-trial p99 from 148.9 to 213.4 ms. These short shared-machine results are noisy.
- V8 exited with `JavaScript engine turn exceeded execution budget` in the initial full matrix at 64 KiB / 32 clients. In the independent stress campaign, V8 passed repeat 1 and exited during repeat 2 with `JavaScript dispatch failed`. The second failure's precise cause remains unconfirmed. Partial/failed trials are retained and are not used as successful capacity estimates. The watchdog was not relaxed.
- After the initial failure, the harness was improved to detect process exit, preserve the interrupted trial and abort promptly, instead of continuing to send requests to a stopped host. Both failed reports remain available.

## Follow-up priorities

1. **Investigate V8 pump fairness and failure diagnostics.** The [engine](../crates/cellar-javascript/runtime/engine.rs) installs one deadline before admitting a batch of invocations and running microtasks. Batch work, garbage collection and OS scheduling can consume the shared budget. This is a code-based hypothesis consistent with the first failure, not a CPU-profile attribution or proof of the second failure's cause. Preserve the reason for a failed dispatch.
2. **Replace fixed polling with wake-driven scheduling.** Both engine pumps sleep for 1 ms each iteration. Nested cell/storage calls cross multiple rounds. This is a candidate contributor to low-concurrency latency, not a measured attribution.
3. **Profile buffered codecs.** Bodies traverse base64/JSON; the common SDK encodes base64 in JavaScript. Test binary/native codecs while preserving limits and failure contracts.
4. **Handle hot-cell contention explicitly.** This fixture intentionally does not retry. Real applications need serialization, atomic server-side operations, or bounded retries only after proven `not_committed` conflicts. Unknown mutation outcomes must not be blindly retried.
5. **Qualify deployment hardware.** Add isolated-host measurements, a comparable native Rust workload, open-loop arrivals, longer steady-state/soak windows, real object storage and multi-node durability before setting SLOs. These results do not isolate bridge overhead from HTTP/storage.

## Reproduce and evidence

See the [benchmark instructions](../tools/perf/javascript/README.md). The benchmark adds no production runtime behavior changes. Use the required checkout-specific `CARGO_TARGET_DIR` and fresh output directories.

```sh
node tools/perf/javascript/build.mjs
cargo build --locked --release -p cellar-javascript --features embed,node --bin cellar-javascript-perf
CELLAR_PERF_WORKLOADS=echo-1k,read,write-sharded,write-hot \
  CELLAR_PERF_OUTPUT="$PWD/.cellar/perf-small-new" node tools/perf/javascript/run.mjs
CELLAR_PERF_WORKLOADS=echo-64k CELLAR_PERF_CONCURRENCY=1,8 \
  CELLAR_PERF_OUTPUT="$PWD/.cellar/perf-large-new" node tools/perf/javascript/run.mjs
CELLAR_PERF_WORKLOADS=echo-64k CELLAR_PERF_CONCURRENCY=32 CELLAR_PERF_BACKENDS=node,v8 \
  CELLAR_PERF_OUTPUT="$PWD/.cellar/perf-stress-new" node tools/perf/javascript/run.mjs
python3 tools/perf/javascript/summarize.py .cellar/perf-small-new
```

The stress command can fail; inspect its report and host logs rather than hiding its exit status.

[Portable measurement evidence](evidence/javascript-bridge-performance-2026-10-22.json) includes summaries, hashes and configuration. Local raw evidence under `.cellar/` is ignored by Git:

- `javascript-perf-small/`: 72 completed trials, warmups, every successful request latency, resource samples, reconciliation and two cold recoveries.
- `javascript-perf-large/`: 12 completed trials and two cold recoveries.
- `javascript-perf-stress/`: three completed Node trials/cold recovery, one completed V8 trial and a second failed V8 trial.
- `javascript-perf-measured/`: original default-matrix failure, before the harness's fail-fast improvement.
- `javascript-perf-smoke*/` and `javascript-perf-conflict-check/`: setup/short-run diagnostics, excluded from headline measurements.

Validation: strict TypeScript compilation and both bundles; release build; benchmark Clippy with warnings denied; format/layout, native dependency boundary, documentation references; real HTTP benchmark, payload checks and count reconciliation. Existing vendor warnings remain. No production bridge implementation was modified.
