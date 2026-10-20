# Embedded Python development baselines

Measured on 2026-10-20 at source `1b92a324c5311ff814a076f80937ab038724a64f`
with a clean tracked source tree, Rust/Cargo 1.98.1 and the repository's optimized
thin-LTO `lab` profile (`CARGO_INCREMENTAL=0`). The machine reports macOS 26.5.2,
ARM64 and 12 logical CPUs. The client and server share this development machine.
The primary comparison uses provisioned CPython 3.14.5 and 3.14.5t, both built
with Clang 22.1.3. The free-threaded interpreter remains GIL-off before/after
traffic and recovery. The native executable is compiled without `embed`; its
linked-library listing contains no libpython.

This was a busy shared machine: sampled one-minute load averages ranged from
9.71 to 18.94. These are observed development baselines under that load, not
stable production capacity, an approved SLO, or causal evidence about the GIL.
The workload uses one managed Python loop and does not exercise CPU-parallel
Python or numerical libraries. The earlier Homebrew comparison used 3.14.6
against 3.14.5t; it remains separate rather than being pooled into these tables.

## Method and raw evidence

The [harness guide](../tools/python/baseline.md) contains complete build/run
commands and measurement definitions. The primary invocation was:

```sh
python3 tools/python/baseline.py \
  --native-host /tmp/cellar-6a8d-baseline-native-lab \
  --python-host gil=/tmp/cellar-6a8d-baseline-gil-matched-lab \
  --python-host free-threaded=/tmp/cellar-6a8d-baseline-ft-lab \
  --output /tmp/cellar-6a8d-baseline-matched-results --build-profile lab \
  --count 100 --warmup 10 --concurrency 4 --repetitions 3
```

All nine primary cases complete: three repetitions for each runtime, with
rotating execution order. Their 54 workload phases retain 13,500 measured
requests plus 1,350 warmup requests. Exactly 4,950 acknowledged durable counter
updates survive nine killed owners and complete deletion of their local cell
files. Every replacement verifies counts `220, 110, 110, 110`, accepts a further
write to all four counters, and produces complete native/Python joined reports.
There is no mutation retry. Recovery uses a private SQLite development bucket,
not a remote object store or a follower quorum.

- [Primary matched-build raw JSON, gzip](evidence/python-baseline-2026-10-20-matched.json.gz).
  Uncompressed SHA-256:
  `b39c95594b7a8c6080909d48143d141337f1ae01144efb4c9fa883e729bbb05c`.
- [Earlier Homebrew-build raw JSON, gzip](evidence/python-baseline-2026-10-20-homebrew.json.gz).
  Uncompressed SHA-256:
  `cf1a9ad4c119f1cb876d1d36500564f84a0f86e48b901117cc207d063ace1f29`.

Both datasets preserve binary digests, platform/toolchain/profile, actual Python
ABI/GIL state, per-request attempts, raw latencies, copy and memory observations,
recovered receipts and joined reports. The
[report verifier](../tools/python/baseline_report.py) checks each published
percentile and throughput value against raw samples, reconciles every recovered
counter with its acknowledged command ledger, checks subsequent writes and
requires complete native/Python cleanup. Deliberately corrupting a receipt,
p99 value, native drain flag or pending Python object refuses a report.

The primary executable SHA-256s are:

| Runtime | Executable SHA-256 |
| --- | --- |
| native | `2472dc712fd1f3a2e06e0b8de254dd7dbe1eb5f4bdf1c801e29d4432aee4dea9` |
| gil | `b532e987180f262914e1ed90d8f1c8b7872f707c73dcd52b6616589b073c0edd` |
| free-threaded | `823eb178c4e1b49f05abccd05874a875262d1cec1577917ea38e1a2212559615` |

## Measured comparison

Median of 3 per-run measurements; p50/p99 are medians of each run's
percentiles, not percentiles of a pooled distribution.

| Workload | Runtime | p50 (ms) | p99 (ms) | Requests/s |
| --- | --- | ---: | ---: | ---: |
| echo-0-c1 | native | 0.092 | 0.153 | 10300.7 |
| echo-0-c1 | gil | 1.161 | 1.266 | 858.4 |
| echo-0-c1 | free-threaded | 1.156 | 1.262 | 861.1 |
| echo-4096-c1 | native | 0.113 | 0.130 | 8837.9 |
| echo-4096-c1 | gil | 1.165 | 1.256 | 855.8 |
| echo-4096-c1 | free-threaded | 1.165 | 1.305 | 838.5 |
| counter-0-c1 | native | 4.877 | 7.679 | 190.4 |
| counter-0-c1 | gil | 10.421 | 14.522 | 86.8 |
| counter-0-c1 | free-threaded | 10.392 | 16.267 | 92.8 |
| echo-0-c4 | native | 0.192 | 0.555 | 17661.2 |
| echo-0-c4 | gil | 1.177 | 1.597 | 3366.3 |
| echo-0-c4 | free-threaded | 1.176 | 1.388 | 3382.4 |
| echo-4096-c4 | native | 0.302 | 0.693 | 12554.5 |
| echo-4096-c4 | gil | 1.203 | 1.500 | 3302.5 |
| echo-4096-c4 | free-threaded | 1.206 | 1.442 | 3293.3 |
| counter-0-c4 | native | 9.768 | 75.110 | 299.5 |
| counter-0-c4 | gil | 15.151 | 29.134 | 238.0 |
| counter-0-c4 | free-threaded | 14.329 | 26.109 | 258.6 |

| Runtime | Sampled RSS before kill (MiB) | Initial startup (ms) | Cold cell read (ms) | Kill to all verified reads (ms) |
| --- | ---: | ---: | ---: | ---: |
| native | 31.75 | 203.3 | 34.8 | 3469.5 |
| gil | 64.58 | 434.5 | 40.5 | 3702.8 |
| free-threaded | 73.69 | 414.4 | 38.8 | 3718.5 |

RSS is a cached process point sample. Recovery totals include a 3,100 ms
lease-expiry wait; neither is a per-cell limit or a production SLO.

| Workload | Runtime | Bridge input bytes/request | Bridge output bytes/request |
| --- | --- | ---: | ---: |
| echo-0-c1 | native | n/a | n/a |
| echo-0-c1 | gil | 80.0 | 268.0 |
| echo-0-c1 | free-threaded | 80.0 | 268.0 |
| echo-4096-c1 | native | n/a | n/a |
| echo-4096-c1 | gil | 5544.0 | 5735.0 |
| echo-4096-c1 | free-threaded | 5544.0 | 5735.0 |
| counter-0-c1 | native | n/a | n/a |
| counter-0-c1 | gil | 608.9 | 862.4 |
| counter-0-c1 | free-threaded | 608.9 | 862.4 |
| echo-0-c4 | native | n/a | n/a |
| echo-0-c4 | gil | 80.0 | 268.0 |
| echo-0-c4 | free-threaded | 80.0 | 268.0 |
| echo-4096-c4 | native | n/a | n/a |
| echo-4096-c4 | gil | 5544.0 | 5735.0 |
| echo-4096-c4 | free-threaded | 5544.0 | 5735.0 |
| counter-0-c4 | native | n/a | n/a |
| counter-0-c4 | gil | 611.1 | 864.3 |
| counter-0-c4 | free-threaded | 611.1 | 864.3 |

## Scope of the measurements

The bridge-copy table includes warmup in both byte totals and request counts.
For 4,096-byte echo, its two explicit UTF-8 boundary counters total 11,279 bytes
per request. These counters exclude Python codec caches, HTTP/native transport,
SQLite and other internal copies. Native's `n/a` means no Python bridge, not
zero total copies.

Memory is a cached whole-process point sample, including the host, interpreter,
imports, caches and native dependencies. Raw evidence includes its sample age,
jemalloc observations and unavailable libc values as null. It is neither a
precise peak nor an isolated Python/per-cell heap measurement. Allocation
tracing is disabled. Recovery includes an explicit 3,100 ms lease-expiry wait;
cold reads are measured separately, and the owner is killed after acknowledged
work rather than during a write.

The observed echo timings include the configured 1 ms Python bridge polling
interval. Counter timings also include checked storage, native routing and
bucket-backed output proof. This establishes a reproducible comparison for the
stated workload and machine; it does not impose a performance target, qualify
arbitrary packages, or complete the remaining authority/lifecycle/native-library
and final CI gates in the [completion audit](007-embedded-python-audit.md).
