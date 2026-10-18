# SQLite operation costs — 2026-10-18

The candidate removes avoidable SQLite statement compilation in bulk KV writes,
SQL accounting and alarm/work queries. This is a local operation comparison,
not a fleet throughput or HTTP latency result.

## Results

Each value is the median of six samples of the **average microseconds per
operation cycle**, not a request-latency percentile. Full sample ranges and
binary hashes are in the [JSON evidence](2026-10-18-storage-operations.json).

| Cycle | Baseline µs | Candidate µs | Lower cost |
| --- | ---: | ---: | ---: |
| bulk-root | 15.674 | 8.502 | 45.8% |
| bulk-nested | 13.255 | 6.406 | 51.7% |
| clone-delete | 40.114 | 30.237 | 24.6% |
| sql-read | 10.354 | 6.801 | 34.3% |
| alarm-read | 4.426 | 1.406 | 68.2% |

`bulk-root` writes four JSON keys atomically, then checks one stored value.
`bulk-nested` does the same inside an outer transaction, rolled back after the
sample. `clone-delete` puts two 64-byte blobs and deletes both in separate
atomic batches. `sql-read` executes a prepared request returning 42, checks its
rows and read-only metadata, including database-size accounting. `alarm-read`
checks a previously installed alarm. Bulk keys and values stay fixed across
cycles; this isolates operation overhead rather than a changing-data workload.
Every operation cycle verifies its result.

## Method

Baseline production source is `17f6486` (merged PR #86), with the same benchmark
logic added. The candidate uses the changes in this branch. Rust 1.98.1,
workspace `lab` profile, system allocator, Apple M2 Max with 32 GiB memory,
macOS ARM64, default temporary-directory storage, SQLite WAL/NORMAL in both
variants. No replication or durability-output gate is present in this harness.
The takeover reconciliation fix was added after timing; this harness does not
exercise actor routing or ownership, and its results do not measure that fix.

Three rounds alternate variant order. Each process runs two repetitions of
2,000 cycles per case after 100 warm-up cycles; the second repetition reverses
case order. Every process opens a fresh database. The formal comparison ran
after these builds and the selected SDK tests finished. This was a shared
workstation without CPU isolation. The clone/delete sample ranges overlap;
these results do not establish universal gains or high-concurrency tail latency.
The exploratory run during compilation is excluded from the comparison.

Build and run from the repository root:

```sh
cargo run -p cellar --bin cellar-perf-storage --profile lab --locked -- 2000 5
```

## Preserved contracts

| Requirement | Implementation and evidence |
| --- | --- |
| Single lookups avoid writer reservation | A second SQLite connection holds `BEGIN IMMEDIATE`; KV and allocation reads succeed without changing the write position or opening a transaction. |
| Atomic writes and nested rollback | Root batches retain immediate transactions; nested batches retain savepoints. Failure and panic tests preserve prior outer writes and remove partial batch writes. |
| Restricted SQL and cached statements | The authorizer remains installed. Application SQL, cursors and SQL-binding requests still refuse forbidden pragmas and system tables after host accounting. Scoped origin/transaction state restores on unwinding. |
| Wake generation and proof identity | Cache fixed SQL only; keep SQL predicates, generation updates, publication identities, committed-state and unfinished-cursor checks. Existing work/alarm tests and a warm-query regression cover these boundaries. |
| Ownership, durable acknowledgment and recovery | Output gates, replica policy and recovery admission are unchanged. The acquisition reconciliation budget now survives foreign-owner lease lookup and log recovery. Tests require bounded unconfirmed claims to refuse admission and confirmed readback to restore the predecessor. No application mutation retries are added. |

The [storage regressions](../../../crates/cellar/storage/sqlite/transaction_cache_tests.rs),
[work tests](../../../crates/cellar/storage/sqlite/work.rs), and
[takeover regressions](../../../crates/cellar-logic/tests/routing_cache.rs)
exercise these boundaries. The [benchmark source](../storage_operations.rs) defines each cycle.

Before the changes, regression tests observed a warm query recompile after
bulk writes and ten recompilations across ten accounting cycles. Ten repeated
scheduling cycles increased authorizer callbacks from 223 to 2,433. After the
changes, these fixtures add no recompilations or authorizer callbacks after
warm-up. The cache stores compiled SQL, not application values.

## Validation status

- Full library run: 355 passed, including rollback, unwinding, authorization
  isolation and writer-contention regressions.
- Selected SDK suites: native storage, SQL, runtime, replication, call ordering
  and counter tests passed (12 tests).
- Clippy for Cellar and its decision core, all targets/features, passed with
  warnings denied. Format/layout, native-dependency and documentation guards
  passed.
- Captured-output node runs exposed a deadline during cold-source stream
  resolution, also seen in an older binary. The successor's local recovered
  read succeeded; a proposed read-only peer readiness barrier passed isolated
  repetitions but failed under the broader suite and was removed.
- A deterministic decision-core regression then demonstrated that foreign-owner
  lease lookup reset the ambiguous-acquisition reconciliation counter. The
  counter now survives lease lookup, predecessor-log recovery, recovery backoff
  and activation queuing. Unconfirmed
  acquisition stops at the existing bound without admitting a writer; a
  confirmed readback preserves the predecessor restore identity. All 14 public
  decision-core integration tests passed. External simulation/conformance
  sources were not enabled.
- The cold fixture retains its original recovery audit and adoption deadline;
  only diagnostic error context was added. No adoption or ambiguous application
  mutation is retried. The clean-source run passed all 86 node tests with normal
  output capture, including the cold-follower deadline case, settlement replay race
  and failed-predecessor admission test. All temporary probes are removed.
- Cloudflare/R2 performance and fault qualification have not been repeated.
