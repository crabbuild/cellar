# Native and embedded-Python measurements

`baseline.py` drives compiled hosts through real loopback HTTP. The Rust host is
built without `embed` or libpython; Python hosts use the same Cellar node, four
Tokio workers, compiled deployment, private development bucket, leases and
durability settings. The implementations share an HTTP contract: buffered byte
echo, or a named cell's presence-aware read and checked atomic counter update.
One sequential client owns each counter, avoiding conflicts that would obscure
adapter overhead. Successful responses require the normal native output proof.

Durable results include the development bucket's SQLite writes and disk syncs.
Clones of one store admit writers in FIFO order before starting SQLite write
transactions, so local writer contention does not rely on busy-handler retries.
Separate store instances and processes still coordinate through SQLite. Prefix
listings use the key index before decoding metadata, so restoring one cell does
not scan every unrelated cell's history. Literal prefixes and delimiter/page
boundaries retain their object-store semantics. This backend is for local
development. Listing queries and result conversion run in the owning execution
domain's blocking pool; cancellation leaves entered SQLite work tracked until
shutdown joins it. A listing stream retains its original domain even when
another domain polls it. These measurements do not establish production
object-store throughput or lease-renewal latency.

The default run measures empty and 4,096-byte echo plus durable counters at
concurrency one and four. Each worker warms its persistent HTTP connection,
waits at a common measurement barrier, then issues 100 measured requests.
Three repetitions rotate runtime order. Every response is checked. Raw request
attempts, latencies, p50/p95/p99/max and achieved closed-loop requests per second
are retained in `baseline.json`. Errors retain attempted commands with unknown
outcomes and fail the run; no mutation is resent.
The first worker failure remains the reported cause even when other workers
abort afterward. Failed attempts retain their exception type and message. Before
killing a still-running failed fixture, cleanup allows two seconds for its final
diagnostics, then records the process status and whether cleanup killed it in
`primary-exit.json` or `recovered-exit.json`. This grace period is outside the
measurement and changes no request or durability deadline. Native fence warnings
are included in the host log.

After each workload the driver kills and reaps the owner, verifies it produced
no clean report, deletes every owner-local cell file, waits out its 3,000 ms
lease and starts a different owner against the private bucket. Every counter
must match all acknowledged warmup and measured updates. A new write to every
counter must succeed. Recovery records process startup, cold-read latencies and
kill-to-all-verified-reads duration, including the explicit lease wait. The
replacement produces complete native and Python joined shutdown reports.

## Build and run

Use the checkout-specific writable workspace-volume `CARGO_TARGET_DIR` required
by [AGENTS.md](../../AGENTS.md) and the repository Rust toolchain. Select actual
CPython paths; standard and free-threaded builds are separate ABIs. This
standard-library workload measures the adapter without third-party packages;
installed dependency qualification is separate.

```sh
export CELLAR_BASELINE_BIN="$(mktemp -d /tmp/cellar-baseline-XXXXXX)"
cargo build -p cellar-python --example python-baseline --profile lab \
  --no-default-features --locked
cp "$CARGO_TARGET_DIR/lab/examples/python-baseline" "$CELLAR_BASELINE_BIN/native"

PYO3_PYTHON=/absolute/path/python3.14 \
  cargo build -p cellar-python --example python-baseline --profile lab --features embed --locked
cp "$CARGO_TARGET_DIR/lab/examples/python-baseline" "$CELLAR_BASELINE_BIN/python-gil"

PYO3_PYTHON=/absolute/path/python3.14t \
  cargo build -p cellar-python --example python-baseline --profile lab --features embed --locked
cp "$CARGO_TARGET_DIR/lab/examples/python-baseline" "$CELLAR_BASELINE_BIN/python-free-threaded"

python3 tools/python/baseline.py --native-host "$CELLAR_BASELINE_BIN/native" \
  --python-host "gil=$CELLAR_BASELINE_BIN/python-gil" \
  --python-host "free-threaded=$CELLAR_BASELINE_BIN/python-free-threaded" \
  --output "$CELLAR_BASELINE_BIN/evidence" --build-profile lab --count 100 --warmup 10 \
  --concurrency 4 --repetitions 3
python3 tools/python/baseline_report.py "$CELLAR_BASELINE_BIN/evidence/baseline.json" \
  > "$CELLAR_BASELINE_BIN/evidence/report.md"
```

The output directory must be new and owns all processes and test data. The
script never uses a live fleet or deletes outside that directory. Copy each
built executable because Cargo reuses its example path. Native mode refuses an
`embed` executable. Evidence records binary SHA-256s, source commit/diff digest,
platform, client and caller toolchain versions, declared build profile, host
settings and sampled load averages. The host's actual debug-assertion mode must
match that declaration; the caller still owns the build/profile provenance.
Python reports
its actual ABI/GIL state before/after work and recovery; a free-threaded run must
remain GIL-off.

## Initial snapshots and handoff measurements

The `python-initial` and `python-initial-resident` host modes register an initial
`count` snapshot. The `python-direct-initial` and
`python-direct-initial-resident` modes also route `/cells/NAME` through native
ingress before invoking the Python cell. Build the embedded host as above, then
start it with `python-baseline MODE DIRECTORY primary` using a new private
directory. Use the listener address in its readiness file.

Python bridge observations include bounded `dispatch_handoff` and
`completion_handoff` latency histograms. Dispatch measures native enqueue until
Python delivery; completion measures Python enqueue until the original native
invocation receives the reply. These include scheduling delays across the bridge,
but do not replace end-to-end HTTP latency or durability measurements. Compare
counter deltas after draining work, and retain raw evidence outside the repository.

## Independent offered rates

`offered.py` uses a separate native Rust HTTP client to schedule echo requests
independently of response completion. This exposes queueing and overload that
the closed-loop client above can hide. Build the hosts as above, then run:

```sh
cargo build -p cellar-python --example python-offered-load --profile lab \
  --no-default-features --locked
python3 tools/python/test_offered.py "$CARGO_TARGET_DIR/lab/examples/python-offered-load"
python3 tools/python/offered.py \
  --client "$CARGO_TARGET_DIR/lab/examples/python-offered-load" \
  --native-host "$CELLAR_BASELINE_BIN/native" \
  --python-host "free-threaded=$CELLAR_BASELINE_BIN/python-free-threaded" \
  --output "$CELLAR_BASELINE_BIN/offered" --build-profile lab \
  --rates 1000 5000 10000 --duration-ms 5000 --warmup-ms 500 \
  --max-inflight 64 --body-bytes 4096 --timeout-ms 1000 --max-lag-ms 5
```

The driver rotates host order, verifies each raw arrival record, samples host
telemetry and requires joined shutdown. Python observations must advance to a
fresh idle snapshot after each phase; a cached idle sample from during the load
cannot stand in for completed work. It uses local-only hosts with zero
resident cells to isolate HTTP adapter cost. Each warmup starts a separate
client process, so it warms server code but not the measured connection pool.
It does not replace durable-write, recovery, activation-churn or multi-node
qualification.

For cell reads, add `--read-cells 32 --body-bytes 0` and choose a new output
directory. The driver first populates 1–64 local-only cells, then sends GETs
through the Python/native root handler, cell dispatch and JSON storage read.
Every response must match the fresh counter value, and telemetry must confirm
the full resident population before and after each measurement. Python must
retire every created object at shutdown. The same scheduling, failure accounting
and rotated comparisons apply. This measures read dispatch and bridge overhead;
it does not test durable mutations or fleet recovery.

Use `--python-read-mode initial` to capture each cell's `count` snapshot before
Python dispatch, or `--python-read-mode direct-initial` to also route requests
through native ingress. Both require `--read-cells` and keep the same JSON
response and native Rust control. The default `nested` mode retains the root
Python handler and SDK read. Reports record the selected mode and the actual
host fixture; direct snapshot reads must submit zero SDK operations during each
measured phase. Compare these modes separately: the direct route measures one
Python cell handler per request, while nested routes measure two. Initial
snapshots represent state before the handler runs and do not refresh after an
await. These local-only read comparisons do not establish durable-write or
FastAPI performance.

For nested HTTP bodies, add `--cell-echo --body-bytes 65536` and choose a new
output directory. The root handler forwards each POST to one resident cell,
which echoes the exact request bytes without storage access. The driver checks
that cell's residency, two Python handler completions per successful request,
and joined object retirement. Empty through 65,536-byte bodies are supported;
`--cell-echo` cannot be combined with `--read-cells`. This isolates nested body
transport from database work while retaining real native cell dispatch.

On Linux, pass `--host-cpus 0,1 --client-cpus 2,3` to place the host and client
on disjoint logical CPU sets. Choose IDs from `os.sched_getaffinity(0)` on the
measurement machine; both lists are required, and unavailable or overlapping
IDs refuse the run. Placement happens before either executable starts its
threads. The report records the requested sets; `host-affinity.json` and each
`*.affinity.json` retain the child PID and kernel-observed mask before exec.
Host readiness also checks its live mask. Executable hashes still identify the
actual Rust binaries. Without these options placement is unchanged on all OSes.
Affinity is not exclusive CPU allocation, a CPU-time quota, memory isolation or
protection from shared-runner noise. It reduces direct client/server competition
but does not establish production capacity. Compare both implementations with
the same masks, body sizes, rates, build profile and repetition count.

The SDK workflow's `python-load` mode applies disjoint CPU sets on Linux.
Set `load_read_cells` to 1–64 to select the resident-cell read workload; its
default of zero retains echo. Set `load_cell_echo=true` with zero read cells to
forward echoes through one resident cell. `load_body_bytes` selects the echo
body size (0–65,536, default 4,096); cell reads always use zero request bytes.
Live host thread masks are checked before and after each measured phase for all
workloads.
With nonzero `load_read_cells`, set `load_python_read_mode` to `nested`, `initial`
or `direct-initial` to select the corresponding Python path described above.
Set its optional `load_baseline_sha` to a full commit SHA to build another Python
host on that same runner and include it in the rotated offered-load comparison.
Both revisions use the selected matrix interpreter and toolchain. The job
refuses differing measurement fixtures or Cargo build profiles, freezes each
binary before another build can replace it, and retains source revisions and
binary hashes in `builds.json`. Reference builds use a separate target directory;
shared Cargo artifacts can otherwise reuse an executable with stale embedded
Python source. Before measuring, the job verifies that each frozen Python
executable contains its checkout's exact facade and runner bytes and that the
native executable contains neither. Source hashes and byte offsets are retained
in the build record. This compares revisions under one resource
placement; shared-runner noise and all client drops still qualify the results.

Every planned arrival has an outcome in `measured-RATE.json`: success, client
capacity, client lateness, timeout, transport error, HTTP status or body mismatch.
The client never waits for an admission slot, never sends after the measurement
window, and disables redirects and automatic retries. In-flight work and total
samples are bounded; admitted requests drain under their original timeouts.
An existing output file is refused before traffic starts.
For diagnosis, at most sixteen HTTP failures retain the first 1,024 response
bytes as lossy UTF-8. All HTTP statuses and arrival outcomes remain recorded;
the bounded diagnostic sample does not represent the frequency of each error.

Read `offered.json` outcomes together with both latency distributions: request
latency starts at dispatch, while scheduled latency includes client dispatch
delay. Both distributions contain successful responses only; drops and failures
remain in the denominator of the success fraction. The report distinguishes
successful completions inside the fixed window from throughput including the
final drain. A completed experiment can contain overload and failed requests.
Its status is not a pass/fail application SLO.

The two client workers share the host machine. Tokio timer granularity can
group arrivals, and client admission limits can constrain the experiment.
Inspect dispatch delay and client drops before attributing a throughput ceiling
to Cellar. These measurements alone do not establish native parity or production
capacity. `test_offered.py` independently checks real HTTP echo, bounded admission
under slow responses, timeout/body/status failures, no redirect/resend, output
preservation and rejection of corrupted timing evidence.

The client also accepts an optional final `READ_TARGETS.json` argument for
read-only application measurements. It sends GETs, rotating targets by scheduled
arrival ID, and compares each complete HTTP 200 body with its expected bytes.
The manifest is an array of `{ "path": "/orders/id", "expected_body": [123, 125] }`;
paths stay on the supplied private loopback origin. At most 64 distinct paths,
65,536 bytes per body and 1 MiB of total expected bodies are accepted. Set
`BODY_BYTES` to zero in this mode. The manifest is copied into the raw workload
record; response comparison remains streaming and bounded. The
[FastAPI driver](../../examples/python-fastapi/README.md#independent-arrival-reads)
seeds real durable orders and captures their expected responses before timing.

The Python host records `Options::default()` housekeeping and telemetry intervals.
Native readiness notifications wake the Python loop independently of those
timers. Compare an old polling executable and a new executable as separate
runtime labels, retaining their binary digests and the source revision for each
build; a timer value alone does not identify the bridge implementation.

`baseline_report.py` accepts JSON or gzip-compressed JSON. Before rendering it
checks complete repetitions, raw request/percentile/copy consistency, recovered
values against the acknowledged command ledger, subsequent writes, and native
and Python cleanup. Corrupted receipts, published percentiles or incomplete
drain/cleanup refuse a report. Keep generated measurements outside the source
tree with their exact build, runtime and workload scope.

## Resident-cell scaling

`residency.py` measures stateless echo with 0, 64, 512 and 2,048 real resident
cells by default; `--populations` accepts increasing counts through 4,096. It
uses the separate `native-resident` and `python-resident` fixture modes with
4,096 native cell slots and no fleet store, isolating bridge scheduling from
remote proof and lease latency. All other benchmark modes keep their existing
fleet configuration and durability checks.

```sh
python3 tools/python/residency.py \
  --native-host "$CELLAR_BASELINE_BIN/native" \
  --python-host "gil=$CELLAR_BASELINE_BIN/python-gil" \
  --python-host "free-threaded=$CELLAR_BASELINE_BIN/python-free-threaded" \
  --output "$CELLAR_BASELINE_BIN/residency" --build-profile lab \
  --populations 0 64 512 2048 4096 --count 200 --warmup 10 \
  --concurrency 4 --repetitions 3
```

Population requests check each fresh cell's zero value. Before and after each
level, the driver verifies the live native cell count and a fresh, idle Python
object snapshot. Echo responses and raw latency/copy samples are checked through
the same client as the durable baseline. Complete shutdown must retire exactly
the populated Python objects; activation churn cannot masquerade as a stable
population. Runtime order rotates, failed attempts stay in `residency.json`, and
binary digests, ABI/GIL state, load averages and effective policy are recorded.
This is a local scheduling measurement, not a durability, recovery, fleet
capacity or long-running resource qualification.

## Shipping Python defaults

`operating_defaults.py` uses the same compiled Python executable in the separate
`python-defaults` fixture mode. It passes one unchanged `Options::default()` to
the runtime and records its effective limits. Native stateless admission is
explicitly raised to twice the Python invocation limit, with zero admission
wait, so native admission cannot mask Python saturation. Other fixture policies,
including the five-second handler deadline and private SQLite bucket, are
recorded. This does not measure an unchanged default native node policy.

```sh
python3 tools/python/operating_defaults.py \
  --python-host "gil=$CELLAR_BASELINE_BIN/python-gil" \
  --python-host "free-threaded=$CELLAR_BASELINE_BIN/python-free-threaded" \
  --output "$CELLAR_BASELINE_BIN/defaults-evidence" --build-profile lab \
  --count 40 --concurrency 1 4 16 32 --repetitions 3
python3 tools/python/operating_defaults_report.py \
  "$CELLAR_BASELINE_BIN/defaults-evidence/operating-defaults.json"
python3 tools/python/test_operating_defaults_report.py \
  "$CELLAR_BASELINE_BIN/defaults-evidence/operating-defaults.json"
```

Each worker rotates empty echo, one acknowledged durable counter update,
4,096-byte echo and bounded pure-Python CPU work. Warmup only echoes; measured
counter updates are sent exactly once and reconciled after owner kill and local
disk deletion. The body limit is exercised at its exact boundary and one byte
over. A separate cooperative gate holds exactly `max_invocations` requests with
65,536-byte bodies. A fresh interpreter snapshot must directly observe the full
capacity before sixteen excess requests are refused specifically by Python
invocation admission. Every held request must then succeed, bridge reservations
must settle, and subsequent work must succeed. This gate exercises suspension
and pressure; it does not demonstrate parallel CPU execution.

Raw evidence retains attempts, actual limits, latency, throughput, copied-byte
counters, age-stamped RSS/interpreter samples, recovery receipts and combined
shutdown. The report checks raw percentiles, the counter ledger, observed
saturation, refusals and resource conservation before rendering measurements.
Its CLI regression checks corrupt evidence under both ordinary Python and `-O`.
Existing configured-limit tests separately cover tasks, objects, mailbox, batch,
depth and byte exhaustion; this workload does not claim to saturate every limit
simultaneously or to cap arbitrary Python heap or native-library pools.

## Durable activation churn

`churn.py` rotates through more cells than the host's 64-cell resident limit.
Use the fixed executables from the build recipe above:

```sh
python3 tools/python/churn.py --native-host "$CELLAR_BASELINE_BIN/native" \
  --python-host "gil=$CELLAR_BASELINE_BIN/python-gil" \
  --python-host "free-threaded=$CELLAR_BASELINE_BIN/python-free-threaded" \
  --output "$CELLAR_BASELINE_BIN/churn" --build-profile lab \
  --population 128 --rounds 16 --concurrency 4 --repetitions 3
python3 tools/python/churn_report.py "$CELLAR_BASELINE_BIN/churn/churn.json"
python3 tools/python/test_churn.py --evidence "$CELLAR_BASELINE_BIN/churn/churn.json"
```

Each worker owns disjoint counters and sends each mutation once. Every round
must settle to bounded resident objects with no active Python tasks, pending
retirements or outstanding bridge work. The driver then kills the owner,
removes its local disk, recovers every acknowledged counter from the private
bucket and verifies fresh writes and complete joined shutdown. Fresh memory
and interpreter samples accompany the receipts; cached RSS is not a heap-leak
proof or an exact peak.

Failed requests retain unknown outcomes. When the owner reached readiness,
even a failed load run attempts recovery: reads must fall between acknowledged
and attempted counts. New writes follow reconciliation and never replay an
uncertain mutation. Successful recovery cannot change a failed load run into
a pass. The reporter refuses incomplete, failed or inconsistent evidence.
The regression tests include a server that commits and then drops its response.

The SDK CI matrix runs a short two-round correctness check in both Python ABIs
and retains its JSON and logs. For repeated optimized measurements on separate
Linux jobs, dispatch the existing workflow on a branch containing these tools:

```sh
gh workflow run native-sdk.yml --ref YOUR_BRANCH \
  -f mode=python-load -f load_repetitions=3
```

An optional `-f replay_sha=FULL_COMMIT_SHA` pins the checkout. Each ABI job
builds fixed native/Python `lab` artifacts, performs sixteen churn rounds per
repetition, then measures independent echo arrivals at 500, 5,000 and 15,000/s.
It retains raw arrivals, refusals, client drops, process logs and recovery
receipts for seven days, including failed runs. The diagnostic uses a separate
concurrency group so it does not cancel release qualification. It rejects
transport, body and unexpected HTTP errors; a completed experiment can still
contain HTTP 503 refusals and client drops, which must be read alongside
successful latency. These local SQLite/loopback workloads do not qualify the
production backend, a fleet deployment or application SLOs.

## Interpret the evidence

- This is closed-loop development traffic with client and server sharing a
  machine. It establishes neither maximum production capacity nor an SLO.
- `lab` uses the optimized thin-LTO profile with debug assertions disabled;
  shipped `release` uses size optimization and fat LTO. CI's three-request debug
  smoke validates the harness, not performance.
- The private SQLite development bucket does not represent remote object-store
  or follower-quorum latency. The owner is killed after acknowledged work,
  rather than during a write.
- RSS, jemalloc and available libc values are cached point samples every 200 ms,
  collected by the application supervisor outside node execution domains and
  the Python loop. Their age is recorded. They are neither exact peaks nor
  per-cell heap limits. Interpreter snapshots carry their own age. Allocation
  tracing is disabled.
- Python copy deltas count explicit owned UTF-8 and raw request-body bridge copies and include
  warmup. They exclude Python codec caches, transport, SQLite and other internal
  copies. Native has no Python boundary: its bridge field is `null`, not an
  invented total-copy count. Payload sizes and raw attempts exist for both.
- Recovery includes the deliberate lease wait. Compare cold-read/startup fields
  before attributing the total to Python or Cellar.
- Hardware, background load, OS caches, ABI, compiler/profile and offered traffic
  affect results. Compare repeats with those inputs fixed. Failed runs remain
  failed evidence; cleanup cannot erase workload or recovery failure.
