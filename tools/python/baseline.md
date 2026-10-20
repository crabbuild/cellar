# Native and embedded-Python measurements

`baseline.py` drives compiled hosts through real loopback HTTP. The Rust host is
built without `embed` or libpython; Python hosts use the same Cellar node, four
Tokio workers, compiled deployment, private development bucket, leases and
durability settings. The implementations share an HTTP contract: buffered byte
echo, or a named cell's presence-aware read and checked atomic counter update.
One sequential client owns each counter, avoiding conflicts that would obscure
adapter overhead. Successful responses require the normal native output proof.

The default run measures empty and 4,096-byte echo plus durable counters at
concurrency one and four. Each worker warms its persistent HTTP connection,
waits at a common measurement barrier, then issues 100 measured requests.
Three repetitions rotate runtime order. Every response is checked. Raw request
attempts, latencies, p50/p95/p99/max and achieved closed-loop requests per second
are retained in `baseline.json`. Errors retain attempted commands with unknown
outcomes and fail the run; no mutation is resent.

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
- Python copy deltas count explicit owned UTF-8 bridge copies and include
  warmup. They exclude Python codec caches, transport, SQLite and other internal
  copies. Native has no Python boundary: its bridge field is `null`, not an
  invented total-copy count. Payload sizes and raw attempts exist for both.
- Recovery includes the deliberate lease wait. Compare cold-read/startup fields
  before attributing the total to Python or Cellar.
- Hardware, background load, OS caches, ABI, compiler/profile and offered traffic
  affect results. Compare repeats with those inputs fixed. Failed runs remain
  failed evidence; cleanup cannot erase workload or recovery failure.
