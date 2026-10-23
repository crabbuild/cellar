# Embedded Python fleet validation

The Python counter in `fleet_app.py` uses the same S3 fleet host and load
generator as the [native counter](../perf/README.md). Each POST atomically
increments a counter and replaces its payload; GET returns the saved counter
and payload length. Python uses a checked storage batch while Rust uses a
synchronous transaction. Both wait for the configured durability proof before
acknowledging the HTTP response. The load generator never retries a write and
requires recovered counters to equal the number of complete acknowledged
responses.

This is a measurement fixture, not an inventoried application deployment.
Use the [package workflow](README.md) for production runtime and dependency
verification. The Python fixture uses the standard library only.

## Functional S3 recovery

Build on the checkout's Rust toolchain, with its own writable target directory
and a shared-library CPython 3.14.5 or 3.14.5t selected by `PYO3_PYTHON`:

```sh
cargo build --locked -p cellar --bin cellar-perf-cells --bin cellar-perf-load
cargo build --locked -p cellar-python --example python-fleet --features embed
docker pull rustfs/rustfs:1.0.0-beta.8-glibc
python3 tools/python/fleet_smoke.py \
  --runtime python --host "$CARGO_TARGET_DIR/debug/examples/python-fleet" \
  --load "$CARGO_TARGET_DIR/debug/cellar-perf-load" \
  --rustfs-image rustfs/rustfs:1.0.0-beta.8-glibc \
  --output "$CARGO_TARGET_DIR/python-fleet-recovery"
```

Run the native control with `--runtime native` and
`--host "$CARGO_TARGET_DIR/debug/cellar-perf-cells"`, using a new output
directory. Copy binaries to separate paths before rebuilding for another ABI;
never replace a binary while a campaign is running. An OS loader must be able
to find the selected libpython, as in the other embedded Python examples.

The fixture owns a private RustFS container limited to 0.5 CPU and 640 MiB,
random credentials, ephemeral ports and local process state under its output
directory. It uses 32 cells, eight resident cells per node, zero cached remote
routes and bounded request admission. It checks:

- One-node and three-node bucket recovery after killing every host and deleting
  local cell state.
- Three-node fleet takeover after killing an owner, followed by replacement.
- Rolling handoff of every node, new writes after each takeover, and a final
  preserving stop whose last node must reload alone.
- Every acknowledged counter, routing-cache bounds, Python ABI/GIL observations
  and complete native/Python shutdown reports.

Cleanup removes only the fixture's container and joins its processes. Logs,
receipt accounting, observed limits, source hashes and executable hashes remain
in the output directory. The disposable object-store contents do not survive
cleanup. All three Python scenarios require 16 complete joined shutdown reports;
a zero exit code or a saved `completed` flag alone does not establish success.
These short runs have no host CPU/memory isolation and do not qualify throughput,
latency, soak stability or fleet capacity.

Recovery reads may retry while an owner is unavailable. Their reports retain
the status counts and up to 16 failed-response samples, each containing at most
the first 1,024 body bytes decoded as UTF-8. Sampling a stalled body stops after
100 ms; it never authorizes a write retry. These diagnostics are for the synthetic
counter fixture and can include internal peer addresses. Eventual recovery with
no counter mismatches does not establish uninterrupted request availability.

## Linux runtime image and CI

`fleet.Dockerfile` builds both applications and the load generator in one release
image. It provisions the pinned CPython under `/opt/cellar-python`, registers its
shared-library directory, imports SSL and SQLite, and checks the host's dynamic
linkage. Build one image per ABI with a private tag and target-cache ID:

```sh
docker build -f tools/python/fleet.Dockerfile \
  --build-arg PYTHON_RUNTIME=3.14.5t \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  --build-arg TARGET_CACHE_ID=cellar-my-checkout-python-free \
  -t cellar-python-fleet:my-checkout-free .
```

The optional `python-fleet` mode in the Native SDK workflow builds both ABIs
on Linux. It probes the interpreter inside each image, exports the exact runtime
and binaries, verifies their hashes, then runs native and Python functional S3
recovery on that runner. This exercises image provisioning and Linux host
behavior; it is not a container-isolated fleet performance measurement.

```sh
gh workflow run native-sdk.yml --ref YOUR_BRANCH \
  -f mode=python-fleet -f replay_sha="$(git rev-parse HEAD)"
```

The workflow ref must contain this mode and the reusable `python-fleet.yml`.
The optional revision must be a full commit SHA. Artifact uploads retain build
logs, image metadata, executable hashes, measurements and process reports,
including failed attempts. A new diagnostic run does not cancel earlier runs.

## Controlled performance comparison

Use the full harness on an existing Docker/Colima VM with at least eight CPUs.
It assigns one CPU and 2 GiB to each of three nodes, two CPUs each to RustFS and
the load generator, and private 5 GiB ext4 volumes. Do not interpret the small
functional fixture as a substitute for this resource envelope.

Run these sequentially against the same image and machine, with fresh output
directories. Repeat with reversed order to reveal drift:

```sh
python3 tools/perf/run.py --image cellar-python-fleet:my-checkout-free \
  --runtime native --output /absolute/new/native-run \
  --cells 128 --max-resident 128 --max-cached-routes 32 --max-requests 512 \
  --warmup-seconds 30 --seconds 20 --repeats 2 --concurrency 1 8 32 128 \
  --shutdown-ms 60000 --scenario three_node_bucket three_node_fleet
python3 tools/perf/run.py --image cellar-python-fleet:my-checkout-free \
  --runtime python --output /absolute/new/python-run \
  --cells 128 --max-resident 128 --max-cached-routes 32 --max-requests 512 \
  --warmup-seconds 30 --seconds 20 --repeats 2 --concurrency 1 8 32 128 \
  --shutdown-ms 60000 --scenario three_node_bucket three_node_fleet
python3 tools/perf/analyze.py /absolute/new/native-run /absolute/native-summary
python3 tools/perf/analyze.py /absolute/new/python-run /absolute/python-summary
```

Use a second workload with more cells than resident capacity to measure churn.
Keep ABI, source revision, profile, payload size, durability, offered concurrency,
resource limits and backend identical within each comparison. Python admission
is bounded to twice the host request budget to accommodate root and nested cell
calls; startup records the exact task, object and bridge limits. Analysis checks
every saved Python shutdown against its raw process log and rejects missing or
inconsistent checkpoints. Report failures and incomplete recovery alongside
throughput and latency; these are not successful benchmark windows.
