# Python in Cellar

This is a runnable embedded CPython example: Python handles HTTP requests and
cell logic, Pydantic validates input, NumPy performs native numerical work, and Cellar owns SQLite, alarms, routing, KV,
SQL, queues and cron. CPython runs in the native Rust process through PyO3.

## Install and run

Use **GIL-enabled Python 3.14**, with development headers and a shared libpython,
and the repository's Rust toolchain. Build and run against the same Python
installation. Linux installations built from source need `--enable-shared` and a
loader path for libpython. No Docker setup is required for local development.

From the repository root:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-my-checkout"
export PYO3_PYTHON="$(command -v python3.14)"
"$PYO3_PYTHON" -m venv examples/python/.venv
examples/python/.venv/bin/python -m pip install --require-hashes --only-binary :all: -r examples/python/requirements.lock
export CELLAR_PYTHON_SITE_PACKAGES="$(examples/python/.venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
cargo run -p cellar --bin cellar --locked -- dev examples/python/project.json --no-watch
```

Use a target directory unique to your checkout. The example host exclusively initializes CPython before starting the Rust host.
The selected interpreter's home/stdlib roots are captured at build time; ambient
`PYTHONHOME`, `PYTHONPATH`, user site and startup hooks are ignored. It explicitly adds
`CELLAR_PYTHON_SITE_PACKAGES` to Python's import path, checks Pydantic's pinned
and NumPy versions at startup, and embeds [app.py](app.py) in its executable. The requirements
lock pins the example's transitive packages and accepted hashes. Rebuild after editing Python source;
this example requires a process restart rather than live interpreter reload.

Using the address printed by `cellar dev`:

```sh
curl http://127.0.0.1:9876/libraries
curl http://127.0.0.1:9876/counters/demo
curl -X POST http://127.0.0.1:9876/counters/demo \
  -H 'content-type: application/json' -H 'idempotency-key: demo-increment-1' -d '{"amount":7}'
curl -X POST http://127.0.0.1:9876/counters/demo/alarm
curl -X POST http://127.0.0.1:9876/kv \
  -H 'content-type: application/json' -d '{"message":"Python KV"}'
curl -X POST http://127.0.0.1:9876/sql -d 'Python SQL'
curl -X POST http://127.0.0.1:9876/queue \
  -H 'content-type: application/json' -d '{"job":"example"}'
curl http://127.0.0.1:9876/jobs
curl http://127.0.0.1:9876/cron
```

The named counter survives preserving restarts. Its alarm increments `alarms` once per persisted occurrence receipt.
`/jobs` shows queue effects in KV; `/cron` shows the latest real minute occurrence.
Queue and cron deliveries can repeat; production business effects need
idempotency keys. Counter POST requests accept an `idempotency-key` header (1..128 ASCII letters, digits, `-`, `_`, `.`). The rendered response status, body bytes and ordered application headers commit atomically with the receipt and increment; matching retries return those retained fields, including after restart or later writes. Transport-generated metadata such as Date is outside this application receipt. Reusing a key for a different amount returns 409. Retain receipt keys for the application's complete retry/reconciliation window.

## Validate and package

Keep the environment above and run:

```sh
cargo test -p cellar-python --features embed --locked
cargo build -p cellar --bin cellar --locked
python3 examples/python/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" --with-cron
cargo run -p cellar --bin cellar --locked -- \
  build examples/python/project.json --output /tmp/cellar-python-package
```

The smoke test uses fresh private state, exercises Pydantic validation, native NumPy matrix work, pure Python inspection, bindings,
queue delivery, alarms and cron, restarts the host and verifies retained state.
The Rust bridge test also checks atomic conflict rollback, missing versus null,
binary HTTP, native-library imports, handler failures, body limits, cancellation,
context expiration and clean shutdown.

Native packaging can include the matching runtime and locked packages through a
`python_environment` declaration. The [environment workflow](../../tools/python/README.md)
builds the inventory/archive, prepares a native project, publishes verified blobs
before the selector and restores them with `cellar install --python-root PREFIX`.
The installed host uses its compiled import roots and checks all file contents
before startup. Python source and the SDK facade stay embedded in the executable.
System-library/OS prerequisites and the remaining production fault/performance
matrix are tracked in the [completion audit](../../plans/007-embedded-python-audit.md).
See the [adapter contract and limitations](../../crates/cellar-python/README.md)
before using it for production workloads.

The example's Python-dispatched `/health` handler supports the shipped Unix
[`cellar supervise` watchdog](../../tools/python/README.md#process-supervision).
It echoes the per-process instance header only when the supervisor explicitly
provides it. The deployment recipe includes finite restart policy, a systemd
service template and a real installed-host stalled-interpreter recovery drill.

Set `CELLAR_SHUTDOWN_REPORT_JSON=1` to emit the combined native/Python shutdown report. The smoke test checks that native drain phases complete, objects retire and bridge reservations/tasks/threads return to zero. `PythonApplication::bridge_report()` supplies live native bridge copy/resource counters and operation latency histograms to a Rust-owned metrics exporter. `interpreter_snapshot()` returns sampled CPU, task/object and memory-block observations without waiting for the GIL; check its sample age. Optional allocation profiling and whole-process allocator samples have explicit exclusions in the adapter guide. These observations do not provide per-cell heap limits or certify private library thread pools.
