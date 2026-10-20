# Provisioned Python environments

For reproducible native/Python latency, throughput, memory, bridge-copy and
owner-loss recovery measurements, use the [baseline harness](baseline.md).

`environment.py` inventories a provisioned CPython 3.14 or free-threaded CPython 3.14t and explicit
package roots. `CELLAR_PYTHON_INVENTORY` binds that inventory into the Rust host at
build time. Startup compares the source, event registrations, distribution pins,
ABI, loaded libpython and filesystem contents before initializing Python or
starting a node. A changed, missing or additional file refuses admission.

A native project's `python_environment` field pairs an inventory and environment
archive with the executable. `cellar build` binds the inventory into that binary,
stages its archive, and emits `embedded-python-v1` metadata with both digests.
`cellar deploy` verifies and uploads the runtime blob before changing the selector.
`cellar install` verifies downloaded contents and restores the closure under its
explicit prefix before publishing the new executable. Existing matching prefixes
are reused after verification; different contents and even competing empty
prefixes are never overwritten. Applications still need a compatible OS; external
system-library attestation remains in the [completion audit](../../plans/007-embedded-python-audit.md).
Use immutable absolute paths and protect them against writes during process life.
Startup verification is not a filesystem sandbox.

## Prepare, capture and build

The following uses an independent runtime, cached platform wheels, then an
offline installation. `uv` is a build-time tool; it is never run by the host.
Select the managed runtime executable printed by `uv python find`.

```sh
export CELLAR_PYTHON_PREFIX="/absolute/private/path/cellar-python-env"
uv python install 3.14.5 --install-dir "$CELLAR_PYTHON_PREFIX/runtime" --no-bin
export PYO3_PYTHON="$(UV_PYTHON_INSTALL_DIR="$CELLAR_PYTHON_PREFIX/runtime" uv python find --managed-python 3.14.5)"
export PYTHONDONTWRITEBYTECODE=1
"$PYO3_PYTHON" -I -B -m pip download --require-hashes --only-binary :all: \
  -r examples/python/requirements.lock --dest "$CELLAR_PYTHON_PREFIX/wheels"
"$PYO3_PYTHON" -I -B -m pip install --no-index \
  --find-links "$CELLAR_PYTHON_PREFIX/wheels" --require-hashes --only-binary :all: \
  --target "$CELLAR_PYTHON_PREFIX/site" -r examples/python/requirements.lock
export CELLAR_PYTHON_SITE_PACKAGES="$CELLAR_PYTHON_PREFIX/site"
export CELLAR_PYTHON_INVENTORY="$CELLAR_PYTHON_PREFIX/inventory.json"
export CELLAR_PYTHON_TARGET="$(rustc -vV | sed -n 's/^host: //p')"
python3 tools/python/environment.py capture --python "$PYO3_PYTHON" \
  --site-packages "$CELLAR_PYTHON_SITE_PACKAGES" --source examples/python/app.py \
  --filename app.py --descriptor examples/python/descriptor.json \
  --target "$CELLAR_PYTHON_TARGET" --output "$CELLAR_PYTHON_INVENTORY"
python3 tools/python/environment.py bundle "$CELLAR_PYTHON_INVENTORY" \
  --prefix "$CELLAR_PYTHON_PREFIX" --output "$CELLAR_PYTHON_PREFIX/environment.tar.gz"
python3 tools/python/environment.py project examples/python/project.json \
  --inventory "$CELLAR_PYTHON_INVENTORY" --archive "$CELLAR_PYTHON_PREFIX/environment.tar.gz" \
  --output "$CELLAR_PYTHON_PREFIX/project.json"
cargo build -p cellar --bin cellar --example python-package-qualification --locked
"$CARGO_TARGET_DIR/debug/cellar" build "$CELLAR_PYTHON_PREFIX/project.json" \
  --output /absolute/private/path/python-package
python3 tools/python/environment.py verify "$CELLAR_PYTHON_INVENTORY"
```

Use the checkout's Rust toolchain and unique writable target directory from
[AGENTS.md](../../AGENTS.md). Keep bytecode writes disabled throughout building;
Python probes can otherwise mutate the tree after capture. The runtime disables
bytecode writes independently. Do not install or edit packages after capture.
When any source, registration, dependency or runtime changes, capture a new
inventory and rebuild the host. The native project selects its inventoried interpreter for a host build and adds libpython's loader paths to the executable; unrelated native projects ignore ambient inventory flags. Compiled package roots become `Options::module_paths` defaults. `Host::runtime_inventory` / `CompiledApplications::runtime_inventory` bind the same digest to generation admission, including after startup. `is_verified_build()` identifies hosts built
with this binding; ordinary developer builds without it remain unverified.

The hash lock permits only the listed wheel/source hashes; the commands above
also prohibit source builds. The inventory then binds the exact installed file
contents for this target. It includes package metadata and bundled native
libraries, and validates the actual loaded libpython rather than assuming that
a matching filename means the correct library was loaded. System shared
libraries outside these roots remain the responsibility of the deployment's OS
image. Arbitrary native packages may require additional libraries or system tools.

## Publish and install

Deploy the staged package using the ordinary bucket options. On a matching target,
restore to the canonical prefix captured during building. The parent directory
must exist; choose a new executable output path for each installation.

```sh
cellar deploy /absolute/private/path/python-package --bucket YOUR_BUCKET
cellar install /absolute/private/path/python-package --bucket YOUR_BUCKET \
  --output /absolute/path/application-v1 --python-root "$CELLAR_PYTHON_PREFIX"
```

Environment archive format 1 contains bounded metadata and content-addressed
regular blobs. The reader never calls a tar filesystem unpacker: it rejects
missing/duplicate/unlisted blobs, invalid types, escaping paths and symlinks,
size/digest mismatches and incompatible targets. Installation uses a private
staging tree and atomic publication that refuses replacement. Limits are 32 MiB
inventory metadata, 200,000 files, 32 roots, 512 MiB compressed archive and 2 GiB
expanded file contents. The native executable digest pairs each target-specific
runtime reference with its exact host. All feature/format markers remain 1.


## Qualification

On a disposable private environment, the mutation drill preserves and restores
files while proving startup rejection. Never share its roots with a running host.
The scratch prefix guard refuses mutation of roots outside the supplied prefix.

```sh
python3 tools/python/qualify_inventory.py \
  --host "$CARGO_TARGET_DIR/debug/cellar-python-example" \
  --inventory "$CELLAR_PYTHON_INVENTORY" --scratch-prefix "$CELLAR_PYTHON_PREFIX"
python3 examples/python/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --project "$CELLAR_PYTHON_PREFIX/project.json" --with-cron
cargo run -p cellar --example python-package-qualification --locked -- \
  /absolute/private/path/python-package /absolute/private/path
```

To qualify a Python source upgrade, generate a private application fixture with
changed cell handler code, build its distinct native package, and supply that
package as the third qualification argument:

```sh
python3 tools/python/rolling_project.py "$CELLAR_PYTHON_PREFIX/project.json" \
  --output /absolute/private/path/rolling-project
"$CARGO_TARGET_DIR/debug/cellar" build /absolute/private/path/rolling-project/project.json \
  --output /absolute/private/path/python-package-v2
cargo run -p cellar --example python-package-qualification --locked -- \
  /absolute/private/path/python-package /absolute/private/path \
  /absolute/private/path/python-package-v2 \
  --supervisor "$CARGO_TARGET_DIR/debug/cellar"
```

The generator creates a new directory, copies the example entry point, changes
the Python cell handler, recaptures/bundles its source inventory and derives a
private Cargo lock from the repository lock offline. It refuses added dependency
versions. Repository sources and provisioned runtime files are unchanged.
The qualification checks different executable, application-version and inventory
identities; observes the old process refusing in-process adoption; verifies that
the old executable refuses the new package; and exercises real TERM and INT
handoff in separate development fleets. A GET-only header from the changed Python cell
handler proves replacement code ran after takeover without changing POST receipt
headers. Acknowledged state and the exact retained application status, body bytes
and headers must survive; the test compares those fields directly. Both combined shutdown
reports must be complete. This covers source changes with the same pinned
CPython/package environment; upgrading OS/native dependencies needs its own
qualification.

The package qualification tool requires a private scratch prefix and no running
host sharing it. It publishes to a private bucket, moves the original runtime
aside, restores the closure, launches the installed host with a cleared process
environment, runs native numerical work and receipt replay, checks a complete
preserving shutdown, then restores the original tree. Failed cleanup preserves
the original tree for recovery. This is a destructive drill only against its
explicitly designated scratch data, never a production provisioning prefix.

The bridge harness separately runs disposable CPU-loop and GIL-held native-call
hosts. It kills each owner, deletes all owner-local databases, recovers from the
fleet bucket and verifies a new write includes the previously acknowledged
state. No clean-drain report may be produced for a hard-killed host. These are
local development-fleet drills. The bridge harness also pauses both real
follower append traffic and bucket data writes in a three-node fleet: sync and
final output wait for proof, actual follower acknowledgements release success
while bucket writes remain held, and failure of both paths refuses success.
Independent KV/shared-SQL/producer fault matrices also cover cancellation,
unknown results without replay, explicit reconciliation and bounded overlap.
Their evidence scope and remaining qualification are in the completion audit.
The shipped Unix process supervisor provides finite
startup/probe/stop deadlines and bounded restarts independently of Python.
Rust cannot safely interrupt arbitrary Python C code inside the host process.

## Process supervision

After installing the verified executable/runtime, start it through the native CLI:

```sh
cellar supervise --json --health-url http://127.0.0.1:8080/health \
  --startup-ms 30000 --probe-ms 2000 --interval-ms 1000 --failures 3 \
  --shutdown-ms 35000 --restart-ms 1000 --max-restarts 3 -- \
  /opt/cellar/python/current/host --listen 0.0.0.0:8080 \
  --internal-listen YOUR_PRIVATE_PEER_ADDRESS
```

Configure the normal fleet bucket, node identity, advertise address and storage
credentials through the host's CLI/environment. `CELLAR_WATCH` selects persistent
local working storage. These budgets are an example deployment policy; choose
them for the service's import time, probe latency and native shutdown budget.

The supervisor supplies a new 128-bit `CELLAR_SUPERVISOR_INSTANCE` on each spawn.
The Python example's CLI explicitly adds only that value to its Python
`Environment`. Its Python-dispatched `/health` handler echoes it in
`x-cellar-supervisor-instance`. A 2xx response with a missing/different value does
not establish readiness. This correlates the probe with this process; it is not
an authentication credential. Applications embedding the adapter should implement
the same small handler using an explicitly configured Python environment. A
Rust-only health endpoint cannot establish that a blocked Python loop progresses.

Probes are sequential, use finite timeouts, and disable redirects and ambient
proxies. Failed startup, consecutive failures after readiness, or child exit
terminates/joins the child before restarting it. A healthy response resets the
consecutive-failure counter. TERM/INT to the supervisor requests TERM of the owned
child group; another signal or the stop deadline escalates to KILL. KILL reaping
has its own five-second bound; failure refuses restart. `--max-restarts` counts
all restarts during that supervisor invocation, including startup failures.
Program arguments after `--` are passed directly without a shell.

Lifecycle records stream on stdout and are flushed immediately; child stdout and
stderr go to stderr. Records include direct child PID, attempt, stop reason,
exit code/signal and whether it required forced termination. They never assert
`drain_complete`: only the child's combined native/Python report proves drain.
Kernel/host failures can prevent reaping, and private detached processes remain
application/outer-service responsibilities. Linux additionally uses the existing
parent-death guard to kill the direct child if its supervisor dies.

[cellar-python.service](cellar-python.service) provides a systemd deployment
template. Provision the `cellar` user/group, `/usr/local/bin/cellar`, the installed
host at `/opt/cellar/python/current/host`, its immutable runtime prefix, and
`/etc/cellar/python.env` with the fleet/listener configuration (port 8080 for the
health URL). Its state lives under `/var/lib/cellar-python`; runtime files remain
read-only. Set the native shutdown budget below the supervisor grace. The unit
uses `KillMode=mixed` so TERM reaches the supervisor first and systemd can kill
remaining cgroup processes if it exits or exceeds the outer stop deadline.
These are the [documented systemd kill semantics](https://www.freedesktop.org/software/systemd/man/latest/systemd.kill.html).
`Type=exec` confirms supervisor execution, rather than application readiness;
the CLI's instance-matched readiness record/probe supplies the latter, consistent
with [systemd's service types](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html).
The outer unit's restart/rate limits are separate from the CLI's per-invocation cap.

Passing `--supervisor CELLAR_EXECUTABLE` to the package qualification tool runs a
real installed-Python drill using the generated changed-source package. A private
qualification-only route blocks the acknowledged owner in a GIL-held C call,
requires a forced KILL and joined replacement, replays the exact durable receipt,
commits another write and verifies complete replacement shutdown with a real
handoff peer. The killed process must produce no clean-shutdown report. The CLI's
own process tests additionally cover wrong-instance probes, startup hangs,
ignored TERM, second-signal escalation, unexpected exit/restart accounting,
invalid policies and consecutive-failure reset. The inventory drill independently
removes source, the native extension, the package root and actual libpython,
then verifies refusal and restoration; run it only with exclusive private roots.
For a shorter watchdog feedback loop, add `--supervisor-only`; that mode skips
archive restoration and rolling qualification and does not establish those gates.

Current local evidence: macOS ARM64, CPython 3.14.5 (independent managed runtime)
and 3.14.6 (Homebrew), Pydantic 2.12.3 / pydantic-core 2.41.4, NumPy 2.3.4,
typing-inspection 0.4.2 and the transitive pins in the example lock. The example
executes matrix multiplication and hashing concurrently through the managed
executor and a pure Python literal-type inspection. Linux CI exercises the
same pinned libraries; its results must be checked before claiming Linux
qualification. No claim covers every package in the Python ecosystem.


## Free-threaded runtime selection

Use a fresh private prefix for the `314t` ABI. In the provisioning commands above,
select `uv python install 3.14.5t` and `uv python find --managed-python 3.14.5t`,
then use that interpreter for wheel download, offline installation, capture and
host compilation. The same lock contains the qualified NumPy/Pydantic native
wheel hashes for both ABIs. Never reuse a regular-CPython package directory or
runtime archive for a free-threaded executable.

The inventory SOABI, compiled layout, actual loaded libpython and source digest
remain checked. There is no runtime ABI conversion or implicit GIL switch.
Run the smoke with `--expect-free-threaded` to require `/health.free_threaded`
true and `/health.gil_enabled` false after native library work. The SDK workflow
runs the full test/package/restore/handoff/supervisor matrix independently for
`3.14` and `3.14t`; reported qualification must name completed runs and versions.
