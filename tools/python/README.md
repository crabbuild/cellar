# Provisioned Python environments

`environment.py` inventories a provisioned GIL-enabled CPython 3.14 and explicit
package roots. `CELLAR_PYTHON_INVENTORY` binds that inventory into the Rust host at
build time. Startup compares the source, event registrations, distribution pins,
ABI, loaded libpython and filesystem contents before initializing Python or
starting a node. A changed, missing or additional file refuses admission.

This is a verified **provisioned environment**, not an automatic native-package
installer. The existing native package still transports only the host executable.
Runtime archive publication/recovery and full external native-library/OS closure
attestation remain open in the [completion audit](../../plans/007-embedded-python-audit.md).
Use the same immutable absolute paths on every node. Protect those paths against
writes for the process lifetime; startup verification is not a filesystem sandbox.

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
cargo build -p cellar-python --features embed --bin cellar-python-example --locked
python3 tools/python/environment.py verify "$CELLAR_PYTHON_INVENTORY"
```

Use the checkout's Rust toolchain and unique writable target directory from
[AGENTS.md](../../AGENTS.md). Keep bytecode writes disabled throughout building;
Python probes can otherwise mutate the tree after capture. The runtime disables
bytecode writes independently. Do not install or edit packages after capture.
When any source, registration, dependency or runtime changes, capture a new
inventory and rebuild the host. `is_verified_build()` identifies hosts built
with this binding; ordinary developer builds without it remain unverified.

The hash lock permits only the listed wheel/source hashes; the commands above
also prohibit source builds. The inventory then binds the exact installed file
contents for this target. It includes package metadata and bundled native
libraries, and validates the actual loaded libpython rather than assuming that
a matching filename means the correct library was loaded. System shared
libraries outside these roots remain the responsibility of the deployment's OS
image. Arbitrary native packages may require additional libraries or system tools.

## Qualification

On a disposable private environment, the mutation drill preserves and restores
files while proving startup rejection. Never share its roots with a running host.
The scratch prefix guard refuses mutation of roots outside the supplied prefix.

```sh
python3 tools/python/qualify_inventory.py \
  --host "$CARGO_TARGET_DIR/debug/cellar-python-example" \
  --inventory "$CELLAR_PYTHON_INVENTORY" --scratch-prefix "$CELLAR_PYTHON_PREFIX"
python3 examples/python/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" --with-cron
```

The bridge harness separately runs disposable CPU-loop and GIL-held native-call
hosts. It kills each owner, deletes all owner-local databases, recovers from the
fleet bucket and verifies a new write includes the previously acknowledged
state. No clean-drain report may be produced for a hard-killed host. These are
local development-fleet drills; quorum replication faults remain a separate
qualification gate. Production must use a process supervisor with a finite
startup deadline, a Python-served health check, bounded graceful-stop time, hard
termination and restart. Rust cannot safely interrupt arbitrary Python C code.

Current local evidence: macOS ARM64, CPython 3.14.5 (independent managed runtime)
and 3.14.6 (Homebrew), Pydantic 2.12.3 / pydantic-core 2.41.4, NumPy 2.3.4,
typing-inspection 0.4.2 and the transitive pins in the example lock. The example
executes matrix multiplication and hashing concurrently through the managed
executor and a pure Python literal-type inspection. Linux CI exercises the
same pinned libraries; its results must be checked before claiming Linux
qualification. No claim covers every package in the Python ecosystem.
