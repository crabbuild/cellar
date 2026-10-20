# Provisioned Python environments

`environment.py` inventories a provisioned GIL-enabled CPython 3.14 and explicit
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
