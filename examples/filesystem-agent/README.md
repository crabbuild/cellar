<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable agent with a per-session filesystem

One `AgentSession` cell owns its filesystem, indexed turn records, saved model
decisions and atomic tool receipts. Follow-up turns in that session see the
same files. Different sessions have independent roots. The example uses explicit
`ctx.fs` operations and needs no mount, container or command runner.

The included model is a deterministic fixture requiring no credentials. A normal
prompt writes `/notes.txt`, reads it and completes. `read:/notes.txt` reads a
file in a later turn. Replace `model.js` to connect a provider using the original
invocation's SDK I/O; the fixture does not claim to call an LLM.

## Build and run

From the repository root with Node 24+ and the repository Rust toolchain:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
npm ci --prefix examples/filesystem-agent --ignore-scripts
cargo build --locked -p cellar-javascript --no-default-features --features node --bin cellar-filesystem-agent-example
mkdir -p .cellar
export CELLAR_NODE_ENTRY="$PWD/examples/filesystem-agent/app.js"
export CELLAR_NODE_EXECUTABLE="$(command -v node)"
export CELLAR_NODE=filesystem-demo
export CELLAR_BUCKET=cellar-dev
export CELLAR_INTERNAL_DEV_STORE="$PWD/.cellar/filesystem-demo-bucket.sqlite"
export CELLAR_TEST_DATA_DIR="$PWD/.cellar/filesystem-demo-node"
export CELLAR_DURABILITY=bucket
export CELLAR_HANDLER_BUDGET_S=60
"$CARGO_TARGET_DIR/debug/cellar-filesystem-agent-example" --listen 127.0.0.1:8080 --internal-listen 127.0.0.1:8081
```

Choose fresh writable locations for a fresh demo; keep the development bucket
when replacing node storage. In another terminal:

```sh
curl -i http://127.0.0.1:8080/sessions/alice/turns/one -d '{"prompt":"rows=42"}'
curl http://127.0.0.1:8080/sessions/alice/turns/one
# After turn one completes:
curl -i http://127.0.0.1:8080/sessions/alice/turns/two -d '{"prompt":"read:/notes.txt"}'
curl http://127.0.0.1:8080/sessions/alice/turns/two
```

Acceptance returns 202 after intent and alarm proof. Poll until `completed`;
turn two then returns `text: "rows=42"`. An early second turn returns 409 while
another turn owns the workspace. Repeating an ID with the same prompt returns
its saved state; different input returns 409. IDs contain 1–64 ASCII letters,
digits, underscores or hyphens; prompts contain 1–8192 UTF-8 bytes.

Stop gracefully through the internal listener:

```sh
curl -X POST http://127.0.0.1:8081/shutdown
```

Remove only the demo directories you selected when discarding its state.
The ingress is a local example; applications supply tenant authentication.

## Decisions, tools and recovery

`session.js` advances one persisted phase per alarm. It saves and proves a
complete model decision before invoking its tool. The stable tool operation ID
combines step and model tool ID within the turn. `@cellar/sdk/filesystem-tools`
commits file mutations, their result receipt and the session checkpoint in one
checked `storage.apply` batch, then awaits proof. Recovery repeats the same
operation ID and request fingerprint, returning the receipt.

Read tools journal the actual bounded result with an inode/version check.
Replaying a read after another edit returns its original bytes. Large artifacts
stay in the filesystem; the example never stores entire large files in every
model observation. Tool operations include read, stat, list, write, replace,
version-checked edit, rename, unlink and bounded literal search. The
helper exposes typed requests, not arbitrary Python/Node file calls or shell.

Only one mutating turn is active per session. Defaults cap a session at 32 turns,
16 steps per turn and five attempts per step. Model inputs are at most 256 KiB,
decisions 64 KiB; tools have separate byte/entry/search/receipt caps. These
application limits remain subject to native filesystem and bridge limits.
The configuration identity includes model, tool contract and policy; a changed
configuration refuses to advance an unfinished run. Recover it with the same
release.

Retry exhaustion leaves the turn `interrupted` and retains workspace ownership.
POST the same prompt with `"resume":true` after resolving the failure, or with
`"abandon":true` to release an idle interrupted turn. Abandon does not undo files
or receipts and reports `outcome: "unknown"`. An in-flight step must finish or
be interrupted before it can be abandoned. Provider work before a saved decision
can repeat. Unknown mutation outcomes are reconciled through receipts; the
runner never infers rollback from an exception.

## Qualification

The adapter integration test runs actual Node and native hosts, verifies file
operations and batches, then interrupts a session after its first effect. After
host replacement it resumes the saved turn and runs a follow-up read without
repeating the write. A separate process test uses SIGKILL and empty node storage:

```sh
CELLAR_FS_HOST="$CARGO_TARGET_DIR/debug/cellar-filesystem-agent-example" \
  npm run --prefix examples/filesystem-agent test:e2e
```

The private `test/crash-fixture.mjs` entry controls interruptions at acceptance,
provider dispatch, saved decision, atomic effect/receipt and completion. Each
replacement uses a new node identity and an empty local directory against the
preserved development bucket. Pending turns must resume by alarm without a
session HTTP request. The test checks content, receipts, file version, saved
model-call counts, duplicate/conflicting IDs, isolation, follow-up files and
graceful shutdown. Production `app.js` contains no fault hooks.

Two additional cases suppress a file-effect reply after the real native batch
and durability proof, then refuse the observation checkpoint until five attempts
leave the turn interrupted. After SIGKILL and removal of the old node data,
explicit resume reconciles the saved decision and receipt without rewriting the
file. Explicit abandon preserves its unknown outcome, file and receipt, and
allows a new turn to read that workspace. Both controls preserve the file version;
duplicate input alone does not resume a turn. The injected reply loss is at the
guest caller boundary, not a network-partition or storage-failure drill.
The provider interruption case also verifies that abandon returns 409 while
an asynchronous workspace step is still running.

Two more cases supply an unknown result before dispatching the file batch.
Their interrupted checkpoint remains at the saved-decision phase, with no file,
allocation or receipt. Resume submits exactly one native file-effect batch;
abandon submits none and a new turn records the missing-file refusal. The test
counts actual native batch completions as well as checking file versions.

`CELLAR_FS_TEST_CASES=uncertain-before-resume,uncertain-before-abandon` selects
the before-dispatch pair; the default `test:e2e` command runs all nine cases.

This drill uses the development bucket, not a multi-node production fleet or
an external object store. Capacity, failure and feature qualification remain
tracked in the [implementation ledger](../../docs/design/per-cell-filesystem-implementation-status.md).

For inventory-verified delivery using the existing Node packager:

```sh
node tools/javascript/node-package.mjs --runtime examples/filesystem-agent/runtime.json .cellar/filesystem-agent-runtime
cargo run --locked -p cellar -- build examples/filesystem-agent/project.json --output .cellar/filesystem-agent-package
```

Use a fresh runtime prefix at the same absolute path on build and serving
machines. See the [Node deployment guide](../node/README.md) for installation.
To qualify archive restoration with the production entry, first move the
captured runtime to a fresh backup name, then run the installed-package test:

```sh
mv .cellar/filesystem-agent-runtime .cellar/filesystem-agent-runtime-captured
CELLAR_FS_PACKAGE="$PWD/.cellar/filesystem-agent-package" \
  cargo test --locked -p cellar-javascript --no-default-features --features node \
    --test fleet_package filesystem_agent_package -- --ignored --nocapture
```

It publishes to a private development bucket, restores and verifies the runtime
and executable, runs four isolated session workspaces, kills the host, and checks
completed runs plus follow-up file reads on a new node with empty local storage.
