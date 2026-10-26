<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cellar bindings in Rust, Python, JavaScript and TypeScript

Run the same counter application in four languages to see how configuration,
binding names, cell classes and business identities fit together. Each response
includes the executing cell's identity and persisted count. No external service
or credential is required.

## What the example demonstrates

| Binding | Class | Meaning of the name `demo` |
| --- | --- | --- |
| `COUNTERS` | `Counter` | One durable counter |
| `COUNTER_ALIAS` | `Counter` | The same counter through another binding name |
| `OTHER_COUNTERS` | `OtherCounter` | A separate counter with independent storage |

Each project contains these declarations under `primary`:

```json
{
  "script_name": "bindings-example",
  "application_version": "v1",
  "cells": ["Counter", "OtherCounter"],
  "bindings": [
    { "type": "durable_object_namespace", "name": "COUNTERS", "class_name": "Counter" },
    { "type": "durable_object_namespace", "name": "COUNTER_ALIAS", "class_name": "Counter" },
    { "type": "durable_object_namespace", "name": "OTHER_COUNTERS", "class_name": "OtherCounter" }
  ]
}
```

The binding name is an application-facing alias. The class selects the cell
implementation and namespace. The entity name selects one cell in that namespace.
For ordinary application cells, identity is derived from script name, class name
and entity name. Binding aliases and application versions are not identity inputs.
Changing a script, class or entity name can therefore select different state.

```text
COUNTERS      ─┐                  ┌─ demo → SQLite count
               ├→ Counter namespace
COUNTER_ALIAS ─┘                  └─ beta → separate SQLite count

OTHER_COUNTERS → OtherCounter namespace → demo → separate SQLite count
```

`Counter` and `OtherCounter` reuse the same behavior. Registering that behavior
under two class names gives it independent namespaces. The counter lives in cell
storage; recreating a language object does not reset it.

## Start a server

Run commands from the repository root. Use the Rust toolchain required by the
repository and a checkout-specific `CARGO_TARGET_DIR` on the workspace volume, as
described in [AGENTS.md](../../AGENTS.md). Python 3 is sufficient for the clients.
Node variants require Node 24+; embedded Python requires CPython 3.14.

Choose one backend at a time. `dev` uses port 9876, `--no-watch` builds once,
and Ctrl-C drains the host. Add `--port 9877` to use a different port.

### Rust

```sh
cargo run --locked -p cellar --bin cellar -- \
  dev examples/bindings/project-rust.json --no-watch
```

[main.rs](main.rs) registers both class factories and calls the native SDK:

```rust
let namespace = context.namespace(binding)?;
let cell = namespace.get_by_name(name);
let response = cell.fetch(&context, request).await?;
```

### JavaScript on Node

```sh
npm ci --prefix examples/bindings --ignore-scripts
CELLAR_NODE_ENTRY="$PWD/examples/bindings/app.js" \
CELLAR_NODE_EXECUTABLE="$(command -v node)" \
  cargo run --locked -p cellar --bin cellar -- \
  dev examples/bindings/project-node.json --no-watch
```

[app.js](app.js) uses the same SDK as the TypeScript version. The [Node host](node.rs)
explicitly registers `Fetch` events for both cell classes.

### TypeScript on Node or embedded V8

```sh
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/bindings --ignore-scripts
npm run --prefix examples/bindings build

# Choose Node:
CELLAR_NODE_ENTRY="$PWD/examples/bindings/dist/app.js" \
CELLAR_NODE_EXECUTABLE="$(command -v node)" \
  cargo run --locked -p cellar --bin cellar -- \
  dev examples/bindings/project-node.json --no-watch

# Or stop Node and choose embedded V8:
cargo run --locked -p cellar --bin cellar -- \
  dev examples/bindings/project-javascript.json --no-watch
```

The build typechecks [app.ts](app.ts), emits `dist/app.js` for Node, and creates
`app.bundle.js` for the [V8 host](javascript.rs). Rebuild before running V8 after
source changes: Cargo embeds the prepared bundle. `javascript: true` in that
project permits the optional engine dependency; native Rust remains engine-free.

The JS/TS handler selects a cell using `ctx.cell(binding, name)`, then calls
`cell.fetch(request(...))`. It forwards the method, headers and body and converts
the decoded nested response to an SDK handler response.

### Python

```sh
export PYO3_PYTHON="$(command -v python3.14)"
cargo run --locked -p cellar --bin cellar -- \
  dev examples/bindings/project-python.json --no-watch
```

Use a matching interpreter, shared library and standard library; see the
[Python installation guide](../python/README.md#install-and-run). No pip packages
are required by [app.py](app.py). The [Python host](python.rs) embeds the source,
registers both classes and checks the shutdown report.

```python
namespace = ctx.namespace(binding)
cell = namespace.get_by_name(name)
return await cell.fetch(request)
```

## Try aliases and isolated state

In another terminal, run these commands against any backend:

```sh
# Fresh state: count 0, class Counter, name demo.
curl -sS http://127.0.0.1:9876/bindings/COUNTERS/demo

# Increment to 1 through the first binding.
curl -sS -X POST http://127.0.0.1:9876/bindings/COUNTERS/demo

# Same scope and count 1 through the alias.
curl -sS http://127.0.0.1:9876/bindings/COUNTER_ALIAS/demo

# Increment the same cell to 2 through the alias.
curl -sS -X POST http://127.0.0.1:9876/bindings/COUNTER_ALIAS/demo
curl -sS http://127.0.0.1:9876/bindings/COUNTERS/demo

# Different name: count 0 and a different scope.
curl -sS http://127.0.0.1:9876/bindings/COUNTERS/beta

# Same name but different class: count 0 and a different scope.
curl -sS http://127.0.0.1:9876/bindings/OTHER_COUNTERS/demo
```

A response has this shape; `<derived-id>` represents the actual 64-digit hex ID:

```json
{"class":"Counter","name":"demo","scope":"Counter:<derived-id>","count":2}
```

Counts above assume fresh project state. Repeating a POST adds another increment.
`GET` reads without incrementing. Names accept 1–64 ASCII letters, digits,
underscores or hyphens. Unknown binding routes return 404, invalid names return
400, and unsupported methods return 405. These route checks belong to the example;
the host also resolves and validates the actual configured binding.

Stop and restart the same project without `--clean`, then read the same URLs:
counts and scopes survive. Each project path has private local `.cellar` state;
the Node JS/TS variants use the same project. Separate backend projects do not
share a local database even though their application identity inputs match.

## Change a binding target

Stop the server and edit **only** `COUNTER_ALIAS` in the project you are running:

```json
{ "type": "durable_object_namespace", "name": "COUNTER_ALIAS", "class_name": "OtherCounter" }
```

Restart that same project. Reading `/bindings/COUNTER_ALIAS/demo` now returns
the same class, scope and count as `/bindings/OTHER_COUNTERS/demo`.
`/bindings/COUNTERS/demo` retains its original state. The application still calls
`COUNTER_ALIAS`; configuration changes which dependency it reaches. Rebinding
does not copy or migrate the original cell's data. Restore `class_name: Counter`
before running the standard client below.

## Verify automatically

The standard-library [client](check.py) can check any running backend:

```sh
python3 examples/bindings/check.py http://127.0.0.1:9876
```

It uses a new name for each run, exercises both aliases, checks name/class
isolation and invalid routes, and sends concurrent increments. An acknowledged
increment must appear exactly once; a refused compare-and-write returns 409.
The client prints `PASS` and leaves its small tutorial counters in that project.
Use `--name demo` to inspect an existing named counter instead.

After preparing the chosen language dependencies, run [smoke.py](smoke.py) to
launch servers in temporary private project directories:

```sh
cargo build --locked -p cellar --bin cellar
python3 examples/bindings/smoke.py \
  --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --backend rust --backend node-js --backend node-ts --backend v8 --backend python
```

Select only installed backends, for example `--backend rust`. The smoke runner
checks fresh state, HTTP calls, concurrent increments, identity and count after
restart, configuration-only rebinding, and graceful shutdown on all three boots.
It deletes its temporary state. JavaScript CI runs Rust, Node JS/TS and V8;
Python CI runs CPython with both configured ABIs.

## Implementation map and limits

1. The [deployment loader](../../crates/cellar/deployment/fleet.rs) stores the
   declared binding name and class in the worker configuration.
2. [Context::namespace](../../crates/cellar/application/cells.rs) checks the current
   invocation, resolves the alias and verifies the exported class. `get_by_name`
   returns a callable stub for a stable identity.
3. [Namespace keys](../../crates/cellar/runtime/contracts.rs) and
   [cell IDs](../../crates/cellar-native/cell_id.rs) derive ordinary cell identities
   from the script, class and business name.
4. The [native dispatcher](../../crates/cellar/runtime/cells/dispatch.rs) resolves
   ownership and dispatches locally or to the owning peer. The host constructs the
   registered cell with its private SQLite storage.
5. The [JS runner](../../crates/cellar-javascript/runtime/runner.js) and
   [Python facade](../../crates/cellar-python/python/cellar.py) submit `cell.fetch`
   to the [shared Rust adapter](../../crates/cellar-guest/operations.rs), which
   uses the same namespace and routing APIs as native Rust.

Rust increments inside a synchronous storage transaction. Python and JS/TS use
a checked storage batch because another invocation can run between awaited
operations. A conflicting batch returns 409 without applying the increment;
successful guest writes await `storage.sync()`. Native output gates enforce the
configured durability requirements. Counts are limited to the nonnegative safe
integer range shared by all four languages.

These POSTs are deliberately non-idempotent. A timeout or other unknown outcome
does not prove an increment failed; the example never automatically retries it.
Business commands that need safe retries should persist command IDs and receipts.
See [cell modeling](../../docs/04-cell-modeling.md) and
[routing contracts](../../docs/07-routing-rpc.md) for those patterns.

This local teaching API has no user authentication. Its binding allowlist and
cell names do not authorize users. In a tenant application, authenticate first,
derive a canonical tenant/entity name and enforce access policy. Bindings do not
create transactions across separate cells. Read-only identity fields in this
example are diagnostics, not credentials.

To reset tutorial state, stop the server and add `--clean` to its next `dev`
command. This deletes only that project's local state. Generated bundles,
`dist/`, `node_modules/` and `.cellar/` are ignored by Git.
