# Loaded child facets

Give a root cell a durable child component under the same owner.

## How it works

A root Supervisor lazily loads a compiled child class as a named facet. The child has
its own counter storage and lifetime under the root activation. POST increments; GET
reads. The startup callback runs in root context and supplies only the loaded class.
Facets are not independent fleet owners.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[main.rs](../main.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar-main --bin cellar --locked -- \
  dev examples/facets/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST http://127.0.0.1:9876/facets/demo
curl -sS -w '\n' http://127.0.0.1:9876/facets/demo
curl -sS -w '\n' http://127.0.0.1:9876/facets/another
```

## Expected output and interpretation

The responses are `{"count":1}`, `{"count":1}` and `{"count":0}`. POST lazily loads and
increments the child facet; GET reads the same child's storage. Another Supervisor has
its own child state. Restart without `--clean` and GET `/facets/demo` to see 1 again.
Durable facet storage remains under its root's ownership.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/facets/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
