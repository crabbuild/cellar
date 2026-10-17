# Business identity routing

Route by business identity through the authoritative cell owner.

## How it works

The stateless handler resolves COUNTERS.get_by_name and dispatches through the owning
node. Equal names address equal cells; different names are independent.

Read the [Rust source](../counter/main.rs) and [project configuration](project.json). The
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
  dev examples/router/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST http://127.0.0.1:9876/router/alpha
curl -sS -w '\n' http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' http://127.0.0.1:9876/router/beta
```

## Expected output and interpretation

The responses are `{"count":1}`, `{"count":1}` and `{"count":0}`. `/router/alpha` and
`/counters/alpha` select the same class/name in this application. Beta selects another
cell. A local run exercises the real routing API with one owner; the fleet recipe adds
peer routing and owner recovery.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/router/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
