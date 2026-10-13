# Native computation

Compile bounded computation directly into the application executable.

## How it works

Rust computation compiles directly into the host executable. GET /compute approximates
pi using bounded numerical integration. samples is limited to 1..10000 to bound work per
turn. The upstream Wasm Rust counter is also covered by counter; Cellar does not execute
Wasm wrappers.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[native_examples.rs](../native_examples.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/wasm/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' 'http://127.0.0.1:9876/compute?samples=1000'
curl -sS -i 'http://127.0.0.1:9876/compute?samples=1000000'
```

## Expected output and interpretation

The valid response has `execution:"native Rust"`, `samples:1000` and `pi` approximately
`3.14159274`. Floating-point formatting may vary. The oversized sample count returns
HTTP 400. More samples within the allowed range improve numerical accuracy while keeping
work bounded. There is no separate Wasm artifact or runtime.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/wasm/.cellar/native-dev`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
