# Native web primitives

Use native URL/header primitives alongside normal Rust hashing code.

## How it works

Uses owned URL and Headers values and computes a SHA-256 digest in Rust.

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
  dev examples/webapi/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/webapi
```

## Expected output and interpretation

The response is HTTP 200 with `pathname:"/webapi"`, `header:"rust"` and
`sha256:"c9c8eb8fa9d1b03bcf0b61ac478a66ed91a224bde86ade4297065c0d3cc5b3ca"`. The hash
always uses the fixed input `cellar`; query parameters do not change it. This separates
SDK HTTP values from computation by an ordinary Rust library.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/webapi/.cellar/native-dev`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
