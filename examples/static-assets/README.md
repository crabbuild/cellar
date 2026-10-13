# Static assets

Package a website with its native application and access it through an asset binding.

## How it works

`project.json` packages `assets/` as `ASSETS`. Normal asset requests serve the
website directly; `/assets-binding/index.html` enters the Rust handler and fetches
the same index through `Context::assets`.

Read the [Rust source](../main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[main.rs](../main.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/static-assets/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/
curl -sS -w '\n' http://127.0.0.1:9876/assets-binding/index.html
```

## Expected output and interpretation

Both requests return HTTP 200 and HTML containing `<title>Cellar native assets</title>`
and `<h1>Assets served by Cellar</h1>`. Open `http://127.0.0.1:9876/` in a browser to
view it. Matching content proves normal asset routing and Rust binding access reach the
same packaged file. The asset directory is project-local so packaging can stage it
safely.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/static-assets/.cellar/native-dev`. Restart with the same command without
`--clean` to retain it; add `--clean` on the next start to reset this example. Stop
before running another example on port 9876, or choose another `--port` and change the
request URLs. JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
