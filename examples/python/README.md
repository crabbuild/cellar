# Native HTML transformation

Port the upstream HTML processing pattern to native Rust.

## How it works

Native HtmlRewriter replaces the upstream Python HTML processing wrapper. POST HTML and
transform h1 content in an owned streaming response. This demonstrates the application
pattern, not Python execution or the upstream summarizer package.

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
cargo run -p cellar --bin cellar --locked -- \
  dev examples/python/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST --data-binary '<h1>Before</h1><p>Hello</p>' http://127.0.0.1:9876/html
```

## Expected output and interpretation

HTTP 200 contains:

```html
<h1>Processed by native Rust</h1><p>Hello</p>
```

Only h1 content changes; the paragraph survives. This verifies a real parser/rewriter
operation. The `python` folder preserves the upstream catalog name, while the
implementation and build are native Rust. It does not execute Python or integrate the
upstream summarizer package.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/python/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
