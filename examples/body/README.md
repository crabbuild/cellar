<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Bytes and streaming

Compare bounded buffering with an owned, backpressured binary stream.

## How it works

POST /body buffers at most 64 KiB; /body/stream forwards an owned stream without
collection. Dropping the stream cancels its source. Streaming bypasses the recipe buffer
limit and relies on the host input limit.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[main.rs](../main.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
curl and Python 3 for generating the test payload. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/body/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST --data-binary 'hello Rust' http://127.0.0.1:9876/body
curl -sS -w '\n' -X POST --data-binary 'streamed Rust' http://127.0.0.1:9876/body/stream
python3 -c 'import sys; sys.stdout.buffer.write(b"x" * 65537)' | \
  curl -sS -i --data-binary @- http://127.0.0.1:9876/body
```

## Expected output and interpretation

The first responses are HTTP 200 with exactly `hello Rust` and `streamed Rust`. The
oversized buffered request returns HTTP 413. The automated smoke test also compares NUL
and non-UTF-8 bytes byte for byte. This endpoint handles bytes without decoding them as
text. Python 3 is used only to generate the oversized test payload.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/body/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
