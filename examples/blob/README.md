# Blob objects

Write, stream and delete objects through a native Blob binding.

## How it works

PUT stores a small object; GET streams its body and metadata; DELETE removes it. Blob
storage uses the node's configured object store.
The tutorial buffers uploads to 64 KiB; the SDK also supports multipart and streamed
uploads.

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
  dev examples/blob/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X PUT --data-binary 'hello Blob' http://127.0.0.1:9876/blobs/demo
curl -sS -i http://127.0.0.1:9876/blobs/demo
curl -sS -w '\n' -X DELETE http://127.0.0.1:9876/blobs/demo
curl -sS -i http://127.0.0.1:9876/blobs/demo
```

## Expected output and interpretation

PUT returns an `object` with `key:"demo"`, `size:10`, version, ETag and upload metadata.
Versions and timestamps vary. GET returns HTTP 200 with exactly `hello Blob` and the
object's HTTP metadata. DELETE returns `deleted`, then GET returns HTTP 404 with
`missing object`. The local example verifies the body and object lifecycle with its
private development store.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/blob/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
