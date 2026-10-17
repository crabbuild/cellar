# KV namespace

Store bounded bytes with metadata, expiration and paginated listing.

## How it works

PUT writes bytes with metadata and a 60-second TTL. GET reads, DELETE removes, GET /kv
returns a bounded page and a continuation cursor. Namespace calls route to the framework
owner; applications never open its database directly.

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
  dev examples/kv/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X PUT --data-binary 'hello KV' http://127.0.0.1:9876/kv/demo
curl -sS -w '\n' http://127.0.0.1:9876/kv/demo
curl -sS -w '\n' http://127.0.0.1:9876/kv
curl -sS -w '\n' -X DELETE http://127.0.0.1:9876/kv/demo
curl -sS -i http://127.0.0.1:9876/kv/demo
```

## Expected output and interpretation

PUT returns `{"stored":"demo","ttl_seconds":60}`. GET returns `hello KV`. Listing
contains `keys:["demo"]` and `complete:true`; a continuation cursor matters when
`complete` is false. URL-encode that cursor into `/kv?cursor=...` to read the next page.
DELETE returns `deleted`, then GET returns HTTP 404 with `missing key`.

To observe TTL instead, PUT again, wait at least 61 seconds and GET: it returns 404
without DELETE. Complete the initial reads within 60 seconds of writing.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/kv/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
