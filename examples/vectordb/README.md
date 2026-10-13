# Per-cell vector search

Keep a real vector index with its business cell and query nearest neighbors.

## How it works

Each palette cell owns a sqlite-vec vec0 index. The seed data is red, green and blue.
GET performs nearest-neighbor search on the RGB vector; three integer components must be
in 0..255.

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
  dev examples/vectordb/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' 'http://127.0.0.1:9876/vectors/palette?rgb=250,20,20'
curl -sS -i 'http://127.0.0.1:9876/vectors/palette?rgb=bad'
```

## Expected output and interpretation

The valid response has `columns:["label","distance"]` and three `rows`. The first row is
`["red", approximately 28.72]`: red has the smallest Euclidean distance to
`[250,20,20]`. Equal-distance rows need not have a fixed tie order. Invalid RGB input
returns HTTP 400. This is a real sqlite-vec index query, not a hard-coded nearest label.

The project explicitly enables the `sqlite_vec` compatibility flag. Keep it when
adapting the recipe; without it, the `vec0` module is unavailable.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/vectordb/.cellar/native-dev`. Restart with the same command without `--clean`
to retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
