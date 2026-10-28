<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Per-cell filesystem in Rust

Each named notebook owns `/notes.txt` in its cell's SQLite state. The Rust
handler uses `context.fs()` to read it, and exact `tx.fs()` to append text with
the entry count and an idempotency receipt in one transaction. No mount or
process runner is required.

## How it works

[main.rs](main.rs) is a module of the [native cookbook](../main.rs).
Its `Notebook` class opts into the filesystem capability through
`Registry::filesystem`. The [focused project](project.json) routes
`/notebooks/NAME` through the `NOTEBOOKS` namespace. Different names have
independent files and receipt IDs.

POST accepts exactly `id` and `text` strings. It opens the file with
write/create/append options, writes a UTF-8 line, and stores the business count
and receipt in the same native transaction. File handles close before the
transaction returns. After commit, `context.fs().sync().await` requests the
configured durability proof before returning success.

Retry an interrupted request with the same ID and text: the saved receipt is
returned without another append. Reusing the ID with different text returns
409. A failed or lost response does not prove that the write was rolled back.
GET returns the accumulated text, count and file metadata.

This recipe bounds each notebook to 128 entries of 1..4096 UTF-8 bytes, plus one
newline per entry. IDs use 1..128 ASCII letters, digits, `-` or `_`. The shared
HTTP reader caps request bodies at 64 KiB. File and receipt growth are both
bounded; the normal filesystem capability limits also apply. Full notebooks
still accept identical retries of existing IDs.

## Run end to end

Prerequisites: the repository Rust toolchain, native C/C++ build tools and curl.
Follow [AGENTS.md](../../AGENTS.md) for the checkout-specific target directory.
From the repository root:

```sh
cargo run --locked -p cellar --bin cellar -- \
  dev examples/filesystem/project.json --no-watch
```

After `ready`, use a second terminal:

```sh
curl -sS http://127.0.0.1:9876/notebooks/alpha
curl -sS -X POST http://127.0.0.1:9876/notebooks/alpha \
  -H 'content-type: application/json' -d '{"id":"first","text":"hello Rust"}'
curl -sS -X POST http://127.0.0.1:9876/notebooks/alpha \
  -H 'content-type: application/json' -d '{"id":"first","text":"hello Rust"}'
curl -sS http://127.0.0.1:9876/notebooks/alpha
curl -sS http://127.0.0.1:9876/notebooks/beta
```

On fresh state, alpha starts with `entries: 0`, empty text and `file: null`.
Both POSTs return the same receipt with `entries: 1` and `size: 11`. Alpha then
contains `hello Rust\n`; beta remains empty. Inode/version strings describe the
stored file and are returned unchanged when replaying that receipt.

Stop with Ctrl-C and restart without `--clean`. Alpha's text/count and the first
receipt remain. Append a new ID to continue the notebook. State lives in the
project's `.cellar` directory; use `--clean` only to reset that tutorial state.
Local development storage demonstrates recovery, not independent storage-failure
domains. Production deployment uses the normal Cellar durability configuration.

## Verify behavior

```sh
cargo build --locked -p cellar --bin cellar --bin cellar-native-examples
python3 examples/focused_smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --example filesystem
```

The smoke test checks fresh state, Unicode bytes, concurrent appends, duplicate
receipts, conflicting IDs, malformed/oversized input, the entry limit, cell
isolation, retained state/receipts after restart and a new post-restart append.
It owns temporary local state and requires both host processes to stop cleanly.

To exercise the optimized CLI and application, build both with `--release`, use
`$CARGO_TARGET_DIR/release/cellar`, and pass `--profile release` to the smoke
test. The profile flag changes the staged application's build configuration as
well as selecting the corresponding artifact. Using a release CLI alone would
leave the example on its default development profile.
