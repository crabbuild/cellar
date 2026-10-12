# Licensing and source provenance

Cellar uses the Apache License, Version 2.0, in the root [LICENSE](../LICENSE).
The Cargo package metadata declares `Apache-2.0` for all four crates. Original
Cellar source, tests, examples and tooling use this repository license without
an individual copyright or license header. Do not add Deno Land attribution to
new Cellar files merely to match nearby files.

For inherited or adapted code, retain the original copyright and attribution
notices. A new filename does not make copied or adapted code original Cellar
work. Mark local changes with `Modified for Cellar` or `Adapted for Cellar from
Celld`, including the source path for moved code. These notices identify the
upstream portions; they do not attribute all new Cellar work to that owner.

This follows the notice and modification requirements in
[Apache License section 4](https://www.apache.org/licenses/LICENSE-2.0).
The repository [NOTICE](../NOTICE) scopes Deno Land's attribution to the Celld
portions. Existing third-party notices and license texts remain intact,
including Tokio's MIT text in `crates/cellar/tokio_select.rs` and the replication
and LZ4 attributions and licenses under `crates/cellar-ltx/`.

## Audited upstream snapshot

The audit uses Celld commit
[`90b43017241f81189453d326d05948f388b34652`](https://github.com/denoland/celld/tree/90b43017241f81189453d326d05948f388b34652).
It checks source contents as well as filenames.

The inherited directory mappings are:

| Cellar path | Original Celld path |
| --- | --- |
| `crates/cellar/` | `crates/celld/` |
| `crates/cellar-logic/` | `crates/logic/` |
| `crates/cellar-ltx/` | `crates/ltx/` |
| `crates/cellar/blob_cli.rs` | `crates/celld/r2_cli.rs` |
| `crates/cellar/blob_store.rs` | `crates/celld/r2_store.rs` |
| `crates/cellar/sql_cli.rs` | `crates/celld/d1_cli.rs` |

The new Rust CLI shim in `crates/cellar/main.rs` replaces the former entrypoint;
its implementation is original Cellar code. The inherited daemon implementation
now lives in `crates/cellar/daemon.rs` and keeps its upstream attribution.
The new SDK, native tests and examples use the repository license. The following
moved or ported files retain upstream attribution despite their new paths:

| Cellar source | Original Celld source |
| --- | --- |
| `crates/cellar-native/cell_id.rs` | `crates/celld/engine_api.rs` |
| `crates/cellar-native/compression.rs` | `crates/celld/js/zlib.rs` |
| `crates/cellar-native/crypto.rs` | `crates/celld/js/crypto.rs` |
| `crates/cellar/application/queue/schema.sql` | `crates/celld/js/harness.js` |
| `crates/cellar/blob_store/multipart.rs` | `crates/celld/js/r2_ops.rs` |
| `crates/cellar/blob_store/streaming.rs` | `crates/celld/js/r2_ops.rs` |
| `crates/cellar/daemon.rs` | `crates/celld/main.rs` |
| `crates/cellar/host_facets.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_facets/calls.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_fetch.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_gates.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_io.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_io/input_gate_lifecycle.rs` | `crates/celld/js/input_gate_lifecycle.rs` |
| `crates/cellar/host_loader.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_loader/calls.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_service.rs` | `crates/celld/main.rs` |
| `crates/cellar/host_tcp.rs` | `crates/celld/js/tcp.rs` |
| `crates/cellar/host_turn.rs` | `crates/celld/js.rs` |
| `crates/cellar/host_websocket.rs` | `crates/celld/js/websocket.rs` |
| `crates/cellar/html_rewriter.rs` | `crates/celld/js/html_rewriter.rs` |
| `crates/cellar/native_deploy/dev.rs` | `crates/celld/dev.rs` |
| `crates/cellar/native_worker.rs` | `crates/celld/js.rs` |
| `crates/cellar/native_worker/facets.rs` | `crates/celld/js.rs` |
| `crates/cellar/native_worker/gate.rs` | `crates/celld/js.rs` |
| `crates/cellar/worker.rs` | `crates/celld/js.rs` |
| `crates/cellar/worker_config.rs` | `crates/celld/js.rs` |
| `crates/cellar/worker_config/imports.rs` | `crates/celld/js/modules.rs` |
| `crates/cellar/ws_tunnel.rs` | `crates/celld/main/peer_tunnel.rs` |

## Future changes

Before removing an attribution, check whether any implementation was copied or
adapted from upstream, including from another filename or language. Retain the
relevant notice for that portion. Reusing an API name or calling an existing
library alone does not require applying its author's copyright to new code.
Keep new Cellar files free of boilerplate headers, and keep the root license and
package metadata as the default license declaration.
