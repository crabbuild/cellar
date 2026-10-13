# Embedded ownership through a peer partition

The native SDK peer-partition scenario passed with 128 business cells and 145 logical operations. The owner retained its process generation and cell epoch, renewed its bucket lease, and acknowledged all 16 test writes while separated from its two peers. Every receipt survived healing, reconciliation and the final preserving reload. The driver exited zero and the independent saved-data audit passed.

The [structured report](2026-10-13-embedded-peer-partition-128.json) records clean source revision `8b755089d66820c89018c36bf00a14def3c7801f`, image `cellar-embedded-perf:8b75508`, and immutable image ID `sha256:da2bf5c1b860f4b4b6fb2f6b7b716195548abe9152998d3377933071536c9650`. Measuring sources were checked against the image revision before provisioning and during the final audit. Raw journals and state captures remain at `.cellar/perf/peer-partition-128-8b75508` and in a retained archive whose digest is recorded in the report. Private containers, network and loop directory were independently confirmed removed.

## Fault and authority observations

The compiled application embeds `cellar::sdk::Node` on caller-owned host and core executors and supplies listeners, authentication and storage clients. The test application atomically records operation IDs, payload hashes and original counter ordinals. Stable-ID replay returns the original receipt; idempotency remains application policy.

After 128 acknowledged initial writes and a deliberately discarded proxy response, the driver submitted a held write directly to the actual owner of cell 0. It observed the local transaction marker while the client request was pending. At 1.785 seconds after submission, a private filter chain inside node 1's network namespace blocked traffic between that owner and nodes 2 and 3. The host firewall and other containers' namespaces were unchanged.

Four TCP probes confirmed loss of connectivity in both directions between the owner and its peers. All three nodes retained bucket connectivity, and nodes 2 and 3 could still reach each other. The same observations held at the end of the 35.876-second partition. The filter chain was removed, and every peer path became reachable again.

The held operation acknowledged from node 1. Fifteen further operations were submitted after the partition and also acknowledged from node 1. Reads of the cell and its bucket lease retained cell epoch 3 and the same process generation. The lease's expiry advanced by 33,356 milliseconds and remained ahead of each observed time. Requests for cell 0 through nodes 2 and 3 timed out during the partition; neither returned cell state as an alternative owner.

New writes acknowledged while every owner-to-peer path was blocked and bucket access remained available. This supports successful bucket fallback for those writes; the SDK's acknowledgement proof source was not separately instrumented. The pre-partition local transaction marker alone supplies no durability proof.

## Receipt and lifecycle checks

During the partition, the owner served all 18 receipts for cell 0: its initial write, the proxy's lost response and the 16 test writes. After healing and before any replay, the verifier checked all 128 cells and required every client and proxy acknowledgement to retain its payload hash and original ordinal. Reconciliation then replayed issued IDs and required exactly one receipt and increment per logical operation. Node 3 alone served the final exact state after its preserving reload.

| Stage | Client acknowledgements | Ambiguous present | Ambiguous absent | Durable counter total |
| --- | ---: | ---: | ---: | ---: |
| Before partition | 128 | 0 | 0 | 128 |
| Healed, before replay | 144 | 1 | 0 | 145 |
| After reconciliation | 145 | 0 | 0 | 145 |
| Preserving reload | 145 | 0 | 0 | 145 |

Each verification checked payload hashes, original receipts, contiguous per-cell ordinals, counters equal to distinct receipts and absence of unissued operations. Four startup policies captured 16 resident cells, eight cached routes and 128 requests per script pool. All ten required snapshots recorded eight cached routes, 16 owned cells, 16 occupied slots and 24 tracked records. Initial resource captures record one CPU and 2 GiB memory per node.

Two handoff and two preserving stops returned complete ingress, application, cell, log and local-durability drain reports, zero pending events and dispatches, and unexhausted budgets. Both caller executors remained usable after every stop; both preserving stops prepared a clean reload.

Five Rust fixture tests, all 39 Python guards, the locked all-target/all-feature workspace check, measuring-binary Clippy, native CLI/artifact tests, formatting and diff checks passed. The all-feature dependency audit found 386 reachable packages and no JavaScript engine.

## Validation limits

This checks one owner separated from two peers on shared Colima infrastructure with a local single-server RustFS bucket. It does not establish production fleet capacity, a load SLO or a whole-process memory bound. Arbitrary partition topologies, long soaks, growing application state, hot-cell saturation and load coverage of every SDK primitive remain separate work. The fixture retains at most 4,096 operation receipts per cell and has no production retention policy.
