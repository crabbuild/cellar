# Embedded recovery after node isolation

The native SDK isolation scenario passed all four receipt verification stages. It used 128 business cells and 145 logical operations: 128 initial writes, one response deliberately lost at a proxy, and 16 concurrent writes held before their handlers returned. All acknowledged receipts survived before replay. Reconciliation and the final preserving reload retained exactly 145 operations with their original payload hashes and ordinals.

The [structured report](2026-10-13-embedded-isolation-128.json) records image `cellar-embedded-perf:a13adb6`, immutable image ID `sha256:c98e6dc595473e887ac24981cd168ecf15fcd9109683861f01887b1c4adb4587`, and clean source revision `a13adb6e528c4a288bb26b1661c1db3aa592c5b8`. Measuring sources were checked against the image revision before provisioning and again during the saved-data audit. The driver exited zero. Private containers, network and loop directory were independently confirmed removed. Raw client journals and state captures remain at `.cellar/perf/isolation-128-a13adb6` and in the retained archive identified in the report.

## Failure and recovery

The compiled application embeds `cellar::sdk::Node` on caller-owned host and core executors. It supplies listeners, authentication and storage clients. Its transaction records each operation ID, payload SHA-256 and original counter ordinal atomically; stable-ID replay returns the same receipt. This application idempotency policy does not imply exactly-once request delivery from Cellar.

The driver observed node 1 commit a local transaction while its client request remained pending in a thirty-second held handler. It started isolation 0.037 seconds after the concurrent burst began and observed the owner disconnected from its sole private Docker network at 0.233 seconds. That cut peer, bucket and public network access. The local transaction marker was diagnostic evidence of acceptance, not an output-gate acknowledgement.

About 8.947 seconds after detachment, the SDK returned `NodeOutcome::Fenced` with code 3 and reason `NodeLeaseExpired`. Both caller executors remained usable after the SDK joined its owned work. The embedding application then chose process exit 1. The process exit does not supply a graceful drain proof; the typed SDK report identifies the authority loss.

The two survivors verified acknowledged receipts before any request replay. All 128 client-acknowledged operations and the proxy's acknowledged but discarded response survived. Ten other ambiguous operations were present once, and six were absent. Reconciliation retried issued IDs, checked original receipts, and required exactly 145 distinct operations. The failed node restarted on its original network and directory. After handoff and preserving shutdowns, node 3 alone served the final exact state following its reload.

| Stage | Client acknowledgements | Ambiguous present | Ambiguous absent | Durable counter total |
| --- | ---: | ---: | ---: | ---: |
| Before isolation | 128 | 0 | 0 | 128 |
| Before replay | 128 | 11 | 6 | 139 |
| After reconciliation | 145 | 0 | 0 | 145 |
| Preserving reload | 145 | 0 | 0 | 145 |

Every stage checked payload hashes, original receipt ordinals, contiguous per-cell ordinals, counters equal to distinct receipts, and absence of unissued operations. The pre-replay check prevents replay from concealing acknowledged data loss.

## Resource and lifecycle checks

All five startup policies captured 16 resident cells, eight cached routes and 128 requests per script pool. Each of the eight required snapshots reported eight cached routes, 16 owned cells, 16 occupied slots and 24 tracked records while visiting 128 cells. Initial resource captures record one CPU and 2 GiB memory per node, with private 5 GiB ext4 data images.

Both handoff stops and both preserving stops returned complete ingress, application, cell, log and local-durability drain reports, zero pending events and dispatches, and unexhausted shutdown budgets. Each preserving stop prepared a clean reload; both caller executors remained usable after every stop.

The fixture's five Rust tests and all 30 Python guards passed. The workspace all-target/all-feature check, measuring-binary Clippy, 13 native node integration tests, native CLI/artifact tests, formatting, dependency audit and diff checks passed. The dependency audit found 386 reachable packages and no JavaScript engine.

## Validation limits

This is a bounded correctness run on shared Colima infrastructure and a local single-server RustFS store. It tests full node isolation, including loss of bucket access. Selective peer-only partitions, long soaks, hot-cell skew, growing application state, production fleet capacity and load coverage of every SDK primitive remain separate work. Fleet durability may fall back to bucket proof; acknowledgement source was not separately instrumented. The fixture retains at most 4,096 operation receipts per cell and has no production receipt-retention policy.
