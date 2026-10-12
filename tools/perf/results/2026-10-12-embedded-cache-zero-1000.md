# Embedded recovery with routing caching disabled

The current Linux release image completed the single-node bucket scenario with 1,000 business cells, 64 resident slots and `max_cached_routes = 0`. All 9,284 acknowledged counter increments survived both a process kill with discarded local state and the final preserving reload. There were no load errors, counter mismatches or verification read retries.

The [structured report](2026-10-12-embedded-cache-zero-1000.json) records image `cellar-embedded-perf:4db0ba6`, its immutable image ID and revision, source hashes, three captured startup policies, four required state snapshots and both returned preserving-stop proofs. Checkout `52519f1` adds documentation only; its production and measuring sources match the image's `4db0ba6` revision.

| Checkpoint | Cached routes | Tracked cells | Owned cells | Occupied slots |
| --- | ---: | ---: | ---: | ---: |
| Warmup and initial verification | 0 | 64 | 64 | 64 |
| After measurement | 0 | 64 | 64 | 64 |
| After cold bucket restoration | 0 | 64 | 64 | 64 |
| After preserving reload | 0 | 64 | 64 | 64 |

Each startup reported the requested 64-resident, zero-cache and 512-request limits. Both preserving stops drained ingress, complete application events, cells and dispatches, quiesced the log, closed local durability and prepared clean reloads. Pending counts were zero, shutdown budgets were not exhausted, and both caller-owned executors remained usable after the SDK node joined its work.

The application used one CPU and 2 GiB memory with a private 5 GiB ext4 data image. The load generator used 32 and 128 concurrent requests for one 10-second repetition after a 60-second warmup. Measurements returned 81.5 and 82.7 acknowledged writes per second. This is a bounded correctness run; its workload and residency differ from the 10,000-cell reference, so those timings are not a before/after performance comparison.

The raw run is retained at `.cellar/perf/embedded-cache-zero-1000-4db0ba6` and archived separately. Its private containers, network and loop directory were removed after completion. Positive routing-cache limits and remote routing still require the current-image three-node run. These sampled counts do not prove a whole-process memory bound or a production fleet size; alarms and unfinished work retain their records independently of routing-history caching.
