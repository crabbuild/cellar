<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Real RustFS log reclamation experiment

Purpose: Record RustFS log reclamation, correctness checks, backend outage and restart observations, and timing contention.

[Design library](../README.md) · [Topic contract](README.md)

| Implementation | Validation or example |
| --- | --- |
| [Code and API](../../../crates/cellar/durability/raft/README.md) | [Checks and usage](../../../examples/raft-counter/README.md) |

The map summarizes this document’s scope; the recorded gates determine completion.

```mermaid
flowchart LR
    A["Verified archive coverage"] --> B["Reclaim covered log prefix"]
    B --> C["Restart and correctness audit"]
```

- Measured on October 26, 2026; recorded on October 27.
- The candidate reclaimed archived multi-group Raft prefixes during live traffic, preserved every checked replica, and passed a bounded RustFS outage and retained-disk restart.
- The throughput and latency results below are **contended observations**: unrelated host Rust compilations overlapped the measured runs.
- They do not establish a latency improvement, sustainable capacity, or completion of MR-14/Q1.

Machine-readable evidence is retained locally outside version control. It contains all thirteen runs, source/executable hashes, exact profiles, counters, resource observations, host contention, and hashes of retained raw evidence.

## Sources and fixture

| Item | Recorded value |
| --- | --- |
| Production baseline | `927d3f34bc9a5681fddc5d6eaa197d9ecc96f9c4` |
| Production candidate | `e5cf3035053b307f4184ea6f70f005dbf7ce6cd5` |
| Candidate plus common measuring code | `3bfcab222d3406cf6eebc1724cf8e2cc8cbe86ba` |
| Candidate executable SHA-256 | `878e961c16f344903975a0af51710fa123ac981c886f32e3c09e2ec9067b085d` |
| Baseline executable SHA-256 | `c61b10cd40af89e38f1cfa5ebc63ef30eeea111f6d6bef010dc2c11e0e991b33` |
| RustFS image and actual image ID | `ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858` |
| Host | Apple M2 Max, 12 CPU cores, 32 GiB RAM; macOS arm64 |
| Guest | Colima Linux aarch64, 8 vCPU, 16,732,610,560 bytes reported memory |
| Cellar voters | Three containers; each 1 CPU, 3 GiB memory, no swap; distinct CPU sets 0–2 |
| RustFS | One container; 2 CPUs, 4 GiB memory, no swap; CPU set 3–4 |
| Storage | Fresh persistent Docker volumes per run; no durable-directory tmpfs |
| Build | Locked Rust 1.98.1 `lab` builds, two build jobs, separate checkout target caches |

- The baseline received the same eleven measurement/build files as the candidate; its production Rust stayed at the recorded parent.
- Its dirty-tree flag records that common instrumentation.
- Both images passed the complete embedded source manifest and executable-hash checks, and every deployed voter executable matched its image.
- Post-run inspection verified the real pinned RustFS image in all thirteen owned backend containers.

The measuring changes support both performance schemas, count append and rewrite files, and add deterministic varied payload contents. They also repair the Docker recipe's missing workspace tool inputs. The same recipe and measurements were used for both production versions.

## Workload and accounting

- Each run initializes 24 cells with real SQLite state, then keeps all 24 groups active.
- Every group has three persistent voters; each node leads eight groups and follows sixteen.
- Initial database sizes alternate between 16 and 64 KiB.
- Commands use conditional `set` and `transfer` operations through the native SDK path, with seeded random choices and 32/64 KiB payloads.
- Payload contents use deterministic SHAKE-256/base64 output rather than repeated bytes.
- Queries request minimum-version follower reads; the advertised read fraction is 20%.
- The exact per-cell serialized history is checked independently.

- The healthy sustained runs offer 40 operations/second, with 20 seconds warmup and 120 seconds measurement: 4,800 measured arrivals per run.
- The generator permits 16 in-flight operations and one operation per cell, keeps the original open-loop schedule, and records refused arrivals.
- Four initial smoke runs use 10 arrivals per second, five seconds warmup, and fifteen seconds measurement.
- Every run uses a fresh bucket, node volumes, command identities, and private credentials.

- Latency is schedule-to-successful-completion, including invocation-journal fsync and dispatch delay.
- Separate wire and journal-sync quantiles remain in the evidence.
- Refused arrivals stay in the denominator; they are not successful operations with zero latency.
- Healthy submitted operations all completed without server refusals, ambiguity, or state-model violations.
- Refusals were predominantly `cell_busy`, with the exact additional reasons retained in the evidence.

- Within each durability contract the sustained order was baseline, candidate, candidate, baseline.
- Both versions use their production defaults.
- The candidate uses a 1,000-ms archive checkpoint spacing, a 1,000-ms purge interval, 16-MiB target files, and a 64-MiB purge watermark.
- File observations include the append and rewrite queues, periodic samples, phase boundaries, and an identical ten-second quiet drain after final replica/archive verification.

## Healthy sustained results

All rows independently reconcile their submitted histories, all 72 replicas, and archive coverage for all 24 groups. The experimental traffic gate requires 99% of offered arrivals completed, at most 1% unsuccessful arrivals, and p99 at most 500 ms. Quorum and S3 acknowledgements have different durability contracts.

| Contract/version/run | Completed / offered | Ops/s | p50 / p95 / p99 ms | Settled log MiB, three nodes | Traffic gate |
| --- | ---: | ---: | ---: | ---: | --- |
| Quorum baseline 1 | 4,784 / 4,800 | 39.87 | 14.47 / 27.64 / 58.70 | 576.31 | pass |
| Quorum candidate 1 | 4,705 / 4,800 | 39.21 | 20.02 / 81.00 / 150.85 | 144.02 | miss |
| Quorum candidate 2 | 4,795 / 4,800 | 39.96 | 14.29 / 22.63 / 30.48 | 157.09 | pass |
| Quorum baseline 2 | 4,780 / 4,800 | 39.83 | 13.38 / 39.68 / 79.07 | 574.68 | pass |
| S3 baseline 1 | 4,232 / 4,800 | 35.27 | 40.76 / 259.90 / 891.85 | 511.18 | miss |
| S3 candidate 1 | 4,221 / 4,800 | 35.18 | 65.11 / 252.13 / 408.98 | 75.48 | miss |
| S3 candidate 2 | 3,741 / 4,800 | 31.18 | 88.52 / 638.05 / 1,066.45 | 12.66 | miss |
| S3 baseline 2 | 3,960 / 4,800 | 33.00 | 92.32 / 380.47 / 805.14 | 482.56 | miss |

- The candidate retained 73–75% fewer log bytes in the Quorum pairs and 85–97% fewer in the S3 pairs at the fixed settled observation.
- Each candidate run observed 36 old engine files disappear across the three voters; each baseline observed zero.
- Maximum periodically sampled retention on any candidate voter was 63.47–65.76 MiB for Quorum and 63.48–64.25 MiB for S3.
- Baseline maxima were 191.58–192.14 and 160.87–170.41 MiB, respectively.

- These sizes are retained log-file lengths.
- Observed deleted-file lengths are lower bounds, and periodic maxima are sampled maxima.
- The census is non-atomic; files grow and live records can move to rewrite files.
- Watermarks remain soft, and settled sizes depend on where the run ends in a purge cycle.
- Counts differ between runs because refused arrivals remain explicit.
- These percentages do not measure physical device space freed, total storage savings, or a hard disk quota.
- Read-only post-shutdown whole-node directory sizes are recorded separately; they include the retained SQLite images and checkpoint files.

- At the settled cut, all 72 candidate replicas in every run had `checkpoint == applied == archived`.
- The baseline sustained runs retained older checkpoints on most replicas; they still preserved their complete acknowledged state and archive coverage.
- The short smoke runs also verified this distinction, but never crossed the purge watermark and deleted no files.
- Candidate smoke retention was about 27 MiB total versus about 20 MiB on the baseline, so the extra checkpoint records do not give immediate disk savings for tiny histories.

## Maintenance cost and contention

| Contract/version/run | Snapshot encodes, three nodes | Engine completed-write MiB | Largest node cgroup lifetime peak MiB |
| --- | ---: | ---: | ---: |
| Quorum baseline 1 | 21 | 495.31 | 431.90 |
| Quorum candidate 1 | 6,918 | 617.29 | 145.61 |
| Quorum candidate 2 | 7,043 | 630.13 | 144.82 |
| Quorum baseline 2 | 18 | 493.97 | 429.06 |
| S3 baseline 1 | 12 | 434.18 | 404.77 |
| S3 candidate 1 | 6,853 | 553.85 | 146.61 |
| S3 candidate 2 | 6,158 | 491.32 | 148.89 |
| S3 baseline 2 | 13 | 405.92 | 395.00 |

- Archive-driven checkpoints add substantial encoding and engine-write work while reducing retained logs and the observed cgroup memory peak.
- Engine write counters are completed API bytes, not device I/O.
- Memory peaks include startup/warmup and file cache; they do not isolate heap use.
- Cgroup I/O remains keyed by device, without summing potentially stacked devices.
- All runs met the memory headroom gate and recorded no OOM or OOM kill.
- All candidate purge operations succeeded.

- Host boundary samples show active unrelated `rustc` processes overlapping every healthy sustained run, sometimes using several host cores and multiple GiB of RSS.
- Only the VM's guest CPU sets are isolated.
- Host compilation, shared storage, and changing memory pressure confound latency and throughput comparisons.
- Both the 30–151 ms candidate Quorum p99 range and the 409–1,066 ms S3 range remain reported.
- No timing-based default tuning is justified by these runs.
- An idle-host repeat and isolated checkpoint-spacing comparison remain necessary before claiming sustainable capacity or choosing a different default.

## RustFS outage and retained-disk restart

The candidate S3 recovery run used the same sustained profile, with one observed seven-second RustFS process pause scheduled 105 seconds into measurement. Its original measured outcomes were 3,725 completed, 16 ambiguous, and 1,059 refused arrivals. The original outcomes remain in the results after recovery.

- Twenty fresh commands were dispatched in the observed store hold; none
  acknowledged before release.
- All sixteen original ambiguous commands were resolved using the same command
  identities and inputs. Thirty-two recovery requests checked stable receipts;
  the independent history and all 72 final replicas matched.
- Ten read-only observation batches fit strictly inside the physically observed
  pause. Of 720 per-replica cuts, 513 had applied state ahead of archive coverage.
  Every observed checkpoint stayed at or below confirmed archive coverage.
  These are per-replica observations, not an atomic cross-replica snapshot.
- Final state/archive verification completed within an observed 12.22-second
  upper bound after store release. This includes the remaining workload window
  and does not measure the first successful retry or isolated backend latency.
- Physical reclamation continued after recovery, with 36 removed files observed
  and 3.44 MiB of settled log files across three nodes.

- After the candidate S3 smoke and first sustained S3 run, all three voters were stopped and restarted with the same data volumes and fresh process generations.
- Each restart checked every cell independently on every voter against its acknowledged version token and exact state: 72 successful checks per restart.
- The sustained restart followed actual physical file deletion.
- This verifies ordinary retained-disk recovery; all-disk-loss restore, crash during purge, independent failure domains, Amazon S3, and long-soak qualification remain outside this experiment.

## Reproduction and evidence

- Use the measuring source at `3bfcab22` and the [checked profiles](../../../tools/perf/raft_reclamation_profiles.json).
- To build the baseline, use a detached checkout at `927d3f34` and copy exactly the eleven common measurement files listed with hashes in the evidence.
- Build both with [raft.Dockerfile](../../../tools/perf/raft.Dockerfile), `--locked`, `BUILD_JOBS=2`, and distinct target cache IDs unique to those checkouts.
- Do not reuse another checkout's target cache.
- Run only one fixture at a time in the selected context.

The executed runner/analyzer invocation has this shape, using each freshly built image's immutable ID and a fresh output directory:

```sh
python3 tools/perf/raft_run.py \
  --context colima-cellar-multi-raft-300 --port-base 21400 \
  --image sha256:41887c51a1a9250b5e796f89453796a30c1cbb6e05180c8b5afb7c9fbbea6557 \
  --profile tools/perf/raft_reclamation_profiles.json --profile-name load-quorum \
  --scenario healthy --observe-log-reclamation --output /absolute/new/run-directory
python3 tools/perf/raft_analyze.py /absolute/new/run-directory
```

- Use `load-s3` for object-store acknowledgements and the matching baseline image for the control.
- The recovery run adds `--scenario s3-recovery` and a fault JSON with `target: object-store`, `pause_ms: 7000`, `max_pause_ms: 9000`, `interval_ms: 180000`, `start_after_seconds: 105`, `stop_before_seconds: 5`, `max_start_lateness_ms: 250`, and `format: 1`.
- The existing analyzer checks the pause windows, acknowledgement witnesses, original recovery identities, histories, replicas, archive coverage, resource limits, and raw file censuses.

- Raw files remain private under `.cellar/raft-reclaim-20261026-3617/`, including the orchestrator, extra read-only outage observer, original ledgers, node observations, build logs, and per-run independent analyses.
- Public evidence includes their relevant hashes and sanitized results; credential/config files are not published.
- All thirteen runs' four owned processes stopped cleanly with zero exit status.
- Volumes and stopped containers remain available for inspection.
- Only verified empty networks belonging to these runs were removed.
- The VM's temporary build-DNS change was restored before the first workload.

- Build setup initially failed on guest DNS and then on missing workspace tool inputs; those attempts are retained.
- Two baseline smoke preflights encountered TCP TIME_WAIT before creating workload resources.
- The private driver was fixed to permit address reuse after confirming there was no live listener; both smoke workloads then passed.
- No failed workload was silently replaced or omitted.

The final Python harness suite ran 222 cases: 221 passed and one existing opt-in Docker build-fixture case was skipped. Repository format and layout checks passed for 1,205 Rust files, and native dependency isolation passed. Production Rust was unchanged by the benchmark work; its earlier source-pinned integration validation remains separate from these live RustFS results.