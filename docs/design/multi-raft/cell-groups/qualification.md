<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Qualification and evidence contract

Purpose: Failure oracles, workload profiles and evidence required for release.

Status: proposed workload contracts and release gates, not measured results.
`profiles.json` freezes candidate requirements. Change a missed target only through
an explicit reviewed profile revision, retaining the old run and its failure.
Do not silently raise budgets, remove failures, retry mutations with new IDs, or
compare different logical cell populations to manufacture an improvement.

## Tests and release levels

Every task has exact named native cases. Its executor brief first lists tests,
then runs each required case with `--exact --test-threads=1`. The evidence validator
requires the name in the inventory and a successful one-test result; Cargo exiting
zero after selecting no tests is a failure. These test targets are planned outputs,
not available commands until their owning task creates them.

Use real SQLite/LTX and Raft Engine for state assertions. Scheduler-only fixtures
with stub image markers can test fairness but cannot qualify durability. Both
materialized and extent state, empty enrollment, managed and unmanaged groups,
all read modes and mixed Quorum/S3 commands must appear in the matrix. Native
test execution is serial between fixtures, concurrent within each fixture.

| Level | Evidence | Claim allowed |
| --- | --- | --- |
| Package | Python validator/tests, source preflight and local links | Executable plan only |
| Native | All task cases on real engines plus injected crash/process cuts | Exact tested functional boundaries |
| Local deployment | Five owned Cellar containers/processes, actual RustFS, enforced resource limits and complete ledgers | Local measured workload only |
| Independent domains | Actual separate machines/failure domains and observed physical isolation under load | Only the measured topology/domain-loss envelope |
| Release | All tasks, regressions and required profiles at one immutable source | Complete cell-group capability within that envelope |

The final release requires all profiles marked required in `profiles.json`.
Container counts alone cannot satisfy the independent-domain profile. Million-cell
and Q2–Q4 scale claims remain separate larger experiments from the parent roadmap.

## Managed S3 component verification

The managed native target now implements these explicit backend cases:

- `bootstrap::lifecycle::rustfs_managed_s3_group_replacement_and_joined_restart`
- `bootstrap::lifecycle::maintenance::rustfs_managed_archive_maintenance_stop_return_and_resume`
- `bootstrap::lifecycle::maintenance::loss::rustfs_managed_maintenance_recovers_one_lost_membership_observation`
- `bootstrap::lifecycle::s3::rustfs_managed_s3_seed_refusal_and_source_failover`
- `bootstrap::lifecycle::isolation::rustfs_managed_isolated_voter_s3_replacement_retains_disk_and_fence`
- `bootstrap::lifecycle::recovery::rustfs_managed_failure_recovery_restarts_and_verifies_s3_before_removal`
- `bootstrap::requests::rustfs_managed_native_axum_read_write_and_retained_restart`

Run each by its exact name with `--ignored --exact --test-threads=1` in
`native_cell_groups_managed`. Configure an owned local backend with
`CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT`, `CELLAR_CELL_GROUPS_RUSTFS_BUCKET`,
`CELLAR_CELL_GROUPS_RUSTFS_PREFIX`, `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. The helper accepts only a loopback HTTP endpoint,
requires an isolated prefix and supplies explicit credentials; absence fails.
Use a fresh prefix for every run. Never copy the private credential file into
evidence or a PR. Build first, record and copy the executables, then run cases
serially after writeback. Record and verify the pinned backend image, actual
resource limits and signed conditional-write/read-after-write probe separately.

Both image forms run. Replacement checks real S3 payload transfer, independent
SQLite histories/receipts, joint finish, lost settlement replies, owned drain and
retained joined-member restart. Maintenance covers initial and joined voters,
owned stop, offline confirmation, paused return and fresh resume. The fault case
denies target payload GETs before attachment, requires retained nonserving owner
and debit, refuses a peer-mode change, stops the source service and requires a
fresh election and completed repair. Replacement-local queries verify actual
responder, values and versions; new writes require S3 proof. A surviving original
voter's missing physical inventory must block catalog settlement and retain its
debit even after membership repair completes.

The isolated-voter case first joins the complete obsolete Node, then supplies
fixture isolation evidence. Reject evidence before rooted exclusion and reject
the wrong process binding without advancing the root. A saved pre-isolation
startup capability must return the typed catalog conflict, with both fleet root
and retained engine files unchanged. Replace that voter from S3, require fresh
stable RF3 and the exact `isolated_sources` receipt, lose the actual settlement
confirmation and reconcile the same request. Check both restored cell
images/receipts through replacement-local queries, explicit S3 writes, the
replacement's physical capacity floor and byte-identical retained obsolete log.
This fixture does not supply permanent Docker launch fencing or terminal node
removal. Keep the original eight reconciliation turns and five-second calls.

The managed HTTP case uses the counter's existing native and Axum adapters with
independently bound applications and catalog clients on three actual managed
Nodes. Cold startup must qualify capacity, reserve/publish/confirm group birth,
activate initial voters and replicate enrollment before HTTP admission. Check
independent cell state, immutable retry receipts, conflicting-input refusal and
cross-cell token refusal. Require a follower command refusal before explicit
write-origin admission, reject foreign namespaces and stale processes, revoke
one current process and require no mutation, then restore permission with a
newer revision and retry the same command. Require explicit S3 acknowledgement
through the follower as well as ordinary quorum responses. Force remote
minimum-version and linearizable reads
through every adapter/node, requiring the actual responder identity. Join the
leader, require successor service, then restart retained disks with new process
generations and require old receipts/version floors plus new writes. After two
joined voter stops, observe actual quorum loss and require both adapters to
refuse writes and linearizable reads. Shutdown joins every server and Node and
accounts for unresolved work; failures retain all voter directories. Both image
forms run. The same fixture supplies `ingress_cannot_cross_cell_authority` in
`native_cell_groups_public`, with the managed exact test names retained. These
local embedded Nodes and fixture capacity attestations are separate from the
complete lifecycle, independent voter-process, permanent fencing,
crash/partition and load gates.

The automatic recovery case uses the Node-owned failure service with the original
failure grace, probe interval and five-second turn limits. Hold an exact-boot
isolation callback, verify repair has not begun, refuse a duplicate worker, then
shut down and require
joined callback cancellation and released control admission. Restart the service
through S3 replacement while the independent verifier refuses payload GETs.
Require fresh replacement inventory, no terminal receipt and unchanged obsolete
disk. Stop again, restore reads and restart: require additional physical S3 GETs,
exact-operation terminal removal, no repeated committed isolation, empty movement
and reservation books, original receipts/new S3 writes and unchanged retained
obsolete disk. Both image forms start with cold private verifier inventories;
no business mutation drives the repair. These fixture attestations do not qualify
Docker launch revocation or the complete public lifecycle gate.

The lost-observation regression uses independently owned catalog clients over the
same actual backend. After handoff, hold one foreground local-member catalog read
without resuming it; background trust refresh and the controller's client remain
unaffected. The walk must return a transient refusal, retry that pending history,
and verify all three members within the original five-second renewal envelope.
Both image forms require one handoff and exactly one verified history. This case
ends at renewal; the unchanged full lifecycle case separately qualifies owned
stop, return, resume and capacity settlement. On failure, bounded read-only local
status and admission snapshots are recorded before cleanup; the original error
still fails the test.

Initial/final test handoffs reconcile actual leadership under one original
five-second envelope. Re-observe the current leader, capture its term and
membership revision, and use guarded transfer admission. NotReady or a lost
admission response requires observing actual state again; acceptance alone is
not election completion. Successful handoff still requires the target's actual
ready leader state. The original eight movement turns remain unchanged.

The fault case also blocks actual registry mutations after target payload
refusal. Require a witnessed denied write, retained pending journal and observable
cleanup error. Restore writes without issuing another learner request: the node's
idle owner must confirm retirement and sync the empty record. A second failed
retirement remains pending through source-service close; restore writes only after
that source joins so the surviving quorum can repair. Preserve the shared-backend
effect when the first fixture owns the underlying store.

After every old host joins shutdown, reopen only that source on its retained disk.
With registry writes denied, background discovery must find the original engine-
and archive-bound intent without a live quorum or another learner request. Restore
writes and require durable retirement without changing fleet authority. Gate an
actual archive read, start close, verify sealed admission and retained S3/sweep
ownership, cancel its waiter, then release the read and require joined shutdown.
Sample retained cell inventory before close seals engine I/O admission. Both image
forms run. Separate journal tests reject a foreign engine, ordinary archive-journal
purpose, missing state and missing identity; existing-only open cannot reconstruct
those records.

These are embedded native Nodes with separate domains, disks and TLS. Real RustFS
verifies the S3 adapter; it does not supply independent or resource-constrained
voter hosts. A source-service stop is distinct from process SIGKILL and permanent
platform fencing. Retain every missed five-second readiness window and failed
run; later passes cannot establish a fixed failover SLA or waive that failure.
Five-second calls and eight reconciliation turns remain unchanged. See
[progress](progress.md) for source-bound results and unresolved qualification.

## Planned harness interface

CG-14 implements `tools/perf/cell_groups.py`; it does not exist at plan delivery.
The following is its required CLI, not a command claimed to work today:

```text
python3 tools/perf/cell_groups.py preflight --config PRIVATE_CONFIG
python3 tools/perf/cell_groups.py run --profile PROFILE --config PRIVATE_CONFIG --output FRESH_RUN_DIRECTORY
python3 tools/perf/cell_groups.py verify --run RUN_DIRECTORY
python3 tools/perf/cell_groups.py stop --run RUN_DIRECTORY
```

The private configuration must bind a run UUID, explicit Docker context or process
host inventory, immutable Cellar/RustFS image digests or executable manifest,
source revision, dedicated data roots, credentials-file paths, port allocation,
node CPU/memory/swap/disk limits, failure domains and fencing-authority endpoint.
Validate all required fields; missing values never fall back to the current Docker
context or a default data directory. Credentials and unrestricted Docker inspect
output stay out of reports. Unsupported resources fail before provisioning.

Use `tools/perf/raft_fleet.py` for managed-node provisioning patterns and the
shared-group daemon fixture under `crates/cellar/tests/native_raft_daemon.rs`
for native process kill and disk-erasure proof. Extend their existing owners. Own resources by immutable IDs, run labels and exact paths. The destructive
profile must confirm ownership and join stopped processes before removing only
the recorded voter roots. Keep RustFS data and retained evidence outside voter
roots, build cleanup and global prune paths. `stop` retains data for inspection.

## Workload accounting

Build once before timing. Record source, compiler/features, binary digest, actual
CPU/memory limits, disk/filesystem, host activity, RustFS digest and TLS topology.
Run cohorts serially with fresh identical initial data and a frozen arrival trace.
Compare the old one-cell baseline against the replacement, and compare group
occupancies 1/8/32/64/128 within the replacement. Capture measured actual groups,
not only configured target occupancy. Each logical cell has a separate SQLite DB.

The offered-load ledger records stable cell/client/sequence, input digest, request
start, deadline, response/timeout/cancel/refusal, returned result, cell version,
group witness and requested/achieved durability. Keep every offered request in
throughput/error denominators. Reconciliation is a separate phase and cannot
retroactively turn a timed-out request into a successful timed sample.

Independently verify all acknowledged effects and receipt results on every serving
replica and the selected recovery cut. Check per-cell real-time strong-read order,
at-most-once mutation by command identity, allowed ambiguous outcomes and no
overlapping writable owners. Deliberately corrupt oracle inputs to prove it detects
lost effects, borrowed versions, double execution and stale strong reads.

Measure CPU cores, process RSS, descriptors, group/cell counts, outgoing control
bytes below/above TLS with the exact boundary stated, log/image/manifest write
bytes, fsyncs, S3 PUT/GET bytes/requests and retained/archive lag. Report p50/p95/p99
and sample counts by durability, hot/cold cell and fault phase. Warmup, setup,
election, cache restore, archive drain and joined shutdown are separate intervals.

For ratios, require a positive meaningful matched baseline; a zero denominator
cannot pass a reduction gate. Resource peaks use actual OS/engine observations,
not configured limits or synthetic host metrics. A recovered follower is not proof
that every replica or every cell recovered. Selected-cut Quorum suffix loss is
reported separately from missing S3-protected history.

## Required fault schedule

Kill/restart at image sync, manifest sync, marker commit, enrollment, snapshot
install, archive publication, reclaim, seal authorization, parent seal, child seed,
root publication, activation, retirement, park and wake generation closure.
Inject corrupt/missing objects, duplicate/delayed messages, stale routes, unknown
CAS responses, leader/controller loss, a slow/minority follower, disk-full/sync
failure, network partitions and bounded S3 outage. Run foreground writes and
strong reads while splitting/merging and moving replicas.

For zero-quorum tests, verify the archive/ledger boundary first, fence all possible
old voting lineages and demonstrate launch revocation. Delete all voter disks,
restore complete groups from real RustFS with no surviving peer images, verify
every cell and receipt, reject old tokens/old-host restart, acknowledge new writes,
and restart the fresh quorum on its new disks. Repeat before/after topology
publication and after reclamation. Managed inventory/transition journal failures
must fail closed. Unreconciled old-origin outbox work must remain refused.

## Task evidence

Store local raw evidence under an ignored repository-relative directory, for
example `.cellar/cell-groups-evidence/RUN/`; CI may download sanitized artifacts
there before validation. Do not commit credentials, cell data, opaque tokens or
the previously excluded MR-13/MR-14 progress JSON files. Add a relative record path
to a task's `evidence` only after its implementation and checks actually pass.

A task record uses this shape (example values are placeholders, not evidence):

```json
{
  "format": 1,
  "task_id": "CG-01",
  "source_revision": "FULL_40_HEX_COMMIT",
  "executable_sha256": "64_HEX",
  "qualified_executable_sha256": "DEPLOYED_CELLAR_BINARY_64_HEX_WHEN_PROFILES_ARE_PRESENT",
  "environment": "Actual compiler, features, OS and resource observations",
  "limitations": "Exact scope and untested boundaries",
  "source_inputs": [{"path": "crates/...", "sha256": "64_HEX"}],
  "commands": [{"argv": ["cargo", "+1.98.1", "test", "..."], "exit_code": 0,
                "output": ".cellar/.../stdout.log", "sha256": "64_HEX"}],
  "profiles": [{"path": ".cellar/.../profile.json", "sha256": "64_HEX"}]
}
```

`source_inputs` must include the task's pinned source anchor paths and its test
target; the validator verifies each hash against the named Git revision. Include
all runtime sources changed since the frozen `tasks.json` planned-at revision in
the record as well. Later source-baseline reviews do not rewrite this evidence
boundary or waive earlier implementation inputs. Test inventory and exact
one-test outputs must be hashed and retained. Every command in `plan.py show`
verification must appear with exit zero. Earlier failed runs stay in the run
directory and are described in the final report; do not aggregate passes across
different binaries into one task record.

For CG-16, one record must contain all new tasks' verification commands at the same
release revision, plus the repository checks listed below and all required profile
reports. Dependencies' earlier passes alone cannot qualify a changed release.
The task record must not claim it verified different executable inputs. Later
plan-only status/evidence commits are permitted; the release command compares all
other repository inputs against the qualified source and rejects dirty files,
including examples, vendor code, build configuration and newly added input paths.

Profile reports bind `profile_id`, `source_revision`, `executable_sha256`, the
SHA-256 of the entire frozen `profiles.json`, actual environment observations,
numeric `metrics`, and hashed artifacts for `ledger`, `measurements`, `topology`,
`fault_schedule` and `verification`. The profile must retain its controls,
repetitions and parameter matrix in those artifacts. `plan.py` checks every numeric
gate, required topology and artifact hash. A bare `passed: true` is insufficient.
Values must be finite and nonnegative; null, missing, NaN and boolean measurements
refuse. The report and every replacement cohort must match the task record's
`qualified_executable_sha256`. The primary `executable_sha256` identifies the native
test artifact; these separate fields avoid equating it with the deployed server.

The measurements artifact is JSON with `metrics` equal to the report summary and
`cohorts` containing exactly the declared occupancy × image mode × repetition
matrix. Each cohort records:

```text
variant: replacement | historical
occupancy, image_mode, repetition
source_revision, executable_sha256, ledger_sha256
cells, nodes, voters, offered_requests_per_second, measured_seconds
metrics: numeric observations for that cohort
```

Historical controls are required only where `historical_control` is true, at
occupancy one and the pinned historical revision. Replacement controls and
candidates use the same qualified binary. Every cohort must meet its frozen
population, resources, arrival rate and minimum duration. Correctness/error gates
apply to every cohort, including historical and occupancy-one controls. Performance
gates apply to `gate_occupancies`. Summaries use the worst eligible cohort: maximum
for a max gate, minimum for a min gate, and equality in every cohort for an eq gate.
Never average a slow or failed cohort into a passing result.

Ratio observations must equal the numerator divided by the matching control's
denominator, with the same image mode and repetition; `ratios` defines the exact
metric names and control occupancy. A missing/zero control refuses the ratio.
Hibernation's active and parked rates are separate phases of the same cohort.
The native harness derives the other measurements from actual ledgers/OS samples;
the plan validator does not replace the workload oracle or establish physical
infrastructure truth from a self-reported number.

The topology artifact is JSON with `kind` and `nodes`, each containing `id`,
`physical_host` and `failure_domain`. Independent-domain runs require a different
physical host for each node and at least three failure domains. The verification
artifact records `source_revision`, `complete: true`, `ledger_sha256` and
`measurements_sha256`, binding its full history oracle to the actual retained
artifacts. Retain the detailed checks and physical isolation observations there,
not just these mandatory header fields. The fault schedule records requested and
observed events, timing, exact owned resource IDs and phase boundaries for every
cohort. Infrastructure evidence must still be reviewed before advertising a tier.

## Repository release checks

Run from the repository root with its own writable workspace target directory:

```sh
cargo +1.98.1 format --check
cargo +1.98.1 check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo +1.98.1 check --workspace --all-targets --all-features --locked
cargo +1.98.1 test --workspace --all-features --lib --tests --locked -- --test-threads=1
cargo +1.98.1 test --workspace --all-features --doc --locked
cargo +1.98.1 clippy --workspace --all-targets --all-features --locked -- -D warnings
python3 -m unittest discover -s tools/perf -p 'test_*.py'
```

Feature-specific and external consumer checks in the SDK CI workflow remain
required. The new package runner supplements CI; it is not a substitute for it.
