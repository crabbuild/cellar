<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Qualification and evidence contract

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

Use existing `tools/perf/raft_fleet.py` for managed-node provisioning patterns and
`tools/perf/process_recovery.py` for native process SIGKILL/erasure proof. Extend
their shared owners; do not implement a competing fake object-store or voter
fixture. Own resources by immutable IDs, run labels and exact paths. The destructive
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
