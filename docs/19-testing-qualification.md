# Testing and deployment qualification

Test application invariants and lifecycle behavior, then qualify the exact
release artifact under the intended workload and failures. Continuous integration
(CI) checks establish repository behavior; deployment qualification also requires
workload and failure evidence.

![Qualification in team delivery](diagrams/team-delivery.svg)

## Repository checks

Run from the repository root with the private workspace-volume target directory
required by [AGENTS.md](../AGENTS.md). SDK CI selects Rust 1.98.1 at this baseline.

```sh
cargo format --check
cargo check-layout
python3 tools/docs/check.py
python3 tools/check_native_dependencies.py
cargo check --workspace --all-targets --all-features --locked
cargo test -p cellar --test native_cli --test native_artifacts --locked
cargo test --workspace --all-features --lib --tests --locked -- --test-threads=1
cargo test --workspace --all-features --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
python3 -m unittest discover -s tools/perf -p 'test_*.py'
```

Run independent test cases serially, as CI does. Node fixtures own several
concurrent runtimes and short leases; unrelated fixtures competing for the
runner can expire those leases. Concurrency inside each fixture stays enabled.

Choose existing tests for the changed binding/host path. The suite includes
storage/transactions, domains, cells, call ordering, Queue/Workflow, sockets,
loaders, packages, nodes, logs and replication. `cellar_internal_tests` requires
external simulation/conformance sources. Do not enable it without those
sources or claim those tests ran in an ordinary build.

## Application test layers

| Layer | Required behavior | Evidence |
| --- | --- | --- |
| Domain | Invariant, canonical identity, schema and receipt rules | Unit/property tests with edge cases |
| SDK integration | Context/epoch invalidation, concurrent calls, rollback/cancellation | Existing harness plus application tests |
| Contract | Old/new clients, errors, auth, duplicate command payloads | Consumer/provider fixtures |
| Lifecycle | Old-generation work, `wait_until`, nested calls, stop/fence outcomes | Reports with pending counts and proof phases |
| Recovery | Acknowledged and ambiguous commands, alarms/workflows, new writes | Durable client ledger and reconciled state |
| Load | Independent arrivals, skew, growth, streams, saturation, soak | Limits, percentile method, resources and failures |
| Security | Tenant escape, malformed peer identity, secrets/rotation | Negative tests and review record |

## Fleet drill method

Use the isolated Docker/Colima harness in [tools/perf](../tools/perf/README.md)
as a starting point. Build a private image/cache for this checkout, record its
source hash, run harness proof guards, then run selected bucket/fleet scenarios.
The harness owns disposable credentials, network and local data images; never
point it at production storage.

For release qualification, expand the matrix to independent failure domains,
network delay/partition during writes, object-store outage, ambiguous CAS,
credential expiry, disk full/stall/loss, hot cells, growing state, independent
arrivals, long soaks and rolling mixed versions. Track a durable client ledger
before/after sends so incomplete responses remain distinguishable from
acknowledged commands. Use unique IDs and inspect receipts after recovery.

Require every acknowledged effect to be present exactly as the application
contract defines, all ambiguous commands to be resolved or explicitly retained
for reconciliation, and new post-takeover writes to succeed. A successful
process exit does not replace the complete SDK shutdown proof.

## Report and acceptance

Record source SHA, image/executable digest, toolchain, store/version,
infrastructure, effective configuration, workload, offered/completed traffic,
run duration, failure injection point, raw evidence, reconciliation and stop
reports. Preserve failed runs. Qualification thresholds must come from the
application's approved SLO/RPO/RTO and loss model; no universal thresholds are
invented here. Link this evidence in [production readiness](21-production-readiness.md).
