<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Production readiness and evidence ledger

Qualify the exact application, executable, store and infrastructure before
making production commitments. The repository contains local test and recovery
evidence; the tables below record its scope and the release gates still needed
for your deployment.

![Release qualification loop](diagrams/team-delivery.svg)

## Evidence available at the documented main baseline

| Evidence | Scope | Limit |
| --- | --- | --- |
| Checked-in native SDK CI/tests | Contracts, bindings, packaging, domains and lifecycle test surfaces | Presence is not a claim this doc change reran every test |
| 1,000-cell embedded report | Local embedding and recovered acknowledged writes | Earlier failures and bounded environment retained in report |
| 10,000-cell reference | All cells written; 34,230 / 23,314 final acknowledged writes; 14 complete stop proofs | Earlier image; one VM/disk/RustFS; small overwrites; between-phase failures |
| 2026-10-12 zero-cache 1,000-cell report | Exactly 64 tracked records; 9,284 acknowledgements recovered; Preserve proofs | Narrow routing/recovery scenario, not whole-process memory bound |
| [Large-cell recovery work](../plans/multi-raft/010-large-cell-recovery-time.md) | Full-size pilots, source-bound smaller trials, recovery phase ledger and strict analyzer | Not a production 10 GB percentile or hard-crash qualification |
| This documentation set | Guides, source map, interactive diagrams and readiness criteria | No deployment certification or claimed production benchmark |

Read [sources.md](sources.md) for direct reports and source/image provenance.
Evidence from another branch or an earlier image cannot silently qualify the
current release. In particular, the 10,000-cell image predates route-cache and
typed-fencing changes and did not test ambiguous acknowledgements.

## Release gates to complete

| Gate | Required evidence | Accountable owner | Current state |
| --- | --- | --- | --- |
| Application correctness | Stable identity, concurrent invariants, receipts, replay-safe effects | Domain team | Application-specific; not supplied |
| Artifact and dependency quality | Exact SHA/digest, locked CI checks, provenance and package validation | Release | Must run for candidate |
| Protocol/state compatibility | Mixed versions, workflow replay, schema migration and rollback | API/domain | Application-specific; not supplied |
| Store and authority contract | Conditional/range/pagination behavior, ambiguous CAS, credentials | Platform | Production backend not qualified here |
| Capacity envelope | SLO under independent arrivals, hot skew, growth, saturation and soak | Performance | Beyond local reference remains open |
| Failure resilience | In-flight crash/partition, store outage, disk failure and fencing | Quality/platform | Release-image matrix remains open |
| Topology resilience | Intended zone/host/store loss model and correlated failure tests | Platform | Independent zones/regions not proven |
| Security and tenancy | Threat model, public auth, isolation, least privilege and rotation | Security | Application/environment review required |
| Observability | Business receipts, proof/outcome signals, alerts and export coverage | SRE | Integration required |
| Recovery / backup | Coherent recovery set, drill reconciliation, measured RPO/RTO | SRE | Production procedure not qualified here |
| Rollout / shutdown | Handoff, Preserve reload, complete returned reports, new writes | SRE/release | Bounded evidence; candidate drill required |
| Operational ownership | Named on-call, escalation, runbooks, approvals and incident practice | Engineering lead | Team adoption required |

“Open” does not mean the implementation necessarily fails a gate; it means
this baseline does not provide evidence to claim it has passed for your release.
For each gate, record pass/fail/open, the evidence link, artifact digest, date,
reviewer and scope. Assign application-specific thresholds before the run.

## Ten-gigabyte cell-state release gate

The 10 GB recovery implementation is not yet qualified for a production RTO
promise. Complete-local extent restoration and bounded streaming are implemented;
their passing pilots do not close the remaining implementation and qualification
work. [Plan 010](../plans/multi-raft/010-large-cell-recovery-time.md) records the
source identities, failed attempts, measured intervals and proposed targets.

| Remaining gate | Evidence required before release |
| --- | --- |
| Current-artifact full-size recovery | Repeated peer and real object-store restores of 9,999,998,976 bytes, with correct payloads, new writes, receipts and complete phase ledgers; failed trials remain counted |
| Crash and authority envelope | Hard process loss and erased-disk recovery at the relevant persistence boundaries, corrupt/missing data, stale grants/owners, lost quorum, disk exhaustion and immutable retries |
| Activation before full hydration | A distinct installed-root durability proof, persistent remote references and safe GC, verified demand loading and incremental first writes while hydration is paused |
| Recovery interference and maintenance | Bounded source/replay work and foreground impact under concurrent recoveries, plus verified warm handoff before planned maintenance |
| Candidate and operations | Green locked checks on the exact artifact, declared hardware/store/workload/cache/concurrency envelope, operator visibility and a rehearsed recovery/rollback procedure |

The proposed complete-restoration target is observed p95 at most 120 seconds.
The proposed incremental-activation target is at most 30 seconds for the declared
hot workload; it additionally requires the installed-root and retention work.
Neither target is a service promise until its reference-profile and failure
gates pass. Keep full hydration and broad-scan latency separate from activation.

The private writer can now use a retained source without first materializing its
base. Its 10 GB data-access test fetches two extents (4,972,544 bytes) for three
counter/receipt transactions and their duplicates. This does not establish a
durable remote reference or permit native activation: installed-root, GC and
restart proofs remain open. Native extent writers also refuse unexpected
whole-image capture fallback and require a rebuild instead.

The verification-only ownership registry adds persistent references, pending
reservations and exact retry intents. It is not yet connected to native markers,
generation-addressed object storage or a collector. Metadata ownership tests
cannot close the partial-hydration or remote-GC release gates.

The ordinary daemon process fixture covers two small counter groups, retained
disk, an actual killed worker, receipt reconciliation and post-restart writes.
It does not replace the full-size crash campaign. Enable authenticated
node-directory refresh when restarted nodes advertise new process generations or
endpoints, as described in the [Raft host guide](../crates/cellar/durability/raft/README.md).

Capacity preflights belong to the test setup: an unavailable run is not a timed
recovery sample, and the fixture's aggregate disk requirement is not the logical
per-cell quota. Qualify on the intended deployment rather than transferring a
shared development host's timing to another storage or network profile.

## Proposed adoption milestones

**Builder milestone:** counter runs, one authenticated/idempotent vertical slice
has tests, and identity/schema/contracts are documented.

**Team staging milestone:** secure fleet, finite limits, dashboards, mixed-version
rollout, full stop proofs and a recovery drill with reconciled state.

**Production milestone:** all applicable gates above pass on the exact artifact
and proposed infrastructure, with business-approved SLO/RPO/RTO and loss model.
Any exception records scope, accountable owner and expiry; it cannot waive
acknowledged-write integrity or fabricate a durability proof.

Use [the release checklist](templates/release-checklist.md) for the concrete
go/no-go record. A supported scale statement must name its workload envelope,
topology, limits and evidence rather than declaring an arbitrary node/cell count.
