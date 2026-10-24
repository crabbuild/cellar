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
