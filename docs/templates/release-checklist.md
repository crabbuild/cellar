<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Release qualification record template

Record the candidate and link actual evidence. Mark every applicable gate
pass / fail / open, with owner, date and scope. A checkbox or process exit is
not proof of acknowledged-write recovery. Resolve each failed or open gate
before recording a go decision, or document a scoped exception below.

| Candidate | Value |
| --- | --- |
| Source SHA / artifact digest / target | Exact built and measured bytes |
| Toolchain / lockfile / CI run | Build provenance and check links |
| Environment / store version / topology | Actual dependencies and failure domains |
| Redacted effective policies | Admission, residency, route cache, loader, proof and shutdown |
| Approved workload and SLO/RPO/RTO | Traffic, skew, growth, durations, limits and loss assumptions |

| Gate | Status | Evidence / accountable reviewer |
| --- | --- | --- |
| Domain/receipt/auth correctness | Open | Required |
| CI and package verification | Open | Required |
| Mixed-version/schema/workflow compatibility | Open | Required |
| Store CAS/range/pagination and credentials | Open | Required |
| Capacity, saturation and soak | Open | Required |
| In-flight failure and ambiguous reconciliation | Open | Required |
| Independent failure-domain loss | Open | Required for claimed topology |
| Tenant/security and credential rotation | Open | Required |
| Probes/dashboards/alerts/export coverage | Open | Required |
| Backup/restore RPO/RTO drill | Open | Required |
| Handoff, Preserve reload and full stop reports | Open | Required |
| On-call and rollback plan | Open | Required |

Decision: go / no-go / scoped exception. Record decision owner, timestamp,
allowed envelope, exception rationale/expiry and rollout stop conditions.
Retain previous compatible artifacts and failed runs. See
[readiness](../21-production-readiness.md) and
[deployment flow](../diagrams/deployment-flow.svg).
