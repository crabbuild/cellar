<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Application brief template

Copy this template into the team's application repository and replace each
decision field with a named owner, requirement or evidence link. An unfilled
field remains open. Complete the brief before choosing capacity targets or
starting deployment qualification.

| Decision | Team answer |
| --- | --- |
| Application / accountable domain owner | Name and escalation contact |
| Users and business operations | Commands, reads and async completion |
| Cells and local invariants | One row per aggregate and invariant |
| Canonical identity algorithm | Namespace/class/name encoding and collision tests |
| Tenant authorization | Identity source and checks before routing |
| State and growth | Schema, bytes per cell, daily growth and retention |
| Idempotency | Command ID/digest, receipt retention and ambiguous-response protocol |
| Cross-cell / external effects | Participant keys, outbox/workflow, compensation and reconciliation |
| SLO | Indicator, threshold, window and admission/ambiguity accounting |
| Recovery | RPO/RTO, failure/loss model and coherent recovery set |
| Capacity | Offered arrivals, skew, payloads, residents, background work and headroom |
| Deployment | Artifact, topology, effective policy, schema/step compatibility and rollback |
| Operations | On-call, probes, dashboards, complete drain and incident process |

Attach a domain diagram using [cell boundaries](../diagrams/cell-boundaries.svg)
as a starting point, then a tested vertical slice. Define pass criteria before
measurement. Link [production gates](../21-production-readiness.md), the service
contracts, qualification evidence and decision records. The review outcome is
approve / revise / defer with a reviewer, date and concrete scope.
