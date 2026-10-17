# Team delivery for distributed applications

Assign an owner to each durable boundary, publish its API contract and qualify
changes before rollout. Deliver vertical slices with compatibility tests and
release evidence. Adapt the roles and templates below to your team.

![Team delivery loop](diagrams/team-delivery.svg)

## Assign ownership by durable boundary

| Responsibility | Accountable role | Required artifact |
| --- | --- | --- |
| Cell identity, invariant and schema | Domain team | Application brief and migration plan |
| Service API and retries | API owner | Versioned service contract and compatibility fixtures |
| Host, store, network and executor policy | Platform team | Effective config and infrastructure contract |
| Authorization and tenant boundaries | Security owner with domain team | Threat model and negative-test evidence |
| Workload capacity and failure acceptance | Performance/quality owner | Qualification report on exact release |
| On-call, restore and rollout | SRE/release owner | Runbooks, drills and signed go/no-go record |

A role can be one person in a small team, but each decision needs one named
accountable owner. Shared code does not imply shared database mutation
authority. Consumers call the owner through a contract; they do not edit another
team's cell SQLite state.

## Deliver vertical slices

For an inventory/order example, first deliver authenticated reserve for one
inventory cell, command deduplication, a concurrent invariant test, one deployment
and one restore drill. Then add an order workflow with stable participant
command IDs, receipt reconciliation and compensation. Add read projections and
scale only after correctness evidence exists for the slice.

Keep service dependencies explicit. Avoid circular synchronous call graphs and
hidden shared tables. A team contract includes identity algorithm, payload/error
schema, retry owner, idempotency retention, deadlines, authorization, version
compatibility, data retention, observable business outcomes and an escalation
owner. Use [the service template](templates/service-contract.md).

## Change review

Review application changes for invariants, schema compatibility, hot partitions,
async replay, command ambiguity and authorization. Review framework changes for
core/effect ownership, epochs, output gates, task/resource lifetimes, persisted
protocol and recovery pointers. Review configuration changes for admission,
whole-system resources and the measured failure envelope.

Write an ADR when changing a durable boundary, identity algorithm, externally
visible contract, proof posture, store assumption or migration/rollback policy.
The [baseline ADRs](adr/README.md) record observed framework constraints; they
are not approvals of a team's proposed production architecture.

## Release and operation agreements

Pin the source and artifact tested. Promote the same bytes across environments;
record compatible old/new APIs and state. Give operators a clear stop mode,
grace-period policy, complete proof requirement and failure response. Define who
can halt a rollout, who decides rollback compatibility and who owns recovery
reconciliation. A successful build cannot overrule a failed business or
durability gate.

Use the [application brief](templates/application-brief.md),
[release checklist](templates/release-checklist.md) and
[incident record](templates/incident-record.md) to turn these recommendations
into project-specific decisions. Keep docs with the change: altered public
behavior needs examples/contracts; altered persisted behavior needs migration
and recovery evidence; altered limits need capacity evidence. Complete the
release gates in [production readiness](21-production-readiness.md) before
claiming a scale or availability commitment.
