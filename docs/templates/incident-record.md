<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Incident and recovery record template

Preserve observations before changing authority or deleting state. Keep secrets
and customer payloads out of the incident record. Complete the timeline and
command reconciliation before closing the incident; assign an owner and due
date to every follow-up.

| Field | Required content |
| --- | --- |
| Incident ID / lead / severity | Business-impact classification and accountable responder |
| Start/detection/containment/recovery times | Timestamp with timezone and evidence source |
| Affected artifact/environment | SHA/digest, effective policies, store, topology and nodes |
| Customer impact | Failed, delayed and ambiguous operations; affected tenants |
| Framework outcome | Exact `NodeOutcome`, typed fence reason and relevant errors |
| Drain evidence | Every phase, pending events/dispatches, timeout/deadline and stop mode |
| Recovery set | Lease/log/LTX/local/object evidence preserved and access location |
| Command ledger | Acknowledged versus ambiguous; receipt reconciliation method/results |
| Actions | Ordered commands/changes, owner, intent and observed outcome |
| Resume criteria | Valid authority, reconciled state, business probes and new writes |
| RPO/RTO outcome | Measured against agreed objectives |
| Follow-ups | Root-cause evidence, test/runbook changes, owners and dates |

Record uncertainty explicitly. A lost report channel is not evidence of a fence;
forced closure is not a graceful durability proof. Link
[operations](../17-operations.md), [recovery](../18-recovery-backup.md) and
[recovery flow](../diagrams/recovery-flow.svg). Review which release gate would
have detected the failure and update that evidence requirement.
