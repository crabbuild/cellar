# Service and cell contract template

Use one contract per owned API boundary. Consumers should be able to implement
retries and compatibility without reading the provider's internal code.

| Field | Required content |
| --- | --- |
| Owner / consumers / escalation | Named team and supported dependency versions |
| Address | Script, binding, class, canonical cell identity and tenant derivation |
| Authorization | Caller identity, permissions, negative cases |
| Operations | Fetch route/method or RPC method with request/response schemas |
| Errors | Validation, conflict, unauthorized, admission, unavailable and ambiguous outcomes |
| Idempotency | Command key/digest, receipt scope/retention, mismatched reuse behavior |
| Ordering/concurrency | Local invariant, expected-version semantics, await/transaction behavior |
| Deadlines/retries | End-to-end budget, retry owner, attempt/backoff policy and status lookup |
| Limits | Payload/body, request, copied properties and pagination limits |
| External effects | Receiver keys, outbox/step completion and reconciliation |
| Compatibility | Additive rules, old/new versions, schema and workflow-step changes |
| Observation | Business result ledger, SLI and low-cardinality dashboard labels |
| Data lifecycle | Retention, deletion, tenant isolation and backup reference |
| Evidence | Contract tests, concurrent/duplicate tests and recovery qualification |

Include examples for success, duplicate command, same-ID/different-payload
conflict, timeout/status resolution and authorization denial. Link the
[routing flow](../diagrams/routing-flow.svg) and [RPC guide](../07-routing-rpc.md).
No receipt policy may imply atomic cross-cell or external side effects unless
that mechanism is explicitly designed and tested.
