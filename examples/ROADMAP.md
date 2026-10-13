# Complete example set for application builders and teams

The runnable catalog is the foundation. Large distributed applications also
need application-specific failure tests, capacity limits and operating
procedures. This is the proposed path to full reference applications.
“Implemented” means a working teaching recipe, not a production-readiness claim.

| Area | Runnable foundation | Next example and acceptance checks |
| --- | --- | --- |
| Getting started | hello, webapi, body, assets, counter, router, rpc | Standalone starter crate with API schema and typed client. Build and package without copying this workspace. |
| Storage and search | SQL guestbook, KV TTL, Blob, vec0 | Document service with migrations, session bookmarks, pagination, conditional Blob writes, streaming/multipart uploads and vector updates. Restart at commit boundaries; verify old/new schema readers. |
| Tenancy and identity | Tenant accounts and atomic receipts | JWT/OIDC claims resolver, per-tenant quotas, secret provider and audit records. Reject cross-tenant calls and bound a noisy tenant. Remove teaching tokens. |
| Durable jobs | Alarm, cron, retry/DLQ, outbox | Webhook ingestion/delivery with signatures, dedup, backoff, retention and DLQ replay. Fail before/after send and ack; receiving effects remain idempotent. |
| Long-running work | Report workflow with stable Blob key | Order fulfillment saga with inventory/payment cells, compensating commands, approval events, cancellation and versioned journals. Restart at every transition; reconcile terminal outcomes. Step rollback handlers are currently rejected; compensation belongs in application logic. |
| Realtime | Hibernating WebSocket room | Collaborative document/chat with sequenced durable events, reconnect cursor, replay and slow-reader limits. Reconnect after owner loss and recover missing acknowledged events. Hibernation does not preserve a dead network connection. |
| Team composition | API and cohosted ledger | Independently versioned API, ledger and notification contracts, compatibility tests and staged rollout. Exercise old/new generations together. The current example deploys one executable/package. |
| Plugins and facets | Compiled loader, copied environment, loaded facet | Trusted plugin registry with quotas, cancellation, cleanup and telemetry. Test handle retirement and loader limits. Native code is trusted in-process code; capability limits are not an OS sandbox. |
| Containers | Rust HTTP peer and bounded fixed-tool exec | Build-job service with uploads, cancellation, inactivity timeouts, streaming output and resource policy. Kill a cell/node mid-job and reconcile durable state. Container disks are ephemeral. |
| Agents | Bounded deterministic native tool loop | Provider-neutral model HTTP adapter, tool allowlist, human approvals, token/cost/time/step budgets and repeatable tool effects. Mock provider failures locally first. PiHarness/OpenCode parity is not implemented. |
| Embedding | Standalone Node host and measuring fleet host | Host with captured limits, metrics, cancellation and outcome classification. Prove executors outlive node join; forced cancellation reports incomplete drain. |
| Fleet operations | RustFS three-node recovery/handoff harness | Multi-machine deployment with TLS, peer authentication, object-store credentials, membership and dashboards. Verify fencing, takeover, preserving restart and acknowledged writes during rolling upgrades. |
| Capacity and fairness | Explicit embedding limits; bounded recipe inputs/batches | Mixed workload with hot cells, independent arrivals, tenant overload, alarms, queues and large storage. Publish throughput, latency, memory and backlog together; bound receipts/dedup retention and enforce admission. |
| Disaster recovery | Acknowledged-write recovery checks | Backup/restore rehearsal, export/import contract, corruption handling and recovery objectives. Restore a fresh isolated fleet and compare all acknowledged state. Applications start with fresh Cellar state. |
| Observability | Host logging and measured results | Spans across HTTP/RPC/queue/workflow, metrics, audit events, dashboards and alerts for retries, fencing, backlog and incomplete shutdown. Compare counts with injected failures. |
| HTTP resilience | Bounded bodies, method checks and streams | Authenticated egress, deadlines, cancellation, circuit breaker and TCP/TLS adapters. Test disconnects and slow peers. Cache currently validates input but always misses; implement a cache adapter before advertising a caching recipe. |

Prioritize webhook delivery, schema evolution, realtime reconnect/replay and
saga recovery. These expose application failure boundaries that API demos alone
cannot establish. Then complete team rollout, observability, model/tool
integration and deployment runbooks against the same reference applications.

Each new example should include Rust source, a checked project config, run
commands, expected responses, ownership and durability semantics, prerequisites,
cleanup instructions and a bounded behavioral smoke scenario. CI should compile
all sources and run local scenarios; container/fleet scenarios need isolated
infrastructure and explicit acceptance checks. Reuse existing SDK tests instead
of simulating success inside example code.

A durable cell transaction can make a local effect idempotent. Output gates
prove required writes before acknowledgement. Queues and external side effects
still need stable operation keys and recovery logic; a network send is not an
atomic cross-cell transaction. Framework correctness, application behavior and
infrastructure capacity each need evidence before production deployment.
