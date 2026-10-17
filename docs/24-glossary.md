# Shared glossary

Use these definitions when reading the guides, designing service contracts or
reviewing an incident. They distinguish cell authority, durability, event delivery
and operational evidence.

![Ownership relationships](diagrams/architecture.svg)

| Term | Meaning |
| --- | --- |
| Application | Compiled Rust handlers and factories registered for a deployment |
| Cell | Business authority with its own durable SQLite state |
| Identity / scope | Persisted addressing derived from namespace, class and cell identity |
| Namespace / stub | Binding that identifies cells / handle that calls one through its owner |
| Owner | Node authorized by the ownership protocol to serve a cell |
| Epoch | Activation authority version; stale work cannot authorize a later activation |
| Node lease generation | Explicit node generation tied to recovery/authority; distinct from application generation |
| Application generation | A loaded set of compiled applications/configuration; old work may drain beside a new generation |
| Execution domain | Node-owned task/service context that retains policy and resource lifetime |
| Event driver | Complete invocation execution, including tracked background work and nested calls |
| Input gate | Controls execution/concurrency around state-sensitive work |
| Output gate | Holds externally visible output until required proof and authority are established |
| Write position | Commit position whose durability the relevant gate must prove |
| LTX | SQLite transaction replication format/mechanics owned by `cellar-ltx` |
| Bucket proof | Durability proof through the configured object-store path |
| Fleet proof | Durability proof through follower disk/log replication, potentially before bucket upload |
| Recovery pointer | Retained reference needed to account for predecessor log history |
| Compare-and-swap (CAS) | Conditional write that succeeds only if the observed storage version still matches |
| Tombstone | Retained expired node-lease record after log sealing; protects recovery pointers and successor generations |
| Fencing | Actual loss of authority reported by the core, with a typed reason |
| Ambiguous command | Client did not receive a conclusive result, so receipt/state must be reconciled |
| Command receipt | Proposed application record binding command ID/digest to its result |
| Resident | Cell currently occupying runtime residency resources |
| Cached route | Idle, unowned routing history retained within `max_cached_routes` |
| Durable wake | Framework discovery of persisted work that needs activation |
| Handoff | Stop mode for a departing logical node that releases ownership for successors |
| Preserve | Stop mode for the same logical node to resume with retained local state/ownership |
| Clean reload | Framework-certified preparation used for preserving same-node startup |
| Shutdown proof | Returned phase evidence; process exit and task retirement alone are insufficient |
| Service-level indicator (SLI) | Measurement of a service outcome, such as successful requests or completion latency |
| Service-level objective (SLO) | Approved target and measurement window for an SLI |
| Recovery point objective (RPO) | Maximum acceptable data loss, measured against the client command ledger |
| Recovery time objective (RTO) | Maximum acceptable time to restore business readiness and reconcile state |
| Qualification envelope | Tested workload, limits, topology, artifact and failure assumptions for a claim |
| Aggregate | Proposed application grouping of data needed for a local business invariant |
| Transactional outbox | Recoverable event obligation committed atomically with business state in the source cell |
| Stream | Ordered retained event history with a stable resource identity and resumable cursors |
| Subscription | Named consumer checkpoint with leased processing authority over a batch |
| Inbox receipt | Consumer-side deduplication record committed with its local business effect |
| Saga / compensation | Application protocol for coordinating steps across cells or external systems and correcting completed effects |

See [architecture](03-architecture.md) and [sources](sources.md). Avoid using
“exactly once,” “quorum,” “sandbox,” “bounded memory,” or “production ready”
without specifying the scope and evidence supporting that claim.
