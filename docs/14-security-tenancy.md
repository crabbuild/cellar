<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Security and multi-tenant application design

Design public authentication, business authorization and tenant isolation in
your host and application. Cellar authenticates peer traffic and checks runtime
authority, but native application code executes as trusted code in the host
process. Use this guide to review each trust boundary before deployment.

![Trust and failure domains](diagrams/fleet-topology.svg)

## Threat model and controls

| Boundary / threat | Proposed control | Acceptance test |
| --- | --- | --- |
| Public caller reaches another tenant | Authenticate first; derive tenant server-side; authorize each command/read | Cross-tenant ID and forged route attempts |
| Untrusted peer reaches internal ingress | Private network plus `PeerAuth`/probe identity; validate custom transports at both ends | Missing/wrong credential, identity and protocol |
| Credential theft or excess authority | Least-privilege store/telemetry permissions; provider-managed credentials | Expiry, revocation, rotation and denied prefix tests |
| User URL accesses internal services | Host egress policy, redirect and resolution validation | Local/private destination and redirect attempts |
| Noisy tenant exhausts resources | Edge quotas, measured tenant fairness, finite event/body/loader budgets | One tenant overload while others receive service |
| Logs reveal customer data | Redaction and minimal payloads, access/retention policy | Automated secret/PII fixture checks |
| Compromised artifact changes application | Verify digest/target/protocol; protect build and promotion authority | Corrupt/wrong-target artifact rejected |

Peer authentication is not end-user authentication. A route cache, cell name,
namespace or structured capability is not evidence that a public caller is
authorized. Validate authority at the service's business boundary and avoid
trusting client-supplied tenant IDs. Keep error messages from revealing another
tenant's existence or state.

## Isolation choices

Cells provide state/ownership separation. They share host processes and
infrastructure unless deployed otherwise. Runtime quotas are per cell, host or
script pool, not a built-in enterprise tenant quota system. Define admission
fairness and billing separately. Use distinct fleet credentials, processes or
infrastructure for stronger customer/regulated boundaries when required.

Compiled loaders and facets are lifecycle mechanisms, not a demonstrated sandbox
for hostile code. Do not accept arbitrary tenant native binaries in a trusted
process based only on loader budgets. An untrusted-code product needs a separate
isolation design and security review.

## Operator and data access

Separate artifact publication, fleet operation, database inspection and
credential administration roles. Use authenticated owner-routed SQL/KV/Queue
operations; direct mutation behind a live owner bypasses authority/durability.
Keep internal listeners out of public ingress. Leave forwarded-header trust
disabled unless an authenticated edge controls those headers.

Document encryption and key ownership for object storage, local volumes,
backups and transport in the actual environment. No certification, compliance
coverage or universal encryption-at-rest claim follows from these docs. Record
data categories, location, retention, deletion and audit requirements with a
named security owner.

## Review before release

Record the following evidence in the release checklist:

- Trust boundaries, credential holders and privileges
- Rejected public and peer requests with invalid authority
- Dependency and license review
- Credential rotation during requests and recovery
- Secret and customer-data checks for logs and backups
- Tenant quota behavior under load

Sources: [peer authentication](../crates/cellar/fleet/peer/auth.rs),
[probe signing](../crates/cellar/fleet/peer/probe.rs),
[Node validation](../crates/cellar/node/embedded.rs),
[artifact protocol](../crates/cellar/deployment/protocol.rs).
