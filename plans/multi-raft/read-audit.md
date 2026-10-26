<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# MR-08 read-contract audit

Status: DONE at the MR-08 read-contract boundary. The [final evidence](evidence/MR-08.json)
pins the qualified source, executable digests, exact commands and raw output.
It includes all seven named cases, the additional capability/HTTP/peer-fault/
nonmember histories and the requirements review below. Preceding local/network/
HTTP/origin progress records retain their historical source provenance. This is
not completion of the original 17-package delivery goal.

## Read proof and image ownership

Each local query retains one committed immutable SQLite generation through its
callback. The SQL owner verifies the durable engine marker against that exact
pin before capturing its served position. Protocol-only entries may cover the
same image; a marker naming another image cannot relabel it. Minimum-version
and ReadIndex comparisons use the captured cut. The spanning-query test checks
the callback-observed and returned positions while another replica commits a
newer value.

AtLeast accepts an opaque authenticated token. Scope, application digest,
index/term and expiry are verified before comparison. Ordinary elections preserve
the history; incarnation changes reject comparisons. Ed25519 verification keys
come from explicit server configuration. A conditional public registry checks
retention across rotation/restart and cannot supply an otherwise untrusted key.

Linearizable reads require a fresh safe ReadIndex for the invocation. Status,
commit watermarks, source observations and leader hints cannot complete it.
Role/term changes can drop a RawNode read, so resubmission retains the invocation
identity and original deadline. Sequenced status/submission replies cannot
regress a query that has already acquired its barrier or begun execution.

## Query RPC boundary under qualification

Query RPCs have their own listener, peer connection, queues and byte budgets.
A query waiting for LTX/application progress cannot occupy the control/data
connection it needs. Marker-1 query framing, MAC purposes and TLS exporter labels
are distinct from Raft messages. Replies bind the exact request tag, sequence,
reversed route and payload digest. Tokens are independently verified at receipt;
the serving member is derived from the authenticated target identity.

Headers and payload lengths are bounded before allocation. Authorization checks
the full group/history/schema and physical node/member/disk/process routing view
before reading a body and again around execution. Query frames cannot initialize
a group, vote, propose a command or alter membership. The destination forces local
query execution, so a routed request cannot recursively route to another server.

Candidate lists are bounded to eight distinct members with one optional untried
leader hint. Every attempt keeps the same contract, input, token, consistency and
stale-read policy. The remaining deadline is computed after queueing/handshake;
the origin independently retains the invocation deadline. Cancellation closes
the query connection, drops the destination SDK waiter and joins its SQL work.
SQL interruption observes a node-owned atomic flag. Completed responses retain
byte admission until consumed or dropped.

Two additional [peer histories](evidence/MR-08-peer-progress.json) exercise
saturation and delayed network replies.
One holds a real remote callback while 64 further requests compete for bounded
peer admission: both overload refusal and queued expiry must occur, expired
requests never execute after release, and a concurrent command still commits.
The held query retains its older pinned image; a subsequent minimum-version
query covers the new write after admission capacity is reclaimed.

The other warms an existing TLS query connection and holds its returning
ciphertext in a bounded proxy buffer after the destination callback completes.
The original invocation expires while a newer command commits. Releasing the
proxy cannot let that delayed response satisfy a later invocation; a fresh
exchange must return the newer image and exact serving member. These component
histories do not establish arbitrary-node routing or the full release failure
matrix.

## Query origin identity

The [origin evidence](evidence/MR-08-origin-progress.json) pins this boundary's
56 owning cases, 13 Raft unit cases and executable drill. Queries use a dedicated
route representation. The source carries a typed
NodeIdentity (node/disk/process) and label, without a voting or learner member
ID; the target retains its full replica/history/schema identity. A response
reverses the exact outstanding route, and that orientation cannot decode as a
new request. Unit checks reject injected origin members, missing/noncanonical
target members, zero physical identities and replies bound to another origin.
The query route cannot deserialize as the member-bearing consensus route.

The destination checks its complete target identity and live membership, then
accepts either a matching current group member or an explicit namespace-origin
grant. Grants bind cluster/namespace/node/disk/process/label independently of
discovery. Revocation cancels body, query and response waits. Live revisions
retain bounded tombstones; a regrant cannot revive a request holding the old
permission. A different namespace can keep the same pool without sharing access.
Process replacement must agree across remaining references. The revocation unit
history exercises these transitions and confirms that granting queries never
authorizes a consensus frame.

`RaftCellPlacement::QueryRouter` reads and verifies an existing namespace/birth,
installs a query-only view, and creates no member, private executor or image.
Its handle refuses commands, local reads and status. A four-node TLS history
uses a router without query code outside both groups: all three read modes work
in the granted namespace, the other namespace is refused, and minimum-version
and linearizable fallback retain their proof after a candidate is isolated.
After join, reopening its actual engine shows zero groups; no SQLite cell image
was created. A separate startup test refuses absent roots/births before opening
member storage. Consensus routes remain unchanged. Experimental query peers
must upgrade together for the marker-1 hard cutover; no old reader or translation
fallback exists.

These grants are trusted control-plane inputs. Runtime revisions/tombstones do
not persist an operator's policy independently of configuration: restart must
supply the current trusted view. Durable distribution, dynamic placement and
large directory lifecycles remain controller/scaling work in the original plan.

## Live capability and HTTP checks

`ordinary_context::active_ordinary_context_and_storage_are_refused` holds an
ordinary cell invocation open on the same node/domain while a Raft query uses
its captured Context and Storage. Context storage/namespace/background-work
entry points and Storage read/write/SQL/alarm calls are refused. The original
invocation remains active throughout the query and resumes with its state
unchanged; retirement is not the refusal oracle.

The counter's native and Axum adapters normalize explicit consistency, minimum
token, stale-read policy and bounded member attempts through the same path.
Successful responses expose their SDK-issued token and actual serving member.
The HTTP parity case covers all three modes, local/member routing, duplicate or
mismatched options, invalid bounds and signature/contract refusal without
consistency downgrade. The executable drill runs both HTTP paths on all three
nodes before and after joined restart. Minimum-version and linearizable reads
select another replica without fallback, report that member, and observe
the required committed value. Pre-restart tokens remain usable; another cell's
token is refused even with fallback enabled.

## Requirement-to-boundary review

| MR-08 requirement | Owning boundary and observable evidence |
| --- | --- |
| Read-only registration, one pinned committed image, exact served version | `class.rs`, `host_sql/mod.rs` and `ImageApplier::queryable` retain and verify the image/marker. SQL/PRAGMA/ATTACH refusal, live captured Context/Storage, spanning-query image/version checks and callback-capability tests cover the observable boundary. |
| Stale, AtLeast and fresh safe ReadIndex under original invocation/deadline | `query.rs` and the host read controller correlate context/term/status generations. Follower minimum-version, completed-write barrier, minority refusal, leader-change, unpolled interruption and exact pinned-prefix histories pass. Runtime status, freshness and leader hints never settle a strong read. |
| Authenticated history tokens and independent access permission | `version.rs`, the public-key catalog ledger, query framing and namespace-origin grants bind complete history/schema/process identity. Byte tampering, cross-history/schema, expiry/key rotation/restart, ambiguous catalog writes, namespace refusal and live revocation histories exercise the contracts. Native callbacks are trusted; no OS sandbox is claimed. |
| Bounded member selection and same-consistency fallback | `query_route.rs` and the independently owned query transport keep the original contract/input/token/policy and deadline. Candidate bounds, fallback, LTX progress while a query waits, peer saturation/queued expiry and delayed TCP reply isolation are covered. The four-node router proves nonmember ingress without query code or member/image creation; missing root/birth startup is refused. |

The controller must supply current trusted topology and grant configuration on
restart. Dynamic placement/repair, durable control-plane distribution and large
directory/federation lifecycles remain their original MR-11/MR-13/MR-17 work.
This boundary review does not claim those packages or the full delivery goal.

Native Rust callbacks remain trusted and cooperative; the framework is not an
OS sandbox. These local/loopback fixtures do not establish physical power-loss,
zone, production capacity or the remaining S3/archive/recovery/scaling contracts.
