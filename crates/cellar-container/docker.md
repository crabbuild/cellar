<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Bounded Docker execution through the controller

Purpose: Define physical process ownership for the shared-group native Container adapter.

`controller::docker::DockerBackend` implements the existing
`ExecutionControllerBackend` contract against an explicitly selected private
Linux Engine socket. Install a trusted scaffold image by immutable local
`sha256:` ID before starting the service. The backend never pulls an image or
selects an ambient Engine. CPU, memory, swap and process limits are explicit.

The container root is read-only and runs only the fixed `/bin/sleep` scaffold
with a fixed environment. Application commands run through bounded
`Container::exec_output` or the named controller provider. Their environment is
applied to the admitted command, never to the scaffold. A separately labeled
volume at `/workspace` survives ownership fencing and retained backend restart.
Custom root entrypoints, Internet access, inactivity timers, interactive exec
and port sessions are outside this profile and refuse admission. The provider
does not advertise its unsupported operations.

Every higher execution claim first persists an inactive high-water mark, stops
and joins the entire old process namespace, joins already-started Engine I/O,
and scans again for a Create that completed during revocation. This includes
detached application children. Only the fixed infrastructure scaffold can
restart before the new claim becomes active; no application command or
entrypoint is replayed. Each physical call checks the exact original deadline,
claim, immutable Engine resource ID and generation/owner labels. Admission
captures a monotonic deadline once, so a wall-clock rollback cannot extend it.

Physical operations and their original outcomes live in an independent,
checksummed, synced journal with exclusive ownership. The job keeps its journal
and admission after an RPC is cancelled. Deadline expiry stops old processes
and joins the original I/O before releasing the job. A lost reply can be
reconciled from its exact retained outcome. Unknown create, command or write
outcomes are never inferred from a missing response or replayed. Missing,
corrupt, incompatible or ambiguous mutation state refuses reopening.

An expired controller submission may have been cancelled before reaching this
backend. Reconciliation serializes with physical admission and checks the exact
current fence and original history. If the original operation is absent, it
syncs an immutable `ExpiredBeforeSubmission` receipt before returning that
proof. Delayed delivery and retained restart must encounter the same receipt.
A live absent request remains uncertain. Any existing physical operation keeps
its original outcome, including unknown state; container absence or expiry
cannot turn it into a refusal or authorize replay.
Existing receipt reads remain concurrent with physical admission and fencing;
only sealing absence takes that admission lock and syncs a new record.

Backend shutdown closes admission and attempts process termination, job joining
and a final process scan even if journal deactivation or an earlier cleanup
step fails. A storage failure is still returned after these attempts; successful
process termination does not acknowledge a failed durable transition. Stores,
original outcomes and workspace volumes remain retained.

Keep both controller and backend stores outside voter disks. Reopen with the
same options and original controller ID; do not create replacement stores to
clear uncertainty. A changed Engine identity refuses admission. Fresh cell
incarnations additionally require the host's `DockerHistoryFence` authority to
independently establish permanent old-host revocation, including prevention of
restart. Local process termination is insufficient. Pending or failed history
fencing leaves both histories blocked.

Register the backend with `ExecutionController`, then pass its
`ControllerProvider` into `NativeContainerAdapter` and
`RaftClass::container_adapter`. Use the same explicit body/deadline policy in
the backend, controller and class. Use the existing authenticated controller
endpoint when the service is outside the voter process. Private Engine access
must remain exclusive to that service.

Quorum protects the cell's SQLite state and source admission. The workspace
retains the provider's separate storage, disk-capacity and recovery contract;
it is not automatically replicated to three voters or archived to S3. Normal
`SandboxCatalog` uses explicit shared-group parent and execution classes. Declare
`sandbox_catalog()` on the parent and install this adapter before selecting
`sandbox_execution()` on a dedicated child class. Register the child through
`Registry::raft_class`; declare its namespace and binding with an explicit
minimum durability. Ordinary RPC/fetch cannot invoke the execution class.

Each named generation gets its own deterministic child identity. Private calls
bind the source history, parent nonce, generation, target name and immutable
execution policy. Both namespaces prove their own minimum before effects or
results. Changed or missing history pins refuse admission. This does not yet
provide managed fresh-history Sandbox recovery.

High-level shared `SandboxCatalog::start_child` proves its reservation and exact
child-call intent before target I/O. After verifying the durable child receipt,
it stages Ready together with private binding completion. The next gated source
effect, explicit checkpoint or final command output proves that source metadata;
an unavailable final S3 proof still refuses public success. Explicit
`reserve`/`confirm_ready` calls and Fleet retain their separately awaited
checkpoints. Receipt-only recovery must preserve the original allocation without
replaying the callback or provider Create.

Parent cleanup runs bounded shared work independently of business construction.
It retains exact deletion identities and confirmation debt until a mandatory
source proof covers the terminal parent revision. Cleanup retains the strongest
requested minimum across the parent lifetime, including explicit S3 requests
in Quorum-default namespaces. Discovery projects that minimum; stale Quorum
work refuses before claiming the wake or reserving an allocation. A Quorum
terminal confirmation cannot settle a newly strengthened S3 obligation.

Shared cleanup retry uses the exact current native-work lease. Reason,
generation, attempt, due time and claimed state must match, and the lease must
remain unexpired before installing a successor wake. The allocation retry and
successor are written atomically; completing the old claim cannot clear the new
generation. A short lease does not inherit a fixed 30-second delay, and a long
lease cannot admit its successor early. Retrying discovery grants no permission
to replay an unknown physical operation or bypass an input/output proof.

Retained child/parent
tombstones prevent delayed Start from reopening retired generations. Immutable
`QueryContext::sandbox_parent`, `sandbox_allocation` and
`sandbox_cleanup_pending` observe that state without constructing a business
cell. Shared `request_erasure` remains refused; physical namespace erasure and
identity/receipt garbage collection require their separate managed protocol.

The opt-in `native-docker` qualification suite includes a physical backend
restart/fence case, a journal-failure shutdown case and public native RF3 recovery and archive-refusal drivers
against real RustFS. See the [runner instructions](../../tools/multi-raft/README.md)
and [CG-17 progress](../../docs/design/multi-raft/cell-groups/progress.md) for
executed results and scope. Implementation and type checking alone are not a
passing qualification.
