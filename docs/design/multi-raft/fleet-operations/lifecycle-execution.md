<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Fleet lifecycle execution contract

Purpose: Specify independent maintenance grants, exact-process stop, joined closure, retained return, and verified resume.

[Design library](../../README.md) · [Topic contract](README.md)

| Implementation | Validation or example |
| --- | --- |
| [Code and API](../../../../crates/cellar/fleet/management/raft/mod.rs) | [Checks and usage](../../../../tools/perf/raft_fleet.md) |

The map summarizes this document’s scope; the recorded gates determine completion.

```mermaid
flowchart LR
    A["Separate maintenance grants"] --> B["Exact-process joined stop"]
    B --> C["Retained boot and verified resume"]
```

Status: proposed operating contract for FO-03 through FO-10. This document expands the effect boundaries in [capacity and scaling](capacity-and-scaling.md) and [maintenance and rebalancing](maintenance-and-rebalancing.md). Read [implementation progress](progress.md) for delivered behavior. Neither the examples nor the future operator commands are deployment qualification.

## Who is allowed to change what

Use the object store to coordinate infrequent intent, reservations and results. Use the affected Raft group to commit its membership and durability decisions. Use the local node lifecycle owner to stop its exact process. These authorities must remain distinct even when one controller calls all three adapters.

| Decision | Authority and linearization point | Insufficient evidence |
| --- | --- | --- |
| Register or exclude a node | Successful root CAS referencing the typed node transition | A descriptor object, process start or bucket listing |
| Reserve a move | Root CAS references the exact permit and resource charges | An immutable plan page or successful dry-run |
| Start, cancel or finish membership work | Applied Raft entry with exact fleet binding, sequence and permit | RPC acceptance, timeout or local cached status |
| Declare additional usable resources | Root accepts the verified profile and current process observation | Edited desired profile or a larger requested container limit |
| Stop for maintenance | Local durable stop guard accepts the exact operation epoch/process | Root exclusion, old readiness receipt or closed TCP port |
| Complete evacuation or removal | Terminal root references the checked receipt and retained fences | Receipt PUT or absence of replies from a dead node |
| Provision, resize or destroy infrastructure | External operator/platform action at the specified boundary | A change to Cellar metadata alone |

The generic catalog adapter is a privileged storage mechanism. Typed lifecycle adapters must enforce these transitions; arbitrary projection writes are not operator APIs. No fleet-root read or write is added to the business write acknowledgement path. Archive retention limits can still constrain business admission during an object-store outage.

## RestartReady proof and receipt boundary

- The SDK runtime walk can publish a point-in-time RestartReady assessment with `FleetRestartVerification::publish_ready`.
- Consume every canonical history with owned `reconcile_next` first.
- Inspection-only `verify_next` cannot substitute for observed surviving leadership and matched durable publication on all three current voters.
- The live walk retains the oldest actual voting sample in its catalog owner's monotonic clock; these clock values are never serialized.
- All samples must remain within the existing five-second envelope when the Root CAS is submitted.
- An expired walk must be repeated.
- Large-fleet proof latency still requires qualification within that unchanged bound.

Publication repeats exact quiescence and complete physical inventory, rejects relevant Root drift and unresolved settlement, then atomically journals the exact human request and its candidate digest under `restart-ready/`. Generic catalog writes cannot create, replace or delete that reserved projection. An immutable candidate without a journal-confirmed readiness transition grants no readiness authority.

- `FleetCatalog::restart_readiness` recovers an exact historical receipt after a lost confirmation or controller restart.
- Changed process, principal or request payload refuses.
- Recovery checks the canonical request, committed journal, gate parent and immutable candidate without requiring old observations to remain fresh.
- A recovered receipt is historical evidence, never a live health lease.
- The target remains quiesced in RestartPending, its placement capacity excluded and its shared disruption slot held.
- Cancellation still owns a synced exact process Fence.
- Stop requires separate consent and fresh receiving-owner proof; readiness does not start the repair-delay clock or authorize new boot, capacity resume or slot release.
- The separate stop/join and M8 return boundaries follow; M9 verified resume is a separate SDK boundary described below; the independently granted operator sequence is described below.

## Separate maintenance stop and joined confirmation

`admit_restart_stop` records an exact human stop request referring to its Ready request, node, process and stable child stop identity. This is intent only: the process remains alive and the disruption slot stays held. Retry a lost consent confirmation with that same payload. The `restart-stop/` and `restart-offline/` projections reject generic catalog writes.

- `stop_restart_maintenance` submits that request through authenticated node control.
- The receiving Node owns work beyond the caller's response lifetime.
- It independently renews complete physical inventory and every affected group's actual quorum, publication and surviving matched leadership.
- No serialized health samples are restored.
- A regained primary may request handoff, but that attempt returns unconfirmed until an exact retry observes its surviving leader.
- Normal consensus continues.
- The existing five-second sample envelope and the original caller deadline both bound the final engine admission.

- The receiver checks its own synced quiescence and both reversible barriers.
- It avoids sending itself a quiesce request through the worker currently handling stop.
- The maintenance-specific engine update requires that exact quiescence, boot, epoch and operation; the ordinary evacuation stop update cannot substitute.
- A Fence or newer guard defeats a queued old stop.
- Accepted shutdown remains permanent and cannot be cancelled or applied to a later process.

`restart_stop_closure` verifies the exact signed Node/domain/engine join. A missing response, closed port or raw process kill is insufficient. Only then can `confirm_restart_offline` journal that joined outcome; `restart_offline` recovers the fact across controllers. Lost successful confirmations reconcile before current live prerequisites. These receipts grant no fresh boot, capacity, resource release or disruption-slot release.

- Stop consent pins the Ready inventory basis; relevant Root drift before execution refuses.
- `refresh_restart_stop` accepts separate human consent naming the exact previous consent and a fresh owned Ready assessment.
- It requires the same operation, gate, boot, process and physical stop child.
- A Root CAS replaces only the current consent projection; a competing refresh or changed input cut refuses.
- An uncommitted original request cannot anchor a refresh.

- The original admitted consent fixes the physical selector's finalization ID for its entire life.
- Refreshes keep that anchor and validate its canonical committed journal in bounded reads.
- They never walk an unbounded predecessor chain.
- A stop already accepted while a refresh CAS is pending retains its original signed closure; the refreshed consent can confirm that same joined outcome and proceed through retained return/resume.
- No second physical stop scope is created.

- `restart_stop_consent` is a read-only historical reader.
- Exact refresh replay reconciles before current live prerequisites, including after cancellation or physical join, but rejects a changed predecessor payload.
- Neither reader nor refresh restores live evidence or releases capacity/the disruption slot.
- The receiving Node still renews current physical/quorum proof, and a synced Fence still defeats delayed stop.
- The protocol adds `/stop-consent-refresh-1`; markers stay at 1, with matching binaries/fresh prefixes and no legacy reader.
- The M8/M9 SDK return and verified-resume boundaries below are delivered.
- Grace/rolling/second-failure handling remains delivery work.
- The intact maintenance SDK/admin/CLI sequence now composes these boundaries.
- Use matching binaries and fresh data prefixes for the current protocol fingerprint.
- Numeric markers remain 1.

## Independently granted maintenance operator sequence

- The Root-driven operator composes M1–M9 through bounded durable child references.
- The original prepare request fixes its node, full boot, grace and birth decision.
- Separate authenticated human stop, return and resume grants are journaled before any corresponding SDK effect.
- They do not themselves stop, boot, open admission or restore capacity.
- Stop fixes the original process and physical stop child; return fixes a distinct process on the retained engine; resume fixes the actual returned boot, intended resource profile and resume child.

- The Node-owned service repeats fresh Ready/stop proof where necessary, retains one immutable physical stop scope across consent refresh and confirms only the actual signed joined closure.
- Canonical offline state admits the next human return grant even if the worker has not advanced its next checkpoint.
- The platform starts the retained engine separately under M8; read-only return context is deployment data, never startup authority by itself.

- The resume driver retains only an ephemeral current-voter proof walk.
- A stopped controller restores canonical child identities from Root and repeats observations.
- M9 consent precedes receiver-owned reopening.
- Fresh terminal verification then restores Active capacity and releases the original shared disruption slot.
- Only after actual M9 completion does a separate terminal operator CAS remove its own active pointer.
- Historical reads neither execute effects nor delete later work.
- Checkpoint records contain bounded references rather than copied attestations or samples; the existing 4-KiB value bound and five-second proof envelope remain.

- If cancellation wins before stop executes, its exact synced fence blocks delayed shutdown.
- If actual joined stop wins, the driver confirms M7 and journals a cancellation refusal bound to that signed closure.
- Refusal releases nothing and permits return/resume to continue.
- A timeout, closed port or missing reply cannot produce this refusal or an offline fact.
- An incorrect unexecuted resume profile can be replaced by a new human request before SDK consent admission; afterward the intended scope is immutable.
- Failed live checks with unchanged metadata and no staged mutation back off without churning Root.
- Root drift or a staged uncommitted child is fenced before a new child identity is used.

A stale discovery hint at transport preflight reports the typed `Observation(StalePeer)` failure. It has not invalidated Ready or the admitted physical scope, so the owned service keeps the exact consent/checkpoint while discovery catches up. Actual receiver/root/epoch conflicts still require re-verification. Missing responses still retain unknown-outcome semantics.

- The protocol adds `/extent-owners-1/operator-lifecycle-1`, with markers still 1, matching binaries and fresh data prefixes.
- Managed namespaces and imported fleet histories currently require materialized images.
- Main's extent-backed image commitments cannot substitute for the existing materialized fleet publication and physical archive proofs; binding, Root validation and declaration reject that unsupported combination.
- Ordinary extent-backed Raft recovery remains available.
- Extent-aware fleet proof support, grace expiry, rolling orchestration, second-failure handling and resize remain required.

## Retained-engine return and startup exclusion

- The M8 SDK backend provides retained-engine return.
- `admit_restart_return` accepts separate human intent bound to an exact signed offline confirmation, predecessor process and distinct intended process.
- It does not change node capacity or release the disruption slot.
- Exact historical consent reconciles from the canonical request/journal before checking live admission prerequisites.
- Generic writes cannot mint or delete the reserved `restart-return/` or `restart-return-boot/` projections, or change the boot/engine of a RestartPending node.

- Only the actual exclusive engine owner consumes this consent during managed startup.
- It verifies the retained node/disk/engine identity and the synced exact predecessor stop.
- Its bounded Root CAS installs the new physical boot and its canonical boot record while the node stays RestartPending.
- A lost startup confirmation retains the original admitted nonce; an exact retry by that exclusive owner reconciles it instead of publishing another boot.
- The intended process and actual signer key cannot be rebound by a retry.

- The serialized engine has a dedicated maintenance boot transition.
- It requires the exact accepted predecessor stop, same physical owner, a distinct process, higher boot epoch and matching operation, then syncs the new boot directly into quiescence.
- An ordinary boot cannot open a maintenance stop or quiescence.
- Maintenance startup holds both reversible admission gates immediately; failed startup retains this hold.
- The exact new selector is installed before endpoint publication, and the SDK admission gate is bound before the peer query gateway is installed.
- Consensus and archive recovery can run while public and SDK application writes remain closed.
- Old-process cancellation and stop cannot affect the fresh boot.
- No permanent shutdown flag is reset.

- M8 startup is not an intact-restart completion certificate.
- The verified resume boundary follows.
- A subsequent process death, changed signer after a failed startup, grace expiry, rolling execution and operator workflow reconciliation remain explicit delivery work.
- No old live sample or serialized clock is revived during return.
- The protocol fingerprint adds `/restart-return-1`; numeric markers remain 1 with matching binaries and fresh prefixes, without migration or legacy readers.

## Verified returned-boot resume

- `prepare_restart_resume` creates a live bounded walk over the complete current namespace/history cut and the exact returned boot.
- A new human request names its return consent, stable resume child and intended open-profile deployment attestation.
- The physical observation must actually report paused; only that paused bit is normalized for comparison with the planned profile digest.
- Preparation refuses incomplete inventory or touching births, reservations and movements.
- It grants no resume or capacity authority.

- `reconcile_next` compares the structural pre-stop inventory with current settled membership and actual physical occupancy.
- It never recreates a retired member or treats retained occupancy as free capacity.
- Each affected live history needs current RF3, actual safe ReadIndex/publication evidence on all three voters and matched progress at the serialized leader.
- The returned paused voter can win an election.
- Owned reconciliation requests a matched transfer to an eligible Active survivor, retaining the history until surviving leadership is actually observed.
- In Resumed phase, the returned voter can remain a matched current leader.

- `admit_resume` consumes the complete live walk to persist separate resume consent.
- The receiving Node resolves that canonical consent and independently renews its physical inventory, actual profile and all-current-voter proof.
- It submits an engine update bound to the exact synced quiescence and a deadline on the actual engine owner's clock.
- The update consumes that quiescence atomically and persists the distinct resume child.
- Only then do local reversible public and SDK gates open.
- Permanent shutdown flags are never reset.
- Exact already-synced work finishes even after its request waiter or reply is lost; it grants no new scope or health.

- Physical reopening leaves Root RestartPending, quantified capacity excluded, resource charges retained and the disruption slot held.
- Consume a fresh walk in Resumed phase and call `complete_resume`.
- A single guarded Root CAS installs the exact Active Node, its matching capacity row and terminal completion, then clears only the original disruption slot.
- The terminal record binds the canonical Node change by digest rather than duplicating its boot inside the bounded record.

- A relevant Root input change refuses old consent before physical opening.
- Repeat current verification and use a fresh human identity with the same return/profile/ resume child scope.
- If inputs change after physical reopening, capacity exclusion and the slot remain until renewed consent and fresh terminal proof succeed; recovery does not reclose or reset an already admitted physical effect.
- `restart_resume_admission`, `restart_resume_completion` and `restart_resumed` reconcile exact historical journal facts, including lost confirmations and another controller's completion.
- Reading those facts restores no live samples or clocks.

- All actual voting evidence must fit the existing five-second envelope at effect execution or the Root CAS.
- Generic intent, completion and Active-node projections cannot bypass the private transition.
- The protocol fingerprint adds `/restart-resume-1`, retaining numeric marker 1 and requiring matching binaries and fresh prefixes.
- This SDK boundary does not itself implement the management operation driver, rolling policy, grace expiry or failure classification.

## Persist a resumable operation, not a worker's lifetime

Keep the canonical request immutable. Its operation projection records phase, subphase, last completed cursor, plan digest, child action identities and the next prerequisite. A blocked observation retains the underlying durable phase. Retrying after `StoreUnavailable` must not start again from an empty operation. An action worker or controller disappearing does not cancel its admitted work.

Each reconciliation turn performs bounded work:

```text
read one validated root and bounded operation/proof pages
reconcile already admitted effects before proposing another effect
refresh only the observations needed for the next decision
compute a pure transition using current fences and reservations
CAS its durable intent when a new effect needs authorization
dispatch that exact idempotent effect through its owning adapter
record the observed result, or preserve an unresolved result
persist a bounded cursor and yield
```

Re-read proof-relevant state after CAS conflict. A newly submitted request with a clean non-admission revision conflict needs a new preview/request identity; reconciling an already admitted request retains its original identity. Do not rewrite a request's expected revision to hide an ambiguous first submission.

| Crash or lost response | Resume rule |
| --- | --- |
| Plan pages written, root admission absent | Pages grant no work; reconcile request history before reporting non-admission |
| Permit rooted, group has not consumed it | Retry the exact permit or commit its cancellation fence |
| Group consumed permit, response absent | Read committed membership/progress; continue that plan, never allocate another sequence to guess |
| Learner initialized, status update absent | Reconcile physical owner and persisted target binding; keep reservations charged |
| Group terminal, root release absent | Obtain fresh terminal quorum proof and join/fence physical work before release |
| Stop accepted, stop reply absent | Reconcile local guard/process; keep disruption slot through actual stop/rejoin |
| Candidate receipt written, terminal CAS absent | Revalidate proof inputs and finish the same operation; candidate alone grants no completion |

- The current catalog commit bound is 16 changed records per mutation.
- A 300-move plan is therefore stored through bounded immutable pages and admitted in small batches, never one giant multi-record commit.
- Each batch atomically changes all of its relevant admission, operation and node projections within that bound.
- Plan publication cannot reserve every listed move.
- Keep the separate limits of 16 admitted membership permits and two physically active moves for the initial five-node execution profile.
- A permit's reservation survives queued, uncertain and controller-restart states.

## Add capacity: establish identity before making placement promises

Admission pins node, disk, engine lineage, authentication identity and physical failure domain. If a fresh engine identity is first learned during preparation, registration remains ineligible until that identity has been observed and accepted; it cannot change after acceptance. A repeated display label is not evidence that a different volume is the same node.

Use these acknowledgements in order:

1. **Registration:** CAS creates the excluded descriptor and enrollment epoch.
   Record supported protocol and a digest of compiled namespace/class/schema
   capabilities. Reject duplicate durable identity or incompatible capabilities.
2. **Boot observation:** an authenticated node endpoint reports the current
   process, actual storage pin and configuration, without requiring a business
   cell. Explicit namespace/class declarations establish what an empty host may
   later register. Report retained histories, including unknown ones; a zero
   serving-cell count alone is not an empty-engine proof.
3. **Capacity observation:** verify effective CPU/memory limits, startup pool
   bounds, retained slots/bytes and required storage/object-store/network probes.
   Bind the observation to process/profile and enrollment revisions. Missing
   kernel visibility requires the declared deployment attestation; it is shown
   as such rather than reported as an independently measured limit.
4. **Trust installation:** participating hosts acknowledge the exact trusted-node
   revision. The next move requires acknowledgement from its source leader and
   destination, plus every participant its existing seed/configuration protocol
   needs. Do not block unrelated groups on an irrelevant offline host, or skip
   a participant needed for this move. Revalidate acknowledgements after a
   process/authentication change.
5. **Activation:** CAS accepts the readiness cut and profile, then grants target
   eligibility. Readiness is not a reusable authorization for a different boot.
   If the node fails just afterward, current health checks prevent a new move;
   already admitted work follows ordinary reconciliation.
6. **Distribution:** reserve a group move, lazily register its dormant history
   under the declared class, then seed/catch up/promote. Target registration
   cannot overwrite an old or retired binding. The group still grants its own
   fresh-quorum seed authority.

Registering, activating and balancing have different receipts. An operator can stop after qualifying a spare. `balanced` completion requires actual role distribution and its observation window; `capacity_only` reports unused spare capacity explicitly. An inactive or restarting node can remain authenticated for inspection/catch-up without being an eligible placement target.

## Scale in: pin the destination fleet before moving the first group

- The accepted operation includes an exact keep-set, all departures and the chosen failure-headroom contract.
- For a requested count, the preview chooses victims deterministically; a request naming a particular victim never substitutes one silently.
- The same root admission excludes every departing node from future birth/move targets.
- It authorizes only one node's current evacuation under the shared disruption slot; later departures continue serving their existing groups.
- Represent those later exclusions in the operation's target set, rather than marking every departure as an active evacuation and triggering parallel drains.

Execute each departure with these barriers:

| Barrier | What it closes | What may still run |
| --- | --- | --- |
| Target exclusion | New admissions into any departing node | Existing consensus, business work and previously admitted actions |
| Prior-action settlement | Delayed pre-exclusion births/learners/permits | Quorum-backed replacement of current memberships |
| Membership absence | Every inventory reference to this node, including joint voters | New writes on surviving configurations |
| Physical reconciliation | Reachable owned workers joined; unreachable authority fenced and occupancy retained | Unrelated fleet work within its budgets |
| Terminal proof | Complete inventory cut, required archive cuts, policy and fresh survivor checks | Infrastructure action explicitly permitted by the receipt |

- The complete planner must find both the final placement and a staged execution path, including the final fleet after each declared failure.
- This happens before evacuation starts; each later move also revalidates its local prerequisites.
- Count new births against the keep-set.
- If demand grows beyond the accepted fit, pause further departures and publish a revised remaining plan.
- Do not silently reduce RF, weaken domain policy, or freeze ordinary writes to preserve a preview.

- After each node, verify remaining redundancy and headroom before advancing.
- Cancellation restores eligibility only for nodes whose unresolved capabilities are settled and whose lifecycle permits a return.
- It cannot resurrect retired memberships.
- The terminal cancellation receipt lists completed removals, partial moves, remaining exclusions and the resources still retained.
- Returning evacuated hardware as general capacity requires the fresh registration/volume workflow.

## Vertical changes need three separate resource values

Track `desired_profile`, `effective_placement_profile` and observed platform limits independently. Every profile has a revision and canonical boot digest; every observation also names its process. CPU demand estimates influence the SLO decision; memory, storage, image, queue and slot bounds must fit structurally.

| Boundary | Grow resources | Reduce resources |
| --- | --- | --- |
| Request accepted | Keep old effective capacity; store desired larger profile | Install target exclusion and a prospective lower admission ceiling; retain old physical limits |
| Pre-stop preparation | Settle incoming actions and prepare intact restart | Also move selected leaders/replicas and prove target startup/retained-state fit |
| Stop accepted | Join actual old workers through guarded shutdown | Same; pre-stop RSS is not the final memory-fit proof |
| Process offline | Operator applies larger limits | Recheck persistent occupancy/owned work, then operator applies lower limits |
| New boot | Verify actual limits, full configuration and immutable engine identity | Reject startup if retained state or worst-case configured work cannot fit |
| Rejoin accepted | Publish larger effective placement profile | Publish verified lower effective profile |
| Resume | Admit new work under the accepted new ceiling | Admit new work under the accepted new ceiling |

- The prospective shrink ceiling must govern births, learners and background reservations before the platform is reduced.
- A stale root reader cannot reserve against the old ceiling: typed root admission rechecks the profile/exclusion epoch, and the host rechecks it at initialization.
- Existing admitted work stays charged until settled.
- Consensus and log growth can continue before stop, so a pre-stop sample is conservative planning input; repeat persistent-fit checks after owned shutdown before requesting the external reduction.

- A fit receipt includes fixed pools, decoder/image maxima, retained engine slots, physical data, outstanding owners and required recovery reserve.
- It does not infer memory fit from a quiet RSS sample or disk fit from fewer live voters.
- V1 cannot shrink a filesystem or reclaim a retired history merely to pass this test.
- Use fresh replacement capacity if the retained footprint cannot fit.

- If the new boot fails, retain exclusion and the pending operation.
- Restoring the last known profile requires the platform to provide its limits and the boot to verify them again.
- A failed/OOM boot never advances the effective profile or releases the disruption slot.
- A platform change outside this sequence is an observed mismatch to reconcile, not automatic capacity authorization.

## Durable maintenance preparation controller

- `begin_restart_operator` fixes the original physical boot, bounded grace and birth-resolution choice in a canonical journal and one Root CAS.
- The active operator pointer serializes controller intent; it does not claim the actual shared disruption slot or authorize quiescence.
- The configured `FleetRestartOperatorDriver` calls the existing guarded SDK transitions.
- Its bounded `reconcile` turns do not start a background task or grant consent to stop, return, resume, restore placement capacity or release the disruption slot.

- The driver persists a child attempt before each effect.
- A stable human operator ID is separate from its underlying SDK execution operation.
- M1 writes an immutable operation request before its Root CAS; if that CAS loses, a replacement must use a fresh underlying operation.
- A coordination CAS first fences the delayed old request.
- Once M1 is admitted, every subsequent child retains that exact SDK operation.
- Each step checks its persisted predecessor, so a stale controller cannot overwrite a later admitted step.
- Unknown results reconcile exact canonical receipts before a replacement is considered.

- Gate admission and physical quiescence are separate durable steps.
- The receiving node must observe the rooted enrollment epoch before consuming the physical request.
- The driver retains the admitted gate identity, settles the original birth/movement choice and performs the actual complete inventory, physical and quorum walk.
- Ready requires matched surviving leadership.
- A recreated controller resumes its Root children and repeats ephemeral proof; neither the journal nor progress counters restore health samples.
- Relevant Root drift restarts proof.

- `cancel_restart_operator` roots a separate human command and fences uncommitted older CAS requests.
- Before M1 is admitted, terminal cancellation releases only the operator pointer and has no physical effect.
- An admitted preparation uses its actual SDK cancellation.
- Once the physical gate is rooted, cancellation must own the synced exact-process Fence; an already accepted stop refuses.
- Pending winning birth/movement work retains its original resolution and must settle first.
- Only the checked canonical SDK cancellation receipt permits the terminal Root fact and operator-pointer release.
- An admitted target returns to excluded Ready; its retained occupancy is charged and capacity activation remains separate.

- `restart_operator_cancelled` reads the historical terminal fact and its canonical SDK predecessor after a lost confirmation.
- Retrying the old human cancellation cannot clear another operation's active pointer.
- Generic catalog writes cannot mint intent, attempts, cancellation commands or terminal facts.
- The operator fingerprint adds `/maintenance-operator-1`; internal markers stay at 1 and no legacy reader or migration is provided.

- `restart_operator_status` reads explicit Root snapshots without a configured driver or observer.
- It reports the canonical human intent, durable phase, execution-attempt identity, historical Ready reference and cancellation/terminal facts.
- Polling performs no physical work or mutation.
- A missing operation is only absence at the selected Root, and Ready is a historical assessment; neither is a live-health lease.
- Older-snapshot progress remains tied to its original cut.

- Preparation, cancellation and read-only durable status also have management SDK/admin/CLI adapters.
- `fleet_maintenance` admits preparation independently of stop/capacity permissions; `fleet_cancel` admits cancellation.
- The explicitly started `FleetRestartService` is owned and joined by the enrolled Node job set, uses its installed catalog backend/domain and bounded control budget, and shares one per-Node service slot across independent clients.
- It resumes Root intent after caller/controller loss.
- Active rounds share one original deadline, a soft quantum and at most sixteen complete steps; idle/blocked work is paced separately.
- No counter or status field restores health evidence.
- Reads never start execution.
- See the [operator workflow](operator-workflows.md#intact-maintenance-operator-workflow).

The independently authorized stop/return/resume management sequence, grace expiry and rolling operations remain required for the complete workflow. See [implementation progress](progress.md) for qualified source and test limits.

## Maintenance must remain safe when the controller or operator disappears

Restart maintenance holds memberships on the intact disk during its bounded grace. Evacuation replaces them. Do not pass the legacy evacuation `draining` flag into an intact restart and accidentally initiate replacement immediately.

- The pre-stop receipt names every affected history, its verified configuration and surviving voters, the intended process and the node-operation epoch.
- Stop revalidates relevant inputs, hands off any regained leadership and persists the local stop guard before permanent shutdown.
- A delayed request is refused after cancellation fencing or after the process generation changes.
- Transport traffic and normal elections continue while placement/leadership eligibility is withheld.

- If the controller disappears, another reconciler resumes the rooted operation; there is no expired lock to steal or permission for a second planned stop.
- If the operator disappears after shutdown, the operation stays `WaitingForRejoin`.
- Grace expiry permits quorum-backed repair and exposes partial retirement; it does not invent a return or make the node reusable.
- A second unexpected failure invalidates planned-stop readiness and pauses optional movement immediately.

Rolling maintenance advances only after the previous node's verified rejoin, restored redundancy and resume receipt. An offline node, a closed port and a successful container restart are three observations, none of which alone proves that its durable work is joined and its groups have recovered.

## Rebalance using the cost that can actually move

Keep topology redistribution, required repair and optional performance balancing as distinct objectives under the same resource ledger. The algorithm and initial rate settings remain in [maintenance and rebalancing](maintenance-and-rebalancing.md#select-and-schedule-a-concrete-balancing-action). Each candidate records a reason and a prediction that can be checked afterward:

| Candidate | Movable cost and admission inputs | Post-action observation |
| --- | --- | --- |
| Existing-voter leadership transfer | Incremental leader execution cost; target apply lag and spare CPU/SQL capacity | Observed leadership, source/target queue and end-to-end latency/TPS |
| Replica replacement | Follower-equivalent apply work plus serving metadata; target image/tail cost | Stable new voters, actual apply pressure, copied bytes and retained old footprint |
| Explicit distribution after addition | Distance to the feasible assignment, constrained by bytes/domains | Requested role/weight tolerance and no unresolved movement |
| Backend-pressure response | Shared archive/seed/CAS service pressure | Backlog trend and foreground latency after throttling; no fictitious local CPU relief |

- Capture prediction, input window/process revisions, action cost and uncertainty before admission.
- Observe the same metrics over fresh windows after settlement.
- Do not treat seed-time relief, rejected requests or a changed offered load as a gain.
- If measured optional benefit is absent, suppress similar candidates until fresh evidence or a changed model supports them.
- Reversing the last move still requires a new legal decision and cooldown; automatic rollback can itself create another costly retained copy.
- Required safety work continues under its own gates.

For heterogeneous nodes, normalize each resource by that node's verified usable capacity and account for existing reservations. Optimize the limiting resource; an average CPU/memory percentage can conceal a saturated dimension. A hot cell still has one leader, so fleet expansion cannot parallelize that cell's ordered execution. Adding nodes also cannot create RustFS bandwidth.

- Automatic balancing does not automatically provision or remove machines in v1.
- It can recommend a desired count/profile with its complete feasibility preview.
- The operator submits the scaling intent; required repair and redistribution then run autonomously.
- This separates workload-driven recommendations from the destructive platform decision and avoids count oscillation after a short idle period.
- Idle cells retain RF3 until authorized retirement is terminally rooted.

## Evidence required at each user-visible completion

| Result | Required proof | Does not assert |
| --- | --- | --- |
| Node capacity ready | Current authenticated identity/profile, complete inventory and required route/probe acknowledgements | Existing groups moved or node throughput increased |
| Balanced scale-out | Accepted capacity, actual target placement, settled permits, archive cuts and fresh convergence windows | Linear TPS scaling or source disk reclamation |
| Intact maintenance complete | Same engine, new process, reconciled memberships, catch-up and resume receipt | Old stop requests can apply to the new process |
| Resize complete | Verified new effective limits/configuration, retained-state fit and healthy rejoin | Profile text alone proves platform enforcement |
| Evacuated | Exclusion closure, no group references/actions, required progress/archive proof and physical reconciliation | Volume deletion or identity reuse |
| Removed | Rooted terminal removal, retained tombstone and effective revocation/fencing | Archive/volume garbage collection or perpetual survivor health |

- Removal revokes future authentication/activation for the exact registration after its old memberships and capabilities are settled.
- Do not revoke transport early while consensus still needs that voter.
- Required participants acknowledge revocation or are fenced/isolated under the existing protocol; unreachable stale processes cannot reopen admission with an old view.
- A new process on the same physical machine needs its own trusted identity, not a reused removed pin.

- Map this contract to existing acceptance packages: FO-03 checks empty hosts, trusted revisions and immutable histories; FO-04 checks staged placement and resource accounting; FO-05/06 check load evidence and effect recovery; FO-07 checks lifecycle fences and removal; FO-08 checks operator semantics; FO-09 checks scaling/profile sequencing; FO-10 checks the deployed workflows and SLO.
- Their named cases in [tasks.json](tasks.json) must exercise these boundaries.
- No package status advances because this contract has been written.

## Same-Root immutable validation reuse

- Restart and resume walks may retain successful canonical catalog validation in memory for the same Root revision.
- Every use still reads Root and checks the relevant table, inventory, separation and disruption pins.
- A changed revision requires the full canonical gate or returned-cut validation, including settlement exclusion.
- Serialized candidates omit this memo; decoding cannot restore or inject it.
- A recreated controller or receiver repeats validation.

This reuse carries no sample age, physical inventory, current profile, quorum, matched-progress or leadership proof. Those observations and their original five-second envelope remain live requirements. Same-Root memoization reduces repeated immutable journal reconstruction; it does not qualify hundreds of histories or allow an expired walk to publish.