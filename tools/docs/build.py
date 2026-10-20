#!/usr/bin/env python3
"""Generate Cellar's offline documentation catalog and interactive SVG diagrams."""
import html
import json
from pathlib import Path
import textwrap

from illustrations import illustrations, visual_catalog

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
BASELINE = "c24feb25057793e6d6b18502c96eccbfcac2bdf4"

# Metadata is deliberately explicit: roles describe readers, not API guarantees.
GUIDES = [
    ("overview", "Overview and application fit", "Product boundaries, capabilities and evidence limits.", "builder platform team"),
    ("quickstart", "Run and package your first application", "A runnable counter, persistence check and native package.", "builder"),
    ("architecture", "Architecture and ownership boundaries", "Pure decisions, effect adapters and four owning lifetimes.", "builder platform"),
    ("cell-modeling", "Model cells and application boundaries", "Stable identity, invariants, partitioning and cross-cell coordination.", "builder team"),
    ("sdk-and-lifetimes", "SDK contracts, concurrency and lifetimes", "Context authority, transactions, handles and background work.", "builder"),
    ("storage-transactions", "Storage, transactions and schema changes", "Local atomicity, command receipts, outbox and migrations.", "builder"),
    ("routing-rpc", "Routing, RPC and command contracts", "Owner routing, structured calls and ambiguous-response handling.", "builder platform"),
    ("asynchronous-work", "Alarms, queues, workflows and scheduled work", "Durable intent, retries, replay and settlement.", "builder operations"),
    ("integrations", "Integrations and resource ownership", "HTTP, Blob, sockets, containers, facets and compiled loaders.", "builder platform"),
    ("embedding", "Embed an application-owned node", "Caller-owned services, explicit policies and returned outcomes.", "builder platform"),
    ("configuration", "Configuration and policy reference", "Verified defaults, CLI differences and captured limits.", "platform operations"),
    ("packaging-deployment", "Native packaging, deployment and rollback", "Artifacts, promotion, compatibility and gradual rollout.", "platform operations team"),
    ("fleet-topology", "Fleet topology and infrastructure contract", "Proposed failure domains and required dependency evidence.", "platform operations"),
    ("security-tenancy", "Security and multi-tenant application design", "Trust boundaries, credentials, authorization and isolation.", "builder platform team"),
    ("observability", "Observability and service objectives", "Health, typed outcomes, telemetry and business indicators.", "operations platform"),
    ("capacity-scaling", "Capacity planning and scaling", "Admission, skew, state growth and measured operating envelopes.", "platform operations"),
    ("operations", "Operating a Cellar application", "Startup, stop proofs, failure triage and maintenance.", "operations"),
    ("recovery-backup", "Recovery, backup and disaster drills", "Predecessor logs, coherent recovery sets and reconciliation.", "operations platform"),
    ("testing-qualification", "Testing and deployment qualification", "CI, contract tests, load and failure acceptance.", "builder platform operations"),
    ("team-delivery", "Team delivery for distributed applications", "Ownership, contracts, vertical slices and change review.", "team"),
    ("production-readiness", "Production readiness and evidence ledger", "Release-specific go/no-go gates and unproven scope.", "builder platform operations team"),
    ("api-reference", "SDK API map", "Public entry points linked to their actual implementation.", "builder platform"),
    ("cli-reference", "CLI reference and operator boundaries", "Verified packaging syntax and authenticated operator families.", "builder platform operations"),
    ("glossary", "Shared glossary", "Precise language for authority, durability and lifecycle.", "builder platform operations team"),
    ("documentation-maintenance", "Maintain the documentation library", "Source provenance, generated visuals and integrity checks.", "team"),
    ("event-delivery", "Outbox, streams and subscriptions", "Atomic publication, resumable consumption, typed outcomes and operations.", "builder platform operations"),
]

# Six steps per flow keep diagrams readable. Failure edges are simplified
# outcomes at a named decision boundary, not a complete internal state machine.
FLOWS = [
    dict(id="routing", title="Request routing", guide="07-routing-rpc.md", kind="Framework behavior · conceptual path",
         steps=[
             ("Authenticate and name", "Application", "Authenticate the caller, authorize the operation, and derive the canonical tenant and entity identity. Cellar does not supply business authorization."),
             ("Resolve route", "Actor", "A namespace stub asks the actor to resolve the cell's owner. Cached routing history is not ownership authority."),
             ("Establish authority", "Fleet / owner", "Forward to the valid peer owner, or acquire local authority and complete required recovery. Epochs prevent stale work from authorizing a later activation."),
             ("Admit invocation", "Runtime", "The owning runtime applies pool and cell limits and creates an event context bound to the execution domain and activation."),
             ("Execute the handler", "Compiled cell", "Read or mutate cell state with the current context. Protect state invariants with transactions or the appropriate input gate."),
             ("Settle output", "Output gate", "Required ownership and durability proof settle before externally visible success. Clients reconcile an ambiguous response using their command receipt.")],
         fail_at=2, failure=("Authority unavailable", "Fail / reconcile", "A lease, peer protocol, recovery or authority failure cannot grant local ownership. Return failure; mutation outcomes may require application receipt reconciliation.")),
    dict(id="write", title="Write durability", guide="06-storage-transactions.md", kind="Framework behavior · conceptual path",
         steps=[
             ("Validate the command", "Application", "Authenticate, validate the payload and check the command ID. A proposed receipt protocol binds each ID to a request digest and result."),
             ("Commit SQLite state", "Cell storage", "Apply local state and receipt atomically. A SQLite commit is not yet the externally acknowledged durability proof."),
             ("Capture the write", "LTX / replication", "Capture committed SQLite writes and retain the write position needed by the output gate. Replication mechanics preserve the cell's owning lifetime."),
             ("Prove durability", "Bucket / followers", "Bucket posture waits for upload proof. Fleet posture can use follower disk proof before upload and can fall back to bucket proof. Apply the actual loss model."),
             ("Verify output authority", "Output gate", "The gate must retain valid ownership and the required write proof. A stale epoch or unproven write cannot release success."),
             ("Release the result", "Client", "The client receives the gated result. A lost response can remain ambiguous even when the mutation survived; retry the same command ID or query its receipt.")],
         fail_at=3, failure=("Proof fails or fence", "No success proof", "Do not acknowledge success when proof or authority fails. Local state may have changed; the caller resolves uncertainty through the application command contract.")),
    dict(id="recovery", title="Recovery and startup", guide="18-recovery-backup.md", kind="Framework behavior · node bootstrap",
         steps=[
             ("Validate and lock", "Node / host", "Validate the explicit configuration and distinct listeners. Acquire exclusive ownership of the node data directory through completion."),
             ("Inspect predecessor", "Lease / log", "Identify prior generation and recovery pointers. Retain predecessor evidence, including log history that may contain fleet acknowledgements."),
             ("Recover prior log", "Durability", "Recover the predecessor's complete required history before replacing the node lease. Entries-only tails and legacy aliases are not accepted."),
             ("Install node authority", "Conditional store", "Establish replacement node authority only after recovery succeeds. This node bootstrap is distinct from each cell's own activation and restore."),
             ("Serve cell restores", "Runtime / LTX", "As routed cells activate, restore or hydrate their durable state under valid ownership. Confirm required alarms and async obligations for the application."),
             ("Verify readiness", "Host / operator", "Use the actual readiness gate and business probes, then reconcile receipts and exercise new writes. Startup time alone is not the recovery objective.")],
         fail_at=2, failure=("Recovery incomplete", "Preserve predecessor", "A failed recovery cannot replace its predecessor generation. Keep traffic out, retain logs and pointers, and investigate store, peer, credential and disk failures.")),
    dict(id="deployment", title="Native deployment", guide="12-packaging-deployment.md", kind="CLI behavior + proposed team rollout",
         steps=[
             ("Build the executable", "Cargo / project", "Compile registered Rust factories with locked dependencies and the chosen profile and target. Reject JavaScript-engine dependency graphs."),
             ("Verify the package", "Artifact protocol", "Validate format, host protocol, target, executable digest and size, plus assets and configuration. Retain the exact source and artifact identity."),
             ("Publish the package", "cellar deploy", "Publish verified artifacts and configuration to the selected bucket. Publication does not activate a new process across the fleet."),
             ("Install verified bytes", "cellar install", "Download and verify the target executable into a new path. Installation refuses to overwrite an existing executable."),
             ("Qualify and roll out", "Team / supervisor", "Run compatibility, security, capacity, failure and restore gates on the exact candidate. The process supervisor starts nodes and advances a gradual rollout."),
             ("Observe and retain", "Release / SRE", "Require readiness, business probes and complete drain proofs. Keep a compatible prior artifact and record the evidence-backed operating envelope.")],
         fail_at=4, failure=("Release gate fails", "Stop promotion", "Halt rollout and preserve the candidate evidence. Roll back only when data, workflow and peer compatibility are proven; otherwise contain and correct forward.")),
    dict(id="async", title="Asynchronous delivery", guide="26-event-delivery.md", kind="Framework event delivery · under development",
         steps=[
             ("Commit state and event", "Source cell", "db.tx and tx.emit share one transaction. Access and commit recheck domain admission; forced closure rolls back the unfinished transaction. Output gates prove recoverable intent."),
             ("Prove and append", "Framework dispatch", "Durable wakes resume delivery. The stream appends a contiguous source prefix and retains deduplication watermarks; proven acceptance permits source cleanup."),
             ("Lease a batch", "Named subscription", "next returns bounded records and an exact receipt without advancing the checkpoint. Caller-owned invocations or supervision drive the consumer."),
             ("Commit local effects", "Consumer transaction", "tx.inbox.accept and business effects commit together. Retained receipts suppress repeated local effects; external receivers still need idempotency."),
             ("Prove consumer state", "Storage sync", "db.sync proves local effects before changing the remote checkpoint. A failed or lost result does not establish rollback."),
             ("Acknowledge exact receipt", "Subscription checkpoint", "batch.ack settles the exact lease. After response loss, reuse its receipt. Required retention protects unacknowledged records and can backpressure sources.")],
         fail_at=4, failure=("Result uncertain", "Reconcile saved identity", "Retain the command, fetch or exact acknowledgment receipt. Audit before replay; a committed inbox receipt can suppress a repeated local effect. Full failure qualification remains open.")),
    dict(id="shutdown", title="Graceful shutdown", guide="17-operations.md", kind="Framework behavior · both stop modes",
         steps=[
             ("Close public admission", "Ingress", "Request Handoff for a leaving logical node or Preserve for a same-node restart. Stop accepting new public work while retaining accepted events."),
             ("Drain complete events", "Application drivers", "Drain root handlers, post-response wait_until, nested SDK calls, alarms and old-generation events. Task retirement alone is not a durability proof."),
             ("Drain cells and calls", "Actor / runtime", "Complete cell work and queued dispatches before changing lifecycle. Record pending events and dispatches at the semantic drain boundary."),
             ("Settle durability", "Durability owner", "Quiesce logs and close local durability while preserving required authority and recovery evidence. Preserve also requires clean-reload preparation."),
             ("Close and join", "Execution domain", "Close remaining owned work and join it with both caller executors still alive. Already-started blocking work can outlast the semantic budget."),
             ("Inspect the report", "Host supervisor", "Require complete phase flags, zero pending work and no exhausted budget for a graceful proof. Stopped or zero process exit alone is insufficient.")],
         fail_at=2, failure=("Drain budget expires", "Report incomplete", "Record incomplete phases and pending work before forced closure. Closing the domain retires work without a graceful proof; normal lease-expiry recovery remains responsible.")),
]

def esc(value):
    return html.escape(str(value), quote=True)

def svg_start(title, description, width=1200, height=900):
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
<title id="title">{esc(title)}</title><desc id="desc">{esc(description)}</desc>
<defs>
<pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse"><path d="M40 0H0V40" fill="none" stroke="#1e293b" stroke-width=".5"/></pattern>
<marker id="arrow" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto"><path d="M0 0L10 3.5L0 7Z" fill="#94a3b8"/></marker>
<marker id="alert-arrow" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto"><path d="M0 0L10 3.5L0 7Z" fill="#fb7185"/></marker>
</defs>
<style>
text {{ font-family: 'SF Mono', 'Cascadia Code', Menlo, monospace; fill:#e2e8f0; }}
.title {{font-size:26px;font-weight:700}} .subtitle {{font-size:13px;fill:#cbd5e1}}
.name {{font-size:16px;font-weight:600}} .label {{font-size:12px;fill:#cbd5e1}}
.edge {{fill:none;stroke:#94a3b8;stroke-width:1.6;marker-end:url(#arrow)}}
.node,.control {{cursor:pointer}} .node:focus .surface,.control:focus rect {{stroke:#fff;stroke-width:3}}
.node.active .surface {{stroke:#60a5fa;stroke-width:3;fill:#1e3a5f}}
.node.visited .surface {{stroke:#34d399}} .control:hover rect {{fill:#334155}}
.control[aria-disabled="true"] {{opacity:.45}} .error.active .surface {{stroke:#fb7185;fill:#4c1528}}
</style>
<rect width="100%" height="100%" fill="#0f172a"/><rect width="100%" height="100%" fill="url(#grid)"/>
'''

COLORS = {"cyan": ("#143343", "#22d3ee"), "green": ("#14392f", "#34d399"),
          "violet": ("#302446", "#a78bfa"), "amber": ("#3d3020", "#fbbf24"),
          "slate": ("#1e293b", "#94a3b8"), "rose": ("#3f1c2e", "#fb7185")}

def box(x, y, w, title, subtitle, color="cyan", h=70, attrs=""):
    fill, stroke = COLORS[color]
    name_size = min(16, (w - 24) / max(1, len(title)) / .62)
    label_size = min(12, (w - 24) / max(1, len(subtitle)) / .62)
    return f'''<g {attrs}><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="#0f172a"/>
<rect class="surface" x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>
<text class="name" style="font-size:{name_size:.2f}px" x="{x+w/2}" y="{y+29}" text-anchor="middle">{esc(title)}</text>
<text class="label" style="font-size:{label_size:.2f}px" x="{x+w/2}" y="{y+50}" text-anchor="middle">{esc(subtitle)}</text></g>'''

def lines(text, x, y, width=47, size=16, gap=26, cls=""):
    wrapped = textwrap.wrap(text, width=width)
    return f'<text x="{x}" y="{y}" font-size="{size}" class="{cls}">' + "".join(
        f'<tspan x="{x}" dy="{0 if i==0 else gap}">{esc(line)}</tspan>' for i, line in enumerate(wrapped)) + '</text>'

def edge(path, label="", x=0, y=0, alert=False):
    extra = ' stroke="#fb7185" stroke-dasharray="6 4" marker-end="url(#alert-arrow)"' if alert else ''
    # Inline style overrides class-defined defaults for alert edges.
    if alert:
        extra = ' style="stroke:#fb7185;stroke-dasharray:6 4;marker-end:url(#alert-arrow)"'
    return f'<path class="edge" d="{path}"{extra}/>' + (
        f'<text class="label" x="{x}" y="{y}" text-anchor="middle">{esc(label)}</text>' if label else '')

def flow_svg(flow):
    result = svg_start(flow["title"], "Interactive conceptual flow. Tab to a node or control and press Enter or Space; arrows change the step. A static image shows the same labeled path.").replace('role="img"', 'role="group"', 1)
    result += f'<text class="title" x="40" y="48">{esc(flow["title"])}</text><text class="subtitle" x="40" y="79">{esc(flow["kind"])}</text>'
    result += '<rect x="30" y="108" width="600" height="704" rx="12" fill="none" stroke="#334155" stroke-dasharray="8 4"/>'
    for i in range(5):
        y = 140 + i * 115
        result += edge(f'M210 {y+70}V{y+115}', "continue" if i == 0 else "", 270, y+96)
    fy = 140 + flow["fail_at"] * 115
    result += edge(f'M360 {fy+35}H400', "failure", 470, fy-16, True)
    for i, (title, subtitle, detail) in enumerate(flow["steps"]):
        attrs = f'id="step-{i}" class="node{ " active" if i == 0 else ""}" tabindex="0" role="button" aria-pressed="{str(i == 0).lower()}" aria-label="Step {i+1}: {esc(title)}" data-step="{i}"'
        result += box(60, 140+i*115, 300, title, f'{i+1} · {subtitle}', "violet" if i in (1, 3) else "cyan", attrs=attrs)
    result += box(400, fy, 200, flow["failure"][0], flow["failure"][1], "rose",
                  attrs='id="failure-node" class="node error" tabindex="0" role="button" aria-pressed="false" aria-label="Inspect failure branch" data-step="failure"')
    first = flow["steps"][0]
    result += '<rect x="660" y="108" width="510" height="704" rx="12" fill="#152135" stroke="#334155"/>'
    result += '<text class="label" x="690" y="151">STEP INSPECTOR</text><text id="step-number" class="subtitle" x="690" y="185" role="status" aria-live="polite">Step 1 of 6</text>'
    result += f'<text id="step-title" class="name" x="690" y="226">{esc(first[0])}</text>'
    result += lines(first[2], 690, 272, width=43, gap=28).replace('<text ', '<text id="detail" ', 1)
    result += '<text class="label" x="690" y="540">Click a node, or Tab + Enter.</text><text class="label" x="690" y="563">Arrow keys step through the main path.</text>'
    for ident, title, x, y, w in [('prev', 'Previous', 690, 600, 140), ('next', 'Next', 850, 600, 140),
                                  ('reset', 'Reset', 690, 666, 140), ('failure', 'Inspect failure', 850, 666, 280)]:
        result += f'<g id="{ident}" class="control" role="button" tabindex="0" aria-label="{title}"><rect x="{x}" y="{y}" width="{w}" height="46" rx="6" fill="#1e293b" stroke="#64748b"/><text x="{x+w/2}" y="{y+29}" font-size="15" text-anchor="middle">{title}</text></g>'
    result += f'<a href="../{esc(flow["guide"])}" target="_top"><text x="690" y="766" fill="#22d3ee" font-size="14" text-decoration="underline">Open the related guide →</text></a>'
    result += '<text class="label" x="40" y="852">Cyan: application / execution · Violet: persistence / proof · Rose: failure outcome</text>'
    result += '<text class="label" x="40" y="876">Conceptual path; detailed implementation and evidence limits are in the guide.</text>'
    data = json.dumps({"steps": flow["steps"], "failure": flow["failure"]}, ensure_ascii=True).replace('</', '<\\/')
    result += '''<script><![CDATA[
(() => {
  const model = DATA;
  let selected = 0;
  const nodes = Array.from(document.querySelectorAll('[data-step]'));
  const root = document.documentElement;
  function select(value) {
    selected = value;
    const failed = value === 'failure';
    const entry = failed ? model.failure : model.steps[value];
    nodes.forEach(node => {
      const id = node.dataset.step;
      node.classList.toggle('active', id === String(value));
      node.classList.toggle('visited', !failed && id !== 'failure' && Number(id) < value);
      node.setAttribute('aria-pressed', String(id === String(value)));
    });
    document.getElementById('step-number').textContent = failed ? 'Failure branch' : `Step ${value+1} of ${model.steps.length}`;
    document.getElementById('step-title').textContent = entry[0];
    const detail = document.getElementById('detail');
    detail.replaceChildren();
    const words = entry[2].split(' ');
    const wrapped = []; let line = '';
    words.forEach(word => {
      if ((line + ' ' + word).trim().length > 43 && line) { wrapped.push(line); line = word; }
      else { line = (line + ' ' + word).trim(); }
    });
    if (line) wrapped.push(line);
    wrapped.forEach((line, i) => {
      const span = document.createElementNS('http://www.w3.org/2000/svg', 'tspan');
      span.setAttribute('x', '690'); span.setAttribute('dy', i ? '28' : '0');
      span.textContent = line; detail.appendChild(span);
    });
    document.getElementById('prev').setAttribute('aria-disabled', String(!failed && value === 0));
    document.getElementById('next').setAttribute('aria-disabled', String(!failed && value === model.steps.length-1));
    root.setAttribute('data-selected-step', String(value));
  }
  function bind(node, action) {
    node.addEventListener('click', action);
    node.addEventListener('keydown', event => {
      if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); action(); }
    });
  }
  nodes.forEach(node => bind(node, () => select(node.dataset.step === 'failure' ? 'failure' : Number(node.dataset.step))));
  bind(document.getElementById('prev'), () => select(selected === 'failure' ? 0 : Math.max(0, selected-1)));
  bind(document.getElementById('next'), () => select(selected === 'failure' ? 0 : Math.min(model.steps.length-1, selected+1)));
  bind(document.getElementById('reset'), () => select(0));
  bind(document.getElementById('failure'), () => select('failure'));
  root.addEventListener('keydown', event => {
    if (['ArrowDown','ArrowRight','ArrowUp','ArrowLeft','Home','End'].includes(event.key)) {
      event.preventDefault();
      const n = typeof selected === 'number' ? selected : 0;
      select(event.key === 'Home' ? 0 : event.key === 'End' ? model.steps.length-1 :
        Math.max(0, Math.min(model.steps.length-1, n + (['ArrowDown','ArrowRight'].includes(event.key) ? 1 : -1))));
    }
  });
  select(0);
})();
]]></script></svg>'''.replace('DATA;', data + ';')
    return result

def static_diagrams():
    diagrams = {}
    s = svg_start("Cellar: durable business cells", "Conceptual component responsibilities and main request, decision and durability relationships.", height=820)
    s += '<rect x="30" y="195" width="1140" height="340" rx="12" fill="none" stroke="#fbbf24" stroke-dasharray="8 4"/><text class="label" x="55" y="224">NODE EXECUTION DOMAIN · captured policy and owned tasks</text>'
    s += edge('M250 145H400', 'ingress', 320, 127) + edge('M650 145H800', 'invoke', 725, 127)
    s += edge('M525 180V260') + edge('M950 180V260') + edge('M650 295H800', 'dispatch', 725, 278)
    s += edge('M525 330V410', 'decisions', 590, 368) + edge('M950 330V410', 'commit', 1005, 368)
    s += edge('M950 480V590') + edge('M800 625H650', 'proof request', 725, 608) + edge('M400 625H250', 'CAS / upload', 320, 608)
    s += box(50,110,200,'Client / edge','Public authentication','cyan')
    s += box(400,110,250,'Native application','Compiled Rust factories','cyan')
    s += box(800,110,300,'HTTP / RPC bindings','Context and event authority','green')
    s += box(400,260,250,'Serial actor','Effects + completion events','green')
    s += box(800,260,300,'Runtime / cells','Generations and input gates','green')
    s += box(400,410,250,'cellar-logic','Pure decisions, no I/O','amber')
    s += box(800,410,300,'SQLite + cellar-ltx','Commit capture and restore','violet')
    s += box(50,590,200,'Object store','Ownership / LTX / logs','violet')
    s += box(400,590,250,'Fleet / durability','Leases and peer proofs','violet')
    s += box(800,590,300,'Output gate','Proof before visible success','rose')
    s += '<text class="title" x="40" y="45">Cellar: durable business cells</text><text class="subtitle" x="40" y="77">Conceptual relationships, not an exhaustive import graph or timing sequence</text>'
    s += lines('SDK: application-facing contracts · cellar-native: engine-independent owned values',40,727,width=110,size=14,gap=24)
    s += '<text class="label" x="40" y="778">Cyan: callers · Green: execution · Violet: persistence · Amber: policy · Rose: output safety</text></svg>'
    diagrams['architecture'] = s

    s = svg_start('Cells: local atomicity, explicit coordination', 'Proposed inventory and order domain. Transactions remain within each cell; workflow calls and external effects need receipts.', height=720)
    s += '<rect x="30" y="150" width="430" height="350" rx="12" fill="none" stroke="#22d3ee" stroke-dasharray="8 4"/><rect x="740" y="150" width="430" height="350" rx="12" fill="none" stroke="#34d399" stroke-dasharray="8 4"/>'
    s += edge('M400 235H800','command ID + receipt',600,210) + edge('M955 290V390') + edge('M245 290V390')
    s += edge('M245 460V615H450') + edge('M750 615H955V460','replay-safe calls',1020,555)
    s += box(90,200,310,'Order cell','tenant / order identity','cyan',h=90)
    s += box(800,200,310,'Inventory cell','tenant / SKU / location','green',h=90)
    s += box(90,390,310,'Order SQLite','State + command receipts','violet')
    s += box(800,390,310,'Inventory SQLite','Stock invariant + receipts','violet')
    s += box(450,580,300,'Workflow / outbox','Intent, retry, compensation','amber')
    s += '<text class="title" x="40" y="45">Cells: local atomicity, explicit coordination</text><text class="subtitle" x="40" y="77">Proposed application model · Cellar does not establish a global ACID transaction</text>'
    s += '<text class="label" x="55" y="178">TRANSACTION BOUNDARY A</text><text class="label" x="765" y="178">TRANSACTION BOUNDARY B</text><text class="label" x="40" y="685">State invariants stay local. Cross-cell and external effects require an application protocol.</text></svg>'
    diagrams['cell-boundaries'] = s

    s = svg_start('Fleet: qualify the intended failure domains', 'Proposed multi-zone topology. Shared object storage, ingress and peers must be qualified for the claimed failure model.', height=780)
    for x, label in [(40,'FAILURE DOMAIN A'),(430,'FAILURE DOMAIN B'),(820,'FAILURE DOMAIN C')]:
        s += f'<rect x="{x}" y="250" width="340" height="280" rx="12" fill="none" stroke="#fbbf24" stroke-dasharray="8 4"/><text class="label" x="{x+20}" y="278">{label}</text>'
        s += edge(f'M600 200V230H{x+170}V310') + edge(f'M{x+170} 380V440') + edge(f'M{x+170} 500V555H600V610')
    s += box(440,130,320,'Authenticated ingress','Load balance + business probes','cyan')
    for x, label in [(70,'Node A'),(460,'Node B'),(850,'Node C')]:
        s += box(x,310,280,label,'Owner / runtime / peer auth','green') + box(x,440,280,'Persistent local disk','SQLite + follower / node logs','violet',h=60)
    s += box(400,610,400,'Shared object-store authority','Conditional ownership, LTX and recovery','violet',h=90)
    s += '<text class="title" x="40" y="45">Fleet: qualify the intended failure domains</text><text class="subtitle" x="40" y="77">Proposed topology · independent zones and regions are not certified by the local reports</text>'
    s += '<text class="label" x="40" y="747">Authenticated peer network links the nodes. Correlated store/network/disk failures need explicit tests.</text></svg>'
    diagrams['fleet-topology'] = s

    s = svg_start('Teams: evidence travels with the release', 'Proposed team delivery loop from domain contract to implementation, qualification, operation and observed feedback.', height=650)
    for path in ['M330 220H430','M760 220H860','M1025 255V390','M860 425H760','M430 425H330','M165 390V255']:
        s += edge(path)
    for x,y,w,title,sub,c in [(40,185,290,'Domain contract','Invariant, identity and owner','cyan'),(430,185,330,'Native vertical slice','Receipts, schema and tests','green'),(860,185,290,'Release qualification','Exact artifact + failure evidence','amber'),(860,390,290,'Controlled rollout','Readiness + complete drain','green'),(430,390,330,'Operate and recover','SLO, command ledger, restore','violet'),(40,390,290,'Review observations','Incidents and changed assumptions','rose')]:
        s += box(x,y,w,title,sub,c)
    s += '<text class="title" x="40" y="45">Teams: evidence travels with the release</text><text class="subtitle" x="40" y="77">Proposed ownership loop · a passing build is one input to a go/no-go decision</text>'
    s += '<text class="label" x="40" y="556">Each boundary needs a named owner, versioned contract and reviewable evidence.</text><text class="label" x="40" y="588">Failed qualification remains visible. Runtime observations feed the next contract and test.</text></svg>'
    diagrams['team-delivery'] = s
    return diagrams

def catalog():
    entries = []
    for n, (slug, title, summary, roles) in enumerate(GUIDES, 1):
        entries.append(dict(path=f'{n:02}-{slug}.md', title=title, summary=summary,
                            roles=roles.split(), category='Guide' if n < 22 else 'Reference'))
    extras = [
        ('README.md', 'Documentation library', 'Complete document list, visuals and role-based reading paths.', 'Reference'),
        ('sources.md', 'Sources and evidence provenance', 'Reviewed baseline, source map and retained validation scope.', 'Reference'),
        ('adr/README.md', 'Architecture decisions', 'Baseline records and rules for future decisions.', 'Decision'),
        ('adr/001-native-applications.md', 'ADR 001 · Native applications', 'Factories compiled into verified native executables.', 'Decision'),
        ('adr/002-cell-authority-and-proof.md', 'ADR 002 · Authority and proof', 'Epochs, local authority and output durability proof.', 'Decision'),
        ('adr/003-explicit-host-lifetimes.md', 'ADR 003 · Explicit host lifetimes', 'Captured policies and returned lifecycle evidence.', 'Decision'),
        ('adr/004-embedded-python.md', 'ADR 004 · Embedded Python', 'Trusted CPython, original invocation authority and verified process replacement.', 'Decision'),
        ('templates/application-brief.md', 'Application brief template', 'Define the domain, workload, correctness and recovery requirements.', 'Template'),
        ('templates/service-contract.md', 'Service contract template', 'Publish identity, schemas, retry and compatibility semantics.', 'Template'),
        ('templates/release-checklist.md', 'Release checklist template', 'Record candidate identity, gate evidence and go/no-go decision.', 'Template'),
        ('templates/incident-record.md', 'Incident record template', 'Preserve outcomes, command ledger and recovery evidence.', 'Template'),
    ]
    for path, title, summary, category in extras:
        entries.append(dict(path=path,title=title,summary=summary,
                            roles=['builder','platform','operations','team'],category=category))
    return dict(schema_version=1, baseline=BASELINE, reviewed_date='2026-10-12',
                documents=entries, visuals=visual_catalog(), flows=[{k:f[k] for k in ('id','title','guide','kind')} for f in FLOWS])

def build():
    (DOCS / 'diagrams').mkdir(parents=True, exist_ok=True)
    diagrams = {**static_diagrams(), **illustrations()}
    for slug, svg in diagrams.items():
        (DOCS / 'diagrams' / f'{slug}.svg').write_text(svg + '\n')
    for flow in FLOWS:
        (DOCS / 'diagrams' / f'{flow["id"]}-flow.svg').write_text(flow_svg(flow) + '\n')
    data = catalog()
    (DOCS / 'catalog.json').write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n')
    template = (ROOT / 'tools/docs/explorer.html').read_text()
    serialized = json.dumps(data, ensure_ascii=True).replace('</', '<\\/')
    (DOCS / 'index.html').write_text(template.replace('/*CATALOG*/null', serialized))
    print(f'Generated {len(data["documents"])} catalog entries, {len(diagrams) + len(FLOWS)} SVGs and offline explorer.')

if __name__ == '__main__':
    build()
