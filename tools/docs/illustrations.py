# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Generate static SVG explanations shared by the guides and crate READMEs.

Keep these conceptual views separate from interactive execution flows. Text and
captions describe authority boundaries; arrows are not a complete import graph.
"""
from html import escape
import textwrap

PALETTE = {
    'cyan': ('#123343', '#22d3ee'),
    'green': ('#14392f', '#34d399'),
    'violet': ('#302446', '#a78bfa'),
    'amber': ('#3d3020', '#fbbf24'),
    'rose': ('#3f1c2e', '#fb7185'),
    'slate': ('#1e293b', '#94a3b8'),
}

VISUALS = [
    ('design-library', 'Navigate designs and evidence',
     'Connect contracts, implementation, acceptance cases, and retained evidence.', 'design/README.md'),
    ('consensus-contract', 'Follow consensus proofs',
     'Private execution, committed images, explicit reads, and archive durability.', 'design/multi-raft/README.md'),
    ('fleet-lifecycle', 'Separate fleet authorities',
     'Fleet intent, Raft membership, and exact-process lifecycle have distinct owners.', 'design/multi-raft/fleet-operations/README.md'),
    ('language-invocations', 'Trace language invocation authority',
     'Python, V8, and Node handlers use the originating native invocation.', 'design/008-embedded-javascript-typescript.md'),
    ('typed-rpc-contract', 'Trace a named typed call',
     'Protobuf contracts and generated registrations preserve native invocation authority.', 'design/009-named-typed-rpc.md'),
    ('filesystem-contract', 'Compose files and business state',
     'Invocation-bound filesystem operations share the cell transaction and durability proof.', 'design/per-cell-filesystem-design.md'),
    ('workspace-crates', 'Find the owning crate',
     'Application entry points and the internals that support them.', '03-architecture.md'),
    ('decision-loop', 'Decisions and effects',
     'The serial actor calls the pure core and receives adapter completions.', '03-architecture.md'),
    ('hosting-modes', 'Choose an HTTP host',
     'Cellar-owned HTTP and external Axum ingress share the cell runtime.', '10-embedding.md'),
    ('management-requests', 'Follow a console request',
     'Authentication, owner routing, cell state and the separate audit store.', '../crates/cellar-admin/README.md'),
    ('native-primitives', 'Values and runtime authority',
     'Owned Rust values and codecs stay separate from host lifecycle policy.', '../crates/cellar-native/README.md'),
    ('sdk-lifetimes', 'Understand overlapping lifetimes',
     'Generation adoption can overlap an old invocation and its background work.', '05-sdk-and-lifetimes.md'),
    ('transaction-boundaries', 'Keep atomic changes together',
     'Cell state, command receipts and outbox rows share one database transaction.', '06-storage-transactions.md'),
    ('command-retries', 'Resolve a lost command reply',
     'An application receipt returns the original result when the same command is retried.', '07-routing-rpc.md'),
    ('event-delivery', 'Publish, consume and acknowledge',
     'Source atomicity, durable dispatch, local inbox protection and checkpoint settlement.', '26-event-delivery.md'),
    ('ltx-replication', 'Capture and restore SQLite',
     'WAL capture, LTX segments, replica storage and restored state.', '../crates/cellar-ltx/README.md'),
]


class Diagram:
    """Small SVG writer with fixed readable text sizes and explicit geometry."""

    def __init__(self, title, description, height=660):
        self.title = title
        self.description = description
        self.height = height
        self.regions = []
        self.edges = []
        self.nodes = []
        self.labels = []

    def text(self, x, y, text, size=17, color='#cbd5e1', anchor='start', weight=400):
        return (f'<text x="{x}" y="{y}" fill="{color}" font-size="{size}" '
                f'font-weight="{weight}" text-anchor="{anchor}">{escape(text)}</text>')

    def label(self, x, y, text, **kwargs):
        self.labels.append(self.text(x, y, text, **kwargs))

    def panel(self, x, y, width, height, title, body=(), color='cyan'):
        fill, stroke = PALETTE[color]
        node = (f'<g data-panel="{escape(title, quote=True)}">'
                f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="10" fill="#0f172a"/>'
                f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="10" '
                f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        node += self.text(x + 20, y + 32, title, size=20, color='#f1f5f9', weight=600)
        line_y = y + 60
        for paragraph in body:
            for line in textwrap.wrap(paragraph, width=int((width - 40) / 8.0)):
                node += self.text(x + 20, line_y, line, size=16.5)
                line_y += 23
        if line_y > y + height + 5:
            raise ValueError(f'Panel too short for {title}')
        self.nodes.append(node + '</g>')

    def region(self, x, y, width, height, label, color='slate'):
        stroke = PALETTE[color][1]
        self.regions.append(
            f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="12" '
            f'fill="none" stroke="{stroke}" stroke-width="1.5" stroke-dasharray="7 5"/>'
            + self.text(x + 18, y + 28, label, size=15, color=stroke, weight=600))

    def arrow(self, path, color='slate', dashed=False):
        stroke = PALETTE[color][1]
        dash = ' stroke-dasharray="7 5"' if dashed else ''
        self.edges.append(f'<path d="{path}" fill="none" stroke="{stroke}" stroke-width="2" '
                          f'marker-end="url(#arrow-{color})"{dash}/>')

    def rule(self, x1, y1, x2, y2):
        self.regions.append(f'<path d="M{x1} {y1}L{x2} {y2}" fill="none" '
                            'stroke="#475569" stroke-dasharray="5 6"/>')

    def note(self, y, title, body, color='amber'):
        self.panel(48, y, 864, 94, title, [body], color)

    def svg(self):
        markers = ''.join(
            f'<marker id="arrow-{name}" markerWidth="9" markerHeight="9" refX="8" '
            f'refY="4.5" orient="auto"><path d="M0 0L9 4.5L0 9Z" fill="{stroke}"/></marker>'
            for name, (_, stroke) in PALETTE.items())
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 {self.height}" '
            'role="img" aria-labelledby="title desc">\n'
            f'<title id="title">{escape(self.title)}</title>\n'
            f'<desc id="desc">{escape(self.description)}</desc>\n'
            f'<defs>{markers}<pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">'
            '<path d="M40 0H0V40" fill="none" stroke="#1e293b" stroke-width=".5"/></pattern></defs>\n'
            '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}'
            'svg{background:#0f172a}</style>\n'
            '<rect width="100%" height="100%" fill="#0f172a"/>'
            '<rect width="100%" height="100%" fill="url(#grid)"/>\n'
            + '\n'.join(self.regions + self.edges + self.nodes + self.labels)
            + self.text(48, 55, self.title, size=30, color='#f8fafc', weight=700)
            + '</svg>')


def illustrations():
    drawings = {}

    d = Diagram('Designs connect contracts to evidence',
                'A design defines behavior. Implementation and named acceptance cases connect it '
                'to exact-source evidence. Release decisions retain remaining qualification gates.', 620)
    d.label(48, 90, 'One documentation library; each claim retains its owning source and scope.')
    d.panel(48, 150, 264, 132, 'Design contract',
            ['Required behavior', 'Invariants and failure semantics'], 'cyan')
    d.panel(348, 150, 264, 132, 'Implementation',
            ['SDK and runtime owners', 'Runnable examples and tests'], 'green')
    d.panel(648, 150, 264, 132, 'Acceptance',
            ['Named cases and oracles', 'Exact verification commands'], 'amber')
    d.arrow('M312 216H348', 'cyan')
    d.arrow('M612 216H648', 'green')
    d.panel(348, 356, 564, 132, 'Retained evidence',
            ['Source and executable identities; workload and topology',
             'Passing checks, failed attempts, and remaining gates'], 'violet')
    d.arrow('M780 282V356', 'amber')
    d.panel(48, 356, 264, 132, 'Release decision',
            ['Apply the declared gates', 'Retain unqualified scope'], 'rose')
    d.arrow('M348 422H312', 'violet')
    d.label(48, 566, 'A passing component record establishes only its recorded acceptance boundary.', size=17)
    drawings['design-library'] = d.svg()

    d = Diagram('Consensus execution and proof boundaries',
                'The leader executes a command privately. Raft commits its history before publishing '
                'the SQLite image. Quorum and S3 output proofs differ; reads select explicit consistency.', 660)
    d.label(48, 90, 'Conceptual contract; later release and recovery-time qualification remains open.')
    for x, title, body, color in [
        (48, 'Private command', ['Exact history and command', 'Leader-owned execution'], 'cyan'),
        (348, 'Raft commit', ['Durable voter history', 'Membership authority'], 'green'),
        (648, 'Published image', ['Committed SQLite and LTX', 'Bound version and receipt'], 'violet'),
    ]:
        d.panel(x, 150, 264, 132, title, body, color)
    d.arrow('M312 216H348', 'cyan'); d.arrow('M612 216H648', 'green')
    d.panel(48, 386, 264, 132, 'Output proof',
            ['Quorum: voter durability', 'S3: verified archive proof'], 'rose')
    d.panel(348, 386, 264, 132, 'Archive recovery',
            ['Verified image and tail', 'Retain dependencies'], 'violet')
    d.panel(648, 386, 264, 132, 'Explicit query',
            ['Stale or minimum-version', 'Strong: fresh quorum barrier'], 'cyan')
    d.arrow('M780 282V386', 'violet')
    d.arrow('M690 282V332H480V386', 'violet')
    d.arrow('M348 452H312', 'violet')
    d.arrow('M420 282V320H180V386', 'green')
    d.label(48, 610, 'Archive recovery restores state; it cannot restore old votes or authorize quorum loss.', size=17)
    drawings['consensus-contract'] = d.svg()

    d = Diagram('Fleet intent, group authority, and node lifecycle',
                'Conditional object-store records coordinate intent and capacity. Raft authorizes '
                'membership. The node owns exact-process stop, joining, retained return, and resume.', 660)
    d.label(48, 90, 'Selected authority boundaries; grants and live proof checks remain independent.')
    for x, title, body, color in [
        (48, 'Fleet catalog', ['Conditional durable intent', 'Reservations and receipts'], 'cyan'),
        (348, 'Raft group', ['Membership and leadership', 'Fresh surviving-voter proof'], 'green'),
        (648, 'Node owner', ['Exact boot and disk state', 'Quiescence and joined stop'], 'violet'),
    ]:
        d.panel(x, 150, 264, 132, title, body, color)
    d.arrow('M312 216H348', 'cyan'); d.arrow('M612 216H648', 'green')
    d.panel(48, 386, 264, 132, 'Prepare', ['Retain accepted work', 'Exclude new placement'], 'amber')
    d.panel(348, 386, 264, 132, 'Stop and return', ['Independent stop consent', 'Retained engine; fresh boot'], 'violet')
    d.panel(648, 386, 264, 132, 'Verify and resume', ['Renew physical/voter proof', 'Release only the owned slot'], 'green')
    d.arrow('M780 282V330H180V386', 'violet')
    d.arrow('M312 452H348', 'amber'); d.arrow('M612 452H648', 'violet')
    d.label(48, 610, 'Unknown outcomes retain their receipts and debt; status reads do not advance operations.', size=17)
    drawings['fleet-lifecycle'] = d.svg()

    d = Diagram('Language handlers retain native invocation authority',
                'A native invocation admits Python, V8, or Node handlers. Their bridge calls retain '
                'the original context, cancellation, resource bounds, and durability requirements.', 610)
    d.label(48, 90, 'TypeScript compiles before execution; runtime-specific release gates remain separate.')
    d.panel(48, 166, 264, 132, 'Native invocation',
            ['Domain and activation', 'Admission and cancellation'], 'cyan')
    d.panel(348, 166, 264, 132, 'Language handler',
            ['CPython, V8, or Node', 'Runtime-owned execution'], 'green')
    d.panel(648, 166, 264, 132, 'Scoped SDK bridge',
            ['Original invocation context', 'Bounded values and calls'], 'violet')
    d.arrow('M312 232H348', 'cyan'); d.arrow('M612 232H648', 'green')
    d.panel(348, 388, 564, 116, 'Native state and output gates',
            ['Cell transactions, routing, recovery, and required durability proof'], 'rose')
    d.arrow('M780 298V388', 'violet')
    d.label(48, 562, 'Runtime selection does not grant new cell authority or extend an expired context.', size=17)
    drawings['language-invocations'] = d.svg()

    d = Diagram('Named Protobuf calls use the native cell path',
                'Protobuf declares named methods. Generated clients and server registrations '
                'connect calls to namespace bindings, native routing, and typed results.', 610)
    d.label(48, 90, 'Registration and dispatch are distinct; consult the ledger for delivered API boundaries.')
    d.panel(48, 150, 264, 132, 'Protobuf contract',
            ['Service and method names', 'Request and response types'], 'cyan')
    d.panel(348, 150, 264, 132, 'Generated bindings',
            ['Clients and handlers', 'Registered cell class'], 'green')
    d.panel(648, 150, 264, 132, 'Invocation dispatch',
            ['Namespace and cell identity', 'Admission and native routing'], 'violet')
    d.arrow('M312 216H348', 'cyan'); d.arrow('M612 216H648', 'green')
    d.panel(348, 382, 564, 116, 'Typed result and effect outcome',
            ['Retain operation identity after an ambiguous mutation reply'], 'rose')
    d.arrow('M780 282V382', 'violet')
    d.label(48, 558, 'A descriptor names an interface; the originating invocation supplies authority.', size=17)
    drawings['typed-rpc-contract'] = d.svg()

    d = Diagram('Files and business state share the cell transaction',
                'An invocation accesses context.fs or tx.fs. File state and business receipts can '
                'commit in one SQLite transaction. An explicit barrier or output gate proves durability.', 650)
    d.label(48, 90, 'Core filesystem primitive; the optional FUSE adapter has an independent proposal.')
    d.panel(48, 150, 264, 132, 'Invocation',
            ['context.fs() or tx.fs()', 'Exact handle lifetime'], 'cyan')
    d.panel(348, 150, 264, 132, 'Bounded operation',
            ['POSIX-style SDK controls', 'Paths, quotas, typed errors'], 'green')
    d.panel(648, 150, 264, 132, 'Cell SQLite',
            ['File and business state', 'Atomic receipt transaction'], 'violet')
    d.arrow('M312 216H348', 'cyan'); d.arrow('M612 216H648', 'green')
    d.panel(48, 382, 264, 132, 'Optional mount',
            ['Separate adapter sessions', 'Separate delivery gates'], 'slate')
    d.panel(348, 382, 564, 132, 'Configured durability proof',
            ['Commit first; then sync, fsync, or the owning output gate',
             'Handle close or drop does not establish durability'], 'rose')
    d.arrow('M780 282V382', 'violet')
    d.arrow('M180 382V282', 'slate', dashed=True)
    d.label(48, 602, 'Persist stable operation receipts; reopen handles in each later invocation.', size=17)
    drawings['filesystem-contract'] = d.svg()

    d = Diagram('Find the crate that owns the change',
                'Axum and administration integrate with cellar. The host uses the pure logic core, '
                'native value primitives and LTX replication. Selected relationships, not every Cargo dependency.', 700)
    d.label(48, 89, 'Application entry points above; runtime internals below.')
    d.panel(120, 132, 300, 108, 'cellar-axum', ['HTTP handlers and ingress', 'Optional OpenAPI'], 'cyan')
    d.panel(540, 132, 300, 108, 'cellar-admin', ['Management API and console', 'Owner-routed controls'], 'cyan')
    d.panel(300, 308, 360, 108, 'cellar', ['Application SDK, host and CLI', 'Execution and fleet coordination'], 'green')
    d.arrow('M270 240V274H410V308', 'cyan')
    d.arrow('M690 240V274H550V308', 'cyan')
    for x, title, body, color in [
        (48, 'cellar-logic', ['Pure state transitions', 'Decisions return effects'], 'amber'),
        (348, 'cellar-native', ['Owned values and codecs', 'Web and byte primitives'], 'green'),
        (648, 'cellar-ltx', ['SQLite WAL capture', 'LTX replication and restore'], 'violet'),
    ]:
        d.panel(x, 516, 264, 116, title, body, color)
        d.arrow(f'M480 416V468H{x + 132}V516', color)
    d.label(48, 672, 'Arrows show host integration; the Cargo manifests define exact dependencies.', size=16)
    drawings['workspace-crates'] = d.svg()

    d = Diagram('Decisions stay in the pure core',
                'A serial actor owns State and calls on_event. It executes returned effects through adapters. '
                'Adapters submit versioned completion events through the actor mailbox.', 570)
    d.label(48, 89, 'The actor calls the core, executes effects and receives adapter completions.')
    d.panel(40, 170, 260, 120, 'Serial actor', ['Owns State', 'Serializes mailbox events'], 'green')
    d.panel(350, 170, 260, 120, 'on_event', ['Pure state transitions', 'Returns Effect values'], 'amber')
    d.panel(660, 170, 260, 120, 'Effect adapters', ['Storage and network I/O', 'Return completion events'], 'violet')
    d.arrow('M300 230H350', 'green')
    d.arrow('M610 230H660', 'amber')
    d.arrow('M790 290V390H170V290', 'violet')
    d.label(480, 369, 'Versioned completions return through the mailbox.', anchor='middle')
    d.note(446, 'No ambient work in the core', 'Supply time, randomness and I/O observations through explicit events.')
    drawings['decision-loop'] = d.svg()

    d = Diagram('Two hosts, one cell runtime',
                'Cellar may serve public HTTP itself or receive requests from an application-owned Axum server '
                'through CellarService and an IngressHandle. Both retain Cellar ownership, recovery and output gates.', 670)
    d.label(48, 89, 'Choose who serves public HTTP; Cellar retains cell authority and write proof.')
    d.region(40, 122, 416, 334, 'CELLAR-OWNED PUBLIC HTTP', 'cyan')
    d.region(504, 122, 416, 334, 'APPLICATION-OWNED AXUM HTTP', 'green')
    d.panel(64, 178, 368, 96, 'Cellar public listener', ['sdk::Host or Node::start'], 'cyan')
    d.panel(64, 338, 368, 96, 'Axum application', ['Registered gateways and handlers'], 'cyan')
    d.panel(528, 178, 368, 96, 'Outer Axum server', ['Your routes, listener and shutdown'], 'green')
    d.panel(528, 338, 368, 96, 'CellarService', ['IngressHandle from start_external'], 'green')
    d.arrow('M248 274V338', 'cyan')
    d.arrow('M712 274V338', 'green')
    d.panel(128, 524, 704, 98, 'Cellar node and durable cells',
            ['Owner routing, activation, SQLite, recovery and output gates'], 'violet')
    d.arrow('M248 434V524', 'cyan')
    d.arrow('M712 434V524', 'green')
    d.label(48, 650, 'The authenticated peer listener stays separate from public HTTP.', size=16)
    drawings['hosting-modes'] = d.svg()

    d = Diagram('Follow a management request',
                'The browser authenticates to Console. Console checks fleet permissions, records management '
                'operation intent and routes resource work to its authenticated owner. Only the owner opens cell state.', 630)
    d.label(48, 89, 'The console routes resource work to its owner and records operation outcomes.')
    d.panel(40, 148, 240, 120, 'Browser console', ['In-memory access token', 'Explicit operator action'], 'cyan')
    d.panel(360, 148, 240, 120, 'Console API', ['Origin and grant checks', 'Binding resolution'], 'green')
    d.panel(680, 148, 240, 120, 'Cell owner', ['Authenticated route', 'Authority and proof'], 'green')
    d.arrow('M280 208H360', 'cyan')
    d.arrow('M600 208H680', 'green')
    d.label(320, 190, 'token', size=14, anchor='middle')
    d.label(640, 190, 'signed', size=14, anchor='middle')
    d.panel(40, 350, 240, 118, 'Fleet metadata', ['Discovery and routing', 'Object-store records'], 'slate')
    d.panel(360, 350, 240, 118, 'Console audit DB', ['Intent and receipts', 'Separate SQLite file'], 'amber')
    d.panel(680, 350, 240, 118, 'Cell SQLite', ['Resource reads/writes', 'Opened by its owner'], 'violet')
    d.arrow('M480 268V350', 'amber')
    d.arrow('M800 268V350', 'violet')
    d.arrow('M360 238H320V320H160V350', 'slate', dashed=True)
    d.note(514, 'A lost reply needs reconciliation', 'Inspect the saved receipt; the browser does not automatically retry mutations.', 'rose')
    drawings['management-requests'] = d.svg()

    d = Diagram('Owned values do not grant cell authority',
                'Callers supply Rust values to native codecs and web primitives, which return owned values. '
                'The Cellar host separately owns event dispatch, activation authority, resource lifetimes and durability.', 586)
    d.label(48, 89, 'cellar-native operates on caller-owned data without a JavaScript engine.')
    d.panel(48, 154, 264, 142, 'Rust values', ['Text, bytes and graphs', 'Caller-owned resource state'], 'cyan')
    d.panel(348, 154, 264, 142, 'Native primitives', ['JSON, RPC and encoding', 'Crypto, compression and web'], 'green')
    d.panel(648, 154, 264, 142, 'Owned results', ['Encoded bytes and values', 'Streams, bodies and forms'], 'cyan')
    d.arrow('M312 225H348', 'cyan')
    d.arrow('M612 225H648', 'green')
    d.label(480, 344, 'Primitives process values; SDK contexts authorize runtime operations.', anchor='middle')
    d.region(48, 390, 864, 156, 'SEPARATE HOST RESPONSIBILITIES', 'violet')
    d.label(76, 466, 'Invocation dispatch · activation epochs · execution-domain lifetime', size=19)
    d.label(76, 505, 'Ownership checks · tracked background work · durability and output gates', size=18)
    drawings['native-primitives'] = d.svg()

    d = Diagram('An invocation can outlive its response',
                'Illustrative overlap: the node domain contains an old generation and a new generation. '
                'An event in the old cell activation continues tracked background work after its response and after '
                'adoption starts a new generation. This is not a timing or retention guarantee.', 700)
    d.label(48, 89, 'A new generation may serve while accepted work from the old one drains.')
    d.region(40, 128, 880, 420, 'NODE EXECUTION DOMAIN: SERVICES, TASKS AND CAPTURED POLICY', 'slate')
    for x, label in [(478, 'response'), (760, 'event ends')]:
        d.rule(x, 403, x, 508)
        d.label(x, 528, label, anchor='middle', size=15)
    d.label(65, 231, 'Old generation', size=16)
    d.label(65, 301, 'New generation', size=16)
    d.label(65, 381, 'Cell activation', size=16)
    d.label(65, 461, 'Event driver', size=16)
    # Bars are separate lifetimes, not nested ownership boxes or fixed durations.
    for x, y, w, text, color in [
        (250, 202, 570, 'Compiled application and pools remain for draining work', 'cyan'),
        (540, 272, 330, 'Adopted generation serves', 'green'),
        (250, 352, 570, 'Storage authority is bound to this activation epoch', 'violet'),
        (300, 432, 460, 'Handler, nested calls and wait_until', 'amber'),
    ]:
        fill, stroke = PALETTE[color]
        d.nodes.append(f'<rect x="{x}" y="{y}" width="{w}" height="48" rx="8" '
                       f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>'
                       + d.text(x + 16, y + 30, text, size=16))
    d.note(575, 'Keep the owning boundary alive', 'A retained handle does not extend authority. Keep host executors alive through join.')
    drawings['sdk-lifetimes'] = d.svg()

    d = Diagram('Atomicity stops at the cell database',
                'Business state, a command receipt and an outbox obligation commit or roll back in one cell-local '
                'transaction. A separate SQL binding and an external API are different transaction boundaries.', 650)
    d.label(48, 89, 'Group related local changes; coordinate separate databases and external effects explicitly.')
    d.region(40, 130, 416, 376, 'CURRENT CELL: ONE db.tx TRANSACTION', 'violet')
    d.panel(64, 188, 368, 76, 'Business state', [], 'violet')
    d.panel(64, 292, 368, 76, 'Command receipt', [], 'violet')
    d.panel(64, 396, 368, 76, 'Outbox obligation', [], 'violet')
    d.rule(500, 145, 500, 500)
    d.panel(544, 162, 368, 130, 'Separate SQL binding', ['Another database', 'Its own transaction boundary'], 'slate')
    d.panel(544, 358, 368, 130, 'External API', ['Remote side effect', 'Receiver idempotency or reconciliation'], 'slate')
    d.note(534, 'Commit together, or roll back together', 'tx.emit records local intent; delivery happens after the transaction commits.')
    drawings['transaction-boundaries'] = d.svg()

    d = Diagram('Resolve a lost reply with the same command ID',
                'Proposed application receipt protocol: commit a mutation and digest-checked receipt atomically. '
                'If the proven reply is lost, retry the same ID and digest. Read the receipt and return its stored '
                'result through the output gate without applying the mutation again.', 792)
    d.label(48, 89, 'Application receipt protocol; Cellar does not invent command IDs or replay business calls.')
    actors = [(160, 'Client', 'cyan'), (480, 'Cell owner', 'green'), (800, 'Cell SQLite', 'violet')]
    for x, title, color in actors:
        d.panel(x - 100, 136, 200, 74, title, [], color)
        d.rule(x, 210, x, 710)
    for left, right, y, label, color, dashed in [
        (160, 480, 264, 'command ID + digest', 'cyan', False),
        (480, 800, 324, 'atomic: mutation + receipt', 'green', False),
        (800, 480, 384, 'committed result', 'violet', True),
        (480, 160, 444, 'proof passed; reply lost', 'rose', True),
        (160, 480, 504, 'retry same ID + same digest', 'cyan', False),
        (480, 800, 564, 'read retained receipt', 'green', False),
        (800, 480, 624, 'stored result', 'violet', True),
        (480, 160, 684, 'return result after output proof', 'green', True),
    ]:
        d.arrow(f'M{left} {y}H{right}', color, dashed)
        d.label((left + right) / 2, y - 13, label, size=16, anchor='middle')
    d.labels.append('<path d="M285 435L303 453M303 435L285 453" stroke="#fb7185" stroke-width="3"/>')
    d.label(48, 750, 'Same ID with a different payload digest is a conflict. Retention bounds replay protection.', size=16)
    drawings['command-retries'] = d.svg()

    d = Diagram('Publish, prove, then acknowledge',
                'Source state and outbox intent commit atomically. Dispatch resumes the obligation and proves append. '
                'A subscription leases records without moving its checkpoint. The consumer commits effects with inbox '
                'receipts, syncs local durability, then acknowledges the exact batch. Release qualification remains open.', 690)
    d.label(48, 89, 'Separate the source commit, destination append, consumer effect and checkpoint.')
    for x, y, title, body, color in [
        (48, 152, '1  Source transaction', ['Business state + outbox row', 'db.tx / tx.emit'], 'violet'),
        (348, 152, '2  Durable dispatch', ['Resume stored obligation', 'Prove destination append'], 'green'),
        (648, 152, '3  Stream + lease', ['Ordered retained records', 'next leaves checkpoint intact'], 'cyan'),
        (648, 418, '4  Consumer transaction', ['Local effect + inbox receipt', 'Commit in the same database'], 'violet'),
        (348, 418, '5  Prove local effects', ['db.sync().await', 'Before remote settlement'], 'green'),
        (48, 418, '6  Acknowledge batch', ['Exact receipt', 'Advance stream checkpoint'], 'cyan'),
    ]:
        d.panel(x, y, 264, 128, title, body, color)
    d.arrow('M312 216H348', 'green')
    d.arrow('M612 216H648', 'green')
    d.arrow('M780 280V418', 'cyan')
    d.label(760, 350, 'subscription.next', anchor='end', size=16)
    d.arrow('M648 482H612', 'violet')
    d.arrow('M348 482H312', 'green')
    d.label(48, 605, 'A lost acknowledgment can redeliver records. Retained inbox receipts suppress local repeats.', size=16)
    d.label(48, 640, 'External effects still need receiver idempotency. Open release gates remain in the status ledger.', size=16)
    drawings['event-delivery'] = d.svg()

    d = Diagram('Capture SQLite commits and restore state',
                'A source SQLite WAL feeds capture into L0 LTX segments. Replica storage retains files and bundles, '
                'with compaction and snapshots. Restore or page-on-demand access reconstructs state. '
                'Cellar adds ownership fencing and output proof around these mechanics.', 670)
    d.label(48, 89, 'cellar-ltx supplies replication mechanics; the host decides when a write is proven.')
    for x, y, title, body, color in [
        (48, 154, 'SQLite source', ['Committed WAL data', 'Cell-local database'], 'violet'),
        (348, 154, 'WAL capture', ['Capture committed pages', 'Report the write position'], 'green'),
        (648, 154, 'L0 LTX segments', ['Transaction ranges', 'Checksummed page data'], 'amber'),
        (648, 398, 'Replica storage', ['Filesystem or object store', 'Bundles, compaction, snapshots'], 'violet'),
        (348, 398, 'Restore or page in', ['Rebuild a database', 'Or load pages on demand'], 'green'),
        (48, 398, 'Restored SQLite', ['Recovered page state', 'Host validates authority'], 'violet'),
    ]:
        d.panel(x, y, 264, 124, title, body, color)
    for path in ['M312 216H348', 'M612 216H648', 'M780 278V398', 'M648 460H612', 'M348 460H312']:
        d.arrow(path, 'violet')
    d.note(558, 'Capture alone is not a fleet acknowledgment', 'The host adds ownership, epoch fencing, log recovery and output proof.', 'rose')
    drawings['ltx-replication'] = d.svg()

    assert set(drawings) == {entry[0] for entry in VISUALS}
    return drawings


def visual_catalog():
    return [dict(id=slug, title=title, summary=summary, guide=guide)
            for slug, title, summary, guide in VISUALS]
