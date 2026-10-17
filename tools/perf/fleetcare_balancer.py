"""Private HTTP ingress and evidence for the three-node capacity lab."""
import collections
import csv
import io
import json
import urllib.request


HAPROXY = "haproxy@sha256:254e6370f01cd654e0744edf607c67f33777c1b6a9dc0d4b28bcba7171460122"


def configuration(gateway, port):
    return """global
    maxconn 8192
    nbthread 1
defaults
    mode http
    timeout connect 3s
    timeout client 65s
    timeout server 65s
    timeout http-request 10s
    timeout http-keep-alive 10s
    timeout queue 30s
    retries 0
    retry-on none
    no option redispatch
frontend ingress
    bind :8080
    default_backend cellar
backend cellar
    balance roundrobin
    http-reuse safe
    http-response set-header X-Cellar-LB-Backend %[srv_name]
""" + "".join(f"    server node{i+1} {gateway}:{port+i+1} check inter 1s fall 2 rise 1\n" for i in range(3)) + """listen observations
    bind :8404
    stats enable
    stats uri /stats
"""


def statistics(url):
    with urllib.request.urlopen(url + ";csv", timeout=10) as response:
        text = response.read().decode()
    rows = list(csv.DictReader(io.StringIO(text.removeprefix("# "))))
    nodes = {row["svname"]: row for row in rows if row["pxname"] == "cellar" and row["svname"].startswith("node")}
    if set(nodes) != {"node1", "node2", "node3"}:
        raise RuntimeError("balancer observations lack the three backend nodes")
    return nodes


def distribution(before, after):
    fields = ("lbtot", "hrsp_1xx", "hrsp_2xx", "hrsp_3xx", "hrsp_4xx", "hrsp_5xx", "econ", "eresp", "wretr", "wredis")
    values = {}
    for node in ("node1", "node2", "node3"):
        delta = {key: int(after[node].get(key) or 0) - int(before[node].get(key) or 0) for key in fields}
        if min(delta.values()) < 0:
            raise RuntimeError("balancer counters reset during a measurement")
        if delta["wretr"] or delta["wredis"]:
            raise RuntimeError("balancer replayed or redispatched measured requests")
        values[node] = {**delta, "status_before": before[node]["status"], "status_after": after[node]["status"],
                        "queue_current": int(after[node].get("qcur") or 0), "queue_max": int(after[node].get("qmax") or 0)}
    total = sum(v["lbtot"] for v in values.values())
    return {"nodes": values, "selected_total": total,
            "selected_fractions": {node: v["lbtot"] / total if total else 0 for node, v in values.items()}}


def coverage(manifest, journal):
    targets = {entry["path"]: entry for entry in manifest["cells"]}
    expected = collections.Counter(entry["kind"] for entry in targets.values())
    seen = set()
    successful = set()
    owners = collections.Counter()
    rows = 0
    for line in journal:
        row = json.loads(line)
        rows += 1
        if row["sent_us"] is not None and row["path"] in targets:
            seen.add(row["path"])
            if row["status"] == "ok":
                successful.add(row["path"])
                if row.get("owner"):
                    owners[row["owner"]] += 1
    return {"attempts": rows, "manifest_roots": len(targets), "unique_sent_roots": len(seen),
            "unique_successful_roots": len(successful), "successful_business_owner_requests": dict(owners),
            "by_kind": {kind: {"expected": count,
                               "sent": sum(targets[p]["kind"] == kind for p in seen),
                               "successful": sum(targets[p]["kind"] == kind for p in successful)}
                        for kind, count in expected.items()}}


def ownership_setup(args):
    if args.profile == 'capacity' and args.ingress == 'haproxy':
        return {'mode':'serial_critical_http', 'primary_queues_per_node':1,
                'dead_letter_queues_per_node':1, 'sql_databases_per_node':6, 'inventory_zero_cells_per_node':2,
                'checks':'seed_and_each_load_boundary', 'connections':'close_during_setup_only',
                'sql_seed':'read_prepared_schema', 'first_backend':'node2',
                'start_window_utc_ms':[2000,5000]}
    return {'mode':'natural'}


def critical_roots(manifest):
    cells = manifest.get('cells', [])
    if manifest.get('count') != 3000 or len(cells) != 3000:
        raise RuntimeError('critical placement requires the 3000-root manifest')
    scopes = [cell.get('scope') for cell in cells]
    if any(not isinstance(scope, str) or not scope for scope in scopes) or len(set(scopes)) != 3000:
        raise RuntimeError('critical placement manifest identities are missing or duplicated')
    roots = [cell for cell in cells if cell['kind'] in ('queue', 'dlq', 'sql') or (cell['kind']=='inventory' and cell.get('id')==0)]
    if collections.Counter(cell['kind'] for cell in roots) != {'queue':3, 'dlq':3, 'sql':18, 'inventory':6}:
        raise RuntimeError('critical placement requires three queues, three DLQs, eighteen SQL roots and six Inventory-0 cells')
    for cell in roots:
        parts = cell['path'].split('/')
        valid = (len(parts)==5 and parts[:3]==['', 'system', cell['kind']] and all(p.isdecimal() for p in parts[3:]))
        if cell['kind']=='inventory':
            valid = len(parts)==5 and parts[:2]==['', 'api'] and parts[2].isdecimal() and parts[3:]==['inventory','0'] and int(parts[2])==cell['tenant']
        if not valid:
            raise RuntimeError('critical placement manifest has an invalid target path')
    if len({cell['path'] for cell in roots}) != len(roots):
        raise RuntimeError('critical placement manifest has duplicate system paths')
    order = {'queue':0, 'dlq':1, 'sql':2, 'inventory':3}
    return sorted(roots, key=lambda cell:(order[cell['kind']], cell.get('tenant', 0), cell.get('id', 0), cell['scope']))


def critical_ownership(manifest, states, expected):
    roots = critical_roots(manifest)
    if set(states) != {'node1', 'node2', 'node3'}:
        raise RuntimeError('critical ownership observations lack three nodes')
    residents = {}
    for node, state in states.items():
        values = state.get('residents')
        if (not isinstance(values, list) or any(not isinstance(v, str) for v in values)
                or len(set(values)) != len(values)):
            raise RuntimeError('invalid resident inventory for critical ownership')
        residents[node] = set(values)
    owners = {cell['scope']:[node for node, scopes in residents.items() if cell['scope'] in scopes] for cell in roots}
    if any(len(nodes) != 1 for nodes in owners.values()):
        raise RuntimeError('critical ownership is missing or duplicated: ' + json.dumps(owners, sort_keys=True))
    counts = {kind:{node:sum(cell['kind'] == kind and owners[cell['scope']] == [node] for cell in roots)
                    for node in states} for kind in ('queue', 'dlq', 'sql', 'inventory')}
    for kind, wanted, label in [('queue', 1, 'primary queue'), ('dlq', 1, 'dead-letter queue'), ('sql', 6, 'SQL'), ('inventory', 2, 'Inventory-0')]:
        if set(counts[kind].values()) != {wanted}:
            raise RuntimeError(f'{label} placement is not balanced: {counts[kind]}')
    mapping = {scope:nodes[0] for scope, nodes in owners.items()}
    if expected is None or set(expected) != set(mapping) or any(node not in states for node in expected.values()):
        raise RuntimeError('critical prewarm ownership contract is missing or invalid')
    if mapping != expected:
        raise RuntimeError('critical ownership changed from its recorded prewarm mapping')
    return {'owners':mapping, 'counts':counts, 'matches_prewarm':True}


# Runs in the existing private simulator container. Setup uses fresh frontend
# connections and no retries; measured clients retain their usual keep-alive.
SETUP_HTTP = r'''
import json,sys,time,urllib.request
config=json.load(sys.stdin)
for target in config['targets']:
 started=time.monotonic()
 body=target.get('body')
 request=urllib.request.Request('http://loadbalancer:8080'+target['path'],
  data=json.dumps(body).encode() if body is not None else None,
  headers={'Authorization':'Bearer '+config['token'],'Connection':'close','Content-Type':'application/json'},
  method='POST' if body is not None else 'GET')
 with urllib.request.urlopen(request,timeout=30) as response:
  payload=response.read(2*1024*1024+1)
  if len(payload)>2*1024*1024:raise RuntimeError('setup response exceeded2MiB')
  print(json.dumps({'path':target['path'],'scope':target.get('scope'),'kind':target.get('kind'),
   'status':response.status,'backend':response.headers.get('X-Cellar-LB-Backend'),
   'value':json.loads(payload),'elapsed_s':time.monotonic()-started}),flush=True)
'''
