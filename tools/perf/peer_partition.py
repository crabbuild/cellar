"""Peer-only fault injection in one private container's network namespace."""

import concurrent.futures
import datetime
import hashlib
import hmac
import ipaddress
import json
import secrets
import subprocess
import time
import urllib.request
import urllib.error

from run import command, logs, validate_node_limits, validate_routing_cache, validate_shutdown


def bucket_json(harness, bucket, key):
    # The host observes authority through the same local test bucket.
    endpoint = f"http://127.0.0.1:{harness.args.port}"
    path = f"/{bucket}/{key}"
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = timestamp[:8]
    host = endpoint.removeprefix("http://")
    digest = hashlib.sha256(b"").hexdigest()
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"GET\n{path}\n\nhost:{host}\nx-amz-content-sha256:{digest}\nx-amz-date:{timestamp}\n\n{signed}\n{digest}"
    scope = f"{date}/us-east-1/s3/aws4_request"
    signing = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    secret = ("AWS4" + harness.secret).encode()
    for part in (date, "us-east-1", "s3", "aws4_request"):
        secret = hmac.new(secret, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(secret, signing.encode(), hashlib.sha256).hexdigest()
    headers = {"x-amz-content-sha256": digest, "x-amz-date": timestamp,
               "Authorization": f"AWS4-HMAC-SHA256 Credential={harness.access}/{scope}, SignedHeaders={signed}, Signature={signature}"}
    with urllib.request.urlopen(urllib.request.Request(endpoint + path, headers=headers), timeout=10) as response:
        return json.loads(response.read())


class PeerPartition:
    def __init__(self, harness, owner):
        self.harness = harness
        self.owner = owner
        self.peers = [i for i in range(3) if i != owner]
        self.chain = "CELLAR_" + secrets.token_hex(6)
        self.created = False
        self.hooks = []
        state = json.loads(command("docker", "inspect", "--format", "{{json .State}}", harness.nodes[owner]))
        if state["Running"] is not True or type(state["Pid"]) is not int or state["Pid"] <= 0:
            raise RuntimeError("partition requires a live private node namespace")
        self.pid = state["Pid"]
        self.addresses = []
        for i in self.peers:
            networks = json.loads(command("docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", harness.nodes[i]))
            if set(networks) != {harness.network}:
                raise RuntimeError("peer must use the harness's sole private network")
            address = ipaddress.IPv4Address(networks[harness.network]["IPAddress"])
            if not address.is_private:
                raise RuntimeError("partition may only target private peer addresses")
            self.addresses.append(str(address))

    def iptables(self, *args, check=True):
        return command("colima", "ssh", "--", "sudo", "nsenter", "--target", str(self.pid), "--net", "--",
                       "iptables", "-w", "5", *args, check=check, timeout=15)

    def __enter__(self):
        try:
            self.iptables("-N", self.chain)
            self.created = True
            for address in self.addresses:
                self.iptables("-A", self.chain, "-s", address, "-j", "DROP")
                self.iptables("-A", self.chain, "-d", address, "-j", "DROP")
            self.iptables("-I", "INPUT", "1", "-j", self.chain)
            self.hooks.append("INPUT")
            self.iptables("-I", "OUTPUT", "1", "-j", self.chain)
            self.hooks.append("OUTPUT")
            self.started = time.monotonic()
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        for hook in self.hooks[:]:
            self.iptables("-D", hook, "-j", self.chain)
            self.hooks.remove(hook)
        if self.created:
            self.iptables("-F", self.chain)
            self.iptables("-X", self.chain)
            self.created = False

    def __exit__(self, *_):
        self.close()

    def probe(self, source, target, port):
        result = subprocess.run(["docker", "exec", self.harness.nodes[source], "timeout", "1", "bash", "-c",
                                 'exec 3<>/dev/tcp/$1/$2', "probe", target, str(port)], capture_output=True, timeout=5)
        if result.returncode not in (0, 1, 124):
            raise RuntimeError("TCP probe could not execute")
        return result.returncode == 0

    def connectivity(self):
        paths = [(self.owner, peer) for peer in self.peers] + [(peer, self.owner) for peer in self.peers]
        return {"peer_paths": [{"source": i + 1, "target": j + 1,
                                "connected": self.probe(i, f"node{j + 1}", 8081)} for i, j in paths],
                "bucket_paths": [{"node": i + 1, "connected": self.probe(i, "rustfs", 9000)} for i in range(3)],
                "unaffected_peer_paths": [{"source": i + 1, "target": j + 1,
                                           "connected": self.probe(i, f"node{j + 1}", 8081)}
                                          for i, j in (self.peers, self.peers[::-1])]}


def observation(harness, scenario, owner):
    with urllib.request.urlopen(harness.url(owner, 0), timeout=10) as response:
        state = json.loads(response.read())
    lease = bucket_json(harness, scenario, f"nodes/node{owner + 1}.json")
    return {"observed_ms": time.time_ns() // 1_000_000, "node": state["node"], "epoch": state["epoch"],
            "generation": lease["generation"], "expires_ms": lease["expires_ms"], "lease_node": lease["node"]}


def validate(value):
    owner = value["owner"]
    if type(owner) is not int or not 1 <= owner <= 3 or not 20 <= value["partition_s"] < 120:
        raise RuntimeError("missing sustained peer partition")
    expected = {(owner, i) for i in range(1, 4) if i != owner} | {(i, owner) for i in range(1, 4) if i != owner}
    for stage in ("before", "during", "after"):
        connectivity = value[stage]["connectivity"]
        paths = connectivity["peer_paths"]
        if len(paths) != 4 or {(p["source"], p["target"]) for p in paths} != expected:
            raise RuntimeError("missing bidirectional peer probes")
        if any(p["connected"] is not (stage != "during") for p in paths):
            raise RuntimeError("peer partition or healing was not observed")
        controls = connectivity["bucket_paths"]
        if len(controls) != 3 or {p["node"] for p in controls} != {1, 2, 3} or any(p["connected"] is not True for p in controls):
            raise RuntimeError("bucket access did not remain available")
        peers = {1, 2, 3} - {owner}
        unaffected = connectivity["unaffected_peer_paths"]
        if len(unaffected) != 2 or {(p["source"], p["target"]) for p in unaffected} != {(i, j) for i in peers for j in peers if i != j} or any(p["connected"] is not True for p in unaffected):
            raise RuntimeError("partition affected unrelated peers")
        state = value[stage]["authority"]
        if (state["node"] != f"node{owner}" or state["lease_node"] != state["node"]
                or type(state["epoch"]) is not int or state["epoch"] <= 0
                or type(state["expires_ms"]) is not int or type(state["observed_ms"]) is not int
                or state["expires_ms"] <= state["observed_ms"] or not state["generation"]):
            raise RuntimeError("owner did not retain valid authority")
        if any(state[k] != value["before"]["authority"][k] for k in ("node", "epoch", "generation")):
            raise RuntimeError("peer loss changed cell ownership or process generation")
    if value["during"]["authority"]["expires_ms"] - value["before"]["authority"]["expires_ms"] < 20_000:
        raise RuntimeError("node lease did not renew during the partition")
    if value["controlled_ack_node"] != f"node{owner}" or value["controlled_ack"] is not True:
        raise RuntimeError("accepted operation did not acknowledge from its retained owner")
    denials = value["peer_reads"]
    if (len(denials) != 2 or {d["node"] for d in denials} != {1, 2, 3} - {owner}
            or any(d.get("failed") is not True for d in denials)):
        raise RuntimeError("peers served the partitioned owner's cell without authority")


def scenario(harness):
    from faults import Ledger, LostResponse, validate_snapshot
    name = "inflight-peer-partition"
    harness.start_nodes(name, 3, "fleet")
    ledger = Ledger(harness.output / f"{name}-operations.json", harness.args.cells, harness.args.concurrency, burst_cell=0)
    evidence = harness.output / f"{name}-partition.json"
    try:
        warm = [o for o in ledger.operations if o["phase"] == "warm"]
        with concurrent.futures.ThreadPoolExecutor(harness.args.concurrency) as pool:
            if not all(pool.map(lambda op: ledger.attempt(op, harness.url(op["cell"] % 3, op["cell"])), warm)):
                raise RuntimeError("warmup did not acknowledge every cell")
        harness.verify(name, "before-fault", ledger, [0, 1, 2], final=True)
        harness.snapshot(name, 3, "warm")
        dropped = next(o for o in ledger.operations if o["phase"] == "lost-response")
        with LostResponse(ledger, dropped, harness.url(0, 0)) as url:
            if ledger.attempt(dropped, url) or dropped["upstream_ack"] is None:
                raise RuntimeError("proxy response loss was not observed")
        with urllib.request.urlopen(harness.url(0, 0), timeout=10) as response:
            owner = int(json.loads(response.read())["node"].removeprefix("node")) - 1
        partition = PeerPartition(harness, owner)
        value = {"owner": owner + 1, "before": {"connectivity": partition.connectivity(), "authority": observation(harness, name, owner)}}
        evidence.write_text(json.dumps(value, indent=2) + "\n")
        burst = [o for o in ledger.operations if o["phase"] == "burst"]
        controlled = burst[0]
        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            pending = pool.submit(ledger.attempt, controlled, harness.url(owner, 0), 10_000)
            marker = None
            while time.monotonic() - started < 5:
                for line in logs(harness.nodes[owner]).splitlines():
                    if line.startswith("{"):
                        event = json.loads(line)
                        with ledger.lock:
                            is_pending = controlled["operation_id"] in ledger.pending
                        if (event.get("event") == "fault_write_applied" and event.get("operation_id") == controlled["operation_id"]
                                and event.get("node") == f"node{owner + 1}" and event.get("inserted") is True
                                and event.get("hold_ms") == 10_000 and is_pending):
                            marker = dict(event, client_pending=True)
                            break
                if marker:
                    break
                time.sleep(0.05)
            if not marker:
                raise RuntimeError("no accepted write observed before peer partition")
            value["marker"] = marker
            with partition:
                value["burst_to_partition_s"] = partition.started - started
                if value["burst_to_partition_s"] >= 10:
                    raise RuntimeError("partition missed the accepted handler hold window")
                harness.phase = name + "-partitioned"
                connectivity = partition.connectivity()
                value["controlled_ack"] = pending.result()
                value["controlled_ack_node"] = controlled.get("ack_node")
                for operation in burst[1:]:
                    if not ledger.attempt(operation, harness.url(owner, 0)):
                        raise RuntimeError("bucket-connected owner could not acknowledge a new write")
                    time.sleep(1)
                while time.monotonic() - partition.started < 25:
                    observation(harness, name, owner)
                    time.sleep(1)
                value["peer_reads"] = []
                for peer in partition.peers:
                    try:
                        with urllib.request.urlopen(harness.url(peer, 0), timeout=3) as response:
                            response.read()
                        raise RuntimeError("a peer served the partitioned owner's cell")
                    except urllib.error.HTTPError as error:
                        if error.code not in (408, 429, 500, 502, 503, 504):
                            raise
                        value["peer_reads"].append({"node": peer + 1, "failed": True, "http_status": error.code})
                    except (urllib.error.URLError, TimeoutError) as error:
                        value["peer_reads"].append({"node": peer + 1, "failed": True, "error_type": type(error).__name__})
                value["during"] = {"connectivity": connectivity, "authority": observation(harness, name, owner),
                                   "final_connectivity": partition.connectivity()}
                if value["during"]["final_connectivity"] != connectivity:
                    raise RuntimeError("peer partition changed before verification")
                value["partition_s"] = time.monotonic() - partition.started
                evidence.write_text(json.dumps(value, indent=2) + "\n")
                with urllib.request.urlopen(harness.url(owner, 0), timeout=10) as response:
                    state = json.loads(response.read())
                operations = [o for o in ledger.operations if o["cell"] == 0]
                validate_snapshot(operations, state, final=True)
                (harness.output / f"{name}-partitioned-cell.json").write_text(json.dumps({"state": state, "operations": operations}, indent=2) + "\n")
                harness.snapshot(name, 3, "partitioned")
            value["after"] = {"connectivity": partition.connectivity(), "authority": observation(harness, name, owner)}
        validate(value)
        evidence.write_text(json.dumps(value, indent=2) + "\n")
        harness.faults.append(dict(scenario=name, partition=value))
        # Healing alone precedes this full check; no request has been replayed.
        harness.verify(name, "before-replay", ledger, [0, 1, 2], final=True)
        for operation in ledger.operations:
            if not ledger.attempt(operation, harness.url(owner, operation["cell"])):
                raise RuntimeError("reconciliation after healing failed")
        harness.verify(name, "reconciled", ledger, [0, 1, 2], final=True)
        harness.snapshot(name, 3, "healed")
        for i in range(3):
            harness.remove_node(i, name + "-drained", graceful=True, mode="preserve" if i == 2 else "handoff")
        harness.start_node(2, name, "fleet")
        harness.wait_node(name, 2)
        harness.verify(name, "preserve-reload", ledger, [2], final=True)
        harness.snapshot(name, 3, "preserve-reload", only=2)
        harness.remove_node(2, name + "-preserve-reloaded", graceful=True, mode="preserve")
        harness.clear_disks()
    finally:
        try:
            ledger.save()
        finally:
            ledger.close()


def audit(root, args, results):
    from faults import validate_snapshot
    name = "inflight-peer-partition"
    value = json.loads((root / f"{name}-partition.json").read_text())
    validate(value)
    marker = value["marker"]
    if (marker["inserted"] is not True or marker["client_pending"] is not True or marker["hold_ms"] != 10_000
            or marker["node"] != f"node{value['owner']}" or not 0 <= value["burst_to_partition_s"] < 10):
        raise RuntimeError("partition was not injected during an accepted write")
    if value["during"]["connectivity"] != value["during"]["final_connectivity"]:
        raise RuntimeError("missing sustained connectivity evidence")
    partitioned = json.loads((root / f"{name}-partitioned-cell.json").read_text())
    validate_snapshot(partitioned["operations"], partitioned["state"], final=True)
    if partitioned["state"]["node"] != f"node{value['owner']}" or partitioned["state"]["epoch"] != value["before"]["authority"]["epoch"]:
        raise RuntimeError("partitioned cell changed its owner or epoch")
    for label in ("before-fault", "before-replay", "reconciled", "preserve-reload"):
        operations = json.loads((root / f"{name}-{label}-operations.json").read_text())
        report = json.loads((root / f"{name}-{label}.json").read_text())
        if report["label"] != label or report["final"] is not True or {s["cell"] for s in report["states"]} != set(range(args["cells"])) or len(report["states"]) != args["cells"]:
            raise RuntimeError("missing partition receipt verification")
        totals = {k: 0 for k in ("issued", "client_acknowledged", "ambiguous_present", "ambiguous_absent", "writes")}
        for state in report["states"]:
            counts = validate_snapshot([o for o in operations if o["cell"] == state["cell"]], state, final=True)
            for key, count in counts.items():
                totals[key] += count
            if label == "preserve-reload" and state["node"] != "node3":
                raise RuntimeError("final reload was not served by node 3 alone")
        if any(report[k] != v for k, v in totals.items()):
            raise RuntimeError("partition verification totals disagree with receipts")
        if label == "before-replay":
            burst = [o for o in operations if o["phase"] == "burst"]
            if not burst or any(o["ack"] is None or o["cell"] != 0 or o.get("ack_node") != f"node{value['owner']}" for o in burst):
                raise RuntimeError("partitioned owner did not acknowledge every new write")
            if not any(o["operation_id"] == marker["operation_id"] for o in burst):
                raise RuntimeError("controlled operation absent from journal")
    for label, nodes in (("warm", range(3)), ("partitioned", range(3)), ("healed", range(3)), ("preserve-reload", [2])):
        for i in nodes:
            validate_routing_cache(json.loads((root / f"{name}-{label}-node{i + 1}-state.json").read_text()), args["max_cached_routes"])
    stops = [s for s in results["shutdown_outcomes"] if s["label"].startswith(name)]
    policies = [s for s in results["node_policies"] if s["scenario"] == name]
    if len(stops) != 4 or len(policies) != 4:
        raise RuntimeError("missing partition lifecycle proofs")
    for stop in stops:
        validate_shutdown(stop["proof"], stop["exit_code"], "preserve" if stop["node"] == 3 else "handoff")
    for policy in policies:
        validate_node_limits(json.dumps(policy["proof"]), args["max_resident"], args["max_cached_routes"], args["max_requests"])
