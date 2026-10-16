"""Cold follower custody for destination proof recovery, without replay repair."""
import json
import hashlib
import os
import shutil
import time

from faults import eventually, require
from follower_faults import inspect_follower_disk, log_lineage, replica_inventory
from resumed_owner import authority, store_json
from whole_fleet import inventory

PEERS = {"producer", "consumer"}


def audit_disks(path, complete=True):
    pids = armed = accepted = retained = None
    killed, started, ready = {}, {}, set()
    replacement = None
    discarded = recovered = False
    operation = None
    blocked = 0
    append_gate = None
    held = None
    started_rows = {}
    for line in path.read_text().splitlines():
        row = json.loads(line)
        event = row["event"]
        if event in ("ack_fleet_started", "fetch_fleet_started", "fleet_started"):
            pids = row["pids"]
            operation = {"ack_fleet_started":"ack", "fetch_fleet_started":"fetch", "fleet_started":"append"}[event]
        elif event == "destination_proof_armed":
            require(type(row.get("restart_followers", False)) is bool, "invalid cold follower policy")
            if row.get("restart_followers", False):
                require(armed is None and pids is not None, "cold follower policy preceded process identity")
                armed = row
        elif event == "destination_follower_accepted" and armed:
            accepted = row
        elif event == "append_reply_held" and armed:
            held = row["exchange"]
        elif event == "process_killed" and armed:
            require(accepted is not None and row["pid"] == pids["destination"] and row["exit_code"] == -9,
                    "cold follower loss preceded destination proof")
            killed["destination"] = row["pid"]
        elif event == "destination_follower_process_killed":
            node = row["node"]
            require(armed and accepted and "destination" in killed and not retained and node in PEERS - killed.keys()
                    and row["pid"] == pids[node] and row["exit_code"] == -9 and row["graceful_reports"] == 0,
                    "wrong or premature cold follower loss")
            killed[node] = row["pid"]
        elif event == "destination_followers_retained":
            require(armed and accepted and set(killed) == PEERS | {"destination"} and not retained and not discarded,
                    "cold follower custody preceded loss of all original memory")
            retained = row
            from destination_proof import selected
            lease = accepted["lease"]
            require(selected(row["objects"], armed["scope"], lease)
                    == selected(armed["objects"], armed["scope"], lease), "bucket data repaired cold follower proof")
            require(set(row["leases"]) == PEERS and set(row["followers"]) == set(accepted["lease"]["log"]["ensemble"]),
                    "cold follower custody omitted an original witness")
            for node, lease in row["leases"].items():
                require(lease["node"] == node and (not lease.get("log") or not lease["log"]["may_have_fleet_acks"]),
                        "cold peer lacked bucket-backed leader proof")
            for data in row["followers"].values():
                require(data["files"] and any(batch["epoch"] == accepted["lease"]["log"]["epoch"]
                    and any(entry["cell"] == armed["scope"] and entry["cell_epoch"] == accepted["owner"]["epoch"]
                            for entry in batch["entries"]) for batch in data["batches"]), "cold follower has no original destination epoch")
        elif event == "destination_proof_discarded" and armed:
            require(retained is not None and not discarded, "destination disk removed before witness custody")
            discarded = True
        elif event == "destination_follower_process_started":
            node = row["node"]
            require(armed and discarded and retained and node in PEERS - started.keys()
                    and row["pid"] not in (*pids.values(), *started.values())
                    and row["lease"] == retained["leases"][node] and row["observed_ms"] > row["lease"]["expires_ms"],
                    "cold follower restart reused memory or live authority")
            require(row["files"] == (retained["followers"][node]["files"] if node in retained["followers"] else []),
                    "cold follower disk changed before restart")
            if operation == "append" or operation in ("ack", "fetch") and node == "consumer":
                gate = row["claim_gate"]
                require(gate["scope"] == armed["scope"] and gate["marker_sha256"] == hashlib.sha256(b"").hexdigest()
                        and len(gate["configuration_sha256"]) == 64, "caller could acquire destination and bypass the network gate")
            started[node] = row["pid"]
            started_rows[node] = row
        elif event == "destination_follower_process_ready":
            node = row["node"]
            require(armed and replacement is not None and set(started) == PEERS and node in PEERS - ready and row["pid"] == started[node]
                    and row["lease"]["node"] == node
                    and row["lease"]["generation"] != retained["leases"][node]["generation"]
                    and log_lineage(row["lease"]) == log_lineage(retained["leases"][node]),
                    "cold follower readiness lacks a fresh process generation")
            ready.add(node)
        elif event in ("ack_destination_restarted", "fetch_destination_restarted", "destination_restarted") and armed:
            require(discarded and set(started) == PEERS and replacement is None
                    and row["pid"] not in (*pids.values(), *started.values()), "destination restart reused a witness process")
            replacement = row["pid"]
        elif event == "destination_follower_recovered" and armed:
            require(ready == PEERS and not recovered and (operation != "append" or append_gate is not None),
                    "recovery audit preceded cold follower readiness or append gate")
            require(operation == "append" or row["owner"]["node"] in {"destination","producer"},
                    "cold replay caller owns the destination and can bypass the network gate")
            recovered = True
        elif event in ("ack_audit", "fetch_audit", "fleet_audit") and armed and row["label"] in ("recovered-before-replay", "recovered-before-retry"):
            require(ready == PEERS, "saved recovery snapshot preceded cold follower readiness")
            require(operation != "append" or append_gate is not None, "saved append prefix preceded replay gate")
        elif event == "retry_blocked" and armed and operation == "append" and ready == PEERS and not recovered:
            require(held is not None, "blocked append lacked original request")
            require(row["request_sha256"] == held["request_sha256"], "cold append retry changed its original bytes")
            blocked += 1
        elif event == "destination_append_gate":
            require(armed and operation == "append" and ready == PEERS and blocked > 0 and append_gate is None and not recovered
                    and row["scope"] == armed["scope"] and row["owner"]["node"] == "destination"
                    and row["owner"]["epoch"] > accepted["owner"]["epoch"], "cold append could repair acceptance before audit")
            require(set(row["refused"]) == PEERS and any(row["refused"].values())
                    and all(item == dict(operation="put",path=f"cells/{armed['scope']}/own.json")
                            for entries in row["refused"].values() for item in entries), "private claim fault was not exercised")
            append_gate = row
        elif event in ("retry_resumed", "fetch_attempt") and armed and (event == "retry_resumed" or row["attempt"] > 1):
            require(recovered, "replay repaired cold destination recovery")
        elif event.startswith("destination_follower_process_"):
            require(False, "unknown cold follower lifecycle evidence")
    if complete and armed:
        require(recovered and ready == PEERS, "incomplete cold follower recovery")
        for node, data in retained["followers"].items():
            require(inspect_follower_disk(path.parent / "destination-follower-images" / node) == data,
                    "archived destination follower bytes changed")
        if operation in ("append","ack","fetch"):
            for node in (PEERS if operation == "append" else {"consumer"}):
                gate = started_rows[node]["claim_gate"]
                config = path.parent / (node + "-cold-config.json")
                marker = path.parent / "data" / node / "claim-block"
                require(not config.is_symlink() and not marker.is_symlink()
                        and hashlib.sha256(config.read_bytes()).hexdigest() == gate["configuration_sha256"]
                        and hashlib.sha256(marker.read_bytes()).hexdigest() == gate["marker_sha256"]
                        and json.loads(config.read_text())["blocked_claim"] == armed["scope"], "cold append claim gate custody changed")
    from destination_startup import audit as audit_startup
    startup=audit_startup(path,complete)
    return dict(restarted_destination_followers=len(ready), cold_append_gates=int(append_gate is not None), startup_recovery_cuts=startup)


class ColdFollowers:
    def __init__(self, proof, binary, processes):
        self.proof, self.binary, self.processes = proof, binary, processes
        self.peers = {}
        self.proxies = []

    def stop(self):
        proof = self.proof
        for node in sorted(PEERS):
            process = next(process for process in self.processes if process.node == node)
            require(process.child.poll() is None, "original follower exited before measured loss")
            process.child.kill()
            code = process.child.wait(timeout=10)
            process.output.close()
            graceful = sum(json.loads(line).get("outcome") == "stopped" for line in process.log.read_text().splitlines() if line.startswith('{"'))
            require(code == -9 and graceful == 0, "cold follower loss fabricated graceful shutdown")
            proof.ledger.audited = False
            proof.ledger.record("destination_follower_process_killed", node=node, pid=process.child.pid,
                                exit_code=code, graceful_reports=graceful)
        lease = store_json(proof.root, proof.store, "nodes/destination.json")
        self.generation = lease["generation"]
        self.lineage = log_lineage(lease)
        require(len(self.generation) == 64 and all(c in "0123456789abcdef" for c in self.generation), "unsafe destination session")
        self.retained = {}
        for node in lease["log"]["ensemble"]:
            require(node in PEERS, "unknown destination follower")
            source = self.directory(node)
            files = inventory(source)
            archive = proof.root / "destination-follower-images" / node
            shutil.copytree(source, archive)
            for file in archive.rglob("*"):
                if file.is_file():
                    with file.open("rb") as saved:
                        os.fsync(saved.fileno())
            require(inventory(archive) == files, "destination follower archive changed during copy")
            self.retained[node] = inspect_follower_disk(archive)
        self.leases = {node: store_json(proof.root, proof.store, f"nodes/{node}.json") for node in PEERS}
        proof.ledger.record("destination_followers_retained", followers=self.retained, leases=self.leases,
                            objects=replica_inventory(proof.root, proof.store))
        audit_disks(proof.ledger.path, complete=False)

    def directory(self, node):
        return self.proof.root / "data" / node / "node/peerlog/destination" / self.lineage / self.generation

    def start(self):
        from fleet_faults import FleetProcess, PeerProxy, free_address
        proof = self.proof
        for node in sorted(PEERS):
            lease = self.leases[node]
            observed = eventually(lambda: time.time_ns() // 1000000, lambda now: now > lease["expires_ms"])
            files = inventory(self.directory(node)) if node in self.retained else []
            require(files == (self.retained[node]["files"] if node in self.retained else []), "retained follower disk changed")
            address = free_address()
            # Recovery may place the destination on a returning witness. Keep
            # the same exact-byte measuring gate on every advertised route.
            method = {"ack":"__streamAck", "fetch":"__streamNext", "append":"__streamAppend"}[proof.operation]
            proxy = PeerProxy(address, proof.ledger, method)
            proxy.paused = True
            self.proxies.append(proxy)
            claim_gate = None
            if proof.operation == "append" or proof.operation in ("ack", "fetch") and node == "consumer":
                marker = proof.root / "data" / node / "claim-block"
                with marker.open("xb") as file:
                    file.flush()
                    os.fsync(file.fileno())
                claim_gate = dict(scope=proof.scope, marker_sha256=hashlib.sha256(marker.read_bytes()).hexdigest())
            process = FleetProcess(self.binary, proof.root, node, node + "-cold", address, proxy.address, False, proof.store,
                                   fleet_durability=False, node_lease_ms=proof.node_lease_ms,
                                   blocked_claim=proof.scope if claim_gate else None)
            if claim_gate:
                claim_gate["configuration_sha256"] = hashlib.sha256((proof.root / (node + "-cold-config.json")).read_bytes()).hexdigest()
            self.processes.append(process)
            self.peers[node] = process
            proof.ledger.record("destination_follower_process_started", node=node, pid=process.child.pid,
                                lease=lease, observed_ms=observed, files=files, **(dict(claim_gate=claim_gate) if claim_gate else {}))
        audit_disks(proof.ledger.path, complete=False)
        if proof.operation == "append":
            # Before starting the destination, prove that a returning peer
            # actually hit the selected ownership-write refusal.
            eventually(self.claim_refusals, lambda rows: any(rows.values()), timeout=5)

    def ready(self, origins):
        for node, process in self.peers.items():
            origins[node] = process.ready()
            self.proof.ledger.record("destination_follower_process_ready", node=node, pid=process.child.pid,
                lease=store_json(self.proof.root, self.proof.store, f"nodes/{node}.json"))
        audit_disks(self.proof.ledger.path, complete=False)

    def resume(self):
        require(self.proof.ledger.audited, "cold peer route opened before recovery audit")
        for proxy in self.proxies:
            with proxy.lock:
                proxy.paused = False

    def append_gate(self):
        require(self.proof.operation == "append", "append gate used for another operation")
        def blocked():
            raw = self.proof.ledger.path.read_bytes()
            rows = [json.loads(line) for line in raw[:raw.rfind(b"\n")+1].splitlines()]
            ready = max(i for i, row in enumerate(rows) if row["event"] == "destination_follower_process_ready")
            return any(row["event"] == "retry_blocked" for row in rows[ready+1:])
        eventually(blocked, bool, timeout=5)
        self.proof.ledger.record("destination_append_gate", scope=self.proof.scope,
                                owner=authority(self.proof.root, self.proof.store, self.proof.scope), refused=self.claim_refusals())
        audit_disks(self.proof.ledger.path, complete=False)

    def claim_refusals(self):
        result = {}
        for node in PEERS:
            path = self.proof.root / "data" / node / "owner-refused.jsonl"
            raw = b""
            if path.exists():
                with path.open("rb") as file:
                    raw = file.read((128 << 10) + 1)
            require(len(raw) <= 128 << 10, "private claim evidence exceeds fixture limit")
            result[node] = [json.loads(line) for line in raw[:raw.rfind(b"\n")+1].splitlines()]
        return result

    def quiesce(self):
        for proxy in self.proxies:
            proxy.outage = True

    def close(self):
        failure = None
        while self.proxies:
            try:
                self.proxies.pop().close()
            except Exception as error:
                failure = failure or error
        if failure:
            raise failure
