"""Additional evidence gates for follower-only destination append recovery."""
import json
import os
import shutil
import time

from faults import eventually, require
from follower_faults import replica_inventory
from resumed_owner import authority, store_json
from whole_fleet import inventory


def selected(objects, scope, generation):
    return {key: value for key, value in objects.items()
            if key.startswith(f"cells/{scope}/ltx/") or key.startswith(f"log/destination/{generation}/")}


def audit_proof(path, complete=True):
    armed = accepted = discarded = recovered = None
    killed = audited = restarted = held = False
    for line in path.read_text().splitlines():
        row = json.loads(line)
        event = row["event"]
        if event == "destination_proof_armed":
            require(armed is None and accepted is None, "repeated destination proof arm")
            armed = row
            scope, lease = row["scope"], row["lease"]
            require(row["proof_mode"] == "fleet" and row["peer_proof"] == "bucket"
                    and row["replica_fault_node"] == "destination" and row["node_lease_ms"] == 1000,
                    "destination proof policy missing")
            require(scope.startswith("__EventStream:") and len(scope.split(":")[1]) == 64
                    and row["owner"]["node"] == lease["node"] == "destination"
                    and lease["log"]["state"] == "open" and lease["log"]["ensemble"]
                    and set(lease["log"]["ensemble"]) <= {"producer", "consumer"}, "missing destination follower ensemble")
        elif event == "append_reply_held" and armed is not None:
            require(accepted is None and row["exchange"]["scope"] == armed["scope"], "held append used another destination")
            held = True
        elif event == "destination_follower_accepted":
            require(armed is not None and held and accepted is None and not killed, "destination proof followed loss or lacked a held reply")
            accepted = row
            lease = row["lease"]
            require(row["owner"] == armed["owner"] and lease["generation"] == armed["lease"]["generation"]
                    and lease["log"]["state"] == "open" and lease["log"]["may_have_fleet_acks"] is True
                    and lease["log"]["ensemble"] and set(lease["log"]["ensemble"]) <= {"producer", "consumer"},
                    "append lacks installed fleet proof")
            require(row["scope"] == armed["scope"] and row["refused"]
                    and any(item["path"].startswith(f"cells/{row['scope']}/ltx/") for item in row["refused"]),
                    "destination upload was not refused")
            require(selected(armed["objects"], row["scope"], lease["generation"])
                    == selected(row["objects"], row["scope"], lease["generation"]), "bucket data bypassed follower-only append")
        elif event == "process_killed" and armed is not None:
            require(accepted is not None and not killed and row["label"] == "destination-after-proven-append", "kill preceded follower proof")
            killed = True
        elif event == "destination_proof_discarded":
            require(killed and discarded is None and not restarted and row["files"] and row["remaining_entries"] == 0,
                    "destination local data survived or was removed before loss")
            require(row["lease"]["generation"] == accepted["lease"]["generation"]
                    and row["observed_ms"] > row["lease"]["expires_ms"], "old destination authority has not expired")
            require(selected(row["objects"], armed["scope"], accepted["lease"]["generation"])
                    == selected(armed["objects"], armed["scope"], accepted["lease"]["generation"]),
                    "bucket data repaired append before restart")
            discarded = row
        elif event == "destination_restarted" and armed is not None:
            require(discarded is not None and not restarted, "restart retained old destination data")
            restarted = True
        elif event == "fleet_audit_passed" and row["label"] == "recovered-before-retry":
            audited = True
        elif event == "destination_follower_recovered":
            require(armed is not None and restarted and audited and recovered is None, "proof recovery preceded saved-prefix audit")
            require(row["lease"]["generation"] != accepted["lease"]["generation"]
                    and row["owner"]["epoch"] > accepted["owner"]["epoch"], "destination authority was not replaced")
            recovered = row
        elif event == "retry_resumed" and armed is not None:
            require(recovered is not None, "retry concealed follower proof recovery")
        elif event == "append_reply_lost" and armed is not None:
            require(recovered is not None, "reply loss enabled retry before follower recovery audit")
        elif event == "attempt" and armed is not None:
            require(row["attempt"] == 1 and row["replay"] is False and not held, "source replay concealed follower recovery")
    if complete:
        require(armed is None or recovered is not None, "incomplete destination follower proof evidence")
    return dict(destination_follower_recoveries=int(recovered is not None))


class DestinationProof:
    def __init__(self, root, store, ledger):
        self.root, self.store, self.ledger = root, store, ledger
        self.data = root / "data/destination"

    def arm(self):
        scopes = list((self.data / "node").glob("__EventStream:*"))
        require(len(scopes) == 1, "private fixture must have exactly one destination stream")
        self.scope = scopes[0].name
        lease = eventually(lambda: store_json(self.root, self.store, "nodes/destination.json"),
            lambda value: value.get("log") is not None and value["log"]["state"] == "open" and value["log"]["ensemble"])
        self.generation = lease["generation"]
        with (self.data / "replica-block").open("xb") as marker:
            marker.flush()
            os.fsync(marker.fileno())
        self.ledger.record("destination_proof_armed", scope=self.scope, lease=lease,
            owner=authority(self.root, self.store, self.scope), objects=replica_inventory(self.root, self.store),
            proof_mode="fleet", peer_proof="bucket", replica_fault_node="destination", node_lease_ms=1000)
        audit_proof(self.ledger.path, complete=False)

    def accepted(self, exchange):
        require(exchange["scope"] == self.scope, "append hit another destination")
        def refused():
            path = self.data / "replica-refused.jsonl"
            return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        denied = eventually(refused, lambda rows: any(row["path"].startswith(f"cells/{self.scope}/ltx/") for row in rows))
        self.ledger.record("destination_follower_accepted", scope=self.scope, refused=denied,
            lease=store_json(self.root, self.store, "nodes/destination.json"),
            owner=authority(self.root, self.store, self.scope), objects=replica_inventory(self.root, self.store))
        audit_proof(self.ledger.path, complete=False)

    def discard(self):
        files = inventory(self.data)
        shutil.rmtree(self.data)
        require(not self.data.exists(), "destination local data survived removal")
        lease = store_json(self.root, self.store, "nodes/destination.json")
        observed = eventually(lambda: int(time.time() * 1000), lambda now: now > lease["expires_ms"])
        self.ledger.record("destination_proof_discarded", files=files, remaining_entries=0, lease=lease,
            observed_ms=observed, objects=replica_inventory(self.root, self.store))
        audit_proof(self.ledger.path, complete=False)

    def recovered(self):
        self.ledger.record("destination_follower_recovered", lease=store_json(self.root, self.store, "nodes/destination.json"),
            owner=authority(self.root, self.store, self.scope))
        audit_proof(self.ledger.path)
