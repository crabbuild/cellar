#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independent CG-17 coverage admission. Read artifacts; never run their commands."""

import argparse
import hashlib
import itertools
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
PACKAGE = Path("docs/design/multi-raft/cell-groups")
CONTRACTS = ("primitive-inventory.json", "primitive-coverage-requirements.json",
             "primitive-coverage.schema.json")
SDK_ROOTS = {"crates/cellar/sdk.rs", "crates/cellar/application/mod.rs"}
DOCUMENTED_REFUSALS = {"cellar::sdk::sql::Database::dump": "SQL_ERROR"}
FAMILIES = {"storage-sql-kv", "queue", "event-stream", "workflow", "alarms-cron",
            "filesystem", "payload", "http-rpc", "websocket", "container", "other"}
STATEFUL_FAMILIES = FAMILIES - {"http-rpc", "websocket", "other"}
FEATURES = ("native", "native-no-default", "python", "javascript", "mixed")
AXES = {"image": ["materialized", "extents"], "namespace": ["managed", "unmanaged"],
        "minimum": ["quorum", "s3"], "occupancy": [1, 2]}
SCENARIOS = {
    "retry": (None, {"commit-reply-lost", "same-id-reconciled"}),
    "leader": ("primitives_leader_loss_quorum_refusal_and_restart",
               {"leader-killed", "quorum-refused", "follower-caught-up", "voter-replaced", "retained-restart"}),
    "movement": ("primitives_split_merge_placement_and_wakeup",
                 {"cell-moved", "split", "merge", "placed", "parked", "woken", "controller-restarted"}),
    "archive": ("primitives_archive_outage_reclaim_and_recovery",
                {"rustfs-unavailable", "archive-debt-bounded", "rustfs-returned", "log-reclaimed", "archive-restored"}),
    "disaster": ("primitives_zero_quorum_fresh_incarnation",
                 {"old-writers-fenced", "voter-disk-erased", "fresh-incarnation", "fresh-quorum", "old-token-refused"}),
    "shutdown": ("primitives_cancellation_capacity_and_joined_shutdown",
                 {"request-cancelled", "capacity-refused", "owner-revoked", "joined-close", "obligations-resumed"}),
}
ROLES = {"application", "voter", "cli", "test"}
ID = re.compile(r"[a-z][a-z0-9-]{0,95}")
CASE = re.compile(r"primitives_[a-z0-9_]{1,100}")
SHA = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")
MAX_JSON = 32 * 1024 * 1024
MAX_CELLS = 32768


class CoverageError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise CoverageError(message)


def fields(value, names, label):
    require(isinstance(value, dict) and set(value) == set(names.split()),
            f"Invalid {label} fields")


def array(value, limit, label):
    require(isinstance(value, list) and 0 < len(value) <= limit, f"Invalid {label} length")
    return value


def matches(value, pattern):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def text(value):
    return isinstance(value, str) and 0 < len(value) <= 4096 and bool(value.strip())


def unique(values, label):
    require(all(isinstance(value, str) for value in values), f"Invalid {label} value")
    result = set(values)
    require(len(result) == len(values), f"Duplicate {label}")
    return result


def read_json(data):
    def object_pairs(pairs):
        unique([key for key, _ in pairs], "JSON key")
        return dict(pairs)

    def constant(value):
        raise CoverageError(f"Non-finite JSON number: {value}")

    require(0 < len(data) <= MAX_JSON, "Invalid JSON artifact size")
    return json.loads(data, object_pairs_hook=object_pairs, parse_constant=constant)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def source_digest(source_inputs):
    """Bind to the source inputs already verified by the task evidence gate."""
    return sha(json.dumps(sorted(source_inputs, key=lambda row: row["path"]),
                          sort_keys=True, separators=(",", ":")).encode())


def inside(root, name):
    require(text(name) and not Path(name).is_absolute(), "Expected repository-relative path")
    path = (root / name).resolve()
    require(path.is_relative_to(root), "Coverage artifact escapes repository")
    return path


class Artifacts:
    def __init__(self, root):
        self.root = root.resolve()
        self.verified = {}

    def locate(self, item, allow_empty=False):
        fields(item, "path sha256", "artifact")
        path = inside(self.root, item["path"])
        require(matches(item["sha256"], SHA) and path.is_file() and (allow_empty or path.stat().st_size > 0),
                "Missing, empty or unhashed coverage artifact")
        return path

    def path(self, item, allow_empty=False):
        path = self.locate(item, allow_empty)
        if path not in self.verified:
            with path.open("rb") as stream:
                self.verified[path] = hashlib.file_digest(stream, "sha256").hexdigest()
        require(self.verified[path] == item["sha256"], f"Coverage artifact hash mismatch: {item['path']}")
        return path

    def bytes(self, item):
        path = self.locate(item)
        with path.open("rb") as stream:
            data = stream.read(MAX_JSON + 1)
        require(len(data) <= MAX_JSON and sha(data) == item["sha256"],
                "Coverage text artifact exceeds bound or has a different hash")
        return data

    def json(self, item):
        return read_json(self.bytes(item))


def format_one(value):
    return type(value) is int and value == 1


def same_config(actual, expected):
    return actual == expected and type(actual["occupancy"]) is int


def load_contracts(contract_bytes, cases):
    require(set(contract_bytes) == set(CONTRACTS), "Incomplete primitive contracts")
    inventory, requirements, schema = (read_json(contract_bytes[name]) for name in CONTRACTS)
    fields(inventory, "format status sdk_roots entries", "primitive inventory")
    require(format_one(inventory["format"]) and inventory["status"] in ("partial", "reviewed"),
            "Invalid primitive inventory version/status")
    require(isinstance(inventory["sdk_roots"], dict) and set(inventory["sdk_roots"]) == SDK_ROOTS
            and all(matches(digest, SHA) for digest in inventory["sdk_roots"].values()),
            "Incomplete SDK root provenance")
    entries = array(inventory["entries"], 1024, "primitive inventory")
    seen_ids, seen_symbols, refusals = set(), set(), {}
    for entry in entries:
        fields(entry, "id family symbols owners schemas bindings refusals state provider paths permission proof ordering recovery case",
               "primitive entry")
        ident = entry["id"]
        require(matches(ident, ID) and ident not in seen_ids, "Duplicate/invalid primitive ID")
        seen_ids.add(ident)
        require(text(entry["family"]) and entry["family"] in FAMILIES
                and entry["state"] in ("replicated", "ephemeral", "stateless"),
                "Unclassified primitive family/state")
        require(entry["provider"] in ("none", "rustfs", "container", "external-service"),
                "Unclassified primitive provider")
        symbols = unique(array(entry["symbols"], 2048, "public symbols"), "public symbol")
        require(all(symbol.startswith("cellar::sdk::") and text(symbol) for symbol in symbols)
                and not symbols & seen_symbols, "Invalid/duplicate qualified public API")
        seen_symbols.update(symbols)
        require(isinstance(entry["refusals"], dict) and set(entry["refusals"]) <= symbols
                and all(text(code) for code in entry["refusals"].values()), "Invalid documented refusal inventory")
        refusals.update(entry["refusals"])
        require(all(text(path) for path in array(entry["owners"], 32, "primitive owners")),
                "Invalid primitive owners")
        for key in ("schemas", "bindings"):
            require(isinstance(entry[key], list) and len(entry[key]) <= 32 and all(text(value) for value in entry[key]),
                    f"Invalid primitive {key}")
        paths = unique(array(entry["paths"], 5, "public paths"), "public path")
        require(paths <= {"sdk", "native", "axum", "cli", "admin"}, "Unknown public path")
        require(all(text(entry[key]) for key in ("permission", "proof", "ordering", "recovery")),
                "Missing primitive contract")
        require(matches(entry["case"], CASE) and entry["case"] in cases, "Unknown primitive case")
    require({entry["family"] for entry in entries} == FAMILIES, "Missing required primitive family")
    require(all(refusals.get(symbol) == code for symbol, code in DOCUMENTED_REFUSALS.items()),
            "Missing documented SDK refusal")
    require(STATEFUL_FAMILIES <= {entry["family"] for entry in entries if entry["state"] == "replicated"},
            "Missing stateful primitive family coverage")
    require(any(entry["family"] == "container" and entry["provider"] == "container" for entry in entries)
            and any(entry["family"] == "payload" and entry["provider"] == "rustfs" for entry in entries),
            "Missing required real provider")

    fields(requirements, "format axes features scenarios", "primitive requirements")
    require(format_one(requirements["format"]) and requirements["axes"] == AXES
            and all(type(value) is int for value in requirements["axes"]["occupancy"])
            and requirements["features"] == list(FEATURES), "Incomplete primitive configuration axes")
    require(requirements["scenarios"] == {
        key: {"case": case, "faults": sorted(faults)} for key, (case, faults) in SCENARIOS.items()
    }, "Incomplete primitive fault schedule")
    require(all(case is None or case in cases for case, _ in SCENARIOS.values()),
            "Primitive fault case is not in the task plan")
    require({"primitives_feature_modes_and_cutover", "primitives_fleet_bucket_regression_and_no_fallback"} <= cases,
            "Missing primitive backend/feature cases")
    require(isinstance(schema, dict) and schema.get("$id") == "urn:cellar:primitive-coverage:1",
            "Invalid primitive coverage schema")
    require(sum(1 for _ in cells(inventory)) <= MAX_CELLS, "Primitive coverage matrix exceeds report bound")
    return inventory


def cells(inventory):
    """Expand the reviewed axes. A report cannot declare its own applicability."""
    for entry in inventory["entries"]:
        state = entry["state"]
        scenarios = tuple(SCENARIOS) if state == "replicated" else (
            ("retry", "leader", "movement", "shutdown") if state == "ephemeral" else ("retry",))
        for scenario, image, namespace, minimum, occupancy, path in itertools.product(
                scenarios, AXES["image"], AXES["namespace"], AXES["minimum"], AXES["occupancy"], entry["paths"]):
            config = {"backend": "cell-group-raft", "feature": "native", "image": image, "namespace": namespace,
                      "minimum": minimum, "occupancy": occupancy, "path": path, "provider": entry["provider"]}
            case, faults = SCENARIOS[scenario]
            if state != "replicated" and scenario == "retry":
                faults = set()
            yield cell_id(entry, scenario, config), (entry, scenario, case or entry["case"], config, faults)

        # Preserve ordinary durability and feature configurations independently
        # of the RF3 matrix; a Fleet result cannot satisfy a Raft cell.
        for backend, feature in itertools.product(("fleet", "bucket"), FEATURES):
            config = {"backend": backend, "feature": feature, "image": "ordinary", "namespace": "ordinary",
                      "minimum": backend, "occupancy": 1, "path": "sdk", "provider": entry["provider"]}
            case = ("primitives_fleet_bucket_regression_and_no_fallback" if feature == "native"
                    else "primitives_feature_modes_and_cutover")
            yield cell_id(entry, "compatibility", config), (entry, "compatibility", case, config, set())
        for feature in ("native-no-default", "mixed"):
            config = {"backend": "cell-group-raft", "feature": feature, "image": "materialized",
                      "namespace": "managed", "minimum": "quorum", "occupancy": 1,
                      "path": entry["paths"][0], "provider": entry["provider"]}
            case = "primitives_feature_modes_and_cutover"
            yield cell_id(entry, "compatibility", config), (entry, "compatibility", case, config, set())


def cell_id(entry, scenario, config):
    return "/".join(map(str, (entry["id"], scenario, config["backend"], config["feature"], config["image"],
                              config["namespace"], config["minimum"], config["occupancy"], config["path"],
                              config["provider"])))


def run_provenance(run, revision):
    return {"cell": run["cell"], "source_revision": revision, "configuration": run["configuration"],
            "observations_sha256": run["observations"]["sha256"], "ledger_sha256": run["ledger"]["sha256"]}


def exact_test(output, case, expected):
    # --nocapture can put the first proof after libtest's unfinished test line.
    # Preserve that prefix while removing only the structured evidence records.
    pattern = re.compile(r"^(test \S+ \.\.\. )?cellar_primitive_evidence (\{[^\n]+\})\n", re.M)
    markers = {}
    for match in pattern.finditer(output):
        marker = read_json(match[2].encode())
        fields(marker, "cell source_revision configuration observations_sha256 ledger_sha256", "test evidence marker")
        ident = marker["cell"]
        require(text(ident) and ident not in markers, "Invalid/duplicate test evidence marker")
        markers[ident] = marker
    output = pattern.sub(lambda match: match[1] or "", output)
    require(re.findall(r"^running (\d+) tests?$", output, re.M) == ["1"],
            "Primitive command did not select exactly one test")
    require(re.findall(r"^test (\S+) \.\.\. (?:ok|FAILED|ignored)(?: .*)?$", output, re.M) == [case]
            and re.search(rf"^test {re.escape(case)} \.\.\. ok$", output, re.M) is not None
            and len(re.findall(r"^test result:", output, re.M)) == 1
            and re.search(r"^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured;", output, re.M),
            "Primitive command did not pass its exact required case")
    require(markers.get(expected["cell"]) == expected,
            "Primitive test output lacks matching configuration and artifact provenance")


def check_observations(observation, config, binaries, revision, case, faults):
    fields(observation, "format source_revision case configuration binaries groups processes events",
           "physical observations")
    require(format_one(observation["format"]) and observation["source_revision"] == revision
            and observation["case"] == case and same_config(observation["configuration"], config)
            and observation["binaries"] == {role: binary["artifact"]["sha256"] for role, binary in binaries.items()},
            "Physical observations do not match the selected source, binaries or configuration")
    processes = observation["processes"]
    groups = observation["groups"]
    require(isinstance(processes, list) and len(processes) <= 64 and isinstance(groups, list) and len(groups) <= 1024,
            "Invalid physical process/group inventory")
    if config["backend"] == "cell-group-raft":
        require(len(processes) >= 3 and len(groups) >= 2, "Missing distinct RF3 processes or group histories")
    for process in processes:
        fields(process, "id disk cgroup cpu_millis memory_bytes voter_sha256", "voter process")
        require(matches(process["id"], SHA) and text(process["disk"]) and text(process["cgroup"])
                and type(process["cpu_millis"]) is int and 0 < process["cpu_millis"] <= 1000
                and type(process["memory_bytes"]) is int and 0 < process["memory_bytes"] <= 3 * 1024**3
                and process["voter_sha256"] == binaries["voter"]["artifact"]["sha256"],
                "Unconstrained or unbound voter process")
    voters = unique([process["id"] for process in processes], "voter process")
    unique([process["disk"] for process in processes], "voter disk")
    unique([process["cgroup"] for process in processes], "voter cgroup")
    histories, source_voters = [], set()
    for group in groups:
        fields(group, "history voters cells", "replicated group")
        require(text(group["history"]) and type(group["cells"]) is int and group["cells"] == config["occupancy"]
                and len(unique(array(group["voters"], 3, "group voters"), "group voter")) == 3
                and set(group["voters"]) <= voters, "Invalid RF3 group/occupancy proof")
        histories.append(group["history"])
        source_voters.update(group["voters"])
    unique(histories, "group history")
    events = observation["events"]
    require(isinstance(events, list) and len(events) <= 16384, "Invalid observed fault schedule")
    seen, erased, fenced = set(), set(), set()
    previous = 0
    for event in events:
        fields(event, "sequence kind subjects", "fault event")
        require(type(event["sequence"]) is int and event["sequence"] > previous
                and matches(event["kind"], ID), "Invalid or unordered fault event")
        previous = event["sequence"]
        subjects = unique(array(event["subjects"], 1024, "fault subjects"), "fault subject")
        require(all(text(subject) for subject in subjects), "Empty fault subject")
        if event["kind"] in ("leader-killed", "voter-disk-erased", "old-writers-fenced"):
            require(subjects <= voters, "Fault targets an unobserved voter process")
        if event["kind"] == "voter-disk-erased":
            erased |= subjects
        if event["kind"] == "old-writers-fenced":
            fenced |= subjects
        seen.add(event["kind"])
    require(faults <= seen, f"Missing physical fault observations: {sorted(faults - seen)}")
    if "voter-disk-erased" in faults:
        require(source_voters <= erased and source_voters <= fenced,
                "Disaster observation omits source voter fence/disk erasure")


def check_ledger(ledger, entry, scenario, config, revision, case, artifacts):
    fields(ledger, "format source_revision case configuration records payloads", "state ledger")
    require(format_one(ledger["format"]) and ledger["source_revision"] == revision and ledger["case"] == case
            and same_config(ledger["configuration"], config), "State ledger provenance/configuration mismatch")
    symbols, observed_symbols, identities = set(entry["symbols"]), set(), set()
    committed = 0
    for row in array(ledger["records"], 100000, "state reconciliation records"):
        fields(row, "symbol identity expected observed proof duplicate_commits", "state reconciliation")
        symbol = row["symbol"]
        require(text(symbol) and symbol in symbols, "State ledger substitutes public operations")
        observed_symbols.add(symbol)
        require(text(row["identity"]) and row["identity"] not in identities, "Duplicate/empty operation identity")
        identities.add(row["identity"])
        expected = row["expected"]
        fields(expected, "outcome result_sha256 state_sha256 error_code", "expected state")
        require(expected["outcome"] in ("committed", "observed", "refused", "unresolved")
                and matches(expected["result_sha256"], SHA) and matches(expected["state_sha256"], SHA)
                and (expected["error_code"] is None or text(expected["error_code"]))
                and row["observed"] == expected, "State/result or unresolved outcome mismatch")
        require(expected["outcome"] != "refused" or text(expected["error_code"]), "Missing typed primitive refusal")
        if symbol in entry["refusals"]:
            require(expected["outcome"] == "refused" and expected["error_code"] == entry["refusals"][symbol],
                    "Documented primitive refusal changed")
        require(type(row["duplicate_commits"]) is int and row["duplicate_commits"] == 0,
                "Duplicate committed primitive mutation")
        if expected["outcome"] == "committed":
            committed += 1
            allowed = ({"quorum", "s3"} if config["minimum"] == "quorum" else {config["minimum"]})
            require(text(row["proof"]) and row["proof"] in allowed,
                    "Primitive committed without the required durability proof")
        else:
            require(row["proof"] in ("none", "quorum", "s3", "fleet", "bucket", "provider"), "Unknown proof kind")
    if entry["state"] == "replicated":
        require(committed > 0, "Stateful primitive has no reconciled committed operation")
    require(observed_symbols == symbols, "State ledger omits or substitutes public operations")
    payloads = ledger["payloads"]
    require(isinstance(payloads, list) and len(payloads) <= 16384, "Invalid payload evidence")
    if entry["family"] in ("payload", "filesystem", "container") and scenario in ("archive", "disaster"):
        require(payloads, "Payload recovery requires restored bytes")
    for payload in payloads:
        fields(payload, "expected_sha256 restored", "payload reconciliation")
        artifacts.path(payload["restored"], allow_empty=True)
        require(payload["restored"]["sha256"] == payload["expected_sha256"], "Restored payload bytes differ")


def verify_report(root, report_item, contract_bytes, cases, revision, source_inputs, test_sha256):
    inventory = load_contracts(contract_bytes, cases)
    require(inventory["status"] == "reviewed", "Primitive API inventory is incomplete")
    require(matches(revision, REVISION), "Invalid qualified source revision")
    inputs = {row["path"]: row["sha256"] for row in source_inputs}
    require(all(inputs.get(path) == digest for path, digest in inventory["sdk_roots"].items()),
            "Reviewed SDK roots differ from the verified source inputs")
    artifacts = Artifacts(root)
    report = artifacts.json(report_item)
    fields(report, "format status source_revision source_inputs_sha256 contracts binaries runs", "coverage report")
    require(format_one(report["format"]) and report["status"] == "acceptance"
            and report["source_revision"] == revision and report["source_inputs_sha256"] == source_digest(source_inputs)
            and report["contracts"] == {name: sha(data) for name, data in contract_bytes.items()},
            "Coverage report source, status or contract digests differ")

    binaries = {}
    for binary in array(report["binaries"], 128, "public executables"):
        fields(binary, "id role source_revision feature artifact", "executable manifest")
        require(matches(binary["id"], ID) and binary["id"] not in binaries
                and text(binary["role"]) and binary["role"] in ROLES
                and binary["source_revision"] == revision and binary["feature"] in FEATURES,
                "Duplicate, mixed-source or unsupported executable manifest")
        artifacts.path(binary["artifact"])
        binaries[binary["id"]] = binary

    expected = dict(cells(inventory))
    runs = array(report["runs"], len(expected), "primitive runs")
    covered = set()
    for run in runs:
        fields(run, "cell case configuration binaries command observations ledger", "primitive run")
        cell = run["cell"]
        require(text(cell) and cell in expected and cell not in covered, "Unknown/duplicate primitive coverage cell")
        covered.add(cell)
    require(covered == set(expected), f"Missing {len(set(expected) - covered)} mandatory primitive coverage cells")
    for run in runs:
        entry, scenario, case, config, faults = expected[run["cell"]]
        require(run["case"] == case and same_config(run["configuration"], config),
                "Mislabeled primitive case/configuration")
        require(isinstance(run["binaries"], dict) and set(run["binaries"]) == ROLES, "Missing public executable role")
        selected = {}
        for role, ident in run["binaries"].items():
            require(matches(ident, ID) and ident in binaries and binaries[ident]["role"] == role
                    and binaries[ident]["feature"] == config["feature"], "Wrong executable role/feature mode")
            selected[role] = binaries[ident]
        require(config["feature"] != "native" or selected["test"]["artifact"]["sha256"] == test_sha256,
                "Native primitive test executable differs from task evidence")
        command = run["command"]
        fields(command, "argv exit_code output", "primitive command")
        require(type(command["exit_code"]) is int and command["exit_code"] == 0 and command["argv"] == [
            selected["test"]["artifact"]["path"], case, "--exact", "--test-threads=1", "--nocapture"
        ], "Failed or non-exact primitive command")
        observations = artifacts.json(run["observations"])
        ledger = artifacts.json(run["ledger"])
        exact_test(artifacts.bytes(command["output"]).decode(), case, run_provenance(run, revision))
        check_observations(observations, config, selected, revision, case, faults)
        check_ledger(ledger, entry, scenario, config, revision, case, artifacts)
    return {"entries": len(inventory["entries"]), "covered_cells": len(covered)}


def check_catalog(root):
    package = root / PACKAGE
    contracts = {name: (package / name).read_bytes() for name in CONTRACTS}
    tasks = read_json((package / "tasks.json").read_bytes())["tasks"]
    task = next(task for task in tasks if task["id"] == "CG-17")
    inventory = load_contracts(contracts, {case["name"] for case in task["test_cases"]})
    for path, digest in inventory["sdk_roots"].items():
        require(sha((root / path).read_bytes()) == digest, f"SDK root changed; update primitive inventory: {path}")
    for entry in inventory["entries"]:
        for owner in entry["owners"]:
            require(inside(root.resolve(), owner).is_file(), f"Missing primitive owner: {owner}")
    return {"inventory_status": inventory["status"], "entries": len(inventory["entries"]),
            "public_symbols": sum(len(entry["symbols"]) for entry in inventory["entries"]),
            "required_cells_for_listed_entries": sum(1 for _ in cells(inventory))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["check-catalog"])
    parser.parse_args()
    try:
        print(json.dumps(check_catalog(ROOT), indent=2))
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(f"primitive coverage: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
