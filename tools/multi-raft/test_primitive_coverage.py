# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Synthetic report fixtures exercise refusal, never qualify the runtime."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

import primitive_coverage as coverage


class CoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="cellar-primitive-coverage-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.revision = "a" * 40
        package = coverage.ROOT / coverage.PACKAGE
        tasks = json.loads((package / "tasks.json").read_bytes())["tasks"]
        cls.cases = {case["name"] for task in tasks if task["id"] == "CG-17" for case in task["test_cases"]}
        cls.contracts = {name: (package / name).read_bytes() for name in coverage.CONTRACTS}
        # Only this synthetic fixture uses a reviewed catalogue. The checked-in
        # partial inventory must refuse even this otherwise complete report.
        cls.inventory = json.loads(cls.contracts["primitive-inventory.json"])
        cls.inventory["status"] = "reviewed"
        cls.contracts["primitive-inventory.json"] = cls.encode(cls.inventory)
        cls.inputs = [{"path": path, "sha256": digest} for path, digest in cls.inventory["sdk_roots"].items()]
        cls.expected = dict(coverage.cells(cls.inventory))
        cls.binaries = {}
        for feature in coverage.FEATURES:
            cls.binaries[feature] = {}
            for role in ("application", "voter", "cli", "test"):
                ident = f"{feature}-{role}"
                cls.binaries[feature][role] = {
                    "id": ident, "role": role, "source_revision": cls.revision, "feature": feature,
                    "artifact": cls.put(f"bin/{ident}", f"synthetic {feature} {role}".encode()),
                }
        cls.test_sha = cls.binaries["native"]["test"]["artifact"]["sha256"]
        cls.runs = []
        cls.payload = cls.put("payload.bin", b"known restored bytes")
        for index, (cell, (entry, scenario, case, config, faults)) in enumerate(cls.expected.items()):
            binaries = cls.binaries[config["feature"]]
            process_ids = [f"{n:064x}" for n in range(1, 6)]
            raft = config["backend"] == "cell-group-raft"
            header = {"format": 1, "source_revision": cls.revision, "case": case, "configuration": config}
            observations = {
                **header, "binaries": {role: binary["artifact"]["sha256"] for role, binary in binaries.items()},
                "groups": [{"history": "range-a/initial", "voters": process_ids[:3], "cells": config["occupancy"]},
                           {"history": "range-b/initial", "voters": process_ids[2:], "cells": config["occupancy"]}]
                if raft else [],
                "processes": [{"id": ident, "disk": f"disk-{ident}", "cgroup": f"cgroup-{ident}",
                               "cpu_millis": 1000, "memory_bytes": 3 * 1024**3,
                               "voter_sha256": binaries["voter"]["artifact"]["sha256"]}
                              for ident in process_ids] if raft else [],
                "events": [{"sequence": number, "kind": kind, "subjects": process_ids}
                           for number, kind in enumerate(sorted(faults), 1)],
            }
            records = []
            for number, symbol in enumerate(entry["symbols"]):
                error = entry["refusals"].get(symbol)
                durable = entry["state"] == "replicated" and number == 0 and error is None
                recorded = {"outcome": "refused" if error else "committed" if durable else "observed",
                            "result_sha256": "b" * 64, "state_sha256": "c" * 64, "error_code": error}
                records.append({"symbol": symbol, "identity": f"original-{index}-{number}",
                                "expected": recorded, "observed": recorded,
                                "proof": config["minimum"] if durable else "none", "duplicate_commits": 0})
            ledger = {**header, "records": records,
                      "payloads": [{"expected_sha256": cls.payload["sha256"], "restored": cls.payload}]
                      if entry["family"] in ("payload", "filesystem", "container") and scenario in ("archive", "disaster")
                      else []}
            test = binaries["test"]["artifact"]["path"]
            run = {"cell": cell, "case": case, "configuration": config,
                   "binaries": {role: binary["id"] for role, binary in binaries.items()},
                   "command": {"argv": [test, case, "--exact", "--test-threads=1", "--nocapture"], "exit_code": 0},
                   "observations": cls.put(f"observations/{index}.json", cls.encode(observations)),
                   "ledger": cls.put(f"ledgers/{index}.json", cls.encode(ledger))}
            run["command"]["output"] = cls.put(f"logs/{index}.log", cls.pass_log(run))
            cls.runs.append(run)

    @staticmethod
    def encode(value):
        return json.dumps(value, separators=(",", ":")).encode()

    @classmethod
    def pass_log(cls, run):
        marker = cls.encode({"cell": run["cell"], "source_revision": cls.revision,
                             "configuration": run["configuration"], "observations_sha256": run["observations"]["sha256"],
                             "ledger_sha256": run["ledger"]["sha256"]})
        return (f"\nrunning 1 test\ntest {run['case']} ... cellar_primitive_evidence ".encode() + marker
                + b"\nok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 31 filtered out; finished in 1.00s\n")

    @classmethod
    def put(cls, name, data):
        path = cls.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return {"path": name, "sha256": coverage.sha(data)}

    def report(self):
        return {"format": 1, "status": "acceptance", "source_revision": self.revision,
                "source_inputs_sha256": coverage.source_digest(self.inputs),
                "contracts": {name: coverage.sha(data) for name, data in self.contracts.items()},
                "binaries": [copy.deepcopy(binary) for roles in self.binaries.values() for binary in roles.values()],
                "runs": copy.deepcopy(self.runs)}

    def verify(self, report, contracts=None):
        return coverage.verify_report(self.root, self.put("report.json", self.encode(report)),
                                      contracts or self.contracts, self.cases, self.revision,
                                      self.inputs, self.test_sha)

    def edit_run(self, report, predicate):
        index = next(i for i, run in enumerate(report["runs"]) if predicate(run))
        # Put the changed case first so later valid artifact reads are unnecessary.
        run = report["runs"].pop(index)
        report["runs"].insert(0, run)
        return run

    def mutate_artifact(self, run, role, change):
        value = json.loads((self.root / run[role]["path"]).read_bytes())
        change(value)
        run[role] = self.put(f"changed-{role}.json", self.encode(value))
        run["command"]["output"] = self.put("changed-artifact.log", self.pass_log(run))

    def test_complete_reviewed_fixture(self):
        result = self.verify(self.report())
        self.assertEqual(result["covered_cells"], len(self.expected))

    def test_partial_inventory_cannot_qualify(self):
        contracts = dict(self.contracts)
        contracts["primitive-inventory.json"] = (coverage.ROOT / coverage.PACKAGE / "primitive-inventory.json").read_bytes()
        with self.assertRaisesRegex(coverage.CoverageError, "inventory is incomplete"):
            self.verify(self.report(), contracts)

    def test_removing_a_required_cell_never_redefines_applicability(self):
        for fragment in ("queue/leader", "/extents/", "/unmanaged/", "/s3/", "/python/", "/javascript/",
                         "/native-no-default/", "/mixed/", "/2/axum/", "/cli/", "/admin/", "/rustfs", "/container"):
            with self.subTest(fragment=fragment):
                report = self.report()
                run = self.edit_run(report, lambda row: fragment in row["cell"])
                report["runs"].remove(run)
                with self.assertRaisesRegex(coverage.CoverageError, "Missing 1 mandatory"):
                    self.verify(report)

    def test_duplicate_cell_and_backend_substitution_refuse(self):
        report = self.report()
        report["runs"][-1] = report["runs"][0]
        with self.assertRaisesRegex(coverage.CoverageError, "duplicate primitive coverage"):
            self.verify(report)
        report = self.report()
        report["runs"][0]["configuration"]["backend"] = "bucket"
        with self.assertRaisesRegex(coverage.CoverageError, "Mislabeled"):
            self.verify(report)

    def test_report_and_binary_provenance_are_checked(self):
        report = self.report()
        report["source_inputs_sha256"] = "0" * 64
        with self.assertRaisesRegex(coverage.CoverageError, "contract digests differ"):
            self.verify(report)
        report = self.report()
        report["binaries"][0]["source_revision"] = "d" * 40
        with self.assertRaisesRegex(coverage.CoverageError, "mixed-source"):
            self.verify(report)
        report = self.report()
        report["binaries"][0]["artifact"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(coverage.CoverageError, "artifact hash mismatch"):
            self.verify(report)

    def test_skipped_failed_and_zero_selected_tests_refuse(self):
        for outcome in (b"running 0 tests\n", b"running 1 test\ntest x ... ignored\n", b"running 1 test\ntest x ... FAILED\n"):
            with self.subTest(outcome=outcome):
                report = self.report()
                report["runs"][0]["command"]["output"] = self.put("wrong-test.log", outcome)
                with self.assertRaisesRegex(coverage.CoverageError, "exactly one|exact required"):
                    self.verify(report)

    def test_passed_log_cannot_be_reused_for_a_different_configuration(self):
        report = self.report()
        first = report["runs"][0]
        other = next(run for run in report["runs"] if run["case"] == first["case"]
                     and run["configuration"]["image"] == "extents")
        first["command"]["output"] = other["command"]["output"]
        with self.assertRaisesRegex(coverage.CoverageError, "artifact provenance"):
            self.verify(report)

    def test_missing_fault_and_partial_erasure_refuse(self):
        report = self.report()
        run = self.edit_run(report, lambda row: "/leader/" in row["cell"])
        self.mutate_artifact(run, "observations", lambda value: value["events"].pop())
        with self.assertRaisesRegex(coverage.CoverageError, "Missing physical fault"):
            self.verify(report)
        report = self.report()
        run = self.edit_run(report, lambda row: "/disaster/" in row["cell"])
        self.mutate_artifact(run, "observations", lambda value: next(event for event in value["events"]
                             if event["kind"] == "voter-disk-erased")["subjects"].pop())
        with self.assertRaisesRegex(coverage.CoverageError, "omits source voter"):
            self.verify(report)

    def test_missing_api_wrong_result_and_weaker_proof_refuse(self):
        for change, expected in (
                (lambda value: value["records"].pop(), "omits or substitutes public"),
                (lambda value: value["records"][0]["observed"].update(result_sha256="0" * 64), "outcome mismatch"),
                (lambda value: value["records"][0].update(proof="quorum"), "required durability proof")):
            report = self.report()
            run = self.edit_run(report, lambda row: row["cell"].startswith("cell-storage/retry/")
                                and row["configuration"]["minimum"] == "s3")
            self.mutate_artifact(run, "ledger", change)
            with self.assertRaisesRegex(coverage.CoverageError, expected):
                self.verify(report)

    def test_documented_refusal_is_a_typed_result_not_omitted_coverage(self):
        report = self.report()
        run = self.edit_run(report, lambda row: row["cell"].startswith("sql-binding/retry/"))

        def wrong_refusal(ledger):
            row = next(row for row in ledger["records"] if row["symbol"].endswith("Database::dump"))
            row["expected"]["error_code"] = row["observed"]["error_code"] = "UnsupportedCapability"

        self.mutate_artifact(run, "ledger", wrong_refusal)
        with self.assertRaisesRegex(coverage.CoverageError, "Documented primitive refusal changed"):
            self.verify(report)

    def test_payload_metadata_cannot_substitute_for_bytes(self):
        report = self.report()
        run = self.edit_run(report, lambda row: row["cell"].startswith("blob/disaster/"))
        self.mutate_artifact(run, "ledger", lambda value: value.update(payloads=[]))
        with self.assertRaisesRegex(coverage.CoverageError, "requires restored bytes"):
            self.verify(report)
        report = self.report()
        run = self.edit_run(report, lambda row: row["cell"].startswith("blob/disaster/"))
        wrong = self.put("different-payload.bin", b"wrong restored bytes")
        self.mutate_artifact(run, "ledger", lambda value: value["payloads"][0].update(restored=wrong))
        with self.assertRaisesRegex(coverage.CoverageError, "payload bytes differ"):
            self.verify(report)

    def test_catalog_cannot_drop_family_axis_or_provider(self):
        for name, change, expected in (
                ("primitive-inventory.json", lambda obj: obj.update(entries=[entry for entry in obj["entries"]
                                                                           if entry["family"] != "queue"]), "family"),
                ("primitive-coverage-requirements.json", lambda obj: obj["axes"]["image"].remove("extents"), "axes"),
                ("primitive-inventory.json", lambda obj: next(entry for entry in obj["entries"]
                                                              if entry["id"] == "container").update(provider="none"), "provider")):
            contracts = dict(self.contracts)
            value = json.loads(contracts[name])
            change(value)
            contracts[name] = self.encode(value)
            with self.assertRaisesRegex(coverage.CoverageError, expected):
                coverage.load_contracts(contracts, self.cases)

    def test_sdk_root_change_requires_catalog_review(self):
        root = self.root / "source"
        package = root / coverage.PACKAGE
        package.mkdir(parents=True)
        source_package = coverage.ROOT / coverage.PACKAGE
        (package / "tasks.json").write_bytes((source_package / "tasks.json").read_bytes())
        inventory = copy.deepcopy(self.inventory)
        for path in inventory["sdk_roots"]:
            self.put(f"source/{path}", b"pub use existing::Api;\n")
            inventory["sdk_roots"][path] = coverage.sha(b"pub use existing::Api;\n")
        for entry in inventory["entries"]:
            for owner in entry["owners"]:
                if not (root / owner).exists():
                    self.put(f"source/{owner}", b"synthetic owner\n")
        for name, data in self.contracts.items():
            (package / name).write_bytes(self.encode(inventory) if name == "primitive-inventory.json" else data)
        coverage.check_catalog(root)
        self.put("source/crates/cellar/sdk.rs", b"pub use existing::Api;\npub use new::Api;\n")
        with self.assertRaisesRegex(coverage.CoverageError, "SDK root changed"):
            coverage.check_catalog(root)


if __name__ == "__main__":
    unittest.main()
