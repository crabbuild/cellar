# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""A Python fleet result needs joined reports backed by each process log."""

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import types
import unittest

from analyze import summarize
from run import Harness, validate_python_outcomes, validate_python_shutdown, validate_python_startup


def startup(free=False):
    return {"event": "embedded_python_started", "version": "3.14.5 (fixture)",
            "free_threaded": free, "gil_enabled": not free,
            "limits": {"max_requests": 32, "max_invocations": 64,
                       "max_tasks": 1024, "max_objects": 4096,
                       "max_bridge_bytes": 33554432, "shutdown_timeout_ms": 30000}}


def reports(mode="preserve"):
    native = {"event": "embedded_node_stopped", "outcome": "stopped", "mode": mode,
              "ingress_drained": True, "application_work_drained": True,
              "pending_application_events": 0, "cells_drained": True, "pending_dispatches": 0,
              "log_quiesced": True, "local_durability_closed": True,
              "clean_reload_prepared": mode == "preserve", "drain_timed_out": False,
              "deadline_elapsed": False, "caller_executors_alive": True}
    observed = {key: value for key, value in native.items()
                if key not in ("event", "caller_executors_alive")}
    operations = dict.fromkeys(("pending", "queued", "running", "replies_pending",
                               "failed", "abandoned_queued", "abandoned_running", "undelivered"), 0)
    operations.update(admitted=7, started=7, succeeded=7, delivered=7)
    report = dict.fromkeys(("pending_tasks", "pending_objects", "pending_threads",
                           "unraisable_errors", "thread_errors"), 0)
    report.update(host_failed=False, runtime_failed=False, objects_created=2, objects_retired=2,
                  work=dict.fromkeys(("active_tasks", "active_invocations", "resident_objects",
                                      "pending_retirements"), 0),
                  bridge={"native_invocations": 0, "queued_bytes": 0, "pending_reservations": 0, "operations": operations},
                  native_outcomes=[dict(observed, complete=True)])
    return native, {"event": "embedded_python_stopped", "complete": True, "report": report}


def checkpoint_files(root):
    outcomes = []
    for label in ("one_node_bucket-drained", "one_node_bucket-preserve-reloaded"):
        native, python = reports()
        (root / f"{label}-node1.log").write_text(
            "\n".join(json.dumps(value) for value in (startup(), native, python)) + "\n")
        outcomes.append({"label": label, "node": 1, "exit_code": 0, "proof": native, "python": python})
    return outcomes


class PythonFleetProofTests(unittest.TestCase):
    def test_both_abis_require_observed_gil_and_exact_limits(self):
        for free in (False, True):
            value = startup(free)
            self.assertEqual(validate_python_startup(json.dumps(value), 32, 30000), value)
            for key, replacement in (("gil_enabled", free), ("free_threaded", int(free)),
                                      ("version", "3.13.0"), ("limits", {})):
                with self.subTest(key=key, free=free), self.assertRaises(RuntimeError):
                    validate_python_startup(json.dumps(dict(value, **{key: replacement})), 32, 30000)
        for text in ("", json.dumps(startup()) + "\n" + json.dumps(startup())):
            with self.assertRaises(RuntimeError):
                validate_python_startup(text, 32, 30000)

    def test_unsettled_work_and_inconsistent_native_outcomes_are_rejected(self):
        native, proof = reports()
        validate_python_shutdown(proof, native)
        changes = [(("report", key), value) for key, value in (
            ("pending_tasks", 1), ("pending_objects", True), ("pending_threads", 1),
            ("unraisable_errors", 1), ("thread_errors", 1), ("host_failed", True),
            ("runtime_failed", True), ("objects_retired", 1), ("native_outcomes", []))]
        changes += [(("report", "work", key), 1) for key in proof["report"]["work"]]
        changes += [(("report", "bridge", key), 1) for key in ("native_invocations", "queued_bytes", "pending_reservations")]
        changes += [(("report", "bridge", "operations", key), value) for key, value in (
            ("pending", 1), ("queued", 1), ("running", 1), ("replies_pending", 1),
            ("admitted", 8), ("started", 8), ("failed", -1), ("delivered", True))]
        for path, value in changes:
            bad = deepcopy(proof)
            parent = bad
            for key in path[:-1]:
                parent = parent[key]
            parent[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(RuntimeError):
                validate_python_shutdown(bad, native)
        with self.assertRaises(RuntimeError):
            validate_python_shutdown(proof, dict(native, mode="handoff"))

    def test_settled_abandoned_operations_are_not_misreported_as_pending(self):
        native, proof = reports()
        # Settlement records disposition; it does not prove an effect rolled back.
        proof["report"]["bridge"]["operations"].update(
            admitted=9, started=7, succeeded=4, failed=1, delivered=4,
            abandoned_queued=2, abandoned_running=2, undelivered=1)
        validate_python_shutdown(proof, native)

    def test_every_checkpoint_requires_matching_raw_log(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outcomes = checkpoint_files(root)
            def validate(values):
                validate_python_outcomes(root, values, ["one_node_bucket"], 32, 30000)
            validate(outcomes)
            for values in (outcomes[:1], outcomes + outcomes[:1], []):
                with self.assertRaises(RuntimeError):
                    validate(values)
            corrupted = deepcopy(outcomes)
            corrupted[0]["python"]["report"]["objects_created"] = 999
            with self.assertRaises(RuntimeError):
                validate(corrupted)
            path = root / "one_node_bucket-preserve-reloaded-node1.log"
            original = path.read_text()
            path.write_text(original.replace('"gil_enabled": true', '"gil_enabled": false'))
            with self.assertRaises(RuntimeError):
                validate(outcomes)
            path.unlink()
            with self.assertRaises(FileNotFoundError):
                validate(outcomes)

    def test_analyzer_rechecks_python_proofs_despite_completed_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = {"started_utc": "fixture", "revision": "fixture", "source_sha256": {},
                        "dirty": False, "image": "fixture", "rustfs_image": "fixture",
                        "node_limits": {}, "load_generator_limits": {}, "rustfs_limits": {},
                        "docker": {"Architecture": "arm64", "NCPU": 8, "MemTotal": 1},
                        "arguments": {"runtime": "python", "scenario": ["one_node_bucket"],
                                      "max_requests": 32, "shutdown_ms": 30000, "cells": 4,
                                      "bytes": 64, "seconds": 1, "repeats": 1, "concurrency": [1]}}
            (root / "metadata.json").write_text(json.dumps(metadata))
            results = {"completed": True, "measurements": [], "recovery": [],
                       "shutdown_outcomes": checkpoint_files(root)}
            (root / "results.json").write_text(json.dumps(results))
            self.assertTrue(summarize(root)["completed"])
            (root / "one_node_bucket-drained-node1.log").unlink()
            result = summarize(root)
            self.assertFalse(result["completed"])
            self.assertEqual(result["failures"][0]["operation"], "python_shutdown")

    def test_runtime_selection_preserves_explicit_fault_binary(self):
        for runtime, binary, expected in (("native", None, "cellar-perf-cells"),
                                          ("python", None, "python-fleet"),
                                          ("python", "fault-host", "fault-host")):
            with tempfile.TemporaryDirectory() as directory:
                args = types.SimpleNamespace(runtime=runtime, output=str(Path(directory) / "run"))
                self.assertEqual(Harness(args, binary).node_binary, expected)


if __name__ == "__main__":
    unittest.main()
