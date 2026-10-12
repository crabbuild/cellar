"""The harness must reject a successful process exit without a drain proof."""

import json
import pathlib
import tempfile
import types
import unittest
from unittest import mock

from run import Harness, shutdown_proof, validate_shutdown


class RoutingCacheProofTests(unittest.TestCase):
    def harness(self, output, limit):
        return Harness(types.SimpleNamespace(output=str(output), port=39000,
                                             max_resident=16, max_requests=512,
                                             max_cached_routes=limit))

    def test_ready_node_with_wrong_or_missing_captured_policy_is_rejected(self):
        for fields in ({}, {"max_resident": 16, "max_requests": 512, "max_cached_routes": 2048}):
            with self.subTest(fields=fields), tempfile.TemporaryDirectory() as directory:
                harness = self.harness(pathlib.Path(directory) / "run", 0)
                text = json.dumps(dict(fields, event="embedded_node_started"))
                with mock.patch.object(harness, "wait_url"), mock.patch("run.logs", return_value=text):
                    with self.assertRaises(RuntimeError):
                        harness.wait_node("one_node_bucket", 0)
                self.assertEqual(harness.node_policies, [])

    def test_zero_cache_allows_retained_ownership_and_checks_only_running_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(pathlib.Path(directory) / "run", 0)
            state = {"cached_routes": 0, "owned_cells": 16, "occupied": 16, "phases": {"resident": 16}}
            with mock.patch("run.request", return_value=json.dumps(state)) as request, mock.patch("run.command", return_value="disk capture"):
                harness.snapshot("three_node_fleet", 3, "recovered", exclude=0)
            self.assertEqual(request.call_count, 2)
            self.assertEqual([v["node"] for v in harness.routing_cache_snapshots], [2, 3])
            self.assertTrue(all(v["cached_routes"] == 0 for v in harness.routing_cache_snapshots))
            self.assertEqual(harness.failures, [])

    def test_missing_or_invalid_cache_observation_aborts_and_preserves_evidence(self):
        for state in ({}, {"cached_routes": 33}, {"cached_routes": -1}, {"cached_routes": True}):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as directory:
                harness = self.harness(pathlib.Path(directory) / "run", 32)
                text = json.dumps(state)
                with mock.patch("run.request", return_value=text), mock.patch("run.command") as command:
                    with self.assertRaises(RuntimeError):
                        harness.snapshot("one_node_bucket", 1, "warm")
                    command.assert_not_called()
                self.assertEqual((harness.output / "one_node_bucket-warm-node1-state.json").read_text(), text)
                self.assertEqual(harness.routing_cache_snapshots, [])
                self.assertEqual(harness.failures[0]["operation"], "state_snapshot")

    def test_unavailable_state_cannot_be_downgraded_to_a_telemetry_warning(self):
        with tempfile.TemporaryDirectory() as directory:
            harness = self.harness(pathlib.Path(directory) / "run", 32)
            with mock.patch("run.request", side_effect=TimeoutError("state unavailable")):
                with self.assertRaises(RuntimeError):
                    harness.snapshot("one_node_bucket", 1, "warm")
            self.assertEqual(harness.routing_cache_snapshots, [])
            self.assertEqual(harness.failures[0]["error"], "state unavailable")


class ShutdownProofTests(unittest.TestCase):
    def proof(self, mode="handoff"):
        return {
            "event": "embedded_node_stopped", "outcome": "stopped", "mode": mode,
            "ingress_drained": True, "application_work_drained": True,
            "cells_drained": True, "log_quiesced": True,
            "local_durability_closed": True, "caller_executors_alive": True,
            "pending_application_events": 0, "pending_dispatches": 0,
            "drain_timed_out": False, "deadline_elapsed": False,
            "clean_reload_prepared": mode == "preserve",
        }

    def test_successful_stop_modes(self):
        for mode in ("handoff", "preserve"):
            with self.subTest(mode=mode):
                report = self.proof(mode)
                text = 'warning from stderr\n{"event":"embedded_node_started"}\n' + json.dumps(report)
                self.assertEqual(shutdown_proof(text), report)
                validate_shutdown(report, 0, mode)

    def test_missing_and_duplicate_outcomes_are_not_proofs(self):
        self.assertIsNone(shutdown_proof("shutdown_handoff_stalled\n"))
        report = json.dumps(self.proof())
        self.assertIsNone(shutdown_proof(report + "\n" + report))
        with self.assertRaises(RuntimeError):
            validate_shutdown(None, 0, "handoff")

    def test_forced_or_unproven_phases_are_rejected(self):
        failures = {
            "ingress_drained": False, "application_work_drained": False,
            "cells_drained": False, "log_quiesced": False,
            "local_durability_closed": False, "caller_executors_alive": False,
            "pending_application_events": 1, "pending_dispatches": 1,
            "drain_timed_out": True, "deadline_elapsed": True,
            "outcome": "Cancelled", "mode": "preserve",
        }
        for field, value in failures.items():
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                validate_shutdown(dict(self.proof(), **{field: value}), 0, "handoff")
        with self.assertRaises(RuntimeError):
            validate_shutdown(self.proof(), 3, "handoff")
        with self.assertRaises(RuntimeError):
            validate_shutdown(dict(self.proof("preserve"), clean_reload_prepared=False), 0, "preserve")

    def test_missing_phase_fields_are_rejected(self):
        for field in self.proof():
            if field in ("event", "clean_reload_prepared"):
                continue
            proof = self.proof()
            del proof[field]
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                validate_shutdown(proof, 0, "handoff")


if __name__ == "__main__":
    unittest.main()
