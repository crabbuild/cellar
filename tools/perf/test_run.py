"""The harness must reject a successful process exit without a drain proof."""

import json
import unittest

from run import shutdown_proof, validate_shutdown


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
