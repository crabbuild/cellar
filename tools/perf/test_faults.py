"""Receipt verification must expose lost acknowledgements before any replay."""

import hashlib
import http.server
import json
import pathlib
import tempfile
import threading
import unittest
from unittest import mock

from faults import Ledger, LostResponse, audit_run, validate_reply, validate_snapshot


def operation(key="op1", ack=False, upstream=False):
    digest = hashlib.sha256(b"payload").hexdigest()
    receipt = {"sha256": digest, "ordinal": 1}
    return {"operation_id": key, "sha256": digest, "payload": "payload", "attempts": 1,
            "ack": receipt if ack else None, "upstream_ack": receipt if upstream else None, "observed": None}


class ReceiptOracleTests(unittest.TestCase):
    def state(self, op):
        return {"writes": 1, "receipts": {op["operation_id"]: {"sha256": op["sha256"], "ordinal": 1}}}

    def test_missing_client_acknowledgement_is_rejected_before_replay(self):
        with self.assertRaisesRegex(RuntimeError, "lost acknowledged"):
            validate_snapshot([operation(ack=True)], {"writes": 0, "receipts": {}})

    def test_discarded_upstream_acknowledgement_must_also_survive(self):
        with self.assertRaisesRegex(RuntimeError, "lost acknowledged"):
            validate_snapshot([operation(upstream=True)], {"writes": 0, "receipts": {}})

    def test_ambiguous_commit_may_be_present_or_absent_but_final_replay_is_exact(self):
        op = operation()
        absent = {"writes": 0, "receipts": {}}
        self.assertEqual(validate_snapshot([op], absent)["ambiguous_absent"], 1)
        self.assertEqual(validate_snapshot([op], self.state(op))["ambiguous_present"], 1)
        with self.assertRaises(RuntimeError):
            validate_snapshot([op], absent, final=True)
        self.assertEqual(validate_snapshot([op], self.state(op), final=True)["writes"], 1)

    def test_receipt_with_wrong_payload_is_not_a_durable_write(self):
        op = operation()
        state = self.state(op)
        state["receipts"]["op1"]["sha256"] = "0" * 64
        with self.assertRaises(RuntimeError):
            validate_snapshot([op], state)

    def test_counter_increment_without_distinct_receipt_is_rejected(self):
        op = operation()
        state = self.state(op)
        state["writes"] = 2
        with self.assertRaisesRegex(RuntimeError, "counter"):
            validate_snapshot([op], state)

    def test_duplicate_ordinals_cannot_hide_duplicate_execution(self):
        ops = [operation("a"), operation("b")]
        state = {"writes": 2, "receipts": {op["operation_id"]: {"sha256": op["sha256"], "ordinal": 1} for op in ops}}
        with self.assertRaisesRegex(RuntimeError, "counter increment"):
            validate_snapshot(ops, state)

    def test_receipts_require_an_issued_operation(self):
        op = operation()
        op["attempts"] = 0
        with self.assertRaisesRegex(RuntimeError, "never issued"):
            validate_snapshot([op], self.state(op))

    def test_recovery_and_retry_must_preserve_original_receipt(self):
        op = operation(ack=True)
        op["ack"] = dict(op["ack"], ordinal=2)
        with self.assertRaisesRegex(RuntimeError, "original receipt"):
            validate_snapshot([op], self.state(op))

    def test_boolean_counters_and_ordinals_are_not_numbers(self):
        op = operation()
        for value in ({"writes": True, "receipts": self.state(op)["receipts"]},
                      {"writes": 1, "receipts": {"op1": {"sha256": op["sha256"], "ordinal": True}}}):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                validate_snapshot([op], value)

    def test_wrong_operation_response_is_not_accepted(self):
        op = operation()
        with self.assertRaises(RuntimeError):
            validate_reply(op, {"operation_id": "wrong", "receipt": self.state(op)["receipts"]["op1"], "node": "node1"})

    def test_completion_flag_without_required_stage_evidence_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "metadata.json").write_text(json.dumps({"arguments": {"scenario": ["fleet"], "cells": 1}}))
            (root / "results.json").write_text(json.dumps({"completed": True, "failures": []}))
            with mock.patch("faults.verify_image_sources"), self.assertRaises(FileNotFoundError):
                audit_run(root)


class ResponseLossTests(unittest.TestCase):
    def test_real_client_socket_loses_response_after_upstream_acknowledges(self):
        class Backend(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                write = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                body = json.dumps({"operation_id": write["operation_id"],
                    "receipt": {"sha256": hashlib.sha256(write["payload"].encode()).hexdigest(), "ordinal": 1}, "node": "node1"}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        backend = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        thread = threading.Thread(target=backend.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory) / "operations.json"
                ledger = Ledger(path, cells=1, burst=1)
                try:
                    op = next(o for o in ledger.operations if o["phase"] == "lost-response")
                    upstream = f"http://127.0.0.1:{backend.server_port}/cells/0000"
                    with LostResponse(ledger, op, upstream) as url:
                        self.assertFalse(ledger.attempt(op, url))
                    self.assertIsNone(op["ack"])
                    self.assertIsNotNone(op["upstream_ack"])
                    self.assertFalse(ledger.pending)
                    ledger.save()
                    events = [json.loads(l)["event"] for l in path.with_suffix(".jsonl").read_text().splitlines()]
                    self.assertEqual(events, ["plan", "attempt", "upstream_ack_response_dropped", "ambiguous"])
                finally:
                    ledger.close()
        finally:
            backend.shutdown()
            backend.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
