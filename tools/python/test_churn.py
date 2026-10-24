# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real HTTP loss/reconciliation and refusal of corrupted churn evidence."""
import argparse
from contextlib import contextmanager
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import churn
import churn_report


@contextmanager
def counters(drop_cell=1, drop_value=1, initial=None):
    values = dict(initial or {})

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            self.handle_value(False)

        def do_POST(self):
            self.handle_value(True)

        def handle_value(self, mutate):
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            if self.path == "/echo":
                payload = body
            else:
                cell = int(self.path.rsplit("-", 1)[1])
                if mutate:
                    values[cell] = values.get(cell, 0) + 1
                    if cell == drop_cell and values[cell] == drop_value:
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                payload = json.dumps({"value": values.get(cell, 0)}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01})
    thread.start()
    try:
        yield SimpleNamespace(origin=f"http://127.0.0.1:{server.server_port}"), values
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("HTTP fixture did not join")


class ClientFailures(unittest.TestCase):
    def test_committed_lost_response_is_not_replayed(self):
        with counters() as (host, values):
            stage = {}
            with self.assertRaises(ConnectionError):
                churn.round_trip(host, 2, 1, 1, stage)
            self.assertEqual(values, {0: 1, 1: 1})
            self.assertEqual([r["outcome"] for r in stage["attempts"][0]], ["acknowledged", "unknown"])
            ledger = churn.bounds(2, [stage])
            self.assertEqual(ledger[1], {"acknowledged": 0, "attempted": 1})
            recovery = {}
            churn.recover(host, ledger, recovery)
            self.assertEqual([r["value"] for r in recovery["reads"]], [1, 1])
            self.assertEqual([r["value"] for r in recovery["new_writes"]], [2, 2])
            self.assertEqual(values, {0: 2, 1: 2})

    def test_fresh_recovery_write_keeps_its_unknown_attempt(self):
        with counters(drop_cell=0, drop_value=2, initial={0: 1}) as (host, values):
            recovery = {}
            with self.assertRaises(ConnectionError):
                churn.recover(host, {0: {"acknowledged": 1, "attempted": 1}}, recovery)
            self.assertEqual(recovery["new_writes"], [{"cell": 0, "expected_value": 2, "outcome": "unknown"}])
            self.assertEqual(values[0], 2)

    def test_successful_recovery_does_not_erase_load_failure(self):
        with counters() as (http, values), tempfile.TemporaryDirectory() as directory:
            class Host:
                def __init__(self, executable, mode, root, role):
                    self.root, self.role, self.origin = root, role, http.origin
                    self.process = SimpleNamespace(returncode=None, poll=lambda: None)
                    self.ready = None

                def readiness(self):
                    if self.role == "recovered":
                        self_assert.assertFalse((self.root / "disk").exists())
                    (self.root / "disk").mkdir()
                    self.ready = dict(max_resident=1, durability="bucket", embed_compiled=False,
                                      debug_assertions=True, lease_ttl_ms=0)
                    return 1

                def hard_kill(self):
                    self.process.returncode = -9

                def stop(self):
                    return {"complete": True}

                def cleanup(self):
                    pass

            self_assert = self
            args = SimpleNamespace(population=2, concurrency=1, rounds=2, build_profile="dev")
            case = {}
            with patch.object(churn, "Host", Host), patch.object(churn, "settled", return_value={}):
                churn.run_case({"path": "unused", "mode": "native"}, Path(directory), args, case, lambda: None)
            self.assertEqual(case["status"], "failed")
            self.assertIn("RemoteDisconnected", case["load_failure"])
            self.assertTrue(case["owner_disk_removed"])
            self.assertEqual(values, {0: 2, 1: 2})
            self.assertEqual(case["shutdown"], {"complete": True})

    def test_counter_type_is_exact(self):
        for raw in (b'{"value":true}', b'{"value":1.0}', b'{"value":-1}', b'{"value":1,"extra":0}', b'1'):
            with self.subTest(raw=raw), self.assertRaises(RuntimeError):
                churn.counter(raw)


class ReportFailures(unittest.TestCase):
    document = None

    def test_original_evidence(self):
        churn_report.verify(self.document)

    def test_corrupted_evidence_is_not_reported(self):
        def python_case(document):
            labels = {a["label"] for a in document["artifacts"] if a["mode"] == "python"}
            return next(c for c in document["cases"] if c["runtime"] in labels)
        mutations = [
            lambda d: d["cases"][0].update(load_failure="lost response"),
            lambda d: d["cases"][0]["rounds"][0]["attempts"][0][0].update(outcome="unknown"),
            lambda d: d["cases"][0]["rounds"][0]["attempts"][0][0].update(response={"value": True}),
            lambda d: d["cases"][0]["rounds"][0]["latency_ns"].update(p99=0),
            lambda d: d["cases"][0]["recovery"]["reads"][0].update(value=0),
            lambda d: d["cases"][0]["recovery"]["new_writes"].pop(),
            lambda d: d["cases"][0]["host"].update(lease_ttl_ms=30000),
            lambda d: d["cases"][0].update(owner_exit_before_kill=1),
            lambda d: d["cases"][0]["shutdown"]["native"].update(local_durability_closed=False),
            lambda d: python_case(d)["rounds"][0]["after"]["runtime"]["interpreter"]["work"].update(pending_retirements=1),
            lambda d: python_case(d)["rounds"][0]["after"]["runtime"]["interpreter"].update(age_ns=2_000_000_000),
            lambda d: d["cases"].pop(),
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index):
                document = deepcopy(self.document)
                mutation(document)
                with self.assertRaises(ValueError):
                    churn_report.tables(document)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientFailures)
    if args.evidence:
        ReportFailures.document = json.loads(args.evidence.read_text())
        suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(ReportFailures))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
