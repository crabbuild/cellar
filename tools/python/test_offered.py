#!/usr/bin/env python3
"""Exercise the built native arrival client against controlled HTTP failures."""
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from offered import observed_idle, summarize

CLIENT = Path(sys.argv.pop(1)).resolve(strict=True) if len(sys.argv) > 1 else None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except ConnectionResetError:
            # The client deliberately drops unread failed/oversized replies
            # and closes its keepalive pool after each finite workload.
            pass

    def do_POST(self):
        # A zero-byte HTTP/1.1 request may omit Content-Length entirely.
        if self.headers.get("transfer-encoding") is not None:
            raise ValueError("this fixture expects fixed-length request bodies")
        body = self.rfile.read(int(self.headers.get("content-length", "0")))
        with self.server.lock:
            self.server.received += 1
        if self.server.mode == "slow":
            time.sleep(.08)
        if self.server.mode == "timeout":
            time.sleep(.2)
        if self.server.mode == "wrong":
            body += b"invalid trailing byte"
        code = 307 if self.server.mode == "redirect" else 200
        self.send_response(code)
        self.send_header("content-length", str(len(body)))
        if code == 307:
            self.send_header("location", "/echo")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


@unittest.skipIf(CLIENT is None, "pass the built python-offered-load executable")
class NativeArrivalTests(unittest.TestCase):
    def test_idle_telemetry_from_before_the_workload_is_not_reused(self):
        class Host:
            calls = 0

            def observations(self):
                self.calls += 1
                return {"native_resident_cells": 0, "runtime": {"kind": "python",
                        "interpreter": {"age_ns": 0, "generation": 7 if self.calls < 3 else 8,
                                        "work": {"resident_objects": 0, "active_invocations": 0,
                                                 "pending_retirements": 0}}}}

        host = Host()
        self.assertEqual(observed_idle(host)["runtime"]["interpreter"]["generation"], 8)
        self.assertEqual(host.calls, 3)

    def run_client(self, mode, *, inflight=16, timeout=500, body_bytes=137):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.mode, server.received, server.lock = mode, 0, threading.Lock()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as root:
                output = Path(root) / "samples.json"
                command = [str(CLIENT), f"http://127.0.0.1:{server.server_port}",
                           "100", "500", str(inflight), str(body_bytes), str(timeout), "200", str(output)]
                result = subprocess.run(command, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr.decode())
                report = json.loads(output.read_text())
                summary = summarize(report)
                self.assertEqual(summary["arrivals"], 50)
                dispatched = sum(row["dispatched_ns"] is not None for row in report["samples"])
                self.assertGreater(dispatched, 0)
                with server.lock:
                    self.assertEqual(server.received, dispatched, "redirect or hidden resend")
                # Existing evidence must be refused before another request.
                again = subprocess.run(command, capture_output=True, timeout=10)
                self.assertNotEqual(again.returncode, 0)
                with server.lock:
                    self.assertEqual(server.received, dispatched)
                return report, summary
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_echo_and_raw_schedule_validation(self):
        report, summary = self.run_client("echo")
        self.assertGreater(summary["outcomes"].get("success", 0), 0)
        self.assertFalse(set(summary["outcomes"]) - {"success", "client_late", "client_capacity"})
        for change in ("missing", "schedule", "completion", "drop"):
            forged = copy.deepcopy(report)
            success = next(row for row in forged["samples"] if row["outcome"] == "success")
            if change == "missing":
                forged["samples"].pop()
            elif change == "schedule":
                success["scheduled_ns"] += 1
            elif change == "completion":
                success["completed_ns"] = success["dispatched_ns"] - 1
            else:
                success["outcome"] = "client_capacity"
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                summarize(forged)

    def test_slow_responses_do_not_delay_or_queue_arrivals(self):
        report, summary = self.run_client("slow", inflight=2)
        self.assertEqual(summary["peak_inflight"], 2)
        self.assertGreater(summary["outcomes"].get("client_capacity", 0), 0)
        self.assertGreater(summary["outcomes"].get("success", 0), 0)
        self.assertEqual(report["samples"][-1]["scheduled_ns"], 490_000_000)
        self.assertLess(sum(row["dispatched_ns"] is not None for row in report["samples"]), 25)

    def test_empty_echo_requires_successful_eof(self):
        _, summary = self.run_client("echo", body_bytes=0)
        self.assertGreater(summary["outcomes"].get("success", 0), 0)
        self.assertFalse(set(summary["outcomes"]) - {"success", "client_late", "client_capacity"})

    def test_http_redirects_are_not_followed(self):
        _, summary = self.run_client("redirect")
        self.assertGreater(summary["outcomes"].get("http_status", 0), 0)
        self.assertNotIn("success", summary["outcomes"])

    def test_body_mismatch_is_not_success(self):
        _, summary = self.run_client("wrong")
        self.assertGreater(summary["outcomes"].get("body_mismatch", 0), 0)
        self.assertNotIn("success", summary["outcomes"])

    def test_timeouts_remain_failed_attempts(self):
        _, summary = self.run_client("timeout", timeout=50)
        self.assertGreater(summary["outcomes"].get("timeout", 0), 0)
        self.assertNotIn("success", summary["outcomes"])


if __name__ == "__main__":
    if CLIENT is None:
        raise SystemExit("usage: test_offered.py /absolute/path/python-offered-load")
    unittest.main()
