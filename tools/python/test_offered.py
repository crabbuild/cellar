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

from offered import observed_idle, read_targets, summarize

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
        self.respond(body)

    def do_PATCH(self):
        body = self.rfile.read(int(self.headers.get("content-length", "0")))
        expected, reply = self.server.updates[self.path]
        if body != expected or self.headers.get("content-type") != "application/json":
            raise ValueError("incorrect mutation body or content type")
        self.respond(reply)

    def do_GET(self):
        self.respond(self.server.reads[self.path])

    def respond(self, body):
        with self.server.lock:
            self.server.received += 1
            self.server.paths.append((self.command, self.path))
        if self.server.mode == "slow":
            time.sleep(.08)
        if self.server.mode == "timeout":
            time.sleep(.2)
        if self.server.mode == "wrong":
            body += b"invalid trailing byte"
        if self.server.mode == "error":
            body = b"x" * 8192
        code = 307 if self.server.mode == "redirect" else (503 if self.server.mode == "error" else 200)
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
            population = 0

            def observations(self):
                self.calls += 1
                return {"native_resident_cells": self.population, "runtime": {"kind": "python",
                        "interpreter": {"age_ns": 0, "generation": 7 if self.calls < 3 else 8,
                                        "work": {"resident_objects": self.population, "active_invocations": 0,
                                                 "pending_retirements": 0}}}}

        for population in (0, 32):
            host = Host()
            host.population = population
            self.assertEqual(observed_idle(host, population)["runtime"]["interpreter"]["generation"], 8)
            self.assertEqual(host.calls, 3)

    def test_read_population_bounds_and_targets(self):
        self.assertEqual(read_targets(0), [])
        for count in (1, 32, 64):
            targets = read_targets(count)
            self.assertEqual(len(targets), count)
            self.assertEqual({target["path"] for target in targets},
                             {f"/cells/resident-{index}" for index in range(count)})
            self.assertEqual({bytes(target["expected_body"]) for target in targets}, {b'{"value":0}'})
        for invalid in (-1, 65):
            with self.assertRaises(RuntimeError):
                read_targets(invalid)

    def test_invalid_read_workload_refuses_before_starting_host(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "output"
            base = [sys.executable, "-B", str(Path(__file__).with_name("offered.py")),
                    "--client", str(CLIENT), "--native-host", str(CLIENT),
                    "--python-host", f"python={CLIENT}", "--build-profile", "lab", "--output", str(output)]
            for arguments in (["--read-cells", "-1"], ["--read-cells", "65"],
                              ["--read-cells", "32", "--body-bytes", "1"],
                              ["--python-read-mode", "initial"],
                              ["--python-read-mode", "direct-initial", "--cell-echo"],
                              ["--read-cells", "1", "--body-bytes", "0", "--cell-echo"]):
                with self.subTest(arguments=arguments):
                    result = subprocess.run([*base, *arguments], capture_output=True, timeout=10)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(output.exists(), result.stderr.decode())

    def run_client(self, mode, *, inflight=16, timeout=500, body_bytes=137, reads=False, cell_echo=False, updates=False):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.mode, server.received, server.lock = mode, 0, threading.Lock()
        server.paths = []
        server.reads = {"/orders/first": b'{"id":"first"}', "/orders/second": b'{"id":"second"}'}
        server.updates = {f"/orders/update-{i}": (f'{{"expected_version":{i}}}'.encode(),
                          f'{{"version":{i + 1}}}'.encode()) for i in range(50)}
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as root:
                output = Path(root) / "samples.json"
                origin = f"http://127.0.0.1:{server.server_port}" + ("/cell-echo" if cell_echo else "")
                command = [str(CLIENT), origin,
                           "100", "500", str(inflight), str(0 if reads or updates else body_bytes), str(timeout), "200", str(output)]
                if reads:
                    manifest = Path(root) / "reads.json"
                    manifest.write_text(json.dumps([dict(path=path, expected_body=list(body))
                                                   for path, body in server.reads.items()]))
                    command.append(str(manifest))
                if updates:
                    manifest = Path(root) / "updates.json"
                    manifest.write_text(json.dumps(dict(updates=[dict(path=path, body=list(body), expected_body=list(reply))
                        for path, (body, reply) in server.updates.items()])))
                    command.append(str(manifest))
                result = subprocess.run(command, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr.decode())
                report = json.loads(output.read_text())
                summary = summarize(report)
                self.assertEqual(summary["arrivals"], 50)
                dispatched = sum(row["dispatched_ns"] is not None for row in report["samples"])
                self.assertGreater(dispatched, 0)
                with server.lock:
                    self.assertEqual(server.received, dispatched, "redirect or hidden resend")
                    if updates:
                        self.assertEqual(len(server.paths), len(set(server.paths)), "mutation was replayed")
                        for row in report["samples"]:
                            self.assertEqual(("PATCH", f"/orders/update-{row['id']}") in server.paths,
                                             row["dispatched_ns"] is not None)
                    elif reads:
                        self.assertEqual(set(server.paths), {("GET", path) for path in server.reads})
                    else:
                        self.assertEqual(set(server.paths), {("POST", "/cell-echo" if cell_echo else "/echo")})
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

    def test_cell_echo_keeps_route_and_checks_empty_and_binary_bodies(self):
        for size in (0, 4096):
            with self.subTest(body_bytes=size):
                report, summary = self.run_client("echo", body_bytes=size, cell_echo=True)
                self.assertTrue(report["workload"]["url"].endswith("/cell-echo"))
                self.assertEqual(report["workload"]["body_bytes"], size)
                self.assertGreater(summary["outcomes"].get("success", 0), 0)
                self.assertFalse(set(summary["outcomes"]) - {"success", "client_late", "client_capacity"})
        _, summary = self.run_client("wrong", cell_echo=True)
        self.assertGreater(summary["outcomes"].get("body_mismatch", 0), 0)
        self.assertNotIn("success", summary["outcomes"])

    def test_cell_echo_cannot_mix_read_manifest_or_accept_other_paths(self):
        with tempfile.TemporaryDirectory() as root:
            manifest, output = Path(root) / "reads.json", Path(root) / "samples.json"
            manifest.write_text(json.dumps([dict(path="/orders/one", expected_body=[1])]))
            for route, reads in (("/cell-echo", True), ("/other", False),
                                 ("/cell-echo/", False), ("/cell-echo?x=1", False)):
                command = [str(CLIENT), "http://127.0.0.1:9" + route, "1", "1000", "1",
                           "0", "100", "1", str(output)]
                if reads:
                    command.append(str(manifest))
                result = subprocess.run(command, capture_output=True, timeout=5)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists(), result.stderr.decode())

    def test_http_redirects_are_not_followed(self):
        _, summary = self.run_client("redirect")
        self.assertGreater(summary["outcomes"].get("http_status", 0), 0)
        self.assertNotIn("success", summary["outcomes"])

    def test_body_mismatch_is_not_success(self):
        _, summary = self.run_client("wrong")
        self.assertGreater(summary["outcomes"].get("body_mismatch", 0), 0)
        self.assertNotIn("success", summary["outcomes"])

    def test_http_failure_details_have_bounded_count_and_size(self):
        report, summary = self.run_client("error")
        details = [row["response_prefix"] for row in report["samples"] if "response_prefix" in row]
        self.assertEqual(len(details), min(16, summary["outcomes"]["http_status"]))
        self.assertEqual(set(details), {"x" * 1024})
        self.assertNotIn("success", summary["outcomes"])
        forged = copy.deepcopy(report)
        next(row for row in forged["samples"] if "response_prefix" in row)["response_prefix"] += "x"
        with self.assertRaises(RuntimeError):
            summarize(forged)

    def test_timeouts_remain_failed_attempts(self):
        _, summary = self.run_client("timeout", timeout=50)
        self.assertGreater(summary["outcomes"].get("timeout", 0), 0)
        self.assertNotIn("success", summary["outcomes"])

    def test_read_targets_validate_distinct_response_bodies(self):
        report, summary = self.run_client("echo", reads=True)
        self.assertEqual(len(report["workload"]["reads"]), 2)
        self.assertGreater(summary["outcomes"].get("success", 0), 0)
        self.assertFalse(set(summary["outcomes"]) - {"success", "client_late", "client_capacity"})

    def test_read_targets_reject_trailing_bytes(self):
        _, summary = self.run_client("wrong", reads=True)
        self.assertGreater(summary["outcomes"].get("body_mismatch", 0), 0)
        self.assertNotIn("success", summary["outcomes"])

    def test_updates_are_distinct_and_exact_without_retries(self):
        for mode, timeout, expected in [("echo", 500, "success"), ("wrong", 500, "body_mismatch"),
                                         ("error", 500, "http_status"), ("redirect", 500, "http_status"),
                                         ("timeout", 50, "timeout")]:
            with self.subTest(mode=mode):
                report, summary = self.run_client(mode, updates=True, timeout=timeout)
                self.assertEqual(len(report["workload"]["updates"]), 50)
                self.assertGreater(summary["outcomes"].get(expected, 0), 0)
                if expected != "success":
                    self.assertNotIn("success", summary["outcomes"])

    def test_update_manifest_requires_one_distinct_target_per_arrival(self):
        valid = dict(path="/orders/one", body=[1], expected_body=[2])
        with tempfile.TemporaryDirectory() as root:
            manifest, output = Path(root) / "updates.json", Path(root) / "result.json"
            for document in [dict(updates=[]), dict(updates=[valid, valid]),
                             dict(updates=[valid]), dict(updates=[dict(valid, path="//remote")]),
                             dict(updates=[dict(valid, body=[0] * 65_537)]),
                             dict(updates=[valid], reads=[])]:
                manifest.write_text(json.dumps(document))
                result = subprocess.run([str(CLIENT), "http://127.0.0.1:9", "2", "1000", "1",
                                         "0", "100", "1", str(output), str(manifest)],
                                        capture_output=True, timeout=5)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

    def test_invalid_read_manifest_refuses_before_creating_report(self):
        valid = {"path": "/orders/one", "expected_body": [1, 2]}
        invalid = [[], [valid, valid], [dict(valid, path="//external/one")],
                   [dict(valid, path="/../one")], [dict(valid, path="/one?redirect=1")],
                   [dict(valid, expected_body=[256])], [dict(valid, unknown=True)],
                   [dict(valid, expected_body=[0] * 65_537)], [valid] * 65]
        with tempfile.TemporaryDirectory() as root:
            manifest, output = Path(root) / "reads.json", Path(root) / "result.json"
            for targets in invalid:
                with self.subTest(target_count=len(targets), keys=list(targets[0]) if targets else []):
                    manifest.write_text(json.dumps(targets))
                    result = subprocess.run([str(CLIENT), "http://127.0.0.1:9", "1", "1000", "1",
                                             "0", "100", "1", str(output), str(manifest)],
                                            capture_output=True, timeout=5)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(output.exists(), result.stderr.decode())


if __name__ == "__main__":
    if CLIENT is None:
        raise SystemExit("usage: test_offered.py /absolute/path/python-offered-load")
    unittest.main()
