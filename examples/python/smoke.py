#!/usr/bin/env python3
"""Exercise the packaged Python application through the real Cellar dev host."""
import argparse
import http.client
import http.server
import socket
import threading
import urllib.error
import urllib.request
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from smoke import Host, eventually, private_project, request


def check_report(log):
    reports = []
    for line in log.read_text(errors="replace").splitlines():
        # `cellar dev --logs` forwards each child stdout line with a host prefix.
        if '"event":"embedded_python_shutdown"' in line:
            reports.append(json.loads(line[line.index('{'):]))
    assert reports, "missing combined native/Python shutdown report"
    report = reports[-1]["report"]
    assert not report["runtime_failed"] and not report["host_failed"]
    assert report["pending_tasks"] == report["pending_threads"] == report["pending_objects"] == 0
    assert report["objects_created"] == report["objects_retired"]
    assert report["bridge"]["pending_reservations"] == report["bridge"]["queued_bytes"] == 0
    assert report["bridge"]["reserved_bytes_total"] > 0
    assert report["bridge"]["input_copied_bytes"] > 0
    assert report["bridge"]["output_copied_bytes"] > 0
    operations = report["bridge"]["operations"]
    assert operations["pending"] == operations["queued"] == operations["running"] == operations["replies_pending"] == 0
    assert operations["admitted"] == sum(operations[key] for key in ("delivered", "abandoned_queued", "abandoned_running", "undelivered"))
    assert operations["started"] == sum(operations[key] for key in ("succeeded", "failed", "abandoned_running"))
    assert operations["queue_wait"]["samples"] == operations["sdk_duration"]["samples"] == operations["started"]
    assert operations["reply_wait"]["samples"] == operations["round_trip"]["samples"] == operations["delivered"]
    assert all(value == 0 for value in report["work"].values())
    assert report["interpreter"]["traced_current_bytes"] is None
    assert report["memory"]["before_interpreter"]["process_rss_bytes"] > 0
    assert report["memory"]["after_join"]["process_rss_bytes"] > 0
    assert report["native_outcomes"] and all(outcome["complete"] for outcome in report["native_outcomes"])


def receipt_request(origin, amount, key, status=200):
    operation = urllib.request.Request(origin + "/counters/receipt", method="POST",
        data=json.dumps({"amount": amount}).encode(),
        headers={"content-type": "application/json", "idempotency-key": key})
    try:
        response = urllib.request.urlopen(operation, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        assert response.status == status, (response.status, body)
        return json.loads(body)


def lose_committed_response(origin):
    # The proxy observes the real output proof and drops the connection before
    # delivering any response bytes to the caller. The caller cannot infer the
    # commit from its transport failure and must reconcile with its receipt key.
    observed = []

    class Drop(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers["content-length"]))
            operation = urllib.request.Request(origin + self.path, method="POST", data=body,
                headers={"content-type": "application/json", "idempotency-key": self.headers["idempotency-key"]})
            with urllib.request.urlopen(operation, timeout=10) as response:
                observed.append((response.status, json.loads(response.read())))
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)

    server = http.server.HTTPServer(("127.0.0.1", 0), Drop)
    server.timeout = 15
    worker = threading.Thread(target=server.handle_request)
    worker.start()
    try:
        try:
            receipt_request("http://127.0.0.1:" + str(server.server_port), 9, "lost-response")
            raise AssertionError("fault proxy delivered the intentionally lost response")
        except (urllib.error.URLError, http.client.RemoteDisconnected, ConnectionError):
            pass
    finally:
        worker.join(timeout=15)
        server.server_close()
    assert not worker.is_alive(), "fault proxy did not join"
    assert observed == [(200, {"count": 9, "alarms": 0})], observed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar", required=True, type=Path)
    parser.add_argument("--with-cron", action="store_true")
    parser.add_argument("--project", type=Path, default=Path(__file__).with_name("project.json"))
    args = parser.parse_args()
    cellar = args.cellar.resolve()
    project = args.project.resolve()
    with tempfile.TemporaryDirectory(prefix="cellar-python-smoke-") as temporary:
        root = Path(temporary)
        staged = private_project(project, root)
        host = Host(cellar, staged, root, extra_env={"CELLAR_SHUTDOWN_REPORT_JSON": "1"})
        try:
            origin = host.ready()
            call = lambda path, **kwargs: request(origin, path, **kwargs)
            assert call("/health")["runtime"] == "cpython"
            libraries = call("/libraries")
            assert libraries["matrix"] == [[5.0, 14.0, 23.0], [14.0, 50.0, 86.0], [23.0, 86.0, 149.0]]
            assert libraries["numpy"] == "2.3.4" and len(libraries["sha256"]) == 64
            assert call("/counters/a") == {"count": 0, "alarms": 0}
            assert call("/counters/a", method="POST", data={"amount": 7})["count"] == 7
            call("/counters/a", method="POST", data={"amount": "invalid"}, status=400)
            assert call("/counters/a")["count"] == 7
            lose_committed_response(origin)
            assert call("/counters/b")["count"] == 0
            call("/counters/a/alarm", method="POST")
            eventually(lambda: call("/counters/a"), lambda value: value["alarms"] == 1)
            value = {"message": "Python KV", "nullable": None}
            assert call("/kv", method="POST", data=value) == value
            assert call("/sql", method="POST", data="Python SQL")["results"] == [{"body": "Python SQL"}]
            call("/queue", method="POST", data={"job": "Python queue", "retry_once": True}, status=202)
            eventually(lambda: call("/jobs"), lambda value: value == [{"job": "Python queue", "retry_once": True}])
            if args.with_cron:
                eventually(lambda: call("/cron"), lambda value: value is not None, timeout=75)
        except BaseException:
            print(host.log.read_text(errors="replace")[-16000:], flush=True)
            raise
        finally:
            host.stop()
            check_report(host.log)
        restarted = Host(cellar, staged, root, extra_env={"CELLAR_SHUTDOWN_REPORT_JSON": "1"})
        try:
            origin = restarted.ready()
            assert request(origin, "/counters/a") == {"count": 7, "alarms": 1}
            assert request(origin, "/counters/receipt") == {"count": 9, "alarms": 0}
            assert receipt_request(origin, 9, "lost-response") == {"count": 9, "alarms": 0}
            assert receipt_request(origin, 1, "different-request") == {"count": 10, "alarms": 0}
            # Replay returns the original sealed result even after a later write.
            assert receipt_request(origin, 9, "lost-response") == {"count": 9, "alarms": 0}
            receipt_request(origin, 8, "lost-response", status=409)
            assert request(origin, "/counters/receipt") == {"count": 10, "alarms": 0}
            assert request(origin, "/kv") == {"message": "Python KV", "nullable": None}
            assert request(origin, "/sql")["results"] == [{"body": "Python SQL"}]
            assert request(origin, "/jobs") == [{"job": "Python queue", "retry_once": True}]
        finally:
            restarted.stop()
            check_report(restarted.log)
    print("PASS Python package: Pydantic, HTTP, cells, alarms, KV, SQL, queue, preserving restart, lost-response reconciliation and clean host join" + (", cron" if args.with_cron else ""))


if __name__ == "__main__":
    main()
