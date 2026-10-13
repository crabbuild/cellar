#!/usr/bin/env python3
"""Run the compiled producer/stream/inbox example and verify preserving restart."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from smoke import eventually, request


class Example:
    def __init__(self, binary, root, label):
        self.log = root / (label + ".log")
        self.output = self.log.open("wb")
        self.child = subprocess.Popen(
            [str(binary), str(root / "data"), "0"],
            stdout=self.output, stderr=subprocess.STDOUT,
            env=dict(os.environ, NO_COLOR="1"),
        )

    def ready(self):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            for line in self.log.read_text(errors="replace").splitlines():
                if line.startswith('{"ready":'):
                    return json.loads(line)["ready"]
            assert self.child.poll() is None, self.log.read_text(errors="replace")
            time.sleep(0.1)
        raise AssertionError("stream example readiness timed out\n" + self.log.read_text(errors="replace"))

    def stop(self):
        if self.child.poll() is None:
            self.child.send_signal(signal.SIGTERM)
        try:
            code = self.child.wait(timeout=80)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.child.wait()
            raise AssertionError("stream example did not stop\n" + self.log.read_text(errors="replace"))
        finally:
            self.output.close()
        assert code == 0, self.log.read_text(errors="replace")
        reports = [json.loads(line) for line in self.log.read_text().splitlines()
                   if line.startswith("{") and json.loads(line).get("outcome") == "stopped"]
        assert len(reports) == 1, self.log.read_text()
        assert all(reports[0][name] for name in
                   ("ingress_drained", "application_work_drained", "cells_drained")), reports


def exercise(binary):
    with tempfile.TemporaryDirectory(prefix="cellar-durable-streams-") as directory:
        root = Path(directory)
        initial = Example(binary, root, "initial")
        try:
            origin = initial.ready()
            assert request(origin, "/projection") == {"orders": 0, "total_cents": 0}
            assert request(origin, "/orders/a?amount_cents=1200", method="POST", status=201)["status"] == "created"
            assert request(origin, "/orders/b?amount_cents=300", method="POST", status=201)["status"] == "created"
            assert request(origin, "/orders/a?amount_cents=1200", method="POST")["status"] == "duplicate"
            assert request(origin, "/orders/a?amount_cents=1201", method="POST", status=409)["status"] == "conflict"
            request(origin, "/orders/bad?amount_cents=-1", method="POST", status=400)
            page = eventually(lambda: request(origin, "/events"), lambda page: len(page["records"]) == 2)
            assert sorted(record["event"]["order_id"] for record in page["records"]) == ["a", "b"]
            assert request(origin, "/events?cursor=" + page["cursor"])["records"] == []
            eventually(lambda: request(origin, "/orders/a"), lambda data: data["outbox"]["pending_rows"] == "0")
            retried = request(origin, "/projection/retry", method="POST")
            assert retried["fresh_effects"] == 2 and retried["status"] == "retried", retried
            assert retried["projection"] == {"orders": 2, "total_cents": 1500}
            assert request(origin, "/subscription")["checkpoint"] == "0"
        finally:
            initial.stop()
        resumed = Example(binary, root, "resumed")
        try:
            origin = resumed.ready()
            assert request(origin, "/projection") == {"orders": 2, "total_cents": 1500}
            assert request(origin, "/orders/a")["order"] == {"order_id": "a", "amount_cents": 1200}
            repeated = request(origin, "/projection/process", method="POST")
            assert repeated["status"] == "acknowledged" and repeated["fresh_effects"] == 0, repeated
            assert repeated["projection"] == {"orders": 2, "total_cents": 1500}
            assert request(origin, "/subscription")["checkpoint"] == "2"
            assert request(origin, "/projection/process", method="POST")["status"] == "empty"
            assert request(origin, "/orders/c?amount_cents=500", method="POST", status=201)["status"] == "created"
            eventually(lambda: request(origin, "/orders/c"), lambda data: data["outbox"]["pending_rows"] == "0")
            processed = request(origin, "/projection/process", method="POST")
            assert processed["fresh_effects"] == 1, processed
            assert processed["projection"] == {"orders": 3, "total_cents": 2000}
        finally:
            resumed.stop()
        print("PASS durable streams: atomic producer, resumable read, inbox redelivery, ack and preserving restart")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    exercise(parser.parse_args().binary.resolve())
