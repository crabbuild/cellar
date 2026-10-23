#!/usr/bin/env python3
"""Exercise the compiled counter load client against a deterministic HTTP peer."""
import argparse
import collections
import http.server
import json
import math
from pathlib import Path
import subprocess
import tempfile
import threading


def check(binary):
    lock = threading.Lock()
    state = dict(writes=[7, 11], mode="valid", calls=collections.Counter())

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_GET(self):
            self.respond(False)

        def do_POST(self):
            self.respond(True)

        def respond(self, writing):
            cell = int(self.path.rsplit("/", 1)[1])
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            with lock:
                state["calls"][self.command] += 1
                if writing:
                    assert len(body) == 16
                    state["writes"][cell] += 1
                mode = state["mode"]
                value = dict(writes=state["writes"][cell] + (mode == "counter"),
                             payload_bytes=17 if mode == "payload" else 16, node="test-node")
            payload = json.dumps(value).encode()
            self.send_response(503 if mode == "unavailable" else 200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="cellar-load-check-") as root:
            ledger = Path(root) / "accounting.json"
            url = f"http://127.0.0.1:{server.server_port}"

            def run(operation):
                return subprocess.run([str(binary), operation, url, "2", "2", "1", "16", str(ledger)],
                                      text=True, capture_output=True, timeout=15)

            missing = run("read")
            assert missing.returncode != 0 and not ledger.exists()
            assert not state["calls"], "missing ledger must fail before requests"
            ledger.write_text(json.dumps(dict(writes=state["writes"])))
            original = ledger.read_bytes()
            timestamp = ledger.stat().st_mtime_ns
            for mode in ("valid", "counter", "payload", "unavailable"):
                state["mode"] = mode
                state["calls"].clear()
                result = run("read")
                assert (result.returncode == 0) == (mode == "valid"), result.stderr
                value = json.loads(result.stdout)
                assert value["operation"] == "read"
                assert "successful_writes" not in value and "logical_mib_per_s" not in value
                assert value["successful_reads"] + sum(value["errors"].values()) == state["calls"]["GET"]
                assert not state["calls"]["POST"], "read measurement sent a mutation"
                assert ledger.read_bytes() == original and ledger.stat().st_mtime_ns == timestamp
                samples = sorted(value["latency_us"])
                assert len(samples) == value["successful_reads"]
                assert value["reads_per_s"] == len(samples) / value["elapsed_s"]
                for label, quantile in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1)):
                    expected = samples[max(0, math.ceil(len(samples) * quantile) - 1)] / 1000 if samples else 0
                    assert value["latency_ms"][label] == expected
                if mode != "valid":
                    assert not samples and state["calls"]["GET"] > 0
                print(f"read/{mode}: observed {state['calls']['GET']} attempts", flush=True)
            state["mode"] = "valid"
            state["calls"].clear()
            result = run("write")
            assert result.returncode == 0, result.stderr
            value = json.loads(result.stdout)
            assert value["operation"] == "write" and not state["calls"]["GET"]
            assert value["successful_writes"] == state["calls"]["POST"] == len(value["latency_us"])
            assert json.loads(ledger.read_text())["writes"] == state["writes"]
            assert sum(state["writes"]) == 18 + value["successful_writes"]
            print(f"write/valid: accounted {value['successful_writes']} acknowledgements", flush=True)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--load", type=Path, required=True)
    check(parser.parse_args().load.resolve(strict=True))
