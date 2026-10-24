#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run real native/Axum reads, remote routing and retained receipts after restart.

Usage: python3 examples/raft-counter/smoke.py BINARY NEW_WORK_DIRECTORY
Requires the example's six loopback ports to be free. Retains the work directory
and child logs for inspection; prints no bearer credentials.
"""
import json
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


def run(binary, work):
    root = Path(__file__).resolve().parents[2]
    work.mkdir(parents=True, exist_ok=False)
    token = secrets.token_hex(32)
    secret = work / "http-token"
    secret.write_text(token)
    secret.chmod(0o600)
    fixture = root / "crates/cellar/transport/tcp/fixtures"
    argv = [str(binary), str(work), *(str(fixture / name) for name in
            ["ca.der", "server.der", "key.der"]), str(secret)]
    bases = [f"http://127.0.0.1:{port}" for port in
             [9901, 9902, 9903, 9911, 9912, 9913]]
    retry = int(time.time() * 1000) + 300_000
    client = "07" * 16
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    # Fail late startup after node/service owners exist, then resume the same
    # disks. This must join cleanly without resetting any voting history.
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 9913))
        occupied.listen(1)
        with (work / "startup-failure.log").open("wb") as output:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT)
            try:
                assert process.wait(timeout=30) == 1, "late startup did not return a clean error"
                assert all((work / f"raft-counter-{i}" / "raft").is_dir()
                           for i in range(1, 4)), "failure occurred before all Raft owners started"
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
    print("late startup failure joined owners; resuming the same disks", flush=True)

    def request(base, sequence=None, delta=None, *, cell="one", read_headers=None):
        path = f"/counters/{cell}"
        headers = {"authorization": f"Bearer {token}"}
        headers.update(read_headers or {})
        method = "GET"
        if sequence is not None:
            method = "POST"
            path += f"?delta={delta}"
            headers.update({"x-command-id": f"{client}:{sequence}",
                            "x-retry-until-ms": str(retry)})
        try:
            with opener.open(urllib.request.Request(base + path, headers=headers,
                                                   method=method), timeout=6) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    for phase in range(2):
        with (work / f"process-{phase}.log").open("wb") as output:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT)
            try:
                def command(sequence, delta, expected, candidates, *, cell="one"):
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        if process.poll() is not None:
                            raise RuntimeError(f"example exited {process.returncode}")
                        for base in candidates:
                            try:
                                status, body = request(base, sequence, delta, cell=cell)
                            except OSError:
                                continue
                            if status == 200:
                                result = json.loads(body)
                                assert result["value"] == expected, result
                                assert isinstance(result["version_token"], str)
                                assert result["served_by"] is None
                                return result
                            assert status == 503, (status, body)
                        time.sleep(0.02)
                    raise TimeoutError("command did not obtain a leader/receipt")

                # A successful retry after restart must return the original
                # result 7 even though the current business value is already 9.
                original = command(1, 7, 7, bases)
                if phase == 0:
                    first = original
                    latest = command(2, 2, 9, bases[3:])
                    expected = 9
                else:
                    # A reconstructed receipt may name a later verified covering
                    # image. Its history and stored business result must match.
                    assert all(original[k] == first[k] for k in
                               ["group", "incarnation", "value"]), (original, first)
                    assert original["index"] >= first["index"], (original, first)
                    latest = command(3, 4, 13, bases[:3])
                    expected = 13
                other = command(1, 31, 31, bases, cell="two")
                for i, base in enumerate(bases):
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        status, body = request(base)
                        if status == 200 and json.loads(body)["value"] == expected:
                            break
                        time.sleep(0.02)
                    else:
                        raise TimeoutError(f"replica did not publish at {base}")
                    # Select a different physical node so every ingress path
                    # traverses the dedicated query RPC, without leader fallback.
                    remote = ((i + 1) % 3) + 1
                    for consistency in ["at-least", "linearizable"]:
                        headers = {"x-read-consistency": consistency,
                                   "x-query-members": str(remote),
                                   "x-query-attempt-ms": "2000",
                                   "x-query-timeout-ms": "4000"}
                        if consistency == "at-least":
                            headers["x-min-version"] = latest["version_token"]
                        status, body = request(base, read_headers=headers)
                        assert status == 200, (status, body)
                        read = json.loads(body)
                        assert read["value"] == expected, read
                        assert read["served_by"] == remote, read
                        assert read["index"] >= latest["index"], (read, latest)
                        assert all(read[k] == latest[k] for k in ["group", "incarnation"])
                        assert isinstance(read["version_token"], str)
                    # A pre-restart token remains usable after ordinary restart;
                    # a token from another cell never establishes a minimum.
                    status, body = request(base, read_headers={
                        "x-read-consistency": "at-least", "x-min-version": first["version_token"]})
                    assert status == 200 and json.loads(body)["value"] == expected, (status, body)
                    status, body = request(base, cell="two", read_headers={
                        "x-read-consistency": "at-least", "x-min-version": latest["version_token"],
                        "x-query-members": str(remote), "x-leader-fallback": "true"})
                    assert status == 409 and body == b"WrongIncarnation", (status, body)
                    status, body = request(base, cell="two", read_headers={
                        "x-read-consistency": "at-least", "x-min-version": other["version_token"]})
                    assert status == 200 and json.loads(body)["value"] == 31, (status, body)
                process.send_signal(signal.SIGINT)
                assert process.wait(timeout=30) == 0, "example shutdown failed"
                print(f"phase {phase}: six HTTP endpoints verify strong remote reads and cell-scoped tokens; joined shutdown", flush=True)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
    print("retained command receipt and subsequent writes survived a three-node restart", flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    run(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve())
