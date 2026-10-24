#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Verify separate-process WebSockets, standalone embedding and optional Rust containers."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time

from smoke import Host, private_project, request

EXAMPLES = Path(__file__).resolve().parent


def outbound(cellar):
    with tempfile.TemporaryDirectory(prefix="cellar-outbound-example-") as temporary:
        root = Path(temporary)
        peer_root, client_root = root / "peer", root / "client"
        peer_root.mkdir()
        client_root.mkdir()
        peer = Host(cellar, private_project(EXAMPLES / "wsecho/project.json", peer_root), peer_root)
        client = None
        try:
            peer_origin = peer.ready()
            project = private_project(EXAMPLES / "wsclient/project.json", client_root)
            (client_root / ".dev.vars").write_text("WEBSOCKET_PEER=" + peer_origin.replace("http://", "ws://", 1) + "/rooms/outbound\n")
            client = Host(cellar, project, client_root)
            assert request(client.ready(), "/wsclient/outbound") == {"echo": "native Rust hello"}
            assert request(peer_origin, "/rooms/outbound/state") == {"messages": 1}
            print("PASS outbound WebSocket: separate Rust hosts, network upgrade and durable message", flush=True)
        finally:
            try:
                if client is not None:
                    client.stop()
            finally:
                peer.stop()


def embedded(binary):
    with tempfile.TemporaryDirectory(prefix="cellar-embedded-example-") as temporary:
        root = Path(temporary)
        for restart in (False, True):
            log = root / "embedded.log"
            with log.open("wb") as output:
                child = subprocess.Popen([str(binary), str(root / "data")], stdout=output, stderr=subprocess.STDOUT, env=dict(os.environ, NO_COLOR="1"))
                try:
                    deadline = time.monotonic() + 30
                    while "embedded example ready" not in log.read_text(errors="replace"):
                        assert child.poll() is None, log.read_text(errors="replace")
                        assert time.monotonic() < deadline, "embedded readiness timed out"
                        time.sleep(0.1)
                    origin = "http://127.0.0.1:9876"
                    if not restart:
                        assert request(origin, "/counters/alpha") == {"count": 0}
                        assert request(origin, "/counters/alpha", method="POST") == {"count": 1}
                    assert request(origin, "/counters/alpha") == {"count": 1}
                finally:
                    if child.poll() is None:
                        child.send_signal(signal.SIGTERM)
                    try:
                        code = child.wait(timeout=40)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
                        raise AssertionError("embedded host did not stop within 40 seconds")
                    assert code == 0, log.read_text(errors="replace")
            reports = [json.loads(line) for line in log.read_text().splitlines() if line.startswith('{"application_work_drained"') or line.startswith('{"outcome"')]
            assert len(reports) == 1 and reports[0]["outcome"] == "stopped", log.read_text()
            assert all(reports[0][field] for field in ("ingress_drained", "application_work_drained", "cells_drained"))
        print("PASS embedded Node: owned runtimes, drain proofs and preserving restart", flush=True)


def containers(cellar):
    with tempfile.TemporaryDirectory(prefix="cellar-container-example-") as temporary:
        root = Path(temporary)
        project = private_project(EXAMPLES / "native-container/project.json", root)
        # Exercise the documented Dockerfile path, including native image/fence staging.
        for name in ("Dockerfile", "server.rs"):
            shutil.copyfile(EXAMPLES / "native-container" / name, root / name)
        host = Host(cellar, project, root)
        try:
            origin = host.ready()
            alice = request(origin, "/sandboxes/alice/hello")
            bob = request(origin, "/sandboxes/bob/hello")
            assert alice["path"] == bob["path"] == "/hello"
            assert alice["cell"] != bob["cell"]
            assert request(origin, "/sandboxes/alice/hello") == alice
            result = request(origin, "/sandboxes/alice/exec", method="POST")
            assert result["exit_code"] == 0 and result["stderr"] == ""
            assert json.loads(result["stdout"]) == {"tool": "native-rust", "ok": True}
            request(origin, "/sandboxes/alice/exec", status=405)
            print("PASS containers: Dockerfile packaging, named isolation, forwarding and Rust exec", flush=True)
        except BaseException:
            print(host.log.read_text(errors="replace")[-12000:], flush=True)
            raise
        finally:
            host.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar", type=Path, required=True)
    parser.add_argument("--embedded", type=Path, required=True)
    parser.add_argument("--with-containers", action="store_true", help="requires a reachable engine socket and Linux fence support")
    args = parser.parse_args()
    outbound(args.cellar.resolve())
    embedded(args.embedded.resolve())
    if args.with_containers:
        containers(args.cellar.resolve())


if __name__ == "__main__":
    main()
