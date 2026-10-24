#!/usr/bin/env python3
"""Exercise compiled Rust recipes through the real dev host in private local state."""
import argparse
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def request(origin, path, method="GET", data=None, token=None, status=200):
    headers = {}
    if isinstance(data, dict):
        data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    elif isinstance(data, str):
        data = data.encode()
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(origin + path, data=data, method=method, headers=headers)
    try:
        response = urllib.request.urlopen(req, timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        assert response.status == status, (path, response.status, status, body[:1000])
        if "application/json" in response.headers.get("Content-Type", ""):
            return json.loads(body)
        return body


def eventually(read, accept, timeout=20):
    deadline = time.monotonic() + timeout
    value = None
    while time.monotonic() < deadline:
        value = read()
        if accept(value):
            return value
        time.sleep(0.1)
    raise AssertionError(f"condition not met: {value!r}")


class Host:
    def __init__(self, cellar, project, root, extra_env=None, *, command_prefix=()):
        self.log = root / "host.log"
        self.output = self.log.open("wb")
        self.child = subprocess.Popen(
            [*command_prefix, str(cellar), "dev", str(project), "--no-watch", "--port", "0", "--logs"],
            stdout=self.output, stderr=subprocess.STDOUT,
            env={**os.environ, "NO_COLOR": "1", "CELLAR_TOKIO_THREADS": "2",
                 "CELLAR_SHUTDOWN_TOTAL_MS": "8000", **(extra_env or {})},
        )

    def ready(self):
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            text = self.log.read_text(errors="replace")
            for line in reversed(text.splitlines()):
                if "ready" in line and "http://" in line:
                    return "http://" + line.split("http://", 1)[1].strip()
            if self.child.poll() is not None:
                raise AssertionError(f"dev exited: {self.child.returncode}\n{text[-12000:]}")
            time.sleep(0.1)
        raise AssertionError("dev readiness timed out\n" + self.log.read_text(errors="replace")[-12000:])

    def stop(self):
        if self.child.poll() is None:
            self.child.send_signal(signal.SIGTERM)
        try:
            code = self.child.wait(timeout=40)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.child.wait()
            raise AssertionError("dev did not drain within 40 seconds")
        finally:
            self.output.close()
        assert code == 0, self.log.read_text(errors="replace")[-12000:]


def cookbook(origin, with_cron):
    call = lambda path, **kwargs: request(origin, path, **kwargs)
    assert call("/hello")["language"] == "Rust"
    assert call("/webapi")["header"] == "rust"
    assert b"<!doctype html>" in call("/").lower()
    assert b"<!doctype html>" in call("/assets-binding").lower()
    for path in ("/body", "/body/stream"):
        assert call(path, method="POST", data=b"\x00\x80\xffhello") == b"\x00\x80\xffhello"
    call("/body", method="POST", data=b"x" * 65537, status=413)
    call("/hello", method="POST", status=405)
    assert call("/counters/alpha", method="POST")["count"] == 1
    assert call("/rpc/alpha", method="POST")["count"] == 2
    assert call("/router/beta", method="POST")["count"] == 1
    assert call("/counters/alpha")["count"] == 2
    call("/counters/alpha", method="DELETE", status=405)
    assert call("/vectors/palette?rgb=250,20,20")["rows"][0][0] == "red"
    call("/vectors/palette?rgb=bad", status=400)
    call("/sql", method="POST", data={"message": "native guestbook"})
    assert "native guestbook" in json.dumps(call("/sql"))
    call("/sql", method="POST", data=b"not JSON", status=400)
    call("/kv/demo", method="PUT", data="hello KV")
    assert call("/kv/demo") == b"hello KV"
    assert call("/kv")["keys"] == ["demo"]
    call("/kv/demo", method="DELETE")
    call("/kv/demo", status=404)
    call("/blobs/demo", method="PUT", data="hello Blob")
    assert call("/blobs/demo") == b"hello Blob"
    call("/blobs/demo", method="DELETE")
    call("/blobs/demo", status=404)
    call("/alarms/demo", method="POST")
    eventually(lambda: call("/alarms/demo"), lambda v: v["status"] == "fired")
    call("/async/demo", method="POST")
    eventually(lambda: call("/async/demo"), lambda v: v["completed"] == 1)
    call("/queues", method="POST", data={"id": "job1", "retry_once": True}, status=202)
    eventually(lambda: call("/counters/queue-effects"), lambda v: v["count"] == 1)
    call("/queues", method="POST", data={"id": "job1"}, status=202)
    eventually(lambda: call("/queues"), lambda v: v["backlog_count"] == 0)
    assert call("/counters/queue-effects")["count"] == 1
    call("/queues", method="POST", data={"id": "poison", "fail": True}, status=202)
    eventually(lambda: call("/queues/dead"), lambda v: v["backlog_count"] >= 1)
    call("/outbox/shop", method="POST", data={"id": "order1", "amount": 10}, status=202)
    eventually(lambda: call("/outbox/shop"), lambda v: v["orders"] == [["order1", 1]])
    eventually(lambda: call("/counters/queue-effects"), lambda v: v["count"] == 2)
    call("/outbox/shop", method="POST", data={"id": "order1", "amount": 10}, status=202)
    call("/outbox/shop", method="POST", data={"id": "order1", "amount": 11}, status=409)
    call("/workflows/report1", method="POST", data={"title": "Cellar report"}, status=202)
    result = eventually(lambda: call("/workflows/report1"), lambda v: v["state"] == "Complete")
    assert result["output"]["key"] == "report-report1"
    assert b"Cellar report" in call("/blobs/report-report1")
    assert call("/wsclient")["echo"] == "native Rust hello"
    assert call("/rooms/client/state")["messages"] == 1
    assert call("/facets/supervisor", method="POST")["count"] == 1
    assert call("/facets/supervisor")["count"] == 1
    account = "/tenants/acme/accounts/main"
    assert call(account, method="POST", token="local-acme", data={"id": "credit1", "delta": 10})["balance"] == 10
    assert call(account, method="POST", token="local-acme", data={"id": "credit1", "delta": 10})["balance"] == 10
    call(account, method="POST", token="local-acme", data={"id": "credit1", "delta": 11}, status=409)
    call(account, method="POST", token="local-acme", data={"id": "debit1", "delta": -20}, status=409)
    assert call(account, token="local-acme")["balance"] == 10
    assert call("/tenants/beta/accounts/main", token="local-beta")["balance"] == 0
    call(account, token="local-beta", status=403)
    call(account, status=401)
    call("/agents/demo", method="POST", data={"text": "native durable tools"})
    result = eventually(lambda: call("/agents/demo"), lambda v: v["stage"] == "complete")
    assert result["words"] == 3 and result["steps"] == 2
    call("/agents/demo", method="POST", data={"text": "again"}, status=409)
    if with_cron:
        eventually(lambda: call("/cron"), lambda v: v["count"] >= 1, timeout=75)
    print("PASS cookbook: bindings, retries, DLQ, transactions, tenancy, workflows and tools", flush=True)


def private_project(project, root):
    """Stage only this project's assets and point Cargo at the original workspace."""
    project = project.resolve()
    config = json.loads(project.read_text())
    config["cargo_manifest"] = str((project.parent / config["cargo_manifest"]).resolve())
    for index, app in enumerate([config["primary"], *config.get("cohosted", [])]):
        if "assets" in app:
            asset_source = (project.parent / app["assets"]["directory"]).resolve()
            asset_directory = f"assets-{index}"
            shutil.copytree(asset_source, root / asset_directory)
            app["assets"]["directory"] = asset_directory
    staged = root / "project.json"
    staged.write_text(json.dumps(config))
    return staged


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar", type=Path, required=True, help="compiled cellar CLI path")
    parser.add_argument("--project", type=Path, default=Path(__file__).with_name("project.json"))
    parser.add_argument("--suite", choices=["cookbook", "team"], default="cookbook")
    parser.add_argument("--with-cron", action="store_true", help="also wait for a real minute cron tick")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="cellar-example-smoke-") as temporary:
        root = Path(temporary)
        staged = private_project(args.project, root)
        host = Host(args.cellar.resolve(), staged, root)
        try:
            origin = host.ready()
            if args.suite == "team":
                assert request(origin, "/counters/team", method="POST")["count"] == 1
                assert request(origin, "/counters/team")["count"] == 1
                print("PASS team: service binding routes to cohosted ledger", flush=True)
            else:
                cookbook(origin, args.with_cron)
        except BaseException:
            print(host.log.read_text(errors="replace")[-12000:], flush=True)
            raise
        finally:
            host.stop()
        # Restart the same private project and verify persisted business state.
        restarted = Host(args.cellar.resolve(), staged, root)
        try:
            origin = restarted.ready()
            if args.suite == "team":
                assert request(origin, "/counters/team")["count"] == 1
            else:
                assert request(origin, "/counters/alpha")["count"] == 2
                assert request(origin, "/agents/demo")["stage"] == "complete"
                assert request(origin, "/tenants/acme/accounts/main", token="local-acme")["balance"] == 10
                assert request(origin, "/outbox/shop")["orders"] == [["order1", 1]]
            print("PASS restart: durable state retained; both hosts drain successfully", flush=True)
        finally:
            restarted.stop()


if __name__ == "__main__":
    main()
