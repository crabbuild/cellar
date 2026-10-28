#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run each focused project's documented Rust behavior with private fresh state."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile

from smoke import Host, eventually, private_project, request


def exercise(name, origin, with_cron):
    call = lambda path, **kwargs: request(origin, path, **kwargs)
    # Returned reads must still match after preserving restart.
    persisted = []
    if name == "hello":
        assert call("/hello") == {"hello": "Cellar", "language": "Rust"}
        call("/hello", method="POST", status=405)
    elif name == "webapi":
        result = call("/webapi?name=cellar")
        assert result["pathname"] == "/webapi" and result["header"] == "rust"
        assert len(result["sha256"]) == 64
    elif name == "static-assets":
        assert b"Assets served by Cellar" in call("/")
        assert b"Assets served by Cellar" in call("/assets-binding/index.html")
    elif name in ("counter", "router", "rpc", "team-services", "native-counter"):
        prefix = name if name in ("router", "rpc") else "counters"
        route = f"/{prefix}/alpha"
        assert call(route) == {"count": 0}
        assert call(route, method="POST") == {"count": 1}
        assert call(route, method="POST") == {"count": 2}
        assert call("/counters/beta") == {"count": 0}
        assert call("/counters/alpha") == {"count": 2}
        persisted.append(("/counters/alpha", {}, {"count": 2}))
        if name == "native-counter":
            assert b"<!doctype html>" in call("/").lower()
            assert b"<!doctype html>" in call("/assets-binding/index.html").lower()
    elif name == "alarm":
        call("/alarms/demo", method="POST")
        result = eventually(lambda: call("/alarms/demo"), lambda v: v["status"] == "fired")
        assert result["alarm"] is None
        persisted.append(("/alarms/demo", {}, result))
    elif name == "filesystem":
        empty = {"entries": 0, "text": "", "file": None}
        assert call("/notebooks/alpha") == empty
        body = {"id": "first", "text": "café ☕"}
        receipt = call("/notebooks/alpha", method="POST", data=body)
        assert receipt["entries"] == 1 and receipt["size"] == len((body["text"] + "\n").encode())
        assert call("/notebooks/alpha", method="POST", data=body) == receipt
        before = call("/notebooks/alpha")
        call("/notebooks/alpha", method="POST", data={"id": "first", "text": "changed"}, status=409)
        call("/notebooks/alpha", method="POST", data={"id": "", "text": "bad"}, status=400)
        call("/notebooks/alpha", method="POST", data={**body, "extra": True}, status=400)
        call("/notebooks/alpha", method="POST", data=b"not JSON", status=400)
        call("/notebooks/alpha", method="POST", data={"id": "empty", "text": ""}, status=413)
        call("/notebooks/alpha", method="POST", data={"id": "large", "text": "x" * 4097}, status=413)
        call("/notebooks/alpha", method="POST", data={"id": "large-utf8", "text": "é" * 2049}, status=413)
        call("/notebooks/alpha", method="DELETE", status=405)
        assert call("/notebooks/alpha") == before
        commands = [{"id": f"parallel-{i}", "text": f"line {i}"} for i in range(8)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            receipts = list(pool.map(lambda command: call("/notebooks/alpha", method="POST", data=command), commands))
            repeated = list(pool.map(lambda command: call("/notebooks/alpha", method="POST", data=command), commands))
        assert repeated == receipts and sorted(r["entries"] for r in receipts) == list(range(2, 10))
        alpha = call("/notebooks/alpha")
        assert alpha["entries"] == 9 and alpha["file"]["inode"] == receipt["inode"]
        assert sorted(alpha["text"].splitlines()) == sorted([body["text"], *(c["text"] for c in commands)])
        assert call("/notebooks/beta") == empty
        beta_receipt = call("/notebooks/beta", method="POST", data={"id": "first", "text": "separate"})
        assert beta_receipt["entries"] == 1
        boundary = {"id": "maximum", "text": "é" * 2048}
        boundary_receipt = call("/notebooks/boundary", method="POST", data=boundary)
        assert boundary_receipt["entries"] == 1 and boundary_receipt["size"] == 4097
        assert call("/notebooks/boundary")["text"] == boundary["text"] + "\n"
        with ThreadPoolExecutor(max_workers=4) as pool:
            full_receipts = list(pool.map(lambda i: call("/notebooks/full", method="POST", data={"id": f"entry-{i}", "text": "x"}), range(128)))
        assert sorted(r["entries"] for r in full_receipts) == list(range(1, 129))
        full = call("/notebooks/full")
        assert full["entries"] == 128 and full["text"] == "x\n" * 128
        call("/notebooks/full", method="POST", data={"id": "overflow", "text": "bad"}, status=413)
        assert call("/notebooks/full") == full
        persisted.extend([
            ("/notebooks/alpha", {"method": "POST", "data": body}, receipt),
            ("/notebooks/alpha", {}, alpha),
            ("/notebooks/beta", {}, call("/notebooks/beta")),
            ("/notebooks/boundary", {"method": "POST", "data": boundary}, boundary_receipt),
            ("/notebooks/boundary", {}, call("/notebooks/boundary")),
            ("/notebooks/full", {"method": "POST", "data": {"id": "entry-0", "text": "x"}}, full_receipts[0]),
            ("/notebooks/full", {}, full),
        ])
    elif name == "async":
        call("/async/demo", method="POST")
        eventually(lambda: call("/async/demo"), lambda v: v == {"completed": 1})
        persisted.append(("/async/demo", {}, {"completed": 1}))
    elif name == "body":
        for path in ("/body", "/body/stream"):
            assert call(path, method="POST", data=b"\x00\x80\xffRust") == b"\x00\x80\xffRust"
        call("/body", method="POST", data=b"x" * 65537, status=413)
    elif name == "sql":
        call("/sql", method="POST", data={"message": "hello Rust"})
        result = call("/sql")
        assert result["success"] and result["results"] == [{"id": 1, "message": "hello Rust"}]
        call("/sql", method="POST", data=b"not JSON", status=400)
        # Metadata describes this read, so compare only business data after restart.
        persisted.append(("/sql", {}, result["results"]))
    elif name == "kv":
        assert call("/kv/demo", method="PUT", data="hello KV")["ttl_seconds"] == 60
        assert call("/kv/demo") == b"hello KV"
        assert call("/kv")["keys"] == ["demo"]
        call("/kv/demo", method="DELETE")
        call("/kv/demo", status=404)
    elif name == "blob":
        assert call("/blobs/demo", method="PUT", data="hello Blob")["object"]["size"] == 10
        assert call("/blobs/demo") == b"hello Blob"
        call("/blobs/demo", method="DELETE")
        call("/blobs/demo", status=404)
    elif name == "vectordb":
        assert call("/vectors/palette?rgb=250,20,20")["rows"][0][0] == "red"
        call("/vectors/palette?rgb=bad", status=400)
    elif name == "queues":
        call("/queues", method="POST", data={"id": "job1", "retry_once": True}, status=202)
        eventually(lambda: call("/counters/queue-effects"), lambda v: v == {"count": 1})
        call("/queues", method="POST", data={"id": "job1"}, status=202)
        eventually(lambda: call("/queues"), lambda v: v["backlog_count"] == 0)
        call("/queues", method="POST", data={"id": "bad1", "fail": True}, status=202)
        eventually(lambda: call("/queues/dead"), lambda v: v["backlog_count"] == 1)
        assert call("/counters/queue-effects") == {"count": 1}
        persisted.append(("/counters/queue-effects", {}, {"count": 1}))
    elif name == "cron":
        count = call("/cron")["count"]
        if with_cron:
            eventually(lambda: call("/cron"), lambda v: v["count"] > count, timeout=75)
        else:
            print("cron: add --with-cron to verify a real scheduled occurrence", flush=True)
    elif name == "workflow":
        call("/workflows/report1", method="POST", data={"title": "Cellar report"}, status=202)
        result = eventually(lambda: call("/workflows/report1"), lambda v: v["state"] == "Complete")
        assert result["output"]["key"] == "report-report1"
        assert json.loads(call("/blobs/report-report1")) == {"format": "json", "report": {"title": "Cellar report"}}
        persisted.append(("/workflows/report1", {}, result))
    elif name in ("wsecho", "wsclient"):
        assert call("/wsclient") == {"echo": "native Rust hello"}
        assert call("/rooms/client/state") == {"messages": 1}
        call("/rooms/demo", status=426)
        persisted.append(("/rooms/client/state", {}, {"messages": 1}))
    elif name == "facets":
        assert call("/facets/demo", method="POST") == {"count": 1}
        assert call("/facets/another") == {"count": 0}
        persisted.append(("/facets/demo", {}, {"count": 1}))
    elif name == "multi-tenant":
        path = "/tenants/acme/accounts/main"
        options = {"method": "POST", "token": "local-acme", "data": {"id": "credit1", "delta": 10}}
        assert call(path, **options) == call(path, **options) == {"id": "credit1", "delta": 10, "balance": 10}
        call(path, method="POST", token="local-acme", data={"id": "debit1", "delta": -20}, status=409)
        call(path, method="POST", token="local-acme", data={"id": "credit1", "delta": 11}, status=409)
        assert call("/tenants/beta/accounts/main", token="local-beta") == {"balance": 0}
        call(path, token="local-beta", status=403)
        call(path, status=401)
        persisted.append((path, {"token": "local-acme"}, {"balance": 10}))
    elif name == "transactional-outbox":
        body = {"id": "order1", "amount": 10}
        call("/outbox/shop", method="POST", data=body, status=202)
        eventually(lambda: call("/outbox/shop"), lambda v: v == {"orders": [["order1", 1]]})
        eventually(lambda: call("/counters/queue-effects"), lambda v: v == {"count": 1})
        call("/outbox/shop", method="POST", data=body, status=202)
        call("/outbox/shop", method="POST", data={"id": "order1", "amount": 11}, status=409)
        # The same order id in a second cell must remain a distinct publication.
        call("/outbox/another", method="POST", data=body, status=202)
        eventually(lambda: call("/counters/queue-effects"), lambda v: v == {"count": 2})
        persisted.extend([("/outbox/shop", {}, {"orders": [["order1", 1]]}), ("/counters/queue-effects", {}, {"count": 2})])
    elif name == "durable-agent":
        call("/agents/demo", method="POST", data={"text": "native durable tools"})
        result = eventually(lambda: call("/agents/demo"), lambda v: v["stage"] == "complete")
        assert result["words"] == 3 and result["steps"] == 2
        call("/agents/demo", method="POST", data={"text": "another job"}, status=409)
        persisted.append(("/agents/demo", {}, result))
    elif name == "native-websocket":
        for kind in ("worker", "cell"):
            result = json.loads(call(f"/bind/{kind}"))
            greeting = "worker greeting" if kind == "worker" else "room greeting"
            assert result["messages"] == [greeting, "reply:client hello", [0, 128, 255]]
            assert result["close"] == [4001, "complete", True]
        result = eventually(lambda: json.loads(call("/room-state")), lambda v: v.get("close") == 4001)
        assert result["received"] == [0, 128, 255]
        persisted.append(("/room-state", {}, result))
    else:
        raise AssertionError(f"missing focused scenario: {name}")
    return persisted


def run_one(cellar, entry, with_cron, profile=None):
    examples = Path(__file__).resolve().parent
    extra = {"CELLAR_WEBSOCKET_PEER": "ws://127.0.0.1:9876"} if entry["name"] == "native-websocket" else None
    with tempfile.TemporaryDirectory(prefix="cellar-focused-example-") as temporary:
        root = Path(temporary)
        project = private_project(examples / entry["project"], root)
        config = json.loads(project.read_text())
        if profile is not None:
            config["profile"] = profile
            project.write_text(json.dumps(config))
        selected_profile = config.get("profile", "release")
        host = Host(cellar, project, root, extra)
        try:
            persisted = exercise(entry["name"], host.ready(), with_cron)
        except BaseException:
            print(host.log.read_text(errors="replace")[-12000:], flush=True)
            raise
        finally:
            host.stop()
        if persisted:
            host = Host(cellar, project, root, extra)
            try:
                origin = host.ready()
                for path, options, expected in persisted:
                    result = request(origin, path, **options)
                    if entry["name"] == "sql":
                        result = result["results"]
                    elif entry["name"] == "native-websocket":
                        result = json.loads(result)
                    assert result == expected, (entry["name"], path, result, expected)
                if entry["name"] == "filesystem":
                    before = request(origin, "/notebooks/alpha")
                    receipt = request(origin, "/notebooks/alpha", method="POST", data={"id": "after-restart", "text": "new owner"})
                    after = request(origin, "/notebooks/alpha")
                    assert receipt["entries"] == after["entries"] == before["entries"] + 1
                    assert after["text"] == before["text"] + "new owner\n"
                    assert after["file"]["inode"] == before["file"]["inode"]
                    assert after["file"]["version"] != before["file"]["version"]
            finally:
                host.stop()
        print(f"PASS {entry['name']} ({selected_profile}): focused configuration, behavior and graceful stop" + ("; restart state retained" if persisted else ""), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar", type=Path, required=True)
    parser.add_argument("--example", action="append", help="catalog name; repeat to select several (default: every local HTTP project)")
    parser.add_argument("--with-cron", action="store_true")
    parser.add_argument("--profile", choices=["dev", "release"], help="build profile for the staged application (default: its project profile)")
    args = parser.parse_args()
    catalog = json.loads(Path(__file__).with_name("catalog.json").read_text())["examples"]
    entries = [e for e in catalog if "project" in e and e["status"] not in ("requires-container-engine", "requires-python-runtime", "requires-node-runtime")]
    if args.example:
        names = {e["name"] for e in entries}
        missing = set(args.example) - names
        if missing:
            parser.error(f"not a local HTTP project: {sorted(missing)}")
        entries = [e for e in entries if e["name"] in args.example]
    for entry in entries:
        run_one(args.cellar.resolve(), entry, args.with_cron, args.profile)
    print(f"PASS {len(entries)} focused Rust projects", flush=True)


if __name__ == "__main__":
    main()
