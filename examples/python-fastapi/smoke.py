#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Exercise a restored FastAPI executable with empty-environment native hosts."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from smoke import request
from python.smoke import check_report


@contextmanager
def host(executable, store, work, name):
    log = work / (name + ".log")
    with log.open("wb") as output:
        child = subprocess.Popen([str(executable), "--no-control-plane", "--listen", "127.0.0.1:0",
                                  "--internal-listen", "127.0.0.1:0"], stdout=output, stderr=output,
            env={"CELLAR_NATIVE_FLEET": "1", "CELLAR_INTERNAL_DEV_STORE": str(store),
                 "CELLAR_BUCKET": "cellar-dev", "CELLAR_NODE": "fastapi-example",
                 "CELLAR_TEST_DATA_DIR": str(work / "node"), "CELLAR_TOKIO_THREADS": "2",
                 "CELLAR_PLACEMENT_WEIGHT": "1", "CELLAR_REBALANCE_INTERVAL_MS": "0",
                 "CELLAR_DURABILITY": "bucket", "CELLAR_SHUTDOWN_TOTAL_MS": "8000",
                 "CELLAR_SHUTDOWN_REPORT_JSON": "1"})
        try:
            deadline = time.monotonic() + 30
            while True:
                text = log.read_text(errors="replace")
                addresses = {}
                for line in text.splitlines():
                    for key, prefix in [("origin", "cellar listening on "),
                                        ("internal", "cellar internal listening on ")]:
                        if line.startswith(prefix):
                            addresses[key] = "http://" + line.removeprefix(prefix).split()[0]
                if len(addresses) == 2:
                    try:
                        request(addresses["origin"], "/health")
                        break
                    except urllib.error.URLError:
                        pass
                if child.poll() is not None or time.monotonic() >= deadline:
                    raise AssertionError("FastAPI readiness failed: " + text[-12000:])
                time.sleep(0.05)
            try:
                yield addresses["origin"]
            finally:
                request(addresses["internal"], "/shutdown?handoff=preserve", method="POST")
                code = child.wait(timeout=35)
                if code != 0:
                    raise AssertionError(log.read_text(errors="replace")[-12000:])
                check_report(log)
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--expect-free-threaded", action="store_true")
    args = parser.parse_args()
    payload = {"customer_id": "customer-7", "items": [
        {"sku": "coffee", "quantity": 2, "unit_price_cents": 1250}]}

    def creation_wire(origin):
        operation = urllib.request.Request(origin + "/orders/order-1", method="POST",
            data=json.dumps(payload).encode(), headers={"content-type": "application/json"})
        with urllib.request.urlopen(operation, timeout=20) as response:
            assert response.status == 201
            return response.status, response.read(), response.headers["etag"]

    with host(args.host, args.store, args.work, "fastapi-first") as origin:
        health = request(origin, "/health")
        assert health["framework"] == "fastapi" and health["version"] == "0.157.0", health
        assert health["free_threaded"] == args.expect_free_threaded, health
        if args.expect_free_threaded:
            assert not health["gil_enabled"], health
        schema = request(origin, "/openapi.json")
        assert schema["info"]["title"] == "Cellar Orders"
        assert "post" in schema["paths"]["/orders/{order_id}"]
        assert b"swagger-ui" in request(origin, "/docs")
        request(origin, "/orders/missing", status=404)
        invalid = dict(payload, items=[{"sku": "coffee", "quantity": "2", "unit_price_cents": 1250}])
        request(origin, "/orders/invalid", method="POST", data=invalid, status=422)
        request(origin, "/orders/invalid", status=404)
        receipt = request(origin, "/orders/order-1", method="POST", data=payload, status=201)
        wire = creation_wire(origin)
        assert receipt["total_cents"] == 2500 and receipt["version"] == 1 and receipt["status"] == "pending"
        assert request(origin, "/orders/order-1", method="POST", data=payload, status=201) == receipt
        with ThreadPoolExecutor(max_workers=4) as pool:
            def create(index):
                value = dict(payload, customer_id="customer-" + str(index))
                return request(origin, "/orders/parallel-" + str(index), method="POST", data=value, status=201)
            parallel = list(pool.map(create, range(8)))
        assert [order["customer_id"] for order in parallel] == ["customer-" + str(i) for i in range(8)]
        request(origin, "/orders/order-1", method="POST", data=dict(payload, customer_id="different"), status=409)
        updated = request(origin, "/orders/order-1", method="PATCH",
                          data={"expected_version": 1, "status": "confirmed"})
        assert updated["version"] == 2 and updated["status"] == "confirmed"
        request(origin, "/orders/order-1", method="PATCH",
                data={"expected_version": 1, "status": "cancelled"}, status=409)
        # Creation retries replay the original response even after later writes.
        assert request(origin, "/orders/order-1", method="POST", data=payload, status=201) == receipt
        assert creation_wire(origin) == wire
    with host(args.host, args.store, args.work, "fastapi-second") as origin:
        assert request(origin, "/orders/order-1") == updated
        assert request(origin, "/orders/order-1", method="POST", data=payload, status=201) == receipt
        assert creation_wire(origin) == wire
        for index, expected in enumerate(parallel):
            assert request(origin, "/orders/parallel-" + str(index)) == expected
        second = request(origin, "/orders/order-2", method="POST", data=payload, status=201)
        assert second["order_id"] == "order-2"
    print("PASS installed FastAPI: validation, OpenAPI, durable orders, receipt replay, conflicts, restart and joined reports")


if __name__ == "__main__":
    main()
