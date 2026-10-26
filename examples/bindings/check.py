#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Check a running binding example using only the Python standard library."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import re
import urllib.error
import urllib.request
import uuid


def request(origin, path, method="GET", statuses=(200,)):
    req = urllib.request.Request(origin + path, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        assert response.status in statuses, (path, response.status, body[:1000])
        assert "application/json" in response.headers.get("Content-Type", "")
        return response.status, json.loads(body)


def read(origin, binding, name):
    return request(origin, f"/bindings/{binding}/{name}")[1]


def check(origin, name):
    """Return the snapshots that a restart must preserve, including cell scopes."""
    first = read(origin, "COUNTERS", name)
    alias = read(origin, "COUNTER_ALIAS", name)
    other_name = read(origin, "COUNTERS", name + "-other")
    other_class = read(origin, "OTHER_COUNTERS", name)
    assert alias == first, "binding aliases must resolve to the same state"
    assert first["class"] == "Counter" and first["name"] == name
    assert other_class["class"] == "OtherCounter" and other_class["name"] == name
    assert len({first["scope"], other_name["scope"], other_class["scope"]}) == 3

    for index, binding in enumerate(("COUNTERS", "COUNTER_ALIAS"), start=1):
        _, updated = request(origin, f"/bindings/{binding}/{name}", "POST")
        assert updated == {**first, "count": first["count"] + index}
        assert read(origin, "COUNTERS", name) == updated
        assert read(origin, "COUNTER_ALIAS", name) == updated

    # A different name and a different class must remain untouched by both aliases.
    assert read(origin, "COUNTERS", name + "-other") == other_name
    assert read(origin, "OTHER_COUNTERS", name) == other_class
    _, isolated = request(origin, f"/bindings/OTHER_COUNTERS/{name}", "POST")
    assert isolated == {**other_class, "count": other_class["count"] + 1}
    assert read(origin, "COUNTERS", name) == updated

    # Concurrent alias calls share one database. Guest compare-and-write conflicts
    # may return 409; every acknowledged increment must appear exactly once.
    paths = [f"/bindings/{binding}/{name}" for binding in
             ("COUNTERS", "COUNTER_ALIAS") * 3]
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda path: request(origin, path, "POST", (200, 409)), paths))
    successes = [value for status, value in results if status == 200]
    assert successes, results
    for status, value in results:
        if status == 409:
            assert value == {"error": "concurrent update; increment not applied"}
        else:
            assert value["scope"] == first["scope"]
    expected_count = updated["count"] + len(successes)
    assert sorted(value["count"] for value in successes) == list(
        range(updated["count"] + 1, expected_count + 1)
    )
    final = read(origin, "COUNTERS", name)
    assert final == {**first, "count": expected_count}
    assert read(origin, "COUNTER_ALIAS", name) == final
    assert read(origin, "COUNTERS", name + "-other") == other_name
    assert read(origin, "OTHER_COUNTERS", name) == isolated

    # These are application route validation checks, not resource provisioning.
    request(origin, "/bindings/UNKNOWN/demo", statuses=(404,))
    request(origin, "/bindings/COUNTERS/", statuses=(400,))
    request(origin, "/bindings/COUNTERS/" + "x" * 65, statuses=(400,))
    request(origin, "/bindings/COUNTERS/invalid%20name", statuses=(400,))
    request(origin, f"/bindings/COUNTERS/{name}", "DELETE", (405,))
    request(origin, "/no-such-route", statuses=(404,))
    assert read(origin, "COUNTERS", name) == final

    print(f"PASS {name}: aliases, name/class isolation, concurrent increments and validation", flush=True)
    return {
        ("COUNTERS", name): final,
        ("COUNTER_ALIAS", name): final,
        ("COUNTERS", name + "-other"): other_name,
        ("OTHER_COUNTERS", name): isolated,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("origin", nargs="?", default="http://127.0.0.1:9876")
    parser.add_argument("--name", default="check-" + uuid.uuid4().hex[:12])
    args = parser.parse_args()
    if re.fullmatch(r"[A-Za-z0-9_-]{1,48}", args.name) is None:
        parser.error("--name must contain 1-48 ASCII letters, digits, underscores or hyphens")
    check(args.origin.rstrip("/"), args.name)


if __name__ == "__main__":
    main()
