# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Curl transport and a visible, sequential walkthrough for smoke.py --curl-demo."""

import json
import shlex
import subprocess


def request(origin, path, method="GET", statuses=(200,)):
    # Print the exact command and response. Never retry a non-idempotent POST.
    command = ["curl", "--silent", "--show-error", "--max-time", "20",
               "--write-out", "\\n%{http_code}"]
    if method != "GET":
        command.extend(["-X", method])
    command.append(origin + path)
    print("$ " + shlex.join(command), flush=True)
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
    body, status = result.stdout.rsplit("\n", 1)
    print(body, flush=True)
    assert int(status) in statuses, (path, status, body[:1000])
    return int(status), json.loads(body)


def read(origin, binding, name):
    return request(origin, f"/bindings/{binding}/{name}")[1]


def check(origin, name):
    """Show aliases sharing writes while another name and class stay independent."""
    first = read(origin, "COUNTERS", name)
    other_name = read(origin, "COUNTERS", name + "-other")
    other_class = read(origin, "OTHER_COUNTERS", name)
    assert first["count"] == other_name["count"] == other_class["count"] == 0
    assert first["class"] == "Counter" and first["name"] == name
    assert other_name["class"] == "Counter" and other_name["name"] == name + "-other"
    assert other_class["class"] == "OtherCounter" and other_class["name"] == name
    assert len({first["scope"], other_name["scope"], other_class["scope"]}) == 3
    assert read(origin, "COUNTER_ALIAS", name) == first

    for index, binding in enumerate(("COUNTERS", "COUNTER_ALIAS"), start=1):
        _, updated = request(origin, f"/bindings/{binding}/{name}", "POST")
        assert updated == {**first, "count": first["count"] + index}
        assert read(origin, "COUNTERS", name) == updated
        assert read(origin, "COUNTER_ALIAS", name) == updated
        assert read(origin, "COUNTERS", name + "-other") == other_name
        assert read(origin, "OTHER_COUNTERS", name) == other_class

    _, isolated_name = request(origin, f"/bindings/COUNTERS/{name}-other", "POST")
    assert isolated_name == {**other_name, "count": other_name["count"] + 1}
    for index in range(1, 4):
        _, isolated_class = request(origin, f"/bindings/OTHER_COUNTERS/{name}", "POST")
        assert isolated_class == {**other_class, "count": other_class["count"] + index}
    expected = {
        ("COUNTERS", name): updated,
        ("COUNTER_ALIAS", name): updated,
        ("COUNTERS", name + "-other"): isolated_name,
        ("OTHER_COUNTERS", name): isolated_class,
    }
    for (binding, entity), snapshot in expected.items():
        assert read(origin, binding, entity) == snapshot
    print("PASS curl: shared aliases = 2, separate name = 1, separate class = 3", flush=True)
    return expected
