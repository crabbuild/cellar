"""One-shot update targets and conservative recovery checks for offered load."""
import json

from perf import CREATE, UPDATE, batch, require


def targets(origin, count, prefix):
    paths = [f"/orders/{prefix}-{index}" for index in range(count)]
    created = batch(origin, [("POST", path, CREATE, 201) for path in paths])
    before = batch(origin, [("GET", path, None, 200) for path in paths])
    require(created == before, "seed read differs from acknowledged creation")
    rows = []
    for path in paths:
        order = before[path]
        require(order["order_id"] == path.rsplit("/", 1)[1] and order["version"] == 1
                and order["status"] == "pending" and order["total_cents"] == 2500,
                "invalid update seed acknowledgement")
        # GET preserves the stored object's field order used by the subsequent
        # invocation snapshot. PATCH replaces values without adding fields.
        expected = dict(order, status="confirmed", version=2)
        rows.append(dict(path=path, body=list(UPDATE),
                         expected_body=list(json.dumps(expected, separators=(",", ":")).encode())))
    return dict(updates=rows), before


def reconcile(raw, before, observed):
    """A lost response may have committed; never relabel it as an acknowledgement."""
    rows = raw["workload"]["updates"]
    require(len(rows) == len(raw["samples"]) and len({row["path"] for row in rows}) == len(rows),
            "incomplete or duplicate update targets")
    require(set(before) == set(observed) == {row["path"] for row in rows}, "missing recovery observations")
    result = dict(acknowledged=0, not_dispatched=0, uncertain_unchanged=0, uncertain_committed=0)
    for index, (target, sample) in enumerate(zip(rows, raw["samples"])):
        require(sample["id"] == index, "update arrival identity changed")
        path = target["path"]
        expected = json.loads(bytes(target["expected_body"]))
        actual = observed[path]
        outcome = sample["outcome"]
        if outcome == "success":
            require(actual == expected, "acknowledged update lost or changed after restart")
            key = "acknowledged"
        elif outcome.startswith("client_"):
            require(sample["dispatched_ns"] is None and actual == before[path],
                    "undispatched update changed durable state")
            key = "not_dispatched"
        else:
            # HTTP errors, partial/corrupt bodies and transport loss are not
            # rollback proofs, even if a response prefix names an SDK failure.
            require(actual == before[path] or actual == expected, "uncertain update has unexpected state")
            key = "uncertain_committed" if actual == expected else "uncertain_unchanged"
        result[key] += 1
    return result
