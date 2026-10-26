# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Audit real generated-client peer evidence, including adversarial report probes."""
import copy
import hashlib
import itertools
import json
import re
import sys
from pathlib import Path

LANGUAGES = {"rust": "RustCounter", "python": "PythonCounter", "v8": "V8Counter", "node": "NodeCounter"}
SCENARIOS = ("reconnect", "reply_loss", "method_refusal")


def audit(report):
    assert report["format"] == 1
    assert report["topology"] == "signed-peer-fault-relay"
    assert report["fleet_durability"] is True
    assert report["native_nodes_drained"] is report["relay_joined"] is True
    assert report["relay_live_rpc_connections"] == 0
    assert report["remote_fleet_qualified"] is report["abrupt_crash_qualified"] is False
    assert re.fullmatch(r"[0-9a-f]{64}", report["contract_digest"])
    cases = report["cases"]
    assert len(cases) == report["case_count"] == 48
    expected = set(itertools.product(SCENARIOS, LANGUAGES, LANGUAGES))
    keys = [(c["scenario"], c["caller"], c["provider"]) for c in cases]
    assert len(set(keys)) == len(keys) and set(keys) == expected
    seen_ids = set()
    owners = {}
    totals = {language: set() for language in LANGUAGES}
    for case in cases:
        scenario, caller, provider = case["scenario"], case["caller"], case["provider"]
        assert case["source"] == "peer-fault-source" and case["owner"] == "peer-fault-owner"
        assert case["class"] == LANGUAGES[provider]
        identity = (case["scope"], case["epoch"])
        assert re.fullmatch(re.escape(case["class"]) + r":[0-9a-f]{64}", identity[0])
        assert type(identity[1]) is int and identity[1] > 0
        assert owners.setdefault(provider, identity) == identity
        before, after = case["counter_before"], case["counter_after"]
        assert isinstance(before, str) and isinstance(after, str)
        assert str(int(before)) == before and str(int(after)) == after
        assert int(after) == int(before) + 1
        totals[provider].add(int(before))
        assert case["explicit_receipt_retries"] == 2 and case["changed_command_refused"] is True
        assert case["automatic_replay"] is False and case["factory_count_change"] == 0
        first_handlers = 0 if scenario == "method_refusal" else 1
        assert case["first_handler_count"] == first_handlers
        assert case["total_handler_count"] == first_handlers + 3
        peer = case["peer"]
        assert peer["scope"] == case["scope"]
        assert peer["method"] == "example.counter.v1.Counter/Increment"
        assert peer["operation_id"] == f"{scenario}-{caller}-{provider}"
        assert peer["delta"] == 1 and 0 < peer["remaining_ms"] <= 10000
        request_id = peer["request_id"]
        assert re.fullmatch(r"[0-9a-f]{32}", request_id) and request_id not in seen_ids
        seen_ids.add(request_id)
        assert peer["reply_lost"] is (scenario == "reply_loss")
        if scenario == "method_refusal":
            assert peer["trusted_refusal"] is True
            assert case["first_reply"] == {"code": "MethodNotFound", "outcome": "not_committed"}
        else:
            assert peer["successful_owner_reply_observed"] is True
            if scenario == "reply_loss":
                assert case["first_reply"] == {"code": "Unavailable", "outcome": "unknown"}
            else:
                assert case["first_reply"] == {"value": after}
                assert case["new_signed_handshakes"] > 0
    assert all(values == set(range(12)) for values in totals.values())


def main():
    path = Path(sys.argv[1])
    data = path.read_bytes()
    report = json.loads(data)
    report = report.get("peer_faults", report)
    audit(report)
    # A report must fail closed when coverage, provenance or cleanup is lost.
    probes = (
        lambda r: r["cases"].pop(),
        lambda r: r["cases"].__setitem__(1, copy.deepcopy(r["cases"][0])),
        lambda r: next(c for c in r["cases"] if c["scenario"] == "reply_loss")["first_reply"].update(outcome="not_committed"),
        lambda r: next(c for c in r["cases"] if c["scenario"] == "method_refusal").update(first_handler_count=1),
        lambda r: r["cases"][0]["peer"].update(remaining_ms=10001),
        lambda r: r.update(relay_live_rpc_connections=1),
    )
    for mutate in probes:
        changed = copy.deepcopy(report)
        mutate(changed)
        try:
            audit(changed)
        except AssertionError:
            continue
        raise AssertionError("peer report auditor accepted corrupted evidence")
    print(json.dumps({"cases": 48, "negative_audit_probes": len(probes),
                      "sha256": hashlib.sha256(data).hexdigest(), "release_qualified": False}, indent=2))


if __name__ == "__main__":
    main()
