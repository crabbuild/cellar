# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independently audit source admission coverage, provenance and cleanup."""
import copy
import hashlib
import itertools
import json
import re
import sys
from pathlib import Path

LANGUAGES = {"rust": "RustCounter", "python": "PythonCounter", "v8": "V8Counter", "node": "NodeCounter"}


def audit(report):
    assert report["format"] == 1
    assert report["topology"] == "local-and-signed-peer-rpc-saturation"
    assert re.fullmatch(r"[0-9a-f]{64}", report["contract_digest"])
    assert report["native_nodes_drained"] is True
    assert report["source_reservations_after_drain"] == report["receiver_reservations_after_drain"] == report["receiver_tunnels_after_drain"] == 0
    assert report["all_memory_qualified"] is report["release_qualified"] is False
    cases = report["cases"]
    assert len(cases) == report["case_count"] == 144
    expected = {(side, route, limit, caller, provider) for side, route in (("source", "local"), ("source", "signed-peer"), ("receiver", "signed-peer"))
                for limit, caller, provider in itertools.product(("requests", "bytes"), LANGUAGES, LANGUAGES)}
    expected |= {("receiver-reply", "signed-peer", "bytes", caller, provider)
                 for caller, provider in itertools.product(LANGUAGES, LANGUAGES)}
    expected |= {("provider-response", route, "bytes", caller, provider)
                 for route, caller, provider in itertools.product(("local", "signed-peer"), LANGUAGES, LANGUAGES)}
    keys = [(c["side"], c["route"], c["limit"], c["caller"], c["provider"]) for c in cases]
    assert len(set(keys)) == len(keys) and set(keys) == expected
    identities = {}
    counts = {}
    zero = {"active_requests": 0, "reserved_request_bytes": 0}
    for case in cases:
        route, limit, caller, provider = case["route"], case["limit"], case["caller"], case["provider"]
        assert case["class"] == LANGUAGES[provider]
        assert case["source"] == "saturation-source"
        assert case["owner"] == ("saturation-owner" if route == "signed-peer" else case["source"])
        assert isinstance(case["scope"], str) and case["scope"]
        assert type(case["epoch"]) is int and case["epoch"] > 0
        key = case["side"], route, limit, provider
        identity = case["scope"], case["epoch"]
        assert identities.setdefault(key, identity) == identity
        before, after = int(case["counter_before"]), int(case["counter_after"])
        assert str(before) == case["counter_before"] and str(after) == case["counter_after"]
        assert before == counts.get(key, 0) and after == before + 2
        counts[key] = after
        held = case["held"]
        assert held == {"node": case["owner"], "class": case["class"], "scope": case["scope"],
                        "epoch": case["epoch"], "count": str(before), "transaction_open": False}
        usage = case["usage_while_held"]
        assert usage["active_requests"] == 1
        assert type(usage["reserved_request_bytes"]) is int
        assert 65536 < usage["reserved_request_bytes"] <= case["max_rpc_bytes"]
        response = case["response_usage_while_held"]
        assert response == {"active_buffers": 1, "reserved_response_bytes": 4 * (1024 * 1024 + 2048) + 65536}
        assert case["response_usage_after_case"] == {"active_buffers": 0, "reserved_response_bytes": 0}
        if case["side"] == "provider-response":
            assert limit == "bytes" and case["max_rpc_requests"] == 1
            assert case["max_rpc_bytes"] == response["reserved_response_bytes"]
            assert usage["reserved_request_bytes"] == response["reserved_response_bytes"]
        elif limit == "requests":
            assert case["max_rpc_requests"] == 1
            assert case["max_rpc_bytes"] == 256 * 1024 * 1024
        else:
            assert case["max_rpc_requests"] == 128
            assert case["max_rpc_bytes"] == 128 * 1024
        assert case["first_refusal"] == {"code": "ResourceExhausted", "outcome": "not_committed" if case["side"] == "source" else "unknown"}
        assert case["refused_handler_count"] == case["factory_count_change"] == 0
        reply_limit = case["side"] == "receiver-reply"
        assert case["holder_caller"] == (caller if reply_limit else "rust")
        assert case["holder_stored_count"] == str(before + 1)
        assert case["holder_transaction_open"] is False
        assert type(case["holder_write_position"]) is int and case["holder_write_position"] >= 0
        assert type(case["holder_valid_message_bytes"]) is int
        assert 0 < case["holder_valid_message_bytes"] <= 1024 * 1024
        assert case["holder_receipt_replayed"] is reply_limit
        if reply_limit:
            assert case["holder_first_reply"] == {"code": "ResourceExhausted", "outcome": "unknown"}
            assert case["holder_valid_message_bytes"] > 65536
        else:
            assert case["holder_first_reply"] == {"value": str(before + 1)}
        assert case["total_handler_count"] == (5 if reply_limit else 4)
        assert case["explicit_receipt_retries"] == (3 if reply_limit else 2)
        assert case["changed_command_refused"] is True and case["automatic_replay"] is False
        assert case["usage_after_case"] == zero
    assert set(counts.values()) == {8}


def reply_case(report):
    return next(case for case in report["cases"] if case["side"] == "receiver-reply")


def negative_probes(report):
    mutations = (
        lambda r: r["cases"].pop(),
        lambda r: r["cases"].__setitem__(1, r["cases"][0]),
        lambda r: r["cases"][0]["first_refusal"].__setitem__("outcome", "forged"),
        lambda r: r["cases"][0].__setitem__("refused_handler_count", 1),
        lambda r: r["cases"][0]["usage_after_case"].__setitem__("reserved_request_bytes", 1),
        lambda r: r["cases"][0]["usage_while_held"].__setitem__("active_requests", 0),
        lambda r: r["cases"][0].__setitem__("max_rpc_requests", 0),
        lambda r: r["cases"][0]["held"].__setitem__("epoch", 0),
        lambda r: r["cases"][0].__setitem__("automatic_replay", True),
        lambda r: r.__setitem__("native_nodes_drained", False),
        lambda r: r["cases"][0]["response_usage_while_held"].__setitem__("active_buffers", 0),
        lambda r: r["cases"][0]["response_usage_after_case"].__setitem__("reserved_response_bytes", 1),
        lambda r: next(c for c in r["cases"] if c["side"] == "provider-response").__setitem__("max_rpc_bytes", 0),
        lambda r: reply_case(r)["holder_first_reply"].__setitem__("outcome", "not_committed"),
        lambda r: reply_case(r).__setitem__("holder_valid_message_bytes", 1),
        lambda r: reply_case(r).__setitem__("holder_receipt_replayed", False),
        lambda r: reply_case(r).__setitem__("holder_stored_count", "0"),
        lambda r: reply_case(r).__setitem__("holder_caller", "wrong-language"),
        lambda r: reply_case(r).__setitem__("holder_transaction_open", True),
        lambda r: reply_case(r).__setitem__("holder_write_position", None),
    )
    for mutation in mutations:
        damaged = copy.deepcopy(report)
        mutation(damaged)
        try:
            audit(damaged)
        except (AssertionError, KeyError, TypeError, ValueError):
            continue
        raise AssertionError("saturation auditor accepted corrupted evidence")
    return len(mutations)


def main():
    path = Path(sys.argv[1])
    data = path.read_bytes()
    report = json.loads(data)
    audit(report)
    probes = negative_probes(report)
    print(json.dumps({"case_count": report["case_count"], "negative_probes": probes,
                      "sha256": hashlib.sha256(data).hexdigest(), "release_qualified": False}))


if __name__ == "__main__":
    main()
