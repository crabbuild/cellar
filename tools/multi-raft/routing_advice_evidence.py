# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Reject incomplete fault ledgers; native cases own TLS and receipt verification."""

import copy
import hashlib
import json
import re

WIRE = "rustfs_wire_advice_preserves_admission"
TLS = "rustfs_untrusted_tls_advice_cannot_grant_authority"
BOOT = "rustfs_superseded_boot_advice_cannot_change_route"
REMOVED = "rustfs_removed_member_advice_cannot_grant_authority"
FAULTS = (
    "ForeignCluster", "ForeignNamespace", "ForeignGroup", "ForeignIncarnation",
    "NonMember", "Configuration", "LowerTerm", "ConflictingTerm",
    "WrongProcess", "WrongExporter",
)
AUTHENTICATED = {"NonMember", "Configuration", "LowerTerm", "ConflictingTerm"}


def _uint(value, positive=False):
    return type(value) is int and int(positive) <= value < 2**64


def _hex(value, size):
    return isinstance(value, str) and re.fullmatch(rf"[0-9a-f]{{{size}}}", value) is not None


def _bytes(value, limit):
    if (not isinstance(value, str) or not 0 < len(value) <= 2 * limit
            or len(value) % 2 or re.fullmatch("[0-9a-f]+", value) is None):
        raise ValueError("invalid retained wire bytes")
    return bytes.fromhex(value)


def _peer(peer):
    return (isinstance(peer, dict)
            and all(_hex(peer.get(field), 32) and peer[field] != "00" * 16
                    for field in ("node", "disk", "process"))
            and all(isinstance(peer.get(field), str) and peer[field]
                    for field in ("query_address", "server_name", "label")))


def _common(row):
    before = row.get("callbacks_before")
    command = row.get("command")
    failure = row.get("failure")
    route = row.get("routing_before"), row.get("routing_after_refusal")
    if (not _uint(before, True) or before >= 2**63 - 1
            or type(row.get("callbacks_after_refusal")) is not int
            or row["callbacks_after_refusal"] != before
            or type(row.get("callbacks_after_replay")) is not int
            or row["callbacks_after_replay"] != before + 1
            or not _uint(row.get("sql_stages_before"))
            or type(row.get("sql_stages_after_refusal")) is not int
            or row["sql_stages_after_refusal"] != row["sql_stages_before"]
            or row.get("required_durability") != "S3" or row.get("achieved_durability") != "S3"
            or row.get("result_be") != (before + 1).to_bytes(8, "big").hex()
            or type(row.get("request_timeout_ms")) is not int or row["request_timeout_ms"] != 5000
            or not _uint(row.get("retry_until_ms"), True)
            or not isinstance(command, dict) or not _hex(command.get("client"), 32)
            or command["client"] == "00" * 16 or not _uint(command.get("sequence"), True)
            or not _peer(row.get("issuer")) or not _peer(row.get("origin"))
            or row["issuer"]["node"] == row["origin"]["node"]
            or not _uint(row.get("membership_revision"), True)
            or not _hex(row.get("configuration_digest"), 64)
            or row["configuration_digest"] == "00" * 32
            or not isinstance(failure, dict) or type(failure.get("ambiguous")) is not bool
            or not isinstance(failure.get("failure"), str) or not failure["failure"]
            or not _uint(row.get("proven_rejections_before"))
            or not _uint(row.get("proven_rejections_after_refusal"))
            or any(not isinstance(values, list) or len(values) != 6
                   or any(not _uint(value) for value in values) for values in route)
            or any(a < b for b, a in zip(*route))
            or route[1][0] <= route[0][0] or route[1][4] <= route[0][4]):
        raise ValueError("invalid advice identity, admission, routing or S3 evidence")


def _frame(row, frame, *, leader=None):
    if not isinstance(frame, dict) or frame.get("fault") != row["fault"]:
        raise ValueError("wrong wire fault")
    request = _bytes(frame.get("request"), 64 * 1024)
    response = _bytes(frame.get("response"), 64 * 1024)
    header = frame.get("request_header")
    reply = frame.get("response_header")
    command = row["command"]
    if (len(request) < 74 or request[:8] != b"CLGCCQ01"
            or request[8:24].hex() != command["client"]
            or int.from_bytes(request[24:32], "big") != command["sequence"]
            or frame.get("client") != command["client"] or frame.get("sequence") != command["sequence"]
            or frame.get("input_sha256") != hashlib.sha256(request).hexdigest()
            or not isinstance(header, dict) or set(header) != {"route", "sequence", "digest"}
            or header["digest"] != list(hashlib.sha256(request).digest())
            or header.get("route") != frame.get("route") or not isinstance(header.get("sequence"), str)
            or not header["sequence"].isdecimal() or str(int(header["sequence"])) != header["sequence"]
            or not 0 < int(header["sequence"]) < 2**64):
        raise ValueError("invalid original command framing")
    operation_length = int.from_bytes(request[72:74], "big")
    position = 74 + operation_length
    timeout = position + 32 + 1 + 8 + 8 + 32
    if (request[74:position] != b"Increment" or len(request) != timeout + 8 + 4 + 8
            or request[position + 32] != 2
            or int.from_bytes(request[position + 33:position + 41], "big") != row["retry_until_ms"]
            or not 0 < int.from_bytes(request[timeout:timeout + 8], "big") <= 5000
            or int.from_bytes(request[timeout + 8:timeout + 12], "big") != 8
            or request[-8:] != (1).to_bytes(8, "big")):
        raise ValueError("command input, durability or original deadline changed")
    route = header["route"]
    if (not isinstance(route, dict) or route.get("purpose") != "Command"
            or route.get("source", {}).get("member") is not None
            or route.get("target", {}).get("member") != str(row["seed_member"])
            or route.get("scope", {}).get("kind") != "Shared"):
        raise ValueError("invalid authenticated command route")
    for endpoint, field in (("source", "origin"), ("target", "issuer")):
        if any(route[endpoint].get(key) != list(bytes.fromhex(row[field][key]))
               for key in ("node", "disk", "process")):
            raise ValueError("wrong enrolled physical endpoint")
    group = route["scope"]["identity"]["group"]
    if (not isinstance(group, list) or len(group) != 4
            or any(not isinstance(part, list) or len(part) != 16
                   or any(type(n) is not int or not 0 <= n <= 255 for n in part) for part in group)):
        raise ValueError("invalid group history")
    if len(response) != 139 or response[:10] != b"CLGCCS01\x01\x01" or response[130] != 12:
        raise ValueError("invalid pre-admission reply")
    history = [list(part) for part in group]
    if row["fault"].startswith("Foreign"):
        history[FAULTS.index(row["fault"])] = [251] * 16
    if (response[10:74] != bytes(n for part in history for n in part)
            or int.from_bytes(response[74:82], "big") != row["membership_revision"]
            or response[82:114].hex() != ("fb" * 32 if row["fault"] == "Configuration"
                                         else row["configuration_digest"])):
        raise ValueError("wrong advice history or configuration fault")
    term = row["seed_term"] - 1 if row["fault"] == "LowerTerm" else row["seed_term"]
    if row["fault"] not in ("LowerTerm", "ConflictingTerm"):
        term += 1
    actual_leader = int.from_bytes(response[122:130], "big")
    wrong_leader = (actual_leader != leader if leader is not None
                    else actual_leader != 99 if row["fault"] == "NonMember"
                    else actual_leader == row["seed_member"])
    if (int.from_bytes(response[114:122], "big") != term or actual_leader == 0 or wrong_leader):
        raise ValueError("wrong term or member fault")
    reverse = copy.deepcopy(route)
    reverse["source"], reverse["target"] = reverse["target"], reverse["source"]
    if row["fault"] == "WrongProcess":
        reverse["source"]["process"] = [251] * 16
    authenticator = frame.get("response_authenticator")
    correct = frame.get("expected_authenticator")
    request_tag = frame.get("request_authenticator")
    if (not _hex(request_tag, 64) or not _hex(authenticator, 64) or not _hex(correct, 64)
            or not isinstance(reply, dict) or set(reply) != {"route", "sequence", "request", "digest"}
            or reply["route"] != reverse or reply["sequence"] != header["sequence"]
            or reply["request"] != list(bytes.fromhex(request_tag))
            or reply["digest"] != list(hashlib.sha256(response).digest())
            or (authenticator == correct if row["fault"] == "WrongExporter" else authenticator != correct)):
        raise ValueError("wrong response correlation, endpoint or exporter fault")
    # Only the per-attempt remaining timeout may change within this invocation.
    return request[:timeout] + request[timeout + 8:], int.from_bytes(request[timeout:timeout + 8], "big")


def _boot_observations(output):
    prefix = "WIRE_BOOT_AUTHORITY_EVIDENCE "
    rows = [json.loads(line.split(prefix, 1)[1]) for line in output.splitlines() if prefix in line]
    expected = {(caller, mode) for caller in ("Sdk", "Native", "Axum") for mode in (False, True)}
    if (len(rows) != 6 or any(not isinstance(row, dict) or type(row.get("schema")) is not int
            or row["schema"] != 1 or row.get("case") != BOOT.removeprefix("rustfs_")
            or type(row.get("extents")) is not bool for row in rows)
            or {(row.get("ingress"), row["extents"]) for row in rows} != expected):
        raise ValueError("incomplete superseded-boot caller/image coverage")
    for row in rows:
        _common(row)
        old, current = row.get("old_issuer"), row.get("current_issuer")
        previous, boot = row.get("old_boot"), row.get("current_boot")
        if (row["callbacks_before"] != 1 or row["command"]["sequence"] != 2
                or row.get("fault") != "Control" or not _uint(row.get("seed_term"), True)
                or not _uint(row.get("seed_member"), True) or not _uint(row.get("term"), True)
                or row["seed_term"] != row["term"] + 100
                or old != row["issuer"] or not _peer(current)
                or any(old[key] != current[key] for key in ("node", "disk", "label", "server_name"))
                or old["process"] == current["process"] or old["query_address"] == current["query_address"]
                or not _uint(row.get("trust_revision"), True)
                or not _uint(row.get("elapsed_ms")) or row["elapsed_ms"] >= 5000
                or not _uint(row.get("held_prefix_bytes"), True)
                or not row["failure"]["ambiguous"]
                or row["proven_rejections_before"] != row["proven_rejections_after_refusal"]):
            raise ValueError("invalid actual boot cut, deadline or refusal")
        for item in (previous, boot):
            if (not isinstance(item, dict) or not _uint(item.get("epoch"), True)
                    or any(not _hex(item.get(key), size) or item[key] == "0" * size
                           for key, size in (("catalog", 64), ("fleet", 32), ("node", 32), ("disk", 32),
                                             ("engine", 32), ("boot", 32), ("process", 32), ("join_key_sha256", 64)))):
                raise ValueError("invalid physical boot record")
        if (any(previous[key] != boot[key] for key in ("catalog", "fleet", "node", "disk", "engine"))
                or boot["node"] != current["node"] or boot["disk"] != current["disk"]
                or boot["epoch"] <= previous["epoch"]
                or any(previous[key] == boot[key] for key in ("boot", "process", "join_key_sha256"))):
            raise ValueError("physical boot did not advance on retained storage")
        frames = row.get("wire_requests")
        if not isinstance(frames, list) or len(frames) != 1:
            raise ValueError("missing exact held old-boot attempt")
        _frame(row, frames[0], leader=row["seed_member"])
        body = bytes.fromhex(frames[0]["response"])
        if body[122:130] != body[131:139]:
            raise ValueError("old-boot reply has an unrelated status fault")
        encoded = json.dumps(frames[0]["response_header"], ensure_ascii=False, separators=(",", ":")).encode()
        if row["held_prefix_bytes"] != 20 + len(encoded) + 32 + len(bytes.fromhex(frames[0]["response"])) - 1:
            raise ValueError("old reply was not held before its final byte")
        tail = row.get("old_tail_result")
        if not isinstance(tail, str) or not (tail == "tail_written" or tail.startswith("tail_refused: ") and len(tail) > 14):
            raise ValueError("old writer has no observed completion")
        for phase in ("recovery", "replay"):
            before, after = row.get("routing_before_" + phase), row.get("routing_after_" + phase)
            if (any(not isinstance(values, list) or len(values) != 6 or any(not _uint(value) for value in values)
                    for values in (before, after))
                    or [a-b for b,a in zip(before[:5], after[:5])] != [1, 0, 0, 0, 1]):
                raise ValueError("old completion changed the first recovered or replay route")
        if any(not isinstance(row.get(key), str) or not row[key] for key in ("version", "witness", "token")):
            raise ValueError("missing exact S3 replay proof")
    return rows, 6



def _removed_observations(output):
    prefix = "WIRE_REMOVED_AUTHORITY_EVIDENCE "
    rows = [json.loads(line.split(prefix, 1)[1]) for line in output.splitlines() if prefix in line]
    expected = {(caller, mode) for caller in ("Sdk", "Native", "Axum") for mode in (False, True)}
    if (len(rows) != 6 or any(not isinstance(row, dict) or type(row.get("schema")) is not int
            or row["schema"] != 1 or row.get("case") != REMOVED.removeprefix("rustfs_")
            or type(row.get("extents")) is not bool for row in rows)
            or {(row.get("ingress"), row["extents"]) for row in rows} != expected):
        raise ValueError("incomplete removed-member caller/image coverage")
    for row in rows:
        _common(row)
        removed = row.get("removed_member")
        peer = row.get("removed_peer")
        previous, current = row.get("previous_voters"), row.get("current_voters")
        if (not _uint(removed, True) or removed not in (1,2,3)
                or not _peer(peer) or peer["node"] in (row["issuer"]["node"],row["origin"]["node"])
                or previous != [1,2,3] or any(type(member) is not int for member in previous)
                or current != [member for member in (1,2,3,5) if member != removed]
                or any(type(member) is not int for member in current)
                or not _uint(row.get("seed_member"), True) or row["seed_member"] not in current
                or row.get("current_voters_outgoing") != [] or row.get("current_learners") != []
                or row.get("current_learners_next") != [] or row.get("current_auto_leave") is not False
                or not _uint(row.get("previous_membership_revision"),True)
                or row["membership_revision"] <= row["previous_membership_revision"]
                or row.get("removed_replica_in_configuration") is not False
                or type(row.get("removed_replica_member")) is not int or row["removed_replica_member"] != removed
                or row.get("removed_name_resolved") is not True
                or row.get("removed_name_without_grant_refused") is not True
                or row["callbacks_before"] != 1 or row["command"]["sequence"] != 2
                or type(row.get("replacement_leader")) is not int or row["replacement_leader"] != 5
                or type(row.get("replacement_served_by")) is not int or row["replacement_served_by"] != 5
                or row.get("replacement_result_be") != (1).to_bytes(8,"big").hex()
                or not isinstance(row.get("replacement_version"),str) or not row["replacement_version"]
                or row.get("original_version") != row["replacement_version"]
                or not isinstance(row.get("replacement_token"),str) or not row["replacement_token"]
                or row.get("fault") != "RemovedMember" or row["failure"]["ambiguous"]
                or not _uint(row.get("seed_term"),True) or not _uint(row.get("term"),True)
                or row["seed_term"] != row["term"] + 104
                or type(row.get("term_after")) is not int or row["term_after"] != row["term"]
                or type(row.get("membership_revision_after")) is not int
                or row["membership_revision_after"] != row["membership_revision"]
                or not _hex(row.get("directory_before"),64) or row["directory_before"] == "00"*32
                or row.get("directory_after") != row["directory_before"]):
            raise ValueError("invalid actual removal, role, configuration or unchanged range")
        before, after = row["routing_before"], row["routing_after_refusal"]
        if ([after[index]-before[index] for index in (0,2,3,4)] != [1,0,1,1]
                or row["proven_rejections_after_refusal"] != row["proven_rejections_before"]+1):
            raise ValueError("removed advice lost refusal proof or repaired unrelated placement")
        frames = row.get("wire_requests")
        if not isinstance(frames,list) or len(frames)!=1:
            raise ValueError("missing exact current-issuer application attempt")
        _frame(row,frames[0],leader=removed)
        body = bytes.fromhex(frames[0]["response"])
        if body[122:130] != body[131:139]:
            raise ValueError("removed-member reply has an unrelated status fault")
        if (any(not isinstance(row.get(key),str) or not row[key] for key in ("version","witness","token","replay_token"))
                or row.get("token_observation_version") != row["version"]
                or row.get("token_observation_witness") != row["witness"]
                or row.get("replay_token_observation_version") != row["version"]
                or row.get("replay_token_observation_witness") != row["witness"]):
            raise ValueError("missing authenticated exact S3 receipt replay")
    return rows,6

def _observations(output, case):
    if case == REMOVED:
        return _removed_observations(output)
    if case == BOOT:
        return _boot_observations(output)
    if case not in (WIRE, TLS):
        raise ValueError("unknown advice evidence case")
    tls = case == TLS
    prefix = "WIRE_TLS_AUTHORITY_EVIDENCE " if tls else "WIRE_ADVICE_EVIDENCE "
    rows = [json.loads(line.split(prefix, 1)[1]) for line in output.splitlines() if prefix in line]
    expected = {(caller, mode, fault) for caller in ("Sdk", "Native", "Axum")
                for mode in (False, True) for fault in ((None,) if tls else FAULTS)}
    if (len(rows) != len(expected)
            or any(not isinstance(row, dict) or type(row.get("schema")) is not int or row["schema"] != 1
                   or row.get("case") != case.removeprefix("rustfs_") or type(row.get("extents")) is not bool
                   for row in rows)
            or {(row.get("ingress"), row["extents"], None if tls else row.get("fault"))
                for row in rows} != expected):
        raise ValueError("incomplete advice caller/image/fault coverage")
    for row in rows:
        _common(row)
        ordinal = ("Sdk", "Native", "Axum").index(row["ingress"]) * (1 if tls else len(FAULTS))
        if not tls:
            ordinal += FAULTS.index(row["fault"])
        if row["callbacks_before"] != ordinal + 1 or row["command"]["sequence"] != ordinal + 2:
            raise ValueError("reused or missing logical command phase")
        before, after = row["proven_rejections_before"], row["proven_rejections_after_refusal"]
        if tls:
            if (not _hex(row.get("untrusted_certificate_sha256"), 64)
                    or row["untrusted_certificate_sha256"] == "00" * 32
                    or not _uint(row.get("tls_attempts_before"))
                    or not _uint(row.get("tls_attempts_after"))
                    or row["tls_attempts_after"] <= row["tls_attempts_before"]
                    or not isinstance(row.get("certificate_rejections"), list)
                    or not row["certificate_rejections"]
                    or any(not isinstance(error, str) or "UnknownCA" not in error
                           for error in row["certificate_rejections"])
                    or not _uint(row.get("rpc_requests_before"))
                    or type(row.get("rpc_requests_after_refusal")) is not int
                    or row["rpc_requests_after_refusal"] != row["rpc_requests_before"]
                    or not row["failure"]["ambiguous"] or after != before
                    or not _uint(row.get("term"), True)
                    or any(not isinstance(row.get(field), str) or not row[field]
                           for field in ("version", "witness", "version_token"))):
                raise ValueError("invalid untrusted certificate or replay proof")
        else:
            receipt = row.get("receipt")
            if (row.get("term_unchanged") is not True or not _uint(row.get("seed_term"), True)
                    or row["seed_term"] < 2 or not _uint(row.get("seed_member"), True)
                    or not isinstance(row.get("wire_requests"), list) or not 1 <= len(row["wire_requests"]) <= 1024
                    or not isinstance(receipt, dict)
                    or any(not isinstance(receipt.get(field), str) or not receipt[field]
                           for field in ("version", "witness", "token"))):
                raise ValueError("missing advice or receipt observations")
            if row["fault"] in AUTHENTICATED:
                if after <= before:
                    raise ValueError("missing complete authenticated refusal")
                index = 3 if row["fault"] in ("NonMember", "Configuration") else 1
                if row["routing_after_refusal"][index] <= row["routing_before"][index]:
                    raise ValueError("missing bounded membership repair or discovery")
            elif after != before or not row["failure"]["ambiguous"]:
                raise ValueError("unauthenticated reply became non-commit proof")
            requests = [_frame(row, frame) for frame in row["wire_requests"]]
            if any(request[0] != requests[0][0] for request in requests[1:]):
                raise ValueError("forwarding changed captured command inputs")
            if any(after[1] > before[1] for before, after in zip(requests, requests[1:])):
                raise ValueError("forwarding renewed the remaining deadline")
    return rows, 6


def advice_observations(output, case):
    try:
        return _observations(output, case)
    except (KeyError, TypeError, AttributeError, IndexError, OverflowError) as error:
        raise ValueError("malformed advice evidence") from error
