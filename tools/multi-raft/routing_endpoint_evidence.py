# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Complete endpoint read and held-reply ledgers; runtime checks own proof verification."""

import json
import re


READ = "rustfs_read_first_refreshes_endpoint"
INFLIGHT = "rustfs_unrelated_endpoint_keeps_inflight_reply"


def _uint(value, positive=False):
    return type(value) is int and int(positive) <= value < 2**64


def _text(row, fields):
    return all(isinstance(row.get(field), str) and row[field] for field in fields)


def _hex(row, fields, size):
    return all(isinstance(row.get(field), str)
               and re.fullmatch(rf"[0-9a-f]{{{size}}}", row[field]) for field in fields)


def endpoint_observations(output, case):
    if case not in (READ, INFLIGHT):
        raise ValueError("unknown endpoint evidence case")
    reading = case == READ
    prefix = "ENDPOINT_READ_EVIDENCE " if reading else "ENDPOINT_INFLIGHT_EVIDENCE "
    rows = [json.loads(line.split(prefix, 1)[1])
            for line in output.splitlines() if prefix in line]
    callers = ("Sdk", "Native", "Axum")
    consistencies = ("Strong", "AtLeast") if reading else (None,)
    expected = {(caller, mode, consistency) for caller in callers
                for mode in (False, True) for consistency in consistencies}
    for row in rows:
        if (not isinstance(row, dict) or type(row.get("schema")) is not int
                or row["schema"] != 1 or row.get("event") != "verified"
                or row.get("case") != case.removeprefix("rustfs_")
                or row.get("caller") not in callers or type(row.get("extents")) is not bool
                or (reading and row.get("consistency") not in consistencies)):
            raise ValueError("invalid endpoint evidence schema or coverage")
    observed = {(row["caller"], row["extents"], row.get("consistency") if reading else None)
                for row in rows}
    if len(rows) != len(expected) or observed != expected:
        raise ValueError("incomplete endpoint caller/image/consistency evidence")
    for row in rows:
        routing = (row.get("routing_before"), row.get("routing_after"))
        if (not _hex(row, ("old_process", "current_process"), 32)
                or row["old_process"] == row["current_process"]
                or not _hex(row, ("directory_before", "directory_after"), 64)
                or row["directory_before"] != row["directory_after"]
                or not _text(row, ("membership_revision", "version", "witness"))
                or any(not isinstance(values, list) or len(values) != 7
                       or any(not _uint(value) for value in values) for values in routing)):
            raise ValueError("invalid endpoint identity or ownership evidence")
        delta = [after - before for before, after in zip(*routing)]
        if reading:
            before, after = row.get("queries_before"), row.get("queries_after")
            barriers = row.get("barriers_before"), row.get("barriers_after")
            reply = row.get("reply")
            if (delta != [0, 0, 0, 0, 1, 1, 0]
                    or not _hex(row, ("node", "disk", "invocation"), 32)
                    or not _uint(before) or not _uint(after) or after != before + 1
                    or type(row.get("commands")) is not int or row["commands"] != 1
                    or type(row.get("timeout_ms")) is not int or row["timeout_ms"] != 5000
                    or type(row.get("maximum_attempts")) is not int or row["maximum_attempts"] != 4
                    or type(row.get("selected_member")) is not int or row["selected_member"] != 1
                    or row.get("leader_fallback") is not False
                    or not all(_uint(value) for value in barriers)
                    or (barriers[1] <= barriers[0] if row["consistency"] == "Strong"
                        else barriers[1] != barriers[0])
                    or not _text(row, ("minimum",)) or not isinstance(reply, dict)
                    or not _text(reply, ("cell", "version_token"))
                    or type(reply.get("value")) is not int or reply["value"] != 1
                    or type(reply.get("served_by")) is not int or reply["served_by"] != 1):
                raise ValueError("invalid read endpoint/barrier evidence")
        else:
            before, after = row.get("callbacks_before"), row.get("callbacks_after")
            if (delta[0] != 1 or not 1 <= delta[1] <= 4 or delta[2:] != [0, 0, 0, 0, 0]
                    or not _uint(before, True) or before >= 2**63 - 1
                    or not _uint(after) or after != before + 1
                    or row.get("required") != "S3" or row.get("achieved") != "S3"
                    or row.get("leader_unchanged") is not True
                    or not _uint(row.get("retry_until_ms"), True)
                    or not _text(row, ("command",))
                    or row.get("result_be") != (before + 1).to_bytes(8, "big").hex()):
                raise ValueError("invalid held endpoint reply/receipt evidence")
    return rows, len(expected)
