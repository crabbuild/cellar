# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Synthetic refusal tests; these fixtures establish no runtime qualification."""

import copy
import hashlib
import json
import unittest

from routing_advice_evidence import FAULTS, TLS, WIRE, advice_observations


def peer(node, disk, process):
    return {"node": bytes([node] * 16).hex(), "disk": bytes([disk] * 16).hex(),
            "process": bytes([process] * 16).hex(), "label": f"node-{node}",
            "query_address": "127.0.0.1:1234", "server_name": "localhost"}


def endpoint(value, member):
    return {**{field: list(bytes.fromhex(value[field])) for field in ("node", "disk", "process")},
            "label": value["label"], "member": member}


def rows(tls=False):
    result = []
    for mode in (False, True):
        for caller_index, caller in enumerate(("Sdk", "Native", "Axum")):
            for fault_index, fault in enumerate((None,) if tls else FAULTS):
                ordinal = caller_index * (1 if tls else len(FAULTS)) + fault_index
                before = ordinal + 1
                row = {"schema": 1, "case": (TLS if tls else WIRE).removeprefix("rustfs_"),
                       "extents": mode, "ingress": caller, "callbacks_before": before,
                       "callbacks_after_refusal": before, "callbacks_after_replay": before + 1,
                       "sql_stages_before": 10, "sql_stages_after_refusal": 10,
                       "required_durability": "S3", "achieved_durability": "S3",
                       "result_be": (before + 1).to_bytes(8, "big").hex(),
                       "request_timeout_ms": 5000, "retry_until_ms": 100000,
                       "command": {"client": "fa" * 16, "sequence": ordinal + 2},
                       "issuer": peer(4, 14, 24), "origin": peer(7, 17, 27),
                       "membership_revision": 1, "configuration_digest": "09" * 32,
                       "failure": {"ambiguous": True, "failure": "original identity; unavailable"},
                       "proven_rejections_before": 2, "proven_rejections_after_refusal": 2,
                       "routing_before": [10, 5, 1, 1, 10, 5],
                       "routing_after_refusal": [11, 6, 1, 2, 11, 5]}
                if tls:
                    row.update(untrusted_certificate_sha256="aa" * 32, tls_attempts_before=ordinal,
                               tls_attempts_after=ordinal + 1,
                               certificate_rejections=["AlertReceived(UnknownCA)"],
                               rpc_requests_before=ordinal, rpc_requests_after_refusal=ordinal,
                               term=3, version="exact version", witness="exact witness",
                               version_token="authenticated token")
                else:
                    row.update(fault=fault, seed_term=1000, seed_member=1, term_unchanged=True,
                               receipt={"version": "exact version", "witness": "exact witness", "token": "token"})
                    if fault in ("NonMember", "Configuration", "LowerTerm", "ConflictingTerm"):
                        row["proven_rejections_after_refusal"] = 3
                    request = (b"CLGCCQ01" + bytes.fromhex(row["command"]["client"])
                               + row["command"]["sequence"].to_bytes(8, "big")
                               + (1).to_bytes(8, "big") + b"\x07" * 32
                               + (9).to_bytes(2, "big") + b"Increment" + b"\x08" * 32 + b"\x02"
                               + row["retry_until_ms"].to_bytes(8, "big")
                               + (1234).to_bytes(8, "big") + b"\x06" * 32
                               + (4999).to_bytes(8, "big") + (8).to_bytes(4, "big") + (1).to_bytes(8, "big"))
                    group = [[value] * 16 for value in (1, 2, 3, 4)]
                    history = copy.deepcopy(group)
                    if fault.startswith("Foreign"):
                        history[FAULTS.index(fault)] = [251] * 16
                    term = 999 if fault == "LowerTerm" else 1000 if fault == "ConflictingTerm" else 1001
                    response = (b"CLGCCS01\x01\x01" + bytes(n for part in history for n in part)
                                + (1).to_bytes(8, "big")
                                + (b"\xfb" * 32 if fault == "Configuration" else b"\x09" * 32)
                                + term.to_bytes(8, "big") + (99 if fault == "NonMember" else 2).to_bytes(8, "big")
                                + b"\x0c" + (2).to_bytes(8, "big"))
                    route = {"purpose": "Command", "scope": {"kind": "Shared", "identity": {
                        "group": group, "cell": "Counter:one", "incarnation": [5] * 16}},
                        "schema": [9] * 32, "source": endpoint(row["origin"], None),
                        "target": endpoint(row["issuer"], "1")}
                    reverse = copy.deepcopy(route)
                    reverse["source"], reverse["target"] = reverse["target"], reverse["source"]
                    if fault == "WrongProcess":
                        reverse["source"]["process"] = [251] * 16
                    row["wire_requests"] = [{
                        "fault": fault, "client": row["command"]["client"], "sequence": row["command"]["sequence"],
                        "request": request.hex(), "input_sha256": hashlib.sha256(request).hexdigest(),
                        "route": route, "request_header": {"route": route, "sequence": "1",
                                                         "digest": list(hashlib.sha256(request).digest())},
                        "request_authenticator": "cc" * 32, "response": response.hex(),
                        "response_header": {"route": reverse, "sequence": "1", "request": [204] * 32,
                                            "digest": list(hashlib.sha256(response).digest())},
                        "response_authenticator": ("bb" if fault == "WrongExporter" else "aa") * 32,
                        "expected_authenticator": "aa" * 32,
                    }]
                result.append(row)
    return result


def output(values, tls=False):
    prefix = "WIRE_TLS_AUTHORITY_EVIDENCE " if tls else "WIRE_ADVICE_EVIDENCE "
    return "\n".join(prefix + json.dumps(row) for row in values)


class AdviceEvidenceTests(unittest.TestCase):
    def test_fixed_backend_suite_uses_strict_advice_verifier(self):
        import cell_groups
        self.assertEqual(cell_groups.SUITES["routing-advice"][0],
                         "native_cell_groups_routing_optimization")
        self.assertEqual(cell_groups.SUITES["routing-advice"][1], (WIRE, TLS))
        for tls in (False, True):
            values = rows(tls)
            self.assertEqual(cell_groups.routing_observations(output(values, tls), TLS if tls else WIRE),
                             (values, 6))

    def test_complete_wire_and_tls_contracts(self):
        for tls in (False, True):
            values = rows(tls)
            self.assertEqual(advice_observations(output(values, tls), TLS if tls else WIRE), (values, 6))

    def test_missing_duplicate_or_replaced_configuration_refuses(self):
        for tls in (False, True):
            values = rows(tls)
            for corrupt in (values[:-1], values + [values[0]], values[:-1] + [values[0]]):
                with self.subTest(tls=tls), self.assertRaises(ValueError):
                    advice_observations(output(corrupt, tls), TLS if tls else WIRE)

    def test_schema_identity_admission_and_proof_corruption_refuse(self):
        changes = [("schema", True), ("extents", 0), ("callbacks_after_refusal", 2),
                   ("callbacks_after_replay", 3), ("sql_stages_after_refusal", 11),
                   ("achieved_durability", "Quorum"), ("request_timeout_ms", 6000),
                   ("result_be", "0000000000000003"), ("configuration_digest", "00" * 32),
                   ("command", {"client": "fa" * 16, "sequence": 3}),
                   ("failure", {"ambiguous": False, "failure": "unavailable"})]
        for tls in (False, True):
            for field, value in changes:
                values = rows(tls); values[0][field] = value
                with self.subTest(tls=tls, field=field), self.assertRaises(ValueError):
                    advice_observations(output(values, tls), TLS if tls else WIRE)

    def test_wire_bytes_header_mac_and_capture_corruption_refuse(self):
        for field, value in [("request", "00"), ("response", "00"), ("input_sha256", "00" * 32),
                             ("request_authenticator", "bad"), ("response_header", {}),
                             ("response_authenticator", "bb" * 32), ("request_header", {})]:
            values = rows(); values[0]["wire_requests"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                advice_observations(output(values), WIRE)

    def test_missing_discovery_and_false_exporter_fault_refuse(self):
        values = rows(); target = next(row for row in values if row["fault"] == "WrongExporter")
        target["wire_requests"][0]["response_authenticator"] = "aa" * 32
        with self.assertRaises(ValueError):
            advice_observations(output(values), WIRE)
        for fault, index in (("NonMember", 3), ("LowerTerm", 1)):
            values = rows(); target = next(row for row in values if row["fault"] == fault)
            target["routing_after_refusal"][index] = target["routing_before"][index]
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                advice_observations(output(values), WIRE)

    def test_same_length_advice_corruption_cannot_use_a_fault_label(self):
        for fault, offset in (("ForeignGroup", 42), ("ConflictingTerm", 121), ("NonMember", 129)):
            values = rows()
            target = next(row for row in values if row["fault"] == fault)
            frame = target["wire_requests"][0]
            body = bytearray.fromhex(frame["response"]); body[offset] ^= 1
            frame["response"] = body.hex()
            frame["response_header"]["digest"] = list(hashlib.sha256(body).digest())
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                advice_observations(output(values), WIRE)

    def test_forwarding_preserves_captured_inputs_beside_remaining_timeout(self):
        values = rows()
        first = values[0]["wire_requests"][0]
        second = copy.deepcopy(first)
        body = bytearray.fromhex(second["request"])
        # This byte is in captured randomness, outside the business input and
        # per-attempt timeout. Header/payload digests are independently updated.
        body[-21] ^= 1
        second["request"] = body.hex()
        second["input_sha256"] = hashlib.sha256(body).hexdigest()
        second["request_header"]["digest"] = list(hashlib.sha256(body).digest())
        values[0]["wire_requests"].append(second)
        with self.assertRaises(ValueError):
            advice_observations(output(values), WIRE)

    def test_tls_requires_certificate_alert_without_rpc_entry(self):
        for field, value in [("certificate_rejections", []), ("certificate_rejections", ["UnexpectedEof"]),
                             ("tls_attempts_after", 0), ("rpc_requests_after_refusal", 1),
                             ("proven_rejections_after_refusal", 3), ("version_token", "")]:
            values = rows(True); values[0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                advice_observations(output(values, True), TLS)

    def test_forwarding_cannot_renew_the_remaining_window(self):
        values = rows()
        second = copy.deepcopy(values[0]["wire_requests"][0])
        body = bytearray.fromhex(second["request"])
        body[-20:-12] = (5000).to_bytes(8, "big")
        second["request"] = body.hex()
        second["input_sha256"] = hashlib.sha256(body).hexdigest()
        second["request_header"]["digest"] = list(hashlib.sha256(body).digest())
        values[0]["wire_requests"].append(second)
        with self.assertRaises(ValueError):
            advice_observations(output(values), WIRE)

    def test_unknown_case_or_empty_output_refuses(self):
        for case in (WIRE, TLS, "rustfs_unknown"):
            with self.subTest(case=case), self.assertRaises(ValueError):
                advice_observations("", case)
