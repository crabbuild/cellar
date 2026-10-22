"""HTTP metadata keeps the SDK's JSON representation and rejection rules."""
from http import HTTPStatus
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
from cellar import Response, ValidationError, _encode, _encode_response


class EncodingTests(unittest.TestCase):
    def test_exact_escaping_and_order(self):
        value = dict(body="", headers=[['x-escape', '"\\\n\x00é☃😀']], status=201)
        self.assertEqual(
            _encode_response(value),
            r'{"body":"","headers":[["x-escape","\"\\\n\u0000\u00e9\u2603\ud83d\ude00"]],"status":201}',
        )
        self.assertEqual(_encode_response(value), _encode(value))

    def test_ordinary_and_unusual_metadata(self):
        class Text(str):
            pass

        class Rows(list):
            pass

        class Metadata(dict):
            pass

        values = [Response()._completion()[0], None, Metadata(status=200)]
        for status in (-2**63, 0, 200, 599, 2**64 - 1, True, 200.0, HTTPStatus.OK):
            values.append(dict(status=status, headers=[], body=""))
        for headers in ([], (), [("x", "a")] * 128, [("x", "a")] * 129,
                        [["x", "a"]], Rows([("x", "a")]), [(Text("x"), Text("a"))],
                        None, ["bad-row"], [["x", 1]], [["x", "a", "b"]]):
            values.append(dict(status=200, headers=headers, body=""))
        values.extend((dict(status=200, headers=[], body="nonempty"),
                       dict(status=200, headers=[], body="", extra=[1, None]),
                       dict(status=200, headers=[]),
                       {Text("status"): 200, "headers": [], "body": ""}))
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(_encode_response(value), _encode(value))

    def test_invalid_values_keep_sdk_rejection(self):
        deep = None
        for _ in range(65):
            deep = [deep]
        for value in (dict(status=2**64, headers=[], body=""),
                      dict(status=-2**63 - 1, headers=[], body=""),
                      dict(status=float("nan"), headers=[], body=""),
                      dict(status=200, headers=[("x", float("inf"))], body=""),
                      dict(status=200, headers=[], body="", extra=deep),
                      dict(status=200, headers=[], body="", extra={1: "bad key"}),
                      dict(status=200, headers=[], body=object())):
            for encode in (_encode, _encode_response):
                with self.subTest(encoder=encode.__name__, value=type(value)):
                    with self.assertRaisesRegex(ValidationError, "Unsupported Cellar value"):
                        encode(value)

    def test_nonstring_key_does_not_run_equality(self):
        class Key:
            def __hash__(self):
                return hash("status")

            def __eq__(self, other):
                raise AssertionError("invalid object keys must not run equality")

        with self.assertRaises(ValidationError):
            _encode_response({Key(): 200, "headers": [], "body": ""})

    def test_container_subclasses_do_not_run_metaclass_equality(self):
        class Meta(type):
            def __eq__(self, other):
                raise AssertionError("container type checks must not run equality")

        class Rows(list, metaclass=Meta):
            pass

        class Pair(tuple, metaclass=Meta):
            pass

        for headers in (Rows([("x", "a")]), [Pair(("x", "a"))]):
            value = dict(status=200, headers=headers, body="")
            with self.subTest(headers=type(headers)):
                self.assertEqual(_encode_response(value), _encode(value))

    def test_mutating_original_headers_cannot_change_checked_snapshot(self):
        row = ["x", "before"]
        headers = [row]
        value = dict(status=200, headers=headers, body="")
        dumps = json.dumps

        def mutate_then_encode(snapshot, **options):
            row[1] = {"invalid": object()}
            headers.append(object())
            value["status"] = 2**64
            return dumps(snapshot, **options)

        with patch("cellar._json.dumps", side_effect=mutate_then_encode):
            self.assertEqual(_encode_response(value),
                             '{"status":200,"headers":[["x","before"]],"body":""}')


if __name__ == "__main__":
    unittest.main()
