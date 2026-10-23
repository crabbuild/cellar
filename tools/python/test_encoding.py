"""SDK values and HTTP metadata preserve JSON representation and rejection rules."""
from http import HTTPStatus
from concurrent.futures import ThreadPoolExecutor
import gc
import json
from pathlib import Path
import sys
import threading
import unittest
import weakref
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
from cellar import Response, ValidationError, _encode, _encode_response, _response_metadata


class EncodingTests(unittest.TestCase):
    def test_direct_metadata_owns_immutable_containers(self):
        row = ["x", "before"]
        headers = [row]
        value = dict(status=200, headers=headers, body="")
        snapshot = _response_metadata(value)
        row[1] = object()
        headers.append(object())
        value["status"] = 2**64
        self.assertEqual(snapshot, dict(status=200, headers=(("x", "before"),), body=""))
        self.assertIs(type(snapshot["headers"]), tuple)
        self.assertIs(type(snapshot["headers"][0]), tuple)

    def test_scalar_leaves_obey_the_exact_nesting_limit(self):
        for scalar in (None, True, False, 0, -2**63, 2**64 - 1, 1.25, "text"):
            value = scalar
            for _ in range(64):
                value = [value]
            with self.subTest(scalar=scalar):
                self.assertEqual(_encode(value), json.dumps(value, separators=(",", ":")))
                with self.assertRaises(ValidationError) as caught:
                    _encode([value])
                self.assertEqual(caught.exception.outcome, "not_committed")

    def test_scalar_subclasses_keep_values_and_integer_bounds(self):
        class Meta(type):
            def __eq__(self, other):
                raise AssertionError("scalar type checks must not run metaclass equality")

        class Number(int, metaclass=Meta):
            pass

        class Text(str, metaclass=Meta):
            pass

        class Fraction(float, metaclass=Meta):
            pass

        value = {Text("key"): [Number(-2**63), Number(2**64 - 1), Fraction(1.25), Text("é")]}
        self.assertEqual(_encode(value), json.dumps(value, separators=(",", ":")))
        for invalid in (Number(-2**63 - 1), Number(2**64), Fraction(float("nan")),
                        Fraction(float("inf"))):
            with self.subTest(value=invalid):
                with self.assertRaises(ValidationError):
                    _encode({"nested": [invalid]})

    def test_integer_like_containers_still_validate_their_members(self):
        class IntegerLike:
            @property
            def __class__(self):
                return int

            def __ge__(self, other):
                return True

            def __lt__(self, other):
                return True

        class Mapping(IntegerLike, dict):
            pass

        class Sequence(IntegerLike, list):
            pass

        for value in (Mapping({1: "invalid key"}), Mapping(key=2**64), Sequence([2**64])):
            with self.subTest(container=type(value).__name__):
                with self.assertRaises(ValidationError):
                    _encode(value)

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

        def mutate_then_encode(snapshot):
            row[1] = {"invalid": object()}
            headers.append(object())
            value["status"] = 2**64
            return dumps(snapshot, allow_nan=False, separators=(",", ":"))

        with patch("cellar._value_encoder.encode", side_effect=mutate_then_encode) as encode:
            self.assertEqual(_encode_response(value),
                             '{"status":200,"headers":[["x","before"]],"body":""}')
            encode.assert_called_once()

    def test_shared_encoder_keeps_concurrent_calls_independent(self):
        shared = {"nested": [None, True, "é😀", {"number": 1.25}]}
        barrier = threading.Barrier(4)

        def worker(index):
            value = {"worker": index, "left": shared, "right": shared}
            expected = json.dumps(value, allow_nan=False, separators=(",", ":"))
            response = dict(status=200 + index, headers=[("x-worker", str(index))], body="")
            encoded_response = json.dumps(response, separators=(",", ":"))
            barrier.wait(timeout=5)
            for _ in range(1000):
                self.assertEqual(_encode(value), expected)
                self.assertEqual(_encode_response(response), encoded_response)
                with self.assertRaises(ValidationError):
                    _encode({"invalid": float("nan")})
            return index

        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(list(pool.map(worker, range(4))), list(range(4)))

    def test_shared_encoder_does_not_retain_application_values(self):
        class Payload(list):
            pass

        value = Payload([{"private": "data"}])
        reference = weakref.ref(value)
        self.assertEqual(_encode(value), '[{"private":"data"}]')
        del value
        gc.collect()
        self.assertIsNone(reference())


if __name__ == "__main__":
    unittest.main()
