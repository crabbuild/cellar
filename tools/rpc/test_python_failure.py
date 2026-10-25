# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Fixed RPC categories survive entered handlers; outcome claims never do."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "crates/cellar-python/python"))
import runner


class FailureCategories(unittest.TestCase):
    def test_fixed_categories_ignore_all_exception_attribute_hooks(self):
        class Hostile(Exception):
            def __getattribute__(self, name):
                raise AssertionError("private-attribute-sentinel")

            def __str__(self):
                raise AssertionError("private-text-sentinel")

        for code in runner._rpc_failures:
            error = Hostile()
            BaseException.__dict__["__dict__"].__get__(error)["code"] = code
            self.assertEqual(runner._rpc_failure(error), '{"rpc_error":"' + code + '"}')

    def test_arbitrary_codes_and_non_scalar_categories_are_internal(self):
        class Text(str):
            def __hash__(self):
                raise AssertionError("must not hash application objects")

        for code in ("private-code-sentinel", "x" * (1024 * 1024), Text("MethodNotFound"), {"code":"Internal"}, None):
            error = Exception("private-text-sentinel")
            error.code = code
            error.outcome = "not_committed"
            self.assertEqual(runner._rpc_failure(error), '{"rpc_error":"Internal"}')


if __name__ == "__main__":
    unittest.main()
