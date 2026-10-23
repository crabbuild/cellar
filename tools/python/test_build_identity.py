"""A copied candidate must never masquerade as the reference Python build."""
from pathlib import Path
import tempfile
import unittest

from build_identity import SOURCES, inspect_binary, validate_comparison


class BuildIdentityTests(unittest.TestCase):
    def test_distinct_python_revisions_cannot_share_one_executable(self):
        selected = {"sha256": "same-binary", "embedded_sources": {name: {"sha256": name} for name in SOURCES}}
        baseline = {"sha256": "same-binary", "embedded_sources": {name: {"sha256": name + "-old"} for name in SOURCES}}
        with self.assertRaisesRegex(RuntimeError, "same executable"):
            validate_comparison(dict(selected=selected, baseline=baseline))
        baseline["sha256"] = "different-binary"
        validate_comparison(dict(selected=selected, baseline=baseline))
        validate_comparison(dict(selected=selected))

    def test_frozen_build_must_contain_its_own_source_revision(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            selected, reference = root / "selected", root / "reference"
            for checkout, runner in ((selected, b"selected runner\n"),
                                     (reference, b"reference runner\n")):
                for name, content in zip(SOURCES, (b"shared facade\n", runner)):
                    path = checkout / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(content)
            executable = root / "frozen"
            executable.write_bytes(b"binary prefix\0shared facade\n\0selected runner\n\0")
            result = inspect_binary(executable, selected, embedded=True)
            self.assertTrue(result["embedded"])
            self.assertTrue(all(value["offset"] >= 0 for value in result["embedded_sources"].values()))
            with self.assertRaisesRegex(RuntimeError, "incorrect embedded source"):
                inspect_binary(executable, reference, embedded=True)
            with self.assertRaisesRegex(RuntimeError, "incorrect embedded source"):
                inspect_binary(executable, selected, embedded=False)

    def test_native_build_contains_neither_python_module(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in SOURCES:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("source for " + name)
            executable = root / "native"
            executable.write_bytes(b"native executable with no embedded Python sources")
            result = inspect_binary(executable, root, embedded=False)
            self.assertFalse(result["embedded"])
            self.assertTrue(all(value["offset"] is None for value in result["embedded_sources"].values()))
            with self.assertRaisesRegex(RuntimeError, "incorrect embedded source"):
                inspect_binary(executable, root, embedded=True)


if __name__ == "__main__":
    unittest.main()
