# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Exercise the actual embedded-profile loader under an ASCII process locale."""
import os
from pathlib import Path
import subprocess
import sys
import unittest

import google.protobuf

ROOT = Path(__file__).resolve().parent


class ProfileVectorsTest(unittest.TestCase):
    def test_profile_bootstrap_reads_unicode_corpus_in_ascii_locale(self):
        environment = dict(os.environ)
        environment.update(LC_ALL="C", PYTHONUTF8="0", PYTHONCOERCECLOCALE="0")
        environment["PYTHONPATH"] = os.pathsep.join((
            str(ROOT / "profile"), str(ROOT / "fixtures/profile-generated/python"),
            str(Path(google.protobuf.__file__).resolve().parents[2]),
        ))
        result = subprocess.run([sys.executable, "-c", """
import codecs, locale, sys
assert not sys.flags.utf8_mode
assert codecs.lookup(locale.getencoding()).name == 'ascii'
from live_profile_vectors import VECTORS
assert len(VECTORS) == 18
assert VECTORS['scalar-boundaries']['json']['text'] == '\\ufeff\\U0001f600\\x00'
"""], env=environment, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
