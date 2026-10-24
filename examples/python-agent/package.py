#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Build the example with its verified Python runtime and locked dependencies."""
from pathlib import Path
import sys

EXAMPLE = Path(__file__).resolve().parent
sys.path.insert(0, str(EXAMPLE.parents[1] / "tools/python"))
from package_example import package

if __name__ == "__main__":
    package(EXAMPLE, "cellar-python-agent")
