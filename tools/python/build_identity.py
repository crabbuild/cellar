# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Verify the embedded Python sources in each frozen benchmark executable."""
import hashlib
from pathlib import Path


SOURCES = ("crates/cellar-python/python/cellar.py", "crates/cellar-python/python/runner.py")


def inspect_binary(executable, checkout, *, embedded):
    blob = Path(executable).read_bytes()
    sources = {}
    for name in SOURCES:
        source = (Path(checkout) / name).read_bytes()
        if not source:
            raise RuntimeError(f"empty embedded source: {name}")
        offset = blob.find(source)
        if (offset >= 0) != embedded:
            raise RuntimeError(f"benchmark executable has incorrect embedded source: {name}")
        sources[name] = {"sha256": hashlib.sha256(source).hexdigest(),
                         "offset": offset if embedded else None}
    return {"sha256": hashlib.sha256(blob).hexdigest(), "embedded": embedded,
            "embedded_sources": sources}


def validate_comparison(executables):
    if "baseline" not in executables:
        return
    selected, baseline = executables["selected"], executables["baseline"]
    changed = any(selected["embedded_sources"][name]["sha256"] !=
                  baseline["embedded_sources"][name]["sha256"] for name in SOURCES)
    if changed and selected["sha256"] == baseline["sha256"]:
        raise RuntimeError("different Python revisions resolved to the same executable")
