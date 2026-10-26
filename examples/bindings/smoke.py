#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Start binding examples in private state and verify real HTTP plus restart."""

import argparse
import json
from pathlib import Path
import sys
import tempfile

from check import check, read


# Reuse the repository's process lifecycle and private-project harness.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from smoke import Host, private_project


EXAMPLE = Path(__file__).resolve().parent
BACKENDS = {
    "rust": "rust",
    "node-js": "node",
    "node-ts": "node",
    "v8": "javascript",
    "python": "python",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar", type=Path, required=True)
    parser.add_argument("--backend", choices=BACKENDS, action="append", required=True)
    args = parser.parse_args()

    for backend in args.backend:
        with tempfile.TemporaryDirectory(prefix=f"cellar-bindings-{backend}-") as temporary:
            root = Path(temporary)
            project = private_project(EXAMPLE / f"project-{BACKENDS[backend]}.json", root)
            environment = {}
            if backend.startswith("node-"):
                entry = "app.js" if backend == "node-js" else "dist/app.js"
                environment["CELLAR_NODE_ENTRY"] = str(EXAMPLE / entry)

            expected = None
            for boot in range(2):
                host = Host(args.cellar.resolve(), project, root, environment)
                try:
                    origin = host.ready()
                    if expected is None:
                        for binding in ("COUNTERS", "COUNTER_ALIAS", "OTHER_COUNTERS"):
                            assert read(origin, binding, "demo")["count"] == 0
                    else:
                        # Both values and identities must survive reconstruction.
                        for (binding, name), snapshot in expected.items():
                            assert read(origin, binding, name) == snapshot
                    expected = check(origin, "demo")
                except BaseException:
                    print(host.log.read_text(errors="replace")[-12000:], file=sys.stderr)
                    raise
                finally:
                    host.stop()
                print(f"PASS {backend}: boot {boot + 1} and graceful shutdown", flush=True)

            print(f"PASS {backend}: binding state and identities survive restart", flush=True)

            # Repoint only the alias in the same project. The application code
            # still uses COUNTER_ALIAS, but configuration now selects OtherCounter.
            config = json.loads(project.read_text())
            for binding in config["primary"]["bindings"]:
                if binding["name"] == "COUNTER_ALIAS":
                    binding["class_name"] = "OtherCounter"
            project.write_text(json.dumps(config))
            rebound = Host(args.cellar.resolve(), project, root, environment)
            try:
                origin = rebound.ready()
                assert read(origin, "COUNTERS", "demo") == expected[("COUNTERS", "demo")]
                assert read(origin, "COUNTER_ALIAS", "demo") == expected[("OTHER_COUNTERS", "demo")]
                assert read(origin, "OTHER_COUNTERS", "demo") == expected[("OTHER_COUNTERS", "demo")]
            except BaseException:
                print(rebound.log.read_text(errors="replace")[-12000:], file=sys.stderr)
                raise
            finally:
                rebound.stop()
            print(f"PASS {backend}: configuration rebind selects existing target state", flush=True)


if __name__ == "__main__":
    main()
