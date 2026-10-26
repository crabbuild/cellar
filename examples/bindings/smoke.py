#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Start binding examples in private state and verify real HTTP plus restart."""

import argparse
import json
from pathlib import Path
import sys
import tempfile

import check as http_client
import curl_demo


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
    parser.add_argument("--curl-demo", action="store_true",
                        help="print a sequential curl walkthrough instead of concurrent checks")
    args = parser.parse_args()
    client = curl_demo if args.curl_demo else http_client

    for backend in args.backend:
        print(f"Starting {backend}: private state and automatic port", flush=True)
        with tempfile.TemporaryDirectory(prefix=f"cellar-bindings-{backend}-") as temporary:
            root = Path(temporary)
            project = private_project(EXAMPLE / f"project-{BACKENDS[backend]}.json", root)
            environment = {}
            if backend.startswith("node-"):
                entry = "app.js" if backend == "node-js" else "dist/app.js"
                environment["CELLAR_NODE_ENTRY"] = str(EXAMPLE / entry)

            expected = None
            for boot in range(1 if args.curl_demo else 2):
                host = Host(args.cellar.resolve(), project, root, environment)
                try:
                    origin = host.ready()
                    if expected is None:
                        for binding in ("COUNTERS", "COUNTER_ALIAS", "OTHER_COUNTERS"):
                            assert client.read(origin, binding, "demo")["count"] == 0
                    else:
                        # Both values and identities must survive reconstruction.
                        for (binding, name), snapshot in expected.items():
                            assert client.read(origin, binding, name) == snapshot
                    expected = client.check(origin, "demo")
                except BaseException:
                    print(host.log.read_text(errors="replace")[-12000:], file=sys.stderr)
                    raise
                finally:
                    host.stop()
                print(f"PASS {backend}: boot {boot + 1} and graceful shutdown", flush=True)

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
                rebound_expected = {
                    **expected,
                    ("COUNTER_ALIAS", "demo"): expected[("OTHER_COUNTERS", "demo")],
                }
                for (binding, name), snapshot in rebound_expected.items():
                    assert client.read(origin, binding, name) == snapshot
                _, updated = client.request(origin, "/bindings/COUNTER_ALIAS/demo", "POST")
                other = expected[("OTHER_COUNTERS", "demo")]
                assert updated == {**other, "count": other["count"] + 1}
                rebound_expected[("COUNTER_ALIAS", "demo")] = updated
                rebound_expected[("OTHER_COUNTERS", "demo")] = updated
                for (binding, name), snapshot in rebound_expected.items():
                    assert client.read(origin, binding, name) == snapshot
                expected[("OTHER_COUNTERS", "demo")] = updated
            except BaseException:
                print(rebound.log.read_text(errors="replace")[-12000:], file=sys.stderr)
                raise
            finally:
                rebound.stop()
            print(f"PASS {backend}: rebound alias reads and writes existing target state", flush=True)

            # Restore the alias without moving data. The old cell and the newly
            # incremented target must both retain their values and identities.
            for binding in config["primary"]["bindings"]:
                if binding["name"] == "COUNTER_ALIAS":
                    binding["class_name"] = "Counter"
            project.write_text(json.dumps(config))
            restored = Host(args.cellar.resolve(), project, root, environment)
            try:
                origin = restored.ready()
                for (binding, name), snapshot in expected.items():
                    assert client.read(origin, binding, name) == snapshot
            except BaseException:
                print(restored.log.read_text(errors="replace")[-12000:], file=sys.stderr)
                raise
            finally:
                restored.stop()
            print(f"PASS {backend}: restored alias, persisted state and graceful shutdown", flush=True)


if __name__ == "__main__":
    main()
