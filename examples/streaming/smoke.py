#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run each example over real HTTP/WebSockets, then check storage after restart."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.request

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
        with tempfile.TemporaryDirectory(prefix=f"cellar-streaming-{backend}-") as temporary:
            root = Path(temporary)
            project = private_project(EXAMPLE / f"project-{BACKENDS[backend]}.json", root)
            environment = {}

            if backend.startswith("node-"):
                entry = "app.js" if backend == "node-js" else "dist/app.js"
                environment["CELLAR_NODE_ENTRY"] = str(EXAMPLE / entry)

            for expected in (2, 4):
                host = Host(args.cellar.resolve(), project, root, environment)
                try:
                    origin = host.ready()
                    # On the second boot this value must be recovered from storage.
                    with urllib.request.urlopen(origin + "/rooms/demo/state", timeout=10) as reply:
                        assert json.load(reply)["messages"] == expected - 2

                    subprocess.run(
                        ["node", str(EXAMPLE / "check.mjs"), origin], check=True, timeout=30,
                    )
                except BaseException:
                    print(host.log.read_text(errors="replace")[-12000:], file=sys.stderr)
                    raise
                finally:
                    host.stop()

            print(f"PASS {backend}: network streams, socket frames, restart and shutdown", flush=True)


if __name__ == "__main__":
    main()
