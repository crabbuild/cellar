#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real HTTP, Python/PydanticAI, SQLite and pending-alarm process replacement."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from smoke import Host, eventually, private_project, request
from python.smoke import check_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cellar", type=Path, required=True)
    parser.add_argument("--project", type=Path, default=Path(__file__).with_name("project.json"))
    parser.add_argument("--expect-free-threaded", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="cellar-python-agent-") as temporary:
        root = Path(temporary)
        project = private_project(args.project, root)
        env = {"CELLAR_AGENT_MODEL": "demo", "CELLAR_SHUTDOWN_REPORT_JSON": "1"}
        first = Host(args.cellar.resolve(), project, root, extra_env=env)
        try:
            origin = first.ready()
            health = request(origin, "/health")
            assert health["framework"] == "pydantic-ai" and health["model"] == "demo", health
            assert health["free_threaded"] == args.expect_free_threaded, health
            if args.expect_free_threaded:
                assert not health["gil_enabled"], health
            request(origin, "/agents/missing", status=404)
            request(origin, "/agents/invalid", method="POST", data={"prompt": "x", "extra": 1}, status=422)
            request(origin, "/agents/invalid", status=404)
            # Leave a real deferred tool intent and a durable future alarm pending.
            pending = {"prompt": "remember coffee", "step_delay_ms": 10000}
            receipt = request(origin, "/agents/restart", method="POST", data=pending, status=202)
            state = eventually(lambda: request(origin, "/agents/restart"),
                               lambda value: value["phase"] == "tools", timeout=30)
            assert state["notes"] == {} and state["steps"] == 1, state
        finally:
            first.stop()
        check_report(first.log)
        first.log.rename(root / "first.log")
        second = Host(args.cellar.resolve(), project, root, extra_env=env)
        try:
            origin = second.ready()
            completed = eventually(lambda: request(origin, "/agents/restart"),
                                   lambda value: value["phase"] == "complete", timeout=45)
            assert completed["notes"] == {"demo-note": {"text": "remember coffee"}}, completed
            assert completed["answer"] == "Saved note: remember coffee" and completed["steps"] == 3
            assert request(origin, "/agents/restart", method="POST", data=pending, status=202) == receipt
            request(origin, "/agents/restart", method="POST", data={"prompt": "different"}, status=409)
            assert request(origin, "/agents/restart") == completed

            def run(index):
                path = "/agents/parallel-" + str(index)
                prompt = "note " + str(index)
                body = {"prompt": prompt, "step_delay_ms": 100}
                request(origin, path, method="POST", data=body, status=202)
                final = eventually(lambda: request(origin, path), lambda value: value["phase"] == "complete")
                assert final["notes"] == {"demo-note": {"text": prompt}}, final
                assert final["answer"] == "Saved note: " + prompt

            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(run, range(4)))
            request(origin, "/agents/cancel", method="POST", data=pending, status=202)
            assert request(origin, "/agents/cancel", method="DELETE")["phase"] == "cancelled"
            assert request(origin, "/agents/cancel")["notes"] == {}
        finally:
            second.stop()
        check_report(second.log)
    print("PASS PydanticAI: deferred tools, durable restart, receipts, concurrent isolation, cancellation and joined shutdown")


if __name__ == "__main__":
    main()
