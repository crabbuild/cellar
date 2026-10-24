"""Private comparison: keep each checkout's target and require frozen artifacts."""
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "examples/python-fastapi"))
import offered_perf

scratch = Path(os.environ["RUNNER_TEMP"])
current_target = os.environ["CARGO_TARGET_DIR"]
selections = {}
for label, folder, target in (("before", "fastapi-before", scratch / "fastapi-before-target"),
                               ("direct", "fastapi", Path(current_target))):
    directory = scratch / folder
    project = (directory / "environment/project.json").resolve(strict=True)
    package = json.loads((directory / "package/package.json").read_text())
    config = json.loads(project.read_text())
    inventory_path = Path(config["python_environment"]["inventory"])
    inventory = json.loads(inventory_path.read_text())
    sha = package["primary"]["native"]["artifacts"][0]["sha256"]
    binary = directory / "package" / sha
    if hashlib.sha256(binary.read_bytes()).hexdigest() != sha:
        raise RuntimeError("frozen package executable digest mismatch")
    selections[str(project)] = dict(label=label, target=str(target), executable_sha256=sha,
                                    inventory_sha256=hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
                                    source_sha256=inventory["source"]["sha256"], profile=config["profile"],
                                    soabi=inventory["soabi"])
(scratch / "fastapi-comparison-builds.json").write_text(json.dumps(selections, indent=2) + "\n")
original = offered_perf.start_host


def frozen_host(args, directory, **kwargs):
    selected = selections[str(args.project.resolve())]
    previous = os.environ.get("CARGO_TARGET_DIR")
    os.environ["CARGO_TARGET_DIR"] = selected["target"]
    try:
        host, origin, pid = original(args, directory, **kwargs)
    finally:
        if previous is None:
            os.environ.pop("CARGO_TARGET_DIR", None)
        else:
            os.environ["CARGO_TARGET_DIR"] = previous
    try:
        observed = offered_perf.digest(offered_perf.executable_path(pid))
        if observed != selected["executable_sha256"]:
            raise RuntimeError("dev host differs from frozen package executable")
        with (scratch / "fastapi-comparison-starts.jsonl").open("a") as stream:
            stream.write(json.dumps(dict(**selected, pid=pid, directory=str(directory))) + "\n")
        return host, origin, pid
    except BaseException:
        offered_perf.stop_host(host)
        raise


offered_perf.start_host = frozen_host
for project, selected in selections.items():
    sys.argv.extend(["--project", selected["label"] + "=" + project])
offered_perf.main()
