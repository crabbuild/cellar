#!/usr/bin/env python3
"""Prepare a private, locked application with changed Python cell code for rolling qualification."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib
from types import SimpleNamespace

import environment


REVISION_HEADER = "x-cellar-code-revision"
REVISION = "rolling-v2"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    project = json.loads(args.project.read_text())
    root = Path(project["cargo_manifest"]).resolve().parent
    if project["package"] != "cellar-python" or project["binary"] != "cellar-python-example":
        raise ValueError("This qualification fixture requires the bundled Python example")
    if not Path(project["cargo_manifest"]).is_absolute():
        raise ValueError("Prepare the provisioned project before generating the rolling variant")
    base = json.loads(Path(project["python_environment"]["inventory"]).read_text())
    output = args.output.resolve()
    if any(output.is_relative_to(Path(directory)) for directory in base["roots"]):
        raise ValueError("Fixture output must be outside inventoried runtime/package roots")
    output.mkdir(parents=True, exist_ok=False)
    example = root / "examples/python"
    source = (example / "app.py").read_text()
    if hashlib.sha256(source.encode()).hexdigest() != base["source"]["sha256"]:
        raise ValueError("Original application differs from the inventoried source")
    # Change actual cell handler code, retaining the data/receipt format. A new
    # response header proves which process's Python cell code executed after
    # handoff; changing only compiled version metadata would not prove that.
    source += f'''

_previous_cell_fetch = Counter.fetch
async def _updated_cell_fetch(self, request, ctx):
    response = await _previous_cell_fetch(self, request, ctx)
    if request.method == "GET":
        response.headers.append(({REVISION_HEADER!r}, {REVISION!r}))
    return response
Counter.fetch = _updated_cell_fetch
'''
    (output / "app.py").write_text(source)
    shutil.copyfile(example / "main.rs", output / "main.rs")
    baseline = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["dependencies"]
    dependencies = []
    for name in ("anyhow", "serde_json"):
        spec = baseline[name]
        if isinstance(spec, str):
            spec = {"version": spec}
        fields = [f"{key} = {json.dumps(value)}" for key, value in spec.items()]
        dependencies.append(name + " = { " + ", ".join(fields) + " }")
    dependencies.extend([
        "cellar = { path = " + json.dumps(str(root / "crates/cellar")) + " }",
        "cellar-python = { path = " + json.dumps(str(root / "crates/cellar-python")) + ', features = ["embed"] }',
    ])
    manifest = output / "Cargo.toml"
    manifest.write_text('''[workspace]
[package]
name = "cellar-python-rolling-fixture"
version = "0.0.0"
edition = "2021"
[[bin]]
name = "cellar-python-rolling-example"
path = "main.rs"
[dependencies]
''' + "\n".join(dependencies) + "\n")
    shutil.copyfile(root / "Cargo.lock", output / "Cargo.lock")
    # Add only the private root package to a copy of the repository lock. Do not
    # resolve from network or permit new dependency versions in this fixture.
    subprocess.run(["cargo", "metadata", "--offline", "--manifest-path", str(manifest),
                    "--format-version", "1"], stdout=subprocess.DEVNULL, check=True)
    packages = lambda lock: {(item["name"], item["version"], item.get("source"))
                             for item in tomllib.loads(lock.read_text())["package"]}
    added = packages(output / "Cargo.lock") - packages(root / "Cargo.lock")
    if added != {("cellar-python-rolling-fixture", "0.0.0", None)}:
        raise ValueError("Rolling fixture changed locked dependencies: " + repr(added))
    inventory = output / "inventory.json"
    archive = output / "environment.tar.gz"
    environment.capture(SimpleNamespace(python=Path(base["executable"]),
        site_packages=[Path(path) for path in base["module_paths"]], source=output / "app.py",
        descriptor=example / "descriptor.json", filename="app.py", target=base["target"], output=inventory))
    with tarfile.open(project["python_environment"]["archive"], "r:gz") as reader:
        metadata = json.load(reader.extractfile("environment.json"))
    environment.bundle(SimpleNamespace(inventory=inventory, prefix=Path(metadata["prefix"]), output=archive))
    project.update(cargo_manifest=str(manifest), package="cellar-python-rolling-fixture",
                   binary="cellar-python-rolling-example", features=[])
    project["primary"]["application_version"] = "v2"
    project["python_environment"] = dict(inventory=str(inventory), archive=str(archive))
    (output / "project.json").write_text(json.dumps(project, indent=2) + "\n")
    print("Prepared changed Python application " + str(output / "project.json"))


if __name__ == "__main__":
    main()
