"""Opt-in Docker regression: historical source must replace cached Rust code."""

import os
import pathlib
import shutil
import subprocess
import tempfile
import time
import unittest
import uuid


@unittest.skipUnless(os.environ.get("CELLAR_PERF_DOCKER_TEST") == "1",
                     "set CELLAR_PERF_DOCKER_TEST=1 to build Docker fixtures")
class BuildFreshnessTests(unittest.TestCase):
    def test_archived_source_rebuilds_with_a_warm_target_cache(self):
        docker = shutil.which("docker")
        self.assertIsNotNone(docker, "Docker is required for the opted-in test")
        source = pathlib.Path(__file__).resolve().parent
        packages = {
            "crates/cellar": ["cellar-perf-cells", "cellar-perf-load", "cellar-perf-fault-cells"],
            "examples/fleetcare": ["fleetcare-embedded", "fleetcare-daemon", "fleetcare-load", "fleetcare-publish"],
            "crates/cellar-admin": ["cellar-dashboard"],
        }
        cases = {
            "Dockerfile": "cellar-perf-cells",
            "fleetcare.Dockerfile": "fleetcare-embedded",
            "fleetcare-console.Dockerfile": "cellar-dashboard",
        }
        for filename, executable in cases.items():
            with self.subTest(dockerfile=filename), tempfile.TemporaryDirectory(
                    dir=os.environ.get("CARGO_TARGET_DIR")) as directory:
                context = pathlib.Path(directory)
                nonce = uuid.uuid4().hex
                tag = f"cellar-build-freshness:{nonce}"

                def command(*args):
                    result = subprocess.run([docker, *args], capture_output=True, text=True,
                                            timeout=180, check=False)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    return result.stdout.strip()

                members = ",".join(f'"{p}"' for p in packages)
                (context / "Cargo.toml").write_text(f"[workspace]\nmembers=[{members}]\nresolver='2'\n")
                lock = "version = 4\n"
                for package, bins in packages.items():
                    path = context / package
                    (path / "src").mkdir(parents=True)
                    name = path.name
                    manifest = f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\nautobins=false\n'
                    for binary in bins:
                        manifest += f'[[bin]]\nname="{binary}"\npath="src/main.rs"\n'
                    (path / "Cargo.toml").write_text(manifest)
                    lock += f'[[package]]\nname="{name}"\nversion="0.1.0"\n'
                (context / "Cargo.lock").write_text(lock)
                for path in ["tools/perf", "tools/xtask"]:
                    (context / path).mkdir(parents=True)
                    (context / path / ".keep").touch()
                # Use BuildKit's built-in frontend; resolving the optional
                # syntax image would add an unrelated registry dependency.
                recipe = (source / filename).read_text().splitlines(keepends=True)
                if recipe[0].startswith("# syntax="):
                    recipe = recipe[1:]
                (context / "Dockerfile").write_text("".join(recipe))

                def build(marker, modified):
                    for package in packages:
                        main = context / package / "src/main.rs"
                        main.write_text(f'fn main() {{ println!("{marker}"); }}\n')
                        os.utime(main, (modified, modified))
                    command("build", "--target", "build", "--build-arg",
                            f"TARGET_CACHE_ID=cellar-freshness-{nonce}", "-t", tag, str(context))
                    return command("run", "--rm", "--entrypoint", f"/out/{executable}", tag)

                try:
                    self.assertEqual(build("current", time.time() - 10), "current")
                    # git archive preserves commit times. A content change
                    # must rebuild even when that timestamp predates target/.
                    self.assertEqual(build("historical", 1_600_000_000), "historical")
                finally:
                    subprocess.run([docker, "image", "rm", tag], capture_output=True,
                                   timeout=60, check=False)


if __name__ == "__main__":
    unittest.main()
