"""Exercise the native graph guard against Cargo's actual feature resolution."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

GUARD = Path(__file__).with_name("check_native_dependencies.py").resolve()


class NativeDependencies(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="cellar-native-graph-")
        self.root = Path(self.directory.name)
        (self.root / "Cargo.toml").write_text(
            '[workspace]\nresolver="2"\nmembers=["crates/*", "tools/rpc/host"]\nexclude=["support/v8"]\n')
        self.package("support/v8", "v8")
        self.package("crates/cellar", "cellar")
        self.package("crates/cellar-python", "cellar-python")
        self.package("crates/native-app", "native-app")
        self.package("crates/cellar-javascript", "cellar-javascript", '''
[features]
default=[]
embed=["dep:v8"]
[dependencies]
v8={path="../../support/v8", optional=true}
''')
        self.package("tools/rpc/host", "cellar-rpc-qualification", '''
[features]
default=[]
full=["dep:cellar-javascript", "dep:cellar-python"]
[dependencies]
cellar-javascript={path="../../../crates/cellar-javascript", optional=true, features=["embed"]}
cellar-python={path="../../../crates/cellar-python", optional=true}
''')

    def tearDown(self):
        self.directory.cleanup()

    def package(self, location, name, extra=""):
        directory = self.root / location
        (directory / "src").mkdir(parents=True)
        (directory / "src/lib.rs").write_text("")
        (directory / "Cargo.toml").write_text(
            f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n{extra}')

    def check(self):
        subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=self.root,
                       check=True, capture_output=True, text=True)
        return subprocess.run([sys.executable, str(GUARD)], cwd=self.root,
                              capture_output=True, text=True)

    def test_explicit_runtime_roots_do_not_change_the_native_graph(self):
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no JavaScript engine", result.stdout)

    def test_native_application_cannot_pull_engine_through_excluded_runtime_root(self):
        manifest = self.root / "crates/native-app/Cargo.toml"
        with manifest.open("a") as output:
            output.write('\n[dependencies]\ncellar-rpc-qualification={path="../../tools/rpc/host", features=["full"]}\n')
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("JavaScript engine", result.stderr)
        self.assertIn("v8", result.stderr)

    def test_qualification_runtimes_must_be_explicitly_selected(self):
        manifest = self.root / "tools/rpc/host/Cargo.toml"
        manifest.write_text(manifest.read_text().replace("default=[]", 'default=["full"]'))
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not enable runtimes by default", result.stderr)


if __name__ == "__main__":
    unittest.main()
