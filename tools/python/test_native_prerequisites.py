"""Real native loader capture/refusal tests; uses only the platform C toolchain."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import native_prerequisites as native


class NativePrerequisites(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory).resolve()
        environment, system = root / "environment", root / "system"
        binary = environment / "bin/host"
        binary.parent.mkdir(parents=True)
        system.mkdir()
        sentinel = root / "constructor-executed"
        implementation = root / "provider.c"
        implementation.write_text('#include <stdio.h>\n__attribute__((constructor)) static void init(void) {'
                                  'FILE *f = fopen(' + json.dumps(str(sentinel)) + ', "w"); if(f) fclose(f);}\n'
                                  'int prerequisite_value(void) { return 7; }\n')
        main = root / "host.c"
        main.write_text('extern int prerequisite_value(void); int main(void) { return prerequisite_value() != 7; }\n')
        if sys.platform == "darwin":
            library = system / "libprovider.dylib"
            options = ["-dynamiclib", "-Wl,-install_name,@rpath/libprovider.dylib"]
            rpath = "@loader_path/../../system"
            target = ("aarch64" if os.uname().machine == "arm64" else "x86_64") + "-apple-darwin"
        else:
            library = system / "libprovider.so"
            options = ["-shared", "-Wl,-soname,libprovider.so"]
            rpath = "$ORIGIN/../../system"
            target = os.uname().machine + "-unknown-linux-gnu"
        subprocess.run(["cc", *options, "-fPIC", str(implementation), "-o", str(library)], check=True)
        subprocess.run(["cc", str(main), "-L" + str(system), "-lprovider", "-Wl,-rpath," + rpath, "-o", str(binary)], check=True)
        inventory = dict(target=target, home=str(environment), executable=str(binary),
                         roots=[str(environment)], files={str(binary): dict(kind="file")})
        return inventory, binary, library, sentinel

    def test_capture_does_not_run_initializers_and_hashes_transitive_providers(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory, binary, library, sentinel = self.fixture(directory)
            inventory["prerequisites"] = native.capture(inventory, binary)
            self.assertFalse(sentinel.exists(), "native capture executed a constructor")
            recorded = inventory["prerequisites"]["libraries"][str(library)]
            self.assertEqual(recorded["kind"], "file")
            self.assertEqual(recorded["sha256"], native.digest(library))
            self.assertIn(str(library), inventory["prerequisites"]["dependencies"])
            native.verify(inventory)
            content = library.read_bytes()
            try:
                library.write_bytes(content[:-1] + bytes([content[-1] ^ 1]))
                with self.assertRaisesRegex(ValueError, "identity differs"):
                    native.verify(inventory)
            finally:
                library.write_bytes(content)
            native.verify(inventory)
            library.unlink()
            with self.assertRaises((ValueError, OSError)):
                native.verify(inventory)
            with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                native.capture(inventory, binary)
            self.assertFalse(sentinel.exists())

    def test_os_cohort_mismatch_refuses_and_no_legacy_default_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory, binary, _, sentinel = self.fixture(directory)
            with self.assertRaises(KeyError):
                native.verify(inventory)
            inventory["prerequisites"] = native.capture(inventory, binary)
            key = "build" if sys.platform == "darwin" else "kernel_release"
            inventory["prerequisites"]["platform"][key] += "-unqualified"
            with self.assertRaisesRegex(ValueError, "platform differs"):
                native.verify(inventory)
            self.assertFalse(sentinel.exists())

    def test_parent_search_path_resolves_transitive_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory, binary, provider, sentinel = self.fixture(directory)
            root = Path(directory).resolve()
            secondary_source = root / "secondary.c"
            secondary_source.write_text("int secondary_value(void) { return 7; }\n")
            secondary = provider.with_name("libsecondary.dylib" if sys.platform == "darwin" else "libsecondary.so")
            provider_source = root / "provider.c"
            provider_source.write_text("extern int secondary_value(void);\n" + provider_source.read_text().replace("return 7;", "return secondary_value();"))
            if sys.platform == "darwin":
                secondary_flags = ["-dynamiclib", "-Wl,-install_name,@rpath/libsecondary.dylib"]
                provider_flags = ["-dynamiclib", "-Wl,-install_name,@rpath/libprovider.dylib"]
                host_flags = ["-Wl,-rpath,@loader_path/../../system"]
            else:
                secondary_flags = ["-shared", "-Wl,-soname,libsecondary.so"]
                provider_flags = ["-shared", "-Wl,-soname,libprovider.so"]
                host_flags = ["-Wl,--disable-new-dtags,-rpath,$ORIGIN/../../system", "-Wl,-rpath-link," + str(provider.parent)]
            subprocess.run(["cc", *secondary_flags, "-fPIC", str(secondary_source), "-o", str(secondary)], check=True)
            subprocess.run(["cc", *provider_flags, "-fPIC", str(provider_source), "-L" + str(provider.parent), "-lsecondary", "-o", str(provider)], check=True)
            subprocess.run(["cc", str(root / "host.c"), "-L" + str(provider.parent), "-lprovider", *host_flags, "-o", str(binary)], check=True)
            inventory["prerequisites"] = native.capture(inventory, binary)
            reference = "@rpath/libsecondary.dylib" if sys.platform == "darwin" else "libsecondary.so"
            self.assertEqual(inventory["prerequisites"]["dependencies"][str(provider)][reference], [str(secondary)])
            native.verify(inventory)
            self.assertFalse(sentinel.exists(), "transitive capture executed a constructor")

    def test_distinct_parent_contexts_keep_both_qualified_providers(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory, binary, _, sentinel = self.fixture(directory)
            root = Path(directory).resolve()
            shared, one, two = (root / "system" / name for name in ("shared", "one", "two"))
            for path in (shared, one, two):
                path.mkdir()
            suffix = ".dylib" if sys.platform == "darwin" else ".so"

            def library(source, target, name, links=(), rpaths=()):
                options = ["-dynamiclib", "-Wl,-install_name,@rpath/" + name] if sys.platform == "darwin" else ["-shared", "-Wl,-soname," + name]
                if rpaths:
                    if sys.platform == "darwin":
                        options += ["-Wl,-rpath," + path for path in rpaths]
                    else:
                        options += ["-Wl,--disable-new-dtags,-rpath," + ":".join(rpaths)]
                subprocess.run(["cc", *options, "-fPIC", str(source), *links, "-o", str(target)], check=True)

            secondary_source = root / "secondary.c"
            secondary_source.write_text("int secondary_value(void) { return 7; }\n")
            providers = [path / ("libsecondary" + suffix) for path in (one, two)]
            for target in providers:
                library(secondary_source, target, target.name)
            intermediate_source = root / "intermediate.c"
            intermediate_source.write_text("extern int secondary_value(void); int intermediate_value(void) { return secondary_value(); }\n")
            intermediate = shared / ("libintermediate" + suffix)
            library(intermediate_source, intermediate, intermediate.name, ["-L" + str(one), "-lsecondary"])
            for index, provider in enumerate((one, two)):
                source = root / ("entry" + str(index) + ".c")
                source.write_text("extern int intermediate_value(void); int entry_value(void) { return intermediate_value(); }\n")
                target = Path(inventory["home"]) / ("entry" + str(index) + ".so")
                token = "@loader_path" if sys.platform == "darwin" else "$ORIGIN"
                paths = [token + "/../system/shared", token + "/../system/" + provider.name]
                links = ["-L" + str(shared), "-lintermediate"]
                if sys.platform == "linux":
                    links += ["-Wl,-rpath-link," + str(provider)]
                library(source, target, target.name, links, paths)
                inventory["files"][str(target)] = dict(kind="file")
            inventory["prerequisites"] = native.capture(inventory, binary)
            reference = "@rpath/libsecondary.dylib" if sys.platform == "darwin" else "libsecondary.so"
            self.assertEqual(inventory["prerequisites"]["dependencies"][str(intermediate)][reference], sorted(map(str, providers)))
            native.verify(inventory)
            self.assertFalse(sentinel.exists(), "context inspection executed a constructor")


if __name__ == "__main__":
    unittest.main()
