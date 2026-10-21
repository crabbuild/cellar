"""Build-time native dependency capture. Never imports an application module.

Mach-O commands are read with otool. GNU ELF resolution uses the qualified
system loader's --list mode, not ldd or execution of the inspected program.
Verification on deployed hosts uses Rust and does not require these tools.
"""
import ctypes
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from functools import lru_cache

MAX_LIBRARIES = 4096
MAX_EDGES = 65_536


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def command(*arguments):
    # Build tools operate with explicit loader configuration. Do not inherit a
    # preload, audit module, search path or tunable from the caller's shell.
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("LD_", "DYLD_")) and key != "GLIBC_TUNABLES"}
    result = subprocess.run(arguments, capture_output=True, text=True, check=True,
                            timeout=15, env=environment)
    if len(result.stdout) > 4 * 1024 * 1024:
        raise ValueError("Native dependency inspection exceeds output bound")
    return result.stdout


def platform():
    machine = os.uname().machine
    if sys.platform == "darwin":
        return dict(kind="macos", machine=machine,
                    build=command("/usr/bin/sw_vers", "-buildVersion").strip(),
                    shared_cache_uuid=shared_cache_uuid(machine))
    if sys.platform == "linux":
        return dict(kind="linux", machine=machine, kernel_release=os.uname().release,
                    os_release_sha256=digest("/etc/os-release"))
    raise ValueError("Python native prerequisites support GNU/Linux and macOS")


@lru_cache(maxsize=1)
def shared_cache_uuid(machine):
    source = Path(__file__).resolve().parents[2] / "crates/cellar/cli/native_deploy/python_environment/prerequisites/shared_cache.c"
    with tempfile.TemporaryDirectory() as directory:
        tool = Path(directory) / "shared-cache-identity"
        command("cc", "-arch", machine, "-DCELLAR_PYTHON_SHARED_CACHE_TOOL", str(source), "-o", str(tool))
        value = command(str(tool)).strip()
    if not re.fullmatch(r"[0-9a-f]{32}", value) or value == "0" * 32:
        raise ValueError("Qualified process has no shared cache UUID")
    return value


def cached(path):
    if sys.platform != "darwin":
        return False
    if not (Path(path).is_relative_to("/usr/lib") or Path(path).is_relative_to("/System/Library")):
        return False
    check = ctypes.CDLL(None)._dyld_shared_cache_contains_path
    check.argtypes, check.restype = [ctypes.c_char_p], ctypes.c_bool
    return check(os.fsencode(path))


def native(path):
    with Path(path).open("rb") as stream:
        header = stream.read(8)
    if header[:4] in (b"\x7fELF", b"\xce\xfa\xed\xfe", b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xce", b"\xfe\xed\xfa\xcf"):
        return True
    if header[:4] in (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf"):
        return 1 <= int.from_bytes(header[4:8], "big") <= 32
    if header[:4] in (b"\xbe\xba\xfe\xca", b"\xbf\xba\xfe\xca"):
        return 1 <= int.from_bytes(header[4:8], "little") <= 32
    return False


def macho(path, machine):
    references, rpaths, current = [], [], None
    kinds = {"LC_LOAD_DYLIB", "LC_LOAD_WEAK_DYLIB", "LC_REEXPORT_DYLIB",
             "LC_LOAD_UPWARD_DYLIB", "LC_LAZY_LOAD_DYLIB", "LC_LOAD_DYLINKER"}
    for line in command("/usr/bin/otool", "-arch", machine, "-l", str(path)).splitlines():
        line = line.strip()
        if line.startswith("cmd "):
            current = line.split()[1]
        elif current in kinds and line.startswith("name "):
            references.append((line[5:].rsplit(" (offset ", 1)[0], current == "LC_LOAD_WEAK_DYLIB"))
        elif current == "LC_RPATH" and line.startswith("path "):
            rpaths.append(line[5:].rsplit(" (offset ", 1)[0])
    return references, rpaths


def elf(path):
    dynamic = command("readelf", "--wide", "-d", str(path))
    references = re.findall(r"\(NEEDED\).*?\[([^\]]+)\]", dynamic)
    interpreter = re.search(r"Requesting program interpreter: ([^\]]+)\]",
                            command("readelf", "--wide", "-l", str(path)))
    return references, Path(interpreter[1]) if interpreter else None


def linux_providers(loader, path, library_paths):
    # --list exits after dependency resolution; constructors and entry points
    # of the inspected object do not run. Keep the chosen loader explicit.
    output = command(str(loader), "--library-path", os.pathsep.join(map(str, library_paths)),
                     "--list", str(path))
    result = {}
    for line in output.splitlines():
        entry = re.fullmatch(r"\s*(.*?)\s+=>\s+(/.*?)\s+\(0x[0-9a-fA-F]+\)\s*", line)
        direct = re.fullmatch(r"\s*(/.*?)\s+\(0x[0-9a-fA-F]+\)\s*", line)
        if entry:
            result[entry[1]] = Path(entry[2]).resolve(strict=True)
        elif direct:
            resolved = Path(direct[1]).resolve(strict=True)
            result[direct[1]] = resolved
            result[Path(direct[1]).name] = resolved
    return result


def capture(inventory, native_host, extra_libraries=()):
    cohort = platform()
    target = inventory["target"]
    expected_machine = "arm64" if target.startswith("aarch64-") and cohort["kind"] == "macos" else target.split("-", 1)[0]
    if (cohort["machine"] != expected_machine or
        (cohort["kind"] == "macos" and not target.endswith("-apple-darwin")) or
        (cohort["kind"] == "linux" and not target.endswith("-linux-gnu"))):
        raise ValueError("Capture must run on the qualified target platform")
    roots = [Path(root) for root in inventory["roots"]]
    executable = Path(inventory["executable"])
    native_host = Path(native_host).resolve(strict=True)
    if not native(native_host):
        raise ValueError("Native host is not an ELF or Mach-O executable")
    library_paths = [Path(inventory["home"]) / "lib", Path(inventory["home"]) / "lib64"]
    library_paths = [path for path in library_paths if path.is_dir()]
    if any(os.pathsep in str(path) for path in library_paths):
        raise ValueError("Native library search directories contain a path separator")
    libraries, graph, queue = {}, {}, []
    seen, edges = set(), 0
    if cohort["kind"] == "linux":
        _, loader = elf(executable)
        if loader is None:
            raise ValueError("Qualified CPython needs an explicit GNU ELF interpreter")
        loader = loader.resolve(strict=True)
    else:
        loader = None

    def provider(path, inherited=(), main=native_host):
        # Cache entries may intentionally have no filesystem inode.
        path = Path(os.path.normpath(path))
        if cached(path):
            key = str(path)
            libraries[key] = dict(kind="macos_shared_cache")
            if len(libraries) > MAX_LIBRARIES:
                raise ValueError("Native prerequisite library limit")
            return key
        path = Path(path).resolve(strict=True)
        key = str(path)
        if key not in inventory["files"]:
            if any(path.is_relative_to(root) for root in roots):
                raise ValueError("Native dependency is outside the inventoried file set")
            before = path.stat()
            identity = dict(kind="file", bytes=before.st_size, sha256=digest(path))
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError("External native library changed while hashing")
            libraries[key] = identity
        if len(libraries) > MAX_LIBRARIES:
            raise ValueError("Native prerequisite library limit")
        if not native(path):
            raise ValueError("Native dependency provider is not a native binary")
        queue.append((path, inherited, main))
        return key

    def resolve_macho(reference, source, rpaths, main):
        def expand(value):
            return value.replace("@loader_path", str(source.parent)).replace("@executable_path", str(main.parent))
        if reference.startswith("@rpath/"):
            candidates = [Path(expand(path)) / reference[len("@rpath/"):] for path in rpaths]
            candidates += [path / reference[len("@rpath/"):] for path in library_paths]
        elif reference.startswith(("@loader_path/", "@executable_path/")):
            candidates = [Path(expand(reference))]
        elif Path(reference).is_absolute():
            candidates = [Path(reference)]
        else:
            raise ValueError("Native dependency depends on an ambient working directory")
        for candidate in candidates:
            if cached(candidate) or candidate.is_file():
                return candidate
        return None

    def dependencies(path, main, inherited=()):
        nonlocal edges
        resolved = {}
        if cohort["kind"] == "macos":
            references, rpaths = macho(path, cohort["machine"])
            own = [os.path.normpath(value.replace("@loader_path", str(path.parent)).replace("@executable_path", str(main.parent))) for value in rpaths]
            if any(not Path(value).is_absolute() for value in own):
                raise ValueError("Native dependency has an ambient relative RPATH")
            context = tuple(dict.fromkeys([*own, *inherited]))
            for reference, weak in references:
                destination = resolve_macho(reference, path, context, main)
                if destination is None and weak:
                    continue
                if destination is None:
                    raise ValueError("Unresolved required Mach-O dependency: " + reference)
                resolved[reference] = [provider(destination, context, main)]
        else:
            references, interpreter = elf(path)
            candidates = dict(inherited) if inherited else linux_providers(loader, path, library_paths) if references else {}
            context = tuple(sorted(candidates.items()))
            for reference in references:
                destination = Path(reference) if Path(reference).is_absolute() else candidates.get(reference)
                if destination is None:
                    raise ValueError("Unresolved required ELF dependency: " + reference)
                resolved[reference] = [provider(destination, context, main)]
            if interpreter:
                resolved["$interpreter"] = [provider(interpreter, context, main)]
        edges += len(resolved)
        if edges > MAX_EDGES:
            raise ValueError("Native prerequisite edge limit")
        return resolved

    host_dependencies = dependencies(native_host, native_host)
    native_files = {Path(path) for path, entry in inventory["files"].items()
                    if entry["kind"] == "file" and native(path)}
    for path in sorted(native_files):
        queue.append((path, (), executable if path == executable else native_host))
    for library in extra_libraries:
        provider(Path(library))
    while queue:
        path, inherited, main = queue.pop()
        key = (path, inherited, main)
        if key in seen:
            continue
        try:
            captured = dependencies(path, main, inherited)
        except (ValueError, subprocess.CalledProcessError):
            # A dependency-only DSO can require its parent's inherited RPATH.
            # Keep it unresolved until a real parent context visits it. A
            # missing record is an error after all roots have been inspected.
            if inherited or path == executable:
                raise
            continue
        seen.add(key)
        if len(seen) > MAX_EDGES:
            raise ValueError("Native dependency graph source limit")
        recorded = graph.setdefault(str(path), {})
        for reference, providers in captured.items():
            recorded[reference] = sorted(set(recorded.get(reference, []) + providers))
    required = {str(path) for path in native_files} | {path for path, entry in libraries.items() if entry["kind"] == "file"}
    if not required.issubset(graph):
        raise ValueError("A native binary has no resolved dependency context")
    if not libraries or not host_dependencies:
        raise ValueError("Native dependency capture produced no qualified providers")
    return dict(platform=cohort, libraries=dict(sorted(libraries.items())),
                dependencies=dict(sorted(graph.items())), host_dependencies=host_dependencies)


def verify(inventory):
    expected = inventory["prerequisites"]
    if expected["platform"] != platform():
        raise ValueError("Qualified native host platform differs")
    for path, identity in expected["libraries"].items():
        if identity["kind"] == "macos_shared_cache":
            if not cached(path):
                raise ValueError("Required macOS shared-cache library is absent")
        elif identity["kind"] == "file":
            file = Path(path)
            if str(file.resolve(strict=True)) != path or file.stat().st_size != identity["bytes"] or digest(file) != identity["sha256"]:
                raise ValueError("External native library identity differs")
        else:
            raise ValueError("Unknown native prerequisite kind")
