//! Disposable, bounded interpreter-bootstrap tests. No node or live data is used.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use cellar_python::{Environment, EventKind, Options, PythonRuntime, Source};
use pyo3::types::PyAnyMethods;
use std::{
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const APPLICATION: &str = r#"
import sys, os, sysconfig, signal
assert signal.getsignal(signal.SIGINT) == signal.SIG_DFL
assert os.environ.get('APPLICATION_SETTING') == 'configured-value'
assert 'PYTHONHOME' not in os.environ
assert 'PYTHONPATH' not in os.environ
assert '_PYTHON_SYSCONFIGDATA_NAME' not in os.environ
assert '_PYTHON_SYSCONFIGDATA_PATH' not in os.environ
assert b'PYTHONHOME' not in os.environb
assert sysconfig.get_config_var('SOABI').startswith('cpython-314')
from approved_dependency import VALUE
assert VALUE == 42
assert sys.flags.isolated == 1
assert sys.flags.ignore_environment == 1
assert sys.flags.no_user_site == 1
assert sys.flags.no_site == 1
assert sys.flags.safe_path
assert 'sitecustomize' not in sys.modules
assert 'usercustomize' not in sys.modules
assert 'ambient_dependency' not in sys.modules
if sys.stderr is not None:
    sys.stderr.write('BOOTSTRAP-PYTHON-STDERR-RESTORED\n')
"#;

fn main() -> Result<()> {
    let arguments = std::env::args().collect::<Vec<_>>();
    if arguments.len() == 3 {
        return child(&arguments[1], Path::new(&arguments[2]));
    }
    let modes = [
        "isolated",
        "unlisted",
        "missing-home",
        "empty-mailbox",
        "oversized-mailbox",
        "undersized-value-budget",
        "wrong-abi",
        "wrong-threading-abi",
        "concurrent-errors",
        "relative-root",
        "preinitialized",
        "wrong-thread",
        "missing-handler",
        "sync-handler",
        "bad-constructor",
        "async-constructor",
        "duplicate-event",
        "host-signals",
        "stderr-closed",
        "stderr-cloexec",
        "private-import",
        "private-descriptor",
        "private-nul-source",
        "private-nul-filename",
        "private-relative-root",
        "private-missing-home",
        "private-home-kind",
        "private-nul-path",
        "private-soabi",
        "private-pin-expected",
        "private-pin-actual",
        "private-pin-name",
        "private-missing-pin",
        "private-init-failure",
        "private-init-import",
        "private-init-builtin",
    ];
    // Cargo forwards libtest flags to this harness-free binary too. Preserve
    // full-suite execution for those flags while permitting one named scenario.
    let selected = arguments.get(1).filter(|value| !value.starts_with('-'));
    if let Some(selected) = selected {
        ensure!(
            modes.contains(&selected.as_str()),
            "unknown startup scenario"
        );
    }
    for mode in modes
        .into_iter()
        .filter(|mode| selected.is_none_or(|selected| selected == mode))
    {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let ambient = root.join("ambient");
        let explicit = root.join("explicit");
        std::fs::create_dir_all(&ambient)?;
        std::fs::create_dir_all(&explicit)?;
        let sentinel = root.join("executed-unapproved-code");
        let poison = format!("from pathlib import Path\nPath({:?}).write_text('bad')\nraise RuntimeError('unapproved import executed')\n", sentinel.to_string_lossy());
        for name in [
            "sitecustomize.py",
            "usercustomize.py",
            "asyncio.py",
            "ambient_dependency.py",
            "ambient_sysconfig.py",
        ] {
            std::fs::write(ambient.join(name), &poison)?;
        }
        std::fs::write(explicit.join("approved_dependency.py"), "VALUE = 42\n")?;
        std::fs::write(explicit.join("sitecustomize.py"), &poison)?;
        std::fs::write(
            explicit.join("unapproved.pth"),
            format!(
                "import pathlib; pathlib.Path({:?}).write_text('pth')\n",
                sentinel.to_string_lossy()
            ),
        )?;
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command
            .arg(mode)
            .arg(&root)
            .current_dir(&ambient)
            .env("PYTHONUSERBASE", &ambient)
            .env("PYTHONPATH", &ambient)
            .env("PYTHONHOME", root.join("invalid-ambient-python-home"))
            .env("PYTHONSTARTUP", ambient.join("sitecustomize.py"))
            .env("PYTHONINSPECT", "1")
            .env("_PYTHON_SYSCONFIGDATA_NAME", "ambient_sysconfig")
            .env("_PYTHON_SYSCONFIGDATA_PATH", &ambient);
        if mode == "preinitialized" {
            command
                .env_remove("PYTHONHOME")
                .env_remove("PYTHONPATH")
                .env_remove("PYTHONINSPECT")
                .env_remove("_PYTHON_SYSCONFIGDATA_NAME")
                .env_remove("_PYTHON_SYSCONFIGDATA_PATH")
                .env("PYTHONNOUSERSITE", "1");
        }
        let output = tempfile::NamedTempFile::new()?;
        command
            .stdout(output.as_file().try_clone()?)
            .stderr(output.as_file().try_clone()?);
        let mut process = command.spawn()?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = process.try_wait()? {
                let log = std::fs::read_to_string(output.path())?;
                ensure!(
                    !log.contains("PRIVATE-BOOTSTRAP-SENTINEL"),
                    "bootstrap leaked a private exception or path: {mode}: {}",
                    log.replace("PRIVATE-BOOTSTRAP-SENTINEL", "<redacted>")
                );
                ensure!(status.success(), "bootstrap scenario failed: {mode}: {log}");
                ensure!(
                    log.contains("BOOTSTRAP-DESCRIPTORS-RESTORED"),
                    "bootstrap did not preserve host descriptors: {mode}"
                );
                if matches!(mode, "isolated" | "stderr-cloexec") {
                    ensure!(
                        log.contains("BOOTSTRAP-PYTHON-STDERR-RESTORED"),
                        "Python stderr was not restored before application import: {mode}"
                    );
                }
                break;
            }
            if Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                anyhow::bail!("bootstrap scenario timed out: {mode}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        ensure!(
            !sentinel.exists(),
            "unapproved Python startup code ran in {mode}"
        );
    }
    cellar::note!("PASS isolated CPython bootstrap: ambient paths/site hooks refused, explicit roots accepted, invalid ABI/home/thread/preinitialized state refused");
    Ok(())
}

fn child(mode: &str, root: &Path) -> Result<()> {
    let mut options = Options::default();
    let trace_allocations = matches!(
        mode,
        "isolated" | "unlisted" | "sync-handler" | "bad-constructor"
    );
    options.trace_allocations = trace_allocations;
    options.module_paths.push(root.join("explicit"));
    options.environment =
        Environment::new([("APPLICATION_SETTING".into(), "configured-value".into())])?;
    let entered = Arc::new(AtomicBool::new(false));
    let code = match mode {
        "host-signals" => "import signal; assert signal.getsignal(signal.SIGINT) is None",
        "unlisted" => "import ambient_dependency",
        "sync-handler" => "def fetch(request, ctx): return None",
        "async-constructor" => "class Broken:\n async def __init__(self): pass\n async def fetch(self, request, ctx): pass",
        "bad-constructor" => "class Broken:\n def __init__(self, required): pass\n async def fetch(self, request, ctx): pass",
        "private-import" => "class PrivateError(Exception):\n def __str__(self):\n  print('PRIVATE-BOOTSTRAP-SENTINEL')\n  return 'PRIVATE-BOOTSTRAP-SENTINEL'\nraise PrivateError()",
        "private-descriptor" => "x = 1",
        "private-nul-source" => "# PRIVATE-BOOTSTRAP-SENTINEL\0",
        "private-nul-filename" => "x = 1",
        "concurrent-errors" => r#"
import threading
class Broken:
    def __del__(self):
        raise RuntimeError("PRIVATE-BOOTSTRAP-SENTINEL")
def worker():
    for _ in range(200):
        broken = Broken()
        del broken
    raise RuntimeError("PRIVATE-BOOTSTRAP-SENTINEL")
threads = [threading.Thread(target=worker) for _ in range(16)]
for thread in threads: thread.start()
for thread in threads: thread.join()
import gc
gc.collect()
"#,
        _ => APPLICATION,
    };
    match mode {
        "stderr-closed" => {
            // SAFETY: this disposable child owns its descriptors. The parent
            // still observes stdout and requires successful bootstrap/cleanup.
            ensure!(unsafe { libc::close(libc::STDERR_FILENO) } == 0);
        }
        "stderr-cloexec" | "private-init-builtin" => {
            // SAFETY: this child owns stderr; verify initialization preserves
            // its descriptor flags after both success and codec failure.
            ensure!(
                unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_SETFD, libc::FD_CLOEXEC) } == 0
            );
        }
        "host-signals" => {
            extern "C" fn host_handler(_: libc::c_int) {}
            // SAFETY: this disposable child owns main-thread bootstrap before
            // creating any Python or host threads. The callback is a valid C
            // handler and performs no operations in a signal context.
            let previous = unsafe {
                libc::signal(
                    libc::SIGINT,
                    host_handler as *const () as libc::sighandler_t,
                )
            };
            ensure!(
                previous != libc::SIG_ERR,
                "install native test signal handler"
            );
        }
        "missing-home" => options.interpreter.home = root.join("missing"),
        "private-relative-root" => options
            .module_paths
            .push("PRIVATE-BOOTSTRAP-SENTINEL".into()),
        "private-missing-home" => {
            options.interpreter.home = root.join("PRIVATE-BOOTSTRAP-SENTINEL")
        }
        "private-home-kind" => {
            let path = root.join("PRIVATE-BOOTSTRAP-SENTINEL");
            std::fs::write(&path, "a file is not an interpreter home")?;
            options.interpreter.home = path;
        }
        "private-nul-path" => options.interpreter.home = root.join("PRIVATE-BOOTSTRAP-SENTINEL\0"),
        "private-soabi" => {
            let prefix = if options.interpreter.soabi.starts_with("cpython-314t-") {
                "cpython-314t-"
            } else {
                "cpython-314-"
            };
            options.interpreter.soabi = format!("{prefix}PRIVATE-BOOTSTRAP-SENTINEL");
        }
        _ => {}
    }
    match mode {
        "private-init-builtin" => {
            // Some distributions freeze encodings, so an empty stdlib root
            // does not reach the codec-bootstrap failure. A failing builtin
            // exercises the actual initialization error on either layout.
            unsafe extern "C" fn fail_encodings() -> *mut pyo3::ffi::PyObject {
                // SAFETY: CPython calls this initializer with an attached
                // thread during codec initialization. Its exception type and
                // static string remain live for the call.
                unsafe {
                    pyo3::ffi::PyErr_SetString(
                        pyo3::ffi::PyExc_RuntimeError,
                        c"PRIVATE-BOOTSTRAP-SENTINEL".as_ptr(),
                    );
                }
                std::ptr::null_mut()
            }
            // SAFETY: this disposable process has not initialized Python. The
            // builtin name and initializer have process lifetime; no thread
            // concurrently accesses the initialization table.
            ensure!(
                unsafe {
                    pyo3::ffi::PyImport_AppendInittab(c"encodings".as_ptr(), Some(fail_encodings))
                } == 0,
                "register failing codec initializer"
            );
            let path = root.join("PRIVATE-BOOTSTRAP-SENTINEL");
            std::fs::create_dir(&path)?;
            options.module_paths.push(path);
        }
        "private-init-failure" | "private-init-import" => {
            let path = root.join("PRIVATE-BOOTSTRAP-SENTINEL");
            std::fs::create_dir(&path)?;
            if mode == "private-init-import" {
                let encodings = path.join("encodings");
                std::fs::create_dir(&encodings)?;
                std::fs::write(
                    encodings.join("__init__.py"),
                    "raise RuntimeError('PRIVATE-BOOTSTRAP-SENTINEL')\n",
                )?;
            }
            options.interpreter.standard_library_paths = vec![path];
        }
        "empty-mailbox" => options.max_pending_operations = 0,
        "oversized-mailbox" => options.max_pending_operations = usize::MAX,
        "undersized-value-budget" => options.max_value_bytes = 127,
        "wrong-abi" => {
            options.interpreter.soabi = if options.interpreter.soabi.starts_with("cpython-314t-") {
                "cpython-314t-wrong-abi".into()
            } else {
                "cpython-314-wrong-abi".into()
            }
        }
        "wrong-threading-abi" => {
            options.interpreter.soabi = if options.interpreter.soabi.starts_with("cpython-314t-") {
                "cpython-314-wrong-mode".into()
            } else {
                "cpython-314t-wrong-mode".into()
            }
        }
        "relative-root" => options.module_paths.push(".".into()),
        "preinitialized" => pyo3::Python::initialize(),
        "wrong-thread" => {
            let other = options.clone();
            let result = std::thread::spawn(move || {
                // SAFETY: no other Python APIs are called in this process while
                // this deliberately off-main attempt is checked and refused.
                unsafe {
                    PythonRuntime::run(other, Source::new(APPLICATION, "startup.py"), |_, _| Ok(()))
                }
            })
            .join()
            .map_err(|_| anyhow::anyhow!("wrong-thread test panicked"))?;
            ensure!(result
                .expect_err("non-main thread must be refused")
                .to_string()
                .contains("main thread"));
        }
        _ => {}
    }
    ensure!(Environment::new([("PYTHONPATH".into(), "bad".into())]).is_err());
    ensure!(Environment::new([("_PYTHON_SYSCONFIGDATA_PATH".into(), "bad".into())]).is_err());
    ensure!(!format!(
        "{:?}",
        Environment::new([("PRIVATE_KEY".into(), "secret-value".into())])?
    )
    .contains("secret-value"));
    let check_original_environment = mode != "preinitialized";
    let started = entered.clone();
    let stdout_before = descriptor(libc::STDOUT_FILENO)?;
    let stderr_before = descriptor(libc::STDERR_FILENO)?;
    // SAFETY: each scenario owns a disposable process. There is no concurrent
    // Python initialization/finalization; the preinitialized case is quiescent
    // and must be rejected before Cellar uses it.
    let filename = if mode == "private-nul-filename" {
        "/PRIVATE-BOOTSTRAP-SENTINEL/invalid\0.py"
    } else if mode.starts_with("private-") {
        "/PRIVATE-BOOTSTRAP-SENTINEL/application.py"
    } else {
        "startup.py"
    };
    let mut source = Source::new(code, filename);
    match mode {
        "missing-handler" | "sync-handler" => source = source.events([EventKind::Fetch]),
        "bad-constructor" | "async-constructor" => source = source.cell("Broken"),
        "duplicate-event" => source = source.events([EventKind::Fetch, EventKind::Fetch]),
        "private-descriptor" => source = source.cell("PRIVATE-BOOTSTRAP-SENTINEL"),
        "private-pin-expected" | "private-pin-actual" | "private-pin-name" => {
            let name = if mode == "private-pin-name" {
                "PRIVATE-BOOTSTRAP-SENTINEL"
            } else {
                "approved_dependency"
            };
            let actual = if mode == "private-pin-actual" {
                "PRIVATE-BOOTSTRAP-SENTINEL"
            } else {
                "1.0"
            };
            let expected = if mode == "private-pin-expected" {
                "PRIVATE-BOOTSTRAP-SENTINEL"
            } else {
                "2.0"
            };
            let metadata = root
                .join("explicit")
                .join(format!("{}-1.0.dist-info", name.replace('-', "_")));
            std::fs::create_dir(&metadata)?;
            std::fs::write(
                metadata.join("METADATA"),
                format!("Metadata-Version: 2.1\nName: {name}\nVersion: {actual}\n"),
            )?;
            source = source.require_distribution(name, expected);
        }
        "private-missing-pin" => {
            source = source.require_distribution("PRIVATE-BOOTSTRAP-SENTINEL", "1.0")
        }
        _ => {}
    }
    let result = unsafe {
        PythonRuntime::run(options, source, move |_, _| {
            if check_original_environment {
                ensure!(
                    std::env::var("PYTHONHOME")?.ends_with("invalid-ambient-python-home"),
                    "Python bootstrap changed the Rust process environment"
                );
            }
            started.store(true, Ordering::Release);
            Ok(())
        })
    };
    ensure!(
        descriptor(libc::STDOUT_FILENO)? == stdout_before,
        "bootstrap changed stdout"
    );
    ensure!(
        descriptor(libc::STDERR_FILENO)? == stderr_before,
        "bootstrap changed stderr"
    );
    writeln!(std::io::stdout().lock(), "BOOTSTRAP-DESCRIPTORS-RESTORED")?;
    if trace_allocations {
        let still_tracing = pyo3::Python::attach(|py| -> pyo3::PyResult<bool> {
            py.import("tracemalloc")?
                .call_method0("is_tracing")?
                .extract()
        })?;
        ensure!(!still_tracing, "owned profiler leaked after {mode}");
    }
    let expected = match mode {
        "concurrent-errors" => {
            let failure =
                result.expect_err("thread/destructor failures must prevent a complete report");
            let report = &failure
                .downcast_ref::<cellar_python::Incomplete>()
                .expect("typed incomplete report")
                .report;
            ensure!(
                report.thread_errors == 16 && report.unraisable_errors == 3200,
                "concurrent failure accounting lost updates: threads={} destructors={}",
                report.thread_errors,
                report.unraisable_errors
            );
            return Ok(());
        }
        "isolated" | "wrong-thread" | "host-signals" | "stderr-closed" | "stderr-cloexec" => {
            let report = result?;
            ensure!(report.complete());
            if trace_allocations {
                ensure!(report.interpreter.traced_current_bytes.is_some());
            } else {
                ensure!(report.interpreter.traced_current_bytes.is_none());
            }
            ensure!(entered.load(Ordering::Acquire));
            return Ok(());
        }
        "unlisted" | "private-import" => "Python application import failed",
        "private-descriptor" => "Python cell registration failed",
        "private-nul-source" | "private-nul-filename" => {
            "Python source and filename must not contain NUL bytes"
        }
        "missing-handler" | "sync-handler" | "bad-constructor" | "async-constructor" => {
            "Python handler descriptor validation failed"
        }
        "duplicate-event" => "duplicate Python event",
        "missing-home" => "CPython path is missing",
        "private-missing-home" => "CPython path is missing",
        "private-home-kind" => "invalid CPython path kind",
        "private-nul-path" => "CPython paths must not contain NUL bytes",
        "private-relative-root" => "must be absolute",
        "private-soabi" => "SOABI mismatch",
        "private-pin-expected" | "private-pin-actual" | "private-pin-name" => {
            "Python distribution version mismatch"
        }
        "private-missing-pin" => "Python interpreter execution failed",
        "private-init-failure" | "private-init-import" | "private-init-builtin" => {
            "CPython initialization failed"
        }
        "empty-mailbox" | "oversized-mailbox" => "mailbox capacity is outside its supported range",
        "undersized-value-budget" => "value budget must be at least 128 bytes",
        "wrong-abi" => "SOABI mismatch",
        "wrong-threading-abi" => "threading ABI",
        "relative-root" => "must be absolute",
        "preinitialized" => "already initialized outside Cellar",
        _ => anyhow::bail!("unknown startup mode"),
    };
    let error = result.expect_err("invalid bootstrap must fail");
    if mode.starts_with("private-") {
        ensure!(
            error.chain().count() == 1 && error.to_string().len() <= 128,
            "bootstrap failure retained an unbounded error chain"
        );
    }
    ensure!(
        !error.is::<std::ffi::NulError>(),
        "startup retained original source/filename bytes in a CString error"
    );
    ensure!(
        !format!("{error:?}").contains("PRIVATE-BOOTSTRAP-SENTINEL"),
        "bootstrap retained a private exception source"
    );
    ensure!(
        error.to_string().contains(expected),
        "wrong {mode} failure: {error}"
    );
    ensure!(
        !entered.load(Ordering::Acquire),
        "invalid bootstrap admitted the host"
    );
    Ok(())
}

fn descriptor(fd: libc::c_int) -> Result<Option<(libc::c_int, libc::dev_t, libc::ino_t)>> {
    // SAFETY: these calls inspect a child-owned descriptor; successful fstat
    // initializes the complete stat buffer, and no pointer escapes the call.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        ensure!(std::io::Error::last_os_error().raw_os_error() == Some(libc::EBADF));
        return Ok(None);
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    ensure!(unsafe { libc::fstat(fd, stat.as_mut_ptr()) } == 0);
    // SAFETY: the successful fstat call initialized the buffer.
    let stat = unsafe { stat.assume_init() };
    Ok(Some((flags, stat.st_dev, stat.st_ino)))
}
