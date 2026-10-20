//! Disposable, bounded interpreter-bootstrap tests. No node or live data is used.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use cellar_python::{Environment, EventKind, Options, PythonRuntime, Source};
use pyo3::types::PyAnyMethods;
use std::{
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
"#;

fn main() -> Result<()> {
    let arguments = std::env::args().collect::<Vec<_>>();
    if arguments.len() == 3 {
        return child(&arguments[1], Path::new(&arguments[2]));
    }
    for mode in [
        "isolated",
        "unlisted",
        "missing-home",
        "wrong-abi",
        "relative-root",
        "preinitialized",
        "wrong-thread",
        "missing-handler",
        "sync-handler",
        "bad-constructor",
        "async-constructor",
        "duplicate-event",
        "host-signals",
    ] {
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
        let mut process = command.spawn()?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = process.try_wait()? {
                ensure!(status.success(), "bootstrap scenario failed: {mode}");
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
        _ => APPLICATION,
    };
    match mode {
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
        "wrong-abi" => options.interpreter.soabi = "cpython-314-wrong-abi".into(),
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
    // SAFETY: each scenario owns a disposable process. There is no concurrent
    // Python initialization/finalization; the preinitialized case is quiescent
    // and must be rejected before Cellar uses it.
    let mut source = Source::new(code, "startup.py");
    match mode {
        "missing-handler" | "sync-handler" => source = source.events([EventKind::Fetch]),
        "bad-constructor" | "async-constructor" => source = source.cell("Broken"),
        "duplicate-event" => source = source.events([EventKind::Fetch, EventKind::Fetch]),
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
    if trace_allocations {
        let still_tracing = pyo3::Python::attach(|py| -> pyo3::PyResult<bool> {
            py.import("tracemalloc")?
                .call_method0("is_tracing")?
                .extract()
        })?;
        ensure!(!still_tracing, "owned profiler leaked after {mode}");
    }
    let expected = match mode {
        "isolated" | "wrong-thread" | "host-signals" => {
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
        "unlisted" => "ambient_dependency",
        "missing-handler" | "sync-handler" => "must be async functions",
        "bad-constructor" => "required",
        "async-constructor" => "must be synchronous",
        "duplicate-event" => "duplicate Python event",
        "missing-home" => "CPython path is missing",
        "wrong-abi" => "SOABI mismatch",
        "relative-root" => "must be absolute",
        "preinitialized" => "already initialized outside Cellar",
        _ => anyhow::bail!("unknown startup mode"),
    };
    let error = result.expect_err("invalid bootstrap must fail");
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
