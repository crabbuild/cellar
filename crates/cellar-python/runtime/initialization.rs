//! Process-owned CPython initialization; no ambient imports or site hooks.
// These filesystem checks occur before a node/execution domain exists.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, ensure, Context as _, Result};
use pyo3::ffi;
use std::{
    ffi::{CStr, CString},
    path::{Path, PathBuf},
    ptr::NonNull,
};

pub(super) mod build_layout {
    include!(concat!(env!("OUT_DIR"), "/python_layout.rs"));
}

/// Explicit interpreter layout. The default captures the interpreter selected
/// by Cargo/PyO3 at build time, never an interpreter discovered at node startup.
#[derive(Clone, Debug)]
pub struct Interpreter {
    pub home: PathBuf,
    pub executable: PathBuf,
    pub standard_library_paths: Vec<PathBuf>,
    pub soabi: String,
}

impl Default for Interpreter {
    fn default() -> Self {
        Self {
            home: build_layout::HOME.into(),
            executable: build_layout::EXECUTABLE.into(),
            standard_library_paths: build_layout::PATHS.iter().map(PathBuf::from).collect(),
            soabi: build_layout::SOABI.into(),
        }
    }
}

impl Interpreter {
    pub fn validate(&self, module_paths: &[PathBuf]) -> Result<()> {
        ensure!(
            !self.standard_library_paths.is_empty(),
            "explicit CPython standard library roots are required"
        );
        ensure!(
            self.soabi.starts_with("cpython-314"),
            "expected a CPython 3.14 SOABI"
        );
        canonical(&self.home, true)?;
        canonical(&self.executable, false)?;
        for directory in self.standard_library_paths.iter().chain(module_paths) {
            canonical(directory, true)?;
        }
        Ok(())
    }

    /// # Safety
    /// The caller owns process-wide CPython initialization: no other thread may
    /// initialize or finalize Python concurrently. No Python API may run until
    /// initialization has returned successfully.
    pub(super) unsafe fn initialize(&self, module_paths: &[PathBuf]) -> Result<()> {
        ensure_main_thread()?;
        self.validate(module_paths)?;
        // SAFETY: exclusive bootstrap is the caller's contract; these two
        // CPython inspection functions are documented as callable before init.
        unsafe {
            ensure!(
                ffi::Py_IsInitialized() == 0,
                "CPython was already initialized outside Cellar"
            );
            let version = CStr::from_ptr(ffi::Py_GetVersion()).to_string_lossy();
            ensure!(
                version.starts_with("3.14."),
                "linked runtime must be CPython 3.14, got {version}"
            );
        }
        let config = Config::new()?;
        for (name, value) in [
            (c"isolated", 1),
            (c"use_environment", 0),
            (c"user_site_directory", 0),
            (c"site_import", 0),
            (c"safe_path", 1),
            (c"parse_argv", 0),
            (c"install_signal_handlers", 0),
            (c"write_bytecode", 0),
            (c"configure_c_stdio", 0),
            (c"pathconfig_warnings", 0),
        ] {
            config.integer(name, value)?;
        }
        config.string(c"home", &canonical(&self.home, true)?)?;
        config.string(c"executable", &canonical(&self.executable, false)?)?;
        config.string(c"program_name", &canonical(&self.executable, false)?)?;
        let paths = self
            .standard_library_paths
            .iter()
            .chain(module_paths)
            .map(|path| canonical(path, true))
            .collect::<Result<Vec<_>>>()?;
        config.paths(&paths)?;
        config.integer(c"module_search_paths_set", 1)?;
        // SAFETY: config exclusively owns a valid PyInitConfig; all borrowed
        // path buffers have been copied by the setters. No Python API runs on
        // another thread, and the main-thread check above succeeded.
        config.check(unsafe { ffi::Py_InitializeFromInitConfig(config.0.as_ptr()) })?;
        // SAFETY: successful initialization attached this main thread and holds
        // the GIL. Release it before PyO3 attaches or the Rust host is started.
        unsafe {
            ffi::PyEval_SaveThread();
        }
        Ok(())
    }
}

fn canonical(path: &Path, directory: bool) -> Result<PathBuf> {
    ensure!(
        path.is_absolute(),
        "CPython paths must be absolute: {}",
        path.display()
    );
    let path = path
        .canonicalize()
        .with_context(|| format!("CPython path is missing: {}", path.display()))?;
    let metadata = path.metadata()?;
    ensure!(
        if directory {
            metadata.is_dir()
        } else {
            metadata.is_file()
        },
        "invalid CPython path kind: {}",
        path.display()
    );
    ensure!(path.to_str().is_some(), "CPython paths must be UTF-8");
    Ok(path)
}

pub(super) fn ensure_main_thread() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: pthread_main_np inspects the calling thread and has no inputs.
        ensure!(
            unsafe { libc::pthread_main_np() } != 0,
            "PythonRuntime must start on the process main thread"
        );
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: these process/thread identifiers require no initialized runtime.
        ensure!(
            unsafe { libc::getpid() == libc::gettid() },
            "PythonRuntime must start on the process main thread"
        );
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    bail!("embedded Python main-thread ownership is not implemented for this platform");
    Ok(())
}

struct Config(NonNull<ffi::PyInitConfig>);

impl Config {
    fn new() -> Result<Self> {
        // SAFETY: the opaque configuration API may be called before Python init.
        Ok(Self(
            NonNull::new(unsafe { ffi::PyInitConfig_Create() })
                .context("allocate CPython initialization configuration")?,
        ))
    }

    fn check(&self, status: i32) -> Result<()> {
        if status >= 0 {
            return Ok(());
        }
        let mut error = std::ptr::null();
        // SAFETY: config is live; the returned UTF-8 pointer is borrowed until
        // the next config API call and is copied before any such call or free.
        unsafe {
            ffi::PyInitConfig_GetError(self.0.as_ptr(), &mut error);
            if !error.is_null() {
                bail!(
                    "CPython initialization failed: {}",
                    CStr::from_ptr(error).to_string_lossy()
                );
            }
        }
        bail!("CPython initialization failed")
    }

    fn integer(&self, name: &CStr, value: u64) -> Result<()> {
        // SAFETY: config owns its opaque pointer; name is a live C string.
        self.check(unsafe { ffi::PyInitConfig_SetInt(self.0.as_ptr(), name.as_ptr(), value) })
    }

    fn string(&self, name: &CStr, value: &Path) -> Result<()> {
        let value = CString::new(value.to_str().context("CPython path must be UTF-8")?)?;
        // SAFETY: the setter copies the live C string into the owned config.
        self.check(unsafe {
            ffi::PyInitConfig_SetStr(self.0.as_ptr(), name.as_ptr(), value.as_ptr())
        })
    }

    fn paths(&self, paths: &[PathBuf]) -> Result<()> {
        let strings = paths
            .iter()
            .map(|path| {
                CString::new(path.to_str().context("CPython import root must be UTF-8")?)
                    .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut pointers = strings
            .iter()
            .map(|string| string.as_ptr())
            .collect::<Vec<_>>();
        // SAFETY: all buffers/pointers remain live for this call; the setter
        // copies the complete list, retaining none of these Rust allocations.
        self.check(unsafe {
            ffi::PyInitConfig_SetStrList(
                self.0.as_ptr(),
                c"module_search_paths".as_ptr(),
                pointers.len(),
                pointers.as_mut_ptr(),
            )
        })
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        // SAFETY: this is the sole owner of the config; freeing it does not
        // finalize Python, and no other code retains this opaque pointer.
        unsafe {
            ffi::PyInitConfig_Free(self.0.as_ptr());
        }
    }
}
