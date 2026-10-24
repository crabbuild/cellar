// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::protocol::PythonPlatform;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use anyhow::ensure;
use anyhow::Result;
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::ffi::CStr;
#[cfg(target_os = "linux")]
use std::io::Read;
use std::path::Path;

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn current() -> Result<PythonPlatform> {
    let mut name = std::mem::MaybeUninit::<libc::utsname>::uninit();
    // SAFETY: uname initializes the entire output on success. It takes no
    // borrowed process state and may be called before interpreter startup.
    ensure!(
        unsafe { libc::uname(name.as_mut_ptr()) } == 0,
        "read qualified host platform"
    );
    // SAFETY: successful uname initialized all fields; each field is a
    // NUL-terminated OS-owned byte array copied into this local structure.
    let name = unsafe { name.assume_init() };
    // SAFETY: uname's machine field is a live NUL-terminated local array.
    let machine = unsafe { CStr::from_ptr(name.machine.as_ptr()) }
        .to_str()?
        .to_owned();
    #[cfg(target_os = "macos")]
    {
        let mut buffer = [0u8; 128];
        let mut size = buffer.len();
        // SAFETY: sysctl copies into our bounded live buffer; the name is a
        // static C string, and a null new-value pointer performs no mutation.
        ensure!(
            unsafe {
                libc::sysctlbyname(
                    c"kern.osversion".as_ptr(),
                    buffer.as_mut_ptr().cast(),
                    &mut size,
                    std::ptr::null_mut(),
                    0,
                )
            } == 0
                && size <= buffer.len(),
            "read qualified macOS build"
        );
        let build = CStr::from_bytes_with_nul(&buffer[..size])?
            .to_str()?
            .to_owned();
        extern "C" {
            fn cellar_python_shared_cache_uuid(output: *mut u8) -> libc::c_int;
        }
        let mut uuid = [0u8; 16];
        // SAFETY: the SDK-compiled helper writes exactly 16 bytes to this live
        // buffer, checks the kernel's dyld record version/size, and invokes no
        // Python or application code. The active cache UUID is process-constant.
        ensure!(
            unsafe { cellar_python_shared_cache_uuid(uuid.as_mut_ptr()) } == 0
                && uuid.iter().any(|byte| *byte != 0),
            "read qualified macOS shared cache identity"
        );
        let shared_cache_uuid = uuid.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(PythonPlatform::Macos {
            machine,
            build,
            shared_cache_uuid,
        })
    }
    #[cfg(target_os = "linux")]
    {
        let mut release = Vec::new();
        std::fs::File::open("/etc/os-release")?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut release)?;
        ensure!(
            release.len() <= 1024 * 1024,
            "qualified OS release exceeds bound"
        );
        // SAFETY: uname's release field is another initialized local C string.
        let kernel_release = unsafe { CStr::from_ptr(name.release.as_ptr()) }
            .to_str()?
            .to_owned();
        Ok(PythonPlatform::Linux {
            machine,
            kernel_release,
            os_release_sha256: format!("{:x}", Sha256::digest(&release)),
        })
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(super) fn current() -> Result<PythonPlatform> {
    anyhow::bail!("qualified Python OS prerequisites are unsupported on this target")
}

pub(super) fn cached(path: &Path) -> Result<bool> {
    #[cfg(target_os = "macos")]
    {
        if !crate::protocol::system_cache_path(path) {
            return Ok(false);
        }
        let path = std::ffi::CString::new(
            path.to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid native provider path"))?,
        )?;
        // Resolve lazily so native-only executables retain their older OS
        // deployment target. Python qualification requires this macOS 11 API.
        // SAFETY: dlsym inspects a static symbol without invoking Python.
        let symbol = unsafe {
            libc::dlsym(
                libc::RTLD_DEFAULT,
                c"_dyld_shared_cache_contains_path".as_ptr(),
            )
        };
        ensure!(
            !symbol.is_null(),
            "macOS shared-cache qualification API is unavailable"
        );
        // SAFETY: the resolved public dyld symbol has this exact C signature;
        // its library remains loaded for the complete process lifetime.
        let check: unsafe extern "C" fn(*const libc::c_char) -> bool =
            unsafe { std::mem::transmute(symbol) };
        // SAFETY: path is a live NUL-terminated C string copied above.
        Ok(unsafe { check(path.as_ptr()) })
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        Ok(false)
    }
}
