// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Atomic runtime directory publication outside execution domains.
use anyhow::Result;
use std::path::Path;

pub(super) fn sync_directories(path: &Path) -> Result<()> {
    for child in std::fs::read_dir(path)? {
        let child = child?;
        if child.file_type()?.is_dir() {
            sync_directories(&child.path())?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Runtime files are readable by the serving account; every parent
        // directory must be traversable even when the installer uses umask 077.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
    }
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

pub(super) fn rename_new(source: &Path, destination: &Path) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let source = std::ffi::CString::new(source.as_os_str().as_encoded_bytes())
            .map_err(std::io::Error::other)?;
        let destination = std::ffi::CString::new(destination.as_os_str().as_encoded_bytes())
            .map_err(std::io::Error::other)?;
        #[cfg(target_os = "linux")]
        // SAFETY: both names are owned, NUL-terminated paths valid for this
        // synchronous syscall. Kernel NOREPLACE performs one atomic publication.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(target_os = "macos")]
        // SAFETY: owned path buffers remain alive for the syscall; RENAME_EXCL
        // atomically refuses any existing destination, including empty dirs.
        let result =
            unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source, destination);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "runtime environment installation requires Linux or macOS",
        ))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn runtime_directories_are_readable_after_private_staging() -> Result<()> {
        let root = tempfile::tempdir()?;
        let child = root.path().join("app/node_modules");
        std::fs::create_dir_all(&child)?;
        for path in [root.path(), child.parent().unwrap(), &child] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        }
        sync_directories(root.path())?;
        for path in [root.path(), child.parent().unwrap(), &child] {
            assert_eq!(std::fs::metadata(path)?.permissions().mode() & 0o777, 0o755);
        }
        Ok(())
    }
}
