// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Loader observations own copied paths only. Callbacks never perform I/O,
//! enter Python or execute SDK operations, including after host admission.
use anyhow::Result;
use std::path::PathBuf;

pub(super) fn snapshot() -> Result<Vec<PathBuf>> {
    #[cfg(target_os = "macos")]
    {
        apple::snapshot()
    }
    #[cfg(target_os = "linux")]
    {
        linux::snapshot()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    anyhow::bail!("native loader qualification is unsupported on this target")
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use anyhow::ensure;
    use std::ffi::{c_void, CStr};

    #[derive(Default)]
    struct Observation {
        paths: Vec<PathBuf>,
        invalid: bool,
    }

    unsafe extern "C" fn visit(
        info: *mut libc::dl_phdr_info,
        size: usize,
        data: *mut c_void,
    ) -> libc::c_int {
        // SAFETY: snapshot passes this live, exclusively borrowed Observation;
        // dl_iterate_phdr calls synchronously and retains no callback data.
        let observation = unsafe { &mut *data.cast::<Observation>() };
        if info.is_null()
            || size
                < std::mem::offset_of!(libc::dl_phdr_info, dlpi_name)
                    + std::mem::size_of::<*const libc::c_char>()
        {
            observation.invalid = true;
            return 1;
        }
        // SAFETY: the size check establishes that this documented stable field
        // is present. No reference to a newer/larger whole struct is created.
        let name = unsafe { std::ptr::read(std::ptr::addr_of!((*info).dlpi_name)) };
        if name.is_null() {
            observation.invalid = true;
            return 1;
        }
        // SAFETY: the loader owns the NUL-terminated name during the callback.
        let name = unsafe { CStr::from_ptr(name) };
        let Ok(name) = name.to_str() else {
            observation.invalid = true;
            return 1;
        };
        if name.is_empty() || matches!(name, "linux-vdso.so.1" | "linux-gate.so.1") {
            return 0;
        }
        if name.len() > 16 * 1024
            || !std::path::Path::new(name).is_absolute()
            || observation.paths.len() >= crate::protocol::MAX_PYTHON_NATIVE_LIBRARIES
        {
            observation.invalid = true;
            return 1;
        }
        observation.paths.push(name.into());
        0
    }

    pub(super) fn snapshot() -> Result<Vec<PathBuf>> {
        let mut observation = Observation::default();
        // SAFETY: the callback only copies loader-owned names into this local
        // observation. It does not dlopen/dlclose or retain loader pointers.
        let status = unsafe {
            libc::dl_iterate_phdr(Some(visit), (&mut observation as *mut Observation).cast())
        };
        ensure!(
            status == 0 && !observation.invalid && !observation.paths.is_empty(),
            "cannot observe qualified native library providers"
        );
        Ok(observation.paths)
    }
}

#[cfg(target_os = "macos")]
mod apple {
    use super::*;
    use anyhow::ensure;
    use std::{
        collections::BTreeMap,
        ffi::{c_void, CStr},
        sync::{LazyLock, Mutex, Once},
    };

    #[derive(Default)]
    struct Catalog {
        images: BTreeMap<usize, PathBuf>,
        invalid: bool,
    }

    static IMAGES: LazyLock<Mutex<Catalog>> = LazyLock::new(|| Mutex::new(Catalog::default()));
    static REGISTERED: Once = Once::new();

    extern "C" {
        fn _dyld_register_func_for_add_image(callback: extern "C" fn(*const c_void, isize));

        fn _dyld_register_func_for_remove_image(callback: extern "C" fn(*const c_void, isize));
    }

    extern "C" fn added(header: *const c_void, _: isize) {
        let mut info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
        // SAFETY: dyld owns the bound image during this callback. dladdr is
        // thread-safe and only inspects its address; no header is dereferenced.
        let found = !header.is_null() && unsafe { libc::dladdr(header, info.as_mut_ptr()) } != 0;
        let path = if found {
            // SAFETY: successful dladdr initialized info. Its pathname remains
            // owned by the loader throughout this add-image callback.
            let info = unsafe { info.assume_init() };
            if info.dli_fname.is_null() {
                None
            } else {
                // SAFETY: non-null dli_fname is a live loader-owned C string.
                unsafe { CStr::from_ptr(info.dli_fname) }
                    .to_str()
                    .ok()
                    .filter(|name| {
                        name.len() <= 16 * 1024 && std::path::Path::new(name).is_absolute()
                    })
                    .map(PathBuf::from)
            }
        } else {
            None
        };
        let mut catalog = IMAGES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if catalog.images.len() >= crate::protocol::MAX_PYTHON_NATIVE_LIBRARIES || path.is_none() {
            catalog.invalid = true;
        } else if let Some(path) = path {
            catalog.images.insert(header as usize, path);
        }
    }

    extern "C" fn removed(header: *const c_void, _: isize) {
        IMAGES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .images
            .remove(&(header as usize));
    }

    pub(super) fn snapshot() -> Result<Vec<PathBuf>> {
        // Iterating _dyld_image_count/name directly races dlopen/dlclose. The
        // public callbacks serialize image lifetime and report all existing
        // images during registration. The catalog has process lifetime.
        REGISTERED.call_once(|| {
            // Construct storage before dyld can synchronously call us back.
            let _ = &*IMAGES;
            // SAFETY: callbacks have C ABI and process-lifetime code/storage.
            // They copy paths only and retain no pointers into unloaded images.
            unsafe {
                _dyld_register_func_for_remove_image(removed);
                _dyld_register_func_for_add_image(added);
            }
        });
        let catalog = IMAGES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure!(
            !catalog.invalid && !catalog.images.is_empty(),
            "cannot observe qualified native library providers"
        );
        Ok(catalog.images.values().cloned().collect())
    }
}
