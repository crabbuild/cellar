//! Capture a selected CPython layout at build time; native-only builds need none.
// Build-tool processes and output files are outside node execution domains.
#![allow(clippy::disallowed_methods)]

#[cfg(feature = "embed")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use pyo3_build_config::{BuildFlag, PythonImplementation};
    use std::{
        io::{Error, Write},
        path::PathBuf,
        process::Command,
    };

    pyo3_build_config::use_pyo3_cfgs();
    let interpreter = pyo3_build_config::get();
    if interpreter.implementation() != PythonImplementation::CPython
        || interpreter.version().to_string() != "3.14"
    {
        return Err(Error::other("cellar-python embed requires CPython 3.14").into());
    }
    let gil_disabled = interpreter
        .build_flags()
        .0
        .contains(&BuildFlag::Py_GIL_DISABLED);
    if !interpreter.shared() {
        return Err(Error::other("cellar-python embed requires a shared libpython runtime").into());
    }
    writeln!(
        std::io::stdout().lock(),
        "cargo:rerun-if-env-changed=CELLAR_PYTHON_BUILD_LAYOUT"
    )?;
    let bytes = if let Some(file) = std::env::var_os("CELLAR_PYTHON_BUILD_LAYOUT") {
        writeln!(
            std::io::stdout().lock(),
            "cargo:rerun-if-changed={}",
            PathBuf::from(&file).display()
        )?;
        std::fs::read(file)?
    } else {
        let executable = interpreter.executable().ok_or_else(|| Error::other(
            "cross-builds must set CELLAR_PYTHON_BUILD_LAYOUT to the target interpreter layout JSON",
        ))?;
        writeln!(
            std::io::stdout().lock(),
            "cargo:rerun-if-changed={executable}"
        )?;
        let output = Command::new(executable).args(["-I", "-S", "-B", "-c", r#"
import json, os, sys
json.dump({
    'home': os.path.realpath(sys.base_prefix),
    'executable': os.path.realpath(sys.executable),
    'paths': [os.path.realpath(p) for p in sys.path if os.path.exists(p)],
    'soabi': f'cpython-{sys.version_info.major}{sys.version_info.minor}{sys.abiflags}-{sys.implementation._multiarch}',
    'version': list(sys.version_info[:2]),
    'implementation': sys.implementation.name,
    'gil_disabled': 't' in sys.abiflags,
}, sys.stdout)
"#]).output()?;
        if !output.status.success() {
            return Err(Error::other(format!(
                "selected CPython layout probe failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
            .into());
        }
        output.stdout
    };
    let layout: serde_json::Value = serde_json::from_slice(&bytes)?;
    if layout["version"] != serde_json::json!([3, 14])
        || layout["implementation"] != "cpython"
        || layout["gil_disabled"].as_bool() != Some(gil_disabled)
    {
        return Err(Error::other(
            "build layout must match the selected CPython 3.14 threading ABI",
        )
        .into());
    }
    let string = |key| {
        layout[key]
            .as_str()
            .ok_or_else(|| Error::other(format!("CPython layout requires {key}")))
    };
    let home = string("home")?;
    let executable = string("executable")?;
    let soabi = string("soabi")?;
    let abi_prefix = if gil_disabled {
        "cpython-314t-"
    } else {
        "cpython-314-"
    };
    let paths = layout["paths"]
        .as_array()
        .ok_or_else(|| Error::other("CPython layout requires paths"))?
        .iter()
        .map(|path| {
            path.as_str()
                .ok_or_else(|| Error::other("CPython layout paths must be strings"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !PathBuf::from(home).is_absolute()
        || !PathBuf::from(executable).is_absolute()
        || paths.is_empty()
        || paths.iter().any(|path| !PathBuf::from(path).is_absolute())
        || !soabi.starts_with(abi_prefix)
    {
        return Err(Error::other(
            "CPython layout needs absolute home/executable/import roots and a cpython-314 SOABI",
        )
        .into());
    }
    writeln!(
        std::io::stdout().lock(),
        "cargo:rerun-if-env-changed=CELLAR_PYTHON_INVENTORY"
    )?;
    let inventory = match std::env::var_os("CELLAR_PYTHON_INVENTORY") {
        Some(file) => {
            writeln!(
                std::io::stdout().lock(),
                "cargo:rerun-if-changed={}",
                PathBuf::from(&file).display()
            )?;
            let text = std::fs::read_to_string(file)?;
            let _: serde_json::Value = serde_json::from_str(&text)?;
            text
        }
        None => String::new(),
    };
    let modules: Vec<String> = if inventory.is_empty() {
        Vec::new()
    } else {
        let value: serde_json::Value = serde_json::from_str(&inventory)?;
        value["module_paths"]
            .as_array()
            .ok_or_else(|| Error::other("Python inventory requires module_paths"))?
            .iter()
            .map(|path| {
                let path = path
                    .as_str()
                    .ok_or_else(|| Error::other("Python module path must be a string"))?;
                if !PathBuf::from(path).is_absolute() || path.contains('\0') {
                    return Err(Error::other(
                        "Python module paths must be absolute and NUL-free",
                    ));
                }
                Ok(path.to_string())
            })
            .collect::<Result<Vec<_>, Error>>()?
    };
    let generated = format!(
        "pub const INVENTORY: &str = {inventory:?};\npub const MODULE_PATHS: &[&str] = &{modules:?};\npub const TARGET: &str = {target:?};\npub const HOME: &str = {home:?};\npub const EXECUTABLE: &str = {executable:?};\npub const SOABI: &str = {soabi:?};\npub const GIL_DISABLED: bool = {gil_disabled};\npub const PATHS: &[&str] = &{paths:?};\n",
        target = std::env::var("TARGET")?,
    );
    std::fs::write(
        PathBuf::from(
            std::env::var_os("OUT_DIR").ok_or_else(|| Error::other("Cargo OUT_DIR missing"))?,
        )
        .join("python_layout.rs"),
        generated,
    )?;
    Ok(())
}

#[cfg(not(feature = "embed"))]
fn main() {}
