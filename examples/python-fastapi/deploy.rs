//! Publish and restore a private FastAPI package, then test its installed host.
//! All filesystem/process work belongs to this disposable caller, not a node.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    native_deploy::{current_target, Package},
};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

struct Original {
    prefix: PathBuf,
    original: PathBuf,
    holder: Option<tempfile::TempDir>,
    restored: bool,
}

impl Original {
    fn move_aside(prefix: &Path) -> Result<Self> {
        let holder = tempfile::tempdir_in(prefix.parent().context("prefix needs parent")?)?;
        let original = holder.path().join("original");
        std::fs::rename(prefix, &original)?;
        Ok(Self {
            prefix: prefix.into(),
            original,
            holder: Some(holder),
            restored: false,
        })
    }

    fn restore(&mut self) -> Result<()> {
        if self.restored {
            return Ok(());
        }
        let holder = self
            .holder
            .as_ref()
            .context("qualification backup directory is missing")?;
        if self.prefix.exists() {
            std::fs::rename(&self.prefix, holder.path().join("recovered"))?;
        }
        std::fs::rename(&self.original, &self.prefix)?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for Original {
    fn drop(&mut self) {
        if self.restore().is_err() {
            // Preserve the original tree on any cleanup failure. Never let the
            // temporary directory destructor erase the only original copy.
            if let Some(holder) = self.holder.take() {
                let path = holder.keep();
                let _ = cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(
                    format_args!(
                        "Qualification cleanup requires restoring original tree from {}",
                        path.display()
                    ),
                );
            }
        }
    }
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let package = PathBuf::from(args.next().context("usage: python-fastapi-deployment PACKAGE SCRATCH_ROOT TEST_PYTHON [--expect-free-threaded]")?).canonicalize()?;
    let scratch = PathBuf::from(args.next().context("scratch root is required")?).canonicalize()?;
    let python =
        PathBuf::from(args.next().context("test Python executable is required")?).canonicalize()?;
    let free_threaded = match args.next().as_deref() {
        None => false,
        Some("--expect-free-threaded") => true,
        _ => anyhow::bail!("unexpected deployment smoke argument"),
    };
    ensure!(
        args.next().is_none(),
        "unexpected deployment smoke argument"
    );
    let data: Package = serde_json::from_slice(&std::fs::read(package.join("package.json"))?)?;
    data.validate()?;
    ensure!(
        data.primary.script_name == "python-fastapi",
        "this drill requires the FastAPI example package"
    );
    let reference = data
        .primary
        .native
        .as_ref()
        .context("native package missing")?
        .python
        .iter()
        .find(|reference| reference.target == current_target())
        .context("matching Python environment missing")?;
    ensure!(
        reference.prefix.starts_with(&scratch) && reference.prefix != scratch,
        "runtime prefix must be inside the explicitly supplied private scratch root"
    );
    ensure!(
        !package.starts_with(&reference.prefix) && !python.starts_with(&reference.prefix),
        "package and test interpreter must survive moving the original runtime"
    );
    let work = tempfile::tempdir_in(&scratch)?;
    ensure!(
        !work.path().starts_with(&reference.prefix),
        "work directory must be outside the runtime"
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
        let store = work.path().join("bucket.sqlite");
        let bucket = Bucket::open_dev(&store)?;
        data.publish_staged(&bucket, &package).await?;
        let installed = work.path().join("installed-host");
        let mut original = Original::move_aside(&reference.prefix)?;
        let outcome = async {
            ensure!(!reference.prefix.exists(), "original runtime still present");
            data.install_with_python(
                &bucket,
                current_target(),
                &installed,
                Some(&reference.prefix),
            )
            .await?;
            let mut command = Command::new(&python);
            command
                .args(["-I", "-B"])
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../examples/python-fastapi/smoke.py"
                ))
                .arg("--host")
                .arg(&installed)
                .arg("--store")
                .arg(&store)
                .arg("--work")
                .arg(work.path());
            if free_threaded {
                command.arg("--expect-free-threaded");
            }
            ensure!(
                command.status()?.success(),
                "installed FastAPI smoke failed"
            );
            Ok(())
        }
        .await;
        let restored = original.restore();
        match (outcome, restored) {
            (Err(error), Err(cleanup)) => Err(error.context(format!(
                "original environment restoration also failed: {cleanup}"
            ))),
            (Err(error), _) | (_, Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    })?;
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
        "PASS FastAPI deployment: bucket publication, original runtime removed, archive restored, installed HTTP, receipts, restart and combined drain"))?;
    Ok(())
}
