//! Disposable real-package recovery drill. The provisioning tree must be private
//! scratch data; no running host may share it. All filesystem/process work is
//! owned by this qualification executable, outside node execution domains.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    native_deploy::{current_target, Package},
};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

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
    let mut arguments = std::env::args().skip(1);
    let package = PathBuf::from(
        arguments
            .next()
            .context("usage: python-package-qualification PACKAGE_DIR SCRATCH_PREFIX")?,
    )
    .canonicalize()?;
    let scratch =
        PathBuf::from(arguments.next().context("scratch prefix is required")?).canonicalize()?;
    ensure!(
        arguments.next().is_none(),
        "unexpected qualification arguments"
    );
    let package_data: Package =
        serde_json::from_slice(&std::fs::read(package.join("package.json"))?)?;
    package_data.validate()?;
    ensure!(
        package_data.primary.script_name == "python-example",
        "this probe targets the bundled Python example"
    );
    let reference = package_data
        .primary
        .native
        .as_ref()
        .unwrap()
        .python
        .iter()
        .find(|reference| reference.target == current_target())
        .context("package has no current-target Python runtime")?;
    ensure!(
        reference.prefix.starts_with(&scratch) && reference.prefix != scratch,
        "runtime prefix must be inside explicit scratch directory"
    );
    ensure!(
        !package.starts_with(&reference.prefix),
        "staged package must survive moving the runtime prefix"
    );
    let work = tempfile::tempdir_in(&scratch)?;
    ensure!(
        !work.path().starts_with(&reference.prefix),
        "qualification work cannot be inside runtime prefix"
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
        let store = work.path().join("bucket.sqlite");
        let bucket = Bucket::open_dev(&store)?;
        package_data.publish_staged(&bucket, &package).await?;
        let installed = work.path().join("installed-host");
        let mut original = Original::move_aside(&reference.prefix)?;
        let outcome = async {
            ensure!(
                !reference.prefix.exists(),
                "original provisioning prefix survived move"
            );
            package_data
                .install_with_python(
                    &bucket,
                    current_target(),
                    &installed,
                    Some(&reference.prefix),
                )
                .await?;
            run_installed(&installed, &package, &store, work.path()).await
        }
        .await;
        let restored = original.restore();
        match (outcome, restored) {
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("original tree restoration also failed: {cleanup}")))
            }
            (Err(error), _) => Err(error),
            (_, Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    })?;
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("PASS native Python package: bucket publication, original runtime removed, closure restored, clean-environment host, native libraries, receipt replay and joined shutdown"))?;
    Ok(())
}

async fn run_installed(host: &Path, package: &Path, store: &Path, work: &Path) -> Result<()> {
    let output = work.join("installed-host.log");
    let stdout = std::fs::File::create(&output)?;
    let mut process = Process(
        Command::new(host)
            .env_clear()
            .args([
                "--no-control-plane",
                "--listen",
                "127.0.0.1:0",
                "--internal-listen",
                "127.0.0.1:0",
            ])
            .env("CELLAR_NATIVE_FLEET", "1")
            .env("CELLAR_INTERNAL_DEV_STORE", store)
            .env("CELLAR_BUCKET", "cellar-dev")
            .env("CELLAR_NODE", "installed-python")
            .env("CELLAR_TEST_DATA_DIR", work.join("node"))
            .env("CELLAR_TOKIO_THREADS", "2")
            .env("CELLAR_PLACEMENT_WEIGHT", "1")
            .env("CELLAR_REBALANCE_INTERVAL_MS", "0")
            .env("CELLAR_DURABILITY", "bucket")
            .env("CELLAR_SHUTDOWN_TOTAL_MS", "8000")
            .env("CELLAR_SHUTDOWN_REPORT_JSON", "1")
            .stdout(Stdio::from(stdout.try_clone()?))
            .stderr(Stdio::from(stdout))
            .spawn()?,
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let (origin, internal) = loop {
        ensure!(
            process.0.try_wait()?.is_none(),
            "installed host exited: {}",
            std::fs::read_to_string(&output)?
        );
        let text = std::fs::read_to_string(&output)?;
        if let Some(address) = text.lines().find_map(|line| {
            line.strip_prefix("cellar listening on ")
                .and_then(|rest| rest.split_whitespace().next())
        }) {
            let origin = format!("http://{address}");
            let internal = text.lines().find_map(|line| {
                line.strip_prefix("cellar internal listening on ")
                    .and_then(|rest| rest.split_whitespace().next())
            });
            if let Some(internal) = internal {
                if client
                    .get(format!("{origin}/health"))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    break (origin, format!("http://{internal}"));
                }
            }
        }
        ensure!(
            Instant::now() < deadline,
            "installed host readiness expired: {text}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let libraries: Value = client
        .get(format!("{origin}/libraries"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        libraries["numpy"] == "2.3.4" && libraries["matrix"][2][2] == 149.0,
        "restored numerical dependency failed"
    );
    for _ in 0..2 {
        let result: Value = client
            .post(format!("{origin}/counters/installed"))
            .header("idempotency-key", "installed-request")
            .json(&json!({"amount":3}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            result["count"] == 3,
            "restored host duplicated receipt effect"
        );
    }
    // This drill has one owner and no handoff successor. Use the same
    // preserving shutdown as `cellar dev`, then verify every native phase.
    // A real SIGTERM handoff without a successor correctly reported incomplete
    // in the initial drill; that evidence must not be relabelled as a drain.
    client
        .post(format!("{internal}/shutdown?handoff=preserve"))
        .send()
        .await?
        .error_for_status()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = process.0.try_wait()? {
            ensure!(
                status.success(),
                "installed host failed shutdown: {}",
                std::fs::read_to_string(&output)?
            );
            break;
        }
        ensure!(Instant::now() < deadline, "installed host failed to join");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let text = std::fs::read_to_string(&output)?;
    let report: Value = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|value| value["event"] == "embedded_python_shutdown")
        .context("missing installed-host combined report")?;
    let report = &report["report"];
    ensure!(
        report["runtime_failed"] == false
            && report["host_failed"] == false
            && report["objects_created"] == report["objects_retired"],
        "installed Python host reported incomplete cleanup"
    );
    ensure!(
        report["pending_tasks"] == 0
            && report["pending_threads"] == 0
            && report["pending_objects"] == 0
            && report["bridge"]["pending_reservations"] == 0
            && report["bridge"]["queued_bytes"] == 0,
        "installed host left work pending"
    );
    ensure!(
        report["native_outcomes"]
            .as_array()
            .is_some_and(|outcomes| !outcomes.is_empty()
                && outcomes.iter().all(|outcome| outcome["complete"] == true)),
        "installed native host did not drain"
    );
    // Also exercise the installed package checker under a separately clean
    // process environment, with no ambient interpreter or site-packages flags.
    let mut checker = Process(
        Command::new(host)
            .env_clear()
            .arg("--check-native-package")
            .arg(package.join("package.json"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = checker.0.try_wait()? {
            ensure!(status.success(), "restored host refused its native package");
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "restored package checker timed out"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}
