//! Disposable owner-death and noncooperative-interpreter drills.
use super::*;
use std::{
    process::{Child, Command},
    time::Instant,
};

pub(super) fn child(mode: &str, root: &Path) -> Result<()> {
    let directory = root.to_path_buf();
    let recovered = mode == "--owner-recovered";
    let marker = directory.join("clean-shutdown.json");
    let report = run(
        Options::default(),
        Source::new(include_str!("../bridge.py"), "bridge.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, stop| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket = cellar::bucket::Bucket::open_dev(&directory.join("objects.sqlite"))?;
                let (node, url) = start_with(
                    &directory.join("disk"),
                    application,
                    if recovered {
                        "recovered-python"
                    } else {
                        "killed-python"
                    },
                    Some(bucket),
                )
                .await?;
                std::fs::write(directory.join("ready"), url)?;
                let outcome = node
                    .run_until(async move {
                        stop.clone().cancelled().await;
                        ShutdownMode::Preserve
                    })
                    .await?;
                // Complete shutdown evidence may only be written after the
                // joined node outcome and Python loop cleanup have both passed.
                ensure!(matches!(outcome, NodeOutcome::Stopped(_)));
                Ok(())
            })
        },
    )?;
    std::fs::write(marker, serde_json::to_vec(&report)?)?;
    Ok(())
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(mode: &str, root: &Path) -> Result<Process> {
    let ready = root.join("ready");
    if ready.exists() {
        std::fs::remove_file(ready)?;
    }
    Ok(Process(
        Command::new(std::env::current_exe()?)
            .arg(mode)
            .arg(root)
            .spawn()?,
    ))
}

async fn ready(process: &mut Process, root: &Path, client: &reqwest::Client) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        ensure!(
            process.0.try_wait()?.is_none(),
            "owner exited before readiness"
        );
        if let Ok(url) = std::fs::read_to_string(root.join("ready")) {
            if let Ok(response) = client.get(format!("{url}/library")).send().await {
                if response.status().is_success() {
                    return Ok(url);
                }
            }
        }
        ensure!(
            Instant::now() < deadline,
            "owner readiness deadline elapsed"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(super) fn qualify() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for route in ["cpu-loop", "blocked-native"] {
            let directory = tempfile::tempdir()?;
            let root = directory.path();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()?;
            let mut owner = spawn("--owner-running", root)?;
            let url = ready(&mut owner, root, &client).await?;
            let before: Value = client
                .post(format!("{url}/counter"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            ensure!(
                before["count"] == 1,
                "write was not acknowledged before kill"
            );
            // Trigger noncooperation, verify it cannot fabricate a successful
            // heartbeat or shutdown, then exercise the external hard-stop path.
            let trigger = client.get(format!("{url}/{route}")).send().await;
            ensure!(
                trigger.is_err() || trigger?.status().is_server_error(),
                "blocking handler unexpectedly completed"
            );
            let heartbeat = client.get(format!("{url}/library")).send().await;
            ensure!(
                heartbeat.is_err() || heartbeat?.status().is_server_error(),
                "blocked interpreter answered heartbeat"
            );
            ensure!(
                owner.0.try_wait()?.is_none(),
                "noncooperative runtime exited without watchdog"
            );
            owner.0.kill()?;
            owner.0.wait()?;
            ensure!(
                !root.join("clean-shutdown.json").exists(),
                "hard kill fabricated clean drain"
            );
            // Remove every owner-local SQLite file. Recovery must use the fleet
            // bucket, rather than accidentally reading the killed owner's disk.
            std::fs::remove_dir_all(root.join("disk"))?;
            tokio::time::sleep(Duration::from_millis(1100)).await;
            let mut recovery = spawn("--owner-recovered", root)?;
            let url = ready(&mut recovery, root, &client).await?;
            let recovered: Value = client
                .post(format!("{url}/counter"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            ensure!(
                recovered["count"] == 2,
                "{route}: recovery lost acknowledged state: {recovered}"
            );
        }
        Ok(())
    })
}
