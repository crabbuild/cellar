// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Run the Cargo development workflow against an external application project.
#![allow(clippy::disallowed_methods)]
#![cfg(unix)]

use anyhow::{bail, Context as _, Result};
use cellar::native_deploy::{current_target, executable_identity, ApplicationConfig, Package};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Supervisor {
    child: Child,
    log: PathBuf,
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
            let deadline = Instant::now() + Duration::from_secs(40);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

impl Supervisor {
    fn start(
        root: &Path,
        project: &Path,
        target: &Path,
        name: &str,
        flags: &[&str],
    ) -> Result<Self> {
        let log = root.join(format!("{name}.log"));
        let stdout = File::create(&log)?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_cellar-native"));
        command
            .args(["dev", project.to_str().unwrap(), "--port", "0"])
            .args(flags)
            .env("CARGO_TARGET_DIR", target)
            .env("NO_COLOR", "1")
            .env("CELLAR_TOKIO_THREADS", "2")
            .env("CELLAR_PLACEMENT_WEIGHT", "1")
            .env("CELLAR_REBALANCE_INTERVAL_MS", "0")
            .env("CELLAR_SHUTDOWN_TOTAL_MS", "8000")
            // Inherited fleet/test settings must not replace private dev storage.
            .env("CELLAR_TEST_DATA_DIR", root.join("unowned-storage"))
            .env("CELLAR_TEST_SCRIPT_PATH", root.join("absent-source.js"))
            .env("CELLAR_BUCKET", "unowned-bucket")
            .stdout(Stdio::from(stdout.try_clone()?))
            .stderr(Stdio::from(stdout));
        Ok(Self {
            child: command.spawn()?,
            log,
        })
    }

    fn text(&self) -> Result<String> {
        Ok(std::fs::read_to_string(&self.log)?)
    }

    async fn ready(&mut self, count: usize) -> Result<String> {
        let deadline = Instant::now() + Duration::from_secs(600);
        loop {
            let text = self.text()?;
            let origins: Vec<_> = text
                .lines()
                .filter_map(|line| {
                    let (label, origin) = line.trim().split_once("http://")?;
                    label
                        .trim()
                        .ends_with("ready")
                        .then(|| format!("http://{}", origin.trim()))
                })
                .collect();
            if origins.len() >= count {
                return Ok(origins[count - 1].clone());
            }
            if let Some(status) = self.child.try_wait()? {
                bail!("dev exited with {status}\n{text}");
            }
            if Instant::now() >= deadline {
                bail!("dev readiness timed out\n{text}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn observed(
        &mut self,
        client: &Client,
        expected: (u64, &str, &str),
        assets: Option<&str>,
    ) -> Result<String> {
        let deadline = Instant::now() + Duration::from_secs(600);
        loop {
            let text = self.text()?;
            let origin = text.lines().rev().find_map(|line| {
                let (label, origin) = line.trim().split_once("http://")?;
                label
                    .trim()
                    .ends_with("ready")
                    .then(|| format!("http://{}", origin.trim()))
            });
            if let Some(origin) = origin {
                if let Ok((count, version, greeting)) = counter(client, &origin, false).await {
                    if (count, version.as_str(), greeting.as_str()) == expected {
                        let matches = match assets {
                            None => true,
                            Some(body) => match client.get(format!("{origin}/")).send().await {
                                Ok(response) => {
                                    response.text().await.is_ok_and(|text| text == body)
                                }
                                Err(_) => false,
                            },
                        };
                        if matches {
                            return Ok(origin);
                        }
                    }
                }
            }
            if let Some(status) = self.child.try_wait()? {
                bail!("dev exited with {status}\n{text}");
            }
            if Instant::now() >= deadline {
                bail!("dev never served expected {expected:?}\n{text}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn failure(&mut self, count: usize) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let text = self.text()?;
            if text.matches("error rebuild failed:").count() >= count {
                return Ok(());
            }
            if let Some(status) = self.child.try_wait()? {
                bail!("dev exited with {status}\n{text}");
            }
            if Instant::now() >= deadline {
                bail!("dev did not report failed rebuild\n{text}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            anyhow::ensure!(
                unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) } == 0
            );
        }
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            if let Some(status) = self.child.try_wait()? {
                anyhow::ensure!(
                    status.success(),
                    "dev shutdown failed: {status}\n{}",
                    self.text()?
                );
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("dev did not shut down\n{}", self.text()?);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn counter(client: &Client, origin: &str, write: bool) -> Result<(u64, String, String)> {
    let response = client
        .request(
            if write { Method::POST } else { Method::GET },
            format!("{origin}/counters/acme"),
        )
        .send()
        .await?
        .error_for_status()?;
    let version = response
        .headers()
        .get("x-native-version")
        .context("missing code version")?
        .to_str()?
        .to_owned();
    let greeting = response
        .headers()
        .get("x-dev-greeting")
        .context("missing dev variable")?
        .to_str()?
        .to_owned();
    let body: Value = response.json().await?;
    Ok((
        body["count"].as_u64().context("missing count")?,
        version,
        greeting,
    ))
}

fn write_project(path: &Path, project: &Value) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(project)?)?;
    Ok(())
}

fn check_packages(executable: &Path, root: &Path, primary: &Value) -> Result<()> {
    let mut config: ApplicationConfig = serde_json::from_value(primary.clone())?;
    config.assets = None;
    let artifact = executable_identity(executable, current_target())?;
    let package = |config: &ApplicationConfig,
                   artifact: cellar::protocol::NativeArtifact|
     -> Result<Package> {
        Ok(Package {
            primary: config.manifest(vec![artifact])?,
            cohosted: Vec::new(),
        })
    };
    let check = |package: &Package, expected: Option<&str>| -> Result<()> {
        let path = root.join("check-package.json");
        std::fs::write(&path, serde_json::to_vec(package)?)?;
        let output = Command::new(executable)
            .arg("--check-native-package")
            .arg(path)
            .output()?;
        let error = String::from_utf8_lossy(&output.stderr);
        if let Some(expected) = expected {
            anyhow::ensure!(
                !output.status.success() && error.contains(expected),
                "package check should refuse {expected}: {error}"
            );
        } else {
            anyhow::ensure!(output.status.success(), "package check failed: {error}");
        }
        Ok(())
    };
    check(&package(&config, artifact.clone())?, None)?;
    let mut wrong = artifact.clone();
    wrong.sha256 = "0".repeat(64);
    check(
        &package(&config, wrong)?,
        Some("running executable is not declared"),
    )?;
    let mut wrong = artifact.clone();
    wrong.target = "unknown-target".into();
    check(
        &package(&config, wrong)?,
        Some("running executable is not declared"),
    )?;
    let mut wrong = config.clone();
    wrong.application_version = "unregistered".into();
    check(
        &package(&wrong, artifact.clone())?,
        Some("no compiled application registered"),
    )?;
    let mut wrong = config.clone();
    wrong.cells.push("MissingCell".into());
    check(
        &package(&wrong, artifact.clone())?,
        Some("no factory for MissingCell"),
    )?;
    let mut wrong = config.clone();
    wrong.bindings.push(json!({"type":"workflow", "name":"JOBS", "workflow_name":"jobs", "class_name":"MissingWorkflow"}));
    check(
        &package(&wrong, artifact.clone())?,
        Some("no workflow handler for MissingWorkflow"),
    )?;
    let mut cohost = package(&config, artifact.clone())?;
    let mut wrong = config;
    wrong.script_name = "unregistered-cohost".into();
    cohost.cohosted.push(wrong.manifest(vec![artifact])?);
    check(&cohost, Some("no compiled application registered"))?;
    Ok(())
}

#[tokio::test]
async fn cargo_dev_restarts_preserve_state_and_refuse_invalid_candidates() -> Result<()> {
    let root = tempfile::tempdir()?;
    let result = scenario(root.path()).await;
    if result.is_err() {
        let logs = ["watch", "second", "resume", "clean"]
            .into_iter()
            .filter_map(|name| {
                std::fs::read_to_string(root.path().join(format!("{name}.log")))
                    .ok()
                    .map(|text| format!("{name}:\n{text}"))
            })
            .collect::<Vec<_>>()
            .join("\n");
        return result.context(logs);
    }
    result
}

async fn scenario(root: &Path) -> Result<()> {
    let directory = root.join("application");
    std::fs::create_dir_all(directory.join("src"))?;
    let assets = directory.join("shared-assets");
    std::fs::create_dir_all(&assets)?;
    std::fs::create_dir_all(directory.join(".cargo"))?;
    std::fs::write(
        directory.join(".cargo/config.toml"),
        "[env]\nCELLAR_NATIVE_PROJECT_MARKER = \"fixture\"\n",
    )?;
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.join("target"));
    let domain = root.join("domain");
    std::fs::create_dir_all(domain.join("src"))?;
    std::fs::write(
        domain.join("Cargo.toml"),
        "[package]\nname = \"dev-domain\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
    )?;
    std::fs::write(
        domain.join("src/lib.rs"),
        "pub const VALUE: &str = \"one\";\n",
    )?;
    let manifest = directory.join("Cargo.toml");
    std::fs::write(&manifest, format!(
        "[package]\nname = \"native-dev-fixture\"\nversion = \"0.0.0\"\nedition = \"2021\"\n[dependencies]\ndev-domain = {{ path = \"../domain\" }}\ncellar = {{ path = {:?}, default-features = false }}\nanyhow = \"1\"\nserde_json = \"1\"\nurl = \"2\"\ntikv-jemallocator = {{ version = \"0.6\", features = [\"background_threads\", \"stats\"] }}\n",
        Path::new(env!("CARGO_MANIFEST_DIR")).to_str().unwrap()))?;
    let source = include_str!("../../../examples/native_counter.rs").replace("native-counter", "native-dev-fixture")
        .replace("(\"x-native-version\".into(), version.into())", "(\"x-native-version\".into(), format!(\"{}-{}\", version, dev_domain::VALUE))")
        .replace("            Ok(Output::Http(response))", "            response.headers.push((\"x-dev-greeting\".into(), context.var(\"GREETING\").unwrap_or(\"\").into()));\n            Ok(Output::Http(response))");
    let source = format!("{source}\nconst _: &str = env!(\"CELLAR_NATIVE_PROJECT_MARKER\");\n");
    let source_path = directory.join("src/main.rs");
    std::fs::write(&source_path, &source)?;
    std::fs::write(assets.join("index.html"), "first assets")?;
    std::fs::copy(workspace.join("Cargo.lock"), directory.join("Cargo.lock"))?;
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(&manifest)
        .current_dir(&directory)
        .env("CARGO_TARGET_DIR", &target)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "fixture dependency resolution failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let project_path = directory.join("project.json");
    let mut project = json!({"cargo_manifest":"Cargo.toml", "package":"native-dev-fixture", "binary":"native-dev-fixture", "profile":"dev",
        "primary":{"script_name":"native-dev-fixture", "application_version":"v1", "cells":["Counter"],
        "bindings":[{"type":"durable_object_namespace", "name":"COUNTERS", "class_name":"Counter"}, {"type":"plain_text", "name":"GREETING", "text":"configured"}],
        "assets":{"directory":"shared-assets", "binding":"ASSETS", "run_worker_first":["/counters/*", "/assets-binding/*"]}}});
    write_project(&project_path, &project)?;
    std::fs::write(
        directory.join(".env"),
        "GREETING=dotenv\nCOUNTERS=ignored\n",
    )?;
    std::fs::write(directory.join(".env.local"), "GREETING=local\n")?;
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let mut dev = Supervisor::start(root, &project_path, &target, "watch", &[])?;
    let mut origin = dev
        .observed(&client, (0, "v1-one", "local"), Some("first assets"))
        .await?;
    assert_eq!(
        counter(&client, &origin, true).await?,
        (1, "v1-one".into(), "local".into())
    );
    assert!(!root.join("unowned-storage").exists());
    assert_eq!(
        client
            .get(format!("{origin}/"))
            .send()
            .await?
            .text()
            .await?,
        "first assets"
    );

    let mut second = Supervisor::start(
        root,
        &project_path,
        &target,
        "second",
        &["--clean", "--no-watch"],
    )?;
    let output = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = second.child.try_wait()? {
                return Ok::<_, anyhow::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    assert!(!output.success());
    assert!(second
        .text()?
        .contains("another native dev supervisor owns this project"));
    assert_eq!(counter(&client, &origin, false).await?.0, 1);

    let snapshot = root.join("checked-application");
    std::fs::copy(target.join("debug/native-dev-fixture"), &snapshot)?;
    check_packages(&snapshot, root, &project["primary"])?;

    std::fs::write(directory.join(".dev.vars"), "GREETING=\n")?;
    origin = dev.observed(&client, (1, "v1-one", ""), None).await?;
    assert_eq!(
        counter(&client, &origin, false).await?,
        (1, "v1-one".into(), "".into())
    );
    std::fs::write(directory.join(".dev.vars"), "COUNTERS=conflict\n")?;
    dev.failure(1).await?;
    assert_eq!(counter(&client, &origin, true).await?.0, 2);
    std::fs::write(directory.join(".dev.vars"), "GREETING=secret\n")?;
    origin = dev.observed(&client, (2, "v1-one", "secret"), None).await?;
    assert_eq!(
        counter(&client, &origin, false).await?,
        (2, "v1-one".into(), "secret".into())
    );

    project["primary"]["application_version"] = "unregistered".into();
    write_project(&project_path, &project)?;
    dev.failure(2).await?;
    assert_eq!(counter(&client, &origin, false).await?.0, 2);
    project["primary"]["application_version"] = "v1".into();
    write_project(&project_path, &project)?;
    let rebuilt = source.replace("|| application(\"v1\")", "|| application(\"compiled-two\")");
    anyhow::ensure!(
        rebuilt != source,
        "fixture must change the compiled application"
    );
    std::fs::write(
        &source_path,
        format!("{rebuilt}\ncompile_error!(\"deliberate dev build failure\");\n"),
    )?;
    dev.failure(3).await?;
    assert_eq!(counter(&client, &origin, false).await?.1, "v1-one");
    std::fs::write(&source_path, &rebuilt)?;
    origin = dev
        .observed(&client, (2, "compiled-two-one", "secret"), None)
        .await?;
    assert_eq!(
        counter(&client, &origin, false).await?,
        (2, "compiled-two-one".into(), "secret".into())
    );
    std::fs::write(assets.join("index.html"), "second assets")?;
    origin = dev
        .observed(
            &client,
            (2, "compiled-two-one", "secret"),
            Some("second assets"),
        )
        .await?;
    assert_eq!(
        client
            .get(format!("{origin}/"))
            .send()
            .await?
            .text()
            .await?,
        "second assets"
    );
    assert_eq!(counter(&client, &origin, false).await?.0, 2);
    std::fs::write(
        domain.join("src/lib.rs"),
        "pub const VALUE: &str = \"two\";\n",
    )?;
    origin = dev
        .observed(&client, (2, "compiled-two-two", "secret"), None)
        .await?;
    dev.stop().await?;
    assert!(client
        .get(format!("{origin}/.well-known/cellar/health"))
        .send()
        .await
        .is_err());

    // A restart uses Cellar's own state; state writes do not trigger builds.
    let mut resumed = Supervisor::start(root, &project_path, &target, "resume", &["--no-watch"])?;
    let resumed_origin = resumed.ready(1).await?;
    assert_eq!(counter(&client, &resumed_origin, false).await?.0, 2);
    std::fs::write(directory.join(".dev.vars"), "GREETING=unwatched\n")?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(counter(&client, &resumed_origin, false).await?.2, "secret");
    resumed.stop().await?;
    let mut clean = Supervisor::start(
        root,
        &project_path,
        &target,
        "clean",
        &["--clean", "--no-watch"],
    )?;
    let clean_origin = clean.ready(1).await?;
    assert_eq!(
        counter(&client, &clean_origin, false).await?,
        (0, "compiled-two-two".into(), "unwatched".into())
    );
    clean.stop().await?;
    Ok(())
}
