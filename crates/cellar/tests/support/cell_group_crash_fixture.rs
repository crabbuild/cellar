// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned crash children and bounded synthetic catalog snapshots for native tests.
//! These objects are fixture inputs, never a production recovery protocol.
use super::*;
use futures_util::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::{io::Write, path::Path};

struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub(super) async fn kill_at(root: &Path, fixture: &str, rooted: bool, cut: &str) -> Result<()> {
    let output = root.join("child.log");
    let file = std::fs::File::create(&output)?;
    let mut child = Child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                fixture,
                "--nocapture",
                "--test-threads=1",
            ])
            .env("CELLAR_GROUP_CRASH_ROOT", root)
            .env(
                "CELLAR_GROUP_CRASH_MODE",
                if rooted { "extents" } else { "materialized" },
            )
            .env("CELLAR_GROUP_CRASH_CUT", cut)
            .stdout(file.try_clone()?)
            .stderr(file)
            .spawn()?,
    );
    let wait = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let reached = match std::fs::read(root.join("signal")) {
                Ok(bytes) => bytes == b"reached",
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            };
            if reached {
                break;
            }
            if let Some(status) = child.0.try_wait()? {
                anyhow::bail!("child exited before {cut}: {status}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    let reached = wait.map_err(anyhow::Error::from).and_then(|r| r);
    reached.with_context(|| {
        format!(
            "{rooted}/{cut}: {}",
            std::fs::read_to_string(&output).unwrap_or_default()
        )
    })?;
    assert_eq!(std::fs::read(root.join("signal"))?, b"reached");
    child.0.kill()?;
    let status = child.0.wait()?;
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
    assert!(!status.success());
    Ok(())
}

pub(super) fn save(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

pub(super) async fn stop_at(root: &Path) -> Result<()> {
    save(&root.join("signal"), b"reached")?;
    std::future::pending::<()>().await;
    unreachable!("parent kills the held child")
}

pub(super) fn parameters() -> Result<(std::path::PathBuf, bool, String)> {
    let root = std::path::PathBuf::from(std::env::var("CELLAR_GROUP_CRASH_ROOT")?);
    let rooted = match std::env::var("CELLAR_GROUP_CRASH_MODE")?.as_str() {
        "materialized" => false,
        "extents" => true,
        _ => anyhow::bail!("invalid crash image mode"),
    };
    let cut = std::env::var("CELLAR_GROUP_CRASH_CUT")?;
    anyhow::ensure!(
        root.is_dir() && !root.join("signal").exists(),
        "fresh owned parent directory required"
    );
    Ok((root, rooted, cut))
}

pub(super) async fn capture_catalog(store: &dyn ObjectStore) -> Result<Vec<(String, Vec<u8>)>> {
    let mut objects = Vec::new();
    let listed = store.list(None).try_collect::<Vec<_>>().await?;
    anyhow::ensure!(listed.len() <= 128, "bounded fixture catalog");
    let mut bytes = 0;
    for item in listed {
        let body = store.get(&item.location).await?.bytes().await?.to_vec();
        bytes += body.len();
        anyhow::ensure!(bytes < 1024 * 1024, "bounded fixture catalog bytes");
        objects.push((item.location.to_string(), body));
    }
    Ok(objects)
}

pub(super) async fn restore_catalog(
    objects: Vec<(String, Vec<u8>)>,
    route: &GroupRoute,
    domain: &ExecutionDomain,
) -> Result<(RangeDirectory, CellGroupBirth)> {
    let store = Arc::new(object_store::memory::InMemory::new());
    for (name, bytes) in objects {
        store.put(&ObjectPath::from(name), bytes.into()).await?;
    }
    let catalog = Catalog::new(store, CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.open_namespace(NamespaceConfig::new(
            route.history.cluster,
            route.history.namespace,
            cellar_logic::NamespaceMode::Raft,
            route.schema_digest,
        )))
        .await??;
    let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let birth = domain
        .run(directory.materialize_selected_group(route))
        .await??;
    Ok((directory, birth))
}
