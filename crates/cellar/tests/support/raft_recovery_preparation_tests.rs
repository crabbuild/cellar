// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::Arc;
use tokio::sync::oneshot;

#[tokio::test]
async fn input_survives_pending_markers_and_is_retired_after_their_join() -> Result<()> {
    let root = tempfile::tempdir()?;
    let template = root.path().join("template.sqlite");
    std::fs::write(&template, b"private prepared input")?;
    let (entered, entered_wait) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let prepare: PreloadReplicas = Box::new(move |_, _| {
        Box::pin(async move {
            let _ = entered.send(());
            released.await?;
            Ok(())
        })
    });
    let prepared = retire_after_preload(template.clone(), prepare);
    let task = tokio::spawn(prepared(
        vec![],
        Arc::new(object_store::memory::InMemory::new()),
    ));
    entered_wait.await?;
    assert!(template.is_file(), "pending native preload lost its input");
    let _ = release.send(());
    task.await??;
    assert!(
        !template.try_exists()?,
        "joined preload retained its full input"
    );
    Ok(())
}

#[tokio::test]
async fn failed_preload_keeps_the_input_and_failed_retirement_refuses_startup() -> Result<()> {
    let root = tempfile::tempdir()?;
    let template = root.path().join("template.sqlite");
    std::fs::write(&template, b"private prepared input")?;
    let failed: PreloadReplicas =
        Box::new(|_, _| Box::pin(async { anyhow::bail!("marker not synced") }));
    let result = retire_after_preload(template.clone(), failed)(
        vec![],
        Arc::new(object_store::memory::InMemory::new()),
    )
    .await;
    assert!(result.is_err());
    assert!(template.is_file(), "unconfirmed marker deleted its input");
    let directory = root.path().join("not-a-file");
    std::fs::create_dir(&directory)?;
    let complete: PreloadReplicas = Box::new(|_, _| Box::pin(async { Ok(()) }));
    let result = retire_after_preload(directory.clone(), complete)(
        vec![],
        Arc::new(object_store::memory::InMemory::new()),
    )
    .await;
    assert!(result.is_err(), "failed retirement allowed runtime startup");
    assert!(directory.is_dir());
    Ok(())
}
