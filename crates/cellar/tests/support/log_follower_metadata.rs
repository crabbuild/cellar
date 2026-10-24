// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Corrupt metadata cannot hide acknowledged rows or reset a persisted seal.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflated_follower_base_cannot_hide_retained_acknowledged_rows() -> Result<()> {
    run("base").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_follower_state_cannot_become_an_empty_fragment() -> Result<()> {
    run("malformed").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_follower_state_with_committed_batches_refuses_before_mutation() -> Result<()> {
    run("missing").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_valid_follower_state_cannot_reset_a_retained_seal() -> Result<()> {
    run("foreign").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_follower_cannot_reuse_a_valid_session_state() -> Result<()> {
    run("node").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unchecksummed_follower_state_has_no_legacy_reader() -> Result<()> {
    run("legacy").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_follower_state_format_refuses_before_mutation() -> Result<()> {
    run("format").await
}

async fn run(fault: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let original = pair(
        &directory.path().join("fleet.sqlite"),
        directory.path(),
        "metadata",
        LogOptions::default(),
    )
    .await?;
    write(&original, 11, true).await?;
    write(&original, 22, false).await?;
    let request = TailReq {
        leader: original.session.clone(),
    };
    let proven = original
        .transport
        .follower_domain
        .run(original.transport.follower.tail(&request))
        .await??;
    ensure!(proven.range.complete && proven.entries.len() >= 2);
    let epoch = proven.range.fragment_epoch;
    if fault == "foreign" {
        let sealed = original
            .transport
            .follower_domain
            .run(original.transport.follower.seal(&SealReq {
                leader: request.leader.clone(),
                epoch,
            }))
            .await??;
        ensure!(sealed.fragment_epoch == epoch);
    }
    let bucket = original.bucket.clone();
    let node = original.transport.follower_node.clone();
    let root = directory.path().join("follower-metadata");
    let session = root.join("peerlog").join(&request.leader);
    original.domain.shutdown().await;
    original.transport.follower_domain.shutdown().await;
    drop(original);
    let path = session.join("state.json");
    let saved = std::fs::read(&path)?;
    let batches = std::fs::read_dir(&session)?
        .map(|entry| {
            let path = entry?.path();
            Ok((path.clone(), std::fs::read(path)?))
        })
        .collect::<Result<Vec<_>>>()?;
    match fault {
        "base" => {
            let mut value: serde_json::Value = serde_json::from_slice(&saved)?;
            let state = if value.get("state").is_some() {
                &mut value["state"]
            } else {
                &mut value
            };
            state["base"] = state["end"].clone();
            std::fs::write(&path, serde_json::to_vec(&value)?)?;
        }
        "malformed" => std::fs::write(&path, b"{")?,
        "missing" => std::fs::remove_file(&path)?,
        "node" => {}
        "legacy" => {
            let value: serde_json::Value = serde_json::from_slice(&saved)?;
            std::fs::write(&path, serde_json::to_vec(&value["state"])?)?;
        }
        "format" => {
            let mut value: serde_json::Value = serde_json::from_slice(&saved)?;
            value["format"] = 2.into();
            std::fs::write(&path, serde_json::to_vec(&value)?)?;
        }
        "foreign" => {
            let foreign = pair(
                &directory.path().join("foreign.sqlite"),
                directory.path(),
                "foreign",
                LogOptions::default(),
            )
            .await?;
            write(&foreign, 33, true).await?;
            let foreign_path = directory
                .path()
                .join("follower-foreign/peerlog")
                .join(&foreign.session)
                .join("state.json");
            foreign.domain.shutdown().await;
            foreign.transport.follower_domain.shutdown().await;
            drop(foreign);
            std::fs::copy(foreign_path, &path)?;
        }
        _ => anyhow::bail!("unknown metadata fault"),
    }
    let damaged = std::fs::read(&path).ok();
    let cold = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = cold
        .run(async {
            let follower = FollowerStore::new(
                &root,
                Some(bucket.clone()),
                if fault == "node" {
                    "wrong-follower"
                } else {
                    &node
                },
            );
            for _ in 0..2 {
                let tail = follower.tail(&request).await;
                ensure!(
                    tail.is_err(),
                    "{fault} metadata supplied a readable follower tail: {:?}",
                    tail.as_ref().ok().map(|tail| &tail.range)
                );
                ensure!(
                    follower
                        .seal(&SealReq {
                            leader: request.leader.clone(),
                            epoch
                        })
                        .await
                        .is_err(),
                    "{fault} metadata supplied a seal answer"
                );
                let answer = follower
                    .append(cellar::node_log::AppendReq {
                        leader: request.leader.clone(),
                        epoch,
                        truncate_to: 0,
                        entries: vec![],
                    })
                    .await;
                ensure!(!answer.ok, "{fault} metadata re-credited a follower probe");
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    cold.shutdown().await;
    result?;
    ensure!(
        std::fs::read(&path).ok() == damaged,
        "refusal rewrote damaged state metadata"
    );
    for (path, bytes) in &batches {
        if path.file_name().unwrap() != "state.json" {
            ensure!(
                std::fs::read(path)? == *bytes,
                "refusal altered original committed batches"
            );
        }
    }
    // The refused store wrote nothing; original valid metadata can return
    // without rolling back a newer seal. Original data is never replayed.
    std::fs::write(&path, saved)?;
    let returned = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = returned
        .run(async {
            let follower = FollowerStore::new(&root, Some(bucket), &node);
            ensure!(
                encode_tail_resp(&follower.tail(&request).await?) == encode_tail_resp(&proven),
                "original metadata did not recover the accepted prefix"
            );
            let seal = follower
                .seal(&SealReq {
                    leader: request.leader,
                    epoch,
                })
                .await?;
            ensure!(seal.fragment_epoch == epoch && seal.end == proven.range.end);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    returned.shutdown().await;
    result
}
