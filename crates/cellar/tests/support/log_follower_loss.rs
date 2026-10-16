//! Acknowledged end survives batch loss; no shortened tail certifies recovery.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_all_acknowledged_batches_cannot_certify_an_empty_witness() -> Result<()> {
    run(true, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_last_acknowledged_batch_cannot_certify_a_shorter_witness() -> Result<()> {
    run(false, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_last_acknowledged_batch_preserves_the_required_end() -> Result<()> {
    run(false, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_observed_loss_refuses_previously_proven_credit() -> Result<()> {
    for seal in [false, true] {
        let directory = tempfile::tempdir()?;
        let original = pair(
            &directory.path().join("fleet.sqlite"),
            directory.path(),
            "warm-damaged",
            LogOptions::default(),
        )
        .await?;
        let result = async {
            write(&original, 11, true).await?;
            write(&original, 22, false).await?;
            original
                .transport
                .follower_domain
                .run(async {
                    let follower = &original.transport.follower;
                    let request = TailReq {
                        leader: original.session.clone(),
                    };
                    let before = follower.tail(&request).await?;
                    ensure!(before.range.complete && before.entries.len() >= 2);
                    let state_end = before.range.end;
                    let epoch = before.range.fragment_epoch;
                    let root = directory
                        .path()
                        .join("follower-warm-damaged/peerlog")
                        .join(&original.session);
                    let mut paths = std::fs::read_dir(&root)?
                        .map(|entry| entry.map(|entry| entry.path()))
                        .collect::<std::io::Result<Vec<_>>>()?;
                    paths.retain(|path| path.extension().is_some_and(|suffix| suffix == "batch"));
                    ensure!(
                        paths.len() >= 2,
                        "independent writes did not leave distinct batches"
                    );
                    paths.sort_by_key(|path| {
                        path.file_stem()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .split('-')
                            .next()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap()
                    });
                    let path = paths.last().unwrap();
                    let saved = std::fs::read(path)?;
                    std::fs::remove_file(path)?;
                    if seal {
                        let reply = follower
                            .seal(&SealReq {
                                leader: original.session.clone(),
                                epoch,
                            })
                            .await?;
                        ensure!(reply.end == state_end && reply.fragment_epoch == 0);
                    } else {
                        let reply = follower.tail(&request).await?;
                        ensure!(reply.range.end == state_end && !reply.range.complete);
                    }
                    let state_bytes = std::fs::read(root.join("state.json"))?;
                    let repeated = before
                        .entries
                        .iter()
                        .map(|entry| cellar::node_log::Entry {
                            seq: entry.seq,
                            cell: entry.cell.clone(),
                            cell_epoch: entry.cell_epoch,
                            txid: entry.txid,
                            bytes: entry.bytes.clone(),
                        })
                        .collect();
                    for entries in [Vec::new(), repeated] {
                        let reply = follower
                            .append(cellar::node_log::AppendReq {
                                leader: original.session.clone(),
                                epoch,
                                truncate_to: 0,
                                entries,
                            })
                            .await;
                        ensure!(
                            !reply.ok && reply.end == state_end,
                            "warm known-damaged witness recredited proven data"
                        );
                    }
                    ensure!(std::fs::read(root.join("state.json"))? == state_bytes);
                    // Preserve the new seal and every original durable record;
                    // only return the exact missing batch before cold verification.
                    std::fs::write(path, saved)?;
                    let cold = FollowerStore::new(
                        &directory.path().join("follower-warm-damaged"),
                        Some(original.bucket.clone()),
                        &original.transport.follower_node,
                    );
                    let returned = cold.tail(&request).await?;
                    ensure!(encode_tail_resp(&returned) == encode_tail_resp(&before));
                    ensure!(std::fs::read(root.join("state.json"))? == state_bytes);
                    Ok::<_, anyhow::Error>(())
                })
                .await?
        }
        .await;
        original.domain.shutdown().await;
        original.transport.follower_domain.shutdown().await;
        result?;
    }
    Ok(())
}

async fn run(all: bool, corrupt: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let original = pair(
        &directory.path().join("fleet.sqlite"),
        directory.path(),
        "damaged",
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
    let end = proven.range.end;
    let epoch = proven.range.fragment_epoch;
    let repeated = proven
        .entries
        .iter()
        .map(|entry| cellar::node_log::Entry {
            seq: entry.seq,
            cell: entry.cell.clone(),
            cell_epoch: entry.cell_epoch,
            txid: entry.txid,
            bytes: entry.bytes.clone(),
        })
        .collect();
    let expected = encode_tail_resp(&proven);
    let bucket = original.bucket.clone();
    let node = original.transport.follower_node.clone();
    let root = directory.path().join("follower-damaged");
    let session = root.join("peerlog").join(&request.leader);
    original.domain.shutdown().await;
    original.transport.follower_domain.shutdown().await;
    drop(original);

    let mut batches = std::fs::read_dir(&session)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    batches.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "batch")
    });
    ensure!(
        batches.len() >= 2,
        "writes must use distinct committed batches"
    );
    batches.sort_by_key(|path| {
        path.file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .split('-')
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    });
    let selected = if all {
        &batches[..]
    } else {
        &batches[batches.len() - 1..]
    };
    let saved = selected
        .iter()
        .map(|path| Ok((path.clone(), std::fs::read(path)?)))
        .collect::<Result<Vec<_>>>()?;
    for (path, bytes) in &saved {
        if corrupt {
            let mut damaged = bytes.clone();
            *damaged.last_mut().unwrap() ^= 1;
            std::fs::write(path, damaged)?;
        } else {
            std::fs::remove_file(path)?;
        }
    }
    let cold = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = cold
        .run(async {
            let follower = FollowerStore::new(&root, Some(bucket.clone()), &node);
            let tail = follower.tail(&request).await?;
            ensure!(
                tail.range.end == end && !tail.range.complete,
                "lost acknowledged end: original={end}, recovered={:?}",
                tail.range
            );
            let duplicate = follower
                .append(cellar::node_log::AppendReq {
                    leader: request.leader.clone(),
                    epoch,
                    entries: repeated,
                    truncate_to: 0,
                })
                .await;
            ensure!(
                !duplicate.ok,
                "a damaged cold witness acknowledged a duplicate without its bytes"
            );
            let probe = follower
                .append(cellar::node_log::AppendReq {
                    leader: request.leader.clone(),
                    epoch,
                    entries: vec![],
                    truncate_to: 0,
                })
                .await;
            ensure!(
                !probe.ok,
                "a damaged cold witness acknowledged an empty probe"
            );
            let seal = follower
                .seal(&SealReq {
                    leader: request.leader.clone(),
                    epoch,
                })
                .await?;
            ensure!(
                seal.end == end
                    && seal.fragment_epoch == 0
                    && seal.held_fragment_epoch == Some(epoch),
                "damaged witness supplied a complete seal certificate"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    cold.shutdown().await;
    result?;

    // The seal remains persisted. Return only the original committed bytes;
    // a new store must recover the exact range without rolling back state.json.
    let state = std::fs::read(session.join("state.json"))?;
    for (path, bytes) in saved {
        std::fs::write(path, bytes)?;
    }
    ensure!(std::fs::read(session.join("state.json"))? == state);
    let returned = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = returned
        .run(async {
            let follower = FollowerStore::new(&root, Some(bucket), &node);
            let tail = follower.tail(&request).await?;
            ensure!(
                encode_tail_resp(&tail) == expected,
                "returning original bytes changed the accepted prefix"
            );
            let seal = follower
                .seal(&SealReq {
                    leader: request.leader,
                    epoch,
                })
                .await?;
            ensure!(seal.end == end && seal.fragment_epoch == epoch);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    returned.shutdown().await;
    result
}
