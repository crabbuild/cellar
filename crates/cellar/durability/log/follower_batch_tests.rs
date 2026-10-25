// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Group-commit admission must bound bytes as well as frame count.
use super::*;
use cellar_ltx::{DirectFileSystem, FileSystem};
use tokio::io::AsyncWriteExt as _;

const LEADER: &str = "leader/11111111111111111111111111111111/generation";

fn request(seq: u64, payload_bytes: usize) -> AppendReq {
    AppendReq {
        leader: LEADER.into(),
        epoch: 7,
        truncate_to: 0,
        entries: vec![Entry {
            seq,
            cell: "cell".into(),
            cell_epoch: 1,
            txid: seq,
            bytes: vec![0; payload_bytes],
        }],
    }
}

#[tokio::test]
async fn group_commit_returns_the_unsent_frame_at_its_byte_budget() -> anyhow::Result<()> {
    // CLE1 + seq + cell length/bytes + epoch + TXID + payload length = 38.
    // Both frames fit the wire cap individually; together their entry bytes
    // fill the 64 MiB budget. No format limit may discard the next frame.
    let mut batch = AppendBatch::new(request(1, (32 << 20) - 38));
    assert!(batch.try_push(request(2, (32 << 20) - 38)).is_ok());
    let mut next = request(3, 4);
    next.entries[0].bytes = b"next".to_vec();
    let returned = batch
        .try_push(next)
        .expect_err("group commit admitted more than 64 MiB of encoded entries");
    assert_eq!(batch.frame_count(), 2);
    assert_eq!(batch.entry_count(), 2);
    assert_eq!(returned.leader, LEADER);
    assert_eq!(returned.epoch, 7);
    assert_eq!(returned.entries[0].seq, 3);
    assert_eq!(returned.entries[0].bytes, b"next");

    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = FollowerStore::new(directory.path(), None, "witness");
            store.persist(
                LEADER,
                FollowerState {
                    fragment_epoch: 7,
                    base: 0,
                    end: 0,
                    sealed_to: 0,
                },
            )?;
            // Preload all three wire frames before serving. The ordered
            // reader must split this already-delivered burst at its byte
            // budget, carry the exact third frame and answer in order.
            let (mut client, server) = tokio::io::duplex(70 << 20);
            for frame in batch.frames.into_iter().chain([returned]) {
                write_frame(&mut client, &encode_append(&frame)).await?;
            }
            client.shutdown().await?;
            let receive = async {
                for end in [2, 2, 3] {
                    let payload = read_frame(&mut client)
                        .await?
                        .expect("ordered stream closed before every reply");
                    let reply: AppendResp = serde_json::from_slice(&payload)?;
                    assert!(reply.ok && reply.end == end && reply.epoch == Some(7));
                }
                anyhow::Ok(())
            };
            let (served, received) =
                futures_util::future::join(serve_log_stream(server, Arc::new(store)), receive)
                    .await;
            served?;
            received?;
            let batch_path = directory
                .path()
                .join("peerlog")
                .join(LEADER)
                .join("1-2.batch");
            assert_eq!(DirectFileSystem.metadata(&batch_path)?.len, (64 << 20) + 72);
            let cold = FollowerStore::new(directory.path(), None, "witness");
            let tail = cold
                .tail(&TailReq {
                    leader: LEADER.into(),
                })
                .await?;
            assert!(tail.range.complete);
            assert_eq!(tail.range.end, 3);
            assert_eq!(tail.entries.len(), 3);
            for (index, entry) in tail.entries[..2].iter().enumerate() {
                assert_eq!(entry.seq, index as u64 + 1);
                assert_eq!(entry.cell, "cell");
                assert_eq!(entry.bytes.len(), (32 << 20) - 38);
                assert!(entry.bytes.iter().all(|byte| *byte == 0));
            }
            assert_eq!(tail.entries[2].seq, 3);
            assert_eq!(tail.entries[2].bytes, b"next");
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[test]
fn group_commit_bounds_empty_frames_too() {
    let empty = || AppendReq {
        leader: LEADER.into(),
        epoch: 7,
        truncate_to: 0,
        entries: vec![],
    };
    let mut batch = AppendBatch::new(empty());
    for _ in 1..64 {
        assert!(batch.try_push(empty()).is_ok());
    }
    assert!(
        batch.try_push(empty()).is_err(),
        "public batch admission bypassed the 64-frame serve bound"
    );
    assert_eq!(batch.frame_count(), 64);
}
