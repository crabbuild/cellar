//! Hard-cutover log wire, lease retirement, and caller-owned HTTP pools.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::bucket::Bucket;
use cellar::dead_node_gc::{live_nodes, DeadNodeGc};
use cellar::node_log::{
    decode_tail_resp, encode_entry, encode_tail_resp, read_frame, write_frame, AppendResp, Entry,
    FollowerState, FollowerStore, SealResp, TailRange, TailReq, TailResp, TailWireReq,
};
use cellar::ownership_store::BucketOwnership;
use cellar::peer_auth::{
    PeerAuth, PROTOCOL_VERSION, PROTOCOL_VERSION_TEXT, RESPONSE_VERSION_HEADER,
};
use cellar::sdk::{ExecutionDomain, LogTransport, SignedPeerTransport};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
async fn log_wire_requires_current_shapes_and_complete_frames() -> Result<()> {
    let response = TailResp {
        range: TailRange {
            fragment_epoch: 7,
            base: 4,
            end: 4,
            complete: true,
        },
        entries: Vec::new(),
    };
    let mut wire = encode_tail_resp(&response);
    assert_eq!(&wire[..4], b"CLT1");
    assert_eq!(decode_tail_resp(&wire)?.range, response.range);
    wire[..4].copy_from_slice(b"CLT2");
    ensure!(decode_tail_resp(&wire).is_err());
    let mut entries_only = b"CLT1".to_vec();
    entries_only.extend_from_slice(&0_u32.to_le_bytes());
    ensure!(decode_tail_resp(&entries_only).is_err());
    let request = TailWireReq::ranged(TailReq {
        leader: "leader/00000000000000000000000000000001/generation".into(),
    });
    let encoded = serde_json::to_value(request)?;
    assert_eq!(encoded["tail_format"], 1);
    for marker in [0, 2, 255] {
        let mut unsupported = encoded.clone();
        unsupported["tail_format"] = marker.into();
        ensure!(serde_json::from_value::<TailWireReq>(unsupported).is_err());
    }
    for request in [
        r#"{"leader":"leader/00000000000000000000000000000001/generation"}"#,
        r#"{"leader":"leader","tail_format":1}"#,
        r#"{"leader":"leader/generation","tail_format":1}"#,
        r#"{"leader":"leader/not-a-lineage/generation","tail_format":1}"#,
    ] {
        ensure!(serde_json::from_str::<TailWireReq>(request).is_err());
    }
    serde_json::from_str::<TailWireReq>(
        r#"{"leader":"leader/00000000000000000000000000000001/generation","tail_format":1}"#,
    )?;
    ensure!(serde_json::from_str::<AppendResp>(r#"{"ok":true,"end":4}"#).is_err());
    serde_json::from_str::<AppendResp>(r#"{"ok":false,"end":0,"epoch":null}"#)?;
    for response in [
        r#"{"end":4,"fragment_epoch":7,"held_fragment_epoch":7}"#,
        r#"{"end":4,"base":0,"fragment_epoch":7}"#,
    ] {
        ensure!(serde_json::from_str::<SealResp>(response).is_err());
    }
    let mut empty: &[u8] = &[];
    assert!(read_frame(&mut empty).await?.is_none());
    for torn in [&[3_u8][..], &[3, 0, 0][..], &[3, 0, 0, 0, 1, 2][..]] {
        let mut input = torn;
        ensure!(
            read_frame(&mut input).await.is_err(),
            "accepted a torn frame"
        );
    }
    let mut input: &[u8] = &[3, 0, 0, 0, 1, 2, 3];
    assert_eq!(read_frame(&mut input).await?, Some(vec![1, 2, 3]));
    Ok(())
}

fn lease(node: &str, expiry: u64) -> serde_json::Value {
    serde_json::json!({
        "node": node, "expires_ms": expiry, "generation": "process-generation", "lineage": "00000000000000000000000000000001",
        "addr": "127.0.0.1:1234", "probe_public_key": "process-generation",
        "peer_protocol": PROTOCOL_VERSION, "paced_handoff": true,
        "application_metadata": {"preserve": 17}
    })
}

#[tokio::test]
async fn follower_gc_discovers_current_sessions_and_requires_closed_authority() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            let store =
                FollowerStore::new(directory.path(), Some(Arc::new(bucket.clone())), "observer");
            let seed = FollowerState {
                fragment_epoch: 1,
                base: 0,
                end: 0,
                sealed_to: 0,
            };
            let flat_path = directory.path().join("peerlog/flat/state.json");
            std::fs::create_dir_all(flat_path.parent().unwrap())?;
            let flat_bytes = serde_json::to_vec(&seed)?;
            std::fs::write(&flat_path, &flat_bytes)?;
            ensure!(
                store.load("flat").is_err(),
                "native access read a former flat session"
            );
            ensure!(
                store.persist("flat", seed).is_err(),
                "native access wrote a former flat session"
            );
            for (node, state) in [("closed", "sealed"), ("active", "open")] {
                let mut record = lease(node, 0);
                record["log"] = serde_json::json!({
                    "state":state, "epoch":1, "ensemble":[], "tiered":0,
                    "may_have_fleet_acks":false, "claimant":null, "claimed_ms":null
                });
                bucket
                    .put(&format!("nodes/{node}.json"), serde_json::to_vec(&record)?)
                    .await?;
                store.persist(
                    &format!("{node}/00000000000000000000000000000001/process-generation"),
                    seed,
                )?;
            }
            store.gc_fragments().await;
            ensure!(
                std::fs::read(&flat_path)? == flat_bytes,
                "GC rewrote unsupported development state"
            );
            let closed = store
                .tail(&TailReq {
                    leader: "closed/00000000000000000000000000000001/process-generation".into(),
                })
                .await?;
            let active = store
                .tail(&TailReq {
                    leader: "active/00000000000000000000000000000001/process-generation".into(),
                })
                .await?;
            ensure!(closed.range.complete && active.range.complete);
            let closed_path = directory.path().join(
                "peerlog/closed/00000000000000000000000000000001/process-generation/state.json",
            );
            let active_path = directory.path().join(
                "peerlog/active/00000000000000000000000000000001/process-generation/state.json",
            );
            let closed: serde_json::Value = serde_json::from_slice(&std::fs::read(closed_path)?)?;
            let active: serde_json::Value = serde_json::from_slice(&std::fs::read(active_path)?)?;
            ensure!(
                closed["state"]["sealed_to"] == 1 && active["state"]["sealed_to"] == 0,
                "GC ignored current sessions or closed an active fragment"
            );
            for (generation, name) in [("entry", "1.entry"), ("entries", "1-1.entries")] {
                let session = format!("unsupported/00000000000000000000000000000001/{generation}");
                store.persist(&session, FollowerState { end: 1, ..seed })?;
                let mut bytes = Vec::new();
                encode_entry(
                    &Entry {
                        seq: 1,
                        cell: "cell".into(),
                        cell_epoch: 1,
                        txid: 1,
                        bytes: b"unsupported development payload".to_vec(),
                    },
                    &mut bytes,
                );
                let path = directory.path().join("peerlog").join(&session).join(name);
                std::fs::write(&path, &bytes)?;
                ensure!(
                    store
                        .tail(&TailReq {
                            leader: session.clone()
                        })
                        .await
                        .is_err(),
                    "old entry bytes supplied a tail"
                );
                let cold = FollowerStore::new(directory.path(), None, "observer");
                ensure!(
                    cold.load(&session).is_err(),
                    "cold restore accepted an old entry layout"
                );
                ensure!(
                    std::fs::read(path)? == bytes,
                    "unsupported entry bytes were migrated"
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modern_leases_keep_recovery_pointers_and_cas_tombstones() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .run(async {
            let bucket = Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
            let now = cellar::ownership_store::now_ms();
            let expiry = now - 1;
            for (node, state) in [
                ("plain", None),
                ("sealed", Some("sealed")),
                ("open", Some("open")),
                ("recovering", Some("recovering")),
            ] {
                let mut record = lease(node, expiry);
                if let Some(state) = state {
                    record["log"] = serde_json::json!({
                        "state": state, "epoch": 2, "ensemble": ["follower"],
                        "tiered": 0, "may_have_fleet_acks": true,
                        "claimant": null, "claimed_ms": null
                    });
                }
                bucket
                    .put(&format!("nodes/{node}.json"), serde_json::to_vec(&record)?)
                    .await?;
            }
            let live = lease("live", now + 60_000);
            bucket
                .put("nodes/live.json", serde_json::to_vec(&live)?)
                .await?;
            // The lease reader refuses the original generation alias and marker.
            let ownership = BucketOwnership::new(
                bucket.clone(),
                bucket.clone(),
                "reader".into(),
                "reader-generation".into(),
            );
            let modern = lease("old", expiry);
            for lineage in [
                serde_json::Value::Null,
                serde_json::json!("../unsafe"),
                serde_json::json!("A".repeat(32)),
            ] {
                let mut invalid = modern.clone();
                invalid["lineage"] = lineage.clone();
                bucket
                    .put("nodes/old.json", serde_json::to_vec(&invalid)?)
                    .await?;
                ensure!(ownership.read_node_lease("old").await.is_err());
                let owner = serde_json::json!({"node":"old", "epoch":1, "lineage":lineage});
                bucket
                    .put("cells/old/own.json", serde_json::to_vec(&owner)?)
                    .await?;
                ensure!(ownership.read_owner("old").await.is_err());
            }
            let mut old = modern.clone();
            old.as_object_mut().unwrap().remove("generation");
            old["ownership_index_generation"] = serde_json::json!("process-generation");
            bucket
                .put("nodes/old.json", serde_json::to_vec(&old)?)
                .await?;
            ensure!(ownership.read_node_lease("old").await.is_err());
            let mut old_log = lease("old-log", expiry);
            old_log["log"] = serde_json::json!({"state":"open", "epoch":1,
            "ensemble":["follower"], "tiered":0, "active":true});
            bucket
                .put("nodes/old-log.json", serde_json::to_vec(&old_log)?)
                .await?;
            ensure!(ownership.read_node_lease("old-log").await.is_err());
            assert_eq!(
                live_nodes(&bucket, now).await,
                ["live".into()].into_iter().collect()
            );
            let mut gc = DeadNodeGc;
            assert!(gc
                .run_elected_pass(&bucket, "collector", 1_000)
                .await
                .is_some());
            for (node, retired) in [
                ("plain", true),
                ("sealed", true),
                ("open", false),
                ("recovering", false),
            ] {
                let (body, _) = bucket.get(&format!("nodes/{node}.json")).await?.unwrap();
                let wire: serde_json::Value = serde_json::from_slice(&body)?;
                assert_eq!(wire["expires_ms"], if retired { 0 } else { expiry });
                assert_eq!(wire["generation"], "process-generation");
                assert_eq!(wire["lineage"], "00000000000000000000000000000001");
                assert_eq!(wire["application_metadata"]["preserve"], 17);
                if node != "plain" {
                    assert_eq!(wire["log"]["may_have_fleet_acks"], true);
                }
            }
            // A successor installs through the tombstone CAS. Repeated retirement
            // cannot delete it or shorten its new, live lease.
            let (_, token) = bucket.get("nodes/plain.json").await?.unwrap();
            let mut successor = lease("plain", now + 60_000);
            successor["generation"] = serde_json::json!("successor-generation");
            assert!(bucket
                .put_cas(
                    "nodes/plain.json",
                    serde_json::to_vec(&successor)?,
                    Some(&token)
                )
                .await?
                .is_some());
            assert!(gc
                .run_elected_pass(&bucket, "collector", 1_000)
                .await
                .is_some());
            let (body, _) = bucket.get("nodes/plain.json").await?.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body)?,
                successor
            );
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    domain.shutdown().await;
    Ok(())
}

struct Probe {
    auth: PeerAuth,
    http: AtomicUsize,
    streams: AtomicUsize,
}

async fn receive(
    axum::extract::State(probe): axum::extract::State<Arc<Probe>>,
    mut request: axum::extract::Request,
) -> axum::response::Response {
    use axum::{body::Body, http::StatusCode};
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    let headers = request.headers().clone();
    let upgrade = hyper::upgrade::on(&mut request);
    let body = axum::body::to_bytes(request.into_body(), 1 << 20)
        .await
        .unwrap();
    if let Err(error) = probe
        .auth
        .verify(&method, &path, &headers, &body, "follower")
    {
        return axum::http::Response::builder()
            .status(error.status())
            .body(Body::empty())
            .unwrap();
    }
    let mut response = axum::http::Response::builder();
    if path != "/missing-version" {
        response = response.header(RESPONSE_VERSION_HEADER, PROTOCOL_VERSION_TEXT);
    }
    if matches!(path.as_str(), "/peer/log/stream" | "/peer/log/tail") {
        assert_eq!(headers["x-caller-pool"], "streams");
        assert_eq!(headers["upgrade"], "cellar-log-stream");
        probe.streams.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let mut io = hyper_util::rt::TokioIo::new(upgrade.await.unwrap());
            let frame = read_frame(&mut io).await.unwrap().unwrap();
            write_frame(&mut io, &frame).await.unwrap();
        });
        response
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header("connection", "upgrade")
            .header("upgrade", "cellar-log-stream")
            .body(Body::empty())
            .unwrap()
    } else {
        assert_eq!(headers["x-caller-pool"], "requests");
        probe.http.fetch_add(1, Ordering::SeqCst);
        response.body(Body::from(body)).unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_transport_retains_both_caller_pools_and_authenticates_requests() -> Result<()> {
    let key = [42; 32];
    let probe = Arc::new(Probe {
        auth: PeerAuth::new(key, "follower")?,
        http: AtomicUsize::new(0),
        streams: AtomicUsize::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let router = axum::Router::new()
        .fallback(receive)
        .with_state(probe.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let client = |pool: &'static str| {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "x-caller-pool",
            reqwest::header::HeaderValue::from_static(pool),
        );
        reqwest::Client::builder()
            .http1_only()
            .no_proxy()
            .default_headers(headers)
            .build()
    };
    let transport = SignedPeerTransport::from_clients(
        client("requests")?,
        client("streams")?,
        Arc::new(PeerAuth::new(key, "leader")?),
    );
    let response = transport
        .post(
            "follower",
            &addr,
            "/peer/log/append",
            b"body".to_vec(),
            Some(Duration::from_secs(2)),
        )
        .await?;
    assert_eq!(response.as_ref(), b"body");
    ensure!(transport
        .post(
            "follower",
            &addr,
            "/missing-version",
            Vec::new(),
            Some(Duration::from_secs(2))
        )
        .await
        .is_err());
    for path in ["/peer/log/stream", "/peer/log/tail"] {
        let mut io = tokio::time::timeout(
            Duration::from_secs(2),
            transport.open_stream("follower", &addr, path),
        )
        .await??;
        write_frame(&mut io, b"ordered").await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), read_frame(&mut io)).await??,
            Some(b"ordered".to_vec())
        );
    }
    let bad = SignedPeerTransport::from_clients(
        client("requests")?,
        client("streams")?,
        Arc::new(PeerAuth::new([43; 32], "leader")?),
    );
    ensure!(bad
        .post(
            "follower",
            &addr,
            "/peer/log/append",
            Vec::new(),
            Some(Duration::from_secs(2))
        )
        .await
        .is_err());
    assert_eq!(probe.http.load(Ordering::SeqCst), 2);
    assert_eq!(probe.streams.load(Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;
    Ok(())
}

#[tokio::test]
async fn a_new_lineage_cannot_certify_or_collect_an_old_follower_session() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async {
        let bucket = Arc::new(Bucket::open_dev(&directory.path().join("objects.sqlite"))?);
        let store = FollowerStore::new(directory.path(), Some(bucket.clone()), "observer");
        let mut original = lease("leader", u64::MAX);
        original["log"] = serde_json::json!({"state":"open", "epoch":1, "ensemble":["observer"], "tiered":0, "may_have_fleet_acks":true, "claimant":null, "claimed_ms":null});
        bucket.put("nodes/leader.json", serde_json::to_vec(&original)?).await?;
        let session = "leader/00000000000000000000000000000001/process-generation";
        let accepted = store.append(cellar::node_log::AppendReq { leader: session.into(), epoch:1, truncate_to:0, entries:vec![Entry { seq:1, cell:"cell".into(), cell_epoch:1, txid:1, bytes:b"retained proof".to_vec() }] }).await;
        ensure!(accepted.ok, "original follower append was not proven");
        let before = store.tail(&TailReq { leader: session.into() }).await?;
        ensure!(before.range.complete && before.entries.len() == 1);
        let mut fresh = original.clone();
        fresh["lineage"] = "00000000000000000000000000000002".into();
        fresh["log"]["state"] = "sealed".into();
        bucket.put("nodes/leader.json", serde_json::to_vec(&fresh)?).await?;
        store.gc_fragments().await;
        let after = store.tail(&TailReq { leader: session.into() }).await?;
        ensure!(after.range == before.range && after.entries.len() == 1 && after.entries[0].bytes == before.entries[0].bytes, "another lineage discarded the retained proof");
        ensure!(store.load(session)?.sealed_to == 0, "a foreign lineage sealed the old fragment");
        Ok::<_, anyhow::Error>(())
    }).await?;
    domain.shutdown().await;
    result
}
