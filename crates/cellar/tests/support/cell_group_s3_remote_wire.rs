// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A deliberately slow independent TLS reader holds an actual response write.
use super::super::super::super::network_probes::Connection;
use super::*;
use serde::Serialize;

#[derive(Serialize)]
struct Endpoint<'a> {
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    label: &'a str,
    member: Option<String>,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "identity")]
enum Scope<'a> {
    Shared {
        group: [[u8; 16]; 4],
        cell: &'a str,
        incarnation: [u8; 16],
    },
}

#[derive(Serialize)]
struct Route<'a> {
    purpose: &'a str,
    scope: Scope<'a>,
    schema: [u8; 32],
    source: Endpoint<'a>,
    target: Endpoint<'a>,
}

#[derive(Serialize)]
struct Header<'a> {
    route: Route<'a>,
    sequence: &'a str,
    digest: [u8; 32],
}

fn header(h: &Harness, remote: &Remote, kind: &str, body: &[u8]) -> Result<Vec<u8>> {
    let g = h.route.history;
    let target = h.fixture.cluster.voters[0]
        .log
        .cell_group(&g)?
        .identity()
        .clone();
    let source = &remote.router.peer;
    let route = Route {
        purpose: if kind == "command" {
            "Command"
        } else {
            "Receipt"
        },
        scope: Scope::Shared {
            group: [
                g.cluster.bytes(),
                g.namespace.bytes(),
                g.group.bytes(),
                g.incarnation.bytes(),
            ],
            cell: h.cell.cell.as_str(),
            incarnation: h.cell.incarnation.bytes(),
        },
        schema: h.class.schema_digest(),
        source: Endpoint {
            node: source.node.bytes(),
            disk: source.disk.bytes(),
            process: source.process.bytes(),
            label: &source.label,
            member: None,
        },
        target: Endpoint {
            node: target.node.bytes(),
            disk: target.disk.bytes(),
            process: target.process.bytes(),
            label: "node-1",
            member: Some(target.member.get().to_string()),
        },
    };
    Ok(serde_json::to_vec(&Header {
        route,
        sequence: "1",
        digest: Sha256::digest(body).into(),
    })?)
}

fn request(h: &Harness, kind: &str) -> Result<Vec<u8>> {
    let request = command_request(h, 1, b"held", Durability::S3)?;
    let mut body = if kind == "command" {
        b"CLGCCQ01".to_vec()
    } else {
        b"CLGCRQ01".to_vec()
    };
    body.extend_from_slice(&request.id.client.bytes());
    body.extend_from_slice(&request.id.sequence().to_be_bytes());
    body.extend_from_slice(&h.route.epoch.get().to_be_bytes());
    body.extend_from_slice(&h.route.birth_digest);
    body.extend_from_slice(&(request.contract.operation.len() as u16).to_be_bytes());
    body.extend_from_slice(request.contract.operation.as_bytes());
    body.extend_from_slice(&request.contract.digest);
    body.push(2);
    if kind == "command" {
        body.extend_from_slice(&h.retry_until.to_be_bytes());
        body.extend_from_slice(&(h.retry_until - 60_000).to_be_bytes());
        body.extend_from_slice(&[23; 32]);
    }
    body.extend_from_slice(&15_000_u64.to_be_bytes());
    body.extend_from_slice(&(request.input.len() as u32).to_be_bytes());
    body.extend_from_slice(&request.input);
    Ok(body)
}

fn written(h: &Harness) -> Result<u64> {
    let snapshot = h.fixture.cluster.voters[0].log.performance_snapshot();
    anyhow::ensure!(
        snapshot.available && !snapshot.saturated,
        "socket counter available"
    );
    snapshot
        .counters
        .iter()
        .find(|c| c.name == "transport_query_write_bytes")
        .map(|c| c.value)
        .context("query socket byte counter")
}

async fn complete(connection: &mut Connection, binding: &[u8; 32], kind: &str) -> Result<()> {
    let (reply, length, _) = connection
        .receive_head(b"CLQA", b"cellar-raft-query-response-1", binding)
        .await?;
    assert_eq!(reply["request"], serde_json::json!(binding));
    assert_eq!(reply["sequence"], "1");
    assert!(length > 28 * 1024);
    let bytes = connection.receive_body(length).await?;
    assert_eq!(
        &bytes[..8],
        if kind == "command" {
            b"CLGCCS01"
        } else {
            b"CLGCRS01"
        }
    );
    assert_eq!(&bytes[8..10], &[0, 2]);
    assert_eq!(
        reply["digest"],
        serde_json::json!(<[u8; 32]>::from(Sha256::digest(&bytes)))
    );
    Ok(())
}

pub(crate) async fn s3_remote_slow_delivery(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    for kind in ["command", "receipt"] {
        for fault in ["drain", "disconnect", "revoke", "timeout"] {
            let store = Arc::new(object_store::prefix::PrefixStore::new(
                store.clone(),
                ObjectPath::from(format!("{kind}-{fault}")),
            ));
            // Exactly fills the original 64 KiB S3 envelope (8 KiB + 2*28 KiB).
            // Only the real socket buffers are reduced; no node quota is raised.
            let h = Harness::configured_transport(
                rooted,
                store,
                1,
                Durability::Quorum,
                Some(28 * 1024),
                Some(4096),
            )
            .await?;
            let remote = Remote::start(&h, h.dispatcher.clone()).await?;
            if kind == "receipt" {
                let r = h
                    .command(1, b"held", Durability::Quorum, Duration::from_secs(15))
                    .await?
                    .response()
                    .await?;
                drop(r);
            }
            let mut connection = Connection::open_with_receive_buffer(
                h.fixture.transports[0].query_address(),
                b"EXPORTER-cellar-raft-query-1",
                Some(1024),
            )
            .await?;
            let body = request(&h, kind)?;
            let request_header = header(&h, &remote, kind, &body)?;
            let before = written(&h)?;
            let binding = connection
                .frame_bytes(
                    b"CLQP",
                    b"cellar-raft-query-request-1",
                    None,
                    &request_header,
                    &body,
                )
                .await?;
            // Reading even the header lets TLS prefetch the whole body and
            // releases backpressure. Observe successful socket writes below TLS
            // before any client read. Fewer than 28 KiB cannot carry this result.
            controller::wait_for(async || {
                let bytes = written(&h)?
                    .checked_sub(before)
                    .context("socket counter monotonic")?;
                Ok(bytes >= 4096
                    && bytes < 28 * 1024
                    && h.controllers[0].usage().retained_s3 == 1
                    && h.fixture.transports[0].usage().query_items == 1)
            })
            .await?;
            assert_eq!(h.controllers[0].usage().retained_s3, 1, "{kind}-{fault}");
            assert_eq!(h.controllers[0].usage().s3_bytes, 64 * 1024);
            assert_eq!(h.archive.tails.usage().operations, 1);
            assert_eq!(
                h.fixture.appliers[&(0, h.route.history)]
                    .0
                    .usage()?
                    .retained_operations,
                0
            );
            let node = &h.controllers[0];
            controller::wait_for(async || Ok(node.usage().retained_commands == 0)).await?;
            let successor = h
                .command(2, b"successor", Durability::Quorum, Duration::from_secs(15))
                .await?
                .response()
                .await?;
            drop(successor);
            assert_eq!(h.controllers[0].usage().retained_s3, 1);
            let mut overloaded = h.dispatcher.lookup_receipt(
                h.cell.clone(),
                h.receipt_request(1, b"held", Durability::S3)?,
                bounds(),
                None,
            )?;
            overloaded.begin().await?;
            let refused = overloaded
                .execute(
                    &h.fixture.runtimes[0],
                    &h.fixture.appliers[&(0, h.route.history)].0,
                    h.fixture.cell_options(&h.cell).await?,
                )
                .await
                .err()
                .context("held wire proof bounds S3 admission")?;
            assert!(matches!(
                refused,
                CellDispatchError::Runtime(RuntimeError::Overloaded)
                    | CellDispatchError::Execution(ExecutionError::Refused(
                        CommandFailure::Overloaded
                    ))
            ));
            drop(overloaded);
            assert!(
                written(&h)?
                    .checked_sub(before)
                    .context("socket counter monotonic")?
                    < 28 * 1024
            );
            match fault {
                "drain" => {
                    complete(&mut connection, &binding, kind).await?;
                    drop(connection);
                }
                "disconnect" => drop(connection),
                "revoke" => {
                    h.fixture.transports[0].revoke_command_origin(
                        h.route.history.cluster,
                        h.route.history.namespace,
                        remote.router.peer.node,
                        2,
                    )?;
                    refunded(&h).await?;
                    assert!(complete(&mut connection, &binding, kind).await.is_err());
                    drop(connection);
                    h.fixture.transports[0].grant_command_origin(
                        cellar::application::consensus::NamespaceCommandOrigin {
                            cluster: h.route.history.cluster,
                            namespace: h.route.history.namespace,
                            revision: 3,
                            peer: remote.router.peer.clone(),
                        },
                    )?;
                }
                "timeout" => {
                    refunded(&h).await?;
                    assert!(complete(&mut connection, &binding, kind).await.is_err());
                    drop(connection);
                }
                _ => unreachable!(),
            }
            refunded(&h).await?;
            assert_eq!(h.calls.load(Ordering::SeqCst), 2);
            assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
            let retry = command(&h, &remote, 1, b"held", Durability::S3).await?;
            assert_eq!(retry.result(), vec![7; 28 * 1024]);
            assert_eq!(retry.durability(), Durability::S3);
            drop(retry);
            refunded(&h).await?;
            assert_eq!(h.calls.load(Ordering::SeqCst), 2);
            remote.close().await?;
            h.close().await?;
        }
    }
    Ok(())
}
