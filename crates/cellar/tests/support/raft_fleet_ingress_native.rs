//! Valid bucket pins and correctly authenticated native frames do not enroll a node.
use super::*;
use hmac::{Hmac, Mac};
use object_store::{path::Path, ObjectStore};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Wire {
    request: [u8; 4],
    response: [u8; 4],
    exporter: &'static [u8],
    purpose: &'static [u8],
    reply_purpose: &'static [u8],
    node: bool,
}

const NODE: Wire = Wire {
    request: *b"CLNQ",
    response: *b"CLNR",
    exporter: b"EXPORTER-cellar-raft-node-inspection-1",
    purpose: b"cellar-node-inspection-request-1",
    reply_purpose: b"cellar-node-inspection-response-1",
    node: true,
};
const RAFT: Wire = Wire {
    request: *b"CLRP",
    response: *b"CLRA",
    exporter: b"EXPORTER-cellar-raft-1",
    purpose: b"cellar-raft-request-1",
    reply_purpose: b"cellar-raft-response-1",
    node: false,
};

fn mac(purpose: &[u8], parts: &[&[u8]]) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(&[11; 32]).expect("fixed synthetic fixture key");
    for part in std::iter::once(purpose).chain(parts.iter().copied()) {
        mac.update(&(part.len() as u64).to_be_bytes());
        mac.update(part);
    }
    mac
}

async fn exchange(
    f: &Fixture,
    address: std::net::SocketAddr,
    wire: &Wire,
    header: serde_json::Value,
    body: &[u8],
) -> Result<serde_json::Value> {
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(
                include_bytes!("../../transport/tcp/fixtures/ca.der")
                    .to_vec()
                    .into(),
            )?;
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots)
            .with_no_client_auth();
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
            let mut stream = connector
                .connect(
                    rustls::pki_types::ServerName::try_from("localhost")?,
                    tokio::net::TcpStream::connect(address).await?,
                )
                .await?;
            let exporter =
                stream
                    .get_ref()
                    .1
                    .export_keying_material([0; 32], wire.exporter, None)?;
            let header = serde_json::to_vec(&header)?;
            let mut prefix = [0; 20];
            prefix[..4].copy_from_slice(&wire.request);
            prefix[4..8].copy_from_slice(&1u32.to_be_bytes());
            prefix[8..12].copy_from_slice(&(header.len() as u32).to_be_bytes());
            prefix[12..].copy_from_slice(&(body.len() as u64).to_be_bytes());
            let tag: [u8; 32] = if wire.node {
                mac(wire.purpose, &[&exporter, &[], &prefix, &header])
            } else {
                mac(wire.purpose, &[&exporter, &prefix, &header])
            }
            .finalize()
            .into_bytes()
            .into();
            stream.write_all(&prefix).await?;
            stream.write_all(&header).await?;
            stream.write_all(&tag).await?;
            stream.flush().await?;
            // Deliberately withhold the declared Raft body: refusal must precede
            // body admission, rather than eventually rejecting application bytes.
            let mut response = [0; 20];
            stream.read_exact(&mut response).await?;
            ensure!(
                response[..4] == wire.response
                    && response[4..8] == 1u32.to_be_bytes()
                    && response[12..] == [0; 8]
            );
            let length = u32::from_be_bytes(response[8..12].try_into()?) as usize;
            ensure!((1..=16 * 1024).contains(&length));
            let mut bytes = vec![0; length];
            stream.read_exact(&mut bytes).await?;
            let mut response_tag = [0; 32];
            stream.read_exact(&mut response_tag).await?;
            mac(wire.reply_purpose, &[&exporter, &tag, &response, &bytes])
                .verify_slice(&response_tag)?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        }))
        .await??
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_unknown_node() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.reject_listing.store(true, Ordering::Release);
    let auth = f.enroll().await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (config, services) = f.config(&node, auth).await?;
    let own = config.peers[0].clone();
    let host = node.start_raft_cells(config, services).await?;
    let rogue = f.peer_spec(7)?;
    let volume = tempfile::tempdir()?;
    let process = ProcessGeneration::new([27; 16])?;
    let log = f
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                volume.path().join("log"),
                rogue.node,
                rogue.disk,
                process,
                LogOpenMode::Create,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let listeners = [
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
    ];
    let peer = RaftPeerNode {
        node: rogue.node,
        disk: rogue.disk,
        process,
        label: rogue.label.clone(),
        server_name: rogue.server_name.clone(),
        control_address: listeners[0].local_addr()?,
        data_address: listeners[1].local_addr()?,
        query_address: listeners[2].local_addr()?,
    };
    let namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(f.declaration.namespace.clone())
    .await?;
    let view = namespace
        .register_node_storage(&log, TIMEOUT)
        .await?
        .publication(peer.clone(), TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    ensure!(view.storage_instance() == log.storage_instance()?);
    let key = Path::from(format!(
        "host-enrollment/raft/{}/nodes/{}/view",
        hex::encode(f.declaration.namespace.cluster.bytes()),
        hex::encode(rogue.node.bytes())
    ));
    let bytes = f.store.get(&key).await?.bytes().await?;
    let published: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    let protocol = published["protocol"].clone();
    ensure!(protocol.as_array().is_some_and(|v| v.len() == 32));
    let authorization = f.history(61).await?;
    let dormant = host
        .register_managed_history(authorization, TIMEOUT)
        .await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if host
                    .observe_node(TIMEOUT)
                    .await?
                    .fleet_trust_revision
                    .is_some_and(|r| r >= revision)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await???;
    ensure!(matches!(
        host.inspect_peer_node(rogue.node, revision, TIMEOUT).await,
        Err(InspectionError::Refused)
    ));
    let physical = host.observe_node(TIMEOUT).await?;
    let endpoint = |node: NodeId,
                    disk: DiskLineage,
                    process: ProcessGeneration,
                    instance: EngineInstanceId,
                    label: &str| {
        serde_json::json!({"node":node.bytes(), "disk":disk.bytes(), "process":process.bytes(), "storage_instance":instance.bytes(), "label":label})
    };
    for with_inventory in [false, true] {
        let response = exchange(&f, own.control_address, &NODE, serde_json::json!({
        "route":{"fleet":vec![80;16], "catalog":f.catalog.group_binding(dormant.scope())?.catalog(), "cluster":f.declaration.namespace.cluster.bytes(), "protocol":protocol,
            "minimum_revision":revision.to_string(), "source":endpoint(rogue.node, rogue.disk, peer.process, view.storage_instance(), &rogue.label),
            "target":endpoint(own.node, own.disk, own.process, physical.node.storage_instance, &own.label)},
        "nonce":"1", "timeout_ms":1000, "physical":with_inventory }), &[]).await.context("unrooted node metadata probe")?;
        ensure!(
            response["status"] == 2,
            "unrooted native metadata request was not refused: {response}"
        );
    }
    for known in [false, true] {
        let mut scope = dormant.scope().clone();
        if !known {
            scope.cell = "Counter:unsolicited".into();
            scope.group = GroupId::new([99; 16])?;
        }
        for (lane, address) in [("Control", own.control_address), ("Data", own.data_address)] {
            let raft_protocol: [u8; 32] = Sha256::digest(
                b"cellar-raft-peer-1:tls13-exporter:hmac-sha256:protobuf2:bounded-strict:extent-stream",
            )
            .into();
            let request = serde_json::json!({"protocol":raft_protocol,"route":{
                "scope":{"cluster":hex::encode(scope.cluster.bytes()),"namespace":hex::encode(scope.namespace.bytes()),
                    "cell":scope.cell,"group":hex::encode(scope.group.bytes()),"incarnation":hex::encode(scope.incarnation.bytes())},
                "schema":f.definition.schema,
                "source":{"node":rogue.node.bytes(),"disk":rogue.disk.bytes(),"process":peer.process.bytes(),"member":"4","label":rogue.label},
                "target":{"node":own.node.bytes(),"disk":own.disk.bytes(),"process":own.process.bytes(),"member":"1","label":own.label}},
                "sequence":"1","lane":lane,"body_digest":<Sha256 as Digest>::digest(b"unread").to_vec()});
            let response = exchange(&f, address, &RAFT, request, b"unread")
                .await
                .with_context(|| {
                    format!("unrooted {lane} frame, existing dormant history: {known}")
                })?;
            ensure!(
                response["disposition"] == "UnknownGroup",
                "unsolicited Raft history was admitted: {response}"
            );
        }
    }
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .authorize_host(&root, rogue.node, TIMEOUT)
        .await
        .is_err());
    ensure!(f
        .catalog
        .get(
            &root,
            FleetTable::Nodes,
            &hex::encode(rogue.node.bytes()),
            TIMEOUT
        )
        .await?
        .is_none());
    let after = host.observe_node(TIMEOUT).await?;
    ensure!(
        after.node.groups == 0
            && after.registered_histories == 1
            && after.dormant_histories == 1
            && after.unattached_engine_histories == 0
    );
    ensure!(
        f.calls.load(Ordering::Acquire) == 0 && f.store.listing_calls.load(Ordering::Acquire) == 0
    );
    ensure!(dormant.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    log.close().await?;
    stopped(node).await?;
    f.domain.shutdown().await;
    Ok(())
}
