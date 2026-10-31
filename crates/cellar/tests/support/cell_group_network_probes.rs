// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent wire client exercises authenticated framing on real TLS sockets.
use super::*;
use hmac::{Hmac, Mac};
use protobuf::Message as _;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

pub(super) struct Connection {
    stream: TlsStream<TcpStream>,
    pub(super) exporter: [u8; 32],
}

impl Connection {
    async fn open(address: std::net::SocketAddr) -> Result<Self> {
        Self::open_with_exporter(address, b"EXPORTER-cellar-raft-1").await
    }

    pub(super) async fn open_with_exporter(
        address: std::net::SocketAddr,
        label: &[u8],
    ) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(
            include_bytes!("../../transport/tcp/fixtures/ca.der")
                .to_vec()
                .into(),
        )?;
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![b"cellar-raft-1".to_vec()];
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(
                rustls::pki_types::ServerName::try_from("localhost")?,
                TcpStream::connect(address).await?,
            )
            .await?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], label, None)?;
        Ok(Self { stream, exporter })
    }

    pub(super) async fn frame(
        &mut self,
        magic: &[u8; 4],
        purpose: &[u8],
        binding: &[u8],
        header: Value,
        body: &[u8],
    ) -> Result<[u8; 32]> {
        let header = serde_json::to_vec(&header)?;
        let mut prefix = [0; 20];
        prefix[..4].copy_from_slice(magic);
        prefix[4..8].copy_from_slice(&1u32.to_be_bytes());
        prefix[8..12].copy_from_slice(&(header.len() as u32).to_be_bytes());
        prefix[12..].copy_from_slice(&(body.len() as u64).to_be_bytes());
        let mut mac = Hmac::<Sha256>::new_from_slice(&[17; 32])?;
        for part in [purpose, &self.exporter, binding, &prefix, &header] {
            mac.update(&(part.len() as u64).to_be_bytes());
            mac.update(part);
        }
        let tag: [u8; 32] = mac.finalize().into_bytes().into();
        self.stream.write_all(&prefix).await?;
        self.stream.write_all(&header).await?;
        self.stream.write_all(&tag).await?;
        self.stream.write_all(body).await?;
        self.stream.flush().await?;
        Ok(tag)
    }

    /// Independently check the server's framing and exporter-bound MAC before
    /// using a reply in a later probe. Bounds apply before allocating either body.
    pub(super) async fn receive(
        &mut self,
        magic: &[u8; 4],
        purpose: &[u8],
        binding: &[u8],
    ) -> Result<(Value, Vec<u8>, [u8; 32])> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut prefix = [0; 20];
            self.stream.read_exact(&mut prefix).await?;
            assert_eq!(&prefix[..4], magic);
            assert_eq!(&prefix[4..8], &1u32.to_be_bytes());
            let header_len = u32::from_be_bytes(prefix[8..12].try_into()?) as usize;
            let body_len = u64::from_be_bytes(prefix[12..].try_into()?);
            assert!(header_len <= 64 * 1024 && body_len <= 64 * 1024);
            let mut header = vec![0; header_len];
            self.stream.read_exact(&mut header).await?;
            let mut tag = [0; 32];
            self.stream.read_exact(&mut tag).await?;
            let mut mac = Hmac::<Sha256>::new_from_slice(&[17; 32])?;
            for part in [purpose, &self.exporter, binding, &prefix, &header] {
                mac.update(&(part.len() as u64).to_be_bytes());
                mac.update(part);
            }
            mac.verify_slice(&tag)?;
            let mut body = vec![0; body_len as usize];
            self.stream.read_exact(&mut body).await?;
            Ok((serde_json::from_slice(&header)?, body, tag))
        })
        .await?
    }

    pub(super) async fn refused(mut self) -> Result<()> {
        let mut byte = [0];
        let result =
            tokio::time::timeout(Duration::from_secs(5), self.stream.read(&mut byte)).await?;
        assert!(
            !matches!(result, Ok(n) if n > 0),
            "refused stream must not acknowledge admission"
        );
        Ok(())
    }
}

pub(super) fn peer(peer: &RaftPeer<GroupHistory>) -> Value {
    json!({"node":peer.replica.node.bytes(), "disk":peer.replica.disk.bytes(), "process":peer.replica.process.bytes(), "member":peer.replica.member.get().to_string(), "label":peer.label})
}

pub(super) async fn probe(
    fixture: &mut RuntimeCluster,
    history: GroupHistory,
    snapshot: &raft::prelude::Snapshot,
    pool: &CellGroupCheckpointPool,
    other: GroupHistory,
    registrations: &mut [CellGroupCheckpointRegistration],
) -> Result<()> {
    let route = fixture.tcp_route(history)?;
    let group = fixture.cluster.voters[2].log.cell_group(&history)?;
    let before = group.group_manifest()?;
    let mut message = Message {
        from: 1,
        to: 3,
        term: snapshot.get_metadata().term,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgSnapshot);
    message.set_snapshot(snapshot.clone());
    let body = message.write_to_bytes()?;
    let start = json!({"route": {
        "scope": {"kind":"Group", "identity":[history.cluster.bytes(), history.namespace.bytes(), history.group.bytes(), history.incarnation.bytes()]},
        "schema":route.schema_digest, "source":peer(&route.peers[0]), "target":peer(&route.peers[2])
    }, "digest": <[u8; 32]>::from(Sha256::digest(&body))});
    let address = fixture.transports[2].data_address();
    for case in 0..7 {
        let mut connection = Connection::open(address).await?;
        let mut header = start.clone();
        match case {
            0 => {
                connection.exporter[0] ^= 1;
            }
            1 => {
                header["route"]["source"]["process"] = json!(vec![244; 16]);
            }
            2 => {
                header["digest"] = json!(vec![0; 32]);
            }
            _ => (),
        }
        let binding = connection
            .frame(
                b"CLGS",
                b"cellar-group-snapshot-start-1",
                &[],
                header,
                &body,
            )
            .await?;
        if case >= 3 {
            let sequence = if case == 3 { 1 } else { 0 };
            let binding = if case == 4 { [241; 32] } else { binding };
            // Case 5 sends an empty header part, case 6 a premature End.
            let part = if case == 6 {
                Value::Null
            } else {
                json!({"kind":"Header"})
            };
            connection.frame(b"CLGD", b"cellar-group-snapshot-data-1", &binding,
                json!({"sequence":sequence, "part":part, "offset":0, "length":0, "digest":<[u8; 32]>::from(Sha256::digest([]))}), &[]).await?;
        }
        connection.refused().await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.usage().sources != 0 || fixture.transports[2].usage().data_items != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(
            group.group_manifest()?,
            before,
            "fault {case} changed selected root"
        );
        assert_eq!(group.checkpoint_manifest()?, None);
    }
    // Authenticated start with a declared oversized data body refuses before
    // allocation or waiting for that body.
    let mut connection = Connection::open(address).await?;
    connection
        .frame(
            b"CLGS",
            b"cellar-group-snapshot-start-1",
            &[],
            start.clone(),
            &body,
        )
        .await?;
    let mut oversized = [0; 20];
    oversized[..4].copy_from_slice(b"CLGD");
    oversized[4..8].copy_from_slice(&1u32.to_be_bytes());
    oversized[8..12].copy_from_slice(&1u32.to_be_bytes());
    oversized[12..].copy_from_slice(&65537u64.to_be_bytes());
    connection.stream.write_all(&oversized).await?;
    connection.refused().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.usage().sources != 0 || fixture.transports[2].usage().data_items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    // A disconnected stream cannot use channel closure as successful End.
    let mut connection = Connection::open(address).await?;
    let binding = connection
        .frame(
            b"CLGS",
            b"cellar-group-snapshot-start-1",
            &[],
            start.clone(),
            &body,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.usage().sources == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    // The checkpoint owns receive admission while another group on the same
    // runtimes/transport/engines elects, commits and applies a real cell write.
    fixture.runtimes[0].campaign(other).await?;
    fixture.settle(&[other]).await?;
    let other_route = fixture.cluster.voters[0]
        .log
        .cell_group(&other)?
        .group_manifest()?
        .context("other root")?
        .route()
        .clone();
    let bytes = images::image(
        None,
        "CREATE TABLE progress(n); INSERT INTO progress VALUES(17);",
    )?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let accepted = fixture.runtimes[0]
        .propose(
            other,
            request(
                &other_route,
                super::history(&other_route, "unrelated")?,
                1,
                &cut,
            )?,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture.applied[&other]
            .iter()
            .any(|index| *index < accepted.position.index)
        {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(pool.usage().sources, 1);
    // Revocation while a stream is admitted must refuse its authenticated End.
    fixture.transports[2].remove_physical_group(&history)?;
    connection.frame(b"CLGD", b"cellar-group-snapshot-data-1", &binding,
        json!({"sequence":0, "part":null, "offset":0, "length":0, "digest":<[u8; 32]>::from(Sha256::digest([]))}), &[]).await?;
    connection.refused().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.usage().sources != 0 || fixture.transports[2].usage().data_items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    fixture.transports[2].install_physical_group(route)?;
    // Replacing the old guard after registering must not remove the new owner.
    registrations[1] = fixture.transports[2]
        .register_group_checkpoint(fixture.appliers[&(2, history)].1.clone())?;
    let mut connection = Connection::open(address).await?;
    connection
        .frame(b"CLGS", b"cellar-group-snapshot-start-1", &[], start, &body)
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.usage().sources == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    drop(connection);
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.usage().sources != 0 || fixture.transports[2].usage().data_items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(group.group_manifest()?, before);
    assert_eq!(group.checkpoint_manifest()?, None);
    Ok(())
}
