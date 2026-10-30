// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Three physical runtimes use the production transport and real TLS sockets.
use super::*;
use cellar::application::consensus::verification::GroupRoute as TransportRoute;
use cellar::peer_auth::PeerAuth;
use tokio::net::TcpListener;

fn tls() -> Result<RaftTls> {
    Ok(RaftTls::new(
        vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into()],
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?)
}

async fn start(runtime: &CellGroupRuntime, n: usize) -> Result<RaftTransport, TransportError> {
    RaftTransport::start_shared(
        runtime.clone(),
        Arc::new(
            PeerAuth::new([17; 32], format!("node-{}", n + 1))
                .map_err(|_| TransportError::Identity)?,
        ),
        tls().map_err(|e| TransportError::Invalid(e.to_string()))?,
        TcpListener::bind("127.0.0.1:0").await?,
        TcpListener::bind("127.0.0.1:0").await?,
        TcpListener::bind("127.0.0.1:0").await?,
        TransportOptions::default(),
    )
    .await
}

impl RuntimeCluster {
    pub(super) async fn connect_tcp(&mut self) -> Result<()> {
        assert!(self.transports.is_empty());
        assert!(self.network.is_empty());
        for (n, runtime) in self.runtimes.iter().enumerate() {
            self.transports.push(start(runtime, n).await?);
        }
        for history in self.applied.keys() {
            let route = self.tcp_route(*history)?;
            for transport in &self.transports {
                transport.install_physical_group(route.clone())?;
            }
        }
        Ok(())
    }

    fn tcp_route(&self, history: GroupHistory) -> Result<TransportRoute<GroupHistory>> {
        let marker = self.cluster.voters[0]
            .log
            .cell_group(&history)?
            .group_manifest()?
            .context("manifest")?;
        Ok(TransportRoute {
            scope: history,
            schema_digest: marker.route().schema_digest,
            revision: 1,
            peers: self
                .transports
                .iter()
                .enumerate()
                .map(|(n, transport)| -> Result<_> {
                    Ok(RaftPeer {
                        replica: self.cluster.voters[n]
                            .log
                            .cell_group(&history)?
                            .identity()
                            .clone(),
                        label: format!("node-{}", n + 1),
                        server_name: "localhost".into(),
                        control_address: transport.control_address(),
                        data_address: transport.data_address(),
                        query_address: transport.query_address(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

pub(crate) async fn physical_transport() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let second = fixture.add_group(31).await?;
    let histories = [fixture.cluster.birth.route().history, second];
    fixture.connect_tcp().await?;
    assert!(matches!(
        start(&fixture.runtimes[0], 0).await,
        Err(TransportError::Runtime(RuntimeError::AlreadyAttached))
    ));
    for transport in &fixture.transports {
        assert_eq!(transport.usage().groups, 2);
        assert_eq!(transport.usage().peers, 2);
    }
    for history in histories {
        fixture.runtimes[0].campaign(history).await?;
    }
    fixture.settle(&histories).await?;
    for group in histories {
        let route = fixture.cluster.voters[0]
            .log
            .cell_group(&group)?
            .group_manifest()?
            .context("group")?
            .route()
            .clone();
        for name in ["tls-a", "tls-b", "tls-c"] {
            let cell = history(&route, name)?;
            let bytes =
                images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
            let cut = images::Cut::new(&[], &bytes, 0, true, false);
            let accepted = fixture.runtimes[0]
                .propose(group, request(&route, cell, 1, &cut)?)
                .await?;
            tokio::time::timeout(Duration::from_secs(20), async {
                while fixture.applied[&group]
                    .iter()
                    .any(|index| *index < accepted.position.index)
                {
                    fixture.pump(&[], &mut BTreeMap::new()).await?;
                    tokio::task::yield_now().await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        }
        for voter in &fixture.cluster.voters {
            assert_eq!(
                voter
                    .log
                    .cell_group(&group)?
                    .group_manifest()?
                    .context("applied")?
                    .root()
                    .cells(),
                3
            );
        }
    }
    let route = fixture.tcp_route(histories[0])?;
    let mut heartbeat = Message {
        from: 1,
        to: 2,
        term: fixture.runtimes[0].status(histories[0]).await?.term.get(),
        ..Default::default()
    };
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    let mut snapshot = heartbeat.clone();
    snapshot.set_msg_type(MessageType::MsgSnapshot);
    assert!(matches!(
        fixture.transports[0].submit_physical(&histories[0], snapshot),
        Err(TransportError::Refused)
    ));
    let mut unknown = histories[0];
    unknown.group = GroupId::new([222; 16])?;
    assert!(matches!(
        fixture.transports[0].submit_physical(&unknown, heartbeat.clone()),
        Err(TransportError::UnknownGroup)
    ));
    let mut wrong = route.clone();
    wrong.schema_digest[0] ^= 1;
    assert!(matches!(
        fixture.transports[0].install_physical_group(wrong),
        Err(TransportError::Identity)
    ));
    let mut wrong = route.clone();
    wrong.revision += 1;
    wrong.peers[0].replica.process = ProcessGeneration::new([222; 16])?;
    assert!(matches!(
        fixture.transports[0].install_physical_group(wrong),
        Err(TransportError::Identity)
    ));
    let mut conflicting = RaftPeerNode::from(&route.peers[1]);
    conflicting.process = ProcessGeneration::new([223; 16])?;
    assert!(matches!(
        fixture.transports[0].grant_query_origin(NamespaceQueryOrigin {
            cluster: histories[0].cluster,
            namespace: histories[0].namespace,
            revision: 1,
            peer: conflicting.clone(),
        }),
        Err(TransportError::StalePeer)
    ));
    assert!(matches!(
        fixture.transports[0].grant_inspection_origin(NamespaceInspectionOrigin {
            cluster: histories[0].cluster,
            namespace: histories[0].namespace,
            revision: 1,
            peer: conflicting,
        }),
        Err(TransportError::StalePeer)
    ));
    fixture.transports[1].remove_physical_group(&histories[0])?;
    assert_eq!(fixture.transports[1].usage().groups, 1);
    assert_eq!(fixture.transports[1].usage().peers, 2);
    assert!(fixture.transports[0]
        .submit_physical(&histories[0], heartbeat.clone())?
        .accepted()
        .await
        .is_err());
    fixture.transports[1].install_physical_group(route.clone())?;
    fixture.transports[0]
        .submit_physical(&histories[0], heartbeat)?
        .accepted()
        .await?;
    for transport in &fixture.transports {
        assert!(transport.usage().accepted > 0);
    }
    fixture.transports[0].close().await;
    assert_eq!(fixture.transports[0].usage().tasks, 0);
    let replacement = start(&fixture.runtimes[0], 0).await?;
    replacement.close().await;
    fixture.close().await
}
