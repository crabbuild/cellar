// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent authenticated seed requests against the real source listener.
use super::*;
use network_probes::{peer, Connection};
use serde_json::{json, Value};

pub(super) async fn source_protocol(
    fixture: &RuntimeCluster,
    route: &cellar::application::consensus::verification::GroupRoute<GroupHistory>,
    pool: &CellGroupCheckpointPool,
    registration: &mut CellGroupCheckpointRegistration,
) -> Result<()> {
    let source = &fixture.transports[0];
    let before = fixture.cluster.voters[0]
        .log
        .cell_group(&route.scope)?
        .group_manifest()?;
    let target = route.peers.last().context("joining peer")?;
    assert_eq!(target.replica.member.get(), 4);
    let history = route.scope;
    let request = json!({"route": {
        "scope": {"kind":"Group", "identity":[history.cluster.bytes(), history.namespace.bytes(), history.group.bytes(), history.incarnation.bytes()]},
        "schema":route.schema_digest, "source":peer(target), "target":peer(&route.peers[0])
    }, "nonce":"1", "timeout_ms":10000});
    for case in 0..11 {
        let mut connection =
            Connection::open_with_exporter(source.data_address(), b"EXPORTER-cellar-group-seed-1")
                .await?;
        let mut invalid = request.clone();
        match case {
            0 => connection.exporter[0] ^= 1,
            1 => invalid["route"]["source"]["process"] = json!(vec![244; 16]),
            2 => invalid["route"]["source"]["disk"] = json!(vec![244; 16]),
            3 => invalid["route"]["scope"]["identity"][3] = json!(vec![244; 16]),
            4 => invalid["route"]["schema"] = json!(vec![244; 32]),
            5 => invalid["nonce"] = json!("0"),
            6 => invalid["nonce"] = json!("01"),
            7 => invalid["timeout_ms"] = json!(0),
            8 => invalid["timeout_ms"] = json!(3_600_001),
            9 => invalid["extra"] = json!(true),
            // A current voter cannot request a fresh learner seed.
            10 => invalid["route"]["source"] = peer(&route.peers[1]),
            _ => unreachable!(),
        }
        connection
            .frame(b"CLGJ", b"cellar-group-seed-request-1", &[], invalid, &[])
            .await?;
        connection
            .refused()
            .await
            .with_context(|| format!("seed request fault {case}"))?;
        released(source, pool).await?;
    }
    let mut previous = None;
    for case in 0..5 {
        let mut connection =
            Connection::open_with_exporter(source.data_address(), b"EXPORTER-cellar-group-seed-1")
                .await?;
        let tag = connection
            .frame(
                b"CLGJ",
                b"cellar-group-seed-request-1",
                &[],
                request.clone(),
                &[],
            )
            .await?;
        let (reply, body, binding) = connection
            .receive(b"CLGK", b"cellar-group-seed-grant-1", &tag)
            .await?;
        assert_eq!(reply["request"], json!(tag));
        assert_eq!(reply["nonce"], request["nonce"]);
        assert_eq!(reply["route"]["source"], request["route"]["target"]);
        assert_eq!(reply["route"]["target"], request["route"]["source"]);
        assert_eq!(
            reply["digest"],
            json!(<[u8; 32]>::from(Sha256::digest(&body)))
        );
        assert_eq!(
            pool.usage().sources,
            1,
            "complete source remains pinned awaiting Ready"
        );
        let mut ready = json!({"sequence":0});
        let bound = match case {
            0 => {
                ready["sequence"] = json!(1);
                binding
            }
            1 => previous.context("previous authenticated reply")?,
            2 => {
                ready["extra"] = Value::Bool(true);
                binding
            }
            // The captured source registration is revoked while the peer has
            // its signed descriptor. Its Ready must not revive the old owner.
            3 => {
                source.remove_physical_group(&history)?;
                binding
            }
            4 => {
                drop(connection);
                released(source, pool).await?;
                continue;
            }
            _ => unreachable!(),
        };
        connection
            .frame(b"CLGL", b"cellar-group-seed-ready-1", &bound, ready, &[])
            .await?;
        connection
            .refused()
            .await
            .with_context(|| format!("seed Ready fault {case}"))?;
        released(source, pool).await?;
        if case == 3 {
            source.install_physical_group(route.clone())?;
            *registration =
                source.register_group_checkpoint(fixture.appliers[&(0, history)].1.clone())?;
        }
        previous = Some(binding);
    }
    assert_eq!(
        fixture.cluster.voters[0]
            .log
            .cell_group(&history)?
            .group_manifest()?,
        before
    );
    Ok(())
}

async fn released(transport: &RaftTransport, pool: &CellGroupCheckpointPool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.usage().sources != 0 || transport.usage().data_items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}
