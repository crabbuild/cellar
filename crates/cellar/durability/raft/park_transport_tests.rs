// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{DiskLineage, GroupId, IncarnationId, MemberId, ProcessGeneration};

fn route() -> anyhow::Result<(
    Route,
    ReplicaIdentity<GroupHistory>,
    ReplicaIdentity<GroupHistory>,
)> {
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let replica = |id| -> anyhow::Result<_> {
        Ok(ReplicaIdentity {
            scope: history,
            member: MemberId::new(u64::from(id))?,
            node: NodeId::new([id; 16])?,
            disk: DiskLineage::new([id + 3; 16])?,
            process: ProcessGeneration::new([id + 6; 16])?,
        })
    };
    let leader = replica(1)?;
    let voter = replica(2)?;
    let route = Route::new_group(&leader, "leader", &voter, "voter", [5; 32])?;
    Ok((route, leader, voter))
}

#[test]
fn park_reply_is_bound_to_tls_request_cut_and_process() -> anyhow::Result<()> {
    let auth = PeerAuth::new([8; 32], "leader")?;
    let options = TransportOptions::default();
    let wire = Wire {
        auth: &auth,
        exporter: &[9; 32],
        options: &options,
        max_body: 0,
    };
    let (route, leader, voter) = route()?;
    let cut = serde_json::from_str(r#"{"generation":3,"term":2,"membership_revision":1}"#)?;
    let request = Request {
        route,
        cut,
        timeout_ms: 100,
    };
    assert_eq!(request.deadline(10, 109)?, 110);
    assert!(matches!(
        request.deadline(10, 110),
        Err(TransportError::Timeout)
    ));
    let head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
    let reply = wire.encode(
        RESPONSE_MAGIC,
        RESPONSE_PURPOSE,
        &head.tag,
        &Reply::Acknowledged { sequence: 6 },
        &[],
    )?;

    let mut rebooted = voter.clone();
    rebooted.process = ProcessGeneration::new([19; 16])?;
    let other_route = Route::new_group(&leader, "leader", &rebooted, "voter", [5; 32])?;
    let other = Request {
        route: other_route,
        cut: request.cut.clone(),
        timeout_ms: request.timeout_ms,
    };
    let other_head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &other, &[])?;
    assert!(matches!(
        wire.decode::<Reply>(
            RESPONSE_MAGIC,
            RESPONSE_PURPOSE,
            &reply,
            Some(&other_head.tag)
        ),
        Err(TransportError::Authentication)
    ));
    let other_cut = Request {
        route: request.route.clone(),
        cut: serde_json::from_str(r#"{"generation":4,"term":2,"membership_revision":1}"#)?,
        timeout_ms: request.timeout_ms,
    };
    let other_head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &other_cut, &[])?;
    assert!(matches!(
        wire.decode::<Reply>(
            RESPONSE_MAGIC,
            RESPONSE_PURPOSE,
            &reply,
            Some(&other_head.tag)
        ),
        Err(TransportError::Authentication)
    ));
    let other_wire = Wire {
        exporter: &[10; 32],
        ..wire
    };
    assert!(matches!(
        other_wire.decode::<Reply>(RESPONSE_MAGIC, RESPONSE_PURPOSE, &reply, Some(&head.tag)),
        Err(TransportError::Authentication)
    ));

    let decoded: Reply = wire.decode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &reply, Some(&head.tag))?;
    let expected_cut = request.cut.clone();
    let receipt = decoded.acknowledgement(request)?;
    assert_eq!(receipt.voter, voter);
    assert_eq!(receipt.leader, leader.member);
    assert_eq!(receipt.cut, expected_cut);
    assert_eq!(receipt.sync_sequence, 6);
    Ok(())
}
