// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fixed-width, optional advice inside the existing authenticated RPC response.
use super::*;
use crate::raft::leader_advice::LeaderAdvice;
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{ClusterId, GroupId, IncarnationId, NamespaceId},
};

pub(super) fn encode(
    body: &mut Vec<u8>,
    advice: Option<&LeaderAdvice>,
) -> Result<(), TransportError> {
    let Some(advice) = advice else {
        body.push(0);
        return Ok(());
    };
    validate(advice)?;
    body.push(1);
    body.extend_from_slice(&advice.history.cluster.bytes());
    body.extend_from_slice(&advice.history.namespace.bytes());
    body.extend_from_slice(&advice.history.group.bytes());
    body.extend_from_slice(&advice.history.incarnation.bytes());
    body.extend_from_slice(&advice.membership_revision.to_be_bytes());
    body.extend_from_slice(&advice.configuration_digest);
    body.extend_from_slice(&advice.term.to_be_bytes());
    body.extend_from_slice(&advice.leader.get().to_be_bytes());
    Ok(())
}

pub(super) fn decode(body: &mut Reader<'_>) -> Result<Option<LeaderAdvice>, TransportError> {
    match body.byte()? {
        0 => return Ok(None),
        1 => (),
        _ => return Err(TransportError::Protocol),
    }
    let identity = |_| TransportError::Identity;
    let advice = LeaderAdvice {
        history: GroupHistory {
            cluster: ClusterId::new(body.array()?).map_err(identity)?,
            namespace: NamespaceId::new(body.array()?).map_err(identity)?,
            group: GroupId::new(body.array()?).map_err(identity)?,
            incarnation: IncarnationId::new(body.array()?).map_err(identity)?,
        },
        membership_revision: u64::from_be_bytes(body.array()?),
        configuration_digest: body.array()?,
        term: u64::from_be_bytes(body.array()?),
        leader: MemberId::new(u64::from_be_bytes(body.array()?)).map_err(identity)?,
    };
    validate(&advice)?;
    Ok(Some(advice))
}

fn validate(advice: &LeaderAdvice) -> Result<(), TransportError> {
    if advice.membership_revision == 0 || advice.configuration_digest == [0; 32] || advice.term == 0
    {
        return Err(TransportError::Identity);
    }
    Ok(())
}
