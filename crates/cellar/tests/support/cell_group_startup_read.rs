// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained owners must elect through Raft, without an idle timer or force-leader.
use super::*;

pub(super) async fn retained_read_election(connected: bool) -> Result<()> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    let previous = cluster.voters[0].owner.as_ref().unwrap().status()?.term;
    let roots: Vec<_> = cluster
        .voters
        .iter()
        .map(|voter| voter.owner.as_ref().unwrap().manifest().clone())
        .collect();
    for voter in &mut cluster.voters {
        drop(voter.owner.take());
    }
    for (index, root) in roots.into_iter().enumerate() {
        let permit = cluster.permit(index).await?;
        cluster.voters[index].owner = Some(
            cluster
                .domain
                .run(CellGroupOwner::start(
                    permit,
                    root,
                    cluster.options.clone(),
                    [index as u8 + 11; 32],
                ))
                .await??,
        );
    }
    // A restarted follower that previously voted for another member cannot
    // turn a read into a competing startup campaign. Invalid reads also refuse
    // before initiating any election work.
    assert!(matches!(
        cluster.voters[1]
            .owner
            .as_mut()
            .unwrap()
            .read_index(b"follower".to_vec()),
        Err(RuntimeError::NotReady)
    ));
    assert!(matches!(
        cluster.voters[0].owner.as_mut().unwrap().read_index(vec![]),
        Err(RuntimeError::Invalid(_))
    ));
    cluster.drive(false).await?;
    assert!(cluster.voters.iter().all(|voter| voter
        .owner
        .as_ref()
        .unwrap()
        .status()
        .unwrap()
        .leader
        .is_none()));
    assert!(matches!(
        cluster.voters[0]
            .owner
            .as_mut()
            .unwrap()
            .read_index(b"startup".to_vec()),
        Err(RuntimeError::NotReady)
    ));
    assert!(!cluster.voters[0].owner.as_ref().unwrap().status()?.ready);
    // No ticks and no explicit campaign after reattachment: only ordinary
    // PreVote/vote messages, persistence and committed apply can make it ready.
    cluster.drive(!connected).await?;
    if connected {
        let ready = cluster.voters[0].owner.as_ref().unwrap().status()?;
        assert!(ready.ready && ready.term > previous);
        cluster.voters[0]
            .owner
            .as_mut()
            .unwrap()
            .read_index(b"fresh-proof".to_vec())?;
        cluster.drive(false).await?;
        assert!(cluster
            .reads
            .iter()
            .any(|(voter, term, _, context)| *voter == 0
                && *term == ready.term
                && context == b"fresh-proof"));
    } else {
        for _ in 0..8 {
            assert!(matches!(
                cluster.voters[0]
                    .owner
                    .as_mut()
                    .unwrap()
                    .read_index(b"still-isolated".to_vec()),
                Err(RuntimeError::NotReady)
            ));
            // Consuming the single startup hint cannot create a campaign loop
            // or replenish votes, barriers or request deadlines.
            assert!(cluster.voters[0].owner.as_mut().unwrap().poll()?.is_none());
        }
        assert!(cluster.reads.is_empty());
        for voter in &cluster.voters {
            let status = voter.owner.as_ref().unwrap().status()?;
            assert!(!status.ready && status.term == previous);
        }
    }
    cluster.close().await
}
