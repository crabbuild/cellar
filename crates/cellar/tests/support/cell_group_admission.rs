// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

pub(super) async fn capacity() -> Result<()> {
    let mut cluster = Cluster::with_options(RaftRuntimeOptions {
        max_cells_per_group: 2,
        ..Default::default()
    })
    .await?;
    cluster.elect().await?;
    let route = cluster.birth.route().clone();
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let a = history(&route, "capacity-a")?;
    let b = history(&route, "capacity-b")?;
    let c = history(&route, "capacity-c")?;
    let request_a = request(&route, a.clone(), 1, &cut)?;
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    let term = owner.status()?.term;
    let first = owner.propose_at_term(term, request_a.clone())?;
    let repeated = owner.propose_at_term(term, request_a.clone())?;
    assert_eq!(first.position, repeated.position);
    assert_eq!(first.digest, repeated.digest);
    let mut rival = a.clone();
    rival.incarnation = IncarnationId::new([99; 16])?;
    assert!(matches!(
        owner.propose_at_term(term, request(&route, rival, 1, &cut)?),
        Err(RuntimeError::Invalid(_))
    ));
    let second = owner.propose_at_term(term, request(&route, b.clone(), 1, &cut)?)?;
    assert_eq!(second.position.index.get(), first.position.index.get() + 1);
    assert_eq!(owner.manifest().marker().root().cells(), 0);
    assert!(matches!(
        owner.propose_at_term(term, request(&route, c.clone(), 1, &cut)?),
        Err(RuntimeError::Overloaded)
    ));
    cluster.drive(false).await?;
    for voter in &cluster.voters {
        assert_eq!(
            voter
                .owner
                .as_ref()
                .unwrap()
                .manifest()
                .marker()
                .root()
                .cells(),
            2
        );
    }
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    assert!(matches!(
        owner.propose_at_term(term, request_a),
        Err(RuntimeError::StaleCompletion)
    ));
    assert!(matches!(
        owner.propose_at_term(term, request(&route, c, 1, &cut)?),
        Err(RuntimeError::Overloaded)
    ));
    let updated = images::image(Some(&bytes), "UPDATE value SET n=n+1;")?;
    let mutation = images::Cut::new(&bytes, &updated, 1, true, false);
    let accepted = owner.propose_at_term(term, request(&route, a.clone(), 2, &mutation)?)?;
    assert_eq!(
        accepted.position.index.get(),
        second.position.index.get() + 1
    );
    cluster.drive(false).await?;
    for voter in &cluster.voters {
        let (leaf, _) = voter
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .pin_cell(&a.cell)
            .context("updated cell")?;
        assert_eq!(leaf.version.sequence.get(), 2);
    }
    cluster.close().await
}

pub(super) async fn mutation_retries() -> Result<()> {
    let mut cluster = Cluster::with_options(RaftRuntimeOptions {
        max_requests: 1,
        ..Default::default()
    })
    .await?;
    cluster.elect().await?;
    let route = cluster.birth.route().clone();
    let a = history(&route, "retry-a")?;
    let b = history(&route, "retry-b")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    for cell in [&a, &b] {
        cluster.voters[0].owner.as_mut().unwrap().propose(request(
            &route,
            cell.clone(),
            1,
            &cut,
        )?)?;
        cluster.drive(false).await?;
    }
    let updated = images::image(Some(&bytes), "UPDATE value SET n=n+1;")?;
    let cut = images::Cut::new(&bytes, &updated, 1, true, false);
    let mutation_a = request(&route, a.clone(), 2, &cut)?;
    let mutation_b = request(&route, b.clone(), 2, &cut)?;
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    let term = owner.status()?.term;
    let accepted = owner.propose_at_term(term, mutation_a.clone())?;
    // Model a lost acceptance response: the same bytes recover its original
    // position without another append, even when acceptance capacity is full.
    let retry = owner.propose_at_term(term, mutation_a.clone())?;
    assert_eq!(accepted.position, retry.position);
    assert_eq!(accepted.digest, retry.digest);
    assert!(matches!(
        owner.propose_at_term(term, mutation_b.clone()),
        Err(RuntimeError::Overloaded)
    ));
    let different = images::image(Some(&bytes), "UPDATE value SET n=n+99;")?;
    let cut = images::Cut::new(&bytes, &different, 1, true, false);
    assert!(matches!(
        owner.propose_at_term(term, request(&route, a.clone(), 2, &cut)?),
        Err(RuntimeError::Invalid(_))
    ));
    cluster.drive(false).await?;
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    // The applied image excludes the old base after its acceptance record is
    // released. The caller must inspect its receipt; this is not a failed write.
    assert!(matches!(
        owner.propose_at_term(term, mutation_a),
        Err(RuntimeError::StaleCompletion)
    ));
    let accepted_b = owner.propose_at_term(term, mutation_b)?;
    assert_eq!(
        accepted_b.position.index.get(),
        accepted.position.index.get() + 1
    );
    cluster.drive(false).await?;
    for voter in &cluster.voters {
        for cell in [&a, &b] {
            let (leaf, CellImagePin::Materialized(pin)) = voter
                .owner
                .as_ref()
                .unwrap()
                .manifest()
                .pin_cell(&cell.cell)
                .context("cell")?
            else {
                anyhow::bail!("materialized cell")
            };
            assert_eq!(leaf.version.sequence.get(), 2);
            assert_eq!(images::read(&pin)?, updated);
        }
    }
    cluster.close().await
}

pub(super) async fn restart_capacity() -> Result<()> {
    let mut cluster = Cluster::with_options(RaftRuntimeOptions {
        max_cells_per_group: 1,
        ..Default::default()
    })
    .await?;
    cluster.elect().await?;
    let route = cluster.birth.route().clone();
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let a = history(&route, "inherited")?;
    let b = history(&route, "extra")?;
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    let term = owner.status()?.term;
    let old = owner.propose_at_term(term, request(&route, a.clone(), 1, &cut)?)?;
    cluster.drive(true).await?;
    assert_eq!(
        cluster.voters[0]
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .marker()
            .root()
            .cells(),
        0
    );
    // Recover every voter from disk. Live followers retain the old leader's
    // lease and may correctly reject an immediate pre-vote from a lone restart.
    for n in 0..3 {
        cluster.restart(n).await?;
    }
    cluster.voters[0].paused = true;
    cluster.voters[0].owner.as_mut().unwrap().campaign()?;
    cluster.drive(false).await?;
    assert!(cluster.voters[0].held.is_some());
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    assert!(owner.status()?.term > term);
    assert!(!owner.status()?.ready);
    assert!(matches!(
        owner.propose(request(&route, b.clone(), 1, &cut)?),
        Err(RuntimeError::NotReady)
    ));
    assert!(matches!(
        owner.propose_at_term(term, request(&route, a.clone(), 1, &cut)?),
        Err(RuntimeError::StaleCompletion)
    ));
    let voter = &mut cluster.voters[0];
    voter.paused = false;
    let work = voter.held.take().context("held inherited apply")?;
    assert_eq!(work.ticket.position(), old.position);
    let next = prepare(voter, &work)?;
    voter
        .owner
        .as_mut()
        .unwrap()
        .complete_apply(work.ticket, next)?;
    cluster.drive(false).await?;
    let owner = cluster.voters[0].owner.as_mut().unwrap();
    assert!(owner.status()?.ready);
    assert_eq!(owner.manifest().marker().root().cells(), 1);
    assert!(matches!(
        owner.propose(request(&route, b, 1, &cut)?),
        Err(RuntimeError::Overloaded)
    ));
    cluster.close().await
}
