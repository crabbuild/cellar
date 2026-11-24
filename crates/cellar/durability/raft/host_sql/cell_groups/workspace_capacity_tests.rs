// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A finite invocation can wait for physical workspace credit before SQL entry.

use super::*;

fn occupy(
    fixture: &Fixture,
    token: &crate::asyncrt::DomainToken,
) -> anyhow::Result<Vec<RootPrivateOptions>> {
    let CellSqlOptions::Extents(options) = &fixture.options else {
        unreachable!("rooted fixture")
    };
    ["Counter:held-first", "Counter:held-second"]
        .into_iter()
        .map(|name| {
            let opening = fixture.opening(token, name)?;
            Ok(options.clone().prepare(&opening.cell)?)
        })
        .collect()
}

fn waits_for_release(byte_limit: bool) -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    // Exercise the two dimensions independently; neither ceiling changes while
    // the invocation is waiting or its earlier physical owner is cleaning up.
    let fixture = Fixture::with_capacity(if byte_limit { 3 } else { 2 }, |charge| {
        if byte_limit {
            2 * charge
        } else {
            3 * charge
        }
    })?;
    let mut roots = occupy(&fixture, &token)?;
    let _release = Release(fixture.files.clone());
    let mut opening = fixture
        .opening(&token, "Counter:waiting")?
        .wait_for_capacity_before(token.mono_ms()? + 10_000);
    let opened = AtomicUsize::new(0);
    let mut poll = |cx: &mut Context<'_>| {
        opening.poll_with(cx, Ok(()), |root| {
            opened.fetch_add(1, Ordering::Relaxed);
            root.ok_or_else(unavailable)
        })
    };
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    assert!(poll(&mut cx).is_pending());
    // The one physical worker has completed the real quota refusal before the
    // second poll. A queued future alone is not evidence of capacity waiting.
    runtime.block_on(token.blocking(|| ())?)?;
    let observed = poll(&mut cx);
    match &observed {
        Poll::Pending => (),
        Poll::Ready(Err(error)) => panic!("transient workspace capacity refused: {error:?}"),
        Poll::Ready(Ok(_)) => panic!("SQL entered without workspace capacity"),
    }
    assert_eq!(opened.load(Ordering::Relaxed), 0);

    fixture.files.cleanup.armed.store(true, Ordering::Release);
    let released = roots.pop().unwrap();
    let cleanup = token.blocking(move || drop(released))?;
    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture.files.cleanup.entered.notified(),
        )
        .await
    })?;
    assert!(poll(&mut cx).is_pending());
    let usage = fixture.images.usage()?;
    assert_eq!(
        (usage.workspaces, usage.workspace_bytes, usage.quarantined),
        (2, 2 * fixture.charge, false)
    );
    assert_eq!(fixture.lane.available_permits(), 0);
    assert_eq!(opened.load(Ordering::Relaxed), 0);
    fixture.files.cleanup.release();
    runtime.block_on(cleanup)?;

    let admitted = runtime.block_on(std::future::poll_fn(poll))?;
    assert_eq!(opened.load(Ordering::Relaxed), 1);
    let usage = fixture.images.usage()?;
    assert_eq!(
        (usage.workspaces, usage.workspace_bytes, usage.quarantined),
        (2, 2 * fixture.charge, false)
    );
    opening.close(&waker)?;
    drop(opening);
    assert_eq!(fixture.lane.available_permits(), 1);
    drop(admitted);
    drop(roots);
    fixture.empty()?;
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn workspace_bytes_wait_for_joined_release() -> anyhow::Result<()> {
    waits_for_release(true)
}

#[test]
fn workspace_items_wait_for_joined_release() -> anyhow::Result<()> {
    waits_for_release(false)
}

#[test]
fn capacity_wait_keeps_original_deadline() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    let roots = occupy(&fixture, &token)?;
    let mut opening = fixture
        .opening(&token, "Counter:deadline")?
        .wait_for_capacity_before(token.mono_ms()? + 10_000);
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    assert!(poll_root(&mut opening, &mut cx).is_pending());
    runtime.block_on(token.blocking(|| ())?)?;
    assert!(poll_root(&mut opening, &mut cx).is_pending());
    let opened = AtomicUsize::new(0);
    let result = runtime.block_on(std::future::poll_fn(|cx| {
        opening.poll_with::<RootPrivateOptions>(
            cx,
            Err(crate::raft::execution::authority_failure(
                RuntimeError::DeadlineExceeded,
            )),
            |_| {
                opened.fetch_add(1, Ordering::Relaxed);
                Err(unavailable())
            },
        )
    }));
    assert!(matches!(result, Err(ExecutionError::Storage(ref error))
        if matches!(error.downcast_ref::<RuntimeError>(), Some(RuntimeError::DeadlineExceeded))));
    assert_eq!(opened.load(Ordering::Relaxed), 0);
    opening.close(&waker)?;
    drop(opening);
    assert_eq!(fixture.images.usage()?.workspaces, 2);
    assert_eq!(fixture.lane.available_permits(), 1);
    drop(roots);
    fixture.empty()?;
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn capacity_wait_closes_before_sql_entry() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    let roots = occupy(&fixture, &token)?;
    let mut opening = fixture
        .opening(&token, "Counter:closing")?
        .wait_for_capacity_before(token.mono_ms()? + 10_000);
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    assert!(poll_root(&mut opening, &mut cx).is_pending());
    runtime.block_on(token.blocking(|| ())?)?;
    assert!(poll_root(&mut opening, &mut cx).is_pending());
    opening.close(&waker)?;
    drop(opening);
    assert_eq!(fixture.images.usage()?.workspaces, 2);
    assert_eq!(fixture.lane.available_permits(), 1);
    drop(roots);
    fixture.empty()?;
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn expired_capacity_wait_enters_no_workspace_io() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    fixture.files.create.armed.store(true, Ordering::Release);
    fixture.files.create.release();
    let mut opening = fixture
        .opening(&token, "Counter:already-expired")?
        .wait_for_capacity_before(token.mono_ms()?);
    let result = runtime.block_on(std::future::poll_fn(|cx| poll_root(&mut opening, cx)));
    assert!(matches!(result, Err(ExecutionError::Storage(ref error))
        if matches!(error.downcast_ref::<RuntimeError>(), Some(RuntimeError::DeadlineExceeded))));
    assert_eq!(fixture.files.create.calls.load(Ordering::Acquire), 0);
    opening.close(&waker())?;
    drop(opening);
    fixture.empty()?;
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn permanently_oversized_workspace_refuses() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::with_capacity(2, |charge| charge - 1)?;
    let mut opening = fixture
        .opening(&token, "Counter:oversized")?
        .wait_for_capacity_before(token.mono_ms()? + 10_000);
    let result = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| poll_root(&mut opening, cx)),
        )
        .await
    })?;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    opening.close(&waker())?;
    drop(opening);
    assert_eq!(fixture.lane.available_permits(), 1);
    fixture.empty()?;
    runtime.block_on(domain.shutdown());
    Ok(())
}
