// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::task::Context;

#[test]
#[allow(clippy::disallowed_methods)]
fn binding_io_resumes_its_writer_before_mutating_source_storage() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = crate::storage::CommandDatabase::open(
        &root.path().join("source.sqlite"),
        None,
        16 * 1024 * 1024,
    )?;
    let inputs = crate::storage::CommandInputs {
        now_unix_millis: 1000,
        random: [1; 32],
    };
    database.transaction(inputs, |tx| {
        tx.storage()?.put_json("value", &serde_json::json!(0))
    })?;
    let active = database.begin(inputs, 1)?;
    let storage = active.turn(|tx| tx.storage())?;
    let Owner::RaftCommand(authority) = storage.0.clone() else {
        anyhow::bail!("source command authority required")
    };
    let parked_io = Arc::new(AtomicBool::new(true));
    let io = RaftDispatchIo {
        authority: authority.clone(),
        parked_io: parked_io.clone(),
    };
    let (send, receive) = tokio::sync::oneshot::channel();
    let task = Box::pin(async move {
        receive.await?;
        // A child response becoming ready does not itself grant SQL access.
        assert!(storage.put_json("value", &serde_json::json!(99)).is_err());
        io.resume_writer().await?;
        storage.put_json("value", &serde_json::json!(1))?;
        // A later child can yield the writer again, retaining the same frame.
        io.begin_io()?;
        Ok(())
    });
    active.turn(|_| {
        crate::storage::schedule_raft_dispatch_work(
            &authority,
            task,
            Arc::new(AtomicBool::new(true)),
            parked_io,
            false,
        )
    })?;
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(active.poll_work(&mut cx)?);
    database.park(&active)?;
    assert!(active.can_poll_parked_io());
    assert!(active.poll_parked_io(&mut cx)?);
    send.send(()).map_err(|_| anyhow!("owned binding closed"))?;
    assert!(active.poll_parked_io(&mut cx)?);
    // The parked poll requests a writer and stops before the source mutation.
    assert!(!active.can_poll_parked_io());
    database.continue_checkpoint(&active)?;
    assert!(!active.poll_work(&mut cx)?);
    active.turn(|_| {
        assert_eq!(storage_value(&authority)?, Some(serde_json::json!(1)));
        Ok(())
    })?;
    database.settle(active, Ok(()))
}

fn storage_value(authority: &RaftStorageAuthority) -> Result<Option<serde_json::Value>> {
    Storage::raft_command(authority.clone()).get_json("value")
}

#[test]
#[allow(clippy::disallowed_methods)]
fn query_writer_hint_requires_proof_and_excludes_mixed_work() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = crate::storage::CommandDatabase::open(
        &root.path().join("query-source.sqlite"),
        None,
        16 * 1024 * 1024,
    )?;
    let inputs = crate::storage::CommandInputs {
        now_unix_millis: 1000,
        random: [2; 32],
    };
    database.transaction(inputs, |tx| {
        tx.storage()?;
        Ok(())
    })?;
    let active = database.begin(inputs, 2)?;
    let storage = active.turn(|tx| tx.storage())?;
    let Owner::RaftCommand(authority) = storage.0.clone() else {
        anyhow::bail!("source command authority required")
    };
    let proven = Arc::new(AtomicBool::new(false));
    active.turn(|_| {
        crate::storage::schedule_raft_dispatch_work(
            &authority,
            Box::pin(std::future::pending()),
            proven.clone(),
            Arc::new(AtomicBool::new(true)),
            true,
        )
    })?;
    assert!(!active.waiting_on_queries());
    proven.store(true, Ordering::Release);
    assert!(active.waiting_on_queries());
    assert!(active.can_park()?);
    let checkpoint = active.turn(|_| Ok(crate::storage::commit_raft_work(&authority)))?;
    assert!(!active.waiting_on_queries());
    active
        .take_checkpoint()
        .unwrap()
        .send(Ok(()))
        .map_err(|_| anyhow!("checkpoint cancelled"))?;
    drop(checkpoint);
    assert!(active.waiting_on_queries());
    // Generic external I/O must retain the ordinary park/resume policy.
    active.turn(|_| {
        crate::storage::schedule_raft_dispatch_work(
            &authority,
            Box::pin(std::future::pending()),
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            false,
        )
    })?;
    assert!(!active.waiting_on_queries());
    active.cancel_work()?;
    assert!(!active.waiting_on_queries());
    database.settle(active, Ok(()))
}
