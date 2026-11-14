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
