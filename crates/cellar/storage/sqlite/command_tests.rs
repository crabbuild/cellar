// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn binding_completion_preserves_only_an_already_clean_read_watermark() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = super::super::CommandDatabase::open(
        &root.path().join("completion.sqlite"),
        None,
        16 << 20,
    )?;
    let inputs = super::super::CommandInputs {
        now_unix_millis: 1000,
        random: [1; 32],
    };
    database.transaction(inputs, |tx| {
        tx.storage()?.system_sql(
            "CREATE TABLE __sys_completion_test(value INTEGER NOT NULL)",
            &[],
        )?;
        tx.storage()?
            .system_sql("INSERT INTO __sys_completion_test VALUES(0)", &[])?;
        tx.storage()?.put_json("value", &serde_json::json!(0))
    })?;
    for (quorum, dirty, failed) in [
        (true, false, false),
        (true, true, false),
        (false, false, false),
        (true, false, true),
    ] {
        let active = database.begin(inputs, 1)?;
        let storage = active.turn(|tx| tx.storage())?;
        // This unit tests the marker guard, not quorum or archive authority;
        // the public RF3 regression owns those proofs and final receipts.
        active.mark_quorum_read_base(quorum);
        if quorum {
            // This unit supplies the owner's watermark; public RF3 cases own
            // the actual checkpoint/receipt proof, including a fresh invocation.
            active.mark_checkpoint_proven();
        }
        if dirty {
            active.turn(|_| storage.put_json("value", &serde_json::json!(1)))?;
        }
        active.turn(|_| {
            let result = with_binding_completion(|connection| {
                super::super::as_framework_sql(|| {
                    connection.execute("UPDATE __sys_completion_test SET value=value+1", [])
                })?;
                if failed {
                    anyhow::bail!("completion did not succeed");
                }
                Ok(())
            })
            .ok_or_else(|| anyhow::anyhow!("active command"))?;
            assert_eq!(result.is_err(), failed);
            let connection = active.frame.connection()?;
            assert_eq!(
                active.frame.quorum_read_base.get() == Some(command_marker(&connection)?),
                quorum && !dirty && !failed
            );
            if quorum && !dirty && !failed {
                // The read-only exemption cannot authorize an external effect
                // with unproven receipt/lane bookkeeping in the private image.
                let gate = confirm_checkpoint_work(&storage.raft_command_authority()?);
                let checkpoint = active
                    .take_checkpoint()
                    .context("provider effect reused only the read watermark")?;
                drop(gate);
                drop(checkpoint);
            }
            Ok(())
        })?;
        if quorum && !dirty && !failed {
            active.turn(|_| {
                storage.sql("CREATE TABLE after_completion(value INTEGER)", &[])?;
                assert_ne!(
                    active.frame.quorum_read_base.get(),
                    Some(command_marker(&*active.frame.connection()?)?)
                );
                storage.put_json("value", &serde_json::json!(2))?;
                assert_ne!(
                    active.frame.quorum_read_base.get(),
                    Some(command_marker(&*active.frame.connection()?)?)
                );
                Ok(())
            })?;
        }
        database.settle(active, Ok(()))?;
    }
    Ok(())
}
