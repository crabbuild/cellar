// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public Storage erasure preserves Raft receipt identity and private rollback.

use super::*;

fn sandboxed(storage: &Storage) -> Result<()> {
    ensure!(storage.sql("COMMIT", &[]).is_err());
    ensure!(storage
        .sql("SELECT * FROM __sys_raft_receipts", &[])
        .is_err());
    ensure!(storage.sql("SELECT random()", &[]).is_err());
    ensure!(storage.sql("PRAGMA foreign_keys=OFF", &[]).is_err());
    Ok(())
}

pub(super) fn register(class: RaftClass) -> Result<RaftClass> {
    Ok(class
        .command("EraseReject", [224; 32], |context, _| {
            let storage = context.storage()?;
            storage.delete_all()?;
            storage.put_json("rejected-erase", &json!(true))?;
            anyhow::bail!("reject the erased private image")
        })?
        .command("EraseRollback", [226; 32], |context, _| {
            let storage = context.storage()?;
            let before = storage.get_json("value")?;
            storage.tx(|tx| {
                tx.delete_all()?;
                ensure!(tx.get_json("value")?.is_none());
                ensure!(tx.sql("SELECT * FROM counter", &[]).is_err());
                tx.put_json("rolled-erase", &json!(true))?;
                tx.rollback()
            })?;
            ensure!(storage.get_json("value")? == before);
            ensure!(storage.get_json("rolled-erase")?.is_none());
            ensure!(storage.get_json("rejected-erase")?.is_none());
            ensure!(storage.sql("SELECT value FROM counter", &[])?.1 == vec![vec![json!(7)]]);
            let mut held = storage.sql_cursor(
                "SELECT a.n FROM cursor_items a CROSS JOIN cursor_items b", &[],
            )?;
            ensure!(storage.delete_all().is_err(), "live SQL cursor must refuse erasure");
            held.close()?;
            ensure!(storage.get_json("value")? == before);
            ensure!(storage.sql("SELECT value FROM counter", &[])?.1 == vec![vec![json!(7)]]);
            sandboxed(&storage)?;
            Ok(Vec::new())
        })?
        .command("Erase", [227; 32], |context, _| {
            let storage = context.storage()?;
            storage.sql(
                "CREATE TABLE \"erase\"\"parent\"(v PRIMARY KEY,child REFERENCES erase_child(v) DEFERRABLE INITIALLY DEFERRED);
                 CREATE TABLE erase_child(v PRIMARY KEY,parent REFERENCES \"erase\"\"parent\"(v));
                 INSERT INTO \"erase\"\"parent\" VALUES(1,1); INSERT INTO erase_child VALUES(1,1)", &[],
            )?;
            storage.delete_all()?;
            ensure!(storage.get_json("value")?.is_none());
            ensure!(storage.list(&Default::default())?.is_empty());
            for name in ["counter", "cursor_items", "erase_child"] {
                ensure!(storage.sql(&format!("SELECT * FROM {name}"), &[]).is_err());
            }
            ensure!(storage.sql("SELECT * FROM \"erase\"\"parent\"", &[]).is_err());
            sandboxed(&storage)?;
            Ok(b"erased".to_vec())
        })?
        .command("Recreate", [228; 32], |context, _| {
            let storage = context.storage()?;
            storage.sql("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(23)", &[])?;
            storage.put_json("value", &json!(23))?;
            sandboxed(&storage)?;
            Ok(23i64.to_be_bytes().to_vec())
        })?
        .query("InspectErasure", [229; 32], |context, _| {
            let storage = context.storage();
            ensure!(storage.delete_all().is_err(), "committed query cannot erase");
            let value = storage.get_json("value")?;
            if value.is_none() {
                ensure!(storage.sql("SELECT value FROM counter", &[]).is_err());
            } else {
                ensure!(storage.sql("SELECT value FROM counter", &[])?.1 == vec![vec![json!(23)]]);
            }
            Ok(serde_json::to_vec(&value)?)
        })?)
}

pub(super) async fn exercise(
    first: &RaftCellHandle,
    second: &RaftCellHandle,
    retry: u64,
    original: &CommandReceipt,
    other: &CommandReceipt,
    calls: &AtomicUsize,
) -> Result<()> {
    let options = |sequence| -> Result<CommandOptions> {
        Ok(CommandOptions::new(
            CommandId::new(ClientId::new([230; 16])?, sequence)?,
            retry,
        ))
    };
    let rejected = first
        .command(
            &first.contract("EraseReject")?,
            Vec::new(),
            CommandOptions::new(CommandId::new(ClientId::new([224; 16])?, 1)?, retry),
        )
        .await
        .expect_err("application rejects the erased private image");
    ensure!(rejected
        .to_string()
        .contains("reject the erased private image"));
    first
        .command(&first.contract("EraseRollback")?, Vec::new(), options(1)?)
        .await?;
    let erased = first
        .command(
            &first.contract("Erase")?,
            Vec::new(),
            CommandOptions {
                durability: Durability::S3,
                ..options(2)?
            },
        )
        .await?;
    ensure!(erased.result == b"erased" && erased.durability == Durability::S3);
    let checked = first
        .query(
            ProposalId::new([231; 16])?,
            &first.contract("InspectErasure")?,
            Vec::new(),
            QueryOptions {
                consistency: QueryConsistency::AtLeast(erased.version_token),
                ..Default::default()
            },
        )
        .await?;
    ensure!(checked.result == b"null");
    let replay = first
        .command(
            &first.contract("Add")?,
            7i64.to_be_bytes().to_vec(),
            CommandOptions::new(original.id, retry),
        )
        .await?;
    ensure!(replay.result == original.result && replay.version == original.version);
    ensure!(
        calls.load(Ordering::Acquire) == 3,
        "erasure lost an existing receipt"
    );
    let recreated = first
        .command(&first.contract("Recreate")?, Vec::new(), options(3)?)
        .await?;
    // An exact retry of erase must return its old receipt, leaving new state intact.
    let replay = first
        .command(
            &first.contract("Erase")?,
            Vec::new(),
            CommandOptions {
                durability: Durability::S3,
                ..options(2)?
            },
        )
        .await?;
    ensure!(replay.result == erased.result && replay.version == erased.version);
    let checked = first
        .query(
            ProposalId::new([232; 16])?,
            &first.contract("InspectErasure")?,
            Vec::new(),
            QueryOptions {
                consistency: QueryConsistency::AtLeast(recreated.version_token),
                ..Default::default()
            },
        )
        .await?;
    ensure!(checked.result == b"23");
    // The same client sequence cannot acquire a different operation or request.
    let conflict = first
        .command(&first.contract("Recreate")?, Vec::new(), options(2)?)
        .await
        .expect_err("erase sequence cannot acquire another operation");
    ensure!(conflict.cause.refusal() == Some(&CommandFailure::CommandConflict));
    let conflict = first
        .command(
            &first.contract("Add")?,
            8i64.to_be_bytes().to_vec(),
            CommandOptions::new(original.id, retry),
        )
        .await
        .expect_err("existing sequence cannot acquire another input");
    ensure!(conflict.cause.refusal() == Some(&CommandFailure::CommandConflict));
    let checked = second
        .query(
            ProposalId::new([233; 16])?,
            &second.contract("Get")?,
            Vec::new(),
            QueryOptions {
                consistency: QueryConsistency::AtLeast(other.version_token.clone()),
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        checked.result == 11i64.to_be_bytes(),
        "erasure crossed cell boundaries"
    );
    Ok(())
}
