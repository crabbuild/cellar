// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared noncommutative SQL workload for Raft, Fleet and bucket measurements.
use anyhow::{ensure, Result};
use cellar::sdk::consensus::CommandTransaction;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct State {
    pub revision: u64,
    pub left: i64,
    pub right: i64,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Change {
    Initialize {
        database_bytes: usize,
    },
    /// Dense benchmark setup only; each block contains one MiB of hash output.
    Populate {
        seed: u64,
        block: usize,
        blocks: usize,
    },
    Transfer {
        expected_revision: u64,
        value: i64,
        payload: String,
    },
    Set {
        expected_revision: u64,
        value: i64,
        payload: String,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Transition {
    pub state: State,
    pub changed: bool,
}

pub(crate) const READ_SQL: &str =
    "SELECT revision, left_value, right_value, payload_sha256 FROM workload WHERE id=1";

pub(crate) fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<State> {
    Ok(State {
        revision: row.get(0)?,
        left: row.get(1)?,
        right: row.get(2)?,
        payload_sha256: row.get(3)?,
    })
}

// Every adapter executes these same SQL statements and bindings. Receipt and
// durability machinery belongs to the hosting path and is measured separately.
pub(crate) trait Sql {
    fn execute(&self, statement: &str, params: &[rusqlite::types::Value]) -> Result<usize>;

    fn state(&self) -> Result<State>;
}

impl Sql for CommandTransaction<'_> {
    fn execute(&self, statement: &str, params: &[rusqlite::types::Value]) -> Result<usize> {
        self.execute(statement, rusqlite::params_from_iter(params))
    }

    fn state(&self) -> Result<State> {
        self.query_row(READ_SQL, [], decode)
    }
}

impl Sql for rusqlite::Connection {
    fn execute(&self, statement: &str, params: &[rusqlite::types::Value]) -> Result<usize> {
        Ok(self.execute(statement, rusqlite::params_from_iter(params))?)
    }

    fn state(&self) -> Result<State> {
        Ok(self.query_row(READ_SQL, [], decode)?)
    }
}

pub(crate) fn apply(sql: &impl Sql, change: Change) -> Result<Transition> {
    if let Change::Populate {
        seed,
        block,
        blocks,
    } = change
    {
        ensure!(
            (1..=4).contains(&blocks) && block.checked_add(blocks).is_some_and(|end| end <= 1024),
            "dense seed bound"
        );
        sql.execute("CREATE TABLE IF NOT EXISTS dense_padding(block INTEGER PRIMARY KEY, bytes BLOB NOT NULL)", &[])?;
        for index in block..block + blocks {
            let mut bytes = Vec::with_capacity(1024 * 1024);
            for offset in 0..32768u64 {
                let mut hash = Sha256::new();
                hash.update(seed.to_le_bytes());
                hash.update((index as u64).to_le_bytes());
                hash.update(offset.to_le_bytes());
                bytes.extend_from_slice(&hash.finalize());
            }
            sql.execute(
                "INSERT INTO dense_padding VALUES(?1,?2)",
                &[i64::try_from(index)?.into(), bytes.into()],
            )?;
        }
        return Ok(Transition {
            state: sql.state()?,
            changed: true,
        });
    }
    if let Change::Initialize { database_bytes } = change {
        ensure!(database_bytes <= 8 * 1024 * 1024, "database seed bound");
        sql.execute("CREATE TABLE workload(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, left_value INTEGER NOT NULL, right_value INTEGER NOT NULL, payload_sha256 TEXT NOT NULL, payload BLOB NOT NULL)", &[])?;
        sql.execute("CREATE TABLE padding(bytes BLOB NOT NULL)", &[])?;
        sql.execute(
            "INSERT INTO workload VALUES(1,0,1000000,0,?1,X'')",
            &[hex::encode(Sha256::digest([])).into()],
        )?;
        sql.execute(
            "INSERT INTO padding VALUES(zeroblob(?1))",
            &[i64::try_from(database_bytes)?.into()],
        )?;
        return Ok(Transition {
            state: sql.state()?,
            changed: true,
        });
    }
    let state = sql.state()?;
    let (expected, left, payload) = match change {
        Change::Transfer {
            expected_revision,
            value,
            payload,
        } => (expected_revision, state.left.checked_sub(value), payload),
        Change::Set {
            expected_revision,
            value,
            payload,
        } => (expected_revision, Some(value), payload),
        Change::Initialize { .. } | Change::Populate { .. } => unreachable!(),
    };
    ensure!(payload.len() <= 64 * 1024, "payload bound");
    let Some(left) = left.filter(|left| (0..=1000000).contains(left)) else {
        return Ok(Transition {
            state,
            changed: false,
        });
    };
    if expected != state.revision {
        return Ok(Transition {
            state,
            changed: false,
        });
    }
    let revision = state
        .revision
        .checked_add(1)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("revision overflow"))?;
    sql.execute("UPDATE workload SET revision=?1,left_value=?2,right_value=?3,payload_sha256=?4,payload=?5 WHERE id=1",
        &[i64::try_from(revision)?.into(), left.into(), (1000000-left).into(), hex::encode(Sha256::digest(payload.as_bytes())).into(), payload.into_bytes().into()])?;
    Ok(Transition {
        state: sql.state()?,
        changed: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_blocks_are_real_bounded_and_unique() -> Result<()> {
        let sql = rusqlite::Connection::open_in_memory()?;
        apply(&sql, Change::Initialize { database_bytes: 0 })?;
        let before = sql.state()?;
        apply(
            &sql,
            Change::Populate {
                seed: 0,
                block: 0,
                blocks: 2,
            },
        )?;
        let (count, bytes): (i64, i64) = sql.query_row(
            "SELECT count(*),sum(length(bytes)) FROM dense_padding",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!((count, bytes), (2, 2 * 1024 * 1024));
        let first: Vec<u8> =
            sql.query_row("SELECT bytes FROM dense_padding WHERE block=0", [], |row| {
                row.get(0)
            })?;
        assert!(first.iter().any(|byte| *byte != 0));
        assert_eq!(sql.state()?, before);
        assert!(apply(
            &sql,
            Change::Populate {
                seed: 0,
                block: 0,
                blocks: 1
            }
        )
        .is_err());
        assert!(apply(
            &sql,
            Change::Populate {
                seed: 0,
                block: usize::MAX,
                blocks: 4
            }
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn conditional_results_and_payloads_survive_sqlite_reopen() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("state.sqlite");
        let sql = rusqlite::Connection::open(&path)?;
        apply(
            &sql,
            Change::Initialize {
                database_bytes: 4096,
            },
        )?;
        let first = apply(
            &sql,
            Change::Set {
                expected_revision: 0,
                value: 100,
                payload: "first".into(),
            },
        )?;
        assert_eq!(first.state.left, 100);
        let refused = apply(
            &sql,
            Change::Transfer {
                expected_revision: 0,
                value: 3,
                payload: "wrong".into(),
            },
        )?;
        assert!(!refused.changed);
        assert_eq!(refused.state, first.state);
        let second = apply(
            &sql,
            Change::Transfer {
                expected_revision: 1,
                value: 3,
                payload: "second".into(),
            },
        )?;
        assert_eq!(
            (second.state.left, second.state.right, second.state.revision),
            (97, 999903, 2)
        );
        drop(sql);
        let sql = rusqlite::Connection::open(&path)?;
        assert_eq!(sql.state()?, second.state);
        assert_eq!(
            sql.query_row("SELECT payload FROM workload", [], |row| row
                .get::<_, Vec<u8>>(0))?,
            b"second"
        );
        assert_eq!(
            sql.query_row("SELECT length(bytes) FROM padding", [], |row| row
                .get::<_, u64>(0))?,
            4096
        );
        Ok(())
    }
}
