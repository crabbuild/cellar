// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// The management service owns its audit file and wall clock outside node execution.
#![allow(clippy::disallowed_methods)]

//! The console owns this SQLite file, separate from every cell's state.
//! Persist intent before dispatch; interrupted calls remain outcome_unknown.

use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::{path::Path, sync::Mutex};

pub struct AuditStore(Mutex<Connection>);

#[derive(Clone, Debug, Serialize)]
pub struct Operation {
    pub id: String,
    pub fleet: String,
    pub actor: String,
    pub target: String,
    pub action: String,
    pub status: String,
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

pub enum Begin {
    Started(Operation),
    Existing(Operation),
}

#[derive(Debug)]
pub(crate) struct AuditPayloadConflict;

impl std::fmt::Display for AuditPayloadConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("operation ID was already used for a different request")
    }
}

impl std::error::Error for AuditPayloadConflict {}

impl AuditStore {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS operations (
                id TEXT NOT NULL, fleet TEXT NOT NULL, actor TEXT NOT NULL,
                target TEXT NOT NULL, action TEXT NOT NULL, fingerprint TEXT NOT NULL,
                status TEXT NOT NULL, started_at INTEGER NOT NULL, finished_at INTEGER,
                PRIMARY KEY (fleet, actor, id)
            );
            CREATE TABLE IF NOT EXISTS event_intents (fleet TEXT NOT NULL, actor TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(fleet,actor,id));",
        )?;
        Ok(Self(Mutex::new(connection)))
    }

    pub fn begin(
        &self,
        id: &str,
        fleet: &str,
        actor: &str,
        target: &str,
        action: &str,
        fingerprint: &str,
    ) -> Result<Begin> {
        ensure!(crate::valid_id(id), "invalid operation ID");
        let mut connection = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("audit store unavailable"))?;
        let tx = connection.transaction()?;
        let existing = tx
            .query_row(
                "SELECT fingerprint,target,action FROM operations WHERE fleet=? AND actor=? AND id=?",
                params![fleet, actor, id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing
                != (
                    fingerprint.to_string(),
                    target.to_string(),
                    action.to_string(),
                )
            {
                return Err(AuditPayloadConflict.into());
            }
            let record = tx.query_row("SELECT id,fleet,actor,target,action,status,started_at,finished_at FROM operations WHERE fleet=? AND actor=? AND id=?", params![fleet, actor, id], decode)?;
            return Ok(Begin::Existing(record));
        }
        let started_at = now();
        tx.execute(
            "INSERT INTO operations VALUES (?,?,?,?,?,?, 'outcome_unknown', ?,NULL)",
            params![id, fleet, actor, target, action, fingerprint, started_at],
        )?;
        tx.commit()?;
        Ok(Begin::Started(Operation {
            id: id.into(),
            fleet: fleet.into(),
            actor: actor.into(),
            target: target.into(),
            action: action.into(),
            status: "outcome_unknown".into(),
            started_at,
            finished_at: None,
        }))
    }

    pub fn finish(&self, operation: &mut Operation, status: &str) -> Result<()> {
        let finished = now();
        let connection = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("audit store unavailable"))?;
        connection.execute(
                "UPDATE operations SET status=?,finished_at=? WHERE fleet=? AND actor=? AND id=? AND (action NOT IN ('stop','outbox_retry','subscription_change','outbox_destroy','outbox_recreate','stream_retire') OR status NOT IN ('succeeded','incomplete','failed')) AND NOT (action IN ('raft_topology','raft_topology_cancel') AND status='accepted')",
                params![
                    status,
                    finished,
                    operation.fleet,
                    operation.actor,
                    operation.id
                ],
            )?;
        // A receipt may have reconciled while the request acknowledgment was
        // in flight. Return the persisted result as well as retaining it on disk.
        *operation = connection.query_row(
            "SELECT id,fleet,actor,target,action,status,started_at,finished_at FROM operations WHERE fleet=? AND actor=? AND id=?",
            params![operation.fleet, operation.actor, operation.id], decode,
        )?;
        Ok(())
    }

    pub(crate) fn reconcile_stop(
        &self,
        fleet: &str,
        actor: &str,
        operation: &str,
        receipt: &cellar::management::StopReceipt,
    ) -> Result<()> {
        if !receipt.domain_joined {
            return Ok(());
        }
        let status = if receipt
            .proof
            .as_ref()
            .is_some_and(|proof| proof.complete(receipt.requested_mode))
        {
            "succeeded"
        } else if receipt.proof.is_some() {
            "incomplete"
        } else {
            "failed"
        };
        self.0.lock().map_err(|_| anyhow::anyhow!("audit store unavailable"))?.execute(
            "UPDATE operations SET status=?,finished_at=? WHERE fleet=? AND actor=? AND id=? AND target=? AND action='stop'",
            params![status,receipt.observed_at,fleet,actor,operation,format!("node/{}@{}",receipt.node,receipt.generation)],
        )?;
        Ok(())
    }

    pub(crate) fn reconcile_event(
        &self,
        fleet: &str,
        actor: &str,
        id: &str,
        target: &str,
        action: &str,
        fingerprint: &str,
    ) -> Result<bool> {
        self.reconcile_event_state(fleet, actor, id, target, (action, fingerprint), "succeeded")
    }

    pub(crate) fn reconcile_event_state(
        &self,
        fleet: &str,
        actor: &str,
        id: &str,
        target: &str,
        expected: (&str, &str),
        status: &str,
    ) -> Result<bool> {
        let (action, fingerprint) = expected;
        ensure!(
            matches!(status, "succeeded" | "accepted"),
            "invalid event receipt state"
        );
        ensure!(
            matches!(
                action,
                "outbox_retry"
                    | "subscription_change"
                    | "outbox_destroy"
                    | "outbox_recreate"
                    | "stream_retire"
            ),
            "invalid event audit action"
        );
        let changed = self.0.lock().map_err(|_| anyhow::anyhow!("audit store unavailable"))?.execute(
            "UPDATE operations SET status=?,finished_at=? WHERE fleet=? AND actor=? AND id=? AND target=? AND action=? AND fingerprint=? AND (status!='succeeded' OR ?='succeeded')",
            params![status,now(),fleet,actor,id,target,action,fingerprint,status],
        )?;
        Ok(changed == 1)
    }

    pub(crate) fn remember_event(
        &self,
        fleet: &str,
        actor: &str,
        id: &str,
        body: &impl Serialize,
    ) -> Result<()> {
        let body = serde_json::to_string(body)?;
        ensure!(body.len() <= 16 << 10, "event intent exceeds bounds");
        self.0
            .lock()
            .map_err(|_| anyhow::anyhow!("audit store unavailable"))?
            .execute(
                "INSERT OR IGNORE INTO event_intents(fleet,actor,id,body) VALUES(?,?,?,?)",
                params![fleet, actor, id, body],
            )?;
        Ok(())
    }

    pub(crate) fn event_intent(
        &self,
        fleet: &str,
        actor: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let body: Option<String> = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("audit store unavailable"))?
            .query_row(
                "SELECT body FROM event_intents WHERE fleet=? AND actor=? AND id=?",
                params![fleet, actor, id],
                |row| row.get(0),
            )
            .optional()?;
        body.map(|body| serde_json::from_str(&body).map_err(Into::into))
            .transpose()
    }

    pub fn list(&self, fleet: &str) -> Result<Vec<Operation>> {
        let connection = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("audit store unavailable"))?;
        let mut statement = connection.prepare("SELECT id,fleet,actor,target,action,status,started_at,finished_at FROM operations WHERE fleet=? ORDER BY started_at DESC, id DESC LIMIT 100")?;
        let values = statement
            .query_map([fleet], decode)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("read operations")?;
        Ok(values)
    }
}

fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<Operation> {
    Ok(Operation {
        id: row.get(0)?,
        fleet: row.get(1)?,
        actor: row.get(2)?,
        target: row.get(3)?,
        action: row.get(4)?,
        status: row.get(5)?,
        started_at: row.get(6)?,
        finished_at: row.get(7)?,
    })
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_receipts_reconcile_exact_actor_target_and_survive_late_errors() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let audit = AuditStore::open(&directory.path().join("audit.sqlite"))?;
        let Begin::Started(mut operation) = audit.begin(
            "event-one",
            "fleet",
            "alice",
            "outbox/Order:one",
            "outbox_retry",
            "request",
        )?
        else {
            anyhow::bail!("new intent expected")
        };
        audit.reconcile_event(
            "fleet",
            "bob",
            "event-one",
            "outbox/Order:one",
            "outbox_retry",
            "request",
        )?;
        audit.reconcile_event(
            "fleet",
            "alice",
            "event-one",
            "outbox/Order:other",
            "outbox_retry",
            "request",
        )?;
        assert!(!audit.reconcile_event(
            "fleet",
            "alice",
            "event-one",
            "outbox/Order:one",
            "outbox_retry",
            "different-request"
        )?);
        assert_eq!(audit.list("fleet")?[0].status, "outcome_unknown");
        audit.reconcile_event(
            "fleet",
            "alice",
            "event-one",
            "outbox/Order:one",
            "outbox_retry",
            "request",
        )?;
        audit.finish(&mut operation, "outcome_unknown")?;
        assert_eq!(operation.status, "succeeded");
        Ok(())
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[test]
    fn lifecycle_intents_are_actor_scoped_and_terminal_proofs_survive_late_acceptance() -> Result<()>
    {
        let dir = tempfile::tempdir()?;
        let store = AuditStore::open(&dir.path().join("audit.sqlite"))?;
        let body = serde_json::json!({"operation_id":"a".repeat(32),"action":{"kind":"destroy","reason":"retire source"}});
        let Begin::Started(mut operation) = store.begin(
            "operation",
            "fleet",
            "alice",
            "outbox/Order:one",
            "outbox_destroy",
            "digest",
        )?
        else {
            panic!("new operation")
        };
        store.remember_event("fleet", "alice", "operation", &body)?;
        assert_eq!(
            store.event_intent("fleet", "alice", "operation")?,
            Some(body)
        );
        assert_eq!(store.event_intent("fleet", "bob", "operation")?, None);
        assert!(!store.reconcile_event_state(
            "fleet",
            "alice",
            "operation",
            "outbox/Order:one",
            ("outbox_destroy", "wrong"),
            "succeeded"
        )?);
        assert!(store.reconcile_event_state(
            "fleet",
            "alice",
            "operation",
            "outbox/Order:one",
            ("outbox_destroy", "digest"),
            "succeeded"
        )?);
        store.finish(&mut operation, "accepted")?;
        assert_eq!(operation.status, "succeeded");
        assert!(!store.reconcile_event_state(
            "fleet",
            "alice",
            "operation",
            "outbox/Order:one",
            ("outbox_destroy", "digest"),
            "accepted"
        )?);
        Ok(())
    }

    #[test]
    fn fleet_confirmed_admission_survives_late_unknown_retry() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = AuditStore::open(&dir.path().join("raft.sqlite"))?;
        for action in ["raft_topology", "raft_topology_cancel"] {
            let Begin::Started(mut first) =
                store.begin(action, "fleet", "alice", "catalog/op", action, "digest")?
            else {
                panic!("new request")
            };
            let Begin::Existing(mut late) =
                store.begin(action, "fleet", "alice", "catalog/op", action, "digest")?
            else {
                panic!("existing request")
            };
            store.finish(&mut first, "accepted")?;
            for status in ["outcome_unknown", "not_submitted", "accepted"] {
                store.finish(&mut late, status)?;
                assert_eq!(late.status, "accepted");
                assert_eq!(late.finished_at, first.finished_at);
            }
            let error = match store.begin(action, "fleet", "alice", "catalog/op", action, "changed")
            {
                Err(error) => error,
                Ok(_) => panic!("payload conflict"),
            };
            assert!(error.downcast_ref::<AuditPayloadConflict>().is_some());
        }
        Ok(())
    }
}
