// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::collections::BTreeMap;

pub type SqlRow = BTreeMap<String, SqlValue>;

/// An owned cursor token, never a SQLite pointer. Each step requires the
/// original activation, Raft invocation or committed query installed by its driver.
pub struct SqlCursor {
    storage: Storage,
    id: u64,
    columns: Vec<String>,
    prefetched: Option<Vec<SqlValue>>,
    deferred_error: Option<anyhow::Error>,
    rows_read: u64,
    rows_written: u64,
    raft_lifetime: Option<Arc<AtomicBool>>,
}

impl SqlCursor {
    pub(super) fn new(storage: &Storage, query: &str, bindings: &[SqlValue]) -> Result<Self> {
        let storage = storage.cursor_view()?;
        let scope = storage.data_scope()?;
        let bindings: Vec<_> = bindings
            .iter()
            .map(|value| match value {
                SqlValue::Null => rusqlite::types::Value::Null,
                SqlValue::Integer(value) => rusqlite::types::Value::Integer(*value),
                SqlValue::Real(value) => rusqlite::types::Value::Real(*value),
                SqlValue::Text(value) => rusqlite::types::Value::Text(value.clone()),
                SqlValue::Blob(value) => rusqlite::types::Value::Blob(value.clone()),
            })
            .collect();
        let (id, columns, prefetched, rows_written, _) =
            crate::storage::sql_cursor_start_values(scope, query, &bindings)
                .map_err(anyhow::Error::msg)?;
        let rows_read = u64::from(prefetched.is_some());
        let raft_lifetime = if matches!(storage.0, Owner::Native(_)) || id == 0 {
            None
        } else {
            crate::storage::raft_sql_cursor_lifetime(id)
        };
        Ok(Self {
            storage,
            id,
            columns,
            prefetched,
            deferred_error: None,
            rows_read,
            rows_written,
            raft_lifetime,
        })
    }

    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    /// Retained column/row buffers and error text, for adapter payload quotas.
    /// This excludes SQLite VM/page-cache allocations and is not an RSS limit.
    pub fn buffered_bytes(&self) -> usize {
        let columns = self.columns.iter().fold(
            self.columns
                .capacity()
                .saturating_mul(std::mem::size_of::<String>()),
            |total, name| total.saturating_add(name.capacity()),
        );
        let row = self.prefetched.as_ref().map_or(0, |row| {
            row.iter().fold(
                row.capacity()
                    .saturating_mul(std::mem::size_of::<SqlValue>()),
                |total, value| {
                    total.saturating_add(match value {
                        SqlValue::Text(value) => value.capacity(),
                        SqlValue::Blob(value) => value.capacity(),
                        _ => 0,
                    })
                },
            )
        });
        let error = self
            .deferred_error
            .as_ref()
            .map_or(0, |error| error.to_string().len());
        columns.saturating_add(row).saturating_add(error)
    }

    pub fn rows_read(&self) -> u64 {
        self.rows_read
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Keep the last good row when the following step fails. The failure is
    /// delivered on the next call, as in the original lazy cursor.
    pub fn next_raw(&mut self) -> Result<Option<Vec<SqlValue>>> {
        if self.prefetched.is_none() && self.deferred_error.is_none() {
            return Ok(None);
        }
        self.storage.check_transaction_authority()?;
        self.storage.data_activation_scope()?;
        if let Some(error) = self.deferred_error.take() {
            return Err(error);
        }
        let row = self.prefetched.take();
        if self.id != 0 {
            match crate::storage::sql_cursor_next(self.id) {
                Ok((Some(next), _)) => {
                    self.prefetched = Some(next);
                    self.rows_read += 1;
                }
                Ok((None, written)) => {
                    self.rows_written = written;
                    self.id = 0;
                }
                Err(error) => {
                    self.deferred_error = Some(anyhow!(error));
                    self.id = 0;
                }
            }
        }
        Ok(row)
    }

    pub fn to_array(mut self) -> Result<Vec<SqlRow>> {
        self.by_ref().collect()
    }

    pub fn to_raw_array(mut self) -> Result<Vec<Vec<SqlValue>>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_raw()? {
            rows.push(row);
        }
        Ok(rows)
    }

    pub fn one(mut self) -> Result<SqlRow> {
        let row = self.next().transpose()?.ok_or_else(|| {
            anyhow!("Expected exactly one result from SQL query, but got no results")
        })?;
        anyhow::ensure!(
            self.next().transpose()?.is_none(),
            "Expected exactly one result from SQL query, but got multiple results"
        );
        Ok(row)
    }

    pub fn close(&mut self) -> Result<()> {
        self.storage.data_activation_scope()?;
        if self.id != 0 {
            crate::storage::sql_cursor_close(std::mem::take(&mut self.id));
        }
        self.prefetched = None;
        self.deferred_error = None;
        Ok(())
    }
}

impl Iterator for SqlCursor {
    type Item = Result<SqlRow>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_raw() {
            Ok(Some(row)) => Some(Ok(self.columns.iter().cloned().zip(row).collect())),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

impl Drop for SqlCursor {
    fn drop(&mut self) {
        if self.id != 0 {
            if let Some(cancelled) = &self.raft_lifetime {
                // Only the worker owns SQLite. Outside an installed turn, the
                // next worker poll (or frame teardown) finalizes this statement.
                cancelled.store(true, Ordering::Release);
                if self.storage.data_activation_scope().is_ok() {
                    crate::storage::sql_cursor_close(self.id);
                }
            } else {
                self.storage.cleanup(Cleanup::Sql(self.id));
            }
        }
    }
}

pub struct KvCursor {
    storage: Storage,
    backend: Option<KvBackend>,
}

enum KvBackend {
    Native(u64),
    Raft(u64),
}

#[derive(Default)]
pub(super) struct RaftKvCursor {
    serial: u64,
    range: Option<crate::storage::KvCursorRange>,
}

impl KvCursor {
    pub(super) fn new(storage: &Storage, options: &ListOptions) -> Result<Self> {
        let storage = storage.cursor_view()?;
        let backend = match &storage.0 {
            Owner::Native(_) => KvBackend::Native(crate::storage::sync_list_start(
                storage.scope()?,
                options.start.as_deref(),
                options.end.as_deref(),
                options.start_after.as_deref(),
                options.prefix.as_deref(),
                options.limit,
                options.reverse,
            )?),
            Owner::RaftCommand(_) | Owner::RaftQuery(_) => {
                storage.data_scope()?;
                let range = crate::storage::KvCursorRange::new(
                    options.start.as_deref(),
                    options.end.as_deref(),
                    options.start_after.as_deref(),
                    options.prefix.as_deref(),
                    options.limit,
                    options.reverse,
                );
                let mut cursor = storage.resources().kv_cursor.lock().unwrap();
                let serial = cursor
                    .serial
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("KV cursor generation exhausted"))?;
                cursor.serial = serial;
                cursor.range = Some(range);
                KvBackend::Raft(serial)
            }
        };
        Ok(Self {
            storage,
            backend: Some(backend),
        })
    }
}

impl Iterator for KvCursor {
    type Item = Result<(String, crate::storage::StoredValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        let backend = self.backend.as_mut()?;
        let result = match backend {
            KvBackend::Native(id) => {
                if let Err(error) = self.storage.scope() {
                    return Some(Err(error));
                }
                crate::storage::sync_list_next(*id)
            }
            KvBackend::Raft(serial) => {
                let scope = match self.storage.data_scope() {
                    Ok(scope) => scope,
                    Err(error) => return Some(Err(error)),
                };
                let mut cursor = self.storage.resources().kv_cursor.lock().unwrap();
                if cursor.serial != *serial {
                    Err(anyhow!("sync KV cursor was invalidated"))
                } else {
                    let result = match cursor.range.as_mut() {
                        Some(range) => range.next(scope),
                        None => Err(anyhow!("sync KV cursor was invalidated")),
                    };
                    if !matches!(result, Ok(Some(_))) {
                        cursor.range = None;
                    }
                    result
                }
            }
        };
        match result {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.backend = None;
                None
            }
            Err(error) => {
                self.backend = None;
                Some(Err(error))
            }
        }
    }
}

impl Drop for KvCursor {
    fn drop(&mut self) {
        match self.backend.as_ref() {
            Some(KvBackend::Native(id)) => self.storage.cleanup(Cleanup::Kv(*id)),
            Some(KvBackend::Raft(serial)) => {
                let mut cursor = self.storage.resources().kv_cursor.lock().unwrap();
                if cursor.serial == *serial {
                    cursor.range = None;
                }
            }
            None => (),
        }
    }
}
