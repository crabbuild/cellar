// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use std::collections::BTreeMap;

pub type SqlRow = BTreeMap<String, SqlValue>;

/// An owned cursor token, never a SQLite pointer. Each step requires the
/// original activation installed by the native turn driver.
pub struct SqlCursor {
    storage: Storage,
    id: u64,
    columns: Vec<String>,
    prefetched: Option<Vec<SqlValue>>,
    deferred_error: Option<anyhow::Error>,
    rows_read: u64,
    rows_written: u64,
}

impl SqlCursor {
    pub(super) fn new(storage: &Storage, query: &str, bindings: &[SqlValue]) -> Result<Self> {
        let scope = storage.scope()?;
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
        Ok(Self {
            storage: Storage(storage.0.clone(), None),
            id,
            columns,
            prefetched,
            deferred_error: None,
            rows_read,
            rows_written,
        })
    }

    pub fn column_names(&self) -> &[String] {
        &self.columns
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
        self.storage.activation_scope()?;
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
        self.storage.activation_scope()?;
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
            self.storage.cleanup(Cleanup::Sql(self.id));
        }
    }
}

pub struct KvCursor {
    storage: Storage,
    id: Option<u64>,
}

impl KvCursor {
    pub(super) fn new(storage: &Storage, options: &ListOptions) -> Result<Self> {
        let id = crate::storage::sync_list_start(
            storage.scope()?,
            options.start.as_deref(),
            options.end.as_deref(),
            options.start_after.as_deref(),
            options.prefix.as_deref(),
            options.limit,
            options.reverse,
        )?;
        Ok(Self {
            storage: Storage(storage.0.clone(), None),
            id: Some(id),
        })
    }
}

impl Iterator for KvCursor {
    type Item = Result<(String, crate::storage::StoredValue)>;
    fn next(&mut self) -> Option<Self::Item> {
        let id = self.id?;
        if let Err(error) = self.storage.scope() {
            return Some(Err(error));
        }
        match crate::storage::sync_list_next(id) {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.id = None;
                None
            }
            Err(error) => {
                self.id = None;
                Some(Err(error))
            }
        }
    }
}

impl Drop for KvCursor {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.storage.cleanup(Cleanup::Kv(id));
        }
    }
}
