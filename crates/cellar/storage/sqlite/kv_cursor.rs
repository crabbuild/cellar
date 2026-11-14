// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A keyset position owns no SQLite statement, connection or read snapshot.

use super::{list_query, prefix_upper_bound, read_cell_kv, stored_value, StoredValue};
use rusqlite::OptionalExtension;
use std::ops::Bound;

pub(crate) struct KvCursorRange {
    lower: Bound<String>,
    end: Option<String>,
    remaining: usize,
    reverse: bool,
}

impl KvCursorRange {
    pub(crate) fn new(
        begin: Option<&str>,
        end: Option<&str>,
        start_after: Option<&str>,
        prefix: Option<&str>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Self {
        // Keep one bound per direction. Redundant prefix/range predicates can
        // make SQLite rescan already visited keys at every iterator step.
        let lower = match (begin.max(prefix), start_after) {
            (Some(begin), Some(after)) if begin > after => Bound::Included(begin.into()),
            (_, Some(after)) => Bound::Excluded(after.into()),
            (Some(begin), None) => Bound::Included(begin.into()),
            (None, None) => Bound::Unbounded,
        };
        let end = end
            .map(str::to_string)
            .into_iter()
            .chain(prefix.and_then(prefix_upper_bound))
            .min();
        Self {
            lower,
            end,
            remaining: limit.unwrap_or(usize::MAX).min(i64::MAX as usize),
            reverse,
        }
    }

    pub(crate) fn next(&mut self, scope: &str) -> anyhow::Result<Option<(String, StoredValue)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let (begin, after) = match &self.lower {
            Bound::Included(key) => (Some(key.as_str()), None),
            Bound::Excluded(key) => (None, Some(key.as_str())),
            Bound::Unbounded => (None, None),
        };
        let row = read_cell_kv(scope, |connection| {
            let (query, parameters) = list_query(
                scope,
                begin,
                self.end.as_deref(),
                after,
                None,
                Some(1),
                self.reverse,
            );
            let mut statement = connection.prepare_cached(&query)?;
            Ok(statement
                .query_row(rusqlite::params_from_iter(parameters), |row| {
                    Ok((row.get::<_, String>(0)?, stored_value(row.get_ref(1)?)))
                })
                .optional()?)
        })?;
        if let Some((key, _)) = &row {
            if self.reverse {
                self.end = Some(key.clone());
            } else {
                self.lower = Bound::Excluded(key.clone());
            }
            self.remaining -= 1;
        } else {
            self.remaining = 0;
        }
        Ok(row)
    }
}
