//! Failure-only observations of this test's synthetic queue databases.
use rusqlite::{types::ValueRef, Connection, OpenFlags};
use serde_json::{json, Map, Value};
use std::{path::Path, time::SystemTime};

const ROW_LIMIT: usize = 32;

fn rows(connection: &Connection, sql: &str) -> rusqlite::Result<Value> {
    let mut statement = connection.prepare(sql)?;
    let names: Vec<_> = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let mut cursor = statement.query([])?;
    let mut values = Vec::new();
    while let Some(row) = cursor.next()? {
        if values.len() == ROW_LIMIT {
            return Ok(json!({"rows": values, "truncated": true}));
        }
        let mut value = Map::new();
        for (index, name) in names.iter().enumerate() {
            let field = match row.get_ref(index)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(value) => json!(value),
                ValueRef::Real(value) => json!(value),
                ValueRef::Text(value) => {
                    json!(String::from_utf8_lossy(&value[..value.len().min(256)]))
                }
                ValueRef::Blob(_) => json!("blob omitted"),
            };
            value.insert(name.clone(), field);
        }
        values.push(Value::Object(value));
    }
    Ok(json!({"rows": values, "truncated": false}))
}

fn database(path: &Path) -> rusqlite::Result<Value> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(std::time::Duration::ZERO)?;
    // One read transaction keeps the lease, alarm and wake record consistent.
    // Dropping the connection also releases the snapshot on any query error.
    connection.execute_batch("BEGIN")?;
    let messages = rows(&connection, "SELECT id,seq,size,attempts,lease_id,lease_generation,leased_until,visible_at,enqueued_at,purge_on_settle FROM __sys_queue_messages ORDER BY seq LIMIT 33")?;
    let alarms = rows(
        &connection,
        "SELECT at_ms,retry,counted_retry,generation FROM __sys_cell_alarm LIMIT 33",
    )?;
    let wakes = rows(
        &connection,
        "SELECT epoch,sequence,at_ms FROM __sys_cell_wake LIMIT 33",
    )?;
    Ok(json!({"messages": messages, "alarms": alarms, "wakes": wakes}))
}

/// Never opens an application connection or attempts recovery. Errors remain
/// observations, so a damaged or locked database cannot replace the test error.
pub(super) fn capture(node: &Path) -> Value {
    let observed_unix_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
    let mut databases = Vec::new();
    let mut discovery_errors = Vec::new();
    let mut truncated = false;
    let cells = match std::fs::read_dir(node) {
        Ok(cells) => cells,
        Err(error) => {
            return json!({"observed_unix_ms": observed_unix_ms, "discovery_error": format!("{:?}", error.kind()), "databases": []})
        }
    };
    for (index, cell) in cells.enumerate() {
        if index == ROW_LIMIT {
            truncated = true;
            break;
        }
        let cell = match cell {
            Ok(cell) => cell,
            Err(error) => {
                discovery_errors.push(format!("{:?}", error.kind()));
                continue;
            }
        };
        if !cell.file_name().to_string_lossy().starts_with("__Queue:")
            || !cell.file_type().is_ok_and(|kind| kind.is_dir())
        {
            continue;
        }
        let epochs = match std::fs::read_dir(cell.path().join("ltx")) {
            Ok(epochs) => epochs,
            Err(error) => {
                discovery_errors.push(format!("{:?}", error.kind()));
                continue;
            }
        };
        for (index, epoch) in epochs.enumerate() {
            if index == ROW_LIMIT || databases.len() == ROW_LIMIT {
                truncated = true;
                break;
            }
            let epoch = match epoch {
                Ok(epoch) => epoch,
                Err(error) => {
                    discovery_errors.push(format!("{:?}", error.kind()));
                    continue;
                }
            };
            if !epoch.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            let relative = format!(
                "{}/ltx/{}/db.sqlite",
                cell.file_name().to_string_lossy(),
                epoch.file_name().to_string_lossy()
            );
            let state = match database(&epoch.path().join("db.sqlite")) {
                Ok(state) => state,
                Err(error) => json!({"error": error.to_string()}),
            };
            databases.push(json!({"database": relative, "state": state}));
        }
        if truncated {
            break;
        }
    }
    databases.sort_by(|a, b| a["database"].as_str().cmp(&b["database"].as_str()));
    json!({"observed_unix_ms": observed_unix_ms, "databases": databases, "truncated": truncated,
        "discovery_errors": discovery_errors.into_iter().take(ROW_LIMIT).collect::<Vec<_>>()})
}
