//! Versioned, retained source administration receipts.

use super::*;

const SCHEMA: super::event_schema::Schema = super::event_schema::Schema {
    marker: "__sys_outbox_control_schema",
    ddl: include_str!("outbox_control.sql"),
    required: &[
        (
            "__sys_outbox_operations",
            "sequence,operation,digest,operator,receipt,size,created_ms",
        ),
        ("__sys_outbox_operation_stats", "singleton,rows"),
    ],
    optional: &[],
};

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<bool> {
    SCHEMA.check(connection)
}

pub(super) fn restore_schema(connection: &Connection) -> anyhow::Result<()> {
    if check_schema(connection)? {
        SCHEMA.open(connection)?;
        let (stored,actual,invalid):(i64,i64,i64)=connection.query_row("SELECT rows,(SELECT count(*) FROM __sys_outbox_operations),(SELECT count(*) FROM __sys_outbox_operations WHERE size!=length(CAST(receipt AS BLOB)) OR size>4096 OR length(operation)!=32 OR length(digest)!=64 OR length(CAST(operator AS BLOB))>256) FROM __sys_outbox_operation_stats WHERE singleton=1",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
        super::event_schema::compatible(
            stored == actual && invalid == 0,
            "source administration receipt accounting is inconsistent",
        )?;
    }
    Ok(())
}

pub(crate) fn ensure_schema(scope: &str) -> anyhow::Result<()> {
    with_application_storage_write(scope, |connection| {
        without_sql_authorizer(connection, || SCHEMA.open(connection))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_receipts_survive_activation_and_corrupt_accounting_fails_closed() -> anyhow::Result<()>
    {
        let c = Connection::open_in_memory()?;
        c.execute_batch("BEGIN")?;
        SCHEMA.open(&c)?;
        let receipt = "{\"operation\":\"00000000000000000000000000000001\"}";
        c.execute("INSERT INTO __sys_outbox_operations(operation,digest,operator,receipt,size,created_ms) VALUES(?1,?2,'outbox',?3,?4,0)",rusqlite::params!["00000000000000000000000000000001","0".repeat(64),receipt,receipt.len() as i64])?;
        c.execute_batch("COMMIT")?;
        super::super::schema(&c)?;
        let retained: String = c.query_row(
            "SELECT receipt FROM __sys_outbox_operations WHERE sequence=1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(retained, receipt);
        c.execute("UPDATE __sys_outbox_operation_stats SET rows=0", [])?;
        let failure = super::super::schema(&c).unwrap_err();
        assert_eq!(
            failure
                .downcast_ref::<crate::application::event_stream::DeliveryError>()
                .unwrap()
                .code,
            crate::application::event_stream::ErrorCode::UnsupportedProtocol
        );
        let retained: String = c.query_row(
            "SELECT receipt FROM __sys_outbox_operations WHERE sequence=1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(retained, receipt);
        Ok(())
    }
}
