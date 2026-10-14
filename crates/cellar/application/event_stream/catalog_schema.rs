use crate::application::{Result, Storage};

pub(crate) fn open(db: &Storage) -> Result<()> {
    let marked = !db.system_sql("SELECT name FROM sqlite_schema WHERE type='table' AND name='__sys_event_catalog_schema'", &[])?.is_empty();
    if marked {
        let columns = db.system_sql("SELECT name,type FROM pragma_table_info('__sys_event_catalog') WHERE name IN ('tenant','application','family','accepted_revision','desired_revision')", &[])?;
        if columns.len() != 5
            || columns.iter().any(|row| {
                let Ok(name) = super::store::text(row, "name") else {
                    return true;
                };
                let expected = if name.ends_with("revision") {
                    "INTEGER"
                } else {
                    "TEXT"
                };
                !super::store::text(row, "type")
                    .is_ok_and(|kind| kind.eq_ignore_ascii_case(expected))
            })
        {
            return Err(super::error::error(
                super::ErrorCode::UnsupportedProtocol,
                "catalog metadata layout requires recreated development state",
            ));
        }
    }
    super::schema::open_schema(
        db,
        "__sys_event_catalog_schema",
        "__sys_event_catalog",
        "__sys_event_catalog_receipts",
        include_str!("catalog.sql"),
    )
}
