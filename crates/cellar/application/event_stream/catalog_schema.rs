use crate::application::{Result, Storage};

pub(crate) fn open(db: &Storage) -> Result<()> {
    super::schema::open_schema(
        db,
        "__sys_event_catalog_schema",
        "__sys_event_catalog",
        "__sys_event_catalog_receipts",
        include_str!("catalog.sql"),
    )
}
