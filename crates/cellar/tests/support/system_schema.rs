// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

pub fn assert_system_tables(path: &std::path::Path, expected: &[&str]) -> anyhow::Result<()> {
    let connection = rusqlite::Connection::open(path)?;
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        names
            .iter()
            .all(|name| cellar_ltx::system_schema::is_system_name(name)),
        "unexpected tables: {names:?}"
    );
    for name in expected {
        assert!(
            names.iter().any(|actual| actual == name),
            "missing {name}: {names:?}"
        );
    }
    Ok(())
}
