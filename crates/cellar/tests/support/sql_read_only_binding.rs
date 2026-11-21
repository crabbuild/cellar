// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit read RPC on the ordinary native SQL service, including refusals.

use super::*;
use anyhow::Context as _;

async fn query(context: &Context, sql: &str, params: Value, first: bool) -> Result<Value> {
    let arguments = cellar_native::clone::StructuredValue::from_json(json!([[{
        "sql": sql, "params": params, "first": first,
    }]]))?;
    let response = context
        .rpc_cell(
            cellar::engine_api::sql_cell_scope(IDENTITY),
            Some(IDENTITY.into()),
            "__sqlRead".into(),
            encode_rpc(&arguments)?,
        )
        .await?;
    decode_rpc_result(&response)?.to_json()
}

pub(super) async fn check(context: &Context) -> Result<()> {
    let database = context.sql("DB")?;
    let before = database
        .prepare("SELECT * FROM data ORDER BY id")
        .raw(context, true)
        .await?;
    let value = query(context, "SELECT ? AS n", json!([42]), false)
        .await
        .map_err(|error| anyhow!("SQL read-only protocol is missing: {error:#}"))?;
    assert_eq!(value[0]["columns"], json!(["n"]));
    assert_eq!(value[0]["rows"], json!([[42]]));
    assert!(value[0]["meta"]["duration"].is_number());
    assert!(value[0]["meta"]["rows_read"].is_number());
    assert!(value[0]["meta"]["size_after"].is_number());
    assert!(value[0]["meta"].get("served_by_primary").is_none());
    assert!(value[0]["meta"].get("changes").is_none());
    for sql in [
        "UPDATE data SET id=id+100",
        "WITH v(n) AS (SELECT 9) INSERT INTO data(id) SELECT n FROM v RETURNING id",
        "CREATE TABLE forbidden_read(n)",
        "PRAGMA user_version = 23",
        "PRAGMA query_only = OFF",
        "PRAGMA optimize",
        "BEGIN",
        "ATTACH ':memory:' AS other",
        "SELECT 1; DELETE FROM data",
        "SELECT * FROM __sys_sql_migrations",
    ] {
        assert!(
            query(context, sql, json!([]), false).await.is_err(),
            "read-only SQL accepted {sql}"
        );
    }
    assert!(query(context, "SELECT ?", json!([]), false).await.is_err());
    let multiple = cellar_native::clone::StructuredValue::from_json(json!([[{
        "sql":"SELECT 1", "params":[],
    }, { "sql":"SELECT 2", "params":[] }]]))?;
    let refused = context
        .rpc_cell(
            cellar::engine_api::sql_cell_scope(IDENTITY),
            Some(IDENTITY.into()),
            "__sqlRead".into(),
            encode_rpc(&multiple)?,
        )
        .await
        .and_then(|response| decode_rpc_result(&response));
    let error = refused
        .err()
        .context("read-only RPC admitted multiple result budgets")?;
    assert!(error
        .to_string()
        .contains("read-only SQL requires one statement"));
    let first = query(
        context,
        "WITH v(n) AS (VALUES(1),(2)) SELECT n FROM v",
        json!([]),
        true,
    )
    .await?;
    assert_eq!(first[0]["rows"], json!([[1]]));
    assert!(query(context, "PRAGMA table_info(data)", json!([]), false)
        .await?
        .as_array()
        .is_some());
    assert_eq!(
        database
            .prepare("SELECT * FROM data ORDER BY id")
            .raw(context, true)
            .await?,
        before,
        "a refused read-only request changed the database"
    );
    database
        .prepare("INSERT INTO data(id) VALUES(500)")
        .run(context)
        .await?;
    database
        .prepare("DELETE FROM data WHERE id=500")
        .run(context)
        .await?;
    Ok(())
}

pub(super) async fn check_sdk(context: &Context) -> Result<()> {
    let database = context.sql("DB")?;
    let original = database.prepare_read_only("SELECT ? AS n, ? AS value");
    let bound = original.bind([42.into(), vec![0_u8, 127, 255].into()]);
    let all = bound.all(context).await?;
    assert!(all.success);
    assert_eq!(
        all.results,
        vec![json!({"n":42,"value":[0,127,255]})
            .as_object()
            .unwrap()
            .clone()]
    );
    assert!(original.all(context).await.is_err());
    assert_eq!(bound.first(context, Some("n")).await?, Some(json!(42)));
    assert_eq!(
        bound.raw(context, true).await?,
        vec![
            vec![json!("n"), json!("value")],
            vec![json!(42), json!([0, 127, 255])],
        ]
    );
    assert_eq!(
        database
            .prepare_read_only("SELECT NULL AS n")
            .first(context, Some("n"))
            .await?,
        Some(Value::Null)
    );
    assert_eq!(
        database
            .prepare_read_only("SELECT 1 WHERE 0")
            .first(context, None)
            .await?,
        None
    );
    assert_eq!(
        database
            .prepare_read_only("SELECT 1 AS n")
            .first(context, Some("missing"))
            .await
            .unwrap_err()
            .family,
        "SQL_COLUMN_NOTFOUND"
    );
    assert!(database
        .prepare_read_only("DELETE FROM data")
        .all(context)
        .await
        .is_err());
    let streamed = database.prepare_read_only("WITH v(n) AS (VALUES(1),(2)) SELECT CASE WHEN n=2 THEN abs(-9223372036854775808) ELSE n END AS n FROM v");
    let first = streamed
        .first(context, Some("n"))
        .await
        .map_err(|error| anyhow!("read-only first evaluated later rows: {error}"))?;
    assert_eq!(first, Some(json!(1)));
    assert!(
        streamed.all(context).await.is_err(),
        "all() must evaluate the failing row"
    );
    database
        .exec(context, "CREATE TABLE write_first(n)")
        .await?;
    assert_eq!(
        database
            .prepare("INSERT INTO write_first VALUES(1),(2) RETURNING n")
            .first(context, Some("n"))
            .await?,
        Some(json!(1))
    );
    assert_eq!(
        database
            .prepare_read_only("SELECT count(*) AS n FROM write_first")
            .first(context, Some("n"))
            .await?,
        Some(json!(2))
    );
    database.exec(context, "DROP TABLE write_first").await?;
    Ok(())
}
