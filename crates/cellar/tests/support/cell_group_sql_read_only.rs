// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary read-only SQL preserves target history and source proof admission.

use super::*;

pub(super) async fn verify(
    cluster: &ManagedCluster,
    sql_group: GroupHistory,
    source_history: &CellHistory,
) -> Result<()> {
    let target = managed_fixture::leader(&cluster.hosts, sql_group).await?;
    let group = cluster.hosts[target].group(sql_group)?;
    let before = group.observe(TIMEOUT).await?;
    ensure!(before.status.ready);
    let expected = json!([["id", "v"], [1, "one"], [2, "two"], [3, "three"]]);
    for node in 0..3 {
        ensure!(public_sql(cluster, node, "ReadOnly", json!(null)).await? == expected);
        ensure!(public_sql(cluster, node, "ReadOnlyReject", json!(null)).await? == json!(true));
        ensure!(public_sql(cluster, node, "ReadOnlyFirst", json!(null)).await? == json!(true));
        ensure!(public_sql(cluster, node, "ReadOnlyUndeclared", json!(null)).await? == json!(true));
    }
    ensure!(public_sql(cluster, 0, "ReadOnlyCold", json!(null)).await? == json!(true));
    let source_node = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let source = cluster.hosts[source_node].cell(source_history.clone())?;
    let mut options = bindings::options(1, Durability::Quorum)?;
    options.id = CommandId::new(ClientId::new([101; 16])?, 1)?;
    let result = source
        .command(&source.contract("ReadOnly")?, b"true".to_vec(), options)
        .await?;
    ensure!(serde_json::from_slice::<serde_json::Value>(&result.result)? == expected);
    let after = group.observe(TIMEOUT).await?;
    ensure!(
        after.status.term == before.status.term
            && after.status.applied == before.status.applied
            && after.status.committed == before.status.committed,
        "read-only SQL advanced its target log: {:?} -> {:?}",
        before.status,
        after.status
    );
    ensure!(
        cluster.hosts[target]
            .resolve_cell_history(
                sql_group.cluster,
                sql_group.namespace,
                CellId::new(cellar::engine_api::sql_cell_scope("undeclared/read-only"))?,
                QueryOptions::default(),
            )
            .await?
            .is_none(),
        "refused read-only SQL enrolled an undeclared database"
    );
    ensure!(
        cluster.hosts[target]
            .resolve_cell_history(
                sql_group.cluster,
                sql_group.namespace,
                CellId::new(cellar::engine_api::sql_cell_scope(
                    "shared/sql-read-only-cold"
                ))?,
                QueryOptions::default(),
            )
            .await?
            .is_none(),
        "read-only SQL allocated a declared but cold database"
    );
    Ok(())
}

pub(super) async fn quorum_refused(cluster: &ManagedCluster) -> Result<()> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!(
            "http://{}/ReadOnly",
            cluster.nodes[0].info().public_address
        ))
        .body("null")
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status == 503
            && ["DeadlineExceeded", "Unavailable", "NotReady", "Timeout"]
                .iter()
                .any(|reason| body.contains(reason)),
        "read-only SQL returned without a quorum: {status} {body}"
    );
    Ok(())
}
