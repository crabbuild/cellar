// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Runtime-owned contracts for shared primitive cells.

use super::{AdmittedCellConfiguration, NativeCellConfiguration, RaftClass, RegistrationError};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(in crate::application) fn kv_service() -> Result<RaftClass, RegistrationError> {
    let mut class = RaftClass::kv_service()?
        .initializer(
            Sha256::digest(b"cellar-raft-kv-initializer-1").into(),
            |_, _| Ok(()),
        )?
        .durable_callbacks()?;
    for method in [
        "__kvGet",
        "__kvPut",
        "__kvDelete",
        "__kvList",
        "__kvMetrics",
        "__kvSweep",
        crate::application::kv::OPERATOR,
        crate::application::kv::raft_wire::PREPARE,
        crate::application::kv::raft_wire::EXTERNAL,
    ] {
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-kv-operation-1");
        hash.update(method);
        class = class.cell_rpc(method, hash.finalize().into())?;
    }
    class = class.query(
        super::sql_ingress_identity::OPERATION,
        Sha256::digest(b"cellar-kv-ingress-sequence-1").into(),
        |query, input| query.sql_ingress_sequence(input),
    )?;
    class = class.with_cell_factory(Arc::new(|context, _| {
        crate::application::kv::create_cell(&context)
    }))?;
    let options = NativeCellConfiguration {
        do_classes: vec![crate::deploy::KV_CLASS.into()],
        ..Default::default()
    }
    .options("cellar-kv-service");
    Ok(class.configured_cell(&AdmittedCellConfiguration::from_options(&options)?))
}

pub(in crate::application) fn sql_service() -> Result<RaftClass, RegistrationError> {
    // Enrollment commits an empty business database. Framework storage and
    // receipt tables belong to the command owner; SQL creates user tables on
    // the first exec or migration, exactly as the ordinary service does.
    let mut class = RaftClass::sql_service()?.initializer(
        Sha256::digest(b"cellar-raft-sql-initializer-1").into(),
        |_, _| Ok(()),
    )?;
    for method in [
        "__sqlQuery",
        "__sqlBatch",
        "__sqlExec",
        "__sqlMigrations",
        "__sqlMigrate",
        crate::application::sql::operator::METHOD,
    ] {
        let mut digest = Sha256::new();
        digest.update(b"cellar-raft-sql-operation-1");
        digest.update(method);
        class = class.cell_rpc(method, digest.finalize().into())?;
    }
    class = class.query(
        super::sql_ingress_identity::OPERATION,
        Sha256::digest(b"cellar-sql-ingress-sequence-1").into(),
        |query, input| query.sql_ingress_sequence(input),
    )?;
    class = class.with_cell_factory(Arc::new(|context, _| {
        crate::application::sql::create_cell(&context)
    }))?;
    // Database identity and schema are independent of the calling script's
    // variables, aliases and compatibility switches. Exporting the service
    // installs its private child-call retirement contract, not an outgoing
    // grant to arbitrary databases.
    let options = NativeCellConfiguration {
        do_classes: vec![crate::deploy::SQL_CLASS.into()],
        ..Default::default()
    }
    .options("cellar-sql-service");
    Ok(class.configured_cell(&AdmittedCellConfiguration::from_options(&options)?))
}

/// Queue leases and callbacks share the broker's quorum-proven private image.
/// The consumer remains the deployment's ordinary native Queue handler.
pub(in crate::application) fn queue_service(
    configuration: &AdmittedCellConfiguration,
    handler: crate::application::Handler,
) -> Result<RaftClass, RegistrationError> {
    let mut class = RaftClass::queue_service()?
        .initializer(
            Sha256::digest(b"cellar-raft-queue-initializer-1").into(),
            |_, _| Ok(()),
        )?
        .durable_callbacks()?;
    for method in [
        "__queueSend",
        "__queueMetrics",
        "__queueDlqAcceptBatch",
        crate::application::queue::OPERATOR,
    ] {
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-queue-operation-1");
        hash.update(method);
        class = class.cell_rpc(method, hash.finalize().into())?;
    }
    class = class.query(
        super::sql_ingress_identity::OPERATION,
        Sha256::digest(b"cellar-queue-ingress-sequence-1").into(),
        |query, input| query.sql_ingress_sequence(input),
    )?;
    class = class.with_cell_factory(Arc::new(move |context, _| {
        crate::application::queue::create_raft_cell(&context, handler.clone())
    }))?;
    Ok(class.configured_cell(configuration))
}

/// Enrollment installs the first calendar wake in the same durable creation
/// record. Retained startup discovers existing work without rearming it.
pub(in crate::application) fn cron_service(
    configuration: &AdmittedCellConfiguration,
    handler: crate::application::Handler,
    expressions: Vec<String>,
    history: crate::application::cron::HistoryConfiguration,
) -> Result<RaftClass, RegistrationError> {
    crate::deployment_cron::validate(&expressions)
        .map_err(|_| RegistrationError::Invalid("Raft cron schedule"))?;
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-cron-service-1");
    hash.update(
        serde_json::to_vec(&(&expressions, &history))
            .map_err(|_| RegistrationError::Invalid("Raft cron configuration"))?,
    );
    let scope = cellar_logic::cron::reserved_cell(&configuration.script);
    let initial = expressions.clone();
    let class = RaftClass::cron_service(hash.finalize().into())?
        .initializer(
            Sha256::digest(b"cellar-raft-cron-initializer-1").into(),
            move |cell, tx| {
                anyhow::ensure!(
                    cell.cell.as_str() == scope,
                    super::CommandFailure::UnsupportedCapability
                );
                let parsed = initial
                    .iter()
                    .map(|expression| {
                        cellar_logic::cron::parse(expression)
                            .map_err(|error| anyhow::anyhow!("{error}"))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let now = i64::try_from(tx.inputs().now_unix_millis)?;
                if let Some(at) = cellar_logic::cron::plan(&parsed, None, now, 0, false).arm_at {
                    super::native_work::arm(&*tx.framework()?, "alarm", at)?;
                }
                Ok(())
            },
        )?
        .durable_callbacks()?
        .query(
            "__cronHistory",
            Sha256::digest(b"cellar-raft-cron-history-1").into(),
            |query, input| {
                anyhow::ensure!(input.len() <= 4096, super::CommandFailure::Overloaded);
                let request = serde_json::from_slice(input)?;
                let page = crate::application::cron::history_page(&query.storage(), request)?;
                Ok(serde_json::to_vec(&page)?)
            },
        )?
        .with_cell_factory(Arc::new(move |context, _| {
            crate::application::cron::create_raft_cell(
                &context,
                handler.clone(),
                expressions.clone(),
                history.clone(),
            )
        }))?;
    Ok(class.configured_cell(configuration))
}
