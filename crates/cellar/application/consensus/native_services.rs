// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Runtime-owned contracts for globally named database cells.

use super::{AdmittedCellConfiguration, NativeCellConfiguration, RaftClass, RegistrationError};
use sha2::{Digest, Sha256};
use std::sync::Arc;

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
