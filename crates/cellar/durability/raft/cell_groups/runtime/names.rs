// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A quorum name cut supplies no execution authority or absence reconciliation.

use super::*;
use cellar_logic::cell_group::GroupRoute;

impl CellGroupRuntime {
    pub(crate) async fn resolve_cell_name(
        &self,
        route: GroupRoute,
        candidate: cellar_logic::cell_group::CellHistory,
        deadline: u64,
    ) -> Result<Option<super::super::EnrollmentRecord>, RuntimeError> {
        let cut = self.quorum_membership(route.history, deadline).await?;
        let state = self
            .enrollment_state(route.history, candidate.clone())
            .await?;
        if state.route() != &route
            || cut.root().route() != &route
            || !cut.root().is_active()
            || state.term() != cut.barrier().term
        {
            return Err(RuntimeError::NotReady);
        }
        state.check()?;
        if state.record().is_some_and(|record| {
            record.cell().cluster != candidate.cluster
                || record.cell().namespace != candidate.namespace
                || record.cell().cell != candidate.cell
                || record.schema() != route.schema_digest
        }) {
            return Err(RuntimeError::Protocol(
                "cell name enrollment binding".into(),
            ));
        }
        Ok(state.record().cloned())
    }
}
