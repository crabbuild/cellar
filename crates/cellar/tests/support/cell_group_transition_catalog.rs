// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authorization/cancellation races against independently reopened catalog state.
use crate::{managed_fixture::fault, route};
use anyhow::{Context as _, Result};
use cellar::application::consensus::{
    verification::{
        cell_groups::*, Catalog, CatalogError, CatalogOptions, InitialMember, NamespaceConfig,
    },
    *,
};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::{MemberId, NodeId};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[path = "cell_group_directory_fixture.rs"]
mod fixture;
use fixture::*;
#[path = "cell_group_directory_transition.rs"]
mod cases;

pub(super) async fn seal_authorization_cancel_race() -> Result<()> {
    cases::seal_authorization_cancel_race().await?;
    cases::unknown_transition_authorization().await
}
