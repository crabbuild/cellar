// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable resource inspection. Opening a factory here would initialize schema
//! and acquire a private writer; query callers use only the selected read frame.

use super::{authority::Action, catalog, lifecycle_store, name_resolution, store};
use crate::application::consensus::native_event_rpc::Authority;
use crate::application::{Result, Storage};
use serde_json::Value;

pub(crate) fn read(
    db: &Storage,
    scope: &str,
    method: &str,
    authority: &Authority,
    body: &Value,
    result_limit: usize,
) -> Result<Value> {
    match (method, authority) {
        ("__catalogStatus", Authority::Event(caller)) => {
            store::validate_authority(scope, caller, Action::Configure)?;
            catalog::configuration_status_read(db, caller)
        }
        (
            crate::application::consensus::native_event_query::CATALOG_READY,
            Authority::Event(caller),
        ) => {
            store::validate_authority(scope, caller, Action::Configure)?;
            catalog::configuration_ready_read(db, caller)
        }
        ("__catalogConfigureReceipt", Authority::Event(caller)) => {
            store::validate_authority(scope, caller, Action::Configure)?;
            catalog::configuration_receipt_read(db, caller, body)
        }
        ("__catalogResolve", Authority::Event(caller)) => {
            store::validate_authority(scope, caller, Action::Resolve)?;
            catalog::resolve_read(db, caller)
        }
        ("__streamResolve", Authority::Event(caller)) => {
            store::validate_authority(scope, caller, Action::Resolve)?;
            store::resolve_read(db, caller, body)
        }
        ("__streamConfigureReceipt", Authority::Event(caller)) => {
            store::configuration_receipt_read(db, scope, caller, body)
        }
        ("__streamRead", Authority::Event(caller)) => {
            store::validate_authority(scope, caller, Action::Read)?;
            store::read_selected(db, caller, body, result_limit)
        }
        ("__streamSubscriptionStatus", Authority::Event(caller)) => {
            // The status reader admits only Consume/Administer and checks the
            // current stored grant. Scope/provenance remain host authenticated.
            store::validate_authority(scope, caller, caller.action)?;
            lifecycle_store::status_read(db, caller, body)
        }
        ("__streamAckReceipt", Authority::Event(caller)) => {
            store::validate_authority(scope, caller, Action::Consume)?;
            super::subscription_store::ack_receipt_read(db, caller, body)
        }
        (name_resolution::METHOD, Authority::Name(caller)) => {
            name_resolution::resolve_read(db, scope, caller, body)
        }
        _ => Err(crate::application::consensus::CommandFailure::UnsupportedCapability.into()),
    }
}
