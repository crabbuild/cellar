// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{api::ApiError, Console};
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use subtle::ConstantTimeEq;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    MetadataRead,
    DataRead,
    DataWrite,
    Operate,
    Configure,
    FleetTopology,
    FleetCancel,
    FleetRegister,
    /// Trusted deployment authority for explicit node resource attestations.
    FleetCapacity,
    FleetEvacuate,
    FleetRemove,
    FleetStop,
    /// Preparation is independent of stop and capacity grants.
    FleetMaintenance,
}

#[derive(Clone)]
pub struct Principal {
    pub id: String,
    pub grants: BTreeMap<String, Vec<Permission>>,
    token_hash: [u8; 32],
}

impl Principal {
    pub fn new(
        id: String,
        token: &str,
        grants: BTreeMap<String, Vec<Permission>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(crate::valid_id(&id), "invalid principal ID");
        anyhow::ensure!(
            token.len() >= 32,
            "dashboard tokens must contain at least 32 bytes"
        );
        Ok(Self {
            id,
            grants,
            token_hash: Sha256::digest(token.as_bytes()).into(),
        })
    }

    pub fn allows(&self, fleet: &str, permission: Permission) -> bool {
        self.grants
            .get(fleet)
            .is_some_and(|grants| grants.contains(&permission))
    }

    pub(crate) fn require(&self, fleet: &str, permission: Permission) -> Result<(), ApiError> {
        if self.allows(fleet, permission) {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }
}

pub(crate) async fn authenticate(
    State(console): State<Arc<Console>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Bearer-only requests, no ambient cookies. Origin checking also protects
    // operators who paste a token into a page served by an unintended origin.
    if let Some(origin) = request.headers().get("origin") {
        if !origin
            .to_str()
            .ok()
            .is_some_and(|origin| console.origins.iter().any(|allowed| allowed == origin))
        {
            return Err(ApiError::forbidden());
        }
    }
    let token = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(ApiError::unauthorized)?;
    let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let principal = console
        .principals
        .iter()
        .find(|principal| bool::from(principal.token_hash.ct_eq(&hash)))
        .cloned()
        .ok_or_else(ApiError::unauthorized)?;
    request.extensions_mut().insert(principal);
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    Ok(response)
}
