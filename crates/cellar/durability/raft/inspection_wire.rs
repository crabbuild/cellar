// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node capacity profile shared by Fleet inspection and catalog admission.
use super::inspection::RaftNodeProfile;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_BODY: usize = 64 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Profile {
    pool: String,
    region: String,
    zone: String,
    group_capacity: u32,
    byte_capacity: u64,
    bytes_per_group: u64,
    leader_capacity: u32,
    draining: bool,
}

impl From<&RaftNodeProfile> for Profile {
    fn from(value: &RaftNodeProfile) -> Self {
        Self {
            pool: value.pool.clone(),
            region: value.region.clone(),
            zone: value.zone.clone(),
            group_capacity: value.group_capacity,
            byte_capacity: value.byte_capacity,
            bytes_per_group: value.bytes_per_group,
            leader_capacity: value.leader_capacity,
            draining: value.draining,
        }
    }
}

impl From<Profile> for RaftNodeProfile {
    fn from(value: Profile) -> Self {
        Self {
            pool: value.pool,
            region: value.region,
            zone: value.zone,
            group_capacity: value.group_capacity,
            byte_capacity: value.byte_capacity,
            bytes_per_group: value.bytes_per_group,
            leader_capacity: value.leader_capacity,
            draining: value.draining,
        }
    }
}
