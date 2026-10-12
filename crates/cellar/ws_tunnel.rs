// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/main/peer_tunnel.rs.
//! Owned parked WebSocket upgrades shared by forwarding and native binding.
use std::collections::HashMap;

/// The owner half of a tunneled WebSocket 101, parked until the caller's own
/// client socket finishes upgrading.
static TUNNEL_UPGRADES: std::sync::OnceLock<
    std::sync::Mutex<HashMap<u64, hyper::upgrade::OnUpgrade>>,
> = std::sync::OnceLock::new();
static NEXT_TUNNEL_UPGRADE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub(crate) fn park_upgrade(upgrade: hyper::upgrade::OnUpgrade) -> u64 {
    let id = NEXT_TUNNEL_UPGRADE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    TUNNEL_UPGRADES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(id, upgrade);
    id
}

fn remove_upgrade(parked: u64) -> Option<hyper::upgrade::OnUpgrade> {
    TUNNEL_UPGRADES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&parked)
}

/// Own one parked upgrade after its numeric id returns from JavaScript.
///
/// The id itself releases nothing. Binding it to this value makes every early
/// response exit and every cancelled executor task remove the stored upgrade.
/// A successful splice consumes the same value and takes the connection once.
pub(crate) struct TunnelUpgradeClaim {
    upgrade: hyper::upgrade::OnUpgrade,
}

pub(crate) fn claim_upgrade(parked: u64) -> anyhow::Result<TunnelUpgradeClaim> {
    let upgrade = remove_upgrade(parked)
        .ok_or_else(|| anyhow::anyhow!("tunneled upgrade {parked} is gone"))?;
    Ok(TunnelUpgradeClaim { upgrade })
}

impl TunnelUpgradeClaim {
    pub(crate) fn into_upgrade(self) -> hyper::upgrade::OnUpgrade {
        self.upgrade
    }
}

pub(crate) fn discard(parked: u64) {
    drop(remove_upgrade(parked));
}
