// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An atomic nftables program for the helper's own network namespace.
//! Applying it to the host namespace is forbidden: the engine's bridge fence
//! remains independently responsible for protecting the host and private ranges.

use crate::{HttpTarget, MAX_INTERCEPT_ENTRIES};
use anyhow::{ensure, Result};
use std::collections::{BTreeSet, HashSet};

/// Only the helper has NET_ADMIN. Application containers drop all capabilities,
/// so they cannot mark a socket to bypass these redirects or the offline fence.
pub const SOCKET_MARK: u32 = 0xce1101;
pub const TABLE: &str = "cellar_egress";

pub struct Program {
    source: String,
}

impl Program {
    /// Render only validated numbers and fixed identifiers. Host patterns never
    /// enter nft syntax. DNS redirects precede HTTP redirects and Docker's
    /// conventional priority -100 output NAT chain.
    pub fn new(
        targets: &[HttpTarget],
        http_port: u16,
        dns_port: u16,
        internet: bool,
    ) -> Result<Self> {
        ensure!(
            http_port != 0
                && dns_port != 0
                && http_port != dns_port
                && http_port != 53
                && dns_port != 53
                && http_port != 80
                && dns_port != 80,
            "invalid network helper ports"
        );
        ensure!(
            targets.len() <= MAX_INTERCEPT_ENTRIES
                && targets.iter().map(HttpTarget::entries).sum::<usize>() <= MAX_INTERCEPT_ENTRIES,
            "too many intercept entries"
        );
        let mut unique = HashSet::new();
        ensure!(
            targets.iter().all(|target| unique.insert(target.as_str())),
            "duplicate intercept target"
        );
        let mut ports = BTreeSet::from([80]);
        for target in targets {
            let port = target.port();
            ensure!(
                port != 53 && port != http_port && port != dns_port,
                "intercept port conflicts with the network helper"
            );
            ports.insert(port);
        }
        let ports = ports
            .into_iter()
            .map(|port| port.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let policy = if internet { "accept" } else { "drop" };
        // add is idempotent; flush and the replacement are in the same -f
        // transaction. Never flush unrelated tables or Docker's own chains.
        let source = format!(
            r#"add table inet {TABLE}
flush table inet {TABLE}
table inet {TABLE} {{
  chain outbound_nat {{
    type nat hook output priority -110; policy accept;
    meta mark {SOCKET_MARK:#x} return
    udp dport 53 redirect to :{dns_port}
    tcp dport 53 redirect to :{dns_port}
    tcp dport {{ {ports} }} redirect to :{http_port}
  }}
  chain outbound_fence {{
    type filter hook output priority 0; policy {policy};
    meta mark {SOCKET_MARK:#x} accept
    oifname "lo" accept
    ct state established,related accept
  }}
}}
"#
        );
        Ok(Self { source })
    }

    /// Pass the entire byte string to one `nft -f -` invocation in the helper.
    pub fn as_str(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
#[path = "firewall_tests.rs"]
mod tests;
