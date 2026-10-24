// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Internal-development bucket layout admission; no legacy format negotiation.

use crate::CapacityPeer;

/// Current internal-development bucket layout, including paged epoch chains.
pub const BUCKET_FORMAT: u16 = 1;

/// Whether every live lease reads `format`, so a node can write it.
///
/// A lease counts while it has not expired, so a crashed old node holds the
/// gate closed for one lease lifetime, which is the same bound its cells
/// wait before a peer takes them over. No live lease at all is `false`: a
/// node that has not seen the fleet does not know who is in it, and the
/// current layout cannot be admitted from missing capability evidence.
pub fn fleet_reads(peers: &[CapacityPeer], now_ms: u64, format: u16) -> bool {
    let mut live = peers
        .iter()
        .filter(|peer| peer.expires_ms > now_ms)
        .peekable();
    live.peek().is_some() && live.all(|peer| peer.bucket_format == Some(format))
}
