// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Address history for physical peers simulated on one loopback interface.
//! A stopped peer can remain authenticated in the directory. Its assigned
//! ports must not become another logical node's endpoints. History retains
//! metadata only; listeners remain owned and closed by the actual node.

use super::*;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Mutex},
};
use tokio::net::TcpListener;

#[derive(Default)]
pub(super) struct PeerPorts {
    owners: Mutex<BTreeMap<u16, NodeId>>,
    rejected: AtomicUsize,
}

impl PeerPorts {
    pub(super) fn rejections(&self) -> usize {
        self.rejected.load(Ordering::Relaxed)
    }

    pub(super) async fn bind(
        &self,
        node: NodeId,
        candidate: Option<TcpListener>,
    ) -> std::io::Result<NodeRaftListeners> {
        Ok(NodeRaftListeners {
            control: self.one(node, candidate).await?,
            data: self.one(node, None).await?,
            query: self.one(node, None).await?,
        })
    }

    async fn one(
        &self,
        node: NodeId,
        mut candidate: Option<TcpListener>,
    ) -> std::io::Result<TcpListener> {
        // This bounds pre-boot address selection, before any publication. It
        // does not retry a boot/command or extend an invocation's deadline.
        for _ in 0..64 {
            let listener = match candidate.take() {
                Some(listener) => listener,
                None => TcpListener::bind("127.0.0.1:0").await?,
            };
            let address = listener.local_addr()?;
            if address.ip() != std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST) {
                return Err(std::io::Error::other("fixture peer must use IPv4 loopback"));
            }
            // The one-interface u16 port space also bounds retained metadata.
            let foreign = {
                let mut owners = self
                    .owners
                    .lock()
                    .map_err(|_| std::io::Error::other("fixture peer port history poisoned"))?;
                *owners.entry(address.port()).or_insert(node) != node
            };
            if foreign {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                drop(listener);
                continue;
            }
            return Ok(listener);
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "fixture peer port selection exhausted",
        ))
    }
}
