// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Temporary adapter while the public host cuts over. A shared transport starts
//! directly on its physical runtime; it never constructs a legacy runtime.
use super::*;
use crate::raft::{cell_groups::CellGroupRuntime, storage::ReplicaLog};

pub(super) enum TransportRuntime {
    Legacy(RaftRuntime),
    Shared(CellGroupRuntime),
}

impl TransportRuntime {
    pub(super) fn log(&self) -> &ReplicaLog {
        match self {
            Self::Legacy(r) => r.log(),
            Self::Shared(r) => r.log(),
        }
    }

    pub(super) fn admit(&self) -> Result<(), RuntimeError> {
        match self {
            Self::Legacy(r) => r.admit(),
            Self::Shared(r) => r.admit(),
        }
    }

    pub(super) fn claim_transport(&self) -> Result<Box<dyn Send + Sync>, RuntimeError> {
        match self {
            Self::Legacy(r) => Ok(Box::new(r.claim_transport()?)),
            Self::Shared(r) => Ok(Box::new(r.claim_transport()?)),
        }
    }

    pub(super) fn shared(&self) -> Result<&CellGroupRuntime, TransportError> {
        match self {
            Self::Shared(r) => Ok(r),
            Self::Legacy(_) => Err(TransportError::Identity),
        }
    }

    pub(super) async fn admit_route(
        &self,
        route: &Route,
        message: Message,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Shared(runtime) => {
                if message.get_msg_type() == MessageType::MsgSnapshot {
                    return Err(RuntimeError::Protocol(
                        "complete-group snapshot transport unavailable".into(),
                    ));
                }
                let (source, _) = route
                    .decode_group()
                    .map_err(|_| RuntimeError::Protocol("physical route".into()))?;
                runtime.admit_step(source.scope, message).await
            }
            Self::Legacy(runtime) => {
                let (source, _) = route
                    .decode()
                    .map_err(|_| RuntimeError::Protocol("legacy route".into()))?;
                runtime.admit_step(&source.scope, message).await
            }
        }
    }

    pub(super) async fn delivery_hint(
        &self,
        route: &SendRoute,
        member: u64,
        delivered: bool,
        snapshot: bool,
    ) -> Result<(), RuntimeError> {
        match (self, route) {
            (Self::Legacy(runtime), SendRoute::Legacy { scope, .. }) => {
                if !delivered {
                    runtime.admit_unreachable(scope, member).await?;
                }
                if snapshot {
                    runtime
                        .admit_snapshot_status(scope, member, delivered)
                        .await?;
                }
                Ok(())
            }
            (Self::Shared(runtime), SendRoute::Physical { route, .. }) => {
                if snapshot {
                    return runtime
                        .report_snapshot(route.scope, member, delivered)
                        .await;
                }
                if !delivered {
                    runtime.report_unreachable(route.scope, member).await?;
                }
                Ok(())
            }
            _ => Err(RuntimeError::Protocol("transport/runtime identity".into())),
        }
    }
}
