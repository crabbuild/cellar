// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private immutable EventStream inspection, distinct from public queries and
//! event commands. Only host-minted query admission can invoke this contract.

use super::{native_event_rpc, native_rpc, OperationContract, OperationKind, QueryContext};
use crate::application::{event_stream, Output};
use sha2::{Digest, Sha256};

const OPERATION: &str = "__sys_raft_event_query";

pub(crate) fn is_metadata(method: &str) -> bool {
    matches!(
        method,
        "__catalogStatus"
            | "__catalogConfigureReceipt"
            | "__catalogResolveName"
            | "__catalogResolve"
            | "__streamResolve"
            | "__streamSubscriptionStatus"
    )
}

pub(crate) fn contract(class: &str) -> OperationContract {
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-private-event-query-1");
    hash.update((class.len() as u64).to_be_bytes());
    hash.update(class.as_bytes());
    OperationContract {
        operation: OPERATION.into(),
        kind: OperationKind::Query,
        digest: hash.finalize().into(),
    }
}

pub(crate) fn invoke(context: &QueryContext<'_>, input: &[u8]) -> anyhow::Result<Vec<u8>> {
    let scope = context.identity().cell.as_str();
    let request = native_event_rpc::decode(scope, input)?;
    anyhow::ensure!(
        is_metadata(&request.method),
        super::CommandFailure::UnsupportedCapability
    );
    let result = event_stream::broker::body(&request.rpc.args).and_then(|body| {
        event_stream::metadata::read(
            &context.storage(),
            scope,
            &request.method,
            &request.authority,
            &body,
        )
    });
    let Output::Rpc(result) = event_stream::broker::reply(result)? else {
        anyhow::bail!("invalid event metadata response");
    };
    native_rpc::encode_response(&result)
}
