//! Per-operation cancellation stays outside the bounded SDK mailbox.
use cellar::sdk::web::{AbortController, AbortSignal};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// One registry belongs to one native invocation. It never stores Python
/// objects, caller contexts or payloads, and cannot cancel another invocation.
#[derive(Default)]
pub(crate) struct RpcCalls(Mutex<HashMap<u64, AbortController>>);

pub(crate) struct PendingCall {
    id: u64,
    controller: AbortController,
    calls: Arc<RpcCalls>,
}

impl RpcCalls {
    pub(crate) fn register(self: &Arc<Self>, id: u64) -> anyhow::Result<PendingCall> {
        use std::collections::hash_map::Entry;
        let controller = AbortController::new();
        match self.0.lock().unwrap().entry(id) {
            Entry::Occupied(_) => anyhow::bail!("RPC operation ID is already pending"),
            Entry::Vacant(entry) => {
                entry.insert(controller.clone());
            }
        }
        Ok(PendingCall {
            id,
            controller,
            calls: self.clone(),
        })
    }

    pub(crate) fn cancel(&self, id: u64) -> anyhow::Result<bool> {
        // Abort may wake a native waiter. Never hold the registry lock while
        // dispatching notifications; completion removes its entry with Drop.
        let controller = self.0.lock().unwrap().get(&id).cloned();
        if let Some(controller) = controller {
            controller.abort(None)?;
            Ok(true)
        } else {
            // Completion may win the race. Late/repeated cancellation grants
            // no authority and does not create another SDK operation.
            Ok(false)
        }
    }
}

impl PendingCall {
    pub(crate) fn signal(&self) -> AbortSignal {
        self.controller.signal()
    }
}

impl Drop for PendingCall {
    fn drop(&mut self) {
        self.calls.0.lock().unwrap().remove(&self.id);
    }
}

#[cfg(test)]
#[path = "rpc_calls_tests.rs"]
mod tests;
