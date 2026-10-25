// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::wire;
use crate::runtime::Life;
use anyhow::{ensure, Context as _, Result};
use cellar::{
    engine_api::RpcData,
    sdk::{
        rpc::{LeaseClosed, LeaseControl},
        Context,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

pub(crate) const METHOD: &str = "__cellar_workers_rpc_delegate";

#[derive(Default)]
pub(crate) struct Registry(Mutex<HashMap<String, Arc<Entry>>>);

pub(super) struct Entry {
    pub object: u64,
    pub scope: String,
    pub epoch: u64,
    pub owner: Life,
    pub count: usize,
    pub closed: LeaseClosed,
    pub control: LeaseControl,
    sender: mpsc::Sender<Command>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<Command>>,
    slots: Arc<Semaphore>,
    pending: Mutex<HashMap<u64, Command>>,
    next: Mutex<u64>,
}

struct Command {
    id: u64,
    target: usize,
    args: String,
    reply: oneshot::Sender<Result<RpcData>>,
    _permit: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

impl Registry {
    pub(super) fn insert(&self, group: String, entry: Arc<Entry>) -> Result<()> {
        let mut entries = self.0.lock().unwrap();
        ensure!(
            !entries.contains_key(&group),
            "RPC group identity collision"
        );
        entries.insert(group, entry);
        Ok(())
    }

    pub(super) fn remove(&self, group: &str) {
        if let Some(entry) = self.0.lock().unwrap().remove(group) {
            entry.control.close();
        }
    }

    /// This invocation is only the native routing/admission envelope. SDK
    /// effects run on the original producer's mailbox and context below.
    pub async fn dispatch(
        &self,
        context: &Context,
        object: u64,
        args: RpcData,
        runtime: &crate::runtime::Runtime,
    ) -> Result<RpcData> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Call {
            format: u8,
            group: String,
            target: usize,
            args: String,
        }
        let RpcData::Json(encoded) = args else {
            anyhow::bail!("invalid delegate request")
        };
        let maximum = runtime.options.max_body_bytes;
        ensure!(encoded.len() <= maximum, cellar_guest::failure::Validation);
        // A routed delegate bypasses new engine admission, so its queued
        // argument and eventual result must still reserve the shared bridge
        // budget. Keep one maximum-value reservation until the call settles.
        let bytes = runtime.reserve(runtime.options.max_value_bytes)?;
        let call: Call =
            serde_json::from_str(&encoded).context(cellar_guest::failure::Validation)?;
        drop(encoded);
        ensure!(
            call.format == 1 && call.group.len() <= 64,
            cellar_guest::failure::Validation
        );
        wire::arguments(&call.args, maximum).context(cellar_guest::failure::Validation)?;
        let identity = context.identity().context("delegate requires a cell")?;
        let authority = context.invocation_authority()?;
        let entry = self
            .0
            .lock()
            .unwrap()
            .get(&call.group)
            .cloned()
            .context("RPC export expired")?;
        ensure!(
            entry.object == object
                && entry.scope == identity.scope
                && entry.epoch == identity.epoch
                && entry.owner.load(Ordering::Acquire),
            "RPC export belongs to a retired or foreign activation"
        );
        ensure!(call.target < entry.count, cellar_guest::failure::Validation);
        let permit = entry
            .slots
            .clone()
            .try_acquire_owned()
            .context("RPC delegate capacity exhausted")
            .context(cellar_guest::failure::Validation)?;
        let id = {
            let mut next = entry.next.lock().unwrap();
            *next = next
                .checked_add(1)
                .filter(|id| *id <= 9_007_199_254_740_991)
                .context("RPC call identities exhausted")?;
            *next
        };
        let (reply, receive) = oneshot::channel();
        entry
            .sender
            .try_send(Command {
                id,
                target: call.target,
                args: call.args,
                reply,
                _permit: permit,
                _bytes: bytes,
            })
            .map_err(|_| anyhow::anyhow!("RPC export queue closed"))?;
        let result = cellar::asyncrt::select_biased! { "export revocation refuses an undelivered delegate result";
            _ = entry.closed.clone().wait() => anyhow::bail!("RPC export expired"),
            result = receive => result.context("RPC delegate result lost")?,
        };
        authority.check(context)?;
        result
    }
}

impl Entry {
    pub(super) fn new(
        object: u64,
        context: &Context,
        owner: Life,
        count: usize,
        closed: LeaseClosed,
        control: LeaseControl,
        maximum: usize,
    ) -> Result<Arc<Self>> {
        let identity = context.identity().context("RPC export requires a cell")?;
        let (sender, receiver) = mpsc::channel(maximum);
        Ok(Arc::new(Self {
            object,
            scope: identity.scope.clone(),
            epoch: identity.epoch,
            owner,
            count,
            closed,
            control,
            sender,
            receiver: tokio::sync::Mutex::new(receiver),
            slots: Arc::new(Semaphore::new(maximum)),
            pending: Mutex::new(HashMap::new()),
            next: Mutex::new(0),
        }))
    }

    pub(super) async fn poll(&self) -> Result<Value> {
        let mut receiver = self
            .receiver
            .try_lock()
            .context("RPC export already has a poll waiter")
            .context(cellar_guest::failure::Validation)?;
        loop {
            let command = cellar::asyncrt::select_biased! { "the last result alias closes the producer mailbox";
                _ = self.closed.clone().wait() => return Ok(json!({"closed":true})),
                command = receiver.recv() => command.context("RPC export mailbox closed")?,
            };
            if command.reply.is_closed() {
                continue;
            }
            let value =
                json!({"closed":false,"id":command.id,"target":command.target,"args":command.args});
            self.pending.lock().unwrap().insert(command.id, command);
            return Ok(value);
        }
    }

    pub(super) fn reply(&self, id: u64, result: Result<RpcData>) -> Result<()> {
        let command = self
            .pending
            .lock()
            .unwrap()
            .remove(&id)
            .context("RPC call is closed")
            .context(cellar_guest::failure::Validation)?;
        // A caller may have disconnected after execution. Losing the receiver
        // is never permission to replay a mutation or reinterpret its outcome.
        let _ = command.reply.send(result);
        Ok(())
    }
}
