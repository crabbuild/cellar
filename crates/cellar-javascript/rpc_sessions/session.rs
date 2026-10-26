// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    registry::Entry,
    wire::{self, Publication},
    Registry, METHOD,
};
use crate::runtime::{Life, Options};
use anyhow::{ensure, Context as _, Result};
use cellar::{
    engine_api::RpcData,
    sdk::{cells::Stub, rpc::Lease, Context, InvocationAuthority},
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub(crate) struct Session {
    registry: Arc<Registry>,
    authority: InvocationAuthority,
    object: Option<u64>,
    owner: Life,
    options: Options,
    resources: Mutex<Resources>,
}

#[derive(Default)]
struct Resources {
    next: u64,
    imports: HashMap<u64, Remote>,
    exports: HashMap<String, Export>,
    inputs: HashMap<u64, usize>,
}

#[derive(Clone)]
struct Remote {
    stub: Stub,
    lease: Lease,
    group: String,
    target: usize,
}

struct Export {
    entry: Arc<Entry>,
    lease: Option<Lease>,
}

impl Session {
    pub fn new(
        registry: Arc<Registry>,
        context: &Context,
        object: Option<u64>,
        owner: Life,
        options: Options,
    ) -> Result<Self> {
        Ok(Self {
            registry,
            authority: context.invocation_authority()?,
            object,
            owner,
            options,
            resources: Mutex::new(Resources::default()),
        })
    }

    fn reserve(&self, resources: &Resources, count: usize) -> Result<()> {
        let used = resources.inputs.values().sum::<usize>()
            + resources.imports.len()
            + resources
                .exports
                .values()
                .map(|export| export.entry.count)
                .sum::<usize>();
        ensure!(
            count > 0 && count <= self.options.max_batch_operations.saturating_sub(used),
            "RPC handle quota exceeded"
        );
        Ok(())
    }

    fn remote(&self, id: u64) -> Result<Remote> {
        let remote = self
            .resources
            .lock()
            .unwrap()
            .imports
            .get(&id)
            .cloned()
            .context("RPC handle expired")?;
        ensure!(!remote.lease.is_closed(), "RPC handle expired");
        Ok(remote)
    }

    pub async fn execute(
        &self,
        context: &Context,
        name: &str,
        input: Value,
        signal: Option<cellar::sdk::web::AbortSignal>,
    ) -> Result<Value> {
        self.authority.check(context)?;
        let result = match name {
            "workers.rpc" => {
                let (stub, data) = cellar_guest::workers::rpc::call_raw(
                    context,
                    input,
                    self.options.max_body_bytes,
                )
                .await?;
                self.import(stub, data)
            }
            "workers.rpc.error" => {
                // Input completions otherwise nest an opaque encoded error in
                // another request. Apply the same native stack stripping and
                // bounded error validation as ordinary RPC replies first.
                ensure!(input["error"] == true, cellar_guest::failure::Validation);
                let data = cellar_guest::workers::rpc::output(input, self.options.max_body_bytes)?;
                cellar_guest::workers::rpc::reply(&data, self.options.max_body_bytes)
            }
            "workers.rpc.inputs" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input {
                    count: usize,
                    value: String,
                }
                let input: Input =
                    serde_json::from_value(input).context(cellar_guest::failure::Validation)?;
                wire::inputs(&input.value, input.count, self.options.max_body_bytes)
                    .context(cellar_guest::failure::Validation)?;
                // Four session controls plus simultaneous stream data/cancel
                // work must leave native slots for poll, result and submission.
                ensure!(
                    input.count <= self.options.max_pending_operations.saturating_sub(4) / 2,
                    cellar_guest::failure::Validation
                );
                let mut resources = self.resources.lock().unwrap();
                self.reserve(&resources, input.count.saturating_add(4))
                    .context(cellar_guest::failure::Validation)?;
                let id = next(&mut resources)?;
                resources.inputs.insert(id, input.count);
                Ok(json!({"id":id,"max_body_bytes":self.options.max_body_bytes}))
            }
            "workers.rpc.inputs_release" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input {
                    id: u64,
                }
                let input: Input =
                    serde_json::from_value(input).context(cellar_guest::failure::Validation)?;
                self.resources.lock().unwrap().inputs.remove(&input.id);
                Ok(Value::Null)
            }
            "workers.rpc.export" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input {
                    count: usize,
                }
                let input: Input =
                    serde_json::from_value(input).context(cellar_guest::failure::Validation)?;
                let mut resources = self.resources.lock().unwrap();
                self.reserve(&resources, input.count)
                    .context(cellar_guest::failure::Validation)?;
                // Each live group keeps a native poll pending. Leave capacity
                // for a delegate's SDK work and its reply/control operation.
                ensure!(
                    resources.exports.len() < self.options.max_pending_operations.saturating_sub(1),
                    cellar_guest::failure::Validation
                );
                let object = self.object.context("RPC export requires an activation")?;
                let group = cellar::native::crypto::random_uuid()?;
                let lease = Lease::for_context(context)?;
                let entry = Entry::new(
                    object,
                    context,
                    self.owner.clone(),
                    input.count,
                    lease.closed(),
                    lease.control(),
                    self.options.max_pending_operations,
                )?;
                self.registry.insert(group.clone(), entry.clone())?;
                resources.exports.insert(
                    group.clone(),
                    Export {
                        entry,
                        lease: Some(lease),
                    },
                );
                Ok(json!({"group":group}))
            }
            "workers.rpc.poll" => {
                let group = group(&input)?;
                ensure!(
                    input.as_object().is_some_and(|value| value.len() == 1),
                    cellar_guest::failure::Validation
                );
                let entry = self
                    .resources
                    .lock()
                    .unwrap()
                    .exports
                    .get(group)
                    .map(|export| export.entry.clone())
                    .context("RPC export expired")?;
                let value = entry.poll().await?;
                if value["closed"] == true {
                    self.resources.lock().unwrap().exports.remove(group);
                    self.registry.remove(group);
                }
                Ok(value)
            }
            "workers.rpc.reply" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input {
                    group: String,
                    id: u64,
                    result: Value,
                }
                let input: Input =
                    serde_json::from_value(input).context(cellar_guest::failure::Validation)?;
                let entry = self
                    .resources
                    .lock()
                    .unwrap()
                    .exports
                    .get(&input.group)
                    .map(|export| export.entry.clone())
                    .context("RPC export expired")?;
                let output = self.output(input.result);
                // The routing envelope is a different native event. Its output
                // gate cannot prove writes made by this original producer.
                // Sync on the producer before acknowledging a delegated reply.
                let proof = context.storage()?.sync().await;
                let result = match proof {
                    Ok(()) => output,
                    Err(error) => Err(error),
                };
                entry.reply(input.id, result)?;
                Ok(Value::Null)
            }
            "workers.rpc.invoke" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input {
                    id: u64,
                    args: String,
                    #[serde(default)]
                    inputs: bool,
                }
                let input: Input =
                    serde_json::from_value(input).context(cellar_guest::failure::Validation)?;
                wire::arguments(&input.args, self.options.max_body_bytes)
                    .context(cellar_guest::failure::Validation)?;
                let remote = self.remote(input.id)?;
                let request = RpcData::Json(serde_json::to_string(
                    &json!({"format":1,"group":remote.group,"target":remote.target,"args":input.args,"inputs":input.inputs}),
                )?);
                ensure!(
                    request.payload_bytes() <= self.options.max_body_bytes,
                    cellar_guest::failure::Validation
                );
                // Data produced in a cell event can depend on its writes. A
                // root Worker has no SQLite state; it still uses native routing
                // and output gates, without inventing a storage capability.
                if context.identity().is_some() {
                    context.storage()?.sync().await?;
                }
                let data = match signal {
                    Some(signal) => {
                        remote
                            .stub
                            .rpc_with_signal(context, METHOD, request, signal)
                            .await?
                    }
                    None => remote.stub.rpc(context, METHOD, request).await?,
                };
                // Keep the source result alive until its in-flight call settles.
                self.import(remote.stub.clone(), data)
            }
            "workers.rpc.release" | "workers.rpc.dup" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input {
                    id: u64,
                }
                let input: Input =
                    serde_json::from_value(input).context(cellar_guest::failure::Validation)?;
                if name.ends_with("release") {
                    self.resources.lock().unwrap().imports.remove(&input.id);
                    Ok(Value::Null)
                } else {
                    let remote = self.remote(input.id)?;
                    let mut resources = self.resources.lock().unwrap();
                    self.reserve(&resources, 1)
                        .context(cellar_guest::failure::Validation)?;
                    let id = next(&mut resources)?;
                    resources.imports.insert(id, remote);
                    Ok(json!({"id":id}))
                }
            }
            _ => anyhow::bail!(cellar_guest::failure::Validation),
        };
        self.authority.check(context)?;
        result
    }

    fn import(&self, stub: Stub, data: RpcData) -> Result<Value> {
        let RpcData::Leased { value, lease } = data else {
            return cellar_guest::workers::rpc::reply(&data, self.options.max_body_bytes);
        };
        let publication = wire::publication(*value, self.options.max_body_bytes)?;
        ensure!(!lease.is_closed(), "RPC result expired before import");
        let mut resources = self.resources.lock().unwrap();
        self.reserve(&resources, publication.count)?;
        let mut handles = Vec::with_capacity(publication.count);
        for target in 0..publication.count {
            let id = next(&mut resources)?;
            resources.imports.insert(
                id,
                Remote {
                    stub: stub.clone(),
                    lease: lease.clone(),
                    group: publication.group.clone(),
                    target,
                },
            );
            handles.push(id);
        }
        Ok(
            json!({"value":publication.value,"error":false,"handles":handles,
            "max_body_bytes":publication.max_body_bytes}),
        )
    }

    pub fn output(&self, value: Value) -> Result<RpcData> {
        let group = value["exports"]["group"]
            .as_str()
            .filter(|group| group.len() <= 64)
            .map(str::to_owned);
        let result = self.output_inner(value);
        if result.is_err() {
            if let Some(group) = group {
                self.discard(&group);
            }
        }
        result
    }

    pub fn discard_unpublished(&self) {
        let groups: Vec<_> = self
            .resources
            .lock()
            .unwrap()
            .exports
            .iter()
            .filter(|(_, export)| export.lease.is_some())
            .map(|(group, _)| group.clone())
            .collect();
        for group in groups {
            self.discard(&group);
        }
    }

    fn discard(&self, group: &str) {
        let export = {
            let mut resources = self.resources.lock().unwrap();
            if resources
                .exports
                .get(group)
                .is_some_and(|export| export.lease.is_some())
            {
                resources.exports.remove(group)
            } else {
                None
            }
        };
        if export.is_some() {
            self.registry.remove(group);
        }
    }

    fn output_inner(&self, mut value: Value) -> Result<RpcData> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Exports {
            group: String,
            count: usize,
        }
        let exports = value
            .as_object_mut()
            .and_then(|value| value.remove("exports"));
        let Some(exports) = exports else {
            return cellar_guest::workers::rpc::output(value, self.options.max_body_bytes);
        };
        let exports: Exports = serde_json::from_value(exports)?;
        ensure!(
            value["error"] == false,
            "callable errors require a separate profile"
        );
        let encoded = value["value"]
            .as_str()
            .context("missing RPC result")?
            .to_owned();
        // Validate completion shape, graph bounds and markers before publishing
        // any authority. Serialized business objects cannot mint a lease.
        cellar_guest::workers::rpc::output(value, self.options.max_body_bytes)?;
        wire::markers(&encoded, exports.count, self.options.max_body_bytes)?;
        let mut resources = self.resources.lock().unwrap();
        let export = resources
            .exports
            .get_mut(&exports.group)
            .context("unknown RPC export")?;
        ensure!(
            export.entry.count == exports.count,
            "RPC export count changed"
        );
        let publication = Publication {
            format: 1,
            group: exports.group,
            count: exports.count,
            max_body_bytes: self.options.max_body_bytes,
            value: encoded,
        };
        let encoded = serde_json::to_string(&publication)?;
        ensure!(
            encoded.len() <= self.options.max_body_bytes,
            "RPC publication exceeds byte budget"
        );
        let lease = export
            .lease
            .take()
            .context("RPC export already published")?;
        RpcData::Json(encoded).with_lease(lease)
    }
}

fn group(input: &Value) -> Result<&str> {
    input["group"]
        .as_str()
        .filter(|group| group.len() <= 64)
        .context("invalid RPC group")
        .context(cellar_guest::failure::Validation)
}

fn next(resources: &mut Resources) -> Result<u64> {
    resources.next = resources
        .next
        .checked_add(1)
        .filter(|id| *id <= 9_007_199_254_740_991)
        .context("RPC handle identities exhausted")?;
    Ok(resources.next)
}

impl Drop for Session {
    fn drop(&mut self) {
        for group in self.resources.get_mut().unwrap().exports.keys() {
            self.registry.remove(group);
        }
        // Imports drop here even when native cancellation bypasses JS cleanup.
    }
}
