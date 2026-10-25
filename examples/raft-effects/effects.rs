// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit producer lanes and a receiver transaction; no mutable Cell factory.
use anyhow::{ensure, Result};
use cellar::sdk::consensus::*;
use futures_util::future::BoxFuture;
use std::sync::{Arc, RwLock};

pub const LANE: &str = "Events";
pub const BINDING: [u8; 32] = [41; 32];

pub fn scope(receiver: bool) -> Result<GroupScope> {
    Ok(GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([if receiver { 3 } else { 2 }; 16])?,
        cell: if receiver {
            "Receiver:one"
        } else {
            "Producer:one"
        }
        .into(),
        group: GroupId::new([if receiver { 31 } else { 30 }; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    })
}

/// Endpoint refresh is separate from immutable logical target identity. The
/// SDK and signed receipt validate the configured history and command contract.
#[derive(Default)]
pub struct Routing(RwLock<Option<Arc<RaftNativeEffectSink>>>);

impl Routing {
    pub fn install(&self, sink: Arc<RaftNativeEffectSink>) -> Result<()> {
        *self
            .0
            .write()
            .map_err(|_| anyhow::anyhow!("routing lock poisoned"))? = Some(sink);
        Ok(())
    }
}

impl RaftEffectSink for Routing {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, Result<Vec<u8>, CommandFailure>> {
        let sink = self.0.read().ok().and_then(|guard| guard.clone());
        match sink {
            Some(sink) => sink.deliver(request),
            None => Box::pin(async { Err(CommandFailure::Unavailable) }),
        }
    }
}

pub fn receiver(
    source: RaftEffectVerificationKey,
    key: Arc<RaftEffectSigningKey>,
) -> Result<RaftClass> {
    let origin = scope(false)?;
    let inbox = RaftInbox::new(
        vec![RaftEffectSourceGrant::new(
            origin.cluster,
            origin.namespace,
            "Producer",
            LANE,
            BINDING,
            source,
        )?],
        key,
        Durability::S3,
        16,
    )?;
    Ok(RaftClass::new("Receiver",1,[42;32])?
        .inbox("Receive",[43;32],inbox,|context,payload| {
            let amount=i64::from_be_bytes(payload.try_into()?);
            context.sql().execute("UPDATE receiver SET value=value+?1",[amount])?;
            Ok(amount.to_be_bytes().to_vec())
        })?
        .command("Prime",[44;32],|context,_|{context.sql().execute_batch("CREATE TABLE IF NOT EXISTS receiver(value INTEGER);INSERT INTO receiver SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM receiver)")?;Ok(vec![])})?
        .query("Get",[45;32],|context,_|{let value:i64=context.query_row("SELECT value FROM receiver",[],|row|row.get(0))?;Ok(value.to_be_bytes().to_vec())})?)
}

pub fn producer(
    key: Arc<RaftEffectSigningKey>,
    target: RaftEffectTarget,
    routing: Arc<Routing>,
) -> Result<RaftClass> {
    let outbox = RaftOutbox::new(key, RaftOutboxLimits::default())?
        .binding(LANE, BINDING, target, routing)?;
    Ok(RaftClass::new("Producer",1,[46;32])?.outbox(outbox)?
        .command("Prime",[47;32],|context,_|{context.sql().execute_batch("CREATE TABLE IF NOT EXISTS producer(value INTEGER);INSERT INTO producer SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM producer)")?;Ok(vec![])})?
        .command("Emit",[48;32],|context,input|{
            ensure!(input.len()==8);let amount=i64::from_be_bytes(input.try_into()?);
            context.sql().execute("UPDATE producer SET value=value+?1",[amount])?;
            Ok(context.emit(LANE,input)?.to_be_bytes().to_vec())
        })?
        .query("Get",[49;32],|context,_|{
            let value:i64=context.query_row("SELECT value FROM producer",[],|row|row.get(0))?;
            let progress=context.outbox_progress(LANE)?;
            let mut result=value.to_be_bytes().to_vec();result.extend(progress.emitted.to_be_bytes());result.extend(progress.settled.to_be_bytes());Ok(result)
        })?)
}
