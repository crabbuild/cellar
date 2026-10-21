//! A real cell can consume its own durable delivery without recursive schema
//! hashes, reentrant callbacks or a second internal command-client allocator.
#![allow(clippy::disallowed_methods)]
use super::{faults, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, Application, Output, Registry, ShutdownMode};
use futures_util::future::BoxFuture;
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Default)]
struct Routing(RwLock<Option<Arc<RaftNativeEffectSink>>>);

impl RaftEffectSink for Routing {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, Result<Vec<u8>, CommandFailure>> {
        match self.0.read().ok().and_then(|guard| guard.clone()) {
            Some(sink) => sink.deliver(request),
            None => Box::pin(async { Err(CommandFailure::Unavailable) }),
        }
    }
}

pub async fn self_delivery() -> Result<()> {
    let sender = Arc::new(RaftEffectSigningKey::new([95; 32])?);
    let receiver = Arc::new(RaftEffectSigningKey::new([96; 32])?);
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "Loopback:one".into(),
        group: GroupId::new([8; 16])?,
        incarnation: IncarnationId::new([9; 16])?,
    };
    let class = RaftClass::new("Loopback", 1, [97; 32])?;
    let definition = class.definition_digest();
    let contract = OperationContract {
        operation: "Receive".into(),
        kind: OperationKind::Command,
        digest: [98; 32],
    };
    let target = RaftEffectTarget::new(
        scope.clone(),
        definition,
        contract.clone(),
        receiver.verification_key(),
        Durability::S3,
    )?;
    let inbox = RaftInbox::new(
        vec![RaftEffectSourceGrant::new(
            scope.cluster,
            scope.namespace,
            "Loopback",
            "Events",
            [99; 32],
            sender.verification_key(),
        )?],
        receiver,
        Durability::S3,
        4,
    )?;
    let routing = Arc::new(Routing::default());
    let outbox = RaftOutbox::new(sender, RaftOutboxLimits::default())?.binding(
        "Events",
        [99; 32],
        target,
        routing.clone(),
    )?;
    let class = class
        .inbox("Receive", contract.digest, inbox, |context, payload| {
            let amount = i64::from_be_bytes(payload.try_into()?);
            context
                .sql()
                .execute("UPDATE data SET received=received+?1", [amount])?;
            Ok(amount.to_be_bytes().to_vec())
        })?
        .outbox(outbox)?
        .command("Prime", [100; 32], |context, _| {
            context.sql().execute_batch(
                "CREATE TABLE data(sent INTEGER,received INTEGER);INSERT INTO data VALUES(0,0)",
            )?;
            Ok(vec![])
        })?
        .command("Emit", [101; 32], |context, input| {
            let amount = i64::from_be_bytes(input.try_into()?);
            context
                .sql()
                .execute("UPDATE data SET sent=sent+?1", [amount])?;
            Ok(context.emit("Events", input)?.to_be_bytes().to_vec())
        })?
        .query("Get", [102; 32], |context, _| {
            let (sent, received): (i64, i64) =
                context.query_row("SELECT sent,received FROM data", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
            let progress = context.outbox_progress("Events")?;
            let mut bytes = sent.to_be_bytes().to_vec();
            bytes.extend(received.to_be_bytes());
            bytes.extend(progress.emitted.to_be_bytes());
            bytes.extend(progress.settled.to_be_bytes());
            Ok(bytes)
        })?;
    ensure!(class.definition_digest() == definition && class.schema_digest() != definition);
    let prime = class.contract("Prime").context("Prime")?;
    let emit = class.contract("Emit").context("Emit")?;
    let get = class.contract("Get").context("Get")?;
    let mut namespace = NamespaceConfig::new(
        scope.cluster,
        scope.namespace,
        NamespaceMode::Raft,
        class.schema_digest(),
    );
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace,
        class: "Loopback".into(),
        birth: BirthProposal {
            scope: scope.clone(),
            voters: vec![InitialMember {
                member: MemberId::new(1)?,
                node: NodeId::new([1; 16])?,
            }],
            learners: vec![],
        },
    };
    let application: Arc<dyn Application> =
        Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
    let class = Arc::downgrade(&application.resolve_raft_class("Loopback")?);
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), application.clone()).await?;
    let service = public_node::cells_with_host(
        &node,
        application.clone(),
        Arc::new(faults::Store::default()),
        registration,
        false,
        RaftHostOptions {
            effects: Some(RaftEffectOptions {
                interval_ms: 20,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    let cell = service.cell(scope.namespace, &scope.cell)?;
    public_node::ready(&cell).await?;
    *routing
        .0
        .write()
        .map_err(|_| anyhow::anyhow!("routing lock"))? = Some(RaftNativeEffectSink::new(
        vec![cell.clone()],
        Duration::from_secs(1),
        60_000,
    )?);
    for (sequence, contract, input) in [(1, prime, vec![]), (2, emit, 7i64.to_be_bytes().to_vec())]
    {
        let mut options = public_node::options(sequence)?;
        options.durability = Durability::S3;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            let result = cell
                .command(contract.clone(), input.clone(), options.clone())
                .await;
            if !matches!(result, Err(CommandFailure::Overloaded))
                || tokio::time::Instant::now() >= deadline
            {
                let _ = result?;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let read = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let read = cell
                .query(
                    get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await;
            let read = match read {
                Ok(read) => read,
                Err(CommandFailure::Overloaded | CommandFailure::Unavailable) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if read.result.len() == 32 && u64::from_be_bytes(read.result[24..].try_into()?) == 1 {
                return Ok::<_, anyhow::Error>(read);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let expected = [
        7i64.to_be_bytes(),
        7i64.to_be_bytes(),
        1u64.to_be_bytes(),
        1u64.to_be_bytes(),
    ]
    .concat();
    ensure!(read.result == expected);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = cell.status().await;
            if status.is_ok_and(|status| {
                status.command_ready && status.archived >= read.version.position.index
            }) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    ensure!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    drop(cell);
    drop(service);
    drop(application);
    // Retaining the routing adapter must not retain a stopped host's class
    // through class -> outbox -> sink -> handle -> class.
    tokio::time::timeout(Duration::from_secs(5), async {
        while class.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    ensure!(routing
        .0
        .read()
        .map_err(|_| anyhow::anyhow!("routing lock"))?
        .is_some());
    Ok(())
}
