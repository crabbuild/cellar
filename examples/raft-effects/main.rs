//! A finite three-node producer/receiver recovery drill, with retained disks.
#![allow(clippy::disallowed_methods)]
mod effects;
use anyhow::{ensure, Context as _, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::{consensus::*, *};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

fn graph(script: &str) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![],
        native: None,
        script_name: script.into(),
        version: "example".into(),
        prefix: "raft-effects".into(),
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: script.into(),
            do_classes: vec![],
            bindings: vec![],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            stream_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: script.into(),
            modules: vec![],
            compat: Compat::default(),
        },
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        cron_revision: None,
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    })
}

async fn node(root: &Path, label: &str, application: Arc<dyn Application>) -> Result<Node> {
    let public = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let config = NodeConfig::new(
        ActorIdentity {
            node: label.into(),
            advertise: internal.local_addr()?.to_string(),
            region: "local-demo".into(),
        },
        ActorOptions {
            core: ActorPolicy {
                max_resident: 16,
                max_cached_routes: 0,
                max_activations: 8,
                max_evictions: 4,
                max_releases: 4,
                max_outbound_websockets: 8,
                ownership_on_evict: OwnershipOnEvict::Sticky,
                require_node_lease: true,
                peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                operation_deadline_ms: Some(5000),
                owner_log_recovery_backoff_ms: 100,
                owner_log_recovery_attempts: 3,
                alarm_resident_ms: 100,
                idle_evict_ms: None,
                pressure: Default::default(),
            },
            lease_ttl_ms: 5000,
            placement_weight: 1,
            generation: signer.public_key_hex(),
            resume_generation: None,
        },
        root.join(label),
    );
    Node::start(
        config,
        NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: tokio::runtime::Handle::current(),
            core_executor: tokio::runtime::Handle::current(),
            listener: public,
            internal_listener: internal,
            peer_http: reqwest::Client::builder().no_proxy().build()?,
            peer_auth: Arc::new(PeerAuth::new([19; 32], label)?),
            probe_signer: signer,
            fleet: None,
            containers: None,
            metrics: None,
        },
        graph(label),
        CompiledApplications::default()
            .register(label, "example", move || Ok(application.clone()))?,
    )
    .await
}

fn cell(services: &[Option<RaftCells>], index: usize, receiver: bool) -> Result<RaftCellHandle> {
    let scope = effects::scope(receiver)?;
    Ok(services[index]
        .as_ref()
        .context("stopped service")?
        .cell(scope.namespace, &scope.cell)?)
}

async fn leader(services: &[Option<RaftCells>], receiver: bool) -> Result<usize> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            for index in 0..3 {
                if services[index].is_some()
                    && cell(services, index, receiver)?
                        .status()
                        .await
                        .is_ok_and(|s| s.command_ready)
                {
                    return Ok::<_, anyhow::Error>(index);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

async fn write(
    handle: &RaftCellHandle,
    contract: OperationContract,
    input: Vec<u8>,
    sequence: u64,
) -> Result<CommandReceipt> {
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let mut options = CommandOptions::new(
        CommandId::new(ClientId::new([9; 16])?, sequence)?,
        now.checked_add(60_000).context("retry deadline overflow")?,
    );
    options.durability = Durability::S3;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        let result = handle
            .command(contract.clone(), input.clone(), options.clone())
            .await;
        if !matches!(result, Err(CommandFailure::Overloaded))
            || tokio::time::Instant::now() >= deadline
        {
            return Ok(result?);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn drain(services: &mut [Option<RaftCells>], nodes: &mut [Option<Node>]) -> Result<()> {
    let mut failed = false;
    for service in services.iter_mut().filter_map(Option::take) {
        failed |= !matches!(service.close().await,Ok(report) if report.unfinished.is_empty());
    }
    for node in nodes.iter_mut().filter_map(Option::take) {
        failed |= node.shutdown(ShutdownMode::Preserve).await.is_err();
    }
    ensure!(
        !failed,
        "incomplete shutdown; retain disks and pending identities"
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        arguments.len() == 4,
        "usage: cellar-raft-effects DATA_DIR CA.der CERT.der KEY.der"
    );
    let root = PathBuf::from(&arguments[0]);
    std::fs::create_dir_all(&root)?;
    let tls = RaftTls::new(
        vec![std::fs::read(&arguments[1])?.into()],
        vec![std::fs::read(&arguments[2])?.into()],
        rustls::pki_types::PrivateKeyDer::try_from(std::fs::read(&arguments[3])?)
            .map_err(|error| anyhow::anyhow!(error))?,
    )?;
    let store = cellar::bucket::Bucket::open_dev(&root.join("catalog.sqlite"))?.raft_store();
    // Development-only stable keys, never production credentials.
    let sender = Arc::new(RaftEffectSigningKey::new([51; 32])?);
    let receiver = Arc::new(RaftEffectSigningKey::new([52; 32])?);
    let routing = Arc::new(effects::Routing::default());
    let mut nodes = Vec::new();
    let mut services = Vec::new();
    let result=async {
        let mut definitions=Vec::new();let mut listeners=Vec::new();let mut peers=Vec::new();
        for index in 0..3u8 {
            let destination=effects::receiver(sender.verification_key(),receiver.clone())?;
            let target=RaftEffectTarget::new(effects::scope(true)?,destination.definition_digest(),destination.contract("Receive").context("Receive")?,receiver.verification_key(),Durability::S3)?;
            let source=effects::producer(sender.clone(),target,routing.clone())?;
            let contracts=(source.contract("Prime").context("source Prime")?,source.contract("Emit").context("Emit")?,source.contract("Get").context("source Get")?,destination.contract("Prime").context("receiver Prime")?,destination.contract("Get").context("receiver Get")?);
            let schemas=[source.schema_digest(),destination.schema_digest()];
            let application:Arc<dyn Application>=Arc::new(Registry::new(|_,_|Box::pin(async{Ok(Output::Done)})).raft_class(source)?.raft_class(destination)?);
            let label=format!("raft-effects-{}",index+1);
            nodes.push(Some(node(&root,&label,application.clone()).await?));
            let bound=NodeRaftListeners{control:tokio::net::TcpListener::bind("127.0.0.1:0").await?,data:tokio::net::TcpListener::bind("127.0.0.1:0").await?,query:tokio::net::TcpListener::bind("127.0.0.1:0").await?};
            let mut process=[0;16];getrandom::fill(&mut process)?;
            peers.push(RaftPeerNode{node:NodeId::new([index+1;16])?,disk:DiskLineage::new([index+10;16])?,process:ProcessGeneration::new(process)?,label,server_name:"localhost".into(),control_address:bound.control.local_addr()?,data_address:bound.data.local_addr()?,query_address:bound.query.local_addr()?});
            definitions.push((application,schemas,contracts));listeners.push(bound);
        }
        let registrations=[false,true].into_iter().enumerate().map(|(index,receiver)|{
            let scope=effects::scope(receiver)?;
            Ok(RaftCellRegistration{placement:RaftCellPlacement::Replica,namespace:NamespaceConfig::new(scope.cluster,scope.namespace,NamespaceMode::Raft,definitions[0].1[index]),class:if receiver{"Receiver"}else{"Producer"}.into(),birth:BirthProposal{scope,voters:peers.iter().enumerate().map(|(index,peer)|Ok(InitialMember{node:peer.node,member:MemberId::new(index as u64+1)?})).collect::<Result<_>>()?,learners:vec![]}})
        }).collect::<Result<Vec<_>>>()?;
        for (index,listeners) in listeners.into_iter().enumerate() {
            let node=nodes[index].as_ref().context("node")?;let log_dir=node.info().data_dir.join("raft");
            let mode=if log_dir.exists(){LogOpenMode::Resume}else{LogOpenMode::Create};
            let service=node.start_raft_cells(RaftCellsConfig{infrastructure:NodeRaftConfig{log:ReplicaLogOptions::new(log_dir,peers[index].node,peers[index].disk,peers[index].process,mode),consensus:RaftRuntimeOptions{tick_interval_ms:Some(30),..Default::default()},transport:TransportOptions::default()},catalog:CatalogOptions{prefix:"demo/raft-effects".into(),..Default::default()},cells:registrations.clone(),peers:peers.clone(),query_origins:vec![],host:RaftHostOptions{effects:Some(RaftEffectOptions{interval_ms:20,..Default::default()}),..Default::default()}},RaftCellsServices{listeners,tls:tls.clone(),store:store.clone(),application:definitions[index].0.clone(),versions:VersionKeys::new(VersionSigningKey::new([53;32],u64::MAX)?,vec![],Duration::from_secs(86400))?}).await?;
            services.push(Some(service));
        }
        routing.install(RaftNativeEffectSink::new((0..3).map(|index|cell(&services,index,true)).collect::<Result<_>>()?,Duration::from_millis(1000),60_000)?)?;
        let contracts=&definitions[0].2;
        let destination=leader(&services,true).await?;write(&cell(&services,destination,true)?,contracts.3.clone(),vec![],1).await?;
        let primary=leader(&services,false).await?;write(&cell(&services,primary,false)?,contracts.0.clone(),vec![],1).await?;
        let input=7i64.to_be_bytes().to_vec();
        let first=write(&cell(&services,primary,false)?,contracts.1.clone(),input.clone(),2).await?;
        let report=services[primary].take().context("primary service")?.close().await?;
        nodes[primary].take().context("primary node")?.shutdown(ShutdownMode::Preserve).await?;
        let replacement=leader(&services,false).await?;ensure!(replacement!=primary);
        let retried=write(&cell(&services,replacement,false)?,contracts.1.clone(),input,2).await?;
        ensure!(retried.id==first.id && retried.result==first.result && retried.durability==Durability::S3);
        let source=tokio::time::timeout(Duration::from_secs(20),async{loop{
            let read=cell(&services,replacement,false)?.query(contracts.2.clone(),vec![],QueryOptions{consistency:QueryConsistency::Linearizable,..Default::default()}).await?;
            if read.result.len()==24 && u64::from_be_bytes(read.result[16..24].try_into()?)==1 {return Ok::<_,anyhow::Error>(read);}
            tokio::time::sleep(Duration::from_millis(20)).await;
        }}).await??;
        ensure!(i64::from_be_bytes(source.result[..8].try_into()?)==7 && u64::from_be_bytes(source.result[8..16].try_into()?)==1);
        for index in 0..3 {if services[index].is_some(){
            let read=cell(&services,index,false)?.query(contracts.2.clone(),vec![],QueryOptions{consistency:QueryConsistency::AtLeast(source.version_token.clone()),..Default::default()}).await?;ensure!(read.result==source.result);
        }}
        let destination=leader(&services,true).await?;
        let received=cell(&services,destination,true)?.query(contracts.4.clone(),vec![],QueryOptions{consistency:QueryConsistency::Linearizable,..Default::default()}).await?;
        ensure!(i64::from_be_bytes(received.result.as_slice().try_into()?)==7);
        // Await the S3 settlement output before final graceful cleanup. This
        // does not authorize deleting retained replica disks.
        tokio::time::timeout(Duration::from_secs(10),async{loop{
            let status=cell(&services,replacement,false)?.status().await?;
            if status.command_ready && status.archived>=source.version.position.index {return Ok::<_,anyhow::Error>(());}
            tokio::time::sleep(Duration::from_millis(20)).await;
        }}).await??;
        let report=serde_json::json!({"producer_value":7,"emitted":1,"settled":1,"receiver_value":7,"primary_replaced":true,"stopped_primary_unfinished":report.unfinished.len()});
        std::fs::write(root.join("result.json"),serde_json::to_vec_pretty(&report)?)?;
        Ok::<_,anyhow::Error>(())
    }.await;
    let joined = drain(&mut services, &mut nodes).await;
    result?;
    joined?;
    cellar::note!("Producer/receiver recovery passed. Repeat with the same data directory to verify durable deduplication across restart; retain its disks and catalog.");
    Ok(())
}
