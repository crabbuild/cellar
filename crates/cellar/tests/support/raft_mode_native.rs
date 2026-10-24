// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained Raft history cannot downgrade into a mutable Fleet factory.
use super::{public_node, registration, wait_status};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::consensus::verification::{Catalog, CatalogError};
use cellar::sdk::{
    consensus::*, Application, ExecutionDomain, Node, NodeOutcome, Output, Registry, ShutdownMode,
};
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

fn tree_digest(root: &Path) -> Result<[u8; 32]> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for item in std::fs::read_dir(directory)? {
            let item = item?;
            let kind = item.file_type()?;
            ensure!(!kind.is_symlink(), "unexpected link in private fixture");
            if kind.is_dir() {
                pending.push(item.path());
            } else if kind.is_file() {
                files.push(item.path());
            }
        }
    }
    files.sort();
    let mut digest = Sha256::new();
    for file in files {
        let relative = file
            .strip_prefix(root)?
            .to_str()
            .context("fixture file name")?;
        digest.update((relative.len() as u64).to_be_bytes());
        digest.update(relative.as_bytes());
        let bytes = std::fs::read(file)?;
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    Ok(digest.finalize().into())
}

async fn retained_digests(domain: &ExecutionDomain, root: PathBuf) -> Result<[[u8; 32]; 2]> {
    domain
        .run(async {
            cellar::asyncrt::try_blocking(move || {
                Ok::<_, anyhow::Error>([
                    tree_digest(&root.join("raft"))?,
                    tree_digest(&root.join("raft-cells"))?,
                ])
            })?
            .await?
        })
        .await?
}

pub async fn rollback_mode_guard() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("node");
    let verification = ExecutionDomain::new(tokio::runtime::Handle::current());
    let store = Arc::new(object_store::memory::InMemory::new());
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let registration = registration(definition.schema)?;
    let fallback_calls = Arc::new(AtomicUsize::new(0));
    let observed = fallback_calls.clone();
    let disabled: Arc<dyn Application> = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).cell(
            "Counter",
            move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("ordinary factory must not open a Raft history")
            },
        )?,
    );
    let mut node: Option<Node> = None;
    let result=async {
        node=Some(public_node::node(&directory,definition.application.clone()).await?);
        let original=public_node::cells(node.as_ref().context("node")?,definition.application.clone(),store.clone(),registration.clone(),false).await?;
        let cell=original.cell(registration.namespace.namespace,&registration.birth.scope.cell)?;
        public_node::ready(&cell).await?;
        let mut options=public_node::options(1)?;options.durability=Durability::S3;
        let acknowledged=cell.command(definition.increment.clone(),7i64.to_be_bytes().to_vec(),options).await?;
        let outcome=node.take().context("node")?.shutdown(ShutdownMode::Preserve).await?;
        let NodeOutcome::Stopped(report)=outcome else {anyhow::bail!("unexpected stop: {outcome:?}")};
        let closed=report.raft.context("original Raft stop")?.result?;
        ensure!(closed.unfinished.is_empty() && closed.groups_with_archive_lag==0);
        ensure!(original.close().await?==closed && calls.load(Ordering::Acquire)==1);
        let retained=retained_digests(&verification,directory.clone()).await?;
        let key=object_store::path::Path::from(format!("host/test/namespaces/{}/root",hex::encode(registration.namespace.namespace.bytes())));
        let root_bytes=store.get(&key).await?.bytes().await?;

        let denied=public_node::node_declared(&directory,definition.application.clone(),"raft-host",vec!["Counter".into()]).await.err().context("Raft class was admitted as a Fleet class")?;
        ensure!(matches!(denied.downcast_ref::<RegistrationError>(),Some(RegistrationError::WrongMode{mode:NamespaceMode::Fleet,..})));
        node=Some(public_node::node(&directory,disabled.clone()).await?);
        let denied=public_node::cells(node.as_ref().context("node")?,disabled,store.clone(),registration.clone(),true).await.err().context("ordinary factory admitted for retained Raft scope")?;
        ensure!(matches!(denied.downcast_ref::<RegistrationError>(),Some(RegistrationError::UnsupportedFactory(class)) if class=="Counter"));
        ensure!(fallback_calls.load(Ordering::Acquire)==0);
        verification.run(async {
            let catalog=Catalog::new(store.clone(),CatalogOptions{prefix:"host/test".into(),..Default::default()})?;
            let mut fleet=registration.namespace.clone();fleet.mode=NamespaceMode::Fleet;
            ensure!(matches!(catalog.existing_namespace(fleet).await,Err(CatalogError::Conflict(_))));
            Ok::<_,anyhow::Error>(())
        }).await??;
        ensure!(store.get(&key).await?.bytes().await?==root_bytes);
        ensure!(retained_digests(&verification,directory.clone()).await?==retained,"disabled mode touched retained Raft files");
        ensure!(matches!(cell.command(definition.increment.clone(),1i64.to_be_bytes().to_vec(),public_node::options(2)?).await,Err(CommandFailure::Unavailable)));
        node.take().context("node")?.shutdown(ShutdownMode::Preserve).await?;

        node=Some(public_node::node(&directory,definition.application.clone()).await?);
        let restored=public_node::cells(node.as_ref().context("node")?,definition.application.clone(),store.clone(),registration.clone(),true).await?;
        let cell=restored.cell(registration.namespace.namespace,&registration.birth.scope.cell)?;
        public_node::ready(&cell).await?;
        let read=cell.query(definition.get,vec![],QueryOptions{consistency:QueryConsistency::AtLeast(acknowledged.version_token),..Default::default()}).await?;
        ensure!(read.result==7i64.to_be_bytes() && calls.load(Ordering::Acquire)==1 && fallback_calls.load(Ordering::Acquire)==0);
        wait_status(&cell,|status|status.command_ready).await?;
        let next=cell.command(definition.increment,1i64.to_be_bytes().to_vec(),public_node::options(2)?).await?;
        ensure!(next.result==8i64.to_be_bytes() && calls.load(Ordering::Acquire)==2);
        let outcome=node.take().context("node")?.shutdown(ShutdownMode::Preserve).await?;
        let NodeOutcome::Stopped(report)=outcome else {anyhow::bail!("unexpected resume stop: {outcome:?}")};
        let resumed=report.raft.context("resumed Raft stop")?.result?;
        ensure!(resumed.storage_instance==closed.storage_instance && resumed.node.process==ProcessGeneration::new([12;16])? && resumed.unfinished.is_empty());
        ensure!(restored.close().await?==resumed);
        Ok::<_,anyhow::Error>(())
    }.await;
    if let Some(node) = node {
        node.cancel().await?;
    }
    verification.shutdown().await;
    result
}
