use super::*;
use crate::raft::options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions};
use crate::raft::storage::ReplicaLog;
use cellar_logic::consensus::{DiskLineage, ProcessGeneration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poisoned_registry_still_waits_for_resource_owners() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                directory.path().join("log"),
                NodeId::new([1; 16])?,
                DiskLineage::new([2; 16])?,
                ProcessGeneration::new([3; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let (runtime, _events) = RaftRuntime::start(log.clone(), RaftRuntimeOptions::default()).await?;
    let tls = RaftTls::new(
        vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into()],
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?;
    let transport = RaftTransport::start(
        runtime.clone(),
        Arc::new(PeerAuth::new([8; 32], "node-1")?),
        tls,
        TcpListener::bind("127.0.0.1:0").await?,
        TcpListener::bind("127.0.0.1:0").await?,
        TcpListener::bind("127.0.0.1:0").await?,
        TransportOptions::default(),
    )
    .await?;
    let (release, wait) = oneshot::channel();
    // Model a resource destructor which is still finishing after admission
    // closes. close() must retain its claim until this owner actually leaves.
    transport.handle.0.spawn(async move {
        let _ = wait.await;
    })?;
    let poisoned = transport.handle.0.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _registry = poisoned.registry.lock().unwrap();
            panic!("injected registry panic");
        }))
        .is_err()
    );
    domain
        .run(async {
            let close = transport.close();
            tokio::pin!(close);
            assert!(
                crate::asyncrt::timeout(std::time::Duration::from_millis(20), &mut close)
                    .await
                    .is_err()
            );
            assert!(transport.usage().tasks >= 1);
            assert!(transport.handle.0.claim.lock().unwrap().is_some());
            release.send(()).unwrap();
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), close).await?;
            assert_eq!(transport.usage().tasks, 0);
            assert!(transport.handle.0.claim.lock().unwrap().is_none());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    runtime.close().await?;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
