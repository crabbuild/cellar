// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The actual shared registered-command owner with an extent physical backend.
//! Public node configuration and remote root transport remain separate gates.
use super::*;

impl Host {
    async fn authority(&self) -> Result<CommandAuthority> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = self.runtime.status(&scope()).await?;
                match CommandAuthority::from_status(self.log.group(&scope())?, &status) {
                    Ok(authority) => return Ok(authority),
                    Err(ExecutionError::Refused(CommandFailure::Unavailable)) => {
                        tokio::time::sleep(Duration::from_millis(1)).await
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await?
    }

    async fn executor(
        &self,
        root: &Path,
        schema: [u8; 32],
    ) -> Result<(PrivateExecutor, cellar_ltx::internal::NodeWorkspace)> {
        let limits = ExecutionLimits::default();
        let workspace = self
            .store
            .create_workspace([99; 32], RootPrivateOptions::workspace_bounds(limits)?)?;
        let host = cellar_ltx::LtxHost::new(
            || 10,
            |_| Ok(Duration::ZERO),
            |path| async move { std::fs::read(path) },
            |job| async move {
                job();
                Ok(())
            },
        )
        .with_filesystem(Arc::new(DirectFileSystem));
        let executor = PrivateExecutor::open_rooted(
            RootPrivateOptions {
                workspace: workspace.clone(),
                host,
                sqlite_vfs: None,
                images: self.store.clone(),
                query_services: ReadServices {
                    now_unix_millis: Arc::new(|| 10),
                    fill_random: Arc::new(|bytes| bytes.fill(17)),
                },
                application_digest: schema,
                limits,
                cache_directory: root.join("private-cache"),
                cache_pool: ScratchPool::new(ScratchLimits {
                    max_bytes: 2 * EXTENT_BYTES,
                    max_files: 2,
                })?,
                cache: CacheLimits {
                    max_cached_bytes: EXTENT_BYTES,
                    max_cached_extents: 1,
                },
            },
            self.authority().await?,
            self.applier.current(),
        )?;
        Ok((executor, workspace))
    }

    async fn commit_executor(
        &mut self,
        executor: &mut PrivateExecutor,
        prepared: PreparedCommand,
    ) -> Result<CommittedReceipt> {
        executor.mark_submitted(prepared.proposal())?;
        self.runtime
            .propose(&scope(), prepared.context(), prepared.bytes().to_vec())
            .await?;
        let (ticket, entry) = self.next_apply().await?;
        let completion = self
            .applier
            .prepare(ticket.clone(), &entry, &mut self.buffer)?;
        self.runtime.complete_apply(completion).await?;
        let published = self.applied(&ticket).await?;
        let AppliedExecution::Command(receipt) = executor.finish_rooted(published)? else {
            bail!("not a command receipt");
        };
        executor.refresh_authority(self.authority().await?)?;
        Ok(receipt)
    }
}

fn registered_attempt(sequence: u64, tag: u8, input: &[u8]) -> Result<CommandAttempt> {
    Ok(CommandAttempt {
        request: CommandRequest {
            id: CommandId::new(ClientId::new([1; 16])?, sequence)?,
            operation: "Increment".into(),
            contract_digest: [41; 32],
            request_digest: cellar::sdk::consensus::command_request_digest(input),
            durability: Durability::Quorum,
            retry_until_ms: 1000,
        },
        proposal: ProposalId::new([tag; 16])?,
        preview: cellar_ltx::generation::ImageId::new([tag; 16])?,
        inputs: cellar::storage::CommandInputs {
            now_unix_millis: 10,
            random: [17; 32],
        },
    })
}

fn staged(outcome: StageOutcome) -> Result<PreparedCommand> {
    match outcome {
        StageOutcome::Prepared(prepared) => Ok(prepared),
        StageOutcome::Duplicate(_) => bail!("unexpected duplicate"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_executor_preserves_registered_commands_receipts_and_restart_on_extent_images(
) -> Result<()> {
    use cellar::sdk::consensus::RaftClass;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let directory = tempfile::tempdir()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let class=RaftClass::new("Counter",1,[40;32])?.command("Increment",[41;32],move |context,input| {
        called.fetch_add(1,Ordering::SeqCst);
        assert_eq!(context.inputs().now_unix_millis,10);
        assert_eq!(context.inputs().random,[17;32]);
        let delta=i64::from_be_bytes(input.try_into()?);
        context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter);")?;
        assert!(context.sql().query_row("SELECT count(*) FROM __sys_raft_receipts",[],|row|row.get::<_,i64>(0)).is_err());
        context.sql().execute_batch("CREATE TABLE IF NOT EXISTS padding(id INTEGER PRIMARY KEY,value BLOB)")?;
        let initialized:bool=context.sql().query_row("SELECT EXISTS(SELECT 1 FROM padding)",[],|row|row.get(0))?;
        if !initialized {
            for id in 0..24 { context.sql().execute("INSERT INTO padding VALUES(?1,zeroblob(1048576))",[id])?; }
        }
        context.sql().execute("UPDATE counter SET value=value+?1",[delta])?;
        let value:i64=context.sql().query_row("SELECT value FROM counter",[],|row|row.get(0))?;
        Ok(value.to_be_bytes().to_vec())
    })?;
    let schema = class.schema_digest();
    let mut host = Host::open_schema(directory.path(), false, 1, schema).await?;
    host.elect().await?;
    let (mut executor, workspace) = host.executor(directory.path(), schema).await?;
    let input = 7_i64.to_be_bytes();
    let first =
        staged(executor.stage_registered(registered_attempt(1, 1, &input)?, &class, &input)?)?;
    assert_eq!(
        host.commit_executor(&mut executor, first)
            .await?
            .receipt()
            .result(),
        input
    );
    let input = 1_i64.to_be_bytes();
    let second =
        staged(executor.stage_registered(registered_attempt(2, 2, &input)?, &class, &input)?)?;
    assert_eq!(
        executor.extent_observation_work().unwrap().image_bytes_read,
        EXTENT_BYTES
    );
    let committed = host.commit_executor(&mut executor, second).await?;
    assert_eq!(committed.receipt().result(), 8_i64.to_be_bytes());
    let StageOutcome::Duplicate(duplicate) =
        executor.stage_registered(registered_attempt(2, 3, &input)?, &class, &input)?
    else {
        bail!("receipt retry executed again");
    };
    assert_eq!(duplicate.receipt(), committed.receipt());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let logical = host
        .applier
        .current()
        .unwrap()
        .image()
        .manifest()
        .geometry()
        .length();
    assert!(logical > 24 * 1024 * 1024);
    executor.close()?;
    assert!(
        std::fs::metadata(workspace.directory().join("working.sqlite"))?.len() < logical,
        "closing a disposable writer backfilled its whole WAL"
    );
    assert!(
        std::fs::metadata(workspace.directory().join("working.sqlite-wal"))?.len() > 1024 * 1024
    );
    assert_eq!(
        host.store.usage()?.workspaces,
        1,
        "live pin lost its named-file reservation"
    );
    let private_directory = workspace.directory().to_path_buf();
    drop(workspace);
    assert!(!private_directory.exists());
    assert_eq!(host.store.usage()?.workspaces, 0);
    host.close().await?;
    let mut resumed = Host::open_schema(directory.path(), true, 2, schema).await?;
    resumed.elect().await?;
    let (mut executor, workspace) = resumed.executor(directory.path(), schema).await?;
    let StageOutcome::Duplicate(retained) =
        executor.stage_registered(registered_attempt(2, 4, &input)?, &class, &input)?
    else {
        bail!("restart lost the retained receipt");
    };
    assert_eq!(retained.receipt(), committed.receipt());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let third =
        staged(executor.stage_registered(registered_attempt(3, 5, &input)?, &class, &input)?)?;
    assert_eq!(
        executor.extent_observation_work().unwrap().image_bytes_read,
        EXTENT_BYTES
    );
    assert_eq!(
        resumed
            .commit_executor(&mut executor, third)
            .await?
            .receipt()
            .result(),
        9_i64.to_be_bytes()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let wrong = registered_attempt(4, 6, &input)?;
    assert!(matches!(
        executor.stage_registered(wrong, &class, b"different"),
        Err(ExecutionError::Refused(CommandFailure::CommandConflict))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    executor.close()?;
    drop(workspace);
    assert_eq!(resumed.store.usage()?.workspaces, 0);
    resumed.close().await
}
