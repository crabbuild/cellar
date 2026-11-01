// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real TLS output proofs, original deadlines and physical response ownership.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellCommandGateway, CellRemoteCommandResponse, CellRemoteEnrollmentResponse,
    CellRemoteReceiptResponse,
};
#[path = "cell_group_s3_remote_wire.rs"]
mod wire;
pub(crate) use wire::s3_remote_slow_delivery;

pub(super) struct Remote {
    gateway: CellCommandGateway,
    router: super::super::query_origins::Router,
}

impl Remote {
    pub(super) fn transport(&self) -> &RaftTransport {
        &self.router.transport
    }

    pub(super) async fn start(h: &Harness, dispatcher: CellGroupDispatcher) -> Result<Self> {
        let gateway = h.fixture.transports[0].install_cell_command_gateway(
            dispatcher,
            h.controllers[0].clone(),
            h.fixture.cell_options(&h.cell).await?,
        )?;
        let router = super::super::query_origins::Router::start(&h.fixture).await?;
        router
            .transport
            .install_cell_query_group(h.fixture.tcp_route(h.route.history)?)?;
        router.grant(&h.fixture, 1)?;
        h.fixture.transports[0].grant_command_origin(
            cellar::application::consensus::NamespaceCommandOrigin {
                cluster: h.route.history.cluster,
                namespace: h.route.history.namespace,
                revision: 1,
                peer: router.peer.clone(),
            },
        )?;
        Ok(Self { gateway, router })
    }

    pub(super) async fn close(self) -> Result<()> {
        drop(self.gateway);
        self.router.close().await
    }
}

pub(super) fn command_request(
    h: &Harness,
    sequence: u64,
    input: &[u8],
    durability: Durability,
) -> Result<CellCommandRequest> {
    Ok(CellCommandRequest {
        id: CommandId::new(ClientId::new([188; 16])?, sequence)?,
        contract: h.class.contract("Add").context("Add")?,
        input: input.to_vec(),
        durability,
        retry_until_ms: h.retry_until,
    })
}

async fn command(
    h: &Harness,
    remote: &Remote,
    sequence: u64,
    input: &[u8],
    durability: Durability,
) -> Result<CellRemoteCommandResponse> {
    command_using(
        h,
        remote,
        command_request(h, sequence, input, durability)?,
        bounds(),
    )
    .await
}

pub(super) async fn command_using(
    h: &Harness,
    remote: &Remote,
    request: CellCommandRequest,
    limits: CellRoutingLimits,
) -> Result<CellRemoteCommandResponse> {
    let mut invocation = h
        .dispatcher
        .command(h.cell.clone(), request, limits, None)
        .await?;
    invocation.begin().await?;
    Ok(invocation
        .execute_remote(&remote.router.transport, MemberId::new(1)?)
        .await?)
}

async fn receipt(
    h: &Harness,
    remote: &Remote,
    known: bool,
    durability: Durability,
) -> Result<CellRemoteReceiptResponse> {
    let mut request = h.receipt_request(1, b"remote", durability)?;
    if !known {
        request.id = CommandId::new(ClientId::new([189; 16])?, 1)?;
    }
    let mut invocation = h
        .dispatcher
        .lookup_receipt(h.cell.clone(), request, bounds(), None)?;
    invocation.begin().await?;
    Ok(invocation
        .execute_remote(&remote.router.transport, MemberId::new(1)?)
        .await?)
}

async fn enrollment(
    h: &Harness,
    remote: &Remote,
    durability: Durability,
) -> Result<CellRemoteEnrollmentResponse> {
    let mut invocation = h
        .dispatcher
        .enroll(
            h.cell.clone(),
            CellEnrollmentRequest {
                creation: ProposalId::new([210; 16])?,
                durability,
            },
            bounds(),
            None,
        )
        .await?;
    invocation.begin().await?;
    Ok(invocation
        .execute_remote(&remote.router.transport, MemberId::new(1)?)
        .await?)
}

fn bounds() -> CellRoutingLimits {
    CellRoutingLimits {
        timeout: Duration::from_secs(15),
        max_attempts: 4,
    }
}

async fn refunded(h: &Harness) -> Result<()> {
    controller::wait_for(async || {
        Ok(h.controllers[0].usage().retained_s3 == 0
            && h.controllers[0].usage().retained_commands == 0
            && h.fixture.transports[0].usage().query_items == 0)
    })
    .await
}

pub(crate) async fn s3_remote_outputs(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    for minimum in [Durability::Quorum, Durability::S3] {
        let store = Arc::new(object_store::prefix::PrefixStore::new(
            store.clone(),
            ObjectPath::from(if minimum == Durability::S3 {
                "minimum-s3"
            } else {
                "minimum-quorum"
            }),
        ));
        let mut h = Harness::with_minimum(rooted, store, 1, minimum).await?;
        let remote = Remote::start(&h, h.dispatcher.clone()).await?;
        let requested = if minimum == Durability::S3 {
            Durability::Quorum
        } else {
            Durability::S3
        };
        let response = command(&h, &remote, 1, b"remote", requested).await?;
        assert_eq!(response.durability(), Durability::S3);
        assert_eq!(response.result(), b"remote");
        assert_eq!(response.observation().version.sequence.get(), 2);
        let expected = response.observation().clone();
        drop(response);
        refunded(&h).await?;
        let found = receipt(&h, &remote, true, Durability::Quorum).await?;
        assert_eq!(found.durability(), Durability::S3);
        assert_eq!(found.result(), Some(b"remote".as_slice()));
        assert_eq!(*found.observation(), expected);
        drop(found);
        refunded(&h).await?;
        let empty = command(&h, &remote, 2, b"", requested).await?;
        assert_eq!(empty.result(), b"");
        assert_eq!(empty.durability(), Durability::S3);
        drop(empty);
        refunded(&h).await?;
        let unknown = receipt(&h, &remote, false, requested).await?;
        assert_eq!(unknown.durability(), Durability::S3);
        assert_eq!(unknown.result(), None);
        assert_eq!(unknown.observation().version.sequence.get(), 3);
        drop(unknown);
        refunded(&h).await?;
        assert_eq!(h.calls.load(Ordering::SeqCst), 2);
        h.cell.cell = CellId::new("remote-created")?;
        let created = enrollment(&h, &remote, requested).await?;
        assert_eq!(created.durability(), Durability::S3);
        assert_eq!(created.record().cell(), &h.cell);
        let original = created.record().clone();
        drop(created);
        refunded(&h).await?;
        let changed = command(&h, &remote, 1, b"changed", requested).await?;
        assert_eq!(changed.durability(), Durability::S3);
        drop(changed);
        refunded(&h).await?;
        let retry = enrollment(&h, &remote, requested).await?;
        assert_eq!(retry.record(), &original);
        assert_eq!(retry.durability(), Durability::S3);
        drop(retry);
        refunded(&h).await?;
        assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
        assert_eq!(h.calls.load(Ordering::SeqCst), 3);
        assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
        remote.close().await?;
        h.close().await?;
    }
    // Reachable S3 and a bound sender cannot lend an unbound gateway a proof.
    let h = Harness::new(
        rooted,
        Arc::new(object_store::prefix::PrefixStore::new(store, "unbound")),
        1,
    )
    .await?;
    let unbound = CellGroupDispatcher::new(
        &h.fixture.cluster.domain,
        h.fixture.cluster.directory.clone(),
        h.class.clone(),
        VersionKeys::new(
            VersionSigningKey::new([89; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?,
        CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?;
    let remote = Remote::start(&h, unbound).await?;
    let mut attempt = h
        .dispatcher
        .command(
            h.cell.clone(),
            command_request(&h, 1, b"remote", Durability::S3)?,
            bounds(),
            None,
        )
        .await?;
    attempt.begin().await?;
    let error = attempt
        .execute_remote(&remote.router.transport, MemberId::new(1)?)
        .await
        .err()
        .context("unbound command")?;
    assert!(error.ambiguous);
    assert!(matches!(
        *error.cause,
        CellDispatchError::Execution(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability
        ))
    ));
    drop(attempt);
    assert!(receipt(&h, &remote, false, Durability::S3).await.is_err());
    assert!(enrollment(&h, &remote, Durability::S3).await.is_err());
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.initializations.load(Ordering::SeqCst), 0);
    refunded(&h).await?;
    remote.close().await?;
    h.close().await
}

async fn deadline_attempt(h: &Harness, remote: &Remote, kind: &str) -> Result<()> {
    let bounds = CellRoutingLimits {
        timeout: Duration::from_millis(800),
        max_attempts: 4,
    };
    match kind {
        "command" => {
            let mut i = h
                .dispatcher
                .command(
                    h.cell.clone(),
                    command_request(h, 1, b"deadline", Durability::S3)?,
                    bounds,
                    None,
                )
                .await?;
            i.begin().await?;
            let error = i
                .execute_remote(&remote.router.transport, MemberId::new(1)?)
                .await
                .err()
                .context("S3 command deadline")?;
            assert!(error.ambiguous);
        }
        "enrollment" => {
            let mut i = h
                .dispatcher
                .enroll(
                    h.cell.clone(),
                    CellEnrollmentRequest {
                        creation: ProposalId::new([210; 16])?,
                        durability: Durability::S3,
                    },
                    bounds,
                    None,
                )
                .await?;
            i.begin().await?;
            let error = i
                .execute_remote(&remote.router.transport, MemberId::new(1)?)
                .await
                .err()
                .context("S3 enrollment deadline")?;
            assert!(error.ambiguous);
        }
        "receipt" => {
            let mut request = h.receipt_request(1, b"remote", Durability::S3)?;
            request.id = CommandId::new(ClientId::new([189; 16])?, 1)?;
            let mut i = h
                .dispatcher
                .lookup_receipt(h.cell.clone(), request, bounds, None)?;
            i.begin().await?;
            assert!(i
                .execute_remote(&remote.router.transport, MemberId::new(1)?)
                .await
                .is_err());
        }
        _ => anyhow::bail!("deadline kind"),
    }
    Ok(())
}

pub(crate) async fn s3_remote_deadlines(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    for kind in ["command", "enrollment", "receipt"] {
        for cancel in [false, true] {
            let store = Arc::new(object_store::prefix::PrefixStore::new(
                store.clone(),
                ObjectPath::from(format!("{kind}-{cancel}")),
            ));
            let mut h = Harness::new(rooted, store, 1).await?;
            if kind == "enrollment" {
                h.cell.cell = CellId::new("deadline-creation")?;
            }
            let remote = Remote::start(&h, h.dispatcher.clone()).await?;
            h.remote.outage.store(true, Ordering::Release);
            let mut pending = Box::pin(deadline_attempt(&h, &remote, kind));
            match futures_util::future::select(pending.as_mut(), Box::pin(h.waiting())).await {
                futures_util::future::Either::Left((result, _)) => {
                    anyhow::bail!("{kind} completed before S3 wait: {result:?}")
                }
                futures_util::future::Either::Right((result, _)) => result?,
            }
            if cancel {
                drop(pending);
            } else {
                pending.await?;
            }
            refunded(&h).await?;
            assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
            let next = if kind == "command" { 2 } else { 1 };
            let successor = h
                .command(
                    next,
                    b"successor",
                    Durability::Quorum,
                    Duration::from_secs(15),
                )
                .await?
                .response()
                .await?;
            drop(successor);
            h.remote.outage.store(false, Ordering::Release);
            match kind {
                "command" => {
                    let r = command(&h, &remote, 1, b"deadline", Durability::S3).await?;
                    assert_eq!(r.result(), b"deadline");
                    assert_eq!(r.durability(), Durability::S3);
                    drop(r);
                    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
                }
                "enrollment" => {
                    let r = enrollment(&h, &remote, Durability::S3).await?;
                    assert_eq!(r.durability(), Durability::S3);
                    drop(r);
                    assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
                    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
                }
                "receipt" => {
                    let r = receipt(&h, &remote, false, Durability::S3).await?;
                    assert_eq!(r.result(), None);
                    assert_eq!(r.durability(), Durability::S3);
                    drop(r);
                    assert_eq!(h.initializations.load(Ordering::SeqCst), 0);
                    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
                }
                _ => unreachable!(),
            }
            refunded(&h).await?;
            remote.close().await?;
            h.close().await?;
        }
    }
    Ok(())
}
