// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;

impl RaftTransport {
    /// Authenticate a live-quorum source, then reconcile a non-serving staging
    /// mapping and permanent claim. Retry failures with a fresh exchange and the
    /// same identities; an existing remote claim never permits recreating a disk.
    pub async fn prepare_group_seed(
        &self,
        local: ReplicaIdentity<GroupHistory>,
        source: RaftPeer<GroupHistory>,
        directory: RangeDirectory,
        birth: CellGroupBirth,
        deadline: u64,
    ) -> Result<CellGroupSeedSession, CellGroupSeedError> {
        let state = self.handle.0.clone();
        let log = state.runtime.shared()?.log().clone();
        let options = log.limits();
        if local.node != options.node
            || local.disk != options.disk
            || local.process != options.process
            || local.scope != source.replica.scope
            || local.scope != birth.route().history
            || local.node == source.replica.node
            || local.member == source.replica.member
        {
            return Err(TransportError::Identity.into());
        }
        let deadline = deadline.min(self.seed_deadline()?);
        let guard = state.reserve_task()?;
        let process = state.seed_peer_epoch(&RaftPeerNode::from(&source))?;
        let metadata = state
            .seed_metadata
            .acquire(seeds::META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let admission = Arc::new(ReceiveAdmission {
            _wire: state
                .incoming
                .lane(Lane::Data)
                .acquire(wire_charge(&state.options))
                .ok_or(TransportError::Overloaded)?,
            _transfer: state
                .snapshot_receive_budget
                .acquire(snapshots::memory_bound(&state.options))
                .ok_or(TransportError::Overloaded)?,
        });
        // Refuse before remote or local mutations if the node has no receiver pool.
        let pool = self.group_checkpoint_pool()?;
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        let timeout_ms = deadline
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .ok_or(TransportError::Timeout)?;
        let nonce = state
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| TransportError::Closed)?
            .checked_add(1)
            .ok_or(TransportError::Closed)?;
        let request = Request {
            route: Route::new_group(
                &local,
                state.auth.source(),
                &source.replica,
                &source.label,
                birth.route().schema_digest,
            )?,
            nonce: nonce.to_string(),
            timeout_ms,
        };
        let connection = state.outgoing_connections.acquire(Lane::Data);
        let mut stream = state
            .seed_process_io(&process, deadline, async {
                let tcp = TcpStream::connect(source.data_address).await?;
                tcp.set_nodelay(true)?;
                let socket = state.observed_socket(tcp, metrics::Lane::Data);
                let name = rustls::pki_types::ServerName::try_from(source.server_name.clone())
                    .map_err(|_| TransportError::Identity)?;
                tokio_rustls::TlsConnector::from(state.tls.client.clone())
                    .connect(name, socket)
                    .await
                    .map_err(|e| TransportError::Tls(e.to_string()))
            })
            .await?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], EXPORT_LABEL, None)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let wire = snapshots::Wire {
            auth: &state.auth,
            exporter: &exporter,
            options: &state.options,
            max_body: META_BYTES,
        };
        let head = wire.encode(REQUEST_MAGIC, REQUEST, &[], &request, &[])?;
        let (reply, body, binding) = state
            .seed_process_until(&process, deadline, async {
                write_head(&mut stream, &head).await?;
                stream.flush().await?;
                let received = snapshots::read_bulk_head(
                    &mut stream,
                    None,
                    REPLY_MAGIC,
                    META_BYTES,
                    &state.options,
                )
                .await?;
                let reply: Reply = wire.decode(REPLY_MAGIC, REPLY, &received, Some(&head.tag))?;
                if reply.route != request.route.reversed()
                    || reply.request != head.tag
                    || reply.nonce != request.nonce
                {
                    return Err(TransportError::Authentication);
                }
                let mut body = vec![0; received.payload_len];
                stream.read_exact(&mut body).await?;
                if reply.digest != digest(&body) {
                    return Err(TransportError::Authentication);
                }
                Ok((reply, body, received.tag))
            })
            .await?;
        let barrier = LogIndex::new(frame::decimal(&reply.barrier)?);
        let snapshot = ::raft::prelude::Snapshot::parse_from_bytes(&body)
            .map_err(|_| TransportError::Protocol)?;
        if snapshot
            .write_to_bytes()
            .map_err(|_| TransportError::Protocol)?
            != body
        {
            return Err(TransportError::Protocol.into());
        }
        let root = checkpoint::seed_marker(&snapshot, &local, barrier)?;
        let grant = Arc::new(Grant {
            local,
            root,
            snapshot,
            deadline,
            owner: log.owner(),
            domain: state.domain.clone(),
            process,
            stop: state.stop.subscribe(),
            _metadata: metadata,
            _request: Arc::new(guard),
        });
        let group = state
            .domain
            .run(directory.stage_group_learner(&birth, &log, grant.clone()))
            .await
            .map_err(|_| TransportError::Closed)??;
        group
            .consensus_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| RuntimeError::AlreadyAttached)
            .map_err(TransportError::from)?;
        let lease = Arc::new(Lease(group.consensus_claimed.clone()));
        let (command, ready) = oneshot::channel();
        let (response, result) = oneshot::channel();
        let exchange = Exchange {
            state: state.clone(),
            stream,
            exporter,
            binding,
            grant,
            group: group.clone(),
            lease,
            directory,
            birth,
            pool,
            admission,
            _connection: connection,
            operation: OperationId::new(nonce),
        };
        let guard = state.reserve_task()?;
        state.clone().spawn_reserved(guard, async move {
            let outcome = async {
                let applier = state
                    .seed_process_until(&exchange.grant.process, deadline, async {
                        ready.await.map_err(|_| TransportError::Closed)
                    })
                    .await?;
                exchange.install(applier).await
            }
            .await;
            let _ = response.send(outcome);
        })?;
        Ok(CellGroupSeedSession {
            group,
            command,
            result,
        })
    }
}
