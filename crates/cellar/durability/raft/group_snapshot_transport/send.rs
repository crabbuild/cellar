// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;

impl RaftTransport {
    pub(in crate::raft::transport) fn submit_group_checkpoint(
        &self,
        history: &GroupHistory,
        message: Message,
    ) -> Result<Delivery, TransportError> {
        let state = &self.handle.0;
        let (endpoint, checkpoints) = state.checkpoint_endpoint(history)?;
        let group = state
            .runtime
            .shared()?
            .log()
            .cell_group(history)
            .map_err(|_| TransportError::UnknownGroup)?;
        let local = group.identity();
        let peer = endpoint
            .route
            .peers
            .iter()
            .find(|p| p.replica.member.get() == message.to)
            .filter(|p| p.replica.node != local.node)
            .cloned()
            .ok_or(TransportError::Identity)?;
        shared::permitted(&group, &[local, &peer.replica])?;
        let route = Route::new_group(
            local,
            state.auth.source(),
            &peer.replica,
            &peer.label,
            endpoint.route.schema_digest,
        )?;
        if message.compute_size() as usize > MESSAGE_BYTES {
            return Err(TransportError::Limit);
        }
        let body = message
            .write_to_bytes()
            .map_err(|_| TransportError::Protocol)?;
        codec::message(&body, &route)?;
        let pool = state
            .registry
            .lock()
            .map_err(|_| TransportError::Closed)?
            .pools
            .get(&peer.replica.node)
            .cloned()
            .ok_or(TransportError::StalePeer)?;
        let charge = wire_charge(&state.options);
        let admission = Arc::new(Admission {
            _node: state
                .outgoing
                .lane(Lane::Data)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?,
            _peer: pool
                .outgoing
                .lane(Lane::Data)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?,
            _transfer: state
                .snapshot_source_budget
                .acquire(snapshots::memory_bound(&state.options))
                .ok_or(TransportError::Overloaded)?,
        });
        let guard = state.reserve_task()?;
        let mut chunks = workers::source(
            state,
            endpoint.clone(),
            checkpoints,
            message.get_snapshot().clone(),
            admission.clone(),
        )?;
        let (result, wait) = oneshot::channel();
        let state = state.clone();
        state.clone().spawn_reserved(guard, async move {
            let _admission = admission;
            let outcome = state
                .send_group_checkpoint(&endpoint, &peer, &pool, route, body, &mut chunks)
                .await;
            // Closing the channel wakes a blocked physical producer before any
            // runtime hint can await a busy consensus owner.
            drop(chunks);
            let delivered = outcome.is_ok();
            if !delivered {
                state.failed.fetch_add(1, Ordering::Relaxed);
            }
            let _ = result.send(outcome);
            let hint = SendRoute::Physical {
                route: endpoint.route.clone(),
                peer: peer.clone(),
            };
            // Failure feedback is required for Raft to leave Snapshot state.
            // Retain this admitted transfer through reserved hint admission;
            // only transport shutdown may cancel it, not an I/O reply timeout.
            let mut stop = state.stop.subscribe();
            if !*stop.borrow() {
                crate::asyncrt::select! {
                    _ = stop.changed() => (),
                    _ = state.runtime.delivery_hint(&hint, peer.replica.member.get(), delivered, true) => (),
                }
            }
        })?;
        Ok(Delivery { result: wait })
    }
}

impl State {
    async fn send_group_checkpoint(
        &self,
        endpoint: &Arc<Endpoint>,
        peer: &RaftPeer<GroupHistory>,
        pool: &Arc<Pool>,
        route: Route,
        body: Vec<u8>,
        chunks: &mut mpsc::Receiver<workers::Output>,
    ) -> Result<(), TransportError> {
        let deadline = self
            .domain
            .mono_ms()
            .map_err(|_| TransportError::Closed)?
            .saturating_add(self.options.snapshot_timeout_ms);
        let _connection = self.outgoing_connections.acquire(Lane::Data);
        let mut stream = self
            .snapshot_io(pool, deadline, async {
                let tcp = TcpStream::connect(peer.data_address).await?;
                tcp.set_nodelay(true)?;
                let tcp = self.observed_socket(tcp, metrics::Lane::Data);
                let name = rustls::pki_types::ServerName::try_from(peer.server_name.clone())
                    .map_err(|_| TransportError::Identity)?;
                tokio_rustls::TlsConnector::from(self.tls.client.clone())
                    .connect(name, tcp)
                    .await
                    .map_err(|e| TransportError::Tls(e.to_string()))
            })
            .await?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], frame::EXPORT_LABEL, None)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let wire = snapshots::Wire {
            auth: &self.auth,
            exporter: &exporter,
            options: &self.options,
            max_body: MESSAGE_BYTES,
        };
        if !endpoint.active.load(Ordering::Acquire) || !self.current_physical(&endpoint.route, peer)
        {
            return Err(TransportError::StalePeer);
        }
        let start = wire.encode(
            START_MAGIC,
            START,
            &[],
            &Start {
                route,
                digest: digest(&body),
            },
            &body,
        )?;
        let binding = start.tag;
        self.snapshot_io(pool, deadline, async {
            write_head(&mut stream, &start).await?;
            stream.write_all(&body).await?;
            stream.flush().await?;
            Ok(())
        })
        .await?;
        drop(body);
        let mut sequence = 0_u64;
        loop {
            if !endpoint.active.load(Ordering::Acquire)
                || !self.current_physical(&endpoint.route, peer)
            {
                return Err(TransportError::StalePeer);
            }
            let next = self
                .snapshot_io(pool, deadline, async {
                    chunks.recv().await.ok_or(TransportError::Closed)
                })
                .await?;
            let (part, offset, length, bytes) = match next {
                workers::Output::Chunk(chunk) => {
                    (Some(chunk.part), chunk.offset, chunk.length, chunk.bytes)
                }
                workers::Output::End(result) => {
                    result?;
                    (None, 0, 0, Vec::new())
                }
            };
            if bytes.len() > CHUNK_BYTES.min(self.options.snapshot_chunk_bytes) {
                return Err(TransportError::Limit);
            }
            let ending = part.is_none();
            let header = Packet {
                sequence,
                part,
                offset,
                length,
                digest: digest(&bytes),
            };
            let head = wire.encode(DATA_MAGIC, DATA, &binding, &header, &bytes)?;
            self.snapshot_io(pool, deadline, async {
                write_head(&mut stream, &head).await?;
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                Ok(())
            })
            .await?;
            if ending {
                break;
            }
            sequence = sequence.checked_add(1).ok_or(TransportError::Limit)?;
        }
        let head = self
            .snapshot_io(
                pool,
                deadline,
                snapshots::read_bulk_head(&mut stream, None, ACK_MAGIC, 0, &self.options),
            )
            .await?;
        let ack: Ack = wire.decode(ACK_MAGIC, ACK, &head, Some(&binding))?;
        if ack.sequence != sequence {
            return Err(TransportError::Replay);
        }
        if !endpoint.active.load(Ordering::Acquire) || !self.current_physical(&endpoint.route, peer)
        {
            return Err(TransportError::StalePeer);
        }
        Ok(())
    }
}
