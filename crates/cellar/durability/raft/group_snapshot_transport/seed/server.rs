// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;

impl State {
    pub(in crate::raft::transport) async fn receive_group_seed<
        S: AsyncRead + AsyncWrite + Unpin,
    >(
        self: &Arc<Self>,
        stream: &mut S,
        exporter: &[u8; 32],
        prefix: [u8; frame::PREFIX],
    ) -> Result<(), TransportError> {
        let _connection = self
            .snapshot_connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportError::Overloaded)?;
        let _metadata = self
            .seed_metadata
            .acquire(seeds::META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let wire = snapshots::Wire {
            auth: &self.auth,
            exporter,
            options: &self.options,
            max_body: META_BYTES,
        };
        let head = self
            .bounded(
                None,
                snapshots::read_bulk_head(stream, Some(prefix), REQUEST_MAGIC, 0, &self.options),
            )
            .await?;
        let request: Request = wire.decode(REQUEST_MAGIC, REQUEST, &head, None)?;
        if frame::decimal(&request.nonce)? == 0
            || request.timeout_ms == 0
            || request.timeout_ms > 3_600_000
        {
            return Err(TransportError::Protocol);
        }
        let peer = self.authorize_physical(&request.route)?;
        let (target, source) = request.route.decode_group()?;
        let (endpoint, pool) = self.checkpoint_endpoint(&source.scope)?;
        let deadline = self
            .domain
            .mono_ms()
            .map_err(|_| TransportError::Closed)?
            .saturating_add(request.timeout_ms.min(self.options.snapshot_timeout_ms));
        let charge = wire_charge(&self.options);
        let admission = Arc::new(Admission {
            _node: self
                .outgoing
                .lane(Lane::Data)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?,
            _peer: peer
                .outgoing
                .lane(Lane::Data)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?,
            _transfer: self
                .snapshot_source_budget
                .acquire(snapshots::memory_bound(&self.options))
                .ok_or(TransportError::Overloaded)?,
        });
        let authority = self
            .seed_until(deadline, async {
                Ok(self
                    .runtime
                    .shared()?
                    .seed_authority(target.clone(), deadline)
                    .await?)
            })
            .await?;
        let barrier = authority.barrier().index;
        self.checkpoint_current(&endpoint, &request.route)?;
        let work::Source {
            metadata,
            mut chunks,
            completed,
        } = work::source(self, endpoint.clone(), authority, pool, admission.clone())?;
        let snapshot = self
            .seed_until(deadline, async {
                metadata.await.map_err(|_| TransportError::Closed)?
            })
            .await?;
        self.checkpoint_current(&endpoint, &request.route)?;
        let body = snapshot
            .write_to_bytes()
            .map_err(|_| TransportError::Protocol)?;
        if body.len() > META_BYTES {
            return Err(TransportError::Limit);
        }
        let reply = wire.encode(
            REPLY_MAGIC,
            REPLY,
            &head.tag,
            &Reply {
                route: request.route.reversed(),
                request: head.tag,
                nonce: request.nonce,
                barrier: barrier.get().to_string(),
                digest: digest(&body),
            },
            &body,
        )?;
        let binding = reply.tag;
        self.snapshot_io(&peer, deadline, async {
            write_head(stream, &reply).await?;
            stream.write_all(&body).await?;
            stream.flush().await?;
            Ok(())
        })
        .await?;
        drop(body);
        // The destination may need catalog I/O and store creation before it can
        // accept bytes. Wait within the original total deadline, not one I/O tick.
        let ready = self
            .seed_until(
                deadline,
                snapshots::read_bulk_head(stream, None, READY_MAGIC, 0, &self.options),
            )
            .await?;
        let ready: Ack = wire.decode(READY_MAGIC, READY, &ready, Some(&binding))?;
        if ready.sequence != 0 {
            return Err(TransportError::Replay);
        }
        let _admission = admission;
        let mut sequence = 0_u64;
        let mut completed = Some(completed);
        loop {
            self.checkpoint_current(&endpoint, &request.route)?;
            let next = self
                .snapshot_io(&peer, deadline, async {
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
            let final_source = if ending {
                let completed = completed.take().ok_or(TransportError::Protocol)?;
                let source = self
                    .seed_until(deadline, async {
                        completed.await.map_err(|_| TransportError::Closed)
                    })
                    .await?;
                source.check()?;
                Some(source)
            } else {
                None
            };
            let packet = Packet {
                sequence,
                part,
                offset,
                length,
                digest: digest(&bytes),
            };
            let head = wire.encode(CHUNK_MAGIC, CHUNK, &binding, &packet, &bytes)?;
            self.checkpoint_current(&endpoint, &request.route)?;
            self.snapshot_io(&peer, deadline, async {
                write_head(stream, &head).await?;
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                Ok(())
            })
            .await?;
            if let Some(source) = final_source {
                source.check()?;
                return Ok(());
            }
            sequence = sequence.checked_add(1).ok_or(TransportError::Limit)?;
        }
    }
}
