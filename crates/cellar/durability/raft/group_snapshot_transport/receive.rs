// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::raft::cell_groups::checkpoint::wire::Chunk;

impl State {
    pub(in crate::raft::transport) async fn receive_group_checkpoint<
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
        let wire = snapshots::Wire {
            auth: &self.auth,
            exporter,
            options: &self.options,
            max_body: MESSAGE_BYTES,
        };
        let head = self
            .bounded(
                None,
                snapshots::read_bulk_head(
                    stream,
                    Some(prefix),
                    START_MAGIC,
                    MESSAGE_BYTES,
                    &self.options,
                ),
            )
            .await?;
        let start: Start = wire.decode(START_MAGIC, START, &head, None)?;
        let binding = head.tag;
        let pool = self.authorize_physical(&start.route)?;
        let (_, target) = start.route.decode_group()?;
        let (endpoint, checkpoints) = self.checkpoint_endpoint(&target.scope)?;
        let charge = wire_charge(&self.options);
        let admission = Arc::new(Admission {
            _node: self
                .incoming
                .lane(Lane::Data)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?,
            _peer: pool
                .incoming
                .lane(Lane::Data)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?,
            _transfer: self
                .snapshot_receive_budget
                .acquire(snapshots::memory_bound(&self.options))
                .ok_or(TransportError::Overloaded)?,
        });
        let deadline = self
            .domain
            .mono_ms()
            .map_err(|_| TransportError::Closed)?
            .saturating_add(self.options.snapshot_timeout_ms);
        let mut body = vec![0; head.payload_len];
        self.snapshot_io(&pool, deadline, async {
            stream.read_exact(&mut body).await?;
            Ok(())
        })
        .await?;
        if digest(&body) != start.digest {
            return Err(TransportError::Authentication);
        }
        let message = codec::message(&body, &start.route)?;
        drop(body);
        self.checkpoint_current(&endpoint, &start.route)?;
        let receiver = workers::receiver(self, endpoint.clone(), checkpoints, admission.clone())?;
        let mut sequence = 0_u64;
        loop {
            self.checkpoint_current(&endpoint, &start.route)?;
            let head = self
                .snapshot_io(
                    &pool,
                    deadline,
                    snapshots::read_bulk_head(
                        stream,
                        None,
                        DATA_MAGIC,
                        CHUNK_BYTES.min(self.options.snapshot_chunk_bytes),
                        &self.options,
                    ),
                )
                .await?;
            let packet: Packet = wire.decode(DATA_MAGIC, DATA, &head, Some(&binding))?;
            if packet.sequence != sequence {
                return Err(TransportError::Replay);
            }
            let mut bytes = vec![0; head.payload_len];
            self.snapshot_io(&pool, deadline, async {
                stream.read_exact(&mut bytes).await?;
                Ok(())
            })
            .await?;
            if digest(&bytes) != packet.digest {
                return Err(TransportError::Authentication);
            }
            self.checkpoint_current(&endpoint, &start.route)?;
            let input = match packet.part {
                Some(part) => {
                    if bytes.is_empty() {
                        return Err(TransportError::Protocol);
                    }
                    workers::Input::Chunk(Chunk {
                        part,
                        offset: packet.offset,
                        length: packet.length,
                        bytes,
                    })
                }
                None => {
                    if !bytes.is_empty() || packet.offset != 0 || packet.length != 0 {
                        return Err(TransportError::Protocol);
                    }
                    workers::Input::End
                }
            };
            let ending = matches!(input, workers::Input::End);
            self.snapshot_io(&pool, deadline, async {
                receiver
                    .input
                    .send(input)
                    .await
                    .map_err(|_| TransportError::Closed)
            })
            .await?;
            if ending {
                break;
            }
            sequence = sequence.checked_add(1).ok_or(TransportError::Limit)?;
        }
        drop(receiver.input);
        let prepared = self
            .snapshot_io(&pool, deadline, async {
                receiver.result.await.map_err(|_| TransportError::Closed)?
            })
            .await?;
        self.checkpoint_current(&endpoint, &start.route)?;
        // The prepared closure carries physical pins and checkpoint admission
        // into the bounded runtime request and its exact persistence completion.
        self.snapshot_io(&pool, deadline, async {
            self.runtime
                .shared()?
                .step_checkpoint(target.scope, message, prepared)
                .await?;
            Ok(())
        })
        .await?;
        self.checkpoint_current(&endpoint, &start.route)?;
        self.accepted.fetch_add(1, Ordering::Relaxed);
        let ack = wire.encode(ACK_MAGIC, ACK, &binding, &Ack { sequence }, &[])?;
        self.snapshot_io(&pool, deadline, async {
            write_head(stream, &ack).await?;
            stream.flush().await?;
            Ok(())
        })
        .await
    }
}
