// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::raft::cell_groups::checkpoint::wire::Chunk;

impl Exchange {
    pub(super) async fn install(
        mut self,
        applier: CellGroupApplier,
    ) -> Result<Installed, CellGroupSeedError> {
        let state = &self.state;
        let log = state.runtime.log();
        self.grant.check_log(log)?;
        if applier.history() != self.group.identity().scope
            || !applier.belongs_to_engine(&log.owner())
        {
            return Err(TransportError::Identity.into());
        }
        let receiver = work::receiver(
            state,
            applier,
            self.pool.clone(),
            self.admission.clone(),
            self.grant.clone(),
        )?;
        let wire = snapshots::Wire {
            auth: &state.auth,
            exporter: &self.exporter,
            options: &state.options,
            max_body: CHUNK_BYTES,
        };
        let mut result = receiver.result;
        let receiving = async {
            let head = wire.encode(READY_MAGIC, READY, &self.binding, &Ack { sequence: 0 }, &[])?;
            state
                .seed_process_io(&self.grant.process, self.grant.deadline, async {
                    write_head(&mut self.stream, &head).await?;
                    self.stream.flush().await?;
                    Ok(())
                })
                .await?;
            let mut sequence = 0_u64;
            loop {
                self.grant.check_log(log)?;
                let head = state
                    .seed_process_io(
                        &self.grant.process,
                        self.grant.deadline,
                        snapshots::read_bulk_head(
                            &mut self.stream,
                            None,
                            CHUNK_MAGIC,
                            CHUNK_BYTES.min(state.options.snapshot_chunk_bytes),
                            &state.options,
                        ),
                    )
                    .await?;
                let packet: Packet = wire.decode(CHUNK_MAGIC, CHUNK, &head, Some(&self.binding))?;
                if packet.sequence != sequence {
                    return Err(TransportError::Replay.into());
                }
                let mut bytes = vec![0; head.payload_len];
                state
                    .seed_process_io(&self.grant.process, self.grant.deadline, async {
                        self.stream.read_exact(&mut bytes).await?;
                        Ok(())
                    })
                    .await?;
                if digest(&bytes) != packet.digest {
                    return Err(TransportError::Authentication.into());
                }
                let input = match packet.part {
                    Some(part) => {
                        if bytes.is_empty() {
                            return Err(TransportError::Protocol.into());
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
                            return Err(TransportError::Protocol.into());
                        }
                        workers::Input::End
                    }
                };
                let ending = matches!(input, workers::Input::End);
                self.grant.check_log(log)?;
                state
                    .seed_process_until(&self.grant.process, self.grant.deadline, async {
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
            Ok::<(), CellGroupSeedError>(())
        };
        // A physical verifier can reject a part while the peer keeps the next
        // header/body open. Observe it throughout network work rather than
        // retaining the connection and admission until the peer deadline.
        crate::asyncrt::select_biased! { "physical seed verification failure precedes more network input";
            outcome = &mut result => {
                outcome.map_err(|_| TransportError::Closed)??;
                // Successful physical completion requires the explicit End
                // sent by the receiving branch, which completes in that poll.
                return Err(TransportError::Protocol.into());
            },
            outcome = receiving => outcome?,
        }
        drop(receiver.input);
        let prepared = state
            .seed_process_until(&self.grant.process, self.grant.deadline, async {
                result.await.map_err(|_| TransportError::Closed)?
            })
            .await?;
        self.grant.check_log(log)?;
        if prepared.snapshot() != self.grant.snapshot() {
            return Err(TransportError::Authentication.into());
        }
        let current = prepared.manifest().clone();
        let mut write = GroupWrite::new(&self.group);
        let metadata = prepared.snapshot().get_metadata();
        write.hard_state = Some(::raft::prelude::HardState {
            term: metadata.term,
            commit: metadata.index,
            ..Default::default()
        });
        write.snapshot = Some(SnapshotChange::CellGroup(prepared));
        write.seed_grant = Some(self.grant.clone());
        write.consensus_pin = Some(self.lease.clone());
        // Once accepted, retain the network task, grant and consensus lease until
        // actual persistence finishes. Caller cancellation/transport close cannot
        // turn an in-flight fence into an unowned or successful empty bootstrap.
        log.persist_cells(self.operation, vec![write]).await?;
        let permit = self
            .directory
            .bootstrap_joined_group_member(&self.birth, self.grant.local.member, log)
            .await?;
        drop(self.lease);
        Ok((permit, current))
    }
}
