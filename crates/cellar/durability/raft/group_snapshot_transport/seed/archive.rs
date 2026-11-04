// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The archive supplies bytes; the authenticated source and existing engine
//! grant still supply every installation, boot and membership fence.
use super::*;
use crate::raft::cell_groups::{
    ArchiveExecution, ArchiveSeedDescriptor, CellGroupArchiveError, CellGroupArchiveRegistration,
    PreparedCellGroupCheckpoint,
};

impl Exchange {
    pub(super) async fn receive_archive_checkpoint(
        &mut self,
        applier: CellGroupApplier,
        descriptor: ArchiveSeedDescriptor,
        registration: CellGroupArchiveRegistration,
    ) -> Result<PreparedCellGroupCheckpoint, CellGroupSeedError> {
        let state = self.state.clone();
        let log = state.runtime.log();
        self.grant.check_log(log)?;
        if registration.birth.route() != self.grant.root.route() {
            return Err(TransportError::Identity.into());
        }
        let wire = snapshots::Wire {
            auth: &state.auth,
            exporter: &self.exporter,
            options: &state.options,
            max_body: CHUNK_BYTES,
        };
        let ready = wire.encode(READY_MAGIC, READY, &self.binding, &Ack { sequence: 0 }, &[])?;
        state
            .seed_process_io(&self.grant.process, self.grant.deadline, async {
                write_head(&mut self.stream, &ready).await?;
                self.stream.flush().await?;
                Ok(())
            })
            .await?;
        let execution = ArchiveExecution::new(state.domain.clone());
        let data = async {
            let registry = self
                .directory
                .open_group_archive(&execution, &registration.io, &registration.birth)
                .await?;
            let upload = registry
                .open_upload(
                    &execution,
                    &registration.owners,
                    &descriptor.ticket,
                    &registration.tails,
                )
                .await?;
            let receiver = work::receiver(
                &state,
                applier,
                self.pool.clone(),
                self.admission.clone(),
                self.grant.clone(),
            )?;
            let input = receiver.input;
            let download = async {
                let tx = input.clone();
                let grant = self.grant.clone();
                let local = log.clone();
                upload
                    .download_seed_checkpoint(&execution, &descriptor.checkpoint, move |chunk| {
                        let tx = tx.clone();
                        let grant = grant.clone();
                        let local = local.clone();
                        async move {
                            grant
                                .check_log(&local)
                                .map_err(CellGroupCheckpointError::from)?;
                            tx.send(workers::Input::Chunk(chunk)).await.map_err(|_| {
                                CellGroupArchiveError::Invalid("seed receiver closed")
                            })?;
                            grant
                                .check_log(&local)
                                .map_err(CellGroupCheckpointError::from)?;
                            Ok(())
                        }
                    })
                    .await?;
                self.grant.check_log(log)?;
                input
                    .send(workers::Input::End)
                    .await
                    .map_err(|_| TransportError::Closed)?;
                drop(input);
                Ok::<_, CellGroupSeedError>(())
            };
            let physical = async {
                receiver
                    .result
                    .await
                    .map_err(|_| TransportError::Closed)?
                    .map_err(CellGroupSeedError::Transport)
            };
            let (_, prepared) = futures_util::future::try_join(download, physical).await?;
            if prepared.snapshot() != self.grant.snapshot() {
                return Err(TransportError::Authentication.into());
            }
            Ok::<_, CellGroupSeedError>(prepared)
        };
        let result = state
            .seed_process_until(&self.grant.process, self.grant.deadline, async {
                Ok::<_, TransportError>(data.await)
            })
            .await;
        // Timeout/cancellation cannot advertise physical release while archive
        // metadata/receiver work still owns pins or a blocking task.
        execution.join().await;
        let prepared = result??;
        self.grant.check_log(log)?;
        let ready = wire.encode(READY_MAGIC, READY, &self.binding, &Ack { sequence: 1 }, &[])?;
        state
            .seed_process_io(&self.grant.process, self.grant.deadline, async {
                write_head(&mut self.stream, &ready).await?;
                self.stream.flush().await?;
                Ok(())
            })
            .await?;
        // There is no peer-byte fallback. Only the source's authenticated End
        // after its final live-authority check can authorize the engine fence.
        let head = state
            .seed_process_until(
                &self.grant.process,
                self.grant.deadline,
                snapshots::read_bulk_head(&mut self.stream, None, CHUNK_MAGIC, 0, &state.options),
            )
            .await?;
        let packet: Packet = wire.decode(CHUNK_MAGIC, CHUNK, &head, Some(&self.binding))?;
        if packet.sequence != 0
            || packet.part.is_some()
            || packet.offset != 0
            || packet.length != 0
            || packet.digest != digest(&[])
            || head.payload_len != 0
        {
            return Err(TransportError::Authentication.into());
        }
        self.grant.check_log(log)?;
        Ok(prepared)
    }
}
