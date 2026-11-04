// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! First range publication retains its exact standby cut. All configured births
//! must be admitted before any initial range map is selected.
use super::*;
use std::io::Read as _;

impl Api {
    pub(super) async fn initialize_archive(
        &self,
        history: GroupHistory,
        ordinal: u32,
    ) -> Result<()> {
        use cellar::sdk::consensus::verification::cell_groups::CellGroupArchivePool;
        let m = &self.managed;
        let path = PathBuf::from(format!("/data/initialization/archive-{ordinal}.json"));
        let check = path.clone();
        let exists = m
            .domain
            .blocking(move || -> std::io::Result<bool> {
                match std::fs::metadata(check) {
                    Ok(metadata) if metadata.is_file() => Ok(true),
                    Ok(_) => Err(std::io::Error::other("archive intent is not a file")),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                    Err(error) => Err(error),
                }
            })?
            .await??;
        let birth = m.directory.materialize_selected_history(history).await?;
        let pool = CellGroupArchivePool::new(Default::default())?;
        if exists {
            // An uncertain initializer cannot allocate another registry nonce.
            // Keep the exact intent and refuse an incomplete prior commit.
            m.directory
                .open_group_archive(&m.domain, &pool, &birth)
                .await?;
        } else {
            let prepared = m
                .directory
                .prepare_group_archive_binding(&m.domain, &pool, &birth)
                .await?;
            crate::raft_setup::persist(&m.domain, path, prepared.encode().to_vec()).await?;
            prepared.commit(&m.domain).await?;
        }
        Ok(())
    }

    pub(super) async fn select_initial_ranges(&self) -> Result<()> {
        let m = &self.managed;
        let view = m.domain.run(m.directory.observe()).await??;
        if view.groups() == 0 {
            let path = PathBuf::from("/data/initialization/ranges.json");
            let read = path.clone();
            let bytes = m
                .domain
                .blocking(move || -> Result<Option<Vec<u8>>> {
                    match std::fs::File::open(read) {
                        Ok(file) => {
                            let mut bytes = Vec::new();
                            file.take(16 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
                            ensure!(bytes.len() <= 16 * 1024 * 1024, "range intent too large");
                            Ok(Some(bytes))
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                        Err(error) => Err(error.into()),
                    }
                })?
                .await??;
            let mutation = match bytes {
                Some(bytes) => m.directory.decode_mutation(&bytes)?,
                None => {
                    let cut = m.catalog.read(TIMEOUT).await?;
                    let mut routes = Vec::new();
                    for group in &m.layout.groups {
                        let admitted = m
                            .catalog
                            .recover_birth_reservation(&cut, &m.directory, &group.history, TIMEOUT)
                            .await
                            .context(
                                "reserve every configured group before confirming initial births",
                            )?;
                        ensure!(
                            admitted.route().range == group.range,
                            "admitted range differs from layout"
                        );
                        routes.push(admitted.route().clone());
                    }
                    let mutation = m.directory.prepare_initialization_at(
                        &view,
                        ProposalId::new([121; 16])?,
                        &routes,
                    )?;
                    crate::raft_setup::persist(&m.domain, path, mutation.encode().to_vec()).await?;
                    mutation
                }
            };
            m.domain.run(m.directory.publish(&mutation)).await??;
        }
        let view = m.domain.run(m.directory.observe()).await??;
        ensure!(
            view.groups() as usize == m.layout.groups.len(),
            "selected initial population differs"
        );
        for group in &m.layout.groups {
            let birth = m
                .domain
                .run(m.directory.materialize_selected_history(group.history))
                .await??;
            ensure!(
                birth.route().range == group.range,
                "selected range differs from layout"
            );
        }
        Ok(())
    }
}
