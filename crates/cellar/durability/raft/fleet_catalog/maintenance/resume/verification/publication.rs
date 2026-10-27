// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Only live owned walks publish consent or terminal capacity, never descriptors.
use super::*;

impl FleetRestartResumeVerification {
    async fn finish_live(&mut self, deadline: &Deadline<'_>) -> Result<[u8; 32]> {
        self.next_history(deadline.remaining()?).await?;
        if self.pending.is_some()
            || !self.exhausted
            || self.book.count != self.book.basis.histories
            || self.book.covered != self.inventory.histories().len() as u64
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let physical = physical::Context {
            catalog: &self.catalog,
            snapshot: &self.snapshot,
            request: &self.request,
            observer: &self.observer,
            cut: &self.cut,
            basis: &self.book.basis,
            local: self.local.as_ref(),
        }
        .observe(deadline)
        .await?;
        if physical.mode != self.mode || physical.inventory.digest() != self.book.physical {
            return Err(FleetCatalogError::Conflict);
        }
        self.oldest = Some(
            self.oldest
                .map_or(physical.sampled, |old| old.min(physical.sampled)),
        );
        self.check_live()?;
        self.catalog
            .run(deadline.remaining()?, async {
                self.book.basis.check_current(&self.catalog).await?;
                let bytes = encode(92, &self.book, 4096)?;
                let digest = hash(&bytes);
                self.catalog
                    .inner
                    .create_exact(
                        &format!("restart-resume-candidates/{}", hex::encode(digest)),
                        &bytes,
                        4096,
                    )
                    .await?;
                self.book.basis.check_current(&self.catalog).await?;
                Ok(digest)
            })
            .await
    }

    fn check_live(&self) -> Result<()> {
        let oldest = self.oldest.ok_or(FleetCatalogError::Unconfirmed)?;
        proof::fresh(&self.catalog, oldest, oldest)
    }

    pub fn admit_resume(
        mut self,
        request: &FleetRestartResumeRequest,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<FleetRestartResumeAdmission>> {
        let request = request.clone();
        Box::pin(async move {
            request.validate()?;
            if !self.request.same_scope(&request) {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let catalog = self.catalog.clone();
            let deadline = Deadline::new(&catalog, timeout)?;
            let prior = catalog
                .run(deadline.remaining()?, async {
                    let current = catalog.inner.current().await?;
                    if let Some(record) = catalog
                        .resume_intent_candidate(&current, &request.identity)
                        .await?
                    {
                        if !record.request.same_scope(&request) {
                            return Err(FleetCatalogError::PayloadConflict);
                        }
                        if let Some(applied) = catalog
                            .resume_applied(&current, &record.mutation()?)
                            .await?
                        {
                            return Ok(Some(FleetRestartResumeAdmission {
                                record,
                                applied,
                                observed: current.revision(),
                            }));
                        }
                    }
                    Ok(None)
                })
                .await?;
            if let Some(prior) = prior {
                return Ok(prior);
            }
            let candidate = self.finish_live(&deadline).await?;
            catalog
                .run(deadline.remaining()?, async {
                    self.book.basis.check_current(&catalog).await?;
                    self.check_live()?;
                    let current = catalog.inner.current().await?;
                    if current.revision() != request.identity.expected_revision {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let record = Intent::new(
                        &request,
                        &self.cut,
                        &self.book,
                        hash(&encode(HEAD_KIND, &current.head, ROOT_LIMIT)?),
                        candidate,
                    )?;
                    if let Some(bytes) = catalog
                        .inner
                        .lookup(
                            current.head.tables[FleetTable::Operations.slot()],
                            &Intent::key(request.identity.operation),
                        )
                        .await?
                    {
                        let old: Intent = decode(records::INTENT_KIND, &bytes, 4096)?;
                        let canonical = catalog
                            .current_resume_intent(&current, request.identity.operation)
                            .await?;
                        if old != canonical
                            || !old.request.same_scope(&request)
                            || old.effect_selector()? != record.effect_selector()?
                        {
                            return Err(FleetCatalogError::PayloadConflict);
                        }
                    }
                    let committed = catalog
                        .commit_with_disruption(
                            &current,
                            &record.mutation()?,
                            None,
                            Authority::RestartResume,
                            || {
                                self.check_live()?;
                                deadline.remaining()?;
                                Ok(())
                            },
                        )
                        .await?;
                    Ok(FleetRestartResumeAdmission {
                        record,
                        applied: committed.applied_revision,
                        observed: committed.snapshot.revision(),
                    })
                })
                .await
        })
    }

    pub fn complete_resume(
        mut self,
        request: &FleetRestartResumeCompleteRequest,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<FleetRestartResumeCompletion>> {
        let request = request.clone();
        Box::pin(async move {
            validate_request(&request.identity)?;
            validate_request(&request.resume)?;
            let catalog = self.catalog.clone();
            let deadline = Deadline::new(&catalog, timeout)?;
            let prior = catalog
                .run(deadline.remaining()?, async {
                    let current = catalog.inner.current().await?;
                    if let Some(record) = catalog
                        .resume_completion_candidate(&current, &request)
                        .await?
                    {
                        if let Some(applied) = catalog
                            .resume_applied(&current, &record.mutation(&catalog)?)
                            .await?
                        {
                            return Ok(Some(FleetRestartResumeCompletion {
                                record,
                                applied,
                                observed: current.revision(),
                            }));
                        }
                    }
                    Ok(None)
                })
                .await?;
            if let Some(prior) = prior {
                return Ok(prior);
            }
            if self.mode != ResumeMode::Resumed
                || request.resume != self.request.identity
                || request.identity.operation != self.book.basis.operation
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let candidate = self.finish_live(&deadline).await?;
            catalog
                .run(deadline.remaining()?, async {
                    self.book.basis.check_current(&catalog).await?;
                    self.check_live()?;
                    let current = catalog.inner.current().await?;
                    if current.revision() != request.identity.expected_revision {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let intent = catalog
                        .current_resume_intent(&current, request.identity.operation)
                        .await?;
                    if intent.request.identity != request.resume
                        || !intent.request.same_scope(&self.request)
                        || intent.inputs != self.book.basis.digest()?
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    if catalog
                        .inner
                        .lookup(
                            current.head.tables[FleetTable::Operations.slot()],
                            &Completion::key(request.identity.operation),
                        )
                        .await?
                        .is_some()
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    let mut node = self.cut.node.clone();
                    node.resume_restart(request.identity.operation, self.cut.quiescence.epoch)?;
                    let record = Completion {
                        fact: records::Completed {
                            format: 1,
                            request: request.clone(),
                            input_root: hash(&encode(HEAD_KIND, &current.head, ROOT_LIMIT)?),
                            intent: intent.digest()?,
                            returned: self.cut.digest,
                            candidate,
                            slot: self.cut.slot,
                            node: hash(&encode(
                                super::super::super::super::nodes::NODE_KIND,
                                &node,
                                4096,
                            )?),
                            attestation: intent.request.attestation.clone(),
                            retained: self.book.retained,
                            replaced: self.book.replaced,
                        },
                        node,
                    };
                    record.validate(&catalog, &intent, &self.cut)?;
                    let committed = catalog
                        .commit_with_disruption(
                            &current,
                            &record.mutation(&catalog)?,
                            Some(self.cut.slot),
                            Authority::RestartResume,
                            || {
                                self.check_live()?;
                                deadline.remaining()?;
                                Ok(())
                            },
                        )
                        .await?;
                    Ok(FleetRestartResumeCompletion {
                        record,
                        applied: committed.applied_revision,
                        observed: committed.snapshot.revision(),
                    })
                })
                .await
        })
    }

    pub(in crate::raft::fleet_catalog::maintenance::resume) fn effect_check<'a>(
        &'a mut self,
        deadline: &'a Deadline<'_>,
    ) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            self.walk_all(deadline).await?;
            self.finish_live(deadline).await?;
            self.check_live()?;
            let sample = self.oldest.ok_or(FleetCatalogError::Unconfirmed)?;
            proof::budget(&self.catalog, sample, sample)
        })
    }
}
