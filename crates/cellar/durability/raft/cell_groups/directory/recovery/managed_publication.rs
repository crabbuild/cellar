// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Atomically select all recovered ranges while retaining their startup fence.
use super::*;
use crate::raft::catalog::CellGroupBirthRequest;

pub(crate) struct ManagedRecoveryPublicationInput<'a> {
    pub(crate) intent: [u8; 32],
    pub(crate) transition: ProposalId,
    pub(crate) barrier: [u8; 32],
    pub(crate) plan: [u8; 32],
    pub(crate) seeds: [u8; 32],
    pub(crate) mutation: &'a DirectoryMutation,
    pub(crate) births: &'a [CellGroupBirthRequest],
}

pub(in crate::raft::catalog::range_directory) struct ManagedRecoveryPublication<'a> {
    input: &'a ManagedRecoveryPublicationInput<'a>,
}

impl<'a> ManagedRecoveryPublication<'a> {
    pub(super) fn new(input: &'a ManagedRecoveryPublicationInput<'a>) -> Self {
        Self { input }
    }

    pub(in crate::raft::catalog::range_directory) fn check(
        &self,
        view: &DirectoryView,
        mutation: &DirectoryMutation,
    ) -> Result<(), CatalogError> {
        let input = self.input;
        if mutation.encode() != input.mutation.encode()
            || mutation.request.expected != Some(input.barrier)
            || view.digest != input.barrier
            || !view.head.recovery.as_ref().is_some_and(|lock| {
                lock.request == input.intent
                    && matches!(&lock.phase, RecoveryPhase::ManagedSource { transition, .. }
                        if *transition == input.transition.bytes())
            })
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    pub(in crate::raft::catalog::range_directory) async fn check_child(
        &self,
        directory: &RangeDirectory,
        route: &GroupRoute,
    ) -> Result<(), CatalogError> {
        let birth = self
            .input
            .births
            .iter()
            .find(|birth| birth.route() == route)
            .ok_or(CatalogError::Fenced)?;
        directory
            .require_managed_recovery_identity(birth, self.input.plan)
            .await
    }

    pub(in crate::raft::catalog::range_directory) fn published_lock(
        &self,
        view: &DirectoryView,
        publication: [u8; 32],
    ) -> Result<Box<RecoveryLock>, CatalogError> {
        let lock = view.head.recovery.as_ref().ok_or(CatalogError::Fenced)?;
        let RecoveryPhase::ManagedSource {
            transition,
            plan,
            groups,
        } = &lock.phase
        else {
            return Err(CatalogError::Fenced);
        };
        let mut groups = groups.clone();
        groups.extend(self.input.births.iter().map(|birth| {
            let history = birth.route().history;
            (history.group.bytes(), history.incarnation.bytes())
        }));
        groups.sort_unstable();
        Ok(Box::new(RecoveryLock {
            request: lock.request,
            base: lock.base,
            source: lock.source.clone(),
            phase: RecoveryPhase::ManagedPublished {
                transition: *transition,
                plan: *plan,
                groups,
                replacement: self.input.plan,
                seeds: self.input.seeds,
                publication,
            },
        }))
    }

    pub(in crate::raft::catalog::range_directory) async fn check_published(
        &self,
        directory: &RangeDirectory,
        view: &DirectoryView,
        publication: &DirectoryPublication,
    ) -> Result<(), CatalogError> {
        let input = self.input;
        if !view.head.recovery.as_ref().is_some_and(|lock| {
            lock.request == input.intent
                && matches!(&lock.phase, RecoveryPhase::ManagedPublished {
                    transition, replacement, seeds, publication: published, ..
                } if *transition == input.transition.bytes()
                    && *replacement == input.plan && *seeds == input.seeds
                    && *published == publication.journal_digest())
        }) {
            return Err(CatalogError::Fenced);
        }
        for birth in input.births {
            directory.selected_route_in(view, birth.route()).await?;
        }
        Ok(())
    }
}

impl RangeDirectory {
    pub(crate) async fn observe_managed_recovery_publication(
        &self,
        input: &ManagedRecoveryPublicationInput<'_>,
    ) -> Result<Option<DirectoryPublication>, CatalogError> {
        self.bounded(async {
            let (view, _) = self.current().await?;
            let publication = self.confirmed(&view.head, input.mutation).await?;
            if let Some(publication) = &publication {
                ManagedRecoveryPublication::new(input)
                    .check_published(self, &view, publication)
                    .await?;
            }
            Ok(publication)
        })
        .await
    }

    /// Record the exact selected births before Fleet promotes their histories.
    /// The recovery lock remains the exclusive startup and routing authority.
    pub(crate) async fn materialize_managed_recovery_births(
        &self,
        input: &ManagedRecoveryPublicationInput<'_>,
        batch: std::ops::Range<usize>,
    ) -> Result<(), CatalogError> {
        self.bounded(async {
            let (view, _) = self.current().await?;
            let publication = self
                .confirmed(&view.head, input.mutation)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let permit = ManagedRecoveryPublication { input };
            permit.check_published(self, &view, &publication).await?;
            let births = input.births.get(batch).ok_or(CatalogError::Limit)?;
            for birth in births {
                self.require_managed_recovery_identity(birth, input.plan)
                    .await?;
                self.decide_live_birth(birth).await?;
            }
            let (after, _) = self.current().await?;
            permit.check_published(self, &after, &publication).await
        })
        .await
    }

    pub(crate) async fn publish_managed_recovery(
        &self,
        input: &ManagedRecoveryPublicationInput<'_>,
    ) -> Result<DirectoryPublication, CatalogError> {
        self.check_request(&input.mutation.request)?;
        if input.births.len() != input.mutation.request.children.len()
            || input
                .births
                .iter()
                .zip(&input.mutation.request.children)
                .any(|(birth, child)| {
                    !child
                        .decode(self)
                        .is_ok_and(|route| route == *birth.route())
                })
            || [input.plan, input.seeds].contains(&[0; 32])
        {
            return Err(CatalogError::Fenced);
        }
        let permit = ManagedRecoveryPublication::new(input);
        self.bounded(Box::pin(self.publish_inner(
            input.mutation,
            PublicationAuthority::ManagedRecovery(&permit),
        )))
        .await
    }
}
