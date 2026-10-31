// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical transitions shared by live committed apply and archive data replay.
//! This module supplies preparation, never a persistence ticket or serving right.
use super::*;
use crate::application::consensus::group_protocol::{CellGroupEntry, GroupCommand};
use crate::raft::member_state::GroupMembership;
use cellar_logic::cell_group::CommitWitness;
use raft::prelude::{ConfState, Entry};

impl Storage {
    pub(in crate::raft::cell_groups) fn prepare_entry(
        &mut self,
        base: &PreparedGroupManifest,
        entry: &Entry,
        configuration: Option<(&ConfState, &GroupMembership)>,
        verify_existing: bool,
    ) -> Result<PreparedGroupManifest, ExecutionError> {
        let Storage {
            tree,
            pages,
            images,
            limits,
            max_effect_rows,
        } = self;
        if entry.get_entry_type() == ::raft::prelude::EntryType::EntryConfChangeV2 {
            let (conf, membership) = configuration.ok_or(ExecutionError::Invalid(
                "configuration replay state missing",
            ))?;
            return tree
                .prepare_membership(pages, base, entry, conf, membership)
                .map_err(storage);
        }
        // Check page owner and contiguous prefix before any image allocation/I/O.
        let unchanged = tree
            .prepare_unchanged(pages, base, entry_position(entry))
            .map_err(storage)?;
        if entry.data.is_empty() {
            return Ok(unchanged);
        }
        let decoded =
            CellGroupEntry::decode(&entry.data, base.marker().route(), *limits).map_err(storage)?;
        let witness = CommitWitness {
            group: base.marker().route().history,
            position: entry_position(entry),
            proposal: decoded.binding.proposal,
        };
        let next = match decoded.command {
            GroupCommand::Enroll { cell, image, files } => {
                let decision = decide_enrollment(
                    tree.route(),
                    EnrollmentPhase::Active,
                    &entry.data,
                    base.pin_cell(&cell.cell).map(|(leaf, _)| &leaf.enrollment),
                    *limits,
                );
                match decision {
                    Ok(EnrollmentDecision::Existing(_)) | Err(EnrollmentError::Conflict) => {
                        unchanged
                    }
                    Ok(EnrollmentDecision::Propose(record)) => {
                        let prepared = images.prepare(
                            images::ImageApply {
                                cell: &cell,
                                sequence: 1,
                                schema: tree.route().schema_digest,
                                base: None,
                                image,
                                files: &files,
                                max_effect_rows: *max_effect_rows,
                            },
                            verify_existing,
                        )?;
                        let leaf = CellManifestLeaf {
                            enrollment: record,
                            version: cellar_logic::cell_group::CellVersion {
                                history: cell,
                                sequence: cellar_logic::cell_group::MutationSequence::new(1)
                                    .map_err(storage)?,
                                image: image.into(),
                            },
                            geometry: prepared.geometry,
                            last_mutation: entry_position(entry),
                            witness,
                            effects: prepared.effects,
                        };
                        tree.prepare_update(pages, base, leaf, None, prepared.pin)
                            .map_err(storage)?
                    }
                    Err(error) => return Err(storage(error)),
                }
            }
            GroupCommand::Mutation(mutation) => {
                let (old, image_base) = base
                    .pin_cell(&mutation.cell.cell)
                    .ok_or(ExecutionError::Invalid("apply cell missing"))?;
                mutation.check_base(&old.version).map_err(storage)?;
                let prepared = images.prepare(
                    images::ImageApply {
                        cell: &mutation.cell,
                        sequence: mutation.sequence.get(),
                        schema: tree.route().schema_digest,
                        base: Some(&image_base),
                        image: mutation.result,
                        files: &mutation.files,
                        max_effect_rows: *max_effect_rows,
                    },
                    verify_existing,
                )?;
                let leaf = CellManifestLeaf {
                    enrollment: old.enrollment.clone(),
                    version: cellar_logic::cell_group::CellVersion {
                        history: mutation.cell,
                        sequence: mutation.sequence,
                        image: mutation.result.into(),
                    },
                    geometry: prepared.geometry,
                    last_mutation: entry_position(entry),
                    witness,
                    effects: prepared.effects,
                };
                tree.prepare_update(pages, base, leaf, Some(&old.version), prepared.pin)
                    .map_err(storage)?
            }
            _ => {
                return Err(ExecutionError::Invalid(
                    "unsupported group apply administration",
                ))
            }
        };
        Ok(next)
    }
}
