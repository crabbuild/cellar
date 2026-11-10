// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded immutable publication proofs for advisory interval invalidation.
use super::*;

const MAX_PUBLICATIONS: u64 = 4;
const MAX_ROUTES: usize = 16;
const MAX_PAGES: usize = 128;

pub(super) struct RepairProof {
    pub intervals: Vec<CellRange>,
}

#[derive(PartialEq, Eq)]
enum Part {
    Tree(wire::Reference),
    Route(GroupRoute),
}

struct Pages {
    remaining: usize,
}

impl Pages {
    async fn read(
        &mut self,
        directory: &RangeDirectory,
        reference: &wire::Reference,
    ) -> Result<wire::Page, CatalogError> {
        self.remaining = self.remaining.checked_sub(1).ok_or(CatalogError::Limit)?;
        directory.page(reference).await
    }

    async fn selected(
        &mut self,
        directory: &RangeDirectory,
        root: &wire::Reference,
        expected: &GroupRoute,
    ) -> Result<(), CatalogError> {
        let mut next = Some(root.clone());
        for _ in 0..32 {
            let page = self
                .read(directory, &next.take().ok_or(CatalogError::Fenced)?)
                .await?;
            let route = page.route.decode(directory)?;
            match expected.range.lower().cmp(route.range.lower()) {
                std::cmp::Ordering::Less => next = page.left,
                std::cmp::Ordering::Greater => next = page.right,
                std::cmp::Ordering::Equal => {
                    return if route == *expected {
                        Ok(())
                    } else {
                        Err(CatalogError::Fenced)
                    };
                }
            }
        }
        Err(CatalogError::Limit)
    }

    async fn expand(
        &mut self,
        directory: &RangeDirectory,
        frontier: &mut Vec<Part>,
    ) -> Result<(), CatalogError> {
        let Some(Part::Tree(reference)) = frontier.pop() else {
            return Err(CatalogError::Corrupt("routing proof frontier"));
        };
        let page = self.read(directory, &reference).await?;
        if let Some(right) = page.right {
            frontier.push(Part::Tree(right));
        }
        frontier.push(Part::Route(page.route.decode(directory)?));
        if let Some(left) = page.left {
            frontier.push(Part::Tree(left));
        }
        Ok(())
    }

    // Compare the complete ordered maps outside the declared replacement.
    // Equal Merkle subtrees skip I/O even when AVL rotations changed the path.
    // Merely finding the claimed parents/children would not prove that some
    // unrelated range was not changed by the same purported publication.
    async fn unchanged_outside(
        &mut self,
        directory: &RangeDirectory,
        before: &wire::Reference,
        after: &wire::Reference,
        interval: &CellRange,
    ) -> Result<(), CatalogError> {
        let mut before = vec![Part::Tree(before.clone())];
        let mut after = vec![Part::Tree(after.clone())];
        loop {
            discard_inside(directory, &mut before, interval)?;
            discard_inside(directory, &mut after, interval)?;
            match (before.last(), after.last()) {
                (None, None) => return Ok(()),
                (Some(a), Some(b)) if a == b => {
                    before.pop();
                    after.pop();
                }
                (Some(Part::Tree(_)), _) => self.expand(directory, &mut before).await?,
                (_, Some(Part::Tree(_))) => self.expand(directory, &mut after).await?,
                _ => {
                    return Err(CatalogError::Corrupt(
                        "routing proof changed outside interval",
                    ))
                }
            }
        }
    }
}

fn discard_inside(
    directory: &RangeDirectory,
    frontier: &mut Vec<Part>,
    interval: &CellRange,
) -> Result<(), CatalogError> {
    while let Some(part) = frontier.last() {
        let range = match part {
            Part::Tree(reference) => reference.range(directory)?,
            Part::Route(route) => route.range.clone(),
        };
        if range.lower() >= interval.lower() && range.upper() <= interval.upper() {
            frontier.pop();
        } else if matches!(part, Part::Route(_)) && overlaps(&range, interval) {
            return Err(CatalogError::Corrupt("routing proof cuts through a range"));
        } else {
            break;
        }
    }
    Ok(())
}

pub(super) fn overlaps(a: &CellRange, b: &CellRange) -> bool {
    a.lower() < b.upper() && b.lower() < a.upper()
}

impl RangeDirectory {
    pub(super) async fn routing_repair(
        &self,
        view: &DirectoryView,
        previous_revision: u64,
        previous_journal: [u8; 32],
    ) -> Result<RepairProof, CatalogError> {
        let distance = view
            .revision()
            .checked_sub(previous_revision)
            .filter(|distance| (1..=MAX_PUBLICATIONS).contains(distance))
            .ok_or(CatalogError::Limit)?;
        let mut next = view.head.journal;
        let mut revision = view.revision();
        let mut pages = Pages {
            remaining: MAX_PAGES,
        };
        let mut intervals = Vec::with_capacity(distance as usize);
        for _ in 0..distance {
            let journal = self.journal(next).await?;
            if journal.revision != revision
                || (revision == view.revision() && journal.tree != view.head.tree)
            {
                return Err(CatalogError::Corrupt("routing proof journal revision"));
            }
            let bytes = self
                .read(&format!("changes/{}", hex::encode(journal.request)))
                .await?
                .ok_or(CatalogError::Conflict(
                    "routing publication proof unavailable",
                ))?;
            if record::digest(&bytes) != journal.request {
                return Err(CatalogError::Corrupt("routing publication proof hash"));
            }
            let request: wire::Request = record::decode(wire::REQUEST, &bytes, wire::LIMIT)?;
            drop(bytes);
            self.check_request(&request)?;
            if request.operation != journal.operation || ProposalId::new(request.operation).is_err()
            {
                return Err(CatalogError::Corrupt("routing publication proof operation"));
            }
            if request.parents.is_empty()
                || request.parents.len() + request.children.len() > MAX_ROUTES
            {
                return Err(CatalogError::Limit);
            }
            let previous_id = journal.previous.ok_or(CatalogError::Fenced)?;
            let previous = self.journal(previous_id).await?;
            if previous.revision.checked_add(1) != Some(journal.revision) {
                return Err(CatalogError::Corrupt("routing proof predecessor revision"));
            }
            let before = previous.tree.as_ref().ok_or(CatalogError::Fenced)?;
            let after = journal.tree.as_ref().ok_or(CatalogError::Fenced)?;
            for parent in &request.parents {
                pages.selected(self, before, &parent.decode(self)?).await?;
            }
            for child in &request.children {
                pages.selected(self, after, &child.decode(self)?).await?;
            }
            let interval = self.union(&request.parents)?;
            pages
                .unchanged_outside(self, before, after, &interval)
                .await?;
            intervals.push(interval);
            next = previous_id;
            revision = previous.revision;
        }
        if next != previous_journal || revision != previous_revision {
            return Err(CatalogError::Conflict(
                "routing proof does not extend cached root",
            ));
        }
        Ok(RepairProof { intervals })
    }
}
