// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded physical-range enumeration at one immutable directory observation.
//! A complete traversal is metadata inventory, never serving or release proof.
use super::*;

/// Continuation for one exact observed tree. It cannot be rebased onto a later
/// root; restart an independent inventory after a topology change.
#[derive(Clone, Debug)]
pub struct CellGroupDirectoryCursor {
    root: [u8; 32],
    after: CellBound,
}

pub struct CellGroupDirectoryPage {
    pub routes: Vec<GroupRoute>,
    pub next: Option<CellGroupDirectoryCursor>,
}

impl RangeDirectory {
    /// Enumerate physical groups without scanning cells or listing S3 objects.
    /// Each call retains at most `limit` routes and one tree path. The cursor
    /// binds the immutable view, including its namespace and topology revision.
    pub async fn group_page(
        &self,
        view: &DirectoryView,
        after: Option<&CellGroupDirectoryCursor>,
        limit: usize,
    ) -> Result<CellGroupDirectoryPage, CatalogError> {
        self.check_view(view)?;
        if !(1..=128).contains(&limit) {
            return Err(CatalogError::Limit);
        }
        if after.is_some_and(|cursor| cursor.root != view.digest) {
            return Err(CatalogError::Conflict("directory inventory cursor"));
        }
        self.bounded(async {
            let mut lower = after.map(|cursor| cursor.after.clone());
            let mut routes = Vec::with_capacity(limit);
            for _ in 0..limit {
                let Some(route) = self.successor(view, lower.as_ref()).await? else {
                    return Ok(CellGroupDirectoryPage { routes, next: None });
                };
                lower = Some(route.range.lower().clone());
                routes.push(route);
            }
            let next = if self.successor(view, lower.as_ref()).await?.is_some() {
                Some(CellGroupDirectoryCursor {
                    root: view.digest,
                    after: lower.ok_or(CatalogError::Corrupt("empty directory inventory page"))?,
                })
            } else {
                None
            };
            Ok(CellGroupDirectoryPage { routes, next })
        })
        .await
    }

    async fn successor(
        &self,
        view: &DirectoryView,
        after: Option<&CellBound>,
    ) -> Result<Option<GroupRoute>, CatalogError> {
        let mut next = view.head.tree.clone();
        let mut candidate = None;
        for _ in 0..32 {
            let Some(reference) = next.take() else {
                return Ok(candidate);
            };
            let page = self.page(&reference).await?;
            let route = page.route.decode(self)?;
            if after.is_none_or(|lower| route.range.lower() > lower) {
                candidate = Some(route);
                next = page.left;
            } else {
                next = page.right;
            }
        }
        if next.is_none() {
            Ok(candidate)
        } else {
            Err(CatalogError::Limit)
        }
    }
}
