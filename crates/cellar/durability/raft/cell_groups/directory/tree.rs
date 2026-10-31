// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent ordered AVL pages. Intermediate edits may have gaps; only a
//! complete all-domain tree can enter a published namespace root.

use super::{wire::*, *};
use futures_util::future::BoxFuture;

impl RangeDirectory {
    pub(super) async fn page(&self, reference: &Reference) -> Result<Page, CatalogError> {
        reference.range(self)?;
        let bytes = self.range_bytes(reference.digest).await?;
        let page: Page = record::decode(PAGE, &bytes, LIMIT)?;
        if page.reference(&bytes, self)? != *reference {
            return Err(CatalogError::Corrupt("range page reference"));
        }
        Ok(page)
    }

    async fn save(&self, page: Page) -> Result<Reference, CatalogError> {
        let bytes = record::encode(PAGE, &page, LIMIT)?;
        let reference = page.reference(&bytes, self)?;
        self.immutable("ranges", reference.digest, &bytes).await?;
        Ok(reference)
    }

    pub(super) async fn lookup_tree(
        &self,
        root: &Reference,
        cell: &CellId,
    ) -> Result<GroupRoute, CatalogError> {
        root.require_complete(self)?;
        let key = cell_routing_key(self.namespace.config.namespace, cell);
        let mut next = Some(root.clone());
        for _ in 0..32 {
            let page = self
                .page(
                    &next
                        .take()
                        .ok_or(CatalogError::Corrupt("range coverage hole"))?,
                )
                .await?;
            let route = page.route.decode(self)?;
            if route.range.contains(&key) {
                return Ok(route);
            }
            next = if CellBound::Key(key.clone()) < *route.range.lower() {
                page.left
            } else {
                page.right
            };
        }
        Err(CatalogError::Limit)
    }

    pub(super) fn insert<'a>(
        &'a self,
        root: Option<Reference>,
        route: Route,
        depth: u8,
    ) -> BoxFuture<'a, Result<Reference, CatalogError>> {
        Box::pin(async move {
            if depth >= 32 {
                return Err(CatalogError::Limit);
            }
            let Some(reference) = root else {
                return self
                    .save(Page {
                        namespace: self.namespace.root_digest,
                        route,
                        left: None,
                        right: None,
                    })
                    .await;
            };
            let mut page = self.page(&reference).await?;
            match route
                .decode(self)?
                .range
                .lower()
                .cmp(page.route.decode(self)?.range.lower())
            {
                std::cmp::Ordering::Less => {
                    page.left = Some(self.insert(page.left.take(), route, depth + 1).await?)
                }
                std::cmp::Ordering::Greater => {
                    page.right = Some(self.insert(page.right.take(), route, depth + 1).await?)
                }
                std::cmp::Ordering::Equal => {
                    return Err(CatalogError::Conflict("duplicate range lower bound"))
                }
            }
            self.balance(page).await
        })
    }

    pub(super) fn remove<'a>(
        &'a self,
        root: Option<Reference>,
        expected: Route,
        depth: u8,
    ) -> BoxFuture<'a, Result<Option<Reference>, CatalogError>> {
        Box::pin(async move {
            if depth >= 32 {
                return Err(CatalogError::Limit);
            }
            let reference = root.ok_or(CatalogError::Conflict("missing parent range"))?;
            let mut page = self.page(&reference).await?;
            match expected
                .decode(self)?
                .range
                .lower()
                .cmp(page.route.decode(self)?.range.lower())
            {
                std::cmp::Ordering::Less => {
                    page.left = self.remove(page.left.take(), expected, depth + 1).await?
                }
                std::cmp::Ordering::Greater => {
                    page.right = self.remove(page.right.take(), expected, depth + 1).await?
                }
                std::cmp::Ordering::Equal => {
                    if page.route != expected {
                        return Err(CatalogError::Conflict("stale parent range"));
                    }
                    match (&page.left, &page.right) {
                        (None, _) => return Ok(page.right),
                        (_, None) => return Ok(page.left),
                        (Some(_), Some(right)) => {
                            let mut successor = self.page(right).await?;
                            for _ in depth..32 {
                                match &successor.left {
                                    None => break,
                                    Some(left) => successor = self.page(left).await?,
                                }
                            }
                            if successor.left.is_some() {
                                return Err(CatalogError::Limit);
                            }
                            page.route = successor.route;
                            page.right = self
                                .remove(page.right.take(), page.route.clone(), depth + 1)
                                .await?;
                        }
                    }
                }
            }
            Ok(Some(self.balance(page).await?))
        })
    }

    async fn balance(&self, mut page: Page) -> Result<Reference, CatalogError> {
        let height = |r: &Option<Reference>| r.as_ref().map_or(0, |r| i16::from(r.height));
        let balance = height(&page.left) - height(&page.right);
        if balance > 1 {
            let mut left = self
                .page(
                    page.left
                        .as_ref()
                        .ok_or(CatalogError::Corrupt("range rotation"))?,
                )
                .await?;
            if height(&left.left) < height(&left.right) {
                let mut middle = self
                    .page(
                        left.right
                            .as_ref()
                            .ok_or(CatalogError::Corrupt("range rotation"))?,
                    )
                    .await?;
                left.right = middle.left.take();
                middle.left = Some(self.save(left).await?);
                page.left = middle.right.take();
                middle.right = Some(self.save(page).await?);
                return self.save(middle).await;
            }
            page.left = left.right.take();
            left.right = Some(self.save(page).await?);
            return self.save(left).await;
        }
        if balance < -1 {
            let mut right = self
                .page(
                    page.right
                        .as_ref()
                        .ok_or(CatalogError::Corrupt("range rotation"))?,
                )
                .await?;
            if height(&right.right) < height(&right.left) {
                let mut middle = self
                    .page(
                        right
                            .left
                            .as_ref()
                            .ok_or(CatalogError::Corrupt("range rotation"))?,
                    )
                    .await?;
                right.left = middle.right.take();
                middle.right = Some(self.save(right).await?);
                page.right = middle.left.take();
                middle.left = Some(self.save(page).await?);
                return self.save(middle).await;
            }
            page.right = right.left.take();
            right.left = Some(self.save(page).await?);
            return self.save(right).await;
        }
        self.save(page).await
    }
}
