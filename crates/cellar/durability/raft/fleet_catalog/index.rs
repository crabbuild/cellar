//! Persistent hash-radix indexes: bounded paths, immutable pages, no bucket listing.
use super::{
    decode, encode, hash, records, FleetCatalogError, FleetCursor, FleetEntry, FleetPage, Inner,
    Result, PAGE_KIND, PAGE_LIMIT,
};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

const DEPTH_LIMIT: usize = 16;
const LEAF_LIMIT: usize = 128;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    key: String,
    value: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Child {
    digit: u8,
    page: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Page {
    Leaf { depth: usize, entries: Vec<Entry> },
    Branch { depth: usize, children: Vec<Child> },
}

fn digit(hash: &[u8; 32], depth: usize) -> u8 {
    if depth.is_multiple_of(2) {
        hash[depth / 2] >> 4
    } else {
        hash[depth / 2] & 15
    }
}

fn ordering(left: &str, right: &str) -> std::cmp::Ordering {
    hash(left.as_bytes())
        .cmp(&hash(right.as_bytes()))
        .then_with(|| left.cmp(right))
}

impl Inner {
    async fn load(&self, id: [u8; 32], depth: usize, path: &[u8; DEPTH_LIMIT]) -> Result<Page> {
        if depth > DEPTH_LIMIT {
            return Err(FleetCatalogError::Limit);
        }
        let key = self.key(&format!("objects/{}", hex::encode(id)));
        let (bytes, _) = self
            .bucket
            .get_bounded(&key, PAGE_LIMIT)
            .await
            .map_err(FleetCatalogError::Storage)?
            .ok_or(FleetCatalogError::Corrupt("missing index page"))?;
        if hash(&bytes) != id {
            return Err(FleetCatalogError::Corrupt("index hash"));
        }
        let page = decode(PAGE_KIND, &bytes, PAGE_LIMIT)?;
        match &page {
            Page::Leaf {
                depth: actual,
                entries,
            } => {
                if *actual != depth || entries.is_empty() || entries.len() > LEAF_LIMIT {
                    return Err(FleetCatalogError::Corrupt("leaf shape"));
                }
                for entry in entries {
                    records::validate_key(&entry.key)?;
                    if entry.value.len() > 4096 {
                        return Err(FleetCatalogError::Limit);
                    }
                    let hash = hash(entry.key.as_bytes());
                    if (0..depth).any(|at| digit(&hash, at) != path[at]) {
                        return Err(FleetCatalogError::Corrupt("index path"));
                    }
                }
                if entries
                    .windows(2)
                    .any(|pair| !ordering(&pair[0].key, &pair[1].key).is_lt())
                {
                    return Err(FleetCatalogError::Corrupt("leaf ordering"));
                }
            }
            Page::Branch {
                depth: actual,
                children,
            } => {
                if *actual != depth
                    || depth >= DEPTH_LIMIT
                    || children.is_empty()
                    || children.len() > 16
                    || children.iter().any(|child| child.digit > 15)
                    || children
                        .windows(2)
                        .any(|pair| pair[0].digit >= pair[1].digit)
                {
                    return Err(FleetCatalogError::Corrupt("branch shape"));
                }
            }
        }
        Ok(page)
    }

    async fn save(&self, page: Page) -> Result<[u8; 32]> {
        let bytes = encode(PAGE_KIND, &page, PAGE_LIMIT)?;
        let id = hash(&bytes);
        self.create_exact(&format!("objects/{}", hex::encode(id)), &bytes, PAGE_LIMIT)
            .await?;
        Ok(id)
    }

    pub(super) async fn lookup(
        &self,
        root: Option<[u8; 32]>,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let mut at = root;
        let mut depth = 0;
        let hash = hash(key.as_bytes());
        let mut path = [0; DEPTH_LIMIT];
        while let Some(id) = at {
            match self.load(id, depth, &path).await? {
                Page::Leaf { entries, .. } => {
                    return Ok(entries
                        .into_iter()
                        .find(|entry| entry.key == key)
                        .map(|entry| entry.value));
                }
                Page::Branch { children, .. } => {
                    let next = digit(&hash, depth);
                    at = children
                        .into_iter()
                        .find(|child| child.digit == next)
                        .map(|child| child.page);
                    path[depth] = next;
                    depth += 1;
                }
            }
        }
        Ok(None)
    }

    pub(super) fn update<'a>(
        &'a self,
        root: Option<[u8; 32]>,
        key: &'a str,
        value: Option<Vec<u8>>,
        depth: usize,
    ) -> BoxFuture<'a, Result<Option<[u8; 32]>>> {
        Box::pin(async move {
            let key_hash = hash(key.as_bytes());
            let mut path = [0; DEPTH_LIMIT];
            if depth > DEPTH_LIMIT {
                return Err(FleetCatalogError::Limit);
            }
            for (at, slot) in path.iter_mut().enumerate().take(depth) {
                *slot = digit(&key_hash, at);
            }
            let page = match root {
                Some(id) => self.load(id, depth, &path).await?,
                None => Page::Leaf {
                    depth,
                    entries: vec![],
                },
            };
            match page {
                Page::Leaf { mut entries, .. } => {
                    entries.retain(|entry| entry.key != key);
                    if let Some(value) = value {
                        entries.push(Entry {
                            key: key.into(),
                            value,
                        });
                    }
                    entries.sort_by(|left, right| ordering(&left.key, &right.key));
                    self.leaf(entries, depth).await
                }
                Page::Branch { mut children, .. } => {
                    let next = digit(&key_hash, depth);
                    let old = children
                        .iter()
                        .find(|child| child.digit == next)
                        .map(|child| child.page);
                    let updated = self.update(old, key, value, depth + 1).await?;
                    children.retain(|child| child.digit != next);
                    if let Some(page) = updated {
                        children.push(Child { digit: next, page });
                    }
                    children.sort_by_key(|child| child.digit);
                    if children.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(self.save(Page::Branch { depth, children }).await?))
                    }
                }
            }
        })
    }

    fn leaf(&self, entries: Vec<Entry>, depth: usize) -> BoxFuture<'_, Result<Option<[u8; 32]>>> {
        Box::pin(async move {
            if entries.is_empty() {
                return Ok(None);
            }
            let page = Page::Leaf { depth, entries };
            match encode(PAGE_KIND, &page, PAGE_LIMIT) {
                Ok(_) if matches!(&page, Page::Leaf { entries, .. } if entries.len() <= LEAF_LIMIT) =>
                {
                    return Ok(Some(self.save(page).await?));
                }
                Err(FleetCatalogError::Limit) | Ok(_) => {}
                Err(error) => return Err(error),
            }
            if depth >= DEPTH_LIMIT {
                return Err(FleetCatalogError::Limit);
            }
            let Page::Leaf { entries, .. } = page else {
                unreachable!()
            };
            let mut buckets: [Vec<Entry>; 16] = std::array::from_fn(|_| vec![]);
            for entry in entries {
                let slot = usize::from(digit(&hash(entry.key.as_bytes()), depth));
                buckets[slot].push(entry);
            }
            let mut children = vec![];
            for (digit, entries) in buckets.into_iter().enumerate() {
                if let Some(page) = self.leaf(entries, depth + 1).await? {
                    children.push(Child {
                        digit: digit as u8,
                        page,
                    });
                }
            }
            Ok(Some(self.save(Page::Branch { depth, children }).await?))
        })
    }

    pub(super) async fn scan(
        &self,
        root: Option<[u8; 32]>,
        after: Option<&FleetCursor>,
        limit: usize,
    ) -> Result<FleetPage> {
        let mut stack = root
            .map(|id| vec![(id, 0, [0; DEPTH_LIMIT])])
            .unwrap_or_default();
        let mut entries = Vec::new();
        while let Some((id, depth, path)) = stack.pop() {
            match self.load(id, depth, &path).await? {
                Page::Leaf { entries: leaf, .. } => {
                    for entry in leaf {
                        let entry_hash = hash(entry.key.as_bytes());
                        if after.is_some_and(|cursor| {
                            (entry_hash, &entry.key) <= (cursor.hash, &cursor.key)
                        }) {
                            continue;
                        }
                        let cursor = FleetCursor {
                            index: root,
                            hash: entry_hash,
                            key: entry.key.clone(),
                        };
                        entries.push(FleetEntry {
                            key: entry.key,
                            value: entry.value,
                        });
                        if entries.len() == limit {
                            return Ok(FleetPage {
                                entries,
                                next: Some(cursor),
                            });
                        }
                    }
                }
                Page::Branch { children, .. } => {
                    for child in children.into_iter().rev() {
                        let mut path = path;
                        path[depth] = child.digit;
                        if after.is_some_and(|cursor| {
                            (0..=depth)
                                .map(|at| path[at])
                                .cmp((0..=depth).map(|at| digit(&cursor.hash, at)))
                                .is_lt()
                        }) {
                            continue;
                        }
                        stack.push((child.page, depth + 1, path));
                    }
                }
            }
        }
        Ok(FleetPage {
            entries,
            next: None,
        })
    }
}
