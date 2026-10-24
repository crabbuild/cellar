//! Bounded persistent hash-radix pages shared by coordination inventories.
//! Storage owners retain their own errors, key spaces, deadlines and CAS roots.
use super::catalog::CatalogError;
use super::catalog_wire as wire;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

const PAGE_KIND: u8 = 41;

type Result<T, E> = std::result::Result<T, E>;

pub(crate) trait PageStore: Sync {
    type Error: From<CatalogError> + Send;

    fn page_limit(&self) -> usize;

    fn read_page(&self, id: [u8; 32]) -> BoxFuture<'_, Result<Bytes, Self::Error>>;

    fn create_page(&self, id: [u8; 32], bytes: Vec<u8>) -> BoxFuture<'_, Result<(), Self::Error>>;
}

pub(crate) struct Index<'a, S> {
    store: &'a S,
}

impl<'a, S> Index<'a, S> {
    pub(crate) fn new(store: &'a S) -> Self {
        Self { store }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct IndexCursor {
    pub(crate) index: Option<[u8; 32]>,
    hash: [u8; 32],
    key: String,
}

impl IndexCursor {
    /// Restore a rooted controller's exact continuation. A caller-supplied
    /// cursor is only a read position; owners must root progress themselves.
    pub(crate) fn after(index: Option<[u8; 32]>, key: String) -> Result<Self, CatalogError> {
        validate_key(&key)?;
        Ok(Self {
            index,
            hash: hash(key.as_bytes()),
            key,
        })
    }
}

pub struct IndexPage {
    pub entries: Vec<IndexEntry>,
    pub next: Option<IndexCursor>,
}

pub(crate) fn validate_key(key: &str) -> Result<(), CatalogError> {
    if key.is_empty() || key.len() > 256 || key.chars().any(char::is_control) {
        return Err(CatalogError::Invalid("index key"));
    }
    Ok(())
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    wire::digest(bytes)
}

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

impl<S: PageStore> Index<'_, S> {
    async fn load(
        &self,
        id: [u8; 32],
        depth: usize,
        path: &[u8; DEPTH_LIMIT],
    ) -> Result<Page, S::Error> {
        if depth > DEPTH_LIMIT {
            return Err(CatalogError::Limit.into());
        }
        let bytes = self.store.read_page(id).await?;
        if hash(&bytes) != id {
            return Err(CatalogError::Corrupt("index hash").into());
        }
        let page =
            wire::decode(PAGE_KIND, &bytes, self.store.page_limit()).map_err(S::Error::from)?;
        match &page {
            Page::Leaf {
                depth: actual,
                entries,
            } => {
                if *actual != depth || entries.is_empty() || entries.len() > LEAF_LIMIT {
                    return Err(CatalogError::Corrupt("leaf shape").into());
                }
                for entry in entries {
                    validate_key(&entry.key).map_err(S::Error::from)?;
                    if entry.value.len() > 4096 {
                        return Err(CatalogError::Limit.into());
                    }
                    let hash = hash(entry.key.as_bytes());
                    if (0..depth).any(|at| digit(&hash, at) != path[at]) {
                        return Err(CatalogError::Corrupt("index path").into());
                    }
                }
                if entries
                    .windows(2)
                    .any(|pair| !ordering(&pair[0].key, &pair[1].key).is_lt())
                {
                    return Err(CatalogError::Corrupt("leaf ordering").into());
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
                    return Err(CatalogError::Corrupt("branch shape").into());
                }
            }
        }
        Ok(page)
    }

    async fn save(&self, page: Page) -> Result<[u8; 32], S::Error> {
        let bytes =
            wire::encode(PAGE_KIND, &page, self.store.page_limit()).map_err(S::Error::from)?;
        let id = hash(&bytes);
        self.store.create_page(id, bytes).await?;
        Ok(id)
    }

    pub(crate) async fn lookup(
        &self,
        root: Option<[u8; 32]>,
        key: &str,
    ) -> Result<Option<Vec<u8>>, S::Error> {
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

    pub(crate) fn update<'a>(
        &'a self,
        root: Option<[u8; 32]>,
        key: &'a str,
        value: Option<Vec<u8>>,
        depth: usize,
    ) -> BoxFuture<'a, Result<Option<[u8; 32]>, S::Error>> {
        Box::pin(async move {
            let key_hash = hash(key.as_bytes());
            let mut path = [0; DEPTH_LIMIT];
            if depth > DEPTH_LIMIT {
                return Err(CatalogError::Limit.into());
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

    fn leaf(
        &self,
        entries: Vec<Entry>,
        depth: usize,
    ) -> BoxFuture<'_, Result<Option<[u8; 32]>, S::Error>> {
        Box::pin(async move {
            if entries.is_empty() {
                return Ok(None);
            }
            let page = Page::Leaf { depth, entries };
            match wire::encode(PAGE_KIND, &page, self.store.page_limit()) {
                Ok(_) if matches!(&page, Page::Leaf { entries, .. } if entries.len() <= LEAF_LIMIT) =>
                {
                    return Ok(Some(self.save(page).await?));
                }
                Err(CatalogError::Limit) | Ok(_) => {}
                Err(error) => return Err(error.into()),
            }
            if depth >= DEPTH_LIMIT {
                return Err(CatalogError::Limit.into());
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

    pub(crate) async fn scan(
        &self,
        root: Option<[u8; 32]>,
        after: Option<&IndexCursor>,
        limit: usize,
    ) -> Result<IndexPage, S::Error> {
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
                        let cursor = IndexCursor {
                            index: root,
                            hash: entry_hash,
                            key: entry.key.clone(),
                        };
                        entries.push(IndexEntry {
                            key: entry.key,
                            value: entry.value,
                        });
                        if entries.len() == limit {
                            return Ok(IndexPage {
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
        Ok(IndexPage {
            entries,
            next: None,
        })
    }
}
