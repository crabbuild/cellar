// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable extent identities for the incremental image owner. These are not
//! full-file SHA-256 digests or Raft/ownership proofs. Until the generation and
//! execution contracts switch together, this API is exposed only for verification.

use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const EXTENT_BYTES: u64 = 4 * 1024 * 1024;
const DEPTH: u8 = 12;
const MAGIC: &[u8; 8] = b"CLEXT001";
const HEADER_BYTES: usize = 112;
const MAX_EXTENTS: u32 = crate::stream_snapshot::MAX_IMAGE_BYTES.div_ceil(EXTENT_BYTES) as u32;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RootError {
    #[error("invalid extent image geometry")]
    Geometry,
    #[error("extent image exceeds the logical quota")]
    Limit,
    #[error("invalid image history or LTX position")]
    Binding,
    #[error("extent updates do not cover the changed image boundary")]
    Coverage,
    #[error("extent updates are not unique and ordered")]
    Order,
    #[error("extent bytes do not match their content identity")]
    ExtentDigest,
    #[error("invalid or noncanonical extent manifest")]
    Encoding,
    #[error("extent manifest differs from the authoritative image root")]
    RootDigest,
}

/// Content identity of one nonempty extent. Its length is included in the hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtentId {
    digest: [u8; 32],
    length: u32,
}

impl ExtentId {
    pub fn of(bytes: &[u8]) -> Result<Self, RootError> {
        if bytes.is_empty() || bytes.len() as u64 > EXTENT_BYTES {
            return Err(RootError::Geometry);
        }
        let length = bytes.len() as u32;
        Ok(Self {
            digest: hash(b"extent", &[&length.to_be_bytes(), bytes]),
            length,
        })
    }

    pub fn bytes(self) -> [u8; 32] {
        self.digest
    }

    pub fn length(self) -> u32 {
        self.length
    }

    pub fn verify(self, bytes: &[u8]) -> Result<(), RootError> {
        if Self::of(bytes)? == self {
            Ok(())
        } else {
            Err(RootError::ExtentDigest)
        }
    }
}

/// A domain-separated extent-tree commitment, never a full-file SHA-256.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageRoot([u8; 32]);

impl ImageRoot {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Geometry {
    page_size: u32,
    length: u64,
}

impl Geometry {
    pub fn new(page_size: u32, length: u64) -> Result<Self, RootError> {
        if length > crate::stream_snapshot::MAX_IMAGE_BYTES
            || length.div_ceil(EXTENT_BYTES) > (1 << DEPTH)
        {
            return Err(RootError::Limit);
        }
        if !(length == 0 && page_size == 0
            || length != 0
                && crate::ltx::is_valid_page_size(page_size)
                && length.is_multiple_of(u64::from(page_size)))
        {
            return Err(RootError::Geometry);
        }
        Ok(Self { page_size, length })
    }

    pub fn page_size(self) -> u32 {
        self.page_size
    }

    pub fn length(self) -> u64 {
        self.length
    }

    pub fn extent_count(self) -> u32 {
        self.length.div_ceil(EXTENT_BYTES) as u32
    }

    pub fn extent_length(self, index: u32) -> Option<u32> {
        let offset = u64::from(index) * EXTENT_BYTES;
        self.length
            .checked_sub(offset)
            .filter(|length| *length != 0)
            .map(|length| length.min(EXTENT_BYTES) as u32)
    }
}

/// The host supplies canonical history/schema identities. LTX position is bound
/// here; the authenticated Raft record separately binds its own exact position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageBinding {
    pub history: [u8; 32],
    pub schema: [u8; 32],
    pub epoch: u64,
    pub txid: u64,
    pub checksum: u64,
}

impl ImageBinding {
    fn validate(self, geometry: Geometry) -> Result<(), RootError> {
        if self.epoch == 0
            || self.checksum != 0 && self.checksum & crate::CHECKSUM_FLAG == 0
            || (self.txid == 0) != (geometry.length == 0)
            || self.txid == 0 && self.checksum != 0
        {
            return Err(RootError::Binding);
        }
        Ok(())
    }

    fn encode(self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.history);
        output.extend_from_slice(&self.schema);
        output.extend_from_slice(&self.epoch.to_be_bytes());
        output.extend_from_slice(&self.txid.to_be_bytes());
        output.extend_from_slice(&self.checksum.to_be_bytes());
    }
}

enum Node {
    Empty([u8; 32]),
    Leaf {
        id: ExtentId,
        hash: [u8; 32],
    },
    Branch {
        left: Arc<Node>,
        right: Arc<Node>,
        hash: [u8; 32],
    },
}

impl Node {
    fn hash(&self) -> &[u8; 32] {
        match self {
            Self::Empty(hash) | Self::Leaf { hash, .. } | Self::Branch { hash, .. } => hash,
        }
    }
}

fn hash(kind: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar.extent-image.v1\0");
    hash.update((kind.len() as u32).to_be_bytes());
    hash.update(kind);
    for field in fields {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    hash.finalize().into()
}

fn empty_nodes() -> Vec<Arc<Node>> {
    let mut nodes = vec![Arc::new(Node::Empty(hash(b"empty-leaf", &[])))];
    for height in 1..=DEPTH {
        let child = nodes[usize::from(height) - 1].hash();
        nodes.push(Arc::new(Node::Empty(hash(
            b"branch",
            &[&[height], child, child],
        ))));
    }
    nodes
}

fn branch(height: u8, left: Arc<Node>, right: Arc<Node>) -> Arc<Node> {
    let hash = hash(b"branch", &[&[height], left.hash(), right.hash()]);
    Arc::new(Node::Branch { left, right, hash })
}

fn replace(
    node: &Arc<Node>,
    height: u8,
    index: u32,
    leaf: Arc<Node>,
    empty: &[Arc<Node>],
) -> Arc<Node> {
    if height == 0 {
        return leaf;
    }
    let (left, right) = match node.as_ref() {
        Node::Branch { left, right, .. } => (left, right),
        _ => (
            &empty[usize::from(height) - 1],
            &empty[usize::from(height) - 1],
        ),
    };
    if index & (1 << (height - 1)) == 0 {
        branch(
            height,
            replace(left, height - 1, index, leaf, empty),
            right.clone(),
        )
    } else {
        branch(
            height,
            left.clone(),
            replace(right, height - 1, index, leaf, empty),
        )
    }
}

fn truncate(
    node: &Arc<Node>,
    height: u8,
    start: u32,
    count: u32,
    empty: &[Arc<Node>],
) -> Arc<Node> {
    if start >= count {
        return empty[usize::from(height)].clone();
    }
    if start + (1 << height) <= count {
        return node.clone();
    }
    match node.as_ref() {
        Node::Branch { left, right, .. } => branch(
            height,
            truncate(left, height - 1, start, count, empty),
            truncate(right, height - 1, start + (1 << (height - 1)), count, empty),
        ),
        _ => node.clone(),
    }
}

/// A bounded manifest with a persistent tree. Small updates clone only paths to
/// changed leaves; unmodified immutable subtrees remain shared with the parent.
/// Extent data must still be verified against its ID before it can be read.
#[derive(Clone)]
pub struct ExtentManifest {
    binding: ImageBinding,
    geometry: Geometry,
    tree: Arc<Node>,
}

impl ExtentManifest {
    pub fn new(
        binding: ImageBinding,
        geometry: Geometry,
        extents: &[ExtentId],
    ) -> Result<Self, RootError> {
        binding.validate(geometry)?;
        if extents.len() != geometry.extent_count() as usize {
            return Err(RootError::Coverage);
        }
        let empty = empty_nodes();
        let mut tree = empty[usize::from(DEPTH)].clone();
        for (index, id) in extents.iter().enumerate() {
            let index = index as u32;
            let length = geometry.extent_length(index).ok_or(RootError::Coverage)?;
            if id.length != length {
                return Err(RootError::Geometry);
            }
            let leaf = Arc::new(Node::Leaf {
                id: *id,
                hash: hash(b"leaf", &[&length.to_be_bytes(), &id.digest]),
            });
            tree = replace(&tree, DEPTH, index, leaf, &empty);
        }
        Ok(Self {
            binding,
            geometry,
            tree,
        })
    }

    pub fn binding(&self) -> ImageBinding {
        self.binding
    }

    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    pub fn extent(&self, index: u32) -> Option<ExtentId> {
        if index >= self.geometry.extent_count() {
            return None;
        }
        let mut node = &self.tree;
        for height in (1..=DEPTH).rev() {
            node = match node.as_ref() {
                Node::Branch { left, right, .. } => {
                    if index & (1 << (height - 1)) == 0 {
                        left
                    } else {
                        right
                    }
                }
                _ => return None,
            };
        }
        match node.as_ref() {
            Node::Leaf { id, .. } => Some(*id),
            _ => None,
        }
    }

    pub fn root(&self) -> ImageRoot {
        let mut fields = Vec::with_capacity(100);
        self.binding.encode(&mut fields);
        fields.extend_from_slice(&self.geometry.page_size.to_be_bytes());
        fields.extend_from_slice(&self.geometry.length.to_be_bytes());
        ImageRoot(hash(b"root", &[&fields, self.tree.hash()]))
    }

    pub fn with_updates(
        &self,
        binding: ImageBinding,
        geometry: Geometry,
        updates: &[(u32, ExtentId)],
    ) -> Result<Self, RootError> {
        binding.validate(geometry)?;
        if updates.len() > MAX_EXTENTS as usize {
            return Err(RootError::Limit);
        }
        for (position, (index, id)) in updates.iter().enumerate() {
            if *index >= geometry.extent_count() {
                return Err(RootError::Coverage);
            }
            if position > 0 && updates[position - 1].0 >= *index {
                return Err(RootError::Order);
            }
            if Some(id.length) != geometry.extent_length(*index) {
                return Err(RootError::Geometry);
            }
        }
        let supplied = |index| updates.binary_search_by_key(&index, |(i, _)| *i).is_ok();
        for index in self.geometry.extent_count()..geometry.extent_count() {
            if !supplied(index) {
                return Err(RootError::Coverage);
            }
        }
        if let Some(boundary) = self
            .geometry
            .extent_count()
            .min(geometry.extent_count())
            .checked_sub(1)
        {
            if self.geometry.extent_length(boundary) != geometry.extent_length(boundary)
                && !supplied(boundary)
            {
                return Err(RootError::Coverage);
            }
        }
        // A page-size change reinterprets all extents and requires a new image.
        if self.geometry.length != 0
            && geometry.length != 0
            && self.geometry.page_size != geometry.page_size
        {
            return Err(RootError::Geometry);
        }
        let empty = empty_nodes();
        let mut tree = truncate(&self.tree, DEPTH, 0, geometry.extent_count(), &empty);
        for (index, id) in updates {
            let length = geometry.extent_length(*index).ok_or(RootError::Coverage)?;
            let leaf = Arc::new(Node::Leaf {
                id: *id,
                hash: hash(b"leaf", &[&length.to_be_bytes(), &id.digest]),
            });
            tree = replace(&tree, DEPTH, *index, leaf, &empty);
        }
        Ok(Self {
            binding,
            geometry,
            tree,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, RootError> {
        let mut output =
            Vec::with_capacity(HEADER_BYTES + self.geometry.extent_count() as usize * 32);
        output.extend_from_slice(MAGIC);
        self.binding.encode(&mut output);
        output.extend_from_slice(&self.geometry.page_size.to_be_bytes());
        output.extend_from_slice(&self.geometry.length.to_be_bytes());
        output.extend_from_slice(&self.geometry.extent_count().to_be_bytes());
        for index in 0..self.geometry.extent_count() {
            let id = self.extent(index).ok_or(RootError::Coverage)?;
            output.extend_from_slice(&id.digest);
        }
        Ok(output)
    }

    pub fn decode(bytes: &[u8], expected: ImageRoot) -> Result<Self, RootError> {
        if bytes.len() < HEADER_BYTES
            || bytes.len() > HEADER_BYTES + MAX_EXTENTS as usize * 32
            || &bytes[..8] != MAGIC
        {
            return Err(RootError::Encoding);
        }
        // All fixed slices are protected by the minimum header length above.
        let array = |start: usize| -> [u8; 32] { bytes[start..start + 32].try_into().unwrap() };
        let u64_at = |start: usize| u64::from_be_bytes(bytes[start..start + 8].try_into().unwrap());
        let u32_at = |start: usize| u32::from_be_bytes(bytes[start..start + 4].try_into().unwrap());
        let binding = ImageBinding {
            history: array(8),
            schema: array(40),
            epoch: u64_at(72),
            txid: u64_at(80),
            checksum: u64_at(88),
        };
        let geometry = Geometry::new(u32_at(96), u64_at(100))?;
        let count = u32_at(108);
        if count != geometry.extent_count() || bytes.len() != HEADER_BYTES + count as usize * 32 {
            return Err(RootError::Encoding);
        }
        let ids = bytes[HEADER_BYTES..]
            .as_chunks::<32>()
            .0
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                Ok(ExtentId {
                    digest: *bytes,
                    length: geometry
                        .extent_length(index as u32)
                        .ok_or(RootError::Coverage)?,
                })
            })
            .collect::<Result<Vec<_>, RootError>>()?;
        let manifest = Self::new(binding, geometry, &ids)?;
        if manifest.root() != expected {
            return Err(RootError::RootDigest);
        }
        Ok(manifest)
    }
}
