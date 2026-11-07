// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::protocol::{EnvelopeLimits, ImageCommitment, LtxImage, ProtocolError};
use super::types::*;
use cellar_logic::{cell_group::*, consensus::*};
use cellar_ltx::internal::{ExtentRootReference, ImageRoot};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"CLRCGE01";
const MARKER: u32 = 1;
const PREFIX: usize = 24;
const DIGEST: usize = 32;
const DOMAIN: &[u8] = b"cellar-cell-group-entry-1/cell-history-1/range-route-1/administration-1";

impl<'a> CellGroupEntry<'a> {
    pub fn encode(
        &self,
        route: &GroupRoute,
        limits: GroupEntryLimits,
    ) -> Result<Vec<u8>, ProtocolError> {
        let payload_len = self.validate(route, limits)?;
        let mut header = Output {
            bytes: Vec::new(),
            limit: limits.envelope.max_header_bytes,
        };
        header.put(&Sha256::digest(DOMAIN))?;
        header.group(self.binding.group)?;
        header.u64(self.binding.route_epoch.get())?;
        header.put(&self.binding.birth_digest)?;
        header.put(&self.binding.schema_digest)?;
        header.put(&self.binding.proposal.bytes())?;
        match &self.command {
            GroupCommand::Mutation(update) => {
                header.tag(1)?;
                header.cell(&update.cell)?;
                header.u64(update.sequence.get())?;
                header.image(update.base)?;
                header.image(update.result)?;
                header.files(&update.files)?;
            }
            GroupCommand::Enroll { cell, image, files } => {
                header.tag(2)?;
                header.cell(cell)?;
                header.image(*image)?;
                header.files(files)?;
            }
            GroupCommand::Membership {
                previous,
                next,
                transition,
                changes,
            } => {
                header.tag(3)?;
                header.u64(previous.get())?;
                header.u64(next.get())?;
                header.tag(match transition {
                    GroupMembershipTransition::Auto => 1,
                    GroupMembershipTransition::JointImplicit => 2,
                    GroupMembershipTransition::JointExplicit => 3,
                    GroupMembershipTransition::LeaveJoint => 4,
                })?;
                header.u32(changes.len() as u32)?;
                for change in changes {
                    header.u64(change.member.get())?;
                    header.put(&change.node.bytes())?;
                    header.put(&change.disk.bytes())?;
                    header.tag(match change.role {
                        GroupMemberRole::Voter => 1,
                        GroupMemberRole::Learner => 2,
                        GroupMemberRole::Remove => 3,
                    })?;
                }
            }
            GroupCommand::Checkpoint {
                position,
                manifest,
                membership,
            } => {
                header.tag(4)?;
                header.position(*position)?;
                header.put(manifest)?;
                header.u64(membership.get())?;
            }
            GroupCommand::Seal {
                transition,
                authorization,
                children,
            } => {
                header.tag(5)?;
                header.put(&transition.bytes())?;
                header.put(authorization)?;
                header.u32(children.len() as u32)?;
                for child in children {
                    header.route(child)?;
                }
            }
            GroupCommand::Install {
                transition,
                sources,
                manifest,
            } => {
                header.tag(6)?;
                header.put(&transition.bytes())?;
                header.put(manifest)?;
                header.u32(sources.len() as u32)?;
                for source in sources {
                    header.group(source.group)?;
                    header.position(source.position)?;
                    header.put(&source.manifest)?;
                }
            }
            GroupCommand::Activate {
                transition,
                publication,
                manifest,
            } => {
                header.tag(7)?;
                header.put(&transition.bytes())?;
                header.put(publication)?;
                header.put(manifest)?;
            }
            GroupCommand::PreparePark {
                generation,
                term,
                membership,
            } => {
                header.tag(8)?;
                header.u64(generation.get())?;
                header.u64(term.get())?;
                header.u64(membership.get())?;
            }
            GroupCommand::Retire {
                transition,
                publication,
            } => {
                header.tag(9)?;
                header.put(&transition.bytes())?;
                header.put(publication)?;
            }
        }
        let size = PREFIX
            .checked_add(header.bytes.len())
            .and_then(|n| n.checked_add(payload_len))
            .and_then(|n| n.checked_add(DIGEST))
            .ok_or(ProtocolError::Limit)?;
        let mut encoded = Vec::with_capacity(size);
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&MARKER.to_be_bytes());
        encoded.extend_from_slice(&(header.bytes.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&(payload_len as u64).to_be_bytes());
        encoded.extend_from_slice(&header.bytes);
        let files = match &self.command {
            GroupCommand::Mutation(update) => update.files.as_slice(),
            GroupCommand::Enroll { files, .. } => files.as_slice(),
            _ => &[],
        };
        for file in files {
            encoded.extend_from_slice(file.bytes);
        }
        let digest = Sha256::digest(&encoded);
        encoded.extend_from_slice(&digest);
        Ok(encoded)
    }

    /// No payload bytes are copied. Bounds, domain and the expected group route
    /// are checked before decoding variable-length cell/file metadata. The
    /// caller still checks the coherent cell base and all physical LTX content.
    pub fn decode(
        bytes: &'a [u8],
        route: &GroupRoute,
        limits: GroupEntryLimits,
    ) -> Result<Self, ProtocolError> {
        limits.validate()?;
        let mut prefix = Input(bytes);
        if prefix.take(8)? != MAGIC {
            return Err(ProtocolError::Protocol);
        }
        if prefix.u32()? != MARKER {
            return Err(ProtocolError::Protocol);
        }
        let header_len = prefix.u32()? as usize;
        let payload_len = prefix.u64()?;
        if header_len > limits.envelope.max_header_bytes
            || payload_len > limits.envelope.max_payload_bytes as u64
        {
            return Err(ProtocolError::Limit);
        }
        let payload_len = usize::try_from(payload_len).map_err(|_| ProtocolError::Limit)?;
        let payload_start = PREFIX.checked_add(header_len).ok_or(ProtocolError::Limit)?;
        let end = payload_start
            .checked_add(payload_len)
            .ok_or(ProtocolError::Limit)?;
        if end.checked_add(DIGEST) != Some(bytes.len()) {
            return Err(ProtocolError::Framing);
        }
        if Sha256::digest(&bytes[..end])[..] != bytes[end..] {
            return Err(ProtocolError::Digest);
        }
        let mut input = Input(&bytes[PREFIX..payload_start]);
        let mut payload = Input(&bytes[payload_start..end]);
        if input.take(DIGEST)? != &Sha256::digest(DOMAIN)[..] {
            return Err(ProtocolError::Protocol);
        }
        let binding = GroupEntryBinding {
            group: input.group()?,
            route_epoch: input.counter(RouteEpoch::new)?,
            birth_digest: input.array()?,
            schema_digest: input.array()?,
            proposal: input.id(ProposalId::new)?,
        };
        binding.check(route)?;
        let command = match input.tag()? {
            1 => {
                let cell = input.cell(binding.group)?;
                check_cell(route, &cell)?;
                let sequence = input.counter(MutationSequence::new)?;
                let base = input.image()?;
                let result = input.image()?;
                let files = input.files(&mut payload, limits.envelope)?;
                GroupCommand::Mutation(GroupMutation {
                    cell,
                    sequence,
                    base,
                    result,
                    files,
                })
            }
            2 => {
                let cell = input.cell(binding.group)?;
                check_cell(route, &cell)?;
                GroupCommand::Enroll {
                    cell,
                    image: input.image()?,
                    files: input.files(&mut payload, limits.envelope)?,
                }
            }
            3 => {
                let previous = input.counter(MembershipRevision::new)?;
                let next = input.counter(MembershipRevision::new)?;
                let transition = match input.tag()? {
                    1 => GroupMembershipTransition::Auto,
                    2 => GroupMembershipTransition::JointImplicit,
                    3 => GroupMembershipTransition::JointExplicit,
                    4 => GroupMembershipTransition::LeaveJoint,
                    _ => return Err(ProtocolError::Protocol),
                };
                let count = input.count(limits.max_members, 41)?;
                let mut changes = Vec::with_capacity(count);
                for _ in 0..count {
                    let member = input.counter(MemberId::new)?;
                    let node = input.id(NodeId::new)?;
                    let disk = input.id(DiskLineage::new)?;
                    let role = match input.tag()? {
                        1 => GroupMemberRole::Voter,
                        2 => GroupMemberRole::Learner,
                        3 => GroupMemberRole::Remove,
                        _ => return Err(ProtocolError::Protocol),
                    };
                    changes.push(GroupMemberChange {
                        member,
                        node,
                        disk,
                        role,
                    });
                }
                GroupCommand::Membership {
                    previous,
                    next,
                    transition,
                    changes,
                }
            }
            4 => GroupCommand::Checkpoint {
                position: input.position()?,
                manifest: input.array()?,
                membership: input.counter(MembershipRevision::new)?,
            },
            5 => {
                let transition = input.id(TransitionId::new)?;
                let authorization = input.array()?;
                let count = input.count(limits.max_transition_groups, 138)?;
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(input.route()?);
                }
                GroupCommand::Seal {
                    transition,
                    authorization,
                    children,
                }
            }
            6 => {
                let transition = input.id(TransitionId::new)?;
                let manifest = input.array()?;
                let count = input.count(limits.max_transition_groups, 112)?;
                let mut sources = Vec::with_capacity(count);
                for _ in 0..count {
                    sources.push(GroupSeal {
                        group: input.group()?,
                        position: input.position()?,
                        manifest: input.array()?,
                    });
                }
                GroupCommand::Install {
                    transition,
                    sources,
                    manifest,
                }
            }
            7 => GroupCommand::Activate {
                transition: input.id(TransitionId::new)?,
                publication: input.array()?,
                manifest: input.array()?,
            },
            8 => GroupCommand::PreparePark {
                generation: input.counter(ParkGeneration::new)?,
                term: Term::new(input.u64()?),
                membership: input.counter(MembershipRevision::new)?,
            },
            9 => GroupCommand::Retire {
                transition: input.id(TransitionId::new)?,
                publication: input.array()?,
            },
            _ => return Err(ProtocolError::Protocol),
        };
        if !input.0.is_empty() || !payload.0.is_empty() {
            return Err(ProtocolError::Framing);
        }
        let entry = Self { binding, command };
        entry.validate(route, limits)?;
        Ok(entry)
    }
}

struct Output {
    bytes: Vec<u8>,
    limit: usize,
}

impl Output {
    fn put(&mut self, value: &[u8]) -> Result<(), ProtocolError> {
        if self
            .bytes
            .len()
            .checked_add(value.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(ProtocolError::Limit);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn tag(&mut self, value: u8) -> Result<(), ProtocolError> {
        self.put(&[value])
    }

    fn u32(&mut self, value: u32) -> Result<(), ProtocolError> {
        self.put(&value.to_be_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), ProtocolError> {
        self.put(&value.to_be_bytes())
    }

    fn group(&mut self, value: GroupHistory) -> Result<(), ProtocolError> {
        for id in [
            value.cluster.bytes(),
            value.namespace.bytes(),
            value.group.bytes(),
            value.incarnation.bytes(),
        ] {
            self.put(&id)?;
        }
        Ok(())
    }

    fn text(&mut self, value: &CellId) -> Result<(), ProtocolError> {
        self.put(&(value.as_str().len() as u16).to_be_bytes())?;
        self.put(value.as_str().as_bytes())
    }

    fn cell(&mut self, value: &CellHistory) -> Result<(), ProtocolError> {
        self.put(&value.incarnation.bytes())?;
        self.text(&value.cell)
    }

    fn image(&mut self, value: LtxImage) -> Result<(), ProtocolError> {
        for n in [value.epoch, value.txid, value.checksum] {
            self.u64(n)?;
        }
        match value.commitment {
            ImageCommitment::Sha256(digest) => {
                self.tag(1)?;
                self.put(&digest)?;
                self.u32(0)
            }
            ImageCommitment::Extents(root) => {
                self.tag(2)?;
                self.put(&root.root().bytes())?;
                self.u32(root.encoded_length())
            }
        }
    }

    fn files(&mut self, files: &[GroupLtx<'_>]) -> Result<(), ProtocolError> {
        self.u32(files.len() as u32)?;
        for file in files {
            for value in [
                file.bytes.len() as u64,
                file.min_txid,
                file.max_txid,
                file.page_count,
                file.decoded_bytes,
            ] {
                self.u64(value)?;
            }
        }
        Ok(())
    }

    fn position(&mut self, value: LogPosition) -> Result<(), ProtocolError> {
        self.u64(value.index.get())?;
        self.u64(value.term.get())
    }

    fn bound(&mut self, value: &CellBound) -> Result<(), ProtocolError> {
        match value {
            CellBound::NegativeInfinity => self.tag(0),
            CellBound::Key(key) => {
                self.tag(1)?;
                self.put(&key.digest())?;
                self.text(key.cell())
            }
            CellBound::PositiveInfinity => self.tag(2),
        }
    }

    fn route(&mut self, value: &GroupRoute) -> Result<(), ProtocolError> {
        self.group(value.history)?;
        self.u64(value.epoch.get())?;
        self.put(&value.birth_digest)?;
        self.put(&value.schema_digest)?;
        self.bound(value.range.lower())?;
        self.bound(value.range.upper())
    }
}

struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let (value, rest) = self.0.split_at_checked(n).ok_or(ProtocolError::Framing)?;
        self.0 = rest;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ProtocolError> {
        self.take(N)?.try_into().map_err(|_| ProtocolError::Framing)
    }

    fn tag(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn id<T>(
        &mut self,
        make: impl FnOnce([u8; 16]) -> Result<T, Refusal>,
    ) -> Result<T, ProtocolError> {
        make(self.array()?).map_err(|_| ProtocolError::Identity)
    }

    fn counter<T>(
        &mut self,
        make: impl FnOnce(u64) -> Result<T, Refusal>,
    ) -> Result<T, ProtocolError> {
        make(self.u64()?).map_err(|_| ProtocolError::Identity)
    }

    fn count(&mut self, maximum: usize, minimum_bytes: usize) -> Result<usize, ProtocolError> {
        let count = self.u32()? as usize;
        if count > maximum {
            return Err(ProtocolError::Limit);
        }
        if count
            .checked_mul(minimum_bytes)
            .is_none_or(|n| n > self.0.len())
        {
            return Err(ProtocolError::Framing);
        }
        Ok(count)
    }

    fn group(&mut self) -> Result<GroupHistory, ProtocolError> {
        Ok(GroupHistory {
            cluster: self.id(ClusterId::new)?,
            namespace: self.id(NamespaceId::new)?,
            group: self.id(GroupId::new)?,
            incarnation: self.id(IncarnationId::new)?,
        })
    }

    fn text(&mut self) -> Result<CellId, ProtocolError> {
        let n = u16::from_be_bytes(self.array()?) as usize;
        if n > MAX_CELL_ID_BYTES {
            return Err(ProtocolError::Limit);
        }
        CellId::new(std::str::from_utf8(self.take(n)?).map_err(|_| ProtocolError::Identity)?)
            .map_err(|_| ProtocolError::Identity)
    }

    fn cell(&mut self, group: GroupHistory) -> Result<CellHistory, ProtocolError> {
        let incarnation = self.id(IncarnationId::new)?;
        Ok(CellHistory {
            cluster: group.cluster,
            namespace: group.namespace,
            incarnation,
            cell: self.text()?,
        })
    }

    fn image(&mut self) -> Result<LtxImage, ProtocolError> {
        let epoch = self.u64()?;
        let txid = self.u64()?;
        let checksum = self.u64()?;
        let tag = self.tag()?;
        let digest = self.array()?;
        let length = self.u32()?;
        let commitment = match (tag, length) {
            (1, 0) => ImageCommitment::Sha256(digest),
            (2, _) => ImageCommitment::Extents(
                ExtentRootReference::new(ImageRoot::from_bytes(digest), length)
                    .map_err(|_| ProtocolError::Identity)?,
            ),
            _ => return Err(ProtocolError::Protocol),
        };
        let image = LtxImage {
            epoch,
            txid,
            checksum,
            commitment,
        };
        LtxImage::try_from(CellImage::from(image))?;
        Ok(image)
    }

    fn files(
        &mut self,
        payload: &mut Input<'a>,
        limits: EnvelopeLimits,
    ) -> Result<Vec<GroupLtx<'a>>, ProtocolError> {
        let count = self.count(limits.max_files, 40)?;
        let mut files = Vec::with_capacity(count);
        let (mut decoded, mut pages) = (0_u64, 0_u64);
        for _ in 0..count {
            let length = self.u64()?;
            if length == 0 || length > limits.max_payload_bytes as u64 {
                return Err(ProtocolError::Limit);
            }
            let min_txid = self.u64()?;
            let max_txid = self.u64()?;
            let page_count = self.u64()?;
            let decoded_bytes = self.u64()?;
            pages = pages.checked_add(page_count).ok_or(ProtocolError::Limit)?;
            decoded = decoded
                .checked_add(decoded_bytes)
                .ok_or(ProtocolError::Limit)?;
            if pages > limits.max_pages || decoded > limits.max_decoded_bytes {
                return Err(ProtocolError::Limit);
            }
            let length = usize::try_from(length).map_err(|_| ProtocolError::Limit)?;
            files.push(GroupLtx {
                min_txid,
                max_txid,
                page_count,
                decoded_bytes,
                bytes: payload.take(length)?,
            });
        }
        Ok(files)
    }

    fn position(&mut self) -> Result<LogPosition, ProtocolError> {
        Ok(LogPosition {
            index: LogIndex::new(self.u64()?),
            term: Term::new(self.u64()?),
        })
    }

    fn bound(&mut self) -> Result<CellBound, ProtocolError> {
        match self.tag()? {
            0 => Ok(CellBound::NegativeInfinity),
            1 => {
                let digest = self.array()?;
                Ok(CellBound::Key(CellKey::from_digest(digest, self.text()?)))
            }
            2 => Ok(CellBound::PositiveInfinity),
            _ => Err(ProtocolError::Protocol),
        }
    }

    fn route(&mut self) -> Result<GroupRoute, ProtocolError> {
        let history = self.group()?;
        let epoch = self.counter(RouteEpoch::new)?;
        let birth_digest = self.array()?;
        let schema_digest = self.array()?;
        let range =
            CellRange::new(self.bound()?, self.bound()?).map_err(|_| ProtocolError::Identity)?;
        Ok(GroupRoute {
            history,
            epoch,
            birth_digest,
            schema_digest,
            range,
        })
    }
}
