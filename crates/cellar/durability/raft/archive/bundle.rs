// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded immutable upload coalescing. Each cell still verifies and conditionally
//! publishes its own head; a bundle never creates a cross-cell commit proof.
use super::*;
use crate::application::consensus::protocol::WireScope;
use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, oneshot};

pub(crate) const MAX_GROUPS: usize = 2;
pub(crate) const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024;
const MAGIC: &[u8; 8] = b"CLRBDL01";
pub(crate) const COALESCE_MS: u64 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    pub digest: [u8; 32],
    pub length: u64,
    pub member: u32,
}

impl Reference {
    pub(crate) fn validate(&self) -> Result<(), ArchiveError> {
        if self.length < 12 || self.length > MAX_BYTES as u64 || self.member >= MAX_GROUPS as u32 {
            return Err(ArchiveError::Limit);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    members: Vec<Member>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    scope: WireScope,
    root: [u8; 32],
    birth: [u8; 32],
    schema: [u8; 32],
    object: ObjectRef,
    offset: usize,
}

struct Part {
    member: Member,
    bytes: Vec<u8>,
}

fn encode(mut parts: Vec<Part>) -> Result<(Vec<u8>, Vec<ObjectRef>), ArchiveError> {
    if !(2..=MAX_GROUPS).contains(&parts.len()) {
        return Err(ArchiveError::Limit);
    }
    let mut length = 0usize;
    for part in &mut parts {
        part.member.object.verify(&part.bytes)?;
        part.member.offset = length;
        length = length
            .checked_add(part.bytes.len())
            .ok_or(ArchiveError::Limit)?;
    }
    let bodies: Vec<_> = parts
        .iter_mut()
        .map(|part| std::mem::take(&mut part.bytes))
        .collect();
    let index = Index {
        members: parts.into_iter().map(|part| part.member).collect(),
    };
    let header = manifest::encode(3, &index, MAX_INDEX_BYTES)?;
    let total = length
        .checked_add(12 + header.len())
        .filter(|total| *total <= MAX_BYTES)
        .ok_or(ArchiveError::Limit)?;
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(header.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&header);
    for body in bodies {
        bytes.extend_from_slice(&body);
    }
    let digest = manifest::digest(&bytes);
    let objects = index
        .members
        .into_iter()
        .enumerate()
        .map(|(member, part)| {
            let mut object = part.object;
            object.bundle = Some(Reference {
                digest,
                length: bytes.len() as u64,
                member: member as u32,
            });
            object
        })
        .collect();
    Ok((bytes, objects))
}

pub(super) fn extract(
    archive: &CellArchive,
    expected: &ObjectRef,
    bytes: &[u8],
) -> Result<Vec<u8>, ArchiveError> {
    let reference = expected
        .bundle
        .as_ref()
        .ok_or(ArchiveError::Corrupt("missing bundle reference"))?;
    reference.validate()?;
    if reference.length != bytes.len() as u64
        || reference.digest != manifest::digest(bytes)
        || bytes.get(..8) != Some(MAGIC.as_slice())
    {
        return Err(ArchiveError::Corrupt("archive bundle digest/length"));
    }
    let header_length = u32::from_be_bytes(
        bytes
            .get(8..12)
            .ok_or(ArchiveError::Corrupt("archive bundle framing"))?
            .try_into()
            .map_err(|_| ArchiveError::Corrupt("archive bundle framing"))?,
    ) as usize;
    if header_length > MAX_INDEX_BYTES {
        return Err(ArchiveError::Limit);
    }
    let body_start = 12usize
        .checked_add(header_length)
        .ok_or(ArchiveError::Limit)?;
    let index: Index = manifest::decode(
        3,
        bytes
            .get(12..body_start)
            .ok_or(ArchiveError::Corrupt("archive bundle index length"))?,
        MAX_INDEX_BYTES,
    )?;
    if !(2..=MAX_GROUPS).contains(&index.members.len()) {
        return Err(ArchiveError::Limit);
    }
    let mut end = body_start;
    let mut previous = None;
    let mut selected = None;
    for (number, member) in index.members.iter().enumerate() {
        let scope = member
            .scope
            .clone()
            .decode()
            .map_err(|_| ArchiveError::Corrupt("archive bundle scope"))?;
        if previous.as_ref().is_some_and(|old| old >= &scope)
            || scope.cluster != archive.scope.cluster
            || scope.namespace != archive.scope.namespace
            || member.root != archive.root
            || member.schema != archive.schema
            || member.object.bundle.is_some()
            || !matches!(member.object.kind, ObjectKind::Snapshot | ObjectKind::Tail)
            || member.offset.checked_add(body_start) != Some(end)
        {
            return Err(ArchiveError::Corrupt("archive bundle index order/domain"));
        }
        previous = Some(scope.clone());
        let range = member
            .object
            .range
            .ok_or(ArchiveError::Corrupt("archive bundle object range"))?;
        range.first.decode()?;
        range.last.decode()?;
        if range.first.index > range.last.index
            || range.first.term > range.last.term
            || member.object.kind == ObjectKind::Snapshot && range.first != range.last
        {
            return Err(ArchiveError::Corrupt("archive bundle object range"));
        }
        let next = end
            .checked_add(usize::try_from(member.object.length).map_err(|_| ArchiveError::Limit)?)
            .ok_or(ArchiveError::Limit)?;
        let body = bytes
            .get(end..next)
            .ok_or(ArchiveError::Corrupt("archive bundle member range"))?;
        member.object.verify(body)?;
        if number == reference.member as usize {
            let mut logical = expected.clone();
            logical.bundle = None;
            if scope != archive.scope
                || member.root != archive.root
                || member.birth != archive.birth
                || member.schema != archive.schema
                || logical != member.object
            {
                return Err(ArchiveError::Corrupt(
                    "archive bundle history/object binding",
                ));
            }
            selected = Some(body.to_vec());
        }
        end = next;
    }
    if end != bytes.len() {
        return Err(ArchiveError::Corrupt("archive bundle trailing bytes"));
    }
    selected.ok_or(ArchiveError::Corrupt("archive bundle member missing"))
}

struct Request {
    archive: CellArchive,
    object: ObjectRef,
    bytes: Vec<u8>,
    reply: oneshot::Sender<Result<ObjectRef, ArchiveError>>,
}

#[derive(Clone)]
pub(super) struct Client(mpsc::Sender<Request>);

impl Client {
    pub(super) async fn put(
        &self,
        archive: &CellArchive,
        object: ObjectRef,
        bytes: Vec<u8>,
    ) -> Result<ObjectRef, ArchiveError> {
        let (reply, result) = oneshot::channel();
        let mut archive = archive.clone();
        archive.bundle_client = None;
        self.0
            .send(Request {
                archive,
                object,
                bytes,
                reply,
            })
            .await
            .map_err(|_| ArchiveError::Stopped)?;
        result.await.map_err(|_| ArchiveError::Stopped)?
    }
}

async fn flush(mut requests: Vec<Request>) {
    let payload_bytes = requests
        .iter()
        .try_fold(0usize, |sum, request| sum.checked_add(request.bytes.len()));
    let limit = requests
        .iter()
        .map(|request| request.archive.limits.max_object_bytes)
        .min()
        .unwrap_or(0)
        .min(MAX_BYTES);
    // Large objects retain the existing direct path. No fragmentation, extra
    // copies of large recovery sets, or unbounded waiting is introduced.
    if requests.len() == 1
        || payload_bytes.is_none_or(|bytes| {
            limit
                .checked_sub(MAX_INDEX_BYTES + 12)
                .is_none_or(|remaining| bytes > remaining)
        })
    {
        for request in requests {
            let result = request
                .archive
                .put_direct(request.object, request.bytes)
                .await;
            let _ = request.reply.send(result);
        }
        return;
    }
    requests.sort_by(|left, right| left.archive.scope.cmp(&right.archive.scope));
    let archive = requests[0].archive.clone();
    let parts = requests
        .iter_mut()
        .map(|request| Part {
            member: Member {
                scope: (&request.archive.scope).into(),
                root: request.archive.root,
                birth: request.archive.birth,
                schema: request.archive.schema,
                object: request.object.clone(),
                offset: 0,
            },
            bytes: std::mem::take(&mut request.bytes),
        })
        .collect();
    let result = async {
        let (bytes, objects) = archive
            .host
            .run_blocking(move || encode(parts))
            .await
            .map_err(ArchiveError::Worker)??;
        let reference = objects[0]
            .bundle
            .as_ref()
            .ok_or(ArchiveError::Unconfirmed)?;
        let path = archive.bundle_key(reference);
        let length = bytes.len();
        let digest = reference.digest;
        let outcome = archive
            .io(
                archive.deadline()?,
                archive
                    .store
                    .put_opts(&path, bytes.into(), PutOptions::from(PutMode::Create)),
            )
            .await;
        match archive.read(&path, limit, Some(length as u64)).await? {
            Some((observed, _)) => {
                archive
                    .host
                    .run_blocking(move || {
                        if observed.len() != length || manifest::digest(&observed) != digest {
                            return Err(ArchiveError::Corrupt(
                                "archive bundle upload confirmation",
                            ));
                        }
                        Ok(())
                    })
                    .await
                    .map_err(ArchiveError::Worker)??;
                Ok(objects)
            }
            None => match outcome {
                Err(error) => Err(error),
                Ok(_) => Err(ArchiveError::Unconfirmed),
            },
        }
    }
    .await;
    match result {
        Ok(objects) => {
            for (request, object) in requests.into_iter().zip(objects) {
                let _ = request.reply.send(Ok(object));
            }
        }
        Err(error) => {
            let mut error = Some(error);
            for request in requests {
                let _ = request
                    .reply
                    .send(Err(error.take().unwrap_or(ArchiveError::Unconfirmed)));
            }
        }
    }
}

#[cfg(test)]
#[path = "bundle_tests.rs"]
mod tests;

impl CellArchive {
    /// Coalesce bounded immutable payloads from two histories in one namespace.
    /// Results retain input order. Each successful result proves only its own
    /// verified head; another member may fail after this member publishes.
    pub async fn publish_batch(
        inputs: Vec<(Self, CommittedArchiveCut)>,
        verifier: &ArchiveVerifier,
    ) -> Result<Vec<Result<ArchiveRecoverySet, ArchiveError>>, ArchiveError> {
        if inputs.len() > MAX_GROUPS {
            return Err(ArchiveError::Invalid("archive batch namespace/count"));
        }
        let mut completed = (0..inputs.len()).map(|_| None).collect::<Vec<_>>();
        Self::publish_batch_each(inputs, verifier, |number, result| {
            completed[number] = Some(result);
        })
        .await?;
        completed
            .into_iter()
            .map(|result| result.ok_or(ArchiveError::Unconfirmed))
            .collect()
    }

    /// Report each verified history independently, using its original input
    /// index. A later batch error does not revoke an already reported result;
    /// remaining histories are unconfirmed. The callback runs synchronously and
    /// must not block publication. The batch retains its bound of two histories.
    pub async fn publish_batch_each(
        inputs: Vec<(Self, CommittedArchiveCut)>,
        verifier: &ArchiveVerifier,
        mut complete: impl FnMut(usize, Result<ArchiveRecoverySet, ArchiveError>),
    ) -> Result<(), ArchiveError> {
        let first = inputs.first().ok_or(ArchiveError::Limit)?;
        if inputs.len() > MAX_GROUPS
            || inputs
                .iter()
                .any(|(archive, _)| !archive.can_batch_with(&first.0))
            || inputs.len() == 2 && inputs[0].0.scope == inputs[1].0.scope
        {
            return Err(ArchiveError::Invalid("archive batch namespace/count"));
        }
        if inputs.len() == 1 {
            let (archive, cut) = inputs.into_iter().next().ok_or(ArchiveError::Limit)?;
            complete(0, archive.publish(cut, verifier).await);
            return Ok(());
        }
        let domain = first.0.io_domain.clone().ok_or(ArchiveError::Stopped)?;
        let (sender, mut receiver) = mpsc::channel(MAX_GROUPS);
        let mut jobs = FuturesUnordered::new();
        for (number, (mut archive, cut)) in inputs.into_iter().enumerate() {
            archive.bundle_client = Some(Client(sender.clone()));
            jobs.push(async move { (number, archive.publish(cut, verifier).await) });
        }
        drop(sender);
        while !jobs.is_empty() {
            let request = crate::asyncrt::select! {
                done = jobs.next() => {
                    if let Some((number, result)) = done { complete(number, result); }
                    continue;
                }
                request = receiver.recv() => request.ok_or(ArchiveError::Unconfirmed)?,
            };
            let mut requests = vec![request];
            let deadline = domain
                .mono_ms()
                .map_err(|_| ArchiveError::Stopped)?
                .saturating_add(COALESCE_MS);
            let timer = domain
                .sleep_until(deadline)
                .map_err(|_| ArchiveError::Stopped)?;
            tokio::pin!(timer);
            while requests.len() < jobs.len() && requests.len() < MAX_GROUPS {
                crate::asyncrt::select! {
                    done = jobs.next() => if let Some((number, result)) = done { complete(number, result); },
                    request = receiver.recv() => requests.push(request.ok_or(ArchiveError::Unconfirmed)?),
                    _ = &mut timer => break,
                }
            }
            flush(requests).await;
        }
        Ok(())
    }
}
