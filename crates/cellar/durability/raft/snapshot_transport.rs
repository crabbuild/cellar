// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared authenticated bulk framing and bounded checkpoint I/O.
use super::*;

pub(in crate::raft) fn memory_bound(options: &TransportOptions) -> usize {
    // File-backed bodies and indexes do not reserve image-sized memory. Cover
    // wire framing/copies plus codec/hash/compression and independent sealing
    // buffers. One transfer retains this reservation through blocking work.
    // Root streams additionally retain a bounded manifest/dependency index
    // beside their single extent. The global 10 GB geometry bounds that index.
    let root_working = cellar_ltx::internal::EXTENT_BYTES as usize + 1024 * 1024;
    4 * options.snapshot_chunk_bytes + io_profile(options).max_working_bytes().max(root_working)
}

fn io_profile(options: &TransportOptions) -> cellar_ltx::stream_snapshot::IoProfile {
    // Large-cell transports admit the larger codec allowance before capture
    // or download. This changes physical batching, never the encoded format.
    if options.max_snapshot_image_bytes >= 256 * 1024 * 1024 {
        cellar_ltx::stream_snapshot::IoProfile::Buffered
    } else {
        cellar_ltx::stream_snapshot::IoProfile::Compact
    }
}

impl State {
    pub(super) async fn snapshot_io<T>(
        &self,
        pool: &Pool,
        deadline: u64,
        future: impl Future<Output = Result<T, TransportError>>,
    ) -> Result<T, TransportError> {
        let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if now >= deadline {
            return Err(TransportError::Timeout);
        }
        let timer = self
            .domain
            .sleep_until(deadline)
            .map_err(|_| TransportError::Closed)?;
        crate::asyncrt::select! {
            _ = timer => Err(TransportError::Timeout),
            result = self.bounded(Some(pool), future) => result,
        }
    }
}

pub(super) struct Wire<'a> {
    pub(super) auth: &'a PeerAuth,
    pub(super) exporter: &'a [u8; 32],
    pub(super) options: &'a TransportOptions,
    pub(super) max_body: usize,
}

impl Wire<'_> {
    pub(super) fn encode<T: Serialize>(
        &self,
        magic: &[u8; 4],
        purpose: &[u8],
        request: &[u8],
        value: &T,
        body: &[u8],
    ) -> Result<FrameHead, TransportError> {
        let header = serde_json::to_vec(value).map_err(|_| TransportError::Protocol)?;
        if header.is_empty()
            || header.len() > self.options.max_header_bytes
            || body.len() > self.max_body
        {
            return Err(TransportError::Limit);
        }
        let mut prefix = [0; frame::PREFIX];
        prefix[..4].copy_from_slice(magic);
        prefix[4..8].copy_from_slice(&1u32.to_be_bytes());
        prefix[8..12].copy_from_slice(&(header.len() as u32).to_be_bytes());
        prefix[12..].copy_from_slice(&(body.len() as u64).to_be_bytes());
        let tag = self
            .auth
            .raft_tag(purpose, &[self.exporter, request, &prefix, &header]);
        Ok(FrameHead {
            prefix,
            header,
            tag,
            payload_len: body.len(),
        })
    }

    pub(super) fn decode<T: for<'de> Deserialize<'de>>(
        &self,
        magic: &[u8; 4],
        purpose: &[u8],
        head: &FrameHead,
        request: Option<&[u8; 32]>,
    ) -> Result<T, TransportError> {
        let request = request.map_or(&[][..], |request| &request[..]);
        if &head.prefix[..4] != magic
            || !self.auth.verify_raft_tag(
                purpose,
                &[self.exporter, request, &head.prefix, &head.header],
                &head.tag,
            )
        {
            return Err(TransportError::Authentication);
        }
        serde_json::from_slice(&head.header).map_err(|_| TransportError::Protocol)
    }
}

pub(super) async fn read_bulk_head<S: AsyncRead + Unpin>(
    stream: &mut S,
    prefix: Option<[u8; frame::PREFIX]>,
    magic: &[u8; 4],
    max_payload: usize,
    options: &TransportOptions,
) -> Result<FrameHead, TransportError> {
    let prefix = match prefix {
        Some(prefix) => prefix,
        None => {
            let mut prefix = [0; frame::PREFIX];
            stream.read_exact(&mut prefix).await?;
            prefix
        }
    };
    if &prefix[..4] != magic || prefix[4..8] != 1u32.to_be_bytes() {
        return Err(TransportError::Protocol);
    }
    let header_len = u32::from_be_bytes(
        prefix[8..12]
            .try_into()
            .map_err(|_| TransportError::Protocol)?,
    ) as usize;
    let payload_len = u64::from_be_bytes(
        prefix[12..]
            .try_into()
            .map_err(|_| TransportError::Protocol)?,
    );
    if header_len == 0 || header_len > options.max_header_bytes || payload_len > max_payload as u64
    {
        return Err(TransportError::Limit);
    }
    let mut header = vec![0; header_len];
    stream.read_exact(&mut header).await?;
    let mut tag = [0; frame::TAG];
    stream.read_exact(&mut tag).await?;
    Ok(FrameHead {
        prefix,
        header,
        tag,
        payload_len: payload_len as usize,
    })
}
