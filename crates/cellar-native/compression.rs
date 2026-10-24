// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js/zlib.rs.

//! Owned, incremental gzip, zlib and raw-deflate transforms.
use anyhow::{anyhow, Result};

/// The one-shot Node zlib operations, using the same streaming backend.
pub fn transform(operation: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    use std::io::{Read, Write};
    match operation {
        "gzip" => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(bytes)?;
            Ok(encoder.finish()?)
        }
        "gunzip" => {
            let mut decoder = flate2::read::GzDecoder::new(bytes);
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
        "deflate" => {
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(bytes)?;
            Ok(encoder.finish()?)
        }
        "inflate" => {
            let mut decoder = flate2::read::ZlibDecoder::new(bytes);
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
        "deflateRaw" => {
            let mut encoder =
                flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(bytes)?;
            Ok(encoder.finish()?)
        }
        "inflateRaw" => {
            let mut decoder = flate2::read::DeflateDecoder::new(bytes);
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
        _ => Err(anyhow!("unsupported zlib operation")),
    }
}

/// One live `CompressionStream`/`DecompressionStream`: a stateful flate2
/// writer whose `Vec` sink is drained after every push, so output streams
/// chunk by chunk instead of arriving once at close.
enum Codec {
    GzipEncode(flate2::write::GzEncoder<Vec<u8>>),
    GzipDecode(flate2::write::GzDecoder<Vec<u8>>),
    ZlibEncode(flate2::write::ZlibEncoder<Vec<u8>>),
    ZlibDecode(flate2::write::ZlibDecoder<Vec<u8>>),
    RawEncode(flate2::write::DeflateEncoder<Vec<u8>>),
    RawDecode(flate2::write::DeflateDecoder<Vec<u8>>),
}

/// The six writer types share `write`/`flush`/`get_mut`/`finish` but no
/// trait, so dispatch once here.
macro_rules! zlib_each {
    ($stream:expr, $w:ident => $body:expr) => {
        match $stream {
            Codec::GzipEncode($w) => $body,
            Codec::GzipDecode($w) => $body,
            Codec::ZlibEncode($w) => $body,
            Codec::ZlibDecode($w) => $body,
            Codec::RawEncode($w) => $body,
            Codec::RawDecode($w) => $body,
        }
    };
}

impl Codec {
    fn new(format: &str, decompress: bool) -> Option<Codec> {
        use flate2::write as z;
        let level = flate2::Compression::default();
        let sink = Vec::new;
        Some(match (format, decompress) {
            ("gzip", false) => Self::GzipEncode(z::GzEncoder::new(sink(), level)),
            ("gzip", true) => Self::GzipDecode(z::GzDecoder::new(sink())),
            ("deflate", false) => Self::ZlibEncode(z::ZlibEncoder::new(sink(), level)),
            ("deflate", true) => Self::ZlibDecode(z::ZlibDecoder::new(sink())),
            ("deflate-raw", false) => Self::RawEncode(z::DeflateEncoder::new(sink(), level)),
            ("deflate-raw", true) => Self::RawDecode(z::DeflateDecoder::new(sink())),
            _ => return None,
        })
    }

    fn decompress(&self) -> bool {
        matches!(
            self,
            Self::GzipDecode(_) | Self::ZlibDecode(_) | Self::RawDecode(_)
        )
    }

    /// Workerd's canonical message, matched by its conformance suite.
    fn failure(&self) -> &'static str {
        if self.decompress() {
            "Decompression failed."
        } else {
            "Compression failed."
        }
    }

    /// Write one chunk and return whatever output it produced. Decoded
    /// output flows per chunk; encoders hold theirs until a full buffer
    /// accumulates (matching Workerd, whose suite reads a small message
    /// back as one chunk), with the rest arriving from `finish`. A
    /// decoder that stops consuming input has hit the end of the
    /// compressed stream, so leftover bytes are trailing garbage —
    /// Workerd's strict_compression_checks.
    fn push(&mut self, mut input: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;

        const ENCODER_CHUNK: usize = 32 * 1024;
        let failure = self.failure();
        let held = if self.decompress() { 0 } else { ENCODER_CHUNK };
        zlib_each!(self, w => {
            while !input.is_empty() {
                let n = w.write(input).map_err(|_| anyhow!(failure))?;
                if n == 0 { return Err(anyhow!(failure)); }
                input = &input[n..];
            }
            if w.get_ref().len() < held { return Ok(Vec::new()); }
            Ok(std::mem::take(w.get_mut()))
        })
    }

    /// Terminate the stream and return the final output. For gzip this
    /// also validates the trailer, so a truncated stream errors here.
    fn finish(self) -> Result<Vec<u8>> {
        let failure = self.failure();
        zlib_each!(self, w => w.finish().map_err(|_| anyhow!(failure)))
    }
}

/// A transform whose state is owned by its caller. Drop releases native
/// buffers. A failed push poisons it, and finish consumes it.
pub struct CompressionStream {
    codec: Option<Codec>,
}

impl CompressionStream {
    pub fn new(format: &str, decompress: bool) -> Option<Self> {
        Some(Self {
            codec: Some(Codec::new(format, decompress)?),
        })
    }

    pub fn push(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        let result = self
            .codec
            .as_mut()
            .ok_or_else(|| anyhow!("zlib stream already closed"))?
            .push(input);
        if result.is_err() {
            self.codec = None;
        }
        result
    }

    pub fn finish(mut self) -> Result<Vec<u8>> {
        self.codec
            .take()
            .ok_or_else(|| anyhow!("zlib stream already closed"))?
            .finish()
    }

    pub fn cancel(&mut self) {
        self.codec = None;
    }
}
