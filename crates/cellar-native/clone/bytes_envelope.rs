// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Inspect a finite control prefix without materializing its binary payload.
use super::*;

const METADATA_BYTES: usize = 1024;

/// Read exactly `[tag, format, label, fixed_private_array_buffer]`, borrowing
/// the buffer from the encoded document. No buffer or arbitrary object graph
/// is allocated. Metadata is limited to 1 KiB, the label to `max_label_units`
/// UTF-16 units (at most 256), and values/depth to the same finite envelope
/// budgets as the owned decoder. Clone versions 10 through 16 are supported.
///
/// Body admission belongs to the caller: inspect its borrowed length before
/// making an owned copy. This does not admit transfers, views or capabilities.
pub fn decode_tagged_bytes<'a>(
    bytes: &'a [u8],
    tag: &str,
    format: u32,
    max_label_units: usize,
) -> Result<(String, &'a [u8])> {
    ensure!(
        tag.len() <= 128 && max_label_units <= 256,
        "invalid envelope metadata limits"
    );
    let mut envelope = Envelope {
        reader: Reader {
            bytes,
            at: 0,
            version: 0,
            broken_v13: false,
            objects: Vec::new(),
            values: 0,
            limits: Limits {
                bytes: bytes.len(),
                objects: 2,
                values: 8,
                depth: 4,
            },
        },
        body_bytes: 0,
    };
    ensure!(
        envelope.reader.byte()? == 0xff,
        "clone envelope has no version header"
    );
    envelope.reader.version = envelope.reader.u32()?;
    ensure!(
        (10..=16).contains(&envelope.reader.version),
        "unsupported clone envelope version"
    );
    let (kind, depth) = envelope.tag(0)?;
    ensure!(
        kind == b'A' && depth < 4,
        "clone envelope requires a dense array"
    );
    ensure!(
        envelope.reader.u32()? == 4,
        "clone envelope requires four fields"
    );
    let depth = depth + 1;
    ensure!(
        envelope.string(depth, tag.encode_utf16().count())?
            == tag.encode_utf16().collect::<Vec<_>>(),
        "clone envelope tag differs"
    );
    let (kind, _) = envelope.tag(depth)?;
    let marker = match kind {
        b'N' => envelope.reader.double()?,
        b'U' => f64::from(envelope.reader.u32()?),
        b'I' => {
            let value = envelope.reader.u32()?;
            f64::from((value >> 1) as i32 ^ -((value & 1) as i32))
        }
        _ => bail!("clone envelope format requires a number"),
    };
    ensure!(marker == f64::from(format), "clone envelope format differs");
    let label = String::from_utf16(&envelope.string(depth, max_label_units)?)?;
    let (kind, buffer_depth) = envelope.tag(depth)?;
    ensure!(
        kind == b'B' && buffer_depth < 4,
        "clone envelope requires a fixed private buffer"
    );
    let length = usize::try_from(envelope.reader.size()?)?;
    envelope.metadata()?;
    let body = envelope.reader.raw(length)?;
    envelope.body_bytes = length;
    envelope.padding()?;
    ensure!(
        envelope.reader.byte()? == b'$',
        "clone envelope has extra fields or a view"
    );
    ensure!(
        envelope.reader.u32()? == 0 && envelope.reader.u32()? == 4,
        "invalid clone envelope array footer"
    );
    envelope.padding()?;
    ensure!(
        envelope.reader.at == bytes.len(),
        "trailing clone envelope bytes"
    );
    envelope.metadata()?;
    Ok((label, body))
}

struct Envelope<'a> {
    reader: Reader<'a>,
    body_bytes: usize,
}

impl Envelope<'_> {
    fn metadata(&self) -> Result<()> {
        ensure!(
            self.reader.at - self.body_bytes <= METADATA_BYTES,
            "clone envelope metadata exceeds limit"
        );
        Ok(())
    }

    fn padding(&mut self) -> Result<()> {
        while self.reader.bytes.get(self.reader.at) == Some(&0) {
            self.reader.at += 1;
            self.metadata()?;
        }
        self.metadata()
    }

    fn tag(&mut self, mut depth: usize) -> Result<(u8, usize)> {
        loop {
            ensure!(
                depth <= self.reader.limits.depth,
                "clone envelope depth exceeds limit"
            );
            self.reader.values += 1;
            ensure!(
                self.reader.values <= self.reader.limits.values,
                "clone envelope values exceed limit"
            );
            self.padding()?;
            let tag = self.reader.byte()?;
            self.metadata()?;
            if tag != b'?' {
                return Ok((tag, depth));
            }
            self.reader.u32()?;
            self.metadata()?;
            depth += 1;
        }
    }

    fn string(&mut self, depth: usize, maximum: usize) -> Result<Vec<u16>> {
        let (tag, _) = self.tag(depth)?;
        ensure!(
            matches!(tag, b'S' | b'"' | b'c'),
            "clone envelope requires a string"
        );
        let length = self.reader.u32()? as usize;
        // Check the declared size before allocating, including malformed or
        // truncated strings. No metadata field can hide an owned body copy.
        let multiplier = match tag {
            b'S' => 4,
            b'c' => 2,
            _ => 1,
        };
        ensure!(
            length <= maximum * multiplier,
            "clone envelope string exceeds limit"
        );
        let bytes = self.reader.raw(length)?;
        self.metadata()?;
        let units: Vec<u16> = match tag {
            b'S' => std::str::from_utf8(bytes)?.encode_utf16().collect(),
            b'c' => {
                ensure!(length.is_multiple_of(2), "odd clone envelope UTF-16 length");
                bytes
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|unit| u16::from_le_bytes(*unit))
                    .collect()
            }
            _ => bytes.iter().map(|byte| u16::from(*byte)).collect(),
        };
        ensure!(
            units.len() <= maximum,
            "clone envelope string exceeds limit"
        );
        Ok(units)
    }
}

#[cfg(test)]
#[path = "bytes_envelope_tests.rs"]
mod tests;
