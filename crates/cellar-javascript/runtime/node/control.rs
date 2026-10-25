// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Allocation-free admission for the private, flat Node control protocol.
//!
//! Application values stay inside an encoded payload string or raw attachment.
//! General JSON collections must never reach the allocating control decoder.
use anyhow::{ensure, Context as _, Result};

const MAX_FIELDS: usize = 16;
const MAX_KEY_BYTES: usize = 64;
const MAX_NUMBER_BYTES: usize = 32;

pub(super) struct Bounds {
    /// Owned decoded strings (including keys) and the copied attachment.
    pub retained: usize,
    /// serde_json reuses one scratch buffer. Its geometric growth is bounded
    /// by twice the decoded string bytes, with an eight-byte minimum capacity.
    pub scratch: usize,
}

/// Bound shape and string expansion before serde_json allocates any objects.
/// The frame's encoded bytes are already reserved separately by the transport.
pub(super) fn preflight(input: &[u8], body: usize) -> Result<Bounds> {
    std::str::from_utf8(input).context("Node control is not UTF-8")?;
    let mut parser = Parser {
        input,
        position: 0,
        strings: 0,
    };
    parser.whitespace();
    parser.expect(b'{')?;
    parser.whitespace();
    let mut keys: [&[u8]; MAX_FIELDS] = [&[]; MAX_FIELDS];
    let mut fields = 0;
    if !parser.take(b'}') {
        loop {
            ensure!(fields < MAX_FIELDS, "Node control has too many fields");
            parser.whitespace();
            let key = parser.string()?;
            // Bootstrap emits literal ASCII keys. Refusing escaped spellings
            // also permits duplicate detection through borrowed wire slices.
            ensure!(
                !key.escaped && key.raw.is_ascii() && key.raw.len() <= MAX_KEY_BYTES,
                "Node control requires bounded literal ASCII keys"
            );
            ensure!(
                !keys[..fields].contains(&key.raw),
                "duplicate Node control field"
            );
            keys[fields] = key.raw;
            fields += 1;
            parser.whitespace();
            parser.expect(b':')?;
            parser.whitespace();
            if parser.input.get(parser.position) == Some(&b'"') {
                parser.string()?;
            } else {
                parser.primitive()?;
            }
            parser.whitespace();
            if parser.take(b'}') {
                break;
            }
            parser.expect(b',')?;
        }
    }
    parser.whitespace();
    ensure!(parser.position == input.len(), "trailing Node control data");
    Ok(Bounds {
        retained: parser
            .strings
            .checked_add(body)
            .context("Node decoded control size overflow")?,
        scratch: parser
            .strings
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(8))
            .context("Node control scratch size overflow")?,
    })
}

struct StringShape<'a> {
    raw: &'a [u8],
    escaped: bool,
}

struct Parser<'a> {
    input: &'a [u8],
    position: usize,
    strings: usize,
}

impl<'a> Parser<'a> {
    fn whitespace(&mut self) {
        while self
            .input
            .get(self.position)
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
        {
            self.position += 1;
        }
    }

    fn take(&mut self, byte: u8) -> bool {
        if self.input.get(self.position) == Some(&byte) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, byte: u8) -> Result<()> {
        ensure!(self.take(byte), "invalid Node control syntax");
        Ok(())
    }

    fn byte(&mut self) -> Result<u8> {
        let byte = *self
            .input
            .get(self.position)
            .context("truncated Node control")?;
        self.position += 1;
        Ok(byte)
    }

    fn hex(&mut self) -> Result<u16> {
        let mut value = 0;
        for _ in 0..4 {
            let digit = match self.byte()? {
                byte @ b'0'..=b'9' => byte - b'0',
                byte @ b'a'..=b'f' => byte - b'a' + 10,
                byte @ b'A'..=b'F' => byte - b'A' + 10,
                _ => anyhow::bail!("invalid Node control Unicode escape"),
            };
            value = value * 16 + u16::from(digit);
        }
        Ok(value)
    }

    fn string(&mut self) -> Result<StringShape<'a>> {
        self.expect(b'"')?;
        let start = self.position;
        let mut decoded = 0_usize;
        let mut escaped = false;
        loop {
            let bytes = match self.byte()? {
                b'"' => break,
                b'\\' => {
                    escaped = true;
                    match self.byte()? {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => 1,
                        b'u' => match self.hex()? {
                            0x0000..=0x007f => 1,
                            0x0080..=0x07ff => 2,
                            0xd800..=0xdbff => {
                                self.expect(b'\\')?;
                                self.expect(b'u')?;
                                ensure!(
                                    matches!(self.hex()?, 0xdc00..=0xdfff),
                                    "invalid Node control surrogate pair"
                                );
                                4
                            }
                            0xdc00..=0xdfff => anyhow::bail!("unpaired Node control surrogate"),
                            _ => 3,
                        },
                        _ => anyhow::bail!("invalid Node control string escape"),
                    }
                }
                0..=31 => anyhow::bail!("unescaped Node control character"),
                // UTF-8 was validated up front. Counting its raw bytes gives
                // the decoded size without constructing a temporary string.
                _ => 1,
            };
            decoded = decoded
                .checked_add(bytes)
                .context("Node control string size overflow")?;
        }
        self.strings = self
            .strings
            .checked_add(decoded)
            .context("Node control string total overflow")?;
        Ok(StringShape {
            raw: &self.input[start..self.position - 1],
            escaped,
        })
    }

    fn primitive(&mut self) -> Result<()> {
        let start = self.position;
        while self
            .input
            .get(self.position)
            .is_some_and(|byte| !matches!(byte, b',' | b'}' | b' ' | b'\t' | b'\r' | b'\n'))
        {
            self.position += 1;
            ensure!(
                self.position - start <= MAX_NUMBER_BYTES,
                "oversized Node control scalar"
            );
        }
        let token = &self.input[start..self.position];
        // serde_json still owns complete scalar syntax/number validation.
        // This gate excludes containers and bounds its numeric parsing work.
        ensure!(
            matches!(token, b"true" | b"false" | b"null")
                || token
                    .first()
                    .is_some_and(|byte| matches!(byte, b'-' | b'0'..=b'9')),
            "Node control values must be scalars"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
