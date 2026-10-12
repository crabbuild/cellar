// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! WHATWG text encoding with caller-owned incremental state.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecoderOptions {
    pub fatal: bool,
    pub ignore_bom: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    UnknownEncoding,
    InvalidData,
    OutputTooLarge,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnknownEncoding => "The encoding label is not supported.",
            Self::InvalidData => "The encoded data was not valid.",
            Self::OutputTooLarge => "The decoded text is too large.",
        })
    }
}

impl std::error::Error for DecodeError {}

pub fn canonical_label(label: &str) -> Option<String> {
    encoding_rs::Encoding::for_label_no_replacement(label.as_bytes())
        .map(|encoding| encoding.name().to_ascii_lowercase())
}

/// `stream=true` preserves incomplete sequences; a final decode or an error
/// resets state so this decoder can be reused for a new input.
pub struct TextDecoder {
    encoding: &'static encoding_rs::Encoding,
    options: DecoderOptions,
    decoder: Option<encoding_rs::Decoder>,
}

impl TextDecoder {
    pub fn new(label: &str, options: DecoderOptions) -> Result<Self, DecodeError> {
        let encoding = encoding_rs::Encoding::for_label_no_replacement(label.as_bytes())
            .ok_or(DecodeError::UnknownEncoding)?;
        Ok(Self {
            encoding,
            options,
            decoder: None,
        })
    }

    pub fn encoding(&self) -> String {
        self.encoding.name().to_ascii_lowercase()
    }

    pub fn decode(&mut self, bytes: &[u8], stream: bool) -> Result<String, DecodeError> {
        let decoder = self.decoder.get_or_insert_with(|| {
            if self.options.ignore_bom {
                self.encoding.new_decoder_without_bom_handling()
            } else {
                self.encoding.new_decoder_with_bom_removal()
            }
        });
        let result = run_decoder(decoder, bytes, self.options.fatal, !stream);
        if !stream || result.is_err() {
            self.decoder = None;
        }
        result
    }
}

/// Shared low-level backend for adapters which manage their own decoder table.
pub fn run_decoder(
    decoder: &mut encoding_rs::Decoder,
    bytes: &[u8],
    fatal: bool,
    last: bool,
) -> Result<String, DecodeError> {
    // encoding_rs flushes a pending Shift_JIS lead on an empty chunk. The
    // streaming TextDecoder contract requires such a chunk to be a no-op.
    if bytes.is_empty() && !last {
        return Ok(String::new());
    }
    let mut output = String::new();
    let capacity = if fatal {
        decoder.max_utf8_buffer_length_without_replacement(bytes.len())
    } else {
        decoder.max_utf8_buffer_length(bytes.len())
    }
    .ok_or(DecodeError::OutputTooLarge)?;
    output
        .try_reserve(capacity)
        .map_err(|_| DecodeError::OutputTooLarge)?;
    if fatal {
        if !matches!(
            decoder.decode_to_string_without_replacement(bytes, &mut output, last),
            (encoding_rs::DecoderResult::InputEmpty, _),
        ) {
            return Err(DecodeError::InvalidData);
        }
    } else {
        let _ = decoder.decode_to_string(bytes, &mut output, last);
    }
    Ok(output)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EncodeIntoResult {
    /// UTF-16 code units consumed, matching TextEncoder.encodeInto.
    pub read: usize,
    pub written: usize,
}

/// Encodes UTF-16 strings, including the unpaired surrogates JS can represent.
pub fn encode_utf16(source: &[u16]) -> Vec<u8> {
    String::from_utf16_lossy(source).into_bytes()
}

/// Writes whole UTF-8 characters only, replacing lone surrogates with U+FFFD.
pub fn encode_into_utf16(source: &[u16], destination: &mut [u8]) -> EncodeIntoResult {
    let mut result = EncodeIntoResult::default();
    while let Some(&unit) = source.get(result.read) {
        let (character, units) = if (0xd800..=0xdbff).contains(&unit) {
            match source.get(result.read + 1) {
                Some(&low) if (0xdc00..=0xdfff).contains(&low) => {
                    let point =
                        0x10000 + ((u32::from(unit) - 0xd800) << 10) + (u32::from(low) - 0xdc00);
                    (
                        char::from_u32(point).expect("a surrogate pair is a scalar"),
                        2,
                    )
                }
                _ => (char::REPLACEMENT_CHARACTER, 1),
            }
        } else {
            (
                char::from_u32(u32::from(unit)).unwrap_or(char::REPLACEMENT_CHARACTER),
                1,
            )
        };
        let mut buffer = [0; 4];
        let bytes = character.encode_utf8(&mut buffer).as_bytes();
        if bytes.len() > destination.len() - result.written {
            break;
        }
        destination[result.written..result.written + bytes.len()].copy_from_slice(bytes);
        result.read += units;
        result.written += bytes.len();
    }
    result
}
