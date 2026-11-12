// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned ordinary RPC values inside independently durable command receipts.
//! Leased capabilities require their session owner and cannot enter this wire.

use crate::engine_api::RpcData;
use anyhow::{ensure, Context as _, Result};

const MAGIC: &[u8; 4] = b"CRPC";
const FORMAT: u8 = 1;

pub(crate) fn has_ingress_nonce(input: &[u8]) -> bool {
    input.starts_with(&[b'C', b'R', b'P', b'C', FORMAT, 3])
}

pub(crate) struct Request {
    pub name: Option<String>,
    pub args: RpcData,
}

#[derive(Clone, Copy)]
enum ValuePurpose {
    Argument,
    Reply,
}

fn validate_structured(bytes: &[u8], purpose: ValuePurpose) -> Result<()> {
    match purpose {
        ValuePurpose::Argument => {
            cellar_native::clone::decode(bytes)?;
        }
        ValuePurpose::Reply => match cellar_native::rpc::decode_response(bytes) {
            Ok(_) => (),
            // A well-formed remote exception is a complete, retainable reply.
            // Malformed graphs and capability envelopes remain refusals.
            Err(error) if error.is::<cellar_native::rpc::RemoteError>() => (),
            Err(error) => return Err(error),
        },
    }
    Ok(())
}

pub(crate) fn validate_identity(
    script: &str,
    class: &str,
    scope: &str,
    name: Option<&str>,
) -> Result<()> {
    let namespace = cellar_native::cell_id::Namespace::new(
        class,
        crate::engine_api::namespace_key(script, class),
    );
    let (actual_class, id) = scope
        .split_once(':')
        .context("invalid named cell identity")?;
    ensure!(actual_class == class, "named cell class differs");
    namespace.id_from_string(id)?;
    if let Some(name) = name {
        ensure!(
            namespace.id_from_name(name).scope() == scope,
            "named cell identity differs"
        );
    }
    Ok(())
}

pub(crate) fn binding_operation(operation: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(operation.as_bytes());
    format!("__cellar_rpc_{}", hex::encode(&digest[..16]))
}

pub(crate) fn binding_digest(operation: &str, digest: [u8; 32]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"cellar-native-binding-operation-1");
    hash.update((operation.len() as u64).to_le_bytes());
    hash.update(operation.as_bytes());
    hash.update(digest);
    hash.finalize().into()
}

pub(crate) fn encode_request(
    name: Option<&str>,
    args: &RpcData,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let bytes = match args {
        RpcData::Json(value) => value.len(),
        RpcData::Structured(value) => value.len(),
        RpcData::Leased { .. } => anyhow::bail!(super::CommandFailure::UnsupportedCapability),
    };
    let length = bytes
        .checked_add(12)
        .and_then(|n| n.checked_add(name.map_or(0, |name| name.len().saturating_add(4))))
        .ok_or(super::CommandFailure::Overloaded)?;
    ensure!(length <= max_bytes, super::CommandFailure::Overloaded);
    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);
    output.push(FORMAT);
    output.push(1);
    match name {
        Some(name) => {
            ensure!(name.len() <= 1024, "cell routing name exceeds its bound");
            output.push(1);
            write(&mut output, name.as_bytes())?;
        }
        None => output.push(0),
    }
    encode_value(&mut output, args, ValuePurpose::Argument)?;
    Ok(output)
}

pub(crate) fn decode_request(input: &[u8]) -> Result<Request> {
    let purpose = *input.get(5).context("truncated native RPC purpose")?;
    ensure!(
        purpose == 1 || purpose == 3,
        "invalid native RPC request purpose"
    );
    let mut reader = Reader::new(input, purpose)?;
    if purpose == 3 {
        reader.take(16)?;
    }
    let name = match reader.byte()? {
        0 => None,
        1 => {
            let bytes = reader.bytes()?;
            ensure!(bytes.len() <= 1024, "cell routing name exceeds its bound");
            Some(std::str::from_utf8(bytes)?.to_owned())
        }
        _ => anyhow::bail!("invalid cell routing name marker"),
    };
    let args = reader.value(ValuePurpose::Argument)?;
    reader.finish()?;
    Ok(Request { name, args })
}

/// Independent ingress calls with identical SQL still have different retained
/// request fingerprints when they race for the same bounded client sequence.
pub(crate) fn encode_ingress_request(
    name: Option<&str>,
    args: &RpcData,
    nonce: [u8; 16],
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let limit = max_bytes
        .checked_sub(16)
        .ok_or(super::CommandFailure::Overloaded)?;
    let request = encode_request(name, args, limit)?;
    let mut output = Vec::with_capacity(request.len() + 16);
    output.extend_from_slice(&request[..5]);
    output.push(3);
    output.extend_from_slice(&nonce);
    output.extend_from_slice(&request[6..]);
    Ok(output)
}

pub(crate) fn encode_response(value: &RpcData) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);
    output.push(FORMAT);
    output.push(2);
    encode_value(&mut output, value, ValuePurpose::Reply)?;
    Ok(output)
}

pub(crate) fn decode_response(input: &[u8]) -> Result<RpcData> {
    let mut reader = Reader::new(input, 2)?;
    let value = reader.value(ValuePurpose::Reply)?;
    reader.finish()?;
    Ok(value)
}

fn encode_value(output: &mut Vec<u8>, value: &RpcData, purpose: ValuePurpose) -> Result<()> {
    match value {
        RpcData::Json(json) => {
            serde_json::from_str::<serde::de::IgnoredAny>(json)?;
            output.push(1);
            write(output, json.as_bytes())?;
        }
        RpcData::Structured(bytes) => {
            validate_structured(bytes, purpose)?;
            output.push(2);
            write(output, bytes)?;
        }
        RpcData::Leased { .. } => anyhow::bail!(super::CommandFailure::UnsupportedCapability),
    }
    Ok(())
}

fn write(output: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    output.extend_from_slice(&u32::try_from(bytes.len())?.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn new(input: &'a [u8], purpose: u8) -> Result<Self> {
        let mut reader = Self(input);
        ensure!(
            reader.take(4)? == MAGIC && reader.byte()? == FORMAT && reader.byte()? == purpose,
            "invalid native RPC format/purpose"
        );
        Ok(reader)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let (value, rest) = self
            .0
            .split_at_checked(length)
            .context("truncated native RPC value")?;
        self.0 = rest;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = u32::from_le_bytes(self.take(4)?.try_into()?) as usize;
        self.take(length)
    }

    fn value(&mut self, purpose: ValuePurpose) -> Result<RpcData> {
        match self.byte()? {
            1 => {
                let json = std::str::from_utf8(self.bytes()?)?;
                serde_json::from_str::<serde::de::IgnoredAny>(json)?;
                Ok(RpcData::Json(json.to_owned()))
            }
            2 => {
                let bytes = self.bytes()?;
                validate_structured(bytes, purpose)?;
                Ok(RpcData::Structured(bytes.to_vec().into()))
            }
            _ => anyhow::bail!("invalid native RPC value kind"),
        }
    }

    fn finish(self) -> Result<()> {
        ensure!(self.0.is_empty(), "trailing native RPC bytes");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_reply_preserves_graph_and_cause_without_becoming_an_argument() -> Result<()> {
        let bytes = cellar_native::rpc::encode_error(
            "SQL_EXEC_ERROR: no such table: missing_table",
            "Error",
            Some("no such table: missing_table"),
        )?;
        let value = RpcData::Structured(bytes.clone().into());
        let mut packet = encode_response(&value)?;
        let decoded = decode_response(&packet)?;
        let RpcData::Structured(actual) = &decoded else {
            panic!("error reply changed representation");
        };
        assert_eq!(actual.as_ref(), bytes.as_slice());
        let error = crate::application::decode_rpc_result(&decoded).unwrap_err();
        let remote = error
            .downcast_ref::<cellar_native::rpc::RemoteError>()
            .unwrap();
        assert_eq!(
            remote.message,
            "SQL_EXEC_ERROR: no such table: missing_table"
        );
        assert!(encode_request(None, &value, 4096).is_err());
        packet[5] = 1;
        packet.insert(6, 0); // An argument packet also carries the optional name.
        assert!(decode_request(&packet).is_err());
        Ok(())
    }

    #[test]
    fn malformed_and_capability_replies_remain_refused() -> Result<()> {
        for bytes in [
            vec![],
            vec![0],
            vec![0, 0xff, 15],
            vec![1, 0xff, 15],
            vec![2, 0xff, 15],
        ] {
            assert!(encode_response(&RpcData::Structured(bytes.clone().into())).is_err());
            let mut packet = MAGIC.to_vec();
            packet.extend([FORMAT, 2, 2]);
            write(&mut packet, &bytes)?;
            assert!(decode_response(&packet).is_err());
        }
        Ok(())
    }
}
