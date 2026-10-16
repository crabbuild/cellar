//! Integrity and identity for the independent acknowledged-range/seal barrier.
use super::{validate_log_leader, FollowerState};
use anyhow::Context as _;
use sha2::{Digest, Sha256};
use std::path::Path;

#[cfg(test)]
#[path = "follower_state_tests.rs"]
mod tests;

const MAX_BYTES: usize = 4096;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    format: u32,
    leader: String,
    node: String,
    state: FollowerState,
    checksum: String,
}

fn checksum(leader: &str, node: &str, state: FollowerState) -> anyhow::Result<String> {
    validate_log_leader(leader)?;
    anyhow::ensure!(
        leader.len() <= 1024 && !node.is_empty() && node.len() <= 1024,
        "invalid follower state identity length"
    );
    anyhow::ensure!(
        state.base <= state.end && (state.fragment_epoch != 0 || state.base == 0 && state.end == 0),
        "invalid follower state range"
    );
    let mut hash = Sha256::new();
    hash.update(b"cellar-follower-state\0");
    hash.update(1_u32.to_le_bytes());
    for identity in [leader, node] {
        hash.update((identity.len() as u32).to_le_bytes());
        hash.update(identity.as_bytes());
    }
    for number in [state.fragment_epoch, state.base, state.end, state.sealed_to] {
        hash.update(number.to_le_bytes());
    }
    Ok(format!("{:x}", hash.finalize()))
}

pub(super) fn encode(leader: &str, node: &str, state: FollowerState) -> anyhow::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(&Record {
        format: 1,
        leader: leader.to_owned(),
        node: node.to_owned(),
        state,
        checksum: checksum(leader, node, state)?,
    })?;
    anyhow::ensure!(
        bytes.len() <= MAX_BYTES,
        "follower state exceeds format bound"
    );
    Ok(bytes)
}

pub(super) fn decode(bytes: &[u8], leader: &str, node: &str) -> anyhow::Result<FollowerState> {
    anyhow::ensure!(
        bytes.len() <= MAX_BYTES,
        "follower state exceeds format bound"
    );
    let record: Record = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        record.format == 1,
        "unsupported follower state format; recreate development state"
    );
    anyhow::ensure!(
        record.leader == leader && record.node == node,
        "follower state identity mismatch"
    );
    anyhow::ensure!(
        record.checksum == checksum(leader, node, record.state)?,
        "follower state checksum mismatch"
    );
    Ok(record.state)
}

pub(super) fn load(
    filesystem: &dyn cellar_ltx::FileSystem,
    path: &Path,
    leader: &str,
    node: &str,
) -> anyhow::Result<Option<FollowerState>> {
    let mut file = match filesystem.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("cannot open follower state"),
    };
    // Pin one inode across the length checks and bounded read. A path stat
    // followed by a whole-file read can race replacement and allocate without
    // a bound. Only absence at open means optional state; later NotFound/IO
    // errors must not turn retained metadata into a fresh empty fragment.
    let length = file
        .file_len()
        .context("cannot stat opened follower state")?;
    anyhow::ensure!(
        length <= MAX_BYTES as u64,
        "follower state exceeds format bound"
    );
    let bytes = file
        .read_exact_at(0, length as usize)
        .context("cannot read opened follower state")?;
    anyhow::ensure!(
        bytes.len() as u64 == length && file.file_len()? == length,
        "follower state changed during read"
    );
    decode(&bytes, leader, node).map(Some)
}
