use super::super::{apply::PublishedImage, archive::ArchiveDependency, archive::ArchiveLimits};
use anyhow::{ensure, Context as _, Result};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "effect_blob_tests.rs"]
mod tests;

/// Explicit per-history immutable-value bounds. Bytes remain in replicated SQL;
/// no external reference can substitute for absent content. There is no GC API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftBlobLimits {
    pub max_blob_bytes: usize,
    pub max_objects: usize,
    pub max_total_bytes: usize,
}

impl Default for RaftBlobLimits {
    fn default() -> Self {
        Self {
            max_blob_bytes: 64 * 1024,
            max_objects: 128,
            max_total_bytes: 1024 * 1024,
        }
    }
}

impl RaftBlobLimits {
    pub(crate) fn validate(self) -> Result<()> {
        ensure!(
            (1..=8 * 1024 * 1024).contains(&self.max_blob_bytes)
                && (1..=1024).contains(&self.max_objects)
                && self.max_total_bytes >= self.max_blob_bytes
                && self.max_total_bytes <= 64 * 1024 * 1024,
            "invalid Raft Blob bounds"
        );
        Ok(())
    }
}

/// A content identity, not an existence, access or durability proof. Reading it
/// requires matching bytes in the selected cell's exact committed image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftBlobReference {
    digest: [u8; 32],
    length: u64,
}

impl RaftBlobReference {
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    /// Fixed-width network-order content identity. It grants no authority.
    pub fn encode(self) -> [u8; 40] {
        let mut bytes = [0; 40];
        bytes[..32].copy_from_slice(&self.digest);
        bytes[32..].copy_from_slice(&self.length.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == 40, "invalid Blob reference length");
        let digest = bytes[..32].try_into()?;
        let length = u64::from_be_bytes(bytes[32..].try_into()?);
        ensure!(
            digest != [0; 32] && length <= 8 * 1024 * 1024,
            "invalid Blob reference"
        );
        Ok(Self { digest, length })
    }
}

fn exists(connection: &Connection) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_blobs')",
        [],
        |row| row.get(0),
    )?)
}

fn install(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS __sys_raft_blobs (
        digest BLOB PRIMARY KEY CHECK(typeof(digest)='blob' AND length(digest)=32),
        body BLOB NOT NULL CHECK(typeof(body)='blob')
    ) WITHOUT ROWID",
    )?;
    Ok(())
}

fn inventory(connection: &Connection, limits: RaftBlobLimits) -> Result<(usize, usize)> {
    let (count, bytes): (i64, i64) = connection.query_row(
        "SELECT count(*), coalesce(sum(length(body)),0) FROM __sys_raft_blobs",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let count = usize::try_from(count)?;
    let bytes = usize::try_from(bytes)?;
    ensure!(
        count <= limits.max_objects && bytes <= limits.max_total_bytes,
        "Blob inventory exceeds declared bounds"
    );
    Ok((count, bytes))
}

pub(crate) fn put_blob(
    connection: &Connection,
    limits: RaftBlobLimits,
    body: &[u8],
) -> Result<RaftBlobReference> {
    limits.validate()?;
    ensure!(
        body.len() <= limits.max_blob_bytes,
        cellar_logic::consensus::CommandFailure::Overloaded
    );
    let reference = RaftBlobReference {
        digest: Sha256::digest(body).into(),
        length: body.len() as u64,
    };
    install(connection)?;
    let (count, bytes) = inventory(connection, limits)?;
    let present: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM __sys_raft_blobs WHERE digest=?1)",
        [reference.digest.as_slice()],
        |row| row.get(0),
    )?;
    if present {
        ensure!(
            get_blob(connection, limits, reference)? == body,
            "conflicting immutable Blob"
        );
        return Ok(reference);
    }
    ensure!(
        count < limits.max_objects
            && bytes
                .checked_add(body.len())
                .is_some_and(|n| n <= limits.max_total_bytes),
        cellar_logic::consensus::CommandFailure::Overloaded
    );
    connection.execute(
        "INSERT INTO __sys_raft_blobs(digest,body) VALUES(?1,?2)",
        params![reference.digest.as_slice(), body],
    )?;
    Ok(reference)
}

pub(crate) fn get_blob(
    connection: &Connection,
    limits: RaftBlobLimits,
    reference: RaftBlobReference,
) -> Result<Vec<u8>> {
    ensure!(
        reference.length <= limits.max_blob_bytes as u64,
        "Blob exceeds object bound"
    );
    ensure!(exists(connection)?, "Blob is absent");
    let length: Option<i64> = connection
        .query_row(
            "SELECT length(body) FROM __sys_raft_blobs WHERE digest=?1",
            [reference.digest.as_slice()],
            |row| row.get(0),
        )
        .optional()?;
    ensure!(
        length.and_then(|n| u64::try_from(n).ok()) == Some(reference.length),
        "Blob is absent or has conflicting length"
    );
    let bytes: Vec<u8> = connection.query_row(
        "SELECT body FROM __sys_raft_blobs WHERE digest=?1",
        [reference.digest.as_slice()],
        |row| row.get(0),
    )?;
    ensure!(
        <[u8; 32]>::from(Sha256::digest(&bytes)) == reference.digest,
        "Blob digest mismatch"
    );
    Ok(bytes)
}

pub(crate) fn capture_dependencies(
    image: &PublishedImage,
    limits: Option<RaftBlobLimits>,
    services: cellar_ltx::generation::ReadServices,
    archive: ArchiveLimits,
) -> Result<Vec<ArchiveDependency>> {
    let Some(limits) = limits else {
        return Ok(vec![]);
    };
    let database = crate::storage::open_immutable_generation(
        image.pin(),
        crate::storage::ImmutableQueryOptions {
            services,
            sqlite_vec: false,
        },
    )?;
    let connection = database.connection();
    if !exists(connection)? {
        return Ok(vec![]);
    }
    let (count, bytes) = inventory(connection, limits)?;
    ensure!(
        count <= archive.max_dependencies && bytes <= archive.max_capture_bytes,
        "Blob closure exceeds archive bounds"
    );
    // Check every length before allocating bodies. The pinned generation and
    // read-only connection cannot change between this pass and the body pass.
    let mut statement =
        connection.prepare("SELECT digest,length(body) FROM __sys_raft_blobs ORDER BY digest")?;
    let references = statement
        .query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
        })?
        .map(|row| {
            let (digest, length) = row?;
            let reference = RaftBlobReference {
                digest: digest
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid stored Blob digest"))?,
                length: u64::try_from(length)?,
            };
            ensure!(
                reference.length <= limits.max_blob_bytes as u64,
                "stored Blob exceeds object bound"
            );
            Ok(reference)
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(references.len() == count, "Blob inventory changed");
    references
        .into_iter()
        .map(|reference| get_blob(connection, limits, reference).map(ArchiveDependency))
        .collect::<Result<Vec<_>>>()
        .context("capture exact committed Blob closure")
}
