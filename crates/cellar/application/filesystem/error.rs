// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use std::fmt;

/// Symbolic POSIX errors, independent of the host platform's integer values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[allow(non_camel_case_types)]
pub enum Errno {
    ENOENT,
    EEXIST,
    ENOTDIR,
    EISDIR,
    ENOTEMPTY,
    EBADF,
    EINVAL,
    EOVERFLOW,
    EOPNOTSUPP,
    ENAMETOOLONG,
    EACCES,
    EPERM,
    EROFS,
    EFBIG,
    EDQUOT,
    ENOSPC,
    EMFILE,
    ENFILE,
    EXDEV,
    ESTALE,
    EIO,
    EBUSY,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Outcome {
    NotApplied,
    LocallyCommitted,
    Unknown,
}

/// A filesystem failure retains the underlying typed Cellar/SQLite cause.
#[derive(Debug)]
pub struct Error {
    pub errno: Errno,
    pub code: &'static str,
    /// Public SDK operation that failed. Constructors without an SDK boundary
    /// use `filesystem`; nested helpers retain the caller's operation name.
    pub operation: &'static str,
    pub outcome: Outcome,
    /// Bytes completed by a looping helper before this error. The count is
    /// local progress; an unknown outcome may include further unconfirmed work.
    pub completed_bytes: usize,
    message: String,
    source: Option<anyhow::Error>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Construct a refusal proven to precede this operation's filesystem effect.
    pub fn new(errno: Errno, message: impl Into<String>) -> Self {
        Self {
            errno,
            code: "Filesystem",
            operation: "filesystem",
            outcome: Outcome::NotApplied,
            completed_bytes: 0,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn affinity() -> Self {
        Self {
            code: "TransactionAffinity",
            ..Self::new(
                Errno::EBADF,
                "filesystem view does not own the current transaction",
            )
        }
    }

    pub(crate) fn with_code(mut self, code: &'static str) -> Self {
        self.code = code;
        self
    }

    pub(crate) fn in_operation(mut self, operation: &'static str) -> Self {
        self.operation = operation;
        self
    }

    pub(crate) fn after_progress(mut self, bytes: usize, committed: bool) -> Self {
        self.completed_bytes = self.completed_bytes.saturating_add(bytes);
        if bytes > 0 && self.outcome == Outcome::NotApplied {
            self.outcome = if committed {
                Outcome::LocallyCommitted
            } else {
                Outcome::Unknown
            };
        }
        self
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}: {}", self.operation, self.errno, self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|error| error.as_ref())
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        let error = match error.downcast::<Self>() {
            Ok(error) => return error,
            Err(error) => error,
        };
        let full = error.chain().any(|cause| matches!(cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(failure, _)) if failure.code == rusqlite::ErrorCode::DiskFull));
        Self {
            errno: if full { Errno::ENOSPC } else { Errno::EIO },
            code: "Storage",
            operation: "filesystem",
            outcome: Outcome::Unknown,
            completed_bytes: 0,
            message: "cell filesystem storage operation failed".into(),
            source: Some(error),
        }
    }
}

pub(super) fn operation<T>(name: &'static str, call: impl FnOnce() -> Result<T>) -> Result<T> {
    call().map_err(|error| error.in_operation(name))
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        anyhow::Error::new(error).into()
    }
}
