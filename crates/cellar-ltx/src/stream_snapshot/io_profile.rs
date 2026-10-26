// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit codec memory policy. Profiles change physical I/O batching only;
//! encoded identities, frame coverage and destination verification are unchanged.

/// Bounded codec buffers, selected under the caller's memory admission.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IoProfile {
    /// Retain the default 64 KiB buffers and provider operation sizes.
    #[default]
    Compact,
    /// Use 2 MiB sequential buffers; existing providers fall back to 64 KiB I/O.
    Buffered,
}

impl IoProfile {
    pub(crate) const fn buffer_bytes(self) -> usize {
        match self {
            Self::Compact => super::BUFFER_BYTES,
            Self::Buffered => 2 * 1024 * 1024,
        }
    }

    /// Conservative bound for codec-owned heap buffers, independent of image
    /// size. The caller must also admit its source and transport windows.
    /// Selecting a profile supplies no memory reservation or integrity proof.
    pub const fn max_working_bytes(self) -> usize {
        6 * self.buffer_bytes() + 4 * super::BUFFER_BYTES
    }
}

#[derive(Default)]
pub(crate) struct IoOptions {
    pub profile: IoProfile,
    pub activity: Option<crate::host::IoActivity>,
}
