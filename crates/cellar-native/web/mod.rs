//! Owned Web API values. Cloning a facade shares its live list; explicit copy
//! constructors create independent values. UTF-16 entry points preserve DOMStrings.
mod abort;
mod blob;
mod body;
mod clock;
mod event;
mod form;
mod headers;
pub mod pattern;
pub mod url;
pub use abort::{
    AbortController, AbortReason, AbortSignal, AbortWait, DomException, WeakAbortSignal,
};
pub use blob::{Blob, BlobOptions, BlobPart, BlobStream, File, FileOptions};
pub use body::{BodyInit, EncodedBody};
pub use clock::WallClockGuard;
pub use event::{
    CustomEvent, Event, EventDispatchError, EventInit, EventListener, EventTarget, Listener,
    ListenerOptions, SharedEventValue,
};
pub use form::{FormData, FormEntries, FormValue, MultipartBody};
pub use headers::{HeaderEntries, HeaderEntriesUtf16, Headers};
pub use pattern::{
    UrlPattern, UrlPatternComponentResult, UrlPatternInit, UrlPatternInitUtf16, UrlPatternInput,
    UrlPatternOptions, UrlPatternResult,
};
pub use url::{
    SearchEntries, SearchEntriesUtf16, SearchEntry, SearchEntryHandles, SearchParams, Url,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    name: &'static str,
    message: Vec<u16>,
}

impl Error {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn message_utf16(&self) -> &[u16] {
        &self.message
    }

    pub(crate) fn type_error(message: impl AsRef<str>) -> Self {
        Self::type_error_utf16(message.as_ref().encode_utf16().collect())
    }

    pub(crate) fn type_error_utf16(message: Vec<u16>) -> Self {
        Self {
            name: "TypeError",
            message,
        }
    }

    pub(crate) fn uri_error() -> Self {
        Self {
            name: "URIError",
            message: "URI malformed".encode_utf16().collect(),
        }
    }

    pub(crate) fn generic_error(message: impl AsRef<str>) -> Self {
        Self::generic_error_utf16(message.as_ref().encode_utf16().collect())
    }

    pub(crate) fn generic_error_utf16(message: Vec<u16>) -> Self {
        Self {
            name: "Error",
            message,
        }
    }

    pub(crate) fn range_error(message: impl AsRef<str>) -> Self {
        Self {
            name: "RangeError",
            message: message.as_ref().encode_utf16().collect(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&String::from_utf16_lossy(&self.message))
    }
}

impl std::error::Error for Error {}
