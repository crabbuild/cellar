//! The retained always-miss Cache API, including validation and body draining.

use super::{http, Context, Request};
use anyhow::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidUrl,
    NonGet,
    PartialResponse,
    VaryStar,
    UsedBody,
    NameTooLong,
}

impl Error {
    pub fn name(&self) -> &'static str {
        "TypeError"
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidUrl => "Invalid URL. Cache API keys must be fully-qualified, valid URLs.",
            Self::NonGet => "Cannot cache response to non-GET request.",
            Self::PartialResponse => {
                "Cannot cache response to a range request (206 Partial Content)."
            }
            Self::VaryStar => "Cannot cache response with 'Vary: *' header.",
            Self::UsedBody => "Cannot cache a response whose body is already used.",
            Self::NameTooLong => "Cache name is too long.",
        })
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug)]
pub struct CacheStorage {
    _private: (),
}

#[derive(Clone, Debug)]
pub struct Cache {
    _private: (),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MatchOptions {
    pub ignore_method: bool,
}

pub enum Key<'a> {
    Url(&'a str),
    Request(&'a Request),
}

impl<'a> From<&'a str> for Key<'a> {
    fn from(url: &'a str) -> Self {
        Self::Url(url)
    }
}

impl<'a> From<&'a String> for Key<'a> {
    fn from(url: &'a String) -> Self {
        Self::Url(url)
    }
}

impl<'a> From<&'a Request> for Key<'a> {
    fn from(request: &'a Request) -> Self {
        Self::Request(request)
    }
}

impl Key<'_> {
    fn validate(&self) -> Result<String> {
        let (url, method) = match self {
            Self::Url(url) => (*url, "GET"),
            Self::Request(request) => (request.url.as_str(), request.method.as_str()),
        };
        if !url::Url::parse(url)
            .ok()
            .is_some_and(|url| matches!(url.scheme(), "http" | "https"))
        {
            return Err(Error::InvalidUrl.into());
        }
        http::normalize_method(method)
    }
}

impl CacheStorage {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }

    pub fn default(&self) -> Cache {
        Cache { _private: () }
    }

    pub fn open(&self, name: &str) -> Result<Cache> {
        if name.encode_utf16().count() >= 1024 {
            return Err(Error::NameTooLong.into());
        }
        Ok(self.default())
    }

    pub fn open_utf16(&self, name: &[u16]) -> Result<Cache> {
        if name.len() >= 1024 {
            return Err(Error::NameTooLong.into());
        }
        Ok(self.default())
    }
}

impl Cache {
    pub async fn put<'a>(
        &self,
        context: &Context,
        key: impl Into<Key<'a>>,
        response: &mut http::Response,
    ) -> Result<()> {
        context.check_turn()?;
        let method = key.into().validate()?;
        if method != "GET" {
            return Err(Error::NonGet.into());
        }
        if response.status() == 206 {
            return Err(Error::PartialResponse.into());
        }
        if response
            .headers()
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("vary"))
            .any(|(_, value)| value.contains('*'))
        {
            return Err(Error::VaryStar.into());
        }
        if response.body_used() {
            return Err(Error::UsedBody.into());
        }
        if response.has_body() {
            response.drain(context).await?;
        }
        Ok(())
    }

    pub async fn match_request<'a>(
        &self,
        context: &Context,
        key: impl Into<Key<'a>>,
        _options: MatchOptions,
    ) -> Result<Option<http::Response>> {
        context.check_turn()?;
        key.into().validate()?;
        Ok(None)
    }

    pub async fn delete<'a>(
        &self,
        context: &Context,
        key: impl Into<Key<'a>>,
        _options: MatchOptions,
    ) -> Result<bool> {
        context.check_turn()?;
        key.into().validate()?;
        Ok(false)
    }
}
