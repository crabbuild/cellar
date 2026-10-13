//! URL patterns with the retained parser and Rust ECMAScript regex execution.
use super::Error;
pub use quirks::UrlPatternInit;
pub use quirks::{UrlPattern as ParsedPattern, UrlPatternComponent as ParsedComponent};
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use std::{collections::BTreeMap, sync::Arc};
use urlpattern::quirks;
pub use urlpattern::UrlPatternOptions;

const KEYS: [&str; 8] = [
    "protocol", "username", "password", "hostname", "port", "pathname", "search", "hash",
];

/// A string or dictionary after Web IDL USVString conversion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UrlPatternInput {
    String(String),
    Init(UrlPatternInit),
}

impl Default for UrlPatternInput {
    fn default() -> Self {
        Self::Init(UrlPatternInit::default())
    }
}

impl From<&str> for UrlPatternInput {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<String> for UrlPatternInput {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<UrlPatternInit> for UrlPatternInput {
    fn from(value: UrlPatternInit) -> Self {
        Self::Init(value)
    }
}

impl From<&UrlPatternInput> for UrlPatternInput {
    fn from(value: &UrlPatternInput) -> Self {
        value.clone()
    }
}

impl UrlPatternInput {
    pub fn from_utf16(value: &[u16]) -> Self {
        Self::String(String::from_utf16_lossy(value))
    }

    fn into_quirks(self) -> quirks::StringOrInit {
        match self {
            Self::String(value) => quirks::StringOrInit::String(value),
            Self::Init(value) => quirks::StringOrInit::Init(value),
        }
    }
}

/// Dictionary members before USVString conversion, including lone surrogates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UrlPatternInitUtf16 {
    pub protocol: Option<Vec<u16>>,
    pub username: Option<Vec<u16>>,
    pub password: Option<Vec<u16>>,
    pub hostname: Option<Vec<u16>>,
    pub port: Option<Vec<u16>>,
    pub pathname: Option<Vec<u16>>,
    pub search: Option<Vec<u16>>,
    pub hash: Option<Vec<u16>>,
    pub base_url: Option<Vec<u16>>,
}

impl From<UrlPatternInitUtf16> for UrlPatternInput {
    fn from(value: UrlPatternInitUtf16) -> Self {
        let usv = |value: Option<Vec<u16>>| value.map(|value| String::from_utf16_lossy(&value));
        Self::Init(UrlPatternInit {
            protocol: usv(value.protocol),
            username: usv(value.username),
            password: usv(value.password),
            hostname: usv(value.hostname),
            port: usv(value.port),
            pathname: usv(value.pathname),
            search: usv(value.search),
            hash: usv(value.hash),
            base_url: usv(value.base_url),
        })
    }
}

/// Shared construction seam used by the transitional JS adapter.
pub fn parse_components(
    input: UrlPatternInput,
    base_url: Option<&str>,
    options: UrlPatternOptions,
) -> Result<ParsedPattern, Error> {
    quirks::process_construct_pattern_input(input.into_quirks(), base_url)
        .and_then(|init| quirks::parse_pattern(init, options))
        .map_err(|error| Error::type_error(error.to_string()))
}

/// Shared match-input canonicalization. Invalid URLs produce a non-match.
pub fn match_values(
    input: UrlPatternInput,
    base_url: Option<&str>,
) -> Result<Option<[String; 8]>, Error> {
    let Some((input, _)) = quirks::process_match_input(input.into_quirks(), base_url)
        .map_err(|error| Error::type_error(error.to_string()))?
    else {
        return Ok(None);
    };
    Ok(quirks::parse_match_input(input).map(|value| {
        [
            value.protocol,
            value.username,
            value.password,
            value.hostname,
            value.port,
            value.pathname,
            value.search,
            value.hash,
        ]
    }))
}

#[derive(Debug)]
struct PatternData {
    components: [ParsedComponent; 8],
    regexps: [regress::Regex; 8],
    has_regexp_groups: bool,
}

fn component_error(key: &str, expression: &str, flags: &str, error: regress::Error) -> Error {
    // These diagnostics can reach the compiler after the retained Rust parser
    // accepts syntax (such as verbose groups or Rust-specific escape codes)
    // that ECMAScript rejects. Keep the adapter's exception context and wording.
    let reason = match error.text.as_str() {
        "Invalid group modifier" => "Invalid group",
        "Invalid character escape" => "Invalid escape",
        _ => &error.text,
    };
    let flags = if flags == "ui" { "iu" } else { flags };
    Error::type_error(format!(
        "Invalid {key} pattern: Invalid regular expression: /{expression}/{flags}: {reason}"
    ))
}

/// Immutable compiled patterns can be shared across workers and activations.
#[derive(Clone, Debug)]
pub struct UrlPattern(Arc<PatternData>);

impl UrlPattern {
    pub fn new(
        input: impl Into<UrlPatternInput>,
        base_url: Option<&str>,
        options: UrlPatternOptions,
    ) -> Result<Self, Error> {
        let flags = if options.ignore_case { "ui" } else { "u" };
        let parsed = parse_components(input.into(), base_url, options)?;
        let components = [
            parsed.protocol,
            parsed.username,
            parsed.password,
            parsed.hostname,
            parsed.port,
            parsed.pathname,
            parsed.search,
            parsed.hash,
        ];
        let mut compiled = Vec::with_capacity(8);
        for (key, component) in KEYS.into_iter().zip(&components) {
            compiled.push(
                regress::Regex::with_flags(&component.regexp_string, flags).map_err(|error| {
                    component_error(key, &component.regexp_string, flags, error)
                })?,
            );
        }
        Ok(Self(Arc::new(PatternData {
            components,
            regexps: compiled.try_into().expect("eight URL components"),
            has_regexp_groups: parsed.has_regexp_groups,
        })))
    }

    pub fn new_utf16(
        input: impl Into<UrlPatternInput>,
        base_url: Option<&[u16]>,
        options: UrlPatternOptions,
    ) -> Result<Self, Error> {
        let base = base_url.map(String::from_utf16_lossy);
        Self::new(input, base.as_deref(), options)
    }

    pub fn protocol(&self) -> &str {
        &self.0.components[0].pattern_string
    }

    pub fn username(&self) -> &str {
        &self.0.components[1].pattern_string
    }

    pub fn password(&self) -> &str {
        &self.0.components[2].pattern_string
    }

    pub fn hostname(&self) -> &str {
        &self.0.components[3].pattern_string
    }

    pub fn port(&self) -> &str {
        &self.0.components[4].pattern_string
    }

    pub fn pathname(&self) -> &str {
        &self.0.components[5].pattern_string
    }

    pub fn search(&self) -> &str {
        &self.0.components[6].pattern_string
    }

    pub fn hash(&self) -> &str {
        &self.0.components[7].pattern_string
    }

    pub fn has_regexp_groups(&self) -> bool {
        self.0.has_regexp_groups
    }

    pub fn test(
        &self,
        input: impl Into<UrlPatternInput>,
        base_url: Option<&str>,
    ) -> Result<bool, Error> {
        let Some(values) = match_values(input.into(), base_url)? else {
            return Ok(false);
        };
        Ok(self
            .0
            .regexps
            .iter()
            .zip(values)
            .all(|(regexp, value)| regexp.find(&value).is_some()))
    }

    pub fn test_utf16(
        &self,
        input: impl Into<UrlPatternInput>,
        base_url: Option<&[u16]>,
    ) -> Result<bool, Error> {
        let base = base_url.map(String::from_utf16_lossy);
        self.test(input, base.as_deref())
    }

    pub fn exec(
        &self,
        input: impl Into<UrlPatternInput>,
        base_url: Option<&str>,
    ) -> Result<Option<UrlPatternResult>, Error> {
        let input = input.into();
        let Some(values) = match_values(input.clone(), base_url)? else {
            return Ok(None);
        };
        let mut results = Vec::with_capacity(8);
        for ((component, regexp), value) in
            self.0.components.iter().zip(&self.0.regexps).zip(values)
        {
            let Some(found) = regexp.find(&value) else {
                return Ok(None);
            };
            let groups = component
                .group_name_list
                .iter()
                .zip(found.captures)
                .filter_map(|(name, capture)| {
                    // The retained adapter assigns to a plain JS object. Its inherited
                    // __proto__ setter refuses string/undefined values without an own key.
                    (name != "__proto__")
                        .then(|| (name.clone(), capture.map(|range| value[range].to_owned())))
                })
                .collect();
            results.push(UrlPatternComponentResult {
                input: value,
                groups,
            });
        }
        let [protocol, username, password, hostname, port, pathname, search, hash]: [_; 8] =
            results.try_into().expect("eight URL component results");
        let mut inputs = vec![input];
        if let Some(base) = base_url {
            inputs.push(UrlPatternInput::String(base.into()));
        }
        Ok(Some(UrlPatternResult {
            inputs,
            protocol,
            username,
            password,
            hostname,
            port,
            pathname,
            search,
            hash,
        }))
    }

    pub fn exec_utf16(
        &self,
        input: impl Into<UrlPatternInput>,
        base_url: Option<&[u16]>,
    ) -> Result<Option<UrlPatternResult>, Error> {
        let base = base_url.map(String::from_utf16_lossy);
        self.exec(input, base.as_deref())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UrlPatternResult {
    pub inputs: Vec<UrlPatternInput>,
    pub protocol: UrlPatternComponentResult,
    pub username: UrlPatternComponentResult,
    pub password: UrlPatternComponentResult,
    pub hostname: UrlPatternComponentResult,
    pub port: UrlPatternComponentResult,
    pub pathname: UrlPatternComponentResult,
    pub search: UrlPatternComponentResult,
    pub hash: UrlPatternComponentResult,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UrlPatternComponentResult {
    pub input: String,
    /// None is an unmatched, present JS property whose value is undefined.
    #[serde(serialize_with = "serialize_groups")]
    pub groups: BTreeMap<String, Option<String>>,
}

fn serialize_groups<S: Serializer>(
    groups: &BTreeMap<String, Option<String>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut output = serializer.serialize_map(None)?;
    // JSON.stringify omits undefined object properties, while Rust callers can
    // still distinguish an unmatched group from a group absent in the pattern.
    for (name, value) in groups {
        if let Some(value) = value {
            output.serialize_entry(name, value)?;
        }
    }
    output.end()
}
