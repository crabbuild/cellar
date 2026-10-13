//! The retained URL facade over the shared WHATWG parser. Setter writes are
//! deliberately explicit: they retain the existing adapter's component rules.
use super::Error;
use std::sync::{Arc, Mutex};

type Pair = (Vec<u16>, Vec<u16>);

fn utf16(value: &str) -> Vec<u16> {
    value.encode_utf16().collect()
}

fn usv(value: &[u16]) -> Vec<u16> {
    String::from_utf16_lossy(value).encode_utf16().collect()
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct UrlRecord {
    pub protocol: String,
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub pathname: String,
    pub search: String,
    pub hash: String,
    pub href: String,
}

/// Used by the native facade and the transitional JS parser op. Invalid bases
/// are ignored if the input itself is absolute, matching the retained host.
pub fn parse_record(input: &str, base: Option<&str>) -> Result<UrlRecord, Error> {
    let base = base.and_then(|base| ::url::Url::parse(base).ok());
    let parsed = ::url::Url::options()
        .base_url(base.as_ref())
        .parse(input)
        .map_err(|error| Error::type_error(format!("Invalid URL: {error}")))?;
    Ok(UrlRecord {
        protocol: parsed.scheme().into(),
        username: parsed.username().into(),
        password: parsed.password().unwrap_or("").into(),
        host: parsed.host_str().unwrap_or("").into(),
        port: parsed
            .port()
            .map(|port| port.to_string())
            .unwrap_or_default(),
        pathname: parsed.path().into(),
        search: parsed.query().unwrap_or("").into(),
        hash: parsed.fragment().unwrap_or("").into(),
        href: parsed.as_str().into(),
    })
}

const PROTOCOL: usize = 0;
const USERNAME: usize = 1;
const PASSWORD: usize = 2;
const HOSTNAME: usize = 3;
const PORT: usize = 4;
const PATHNAME: usize = 5;
const SEARCH: usize = 6;
const HASH: usize = 7;

#[derive(Clone, Debug)]
struct Parts {
    fields: [Vec<u16>; 8],
    href: Vec<u16>,
}

impl From<UrlRecord> for Parts {
    fn from(record: UrlRecord) -> Self {
        let marked = |mark, value: &str| {
            if value.is_empty() {
                Vec::new()
            } else {
                [vec![mark], utf16(value)].concat()
            }
        };
        Self {
            fields: [
                utf16(&(record.protocol + ":")),
                utf16(&record.username),
                utf16(&record.password),
                utf16(&record.host),
                utf16(&record.port),
                utf16(&record.pathname),
                marked(63, &record.search),
                marked(35, &record.hash),
            ],
            href: utf16(&record.href),
        }
    }
}

impl Parts {
    fn rebuild(&mut self) {
        let mut href = self.fields[PROTOCOL].clone();
        href.extend([47, 47]);
        if !self.fields[USERNAME].is_empty() {
            href.extend(&self.fields[USERNAME]);
            if !self.fields[PASSWORD].is_empty() {
                href.push(58);
                href.extend(&self.fields[PASSWORD]);
            }
            href.push(64);
        }
        href.extend(&self.fields[HOSTNAME]);
        if !self.fields[PORT].is_empty() {
            href.push(58);
            href.extend(&self.fields[PORT]);
        }
        for index in [PATHNAME, SEARCH, HASH] {
            href.extend(&self.fields[index]);
        }
        self.href = href;
    }

    fn host(&self) -> Vec<u16> {
        let mut host = self.fields[HOSTNAME].clone();
        if !self.fields[PORT].is_empty() {
            host.push(58);
            host.extend(&self.fields[PORT]);
        }
        host
    }

    fn default_port(&self) -> Option<&'static str> {
        match String::from_utf16_lossy(&self.fields[PROTOCOL]).as_str() {
            "ftp:" => Some("21"),
            "http:" | "ws:" => Some("80"),
            "https:" | "wss:" => Some("443"),
            _ => None,
        }
    }

    fn clear_default(&mut self) {
        if self
            .default_port()
            .is_some_and(|port| self.fields[PORT] == utf16(port))
        {
            self.fields[PORT].clear();
        }
    }
}

#[derive(Debug)]
struct State {
    parts: Option<Parts>,
    params: Option<Vec<SearchEntry>>,
}

impl State {
    fn sync(&mut self) -> Result<(), Error> {
        if let Some(parts) = &mut self.parts {
            let query = serialize(self.params.as_ref().expect("materialized params"))?;
            parts.fields[SEARCH] = if query.is_empty() {
                Vec::new()
            } else {
                utf16(&format!("?{query}"))
            };
            parts.rebuild();
        }
        Ok(())
    }
}

/// Cloning a URL shares its identity and its lazily materialized search list.
#[derive(Clone, Debug)]
pub struct Url(Arc<Mutex<State>>);
macro_rules! component {
    ($name:ident,$raw:ident,$index:ident) => {
        pub fn $name(&self) -> String {
            String::from_utf16_lossy(&self.$raw())
        }
        pub fn $raw(&self) -> Vec<u16> {
            self.0.lock().unwrap().parts.as_ref().unwrap().fields[$index].clone()
        }
    };
}

impl Url {
    pub fn new(input: &str, base: Option<&str>) -> Result<Self, Error> {
        Ok(Self(Arc::new(Mutex::new(State {
            parts: Some(parse_record(input, base)?.into()),
            params: None,
        }))))
    }

    pub fn new_utf16(input: &[u16], base: Option<&[u16]>) -> Result<Self, Error> {
        Self::new(
            &String::from_utf16_lossy(input),
            base.map(String::from_utf16_lossy).as_deref(),
        )
    }

    pub fn can_parse(input: &str, base: Option<&str>) -> bool {
        parse_record(input, base).is_ok()
    }

    pub fn parse(input: &str, base: Option<&str>) -> Option<Self> {
        Self::new(input, base).ok()
    }
    component!(protocol, protocol_utf16, PROTOCOL);
    component!(username, username_utf16, USERNAME);
    component!(password, password_utf16, PASSWORD);
    component!(hostname, hostname_utf16, HOSTNAME);
    component!(port, port_utf16, PORT);
    component!(pathname, pathname_utf16, PATHNAME);
    component!(search, search_utf16, SEARCH);
    component!(hash, hash_utf16, HASH);

    pub fn href(&self) -> String {
        String::from_utf16_lossy(&self.href_utf16())
    }

    pub fn href_utf16(&self) -> Vec<u16> {
        self.0.lock().unwrap().parts.as_ref().unwrap().href.clone()
    }

    pub fn host(&self) -> String {
        String::from_utf16_lossy(&self.host_utf16())
    }

    pub fn host_utf16(&self) -> Vec<u16> {
        self.0.lock().unwrap().parts.as_ref().unwrap().host()
    }

    pub fn origin(&self) -> String {
        String::from_utf16_lossy(&self.origin_utf16())
    }

    pub fn origin_utf16(&self) -> Vec<u16> {
        let mut parts = self.0.lock().unwrap().parts.as_ref().unwrap().clone();
        loop {
            match String::from_utf16_lossy(&parts.fields[PROTOCOL]).as_str() {
                "http:" | "https:" | "ws:" | "wss:" | "ftp:" => {
                    return [parts.fields[PROTOCOL].clone(), vec![47, 47], parts.host()].concat()
                }
                "blob:" => {
                    let Ok(record) =
                        parse_record(&String::from_utf16_lossy(&parts.fields[PATHNAME]), None)
                    else {
                        return utf16("null");
                    };
                    parts = record.into();
                }
                _ => return utf16("null"),
            }
        }
    }

    fn change(&self, callback: impl FnOnce(&mut Parts)) {
        let mut state = self.0.lock().unwrap();
        let parts = state.parts.as_mut().unwrap();
        callback(parts);
        parts.rebuild();
    }

    pub fn set_protocol(&self, value: &str) {
        self.set_protocol_utf16(&utf16(value));
    }

    pub fn set_protocol_utf16(&self, value: &[u16]) {
        self.change(|parts| {
            parts.fields[PROTOCOL] = value.to_vec();
            if value.last() != Some(&58) {
                parts.fields[PROTOCOL].push(58);
            }
            parts.clear_default();
        });
    }

    pub fn set_username(&self, value: &str) {
        self.set_username_utf16(&utf16(value));
    }

    pub fn set_username_utf16(&self, value: &[u16]) {
        self.change(|parts| parts.fields[USERNAME] = value.to_vec());
    }

    pub fn set_password(&self, value: &str) {
        self.set_password_utf16(&utf16(value));
    }

    pub fn set_password_utf16(&self, value: &[u16]) {
        self.change(|parts| parts.fields[PASSWORD] = value.to_vec());
    }

    pub fn set_hostname(&self, value: &str) {
        self.set_hostname_utf16(&utf16(value));
    }

    pub fn set_hostname_utf16(&self, value: &[u16]) {
        self.change(|parts| parts.fields[HOSTNAME] = value.to_vec());
    }

    pub fn set_host(&self, value: &str) {
        self.set_host_utf16(&utf16(value));
    }

    pub fn set_host_utf16(&self, value: &[u16]) {
        self.change(|parts| {
            let colon = value.iter().position(|c| *c == 58);
            match colon {
                None => {
                    parts.fields[HOSTNAME] = value.to_vec();
                    parts.fields[PORT].clear();
                }
                Some(colon) => {
                    parts.fields[HOSTNAME] = value[..colon].to_vec();
                    parts.fields[PORT] = value[colon + 1..].to_vec();
                    parts.clear_default();
                }
            }
        });
    }

    pub fn set_port(&self, value: &str) {
        self.set_port_utf16(&utf16(value));
    }

    pub fn set_port_utf16(&self, value: &[u16]) {
        self.change(|parts| {
            if value.is_empty() || value.iter().all(|c| matches!(c, 48..=57)) {
                parts.fields[PORT] = value.to_vec();
                parts.clear_default();
            }
        });
    }

    pub fn set_pathname(&self, value: &str) {
        self.set_pathname_utf16(&utf16(value));
    }

    pub fn set_pathname_utf16(&self, value: &[u16]) {
        self.change(|parts| {
            let mut value: Vec<u16> = value
                .iter()
                .copied()
                .filter(|c| !matches!(c, 9 | 10 | 13))
                .collect();
            if !value.is_empty() && value[0] != 47 {
                value.insert(0, 47);
            }
            parts.fields[PATHNAME] = value;
        });
    }

    pub fn set_hash(&self, value: &str) {
        self.set_hash_utf16(&utf16(value));
    }

    pub fn set_hash_utf16(&self, value: &[u16]) {
        self.change(|parts| {
            let mut value: Vec<u16> = value
                .iter()
                .copied()
                .filter(|c| !matches!(c, 9 | 10 | 13))
                .collect();
            if !value.is_empty() && value[0] != 35 {
                value.insert(0, 35);
            }
            parts.fields[HASH] = value;
        });
    }

    pub fn set_search(&self, value: &str) -> Result<(), Error> {
        self.set_search_utf16(&utf16(value))
    }

    pub fn set_search_utf16(&self, value: &[u16]) -> Result<(), Error> {
        let value: Vec<u16> = value
            .iter()
            .copied()
            .filter(|c| !matches!(c, 9 | 10 | 13))
            .collect();
        let value = value.strip_prefix(&[63]).unwrap_or(&value);
        let encoded = encode_query(value)?;
        let mut state = self.0.lock().unwrap();
        let search = if encoded.is_empty() {
            Vec::new()
        } else {
            utf16(&format!("?{encoded}"))
        };
        state.parts.as_mut().unwrap().fields[SEARCH] = search.clone();
        if state.params.is_some() {
            state.params = Some(parse_query(&search));
        }
        state.parts.as_mut().unwrap().rebuild();
        Ok(())
    }

    pub fn set_href(&self, value: &str) -> Result<(), Error> {
        self.set_href_utf16(&utf16(value))
    }

    pub fn set_href_utf16(&self, value: &[u16]) -> Result<(), Error> {
        let parts: Parts = parse_record(&String::from_utf16_lossy(value), None)?.into();
        let mut state = self.0.lock().unwrap();
        if state.params.is_some() {
            state.params = Some(parse_query(&parts.fields[SEARCH]));
        }
        state.parts = Some(parts);
        Ok(())
    }

    pub fn search_params(&self) -> SearchParams {
        let mut state = self.0.lock().unwrap();
        if state.params.is_none() {
            state.params = Some(parse_query(&state.parts.as_ref().unwrap().fields[SEARCH]));
        }
        SearchParams(self.0.clone())
    }

    pub fn copy(&self) -> Self {
        let state = self.0.lock().unwrap();
        Self(Arc::new(Mutex::new(State {
            parts: state.parts.clone(),
            params: None,
        })))
    }
}

impl std::fmt::Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.href())
    }
}

fn parse_query(value: &[u16]) -> Vec<SearchEntry> {
    let value = value.strip_prefix(&[63]).unwrap_or(value);
    ::url::form_urlencoded::parse(String::from_utf16_lossy(value).as_bytes())
        .map(|(name, value)| SearchEntry::new((utf16(&name), utf16(&value))))
        .collect()
}

fn serialize(params: &[SearchEntry]) -> Result<String, Error> {
    let mut serializer = ::url::form_urlencoded::Serializer::new(String::new());
    for entry in params {
        let (name, value) = entry.snapshot_utf16();
        let name = String::from_utf16(&name).map_err(|_| Error::uri_error())?;
        let value = String::from_utf16(&value).map_err(|_| Error::uri_error())?;
        serializer.append_pair(&name, &value);
    }
    Ok(serializer.finish())
}

fn encode_query(value: &[u16]) -> Result<String, Error> {
    let mut output = String::new();
    for character in char::decode_utf16(value.iter().copied()) {
        let character = character.map_err(|_| Error::uri_error())?;
        let code = character as u32;
        if code <= 32 || matches!(code, 34 | 35 | 60 | 62 | 127) {
            output.push_str(&format!("%{code:02X}"));
        } else if code > 126 {
            let mut buffer = [0; 4];
            for byte in character.encode_utf8(&mut buffer).bytes() {
                output.push_str(&format!("%{byte:02X}"));
            }
        } else {
            output.push(character);
        }
    }
    Ok(output)
}

#[derive(Clone, Debug)]
pub struct SearchParams(Arc<Mutex<State>>);

impl Default for SearchParams {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(State {
            parts: None,
            params: Some(Vec::new()),
        })))
    }
}

impl SearchParams {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_query(query: &str) -> Self {
        Self::from_query_utf16(&utf16(query))
    }

    pub fn from_query_utf16(query: &[u16]) -> Self {
        Self(Arc::new(Mutex::new(State {
            parts: None,
            params: Some(parse_query(query)),
        })))
    }

    pub fn from_pairs<N: AsRef<str>, V: AsRef<str>>(
        pairs: impl IntoIterator<Item = (N, V)>,
    ) -> Self {
        Self::from_pairs_utf16(
            pairs
                .into_iter()
                .map(|(name, value)| (utf16(name.as_ref()), utf16(value.as_ref()))),
        )
    }

    pub fn from_pairs_utf16(pairs: impl IntoIterator<Item = Pair>) -> Self {
        Self(Arc::new(Mutex::new(State {
            parts: None,
            params: Some(
                pairs
                    .into_iter()
                    .map(|(name, value)| SearchEntry::new((usv(&name), usv(&value))))
                    .collect(),
            ),
        })))
    }

    pub fn from_record_utf16(pairs: impl IntoIterator<Item = Pair>) -> Self {
        let mut list = Vec::<Pair>::new();
        for (name, value) in pairs {
            let name = usv(&name);
            let value = usv(&value);
            if let Some(entry) = list.iter_mut().find(|(key, _)| *key == name) {
                entry.1 = value;
            } else {
                list.push((name, value));
            }
        }
        Self(Arc::new(Mutex::new(State {
            parts: None,
            params: Some(list.into_iter().map(SearchEntry::new).collect()),
        })))
    }

    pub fn from_params(source: &Self) -> Self {
        Self::from_pairs_utf16(source.entries_utf16())
    }

    pub fn size(&self) -> usize {
        self.0.lock().unwrap().params.as_ref().unwrap().len()
    }

    pub fn get(&self, name: &str) -> Option<String> {
        self.get_utf16(&utf16(name))
            .map(|value| String::from_utf16_lossy(&value))
    }

    pub fn get_utf16(&self, name: &[u16]) -> Option<Vec<u16>> {
        self.0
            .lock()
            .unwrap()
            .params
            .as_ref()
            .unwrap()
            .iter()
            .find_map(|entry| {
                let pair = entry.0.lock().unwrap();
                (pair.0 == name).then(|| pair.1.clone())
            })
    }

    pub fn get_all(&self, name: &str) -> Vec<String> {
        self.get_all_utf16(&utf16(name))
            .iter()
            .map(|value| String::from_utf16_lossy(value))
            .collect()
    }

    pub fn get_all_utf16(&self, name: &[u16]) -> Vec<Vec<u16>> {
        self.0
            .lock()
            .unwrap()
            .params
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|entry| {
                let pair = entry.0.lock().unwrap();
                (pair.0 == name).then(|| pair.1.clone())
            })
            .collect()
    }

    pub fn has(&self, name: &str, value: Option<&str>) -> bool {
        self.has_utf16(&utf16(name), value.map(utf16).as_deref())
    }

    pub fn has_utf16(&self, name: &[u16], value: Option<&[u16]>) -> bool {
        self.0
            .lock()
            .unwrap()
            .params
            .as_ref()
            .unwrap()
            .iter()
            .any(|entry| {
                let pair = entry.0.lock().unwrap();
                pair.0 == name && value.is_none_or(|value| pair.1 == value)
            })
    }

    fn update(&self, callback: impl FnOnce(&mut Vec<SearchEntry>)) -> Result<(), Error> {
        let mut state = self.0.lock().unwrap();
        callback(state.params.as_mut().unwrap());
        state.sync()
    }

    pub fn append(&self, name: &str, value: &str) -> Result<(), Error> {
        self.append_utf16(&utf16(name), &utf16(value))
    }

    pub fn append_utf16(&self, name: &[u16], value: &[u16]) -> Result<(), Error> {
        self.update(|params| params.push(SearchEntry::new((name.to_vec(), value.to_vec()))))
    }

    pub fn set(&self, name: &str, value: &str) -> Result<(), Error> {
        self.set_utf16(&utf16(name), &utf16(value))
    }

    pub fn set_utf16(&self, name: &[u16], value: &[u16]) -> Result<(), Error> {
        self.update(|params| {
            let mut found = false;
            params.retain(|entry| {
                let mut pair = entry.0.lock().unwrap();
                if pair.0 != name {
                    return true;
                }
                if found {
                    return false;
                }
                pair.1 = value.to_vec();
                found = true;
                true
            });
            if !found {
                params.push(SearchEntry::new((name.to_vec(), value.to_vec())));
            }
        })
    }

    pub fn delete(&self, name: &str, value: Option<&str>) -> Result<(), Error> {
        self.delete_utf16(&utf16(name), value.map(utf16).as_deref())
    }

    pub fn delete_utf16(&self, name: &[u16], value: Option<&[u16]>) -> Result<(), Error> {
        self.update(|params| {
            params.retain(|entry| {
                let pair = entry.0.lock().unwrap();
                !(pair.0 == name && value.is_none_or(|value| pair.1 == value))
            })
        })
    }

    pub fn sort(&self) -> Result<(), Error> {
        self.update(|params| params.sort_by_cached_key(SearchEntry::name_utf16))
    }

    pub fn serialize(&self) -> Result<String, Error> {
        serialize(self.0.lock().unwrap().params.as_ref().unwrap())
    }

    /// Handles expose the retained adapter's aliased entry arrays. Direct
    /// entry writes do not synchronize a bound URL until a list mutator runs.
    pub fn entry_handles(&self) -> SearchEntryHandles {
        SearchEntryHandles {
            params: self.clone(),
            index: 0,
            ended: false,
        }
    }

    pub fn entries_utf16(&self) -> SearchEntriesUtf16 {
        SearchEntriesUtf16(self.entry_handles())
    }

    pub fn entries(&self) -> SearchEntries {
        SearchEntries(self.entries_utf16())
    }

    pub fn keys(&self) -> impl Iterator<Item = String> {
        self.entries().map(|(name, _)| name)
    }

    pub fn values(&self) -> impl Iterator<Item = String> {
        self.entries().map(|(_, value)| value)
    }

    pub fn for_each(&self, mut callback: impl FnMut(String, String, &Self)) {
        for (name, value) in self.entries() {
            callback(value, name, self);
        }
    }

    pub fn try_for_each_utf16<E>(
        &self,
        mut callback: impl FnMut(Vec<u16>, Vec<u16>, &Self) -> Result<(), E>,
    ) -> Result<(), E> {
        for (name, value) in self.entries_utf16() {
            callback(value, name, self)?;
        }
        Ok(())
    }
}

/// An owned alias to a search entry. Removed or replaced entries remain
/// readable and writable as detached values, matching escaped entry arrays.
#[derive(Clone, Debug)]
pub struct SearchEntry(Arc<Mutex<Pair>>);

impl SearchEntry {
    fn new(pair: Pair) -> Self {
        Self(Arc::new(Mutex::new(pair)))
    }

    pub fn snapshot_utf16(&self) -> Pair {
        self.0.lock().unwrap().clone()
    }

    pub fn name_utf16(&self) -> Vec<u16> {
        self.0.lock().unwrap().0.clone()
    }

    pub fn value_utf16(&self) -> Vec<u16> {
        self.0.lock().unwrap().1.clone()
    }

    pub fn name(&self) -> String {
        String::from_utf16_lossy(&self.name_utf16())
    }

    pub fn value(&self) -> String {
        String::from_utf16_lossy(&self.value_utf16())
    }

    pub fn set_name(&self, name: &str) {
        self.set_name_utf16(&utf16(name));
    }

    pub fn set_name_utf16(&self, name: &[u16]) {
        self.0.lock().unwrap().0 = name.to_vec();
    }

    pub fn set_value(&self, value: &str) {
        self.set_value_utf16(&utf16(value));
    }

    pub fn set_value_utf16(&self, value: &[u16]) {
        self.0.lock().unwrap().1 = value.to_vec();
    }
}

pub struct SearchEntryHandles {
    params: SearchParams,
    index: usize,
    ended: bool,
}

impl Iterator for SearchEntryHandles {
    type Item = SearchEntry;

    fn next(&mut self) -> Option<SearchEntry> {
        if self.ended {
            return None;
        }
        let entry = self
            .params
            .0
            .lock()
            .unwrap()
            .params
            .as_ref()
            .unwrap()
            .get(self.index)
            .cloned();
        if entry.is_some() {
            self.index += 1;
        } else {
            self.ended = true;
        }
        entry
    }
}

impl std::iter::FusedIterator for SearchEntryHandles {}

pub struct SearchEntriesUtf16(SearchEntryHandles);

impl Iterator for SearchEntriesUtf16 {
    type Item = Pair;

    fn next(&mut self) -> Option<Pair> {
        self.0.next().map(|entry| entry.snapshot_utf16())
    }
}

impl std::iter::FusedIterator for SearchEntriesUtf16 {}

pub struct SearchEntries(SearchEntriesUtf16);

impl Iterator for SearchEntries {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(name, value)| {
            (
                String::from_utf16_lossy(&name),
                String::from_utf16_lossy(&value),
            )
        })
    }
}

impl std::iter::FusedIterator for SearchEntries {}
