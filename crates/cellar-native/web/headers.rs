// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::Error;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

type Pair = (String, Vec<u16>);

#[derive(Clone, Debug)]
struct Entry {
    name: String,
    lower: String,
    value: Vec<u16>,
}

#[derive(Default, Debug)]
struct State {
    list: Vec<Entry>,
    pairs: Option<Arc<Vec<Pair>>>,
    immutable: bool,
}

impl State {
    fn check_mutable(&self) -> Result<(), Error> {
        if self.immutable {
            Err(Error::type_error("Can't modify immutable headers."))
        } else {
            Ok(())
        }
    }

    fn push(&mut self, mut name: String, value: Vec<u16>) {
        let lower = name.to_ascii_lowercase();
        if let Some(entry) = self.list.iter().find(|entry| entry.lower == lower) {
            name.clone_from(&entry.name);
        }
        self.list.push(Entry { name, lower, value });
        self.pairs = None;
    }

    fn pairs(&mut self) -> Arc<Vec<Pair>> {
        if let Some(pairs) = &self.pairs {
            return pairs.clone();
        }
        let mut combined = BTreeMap::<String, Vec<Vec<u16>>>::new();
        for entry in &self.list {
            let values = combined.entry(entry.lower.clone()).or_default();
            if entry.lower == "set-cookie" || values.is_empty() {
                values.push(entry.value.clone());
            } else {
                values[0].extend([44, 32]);
                values[0].extend_from_slice(&entry.value);
            }
        }
        let pairs = Arc::<Vec<Pair>>::new(
            combined
                .into_iter()
                .flat_map(|(name, values)| {
                    values.into_iter().map(move |value| (name.clone(), value))
                })
                .collect(),
        );
        self.pairs = Some(pairs.clone());
        pairs
    }
}

fn name(value: &[u16]) -> Result<String, Error> {
    if value.is_empty()
        || value.iter().any(|value| {
            !matches!(value,33|35..=39|42|43|45|46|48..=57|65..=90|94..=122|124|126)
                || matches!(value, 91..=93)
        })
    {
        let mut message: Vec<u16> = "Invalid header name: ".encode_utf16().collect();
        message.extend(value);
        return Err(Error::type_error_utf16(message));
    }
    // Validated token characters are ASCII. The spelling remains on the wire.
    Ok(value.iter().map(|value| *value as u8 as char).collect())
}

fn value(value: &[u16]) -> Result<Vec<u16>, Error> {
    let start = value
        .iter()
        .position(|c| !matches!(c, 9 | 10 | 13 | 32))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|c| !matches!(c, 9 | 10 | 13 | 32))
        .map(|at| at + 1)
        .unwrap_or(start);
    let value = &value[start..end];
    if value.iter().any(|c| matches!(c, 0 | 10 | 13)) {
        return Err(Error::type_error("Invalid header value"));
    }
    Ok(value.to_vec())
}

/// A shared, live header list with a verbatim wire view and a sorted iteration
/// view. `Clone` is an alias; `copy_list` and `from_headers` are independent.
#[derive(Clone, Default, Debug)]
pub struct Headers(Arc<Mutex<State>>);

impl Headers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_pairs<N: AsRef<str>, V: AsRef<str>>(
        pairs: impl IntoIterator<Item = (N, V)>,
    ) -> Result<Self, Error> {
        let headers = Self::new();
        for (name, value) in pairs {
            headers.append(name.as_ref(), value.as_ref())?;
        }
        Ok(headers)
    }

    pub fn from_pairs_utf16(
        pairs: impl IntoIterator<Item = (Vec<u16>, Vec<u16>)>,
    ) -> Result<Self, Error> {
        let headers = Self::new();
        for (name, value) in pairs {
            headers.append_utf16(&name, &value)?;
        }
        Ok(headers)
    }

    /// The ordinary Headers constructor reads the source's iteration view.
    pub fn from_headers(source: &Self) -> Self {
        Self::from_pairs_utf16(
            source
                .entries_utf16()
                .map(|(name, value)| (name.encode_utf16().collect(), value)),
        )
        .expect("validated header list")
    }

    /// Request copying reads the raw list, preserving spelling and repeats.
    pub fn copy_list(&self) -> Self {
        let state = self.0.lock().unwrap();
        Self(Arc::new(Mutex::new(State {
            list: state.list.clone(),
            ..Default::default()
        })))
    }

    pub fn freeze(&self) {
        self.0.lock().unwrap().immutable = true;
    }

    pub fn is_immutable(&self) -> bool {
        self.0.lock().unwrap().immutable
    }

    pub fn append(&self, name: &str, value: &str) -> Result<(), Error> {
        self.append_utf16(
            &name.encode_utf16().collect::<Vec<_>>(),
            &value.encode_utf16().collect::<Vec<_>>(),
        )
    }

    pub fn append_utf16(&self, raw_name: &[u16], raw_value: &[u16]) -> Result<(), Error> {
        let mut state = self.0.lock().unwrap();
        state.check_mutable()?;
        let name = name(raw_name)?;
        let value = value(raw_value)?;
        state.push(name, value);
        Ok(())
    }

    pub fn set(&self, name: &str, value: &str) -> Result<(), Error> {
        self.set_utf16(
            &name.encode_utf16().collect::<Vec<_>>(),
            &value.encode_utf16().collect::<Vec<_>>(),
        )
    }

    pub fn set_utf16(&self, raw_name: &[u16], raw_value: &[u16]) -> Result<(), Error> {
        let mut state = self.0.lock().unwrap();
        state.check_mutable()?;
        let name = name(raw_name)?;
        let value = value(raw_value)?;
        let lower = name.to_ascii_lowercase();
        let mut added = false;
        state.list.retain_mut(|entry| {
            if entry.lower != lower {
                return true;
            }
            if added {
                return false;
            }
            entry.value.clone_from(&value);
            added = true;
            true
        });
        if !added {
            state.push(name, value);
        } else {
            state.pairs = None;
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Option<String>, Error> {
        Ok(self
            .get_utf16(&name.encode_utf16().collect::<Vec<_>>())?
            .map(|value| String::from_utf16_lossy(&value)))
    }

    pub fn get_utf16(&self, raw_name: &[u16]) -> Result<Option<Vec<u16>>, Error> {
        let lower = name(raw_name)?.to_ascii_lowercase();
        let state = self.0.lock().unwrap();
        let mut value = None::<Vec<u16>>;
        for entry in state.list.iter().filter(|entry| entry.lower == lower) {
            if let Some(value) = &mut value {
                value.extend([44, 32]);
                value.extend_from_slice(&entry.value)
            } else {
                value = Some(entry.value.clone())
            }
        }
        Ok(value)
    }

    pub fn has(&self, name: &str) -> Result<bool, Error> {
        self.has_utf16(&name.encode_utf16().collect::<Vec<_>>())
    }

    pub fn has_utf16(&self, raw_name: &[u16]) -> Result<bool, Error> {
        let lower = name(raw_name)?.to_ascii_lowercase();
        Ok(self
            .0
            .lock()
            .unwrap()
            .list
            .iter()
            .any(|entry| entry.lower == lower))
    }

    pub fn delete(&self, name: &str) -> Result<(), Error> {
        self.delete_utf16(&name.encode_utf16().collect::<Vec<_>>())
    }

    pub fn delete_utf16(&self, raw_name: &[u16]) -> Result<(), Error> {
        let mut state = self.0.lock().unwrap();
        state.check_mutable()?;
        let lower = name(raw_name)?.to_ascii_lowercase();
        state.list.retain(|entry| entry.lower != lower);
        state.pairs = None;
        Ok(())
    }

    pub fn get_set_cookie(&self) -> Vec<String> {
        self.get_set_cookie_utf16()
            .iter()
            .map(|value| String::from_utf16_lossy(value))
            .collect()
    }

    pub fn get_set_cookie_utf16(&self) -> Vec<Vec<u16>> {
        self.0
            .lock()
            .unwrap()
            .list
            .iter()
            .filter(|entry| entry.lower == "set-cookie")
            .map(|entry| entry.value.clone())
            .collect()
    }

    pub fn wire_utf16(&self) -> Vec<Pair> {
        self.0
            .lock()
            .unwrap()
            .list
            .iter()
            .map(|entry| (entry.name.clone(), entry.value.clone()))
            .collect()
    }

    /// An explicit UTF-8 projection of the raw list, with replacement for
    /// unpaired surrogates. Use the transport-specific conversions at boundaries.
    pub fn wire(&self) -> Vec<(String, String)> {
        self.wire_utf16()
            .into_iter()
            .map(|(name, value)| (name, String::from_utf16_lossy(&value)))
            .collect()
    }

    /// Request transport cannot represent an unpaired UTF-16 surrogate.
    /// Refuse it rather than silently changing a forwarded header's value.
    pub fn request_wire(&self) -> Result<Vec<(String, String)>, Error> {
        self.wire_utf16()
            .into_iter()
            .map(|(name, value)| {
                Ok((
                    name,
                    String::from_utf16(&value).map_err(|_| {
                        Error::type_error("Header value contains an unpaired surrogate.")
                    })?,
                ))
            })
            .collect()
    }

    /// The retained response serializer uses the iteration view. Its JSON
    /// bridge falls back to an empty list if any value is not representable.
    pub fn response_wire(&self) -> Vec<(String, String)> {
        self.0
            .lock()
            .unwrap()
            .pairs()
            .iter()
            .map(|(name, value)| {
                String::from_utf16(value)
                    .ok()
                    .map(|value| (name.clone(), value))
            })
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default()
    }

    pub fn entries_utf16(&self) -> HeaderEntriesUtf16 {
        HeaderEntriesUtf16 {
            headers: self.clone(),
            index: 0,
        }
    }

    pub fn entries(&self) -> HeaderEntries {
        HeaderEntries(self.entries_utf16())
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
        mut callback: impl FnMut(Vec<u16>, String, &Self) -> Result<(), E>,
    ) -> Result<(), E> {
        for (name, value) in self.entries_utf16() {
            callback(value, name, self)?;
        }
        Ok(())
    }
}

pub struct HeaderEntriesUtf16 {
    headers: Headers,
    index: usize,
}

impl Iterator for HeaderEntriesUtf16 {
    type Item = Pair;

    fn next(&mut self) -> Option<Pair> {
        let pair = self
            .headers
            .0
            .lock()
            .unwrap()
            .pairs()
            .get(self.index)
            .cloned();
        if pair.is_some() {
            self.index += 1;
        }
        pair
    }
}

pub struct HeaderEntries(HeaderEntriesUtf16);

impl Iterator for HeaderEntries {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(name, value)| (name, String::from_utf16_lossy(&value)))
    }
}
