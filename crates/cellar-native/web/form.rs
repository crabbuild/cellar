//! Ordered live form entries and the retained byte-oriented MIME protocol.
use super::{Blob, Error, File, SearchParams};
use regex::Regex;
use std::sync::{Arc, Mutex, OnceLock};

type Entry = (Vec<u16>, FormValue);

#[derive(Clone, Debug)]
pub enum FormValue {
    Text(Vec<u16>),
    File(File),
}

impl FormValue {
    pub fn text(value: &str) -> Self {
        Self::Text(value.encode_utf16().collect())
    }
}

#[derive(Clone, Debug, Default)]
pub struct FormData(Arc<Mutex<Vec<Entry>>>);

impl FormData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(&self, name: &str, value: FormValue) {
        self.append_utf16(&units(name), value);
    }

    pub fn append_utf16(&self, name: &[u16], value: FormValue) {
        self.0.lock().unwrap().push((name.to_vec(), value));
    }

    pub fn append_blob(&self, name: &str, blob: &Blob, filename: Option<&str>) {
        self.append_blob_utf16(&units(name), blob, filename.map(units).as_deref());
    }

    pub fn append_blob_utf16(&self, name: &[u16], blob: &Blob, filename: Option<&[u16]>) {
        self.append_utf16(
            name,
            FormValue::File(File::from_blob_utf16(
                blob.clone(),
                filename.unwrap_or(&[98, 108, 111, 98]),
                None,
            )),
        );
    }

    pub fn append_file(&self, name: &str, file: &File, filename: Option<&str>) {
        self.append_file_utf16(&units(name), file, filename.map(units).as_deref());
    }

    pub fn append_file_utf16(&self, name: &[u16], file: &File, filename: Option<&[u16]>) {
        self.append_utf16(name, FormValue::File(rename(file, filename)));
    }

    pub fn set(&self, name: &str, value: FormValue) {
        self.set_utf16(&units(name), value);
    }

    pub fn set_utf16(&self, name: &[u16], value: FormValue) {
        let mut entries = self.0.lock().unwrap();
        let mut first = true;
        entries.retain_mut(|(key, entry)| {
            if key != name {
                return true;
            }
            if !first {
                return false;
            }
            *entry = value.clone();
            first = false;
            true
        });
        if first {
            entries.push((name.to_vec(), value));
        }
    }

    pub fn set_blob(&self, name: &str, blob: &Blob, filename: Option<&str>) {
        self.set(
            name,
            FormValue::File(File::from_blob(
                blob.clone(),
                filename.unwrap_or("blob"),
                None,
            )),
        );
    }

    pub fn set_file(&self, name: &str, file: &File, filename: Option<&str>) {
        self.set(
            name,
            FormValue::File(rename(file, filename.map(units).as_deref())),
        );
    }

    pub fn get(&self, name: &str) -> Option<FormValue> {
        self.get_utf16(&units(name))
    }

    pub fn get_utf16(&self, name: &[u16]) -> Option<FormValue> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    }

    pub fn get_all(&self, name: &str) -> Vec<FormValue> {
        self.get_all_utf16(&units(name))
    }

    pub fn get_all_utf16(&self, name: &[u16]) -> Vec<FormValue> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .collect()
    }

    pub fn has(&self, name: &str) -> bool {
        self.has_utf16(&units(name))
    }

    pub fn has_utf16(&self, name: &[u16]) -> bool {
        self.0.lock().unwrap().iter().any(|(key, _)| key == name)
    }

    pub fn delete(&self, name: &str) {
        self.delete_utf16(&units(name));
    }

    pub fn delete_utf16(&self, name: &[u16]) {
        self.0.lock().unwrap().retain(|(key, _)| key != name);
    }

    pub fn entries(&self) -> FormEntries {
        FormEntries {
            form: self.clone(),
            index: 0,
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = Vec<u16>> {
        self.entries().map(|(key, _)| key)
    }

    pub fn values(&self) -> impl Iterator<Item = FormValue> {
        self.entries().map(|(_, value)| value)
    }

    pub fn for_each(&self, mut callback: impl FnMut(FormValue, Vec<u16>, &Self)) {
        for (key, value) in self.entries() {
            callback(value, key, self);
        }
    }

    pub fn try_for_each<E>(
        &self,
        mut callback: impl FnMut(FormValue, Vec<u16>, &Self) -> Result<(), E>,
    ) -> Result<(), E> {
        for (key, value) in self.entries() {
            callback(value, key, self)?;
        }
        Ok(())
    }

    pub fn copy(&self) -> Self {
        Self(Arc::new(Mutex::new(self.0.lock().unwrap().clone())))
    }

    pub fn encode(&self) -> Result<MultipartBody, Error> {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|error| Error::generic_error(error.to_string()))?;
        self.encode_with_random(random)
    }

    /// Supply the boundary's random bytes for deterministic protocol checks.
    pub fn encode_with_random(&self, random: [u8; 16]) -> Result<MultipartBody, Error> {
        let mut boundary = String::from("----cellarFormBoundary");
        for byte in random {
            use std::fmt::Write;
            write!(&mut boundary, "{byte:02x}").unwrap();
        }
        let entries = self.0.lock().unwrap().clone();
        let mut bytes = Vec::new();
        for (name, value) in entries {
            let mut header = format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{}\"",
                mime_escape(&name)?
            );
            if let FormValue::File(file) = &value {
                header.push_str(&format!(
                    "; filename=\"{}\"\r\nContent-Type: {}",
                    mime_escape(file.name_utf16())?,
                    if file.content_type().is_empty() {
                        "application/octet-stream"
                    } else {
                        file.content_type()
                    }
                ));
            }
            header.push_str("\r\n\r\n");
            bytes.extend(header.as_bytes());
            match value {
                FormValue::Text(text) => bytes.extend(crate::encoding::encode_utf16(&text)),
                FormValue::File(file) => bytes.extend(file.blob().as_bytes()),
            }
            bytes.extend(b"\r\n");
        }
        bytes.extend(format!("--{boundary}--\r\n").as_bytes());
        Ok(MultipartBody {
            bytes,
            content_type: format!("multipart/form-data; boundary={boundary}"),
            boundary,
        })
    }

    pub fn parse(bytes: &[u8], content_type: &str) -> Result<Self, Error> {
        Self::parse_utf16(bytes, &units(content_type))
    }

    pub fn parse_utf16(bytes: &[u8], content_type: &[u16]) -> Result<Self, Error> {
        let ct = String::from_utf16_lossy(content_type);
        let patterns = patterns();
        if patterns.urlencoded.is_match(&ct) {
            if let Some(capture) = patterns.charset.captures(&ct) {
                let value = capture.get(1).unwrap();
                let start = ct[..value.start()].encode_utf16().count();
                let end = ct[..value.end()].encode_utf16().count();
                let charset = trim_utf16(&content_type[start..end]);
                if ![units("utf-8"), units("utf8")].iter().any(|valid| {
                    charset
                        .iter()
                        .map(|unit| {
                            if (65..=90).contains(unit) {
                                unit + 32
                            } else {
                                *unit
                            }
                        })
                        .eq(valid.iter().copied())
                }) {
                    let mut message = units("Unsupported charset \"");
                    message.extend(charset);
                    message.extend(units("\". FormData can only parse UTF-8 encoded bodies."));
                    return Err(Error::type_error_utf16(message));
                }
            }
            let form = Self::new();
            for (name, value) in SearchParams::from_query(&decode(bytes)?).entries_utf16() {
                form.append_utf16(&name, FormValue::Text(value));
            }
            return Ok(form);
        }
        if !patterns.multipart.is_match(&ct) {
            return Err(Error::type_error("Unrecognized Content-Type header value. FormData can only parse the following MIME types: multipart/form-data, application/x-www-form-urlencoded"));
        }
        let found = patterns.boundary.captures(&ct).ok_or_else(|| {
            Error::type_error(
                "No boundary was found in the multipart/form-data Content-Type header.",
            )
        })?;
        let boundary = trim(found.get(1).or_else(|| found.get(2)).unwrap().as_str());
        parse_multipart(bytes, format!("--{boundary}").as_bytes())
    }
}

fn rename(file: &File, filename: Option<&[u16]>) -> File {
    filename
        .map(|name| File::from_blob_utf16(file.blob().clone(), name, Some(file.last_modified())))
        .unwrap_or_else(|| file.clone())
}

pub struct FormEntries {
    form: FormData,
    index: usize,
}

impl Iterator for FormEntries {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        let entry = self.form.0.lock().unwrap().get(self.index).cloned();
        if entry.is_some() {
            self.index += 1;
        }
        entry
    }
}

#[derive(Clone, Debug)]
pub struct MultipartBody {
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub boundary: String,
}

fn units(value: &str) -> Vec<u16> {
    value.encode_utf16().collect()
}

fn decode(bytes: &[u8]) -> Result<String, Error> {
    super::blob::decode_text(bytes).map_err(|error| Error::generic_error(error.to_string()))
}

fn whitespace(unit: u32) -> bool {
    matches!(unit,9..=13|32|0xa0|0x1680|0x2000..=0x200a|0x2028|0x2029|0x202f|0x205f|0x3000|0xfeff)
}

fn trim(value: &str) -> &str {
    value.trim_matches(|c: char| whitespace(c as u32))
}

fn trim_utf16(value: &[u16]) -> &[u16] {
    let start = value
        .iter()
        .position(|unit| !whitespace(u32::from(*unit)))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|unit| !whitespace(u32::from(*unit)))
        .map(|i| i + 1)
        .unwrap_or(start);
    &value[start..end]
}

fn mime_escape(value: &[u16]) -> Result<String, Error> {
    if value.last() == Some(&92) {
        return Err(Error::type_error(
            "Name or filename can't end with backslash",
        ));
    }
    let mut escaped = Vec::new();
    for unit in value {
        match unit {
            10 => escaped.extend(units("%0A")),
            13 => escaped.extend(units("%0D")),
            34 => escaped.extend(units("%22")),
            unit => escaped.push(*unit),
        }
    }
    Ok(String::from_utf16_lossy(&escaped))
}

struct Patterns {
    urlencoded: Regex,
    charset: Regex,
    multipart: Regex,
    boundary: Regex,
    name: Regex,
    filename: Regex,
}

fn patterns() -> &'static Patterns {
    static PATTERNS: OnceLock<Patterns> = OnceLock::new();
    PATTERNS.get_or_init(||{
        const SPACE:&str=r"\t-\r \u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}";
        let parameter=|key:&str|Regex::new(&format!(r#"(?:^|;)[{SPACE}]*(?i-u:{key})[{SPACE}]*=[{SPACE}]*(?:"((?:[^"\\]|\\[^\n\r\u{{2028}}\u{{2029}}])*)"|([^;]*))"#)).unwrap();
        Patterns {
            urlencoded:Regex::new(&format!(r"^[{SPACE}]*(?i-u:application/x-www-form-urlencoded)")).unwrap(),
            multipart:Regex::new(&format!(r"^[{SPACE}]*(?i-u:multipart/form-data)")).unwrap(),
            charset:Regex::new(&format!(r#";[{SPACE}]*(?i-u:charset)[{SPACE}]*=[{SPACE}]*"?([^";]+)"#)).unwrap(),
            boundary:Regex::new(&format!(r#"(?i-u:boundary)[{SPACE}]*=[{SPACE}]*(?:"([^"]*)"|([^;]+))"#)).unwrap(),
            name:parameter("name"),filename:parameter("filename"),
        }
    })
}

fn delimiter(bytes: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if from > bytes.len() {
        return None;
    }
    bytes[from..]
        .windows(needle.len())
        .enumerate()
        .find_map(|(offset, window)| {
            let at = from + offset;
            (window == needle && (at == 0 || bytes[at - 1] == 10)).then_some(at)
        })
}

fn line_break(bytes: &[u8], at: usize) -> usize {
    if bytes.get(at) == Some(&13) && bytes.get(at + 1) == Some(&10) {
        2
    } else if bytes.get(at) == Some(&10) {
        1
    } else {
        0
    }
}

fn parameter(pattern: &Regex, disposition: &str) -> Result<Option<String>, Error> {
    let Some(capture) = pattern.captures(disposition) else {
        return Ok(None);
    };
    let raw = capture
        .get(1)
        .map(|value| value.as_str())
        .unwrap_or_else(|| trim(capture.get(2).unwrap().as_str()));
    if raw.chars().rev().take_while(|c| *c == '\\').count() % 2 == 1 {
        return Err(Error::type_error(
            "Name or filename can't end with backslash",
        ));
    }
    let mut result = String::new();
    let mut characters = raw.chars().peekable();
    while let Some(character) = characters.next() {
        if character == '\\'
            && characters
                .peek()
                .is_some_and(|next| !matches!(next, '\n' | '\r' | '\u{2028}' | '\u{2029}'))
        {
            result.push(characters.next().unwrap());
        } else {
            result.push(character);
        }
    }
    Ok(Some(result))
}

fn parse_multipart(bytes: &[u8], needle: &[u8]) -> Result<FormData, Error> {
    let form = FormData::new();
    let mut cursor = delimiter(bytes, needle, 0).ok_or_else(|| {
        Error::type_error("No boundary delimiter was found in the multipart/form-data body.")
    })? + needle.len();
    loop {
        if bytes.get(cursor) == Some(&45) && bytes.get(cursor + 1) == Some(&45) {
            break;
        }
        let line = line_break(bytes, cursor);
        if line == 0 {
            return Err(Error::type_error(
                "A boundary delimiter must be followed by CRLF, LF, or \"--\".",
            ));
        }
        cursor += line;
        let start = cursor;
        let next = delimiter(bytes, needle, start).ok_or_else(|| {
            Error::type_error(
                "The multipart/form-data body ended without a closing boundary delimiter.",
            )
        })?;
        let mut end = next;
        if end > start && bytes[end - 1] == 10 {
            end -= 1;
            if end > start && bytes[end - 1] == 13 {
                end -= 1;
            }
        }
        cursor = next + needle.len();
        let (header_end, body_start) = (start..end)
            .find_map(|at| {
                let first = line_break(bytes, at);
                if first == 0 {
                    return None;
                }
                let second = line_break(bytes, at + first);
                (second != 0).then_some((at, at + first + second))
            })
            .ok_or_else(|| {
                Error::type_error("A FormData part's headers must be terminated by a blank line.")
            })?;
        let headers = decode(&bytes[start..header_end])?;
        let body = if body_start >= end {
            &[][..]
        } else {
            &bytes[body_start..end]
        };
        let mut disposition = None;
        let mut kind = "";
        for line in headers.split('\n') {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let name = trim(name);
            let value = trim(value);
            if name.eq_ignore_ascii_case("content-disposition") {
                disposition = Some(value);
            } else if name.eq_ignore_ascii_case("content-type") {
                kind = value;
            }
        }
        let disposition = disposition.ok_or_else(|| {
            Error::type_error("Content-Disposition header is required for each FormData part.")
        })?;
        let disposition_kind = trim(disposition.split(';').next().unwrap());
        if !disposition_kind.eq_ignore_ascii_case("form-data") {
            return Err(Error::type_error(format!("Content-Disposition header for FormData part must have the value \"form-data\", possibly followed by parameters. Got: \"{disposition_kind}\"")));
        }
        let name = parameter(&patterns().name, disposition)?.ok_or_else(|| {
            Error::type_error(
                "Content-Disposition header for FormData part must have a name parameter.",
            )
        })?;
        match parameter(&patterns().filename, disposition)? {
            None => form.append(&name, FormValue::text(&decode(body)?)),
            Some(filename) => form.append_file(
                &name,
                &File::from_blob(Blob::from_bytes(body, kind)?, &filename, None),
                None,
            ),
        }
    }
    Ok(form)
}
