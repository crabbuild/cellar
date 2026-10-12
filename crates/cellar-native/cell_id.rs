// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Stable cell identity derivation and authentication, without an engine.
use crate::web::Error;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// The authenticated identity format shared with deployed JavaScript cells.
pub fn perform(namespace: &str, operation: &str, input: &str) -> Result<String, Error> {
    match operation {
        "name" => Ok(encode_hex(&for_name(namespace, input))),
        "validate" => {
            let id = decode_hex(input).ok_or_else(|| {
                Error::type_error("Invalid Durable Object ID: must be 64 hex digits")
            })?;
            validate(namespace, &id)?;
            Ok(encode_hex(&id))
        }
        "unique" => {
            use hmac::Mac;
            let mut id = [0; 32];
            getrandom::fill(&mut id[..16])
                .map_err(|_| Error::type_error("secure random generation failed"))?;
            let digest = hmac(&key(namespace), &id[..16]).finalize().into_bytes();
            id[16..].copy_from_slice(&digest[..16]);
            Ok(encode_hex(&id))
        }
        _ => Err(Error::type_error("unknown Durable Object ID operation")),
    }
}
fn validate(namespace: &str, id: &[u8; 32]) -> Result<(), Error> {
    use hmac::Mac;
    hmac(&key(namespace), &id[..16])
        .verify_truncated_left(&id[16..])
        .map_err(|_| Error::type_error("Durable Object ID is not valid for this namespace"))
}

#[derive(Clone, Debug)]
pub struct Namespace {
    class: Arc<str>,
    key: Arc<str>,
}
impl Namespace {
    /// A value factory, not authority to invoke a cell. Runtime adapters mint
    /// their callable namespaces from configured bindings and supplied handles.
    pub fn new(class: impl Into<Arc<str>>, key: impl Into<Arc<str>>) -> Self {
        Self {
            class: class.into(),
            key: key.into(),
        }
    }
    fn id(&self, value: [u8; 32], name: Option<Arc<[u16]>>) -> Id {
        Id {
            class: self.class.clone(),
            value,
            name,
        }
    }
    pub fn id_from_name(&self, name: &str) -> Id {
        self.id(
            for_name(&self.key, name),
            Some(name.encode_utf16().collect::<Vec<_>>().into()),
        )
    }
    pub fn id_from_name_utf16(&self, name: &[u16]) -> Id {
        // The original host op hashes the V8 string's UTF-8 replacement view.
        self.id(
            for_name(&self.key, &String::from_utf16_lossy(name)),
            Some(Arc::from(name)),
        )
    }
    pub fn id_from_string(&self, value: &str) -> Result<Id, Error> {
        let value = perform(&self.key, "validate", value)?;
        Ok(self.id(decode_hex(&value).expect("validated hex"), None))
    }
    pub fn new_unique_id(&self) -> Result<Id, Error> {
        let value = perform(&self.key, "unique", "")?;
        Ok(self.id(decode_hex(&value).expect("generated hex"), None))
    }
    /// Existing ID values are accepted by class, as in the original façade.
    /// Parsing an external hex string separately authenticates its namespace.
    pub fn accepts(&self, id: &Id) -> Result<(), Error> {
        if id.class != self.class {
            return Err(Error::type_error(
                "Durable Object ID is not valid for this namespace",
            ));
        }
        Ok(())
    }
    pub fn jurisdiction(&self, value: Option<&str>) -> Result<Self, Error> {
        match value {
            None => Ok(self.clone()),
            Some(_) => Err(Error::generic_error(
                "Jurisdiction restrictions are not implemented",
            )),
        }
    }
}
#[derive(Clone, Debug)]
pub struct Id {
    class: Arc<str>,
    value: [u8; 32],
    name: Option<Arc<[u16]>>,
}
impl Id {
    pub fn name_utf16(&self) -> Option<&[u16]> {
        self.name.as_deref()
    }
    pub fn name(&self) -> Option<String> {
        self.name.as_deref().map(String::from_utf16_lossy)
    }
    /// The existing host persists at most 1024 UTF-8 bytes of routing name.
    /// The full name always participates in identity derivation.
    pub fn dispatch_name(&self) -> Option<String> {
        self.name().filter(|name| name.len() <= 1024)
    }
    pub fn scope(&self) -> String {
        format!("{}:{}", self.class, self)
    }
    pub fn equals(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&encode_hex(&self.value))
    }
}
impl PartialEq for Id {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}
impl Eq for Id {}

thread_local! {
    static DO_ID_KEYS: RefCell<HashMap<String, [u8; 32]>> = RefCell::new(HashMap::new());
}

fn key(namespace_key: &str) -> [u8; 32] {
    DO_ID_KEYS.with(|keys| {
        if let Some(key) = keys.borrow().get(namespace_key) {
            return *key;
        }
        use sha2::Digest;
        let key: [u8; 32] = sha2::Sha256::digest(namespace_key.as_bytes()).into();
        keys.borrow_mut().insert(namespace_key.to_string(), key);
        key
    })
}

fn hmac(key: &[u8; 32], input: &[u8]) -> hmac::Hmac<sha2::Sha256> {
    use hmac::Mac;
    let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(key)
        .expect("SHA-256 accepts any HMAC key");
    mac.update(input);
    mac
}

/// Ids already derived on this thread, keyed by namespace and name.
///
/// `getByName` costs two HMAC-SHA256 rounds, and a profile of `/c/hello`
/// found them among the largest terms the stateless path does not have —
/// the same name resolving to the same id, recomputed on every request.
///
/// Per thread and unsynchronised, which is sound because the derivation is
/// a pure function of its inputs: a worker that has not seen a name pays
/// for it once, and no answer can differ between threads. Bounded because
/// names come from the application, and cleared wholesale at the cap rather
/// than evicted one at a time — a cache this cheap to refill does not earn
/// an LRU.
const DO_ID_CACHE_MAX: usize = 4096;

pub fn for_name(namespace_key: &str, name: &str) -> [u8; 32] {
    use hmac::Mac;

    thread_local! {
        static IDS: RefCell<HashMap<(String, String), [u8; 32]>> =
            RefCell::new(HashMap::new());
    }
    let cached = IDS.with(|ids| {
        ids.borrow()
            .get(&(namespace_key.to_string(), name.to_string()))
            .copied()
    });
    if let Some(id) = cached {
        return id;
    }

    let key = key(namespace_key);
    let mut id = [0_u8; 32];
    let digest = hmac(&key, name.as_bytes()).finalize().into_bytes();
    id[..16].copy_from_slice(&digest[..16]);
    let digest = hmac(&key, &id[..16]).finalize().into_bytes();
    id[16..].copy_from_slice(&digest[..16]);

    IDS.with(|ids| {
        let mut ids = ids.borrow_mut();
        if ids.len() >= DO_ID_CACHE_MAX {
            ids.clear();
        }
        ids.insert((namespace_key.to_string(), name.to_string()), id);
    });
    id
}

pub fn encode_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub fn decode_hex(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let nibble = |byte| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        };
        output[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(output)
}
