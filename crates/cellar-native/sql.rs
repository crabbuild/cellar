//! Owned SQLite values retain integers, non-finite reals and binary data.

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}
impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Real(value)
    }
}
impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Integer(value as i64)
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}
impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

mod binding;
pub use binding::*;
