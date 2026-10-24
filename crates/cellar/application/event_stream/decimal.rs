// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Structured RPC numbers use binary64. Protocol integers travel as canonical
//! decimal strings so persistent sequence identity retains its full i64 range.

use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    let text = String::deserialize(deserializer)?;
    let value = text.parse::<i64>().map_err(serde::de::Error::custom)?;
    if text != value.to_string() {
        return Err(serde::de::Error::custom("noncanonical protocol integer"));
    }
    Ok(value)
}

pub mod optional {
    use super::*;

    pub fn serialize<S: Serializer>(value: &Option<i64>, serializer: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<i64>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|text| {
                let value = text.parse::<i64>().map_err(serde::de::Error::custom)?;
                if text != value.to_string() {
                    return Err(serde::de::Error::custom("noncanonical protocol integer"));
                }
                Ok(value)
            })
            .transpose()
    }
}
