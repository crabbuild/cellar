// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical logical history for signed effects; physical placement is absent.
use anyhow::{ensure, Result};
use cellar_logic::{
    cell_group::{CellHistory, CellId},
    consensus::{ClusterId, IncarnationId, NamespaceId},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireHistory {
    cluster: String,
    namespace: String,
    cell: String,
    incarnation: String,
}

impl From<&CellHistory> for WireHistory {
    fn from(history: &CellHistory) -> Self {
        Self {
            cluster: hex::encode(history.cluster.bytes()),
            namespace: hex::encode(history.namespace.bytes()),
            cell: history.cell.as_str().to_owned(),
            incarnation: hex::encode(history.incarnation.bytes()),
        }
    }
}

impl WireHistory {
    pub(super) fn decode(self) -> Result<CellHistory> {
        Ok(CellHistory {
            cluster: ClusterId::new(identity(&self.cluster)?)?,
            namespace: NamespaceId::new(identity(&self.namespace)?)?,
            cell: CellId::new(self.cell)?,
            incarnation: IncarnationId::new(identity(&self.incarnation)?)?,
        })
    }
}

fn identity(value: &str) -> Result<[u8; 16]> {
    ensure!(
        value.len() == 32
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "noncanonical effect identity"
    );
    let mut bytes = [0; 16];
    hex::decode_to_slice(value, &mut bytes)?;
    Ok(bytes)
}
