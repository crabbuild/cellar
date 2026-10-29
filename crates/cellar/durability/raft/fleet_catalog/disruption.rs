// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One rooted slot serializes planned offline work across lifecycle modes.
use super::*;

pub(super) const SLOT_KEY: &str = "active-disruption";

pub(super) enum Disruption {
    Evacuation(evacuation::Record),
    Restart(maintenance::Record),
}

impl Disruption {
    fn decode(bytes: &[u8]) -> Result<Self> {
        // Each decoder validates framing, canonical encoding, digest and its
        // complete domain contract. An unknown domain never grants authority.
        if let Ok(record) = decode::<evacuation::Record>(evacuation::ADMISSION_KIND, bytes, 4096) {
            return Ok(Self::Evacuation(record));
        }
        Ok(Self::Restart(decode(maintenance::KIND, bytes, 4096)?))
    }

    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        match self {
            Self::Evacuation(record) => record.validate(catalog),
            Self::Restart(record) => record.validate(catalog),
        }
    }

    pub(super) fn operation(&self) -> [u8; 16] {
        match self {
            Self::Evacuation(record) => record.operation(),
            Self::Restart(record) => record.request.operation,
        }
    }

    pub(super) fn evacuation(self) -> Result<evacuation::Record> {
        match self {
            Self::Evacuation(record) => Ok(record),
            Self::Restart(record) => Err(FleetCatalogError::DisruptionInProgress {
                operation: record.request.operation,
            }),
        }
    }

    fn mutation(&self) -> Result<FleetMutation> {
        match self {
            Self::Evacuation(record) => record.mutation(),
            Self::Restart(record) => record.mutation(),
        }
    }

    fn check_basis(&self, snapshot: &FleetSnapshot) -> Result<()> {
        match self {
            Self::Evacuation(record) => record.check_basis(snapshot),
            Self::Restart(record) => record.check_basis(snapshot),
        }
    }
}

impl FleetCatalog {
    pub(super) async fn active_disruption(
        &self,
        snapshot: &FleetSnapshot,
    ) -> Result<Option<Disruption>> {
        let bytes = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], SLOT_KEY)
            .await?;
        match (snapshot.head.disruption, bytes) {
            (None, None) => Ok(None),
            (Some(digest), Some(bytes)) if hash(&bytes) == digest => {
                let record = Disruption::decode(&bytes)?;
                record.validate(self)?;
                let operation = self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &hex::encode(record.operation()),
                    )
                    .await?;
                if operation.as_deref() != Some(bytes.as_slice()) {
                    return Err(FleetCatalogError::Corrupt("disruption operation reference"));
                }
                match &record {
                    Disruption::Evacuation(record) => {
                        self.confirm_evacuation(snapshot, record).await?;
                    }
                    Disruption::Restart(record) => {
                        self.confirm_restart(snapshot, record).await?;
                    }
                }
                Ok(Some(record))
            }
            _ => Err(FleetCatalogError::Corrupt("disruption root/index mismatch")),
        }
    }

    // Only owned terminal paths may release a previously admitted slot.
    pub(super) fn next_disruption(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        previous: Option<[u8; 32]>,
        bytes: Option<&[u8]>,
    ) -> Result<[u8; 32]> {
        if previous.is_some() {
            return Err(FleetCatalogError::Invalid("unsettled disruption slot"));
        }
        let bytes = bytes.ok_or(FleetCatalogError::Invalid("disruption slot deletion"))?;
        let record = Disruption::decode(bytes)?;
        record.validate(self)?;
        record.check_basis(snapshot)?;
        if record.mutation()? != *mutation {
            return Err(FleetCatalogError::Invalid(
                "non-atomic disruption admission",
            ));
        }
        Ok(hash(bytes))
    }
}
