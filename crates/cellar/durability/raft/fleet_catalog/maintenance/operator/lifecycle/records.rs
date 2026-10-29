// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded canonical child references, with no restored samples or clocks.
use super::*;
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in super::super) struct Stop {
    pub identity: FleetRequestIdentity,
    pub ready: FleetRequestIdentity,
    pub child: [u8; 16],
}

impl Stop {
    pub fn request(&self, record: &Record) -> Result<FleetRestartStopRequest> {
        Ok(FleetRestartStopRequest {
            identity: self.identity.clone(),
            ready: self.ready.clone(),
            node: NodeId::new(record.boot.node)
                .map_err(|_| FleetCatalogError::Corrupt("operator node"))?,
            expected_process: ProcessGeneration::new(record.boot.process)
                .map_err(|_| FleetCatalogError::Corrupt("operator process"))?,
            stop: ProposalId::new(self.child)
                .map_err(|_| FleetCatalogError::Corrupt("operator stop child"))?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in super::super) struct Resume {
    pub identity: FleetRequestIdentity,
}

impl Resume {
    pub fn request(
        &self,
        facts: &Facts,
        grant: &FleetRestartOperatorAction,
    ) -> Result<FleetRestartResumeRequest> {
        let FleetRestartOperatorAction::Resume {
            attestation,
            resume,
        } = grant
        else {
            return Err(FleetCatalogError::Corrupt("operator resume grant"));
        };
        Ok(FleetRestartResumeRequest {
            identity: self.identity.clone(),
            returning: facts
                .returning
                .clone()
                .ok_or(FleetCatalogError::Corrupt("operator return predecessor"))?,
            attestation: attestation.as_ref().clone(),
            resume: ProposalId::new(*resume)
                .map_err(|_| FleetCatalogError::Corrupt("operator resume child"))?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in super::super) struct Facts {
    #[serde(deserialize_with = "nullable")]
    pub stop: Option<Stop>,
    #[serde(deserialize_with = "nullable")]
    pub offline: Option<FleetRequestIdentity>,
    #[serde(deserialize_with = "nullable")]
    pub returning: Option<FleetRequestIdentity>,
    #[serde(deserialize_with = "nullable")]
    pub resume: Option<Resume>,
    #[serde(deserialize_with = "nullable")]
    pub completion: Option<FleetRequestIdentity>,
}

impl Facts {
    pub fn validate(&self, operation: [u8; 16]) -> Result<()> {
        let identities = self
            .stop
            .iter()
            .flat_map(|s| [&s.identity, &s.ready])
            .chain(self.offline.iter())
            .chain(self.returning.iter())
            .chain(self.resume.iter().map(|s| &s.identity))
            .chain(self.completion.iter());
        for identity in identities {
            validate_request(identity)?;
            if identity.operation != operation {
                return Err(FleetCatalogError::Corrupt("operator child operation"));
            }
        }
        if self.offline.is_some() && self.stop.is_none()
            || self.returning.is_some() && self.offline.is_none()
            || self.resume.is_some() && self.returning.is_none()
            || self.completion.is_some() && self.resume.is_none()
        {
            return Err(FleetCatalogError::Corrupt("operator child sequence"));
        }
        if let Some(stop) = &self.stop {
            ProposalId::new(stop.child)
                .map_err(|_| FleetCatalogError::Corrupt("operator stop child"))?;
            if stop.ready.expected_revision >= stop.identity.expected_revision {
                return Err(FleetCatalogError::Corrupt("operator stop readiness"));
            }
        }
        Ok(())
    }
}

fn nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(decoder)
}
