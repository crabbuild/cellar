// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded advisory geometry/counters inside the authenticated Fleet read reply.
use super::cell_groups::{
    CellGroupPartitionCell, CellGroupPartitionObservation, CellGroupPartitionWriter,
    CellGroupStatus, CellGroupWorkload,
};
use super::transport::TransportError;
use cellar_logic::{
    cell_group::{CellId, CellKey, GroupRoute, MembershipRevision},
    consensus::{LogIndex, LogPosition, Term},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Sample {
    owner: [u8; 16],
    sampled: u64,
    admitted: u64,
    applied: u64,
    queue_age: u64,
    cells: u32,
    bytes: u64,
    term: u64,
    committed: u64,
    position: [u64; 2],
    membership: u64,
    leader: u64,
    ready: bool,
    #[serde(deserialize_with = "nullable")]
    mutation: Option<[u64; 2]>,
    #[serde(deserialize_with = "nullable")]
    pivot: Option<String>,
    #[serde(deserialize_with = "nullable")]
    largest: Option<Cell>,
    #[serde(deserialize_with = "nullable")]
    writer: Option<Writer>,
    #[serde(deserialize_with = "nullable")]
    archive_lag: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cell {
    cell: String,
    #[serde(deserialize_with = "nullable")]
    next: Option<String>,
    before: bool,
    bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Writer {
    cell: Cell,
    started: u64,
    admitted: u64,
}

fn nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}

fn point(value: [u64; 2]) -> Result<LogPosition, TransportError> {
    if value.iter().any(|n| *n == 0 || *n >= u64::MAX - 2) {
        return Err(TransportError::Protocol);
    }
    Ok(LogPosition {
        index: LogIndex::new(value[0]),
        term: Term::new(value[1]),
    })
}

fn key(cell: &str, route: &GroupRoute) -> Result<CellKey, TransportError> {
    let cell = CellId::new(cell).map_err(|_| TransportError::Protocol)?;
    let key = crate::application::consensus::cell_routing_key(route.history.namespace, &cell);
    if !route.range.contains(&key) {
        return Err(TransportError::Identity);
    }
    Ok(key)
}

impl Cell {
    fn capture(value: &CellGroupPartitionCell) -> Self {
        Self {
            cell: value.key.cell().as_str().into(),
            next: value.next.as_ref().map(|k| k.cell().as_str().into()),
            before: value.has_predecessor,
            bytes: value.logical_bytes,
        }
    }

    fn decode(self, route: &GroupRoute) -> Result<CellGroupPartitionCell, TransportError> {
        let current = key(&self.cell, route)?;
        let next = self.next.map(|s| key(&s, route)).transpose()?;
        if next.as_ref().is_some_and(|n| *n <= current) {
            return Err(TransportError::Protocol);
        }
        Ok(CellGroupPartitionCell {
            key: current,
            next,
            has_predecessor: self.before,
            logical_bytes: self.bytes,
        })
    }
}

impl Sample {
    pub(crate) fn capture(value: &CellGroupPartitionObservation) -> Result<Self, TransportError> {
        if !value.stable_leader {
            return Err(TransportError::Refused);
        }
        Ok(Self {
            owner: value.workload.owner,
            sampled: value.workload.sampled_at_ms,
            admitted: value.workload.admitted_writes,
            applied: value.workload.applied_writes,
            queue_age: value.workload.queue_age_ms,
            cells: value.cells,
            bytes: value.logical_bytes,
            term: value.status.term.get(),
            committed: value.status.committed.get(),
            position: [
                value.status.applied.index.get(),
                value.status.applied.term.get(),
            ],
            membership: value.membership.get(),
            leader: value.status.leader.ok_or(TransportError::Protocol)?,
            ready: value.status.ready,
            mutation: value.last_mutation.map(|p| [p.index.get(), p.term.get()]),
            pivot: value
                .balanced_pivot
                .as_ref()
                .map(|k| k.cell().as_str().into()),
            largest: value.largest_cell.as_ref().map(Cell::capture),
            writer: value.hot_writer.as_ref().map(|w| Writer {
                cell: Cell::capture(&w.cell),
                started: w.started_at_ms,
                admitted: w.admitted_writes,
            }),
            archive_lag: value.archive_lag_entries,
        })
    }

    pub(crate) fn decode(
        self,
        route: &GroupRoute,
    ) -> Result<Box<CellGroupPartitionObservation>, TransportError> {
        let applied = point(self.position)?;
        if self.owner == [0; 16]
            || self.cells > 1_000_000
            || self.term == 0
            || self.term >= u64::MAX - 2
            || self.leader == 0
            || self.leader >= u64::MAX - 2
            || self.committed < applied.index.get()
            || self.committed >= u64::MAX - 2
        {
            return Err(TransportError::Protocol);
        }
        Ok(Box::new(CellGroupPartitionObservation {
            route: route.clone(),
            membership: MembershipRevision::new(self.membership)
                .map_err(|_| TransportError::Protocol)?,
            status: CellGroupStatus {
                term: Term::new(self.term),
                leader: Some(self.leader),
                applied,
                committed: LogIndex::new(self.committed),
                ready: self.ready,
            },
            stable_leader: true,
            workload: CellGroupWorkload {
                owner: self.owner,
                sampled_at_ms: self.sampled,
                admitted_writes: self.admitted,
                applied_writes: self.applied,
                queue_age_ms: self.queue_age,
            },
            cells: self.cells,
            logical_bytes: self.bytes,
            last_mutation: self.mutation.map(point).transpose()?,
            balanced_pivot: self.pivot.map(|c| key(&c, route)).transpose()?,
            largest_cell: self.largest.map(|c| c.decode(route)).transpose()?,
            hot_writer: self
                .writer
                .map(|w| {
                    Ok::<_, TransportError>(CellGroupPartitionWriter {
                        cell: w.cell.decode(route)?,
                        started_at_ms: w.started,
                        admitted_writes: w.admitted,
                    })
                })
                .transpose()?,
            archive_lag_entries: self.archive_lag,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_logic::{
        cell_group::{CellRange, GroupHistory, RouteEpoch},
        consensus::{ClusterId, GroupId, IncarnationId, NamespaceId},
    };

    fn observation() -> CellGroupPartitionObservation {
        CellGroupPartitionObservation {
            route: GroupRoute {
                history: GroupHistory {
                    cluster: ClusterId::new([1; 16]).unwrap(),
                    namespace: NamespaceId::new([2; 16]).unwrap(),
                    group: GroupId::new([3; 16]).unwrap(),
                    incarnation: IncarnationId::new([4; 16]).unwrap(),
                },
                range: CellRange::all(),
                epoch: RouteEpoch::new(1).unwrap(),
                birth_digest: [5; 32],
                schema_digest: [6; 32],
            },
            membership: MembershipRevision::new(1).unwrap(),
            status: CellGroupStatus {
                term: Term::new(2),
                leader: Some(1),
                applied: LogPosition {
                    index: LogIndex::new(5),
                    term: Term::new(2),
                },
                committed: LogIndex::new(5),
                ready: true,
            },
            stable_leader: true,
            workload: CellGroupWorkload {
                owner: [9; 16],
                sampled_at_ms: 1_000_000,
                admitted_writes: 4,
                applied_writes: 4,
                queue_age_ms: 0,
            },
            cells: 2,
            logical_bytes: 8192,
            last_mutation: None,
            balanced_pivot: None,
            largest_cell: None,
            hot_writer: None,
            archive_lag_entries: Some(0),
        }
    }

    #[test]
    fn partition_observation_roundtrip_retains_source_clock_and_owner() {
        let value = observation();
        let bytes = serde_json::to_vec(&Sample::capture(&value).unwrap()).unwrap();
        let decoded = serde_json::from_slice::<Sample>(&bytes)
            .unwrap()
            .decode(&value.route)
            .unwrap();
        assert!(decoded.same_workload_owner(&value));
        assert_eq!(decoded.workload.sampled_at_ms, 1_000_000);
        assert_eq!(decoded.workload.admitted_writes, 4);
    }

    #[test]
    fn partition_observation_refuses_missing_and_invalid_fields() {
        let value = observation();
        let raw = serde_json::to_value(Sample::capture(&value).unwrap()).unwrap();
        let mut missing = raw.clone();
        missing.as_object_mut().unwrap().remove("archive_lag");
        assert!(serde_json::from_value::<Sample>(missing).is_err());
        for (field, bad) in [
            ("owner", serde_json::json!(vec![0u8; 16])),
            ("committed", serde_json::json!(4)),
            ("pivot", serde_json::json!("x".repeat(1025))),
        ] {
            let mut body = raw.clone();
            body[field] = bad;
            assert!(serde_json::from_value::<Sample>(body)
                .unwrap()
                .decode(&value.route)
                .is_err());
        }
    }
}
