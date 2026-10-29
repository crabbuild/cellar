// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Catalog admission contracts. Pure decisions do not qualify a live group or
//! catalog root publication; those have separate native integration gates.

use cellar::application::consensus::{
    verification::cell_groups::*, ClusterId, GroupId, ImageCommitment, IncarnationId, LtxImage,
    NamespaceId, ProposalId,
};
use sha2::{Digest, Sha256};

#[path = "support/cell_group_directory.rs"]
mod directory;
#[path = "support/raft_catalog_fault.rs"]
#[allow(dead_code)]
mod fault;

#[tokio::test]
async fn range_directory_has_single_owner() -> anyhow::Result<()> {
    directory::single_owner().await
}

#[tokio::test]
async fn bounded_catalog_discovery() -> anyhow::Result<()> {
    directory::bounded_discovery().await
}

#[tokio::test]
async fn directory_reconciles_unknown_and_refuses_missing_state() -> anyhow::Result<()> {
    directory::recovery().await
}

#[tokio::test]
async fn directory_genesis_retry_after_later_mutation() -> anyhow::Result<()> {
    directory::genesis_race().await
}

#[tokio::test]
async fn directory_churn_preserves_exact_partition_and_old_views() -> anyhow::Result<()> {
    directory::churn().await
}

#[tokio::test]
async fn group_birth_is_lazy_and_reconciles_unknown_by_exact_identity() -> anyhow::Result<()> {
    directory::birth_recovery().await
}

#[tokio::test]
async fn group_birth_reservations_bind_global_identity_and_canonical_placement(
) -> anyhow::Result<()> {
    directory::birth_reservations().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn birth_unknown_and_claimed_blank_refuse() -> anyhow::Result<()> {
    directory::member_bootstrap().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_member_permit_refuses_retired_birth_and_foreign_node() -> anyhow::Result<()> {
    directory::member_fencing().await
}

fn route() -> GroupRoute {
    GroupRoute {
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
    }
}

fn entry(route: &GroupRoute) -> CellGroupEntry<'static> {
    CellGroupEntry {
        binding: GroupEntryBinding {
            group: route.history,
            route_epoch: route.epoch,
            birth_digest: route.birth_digest,
            schema_digest: route.schema_digest,
            proposal: ProposalId::new([7; 16]).unwrap(),
        },
        command: GroupCommand::Enroll {
            cell: CellHistory {
                cluster: route.history.cluster,
                namespace: route.history.namespace,
                cell: CellId::new("counter").unwrap(),
                incarnation: IncarnationId::new([8; 16]).unwrap(),
            },
            image: LtxImage {
                epoch: 1,
                txid: 0,
                checksum: 0,
                commitment: ImageCommitment::Sha256(Sha256::digest([]).into()),
            },
            files: vec![],
        },
    }
}

fn encoded(entry: &CellGroupEntry<'_>, route: &GroupRoute) -> Vec<u8> {
    entry.encode(route, GroupEntryLimits::default()).unwrap()
}

fn admitted(route: &GroupRoute, request: &[u8]) -> EnrollmentRecord {
    match decide_enrollment(
        route,
        EnrollmentPhase::Active,
        request,
        None,
        GroupEntryLimits::default(),
    )
    .unwrap()
    {
        EnrollmentDecision::Propose(record) => record,
        EnrollmentDecision::Existing(_) => panic!("new cell cannot be an existing creation"),
    }
}

#[test]
fn enrollment_decision_checks_identity() {
    let route = route();
    let original = entry(&route);
    let request = encoded(&original, &route);
    let selected = admitted(&route, &request);
    assert_eq!(selected.creation(), original.binding.proposal);
    assert_eq!(selected.cell().cell.as_str(), "counter");
    assert_eq!(selected.schema(), route.schema_digest);
    assert_eq!(selected.initial().txid, 0);

    // A lost reply is reconciled using the durable original creation record,
    // even after subsequent mutations have replaced the initial cell image.
    let saved = selected.encode().unwrap();
    let restarted = EnrollmentRecord::decode(&saved).unwrap();
    let retry = |request: &[u8], current: &GroupRoute| {
        decide_enrollment(
            current,
            EnrollmentPhase::Active,
            request,
            Some(&restarted),
            GroupEntryLimits::default(),
        )
    };
    assert_eq!(
        retry(&request, &route),
        Ok(EnrollmentDecision::Existing(selected.clone()))
    );

    // These requests may both be prepared while no committed record exists.
    // Rechecking against the selected record cannot replace its incarnation or
    // repurpose the same operation ID for a different initial image.
    for field in 0..4 {
        let mut competing = original.clone();
        let GroupCommand::Enroll { cell, image, .. } = &mut competing.command else {
            unreachable!()
        };
        match field {
            0 => competing.binding.proposal = ProposalId::new([9; 16]).unwrap(),
            1 => cell.incarnation = IncarnationId::new([10; 16]).unwrap(),
            2 => cell.cell = CellId::new("other").unwrap(),
            _ => image.epoch += 1,
        }
        let competitor = encoded(&competing, &route);
        assert_ne!(admitted(&route, &competitor), selected);
        assert_eq!(retry(&competitor, &route), Err(EnrollmentError::Conflict));
    }

    for field in 0..7 {
        let mut current = route.clone();
        match field {
            0 => current.history.cluster = ClusterId::new([11; 16]).unwrap(),
            1 => current.history.namespace = NamespaceId::new([11; 16]).unwrap(),
            2 => current.history.group = GroupId::new([11; 16]).unwrap(),
            3 => current.history.incarnation = IncarnationId::new([11; 16]).unwrap(),
            4 => current.epoch = current.epoch.checked_next().unwrap(),
            5 => current.schema_digest = [11; 32],
            _ => current.birth_digest = [11; 32],
        }
        assert!(matches!(
            retry(&request, &current),
            Err(EnrollmentError::Protocol(_))
        ));
    }

    // A target at a half-open upper bound belongs to the neighboring range.
    let mut outside = route.clone();
    outside.range = CellRange::new(
        CellBound::NegativeInfinity,
        CellBound::Key(cell_routing_key(
            route.history.namespace,
            &selected.cell().cell,
        )),
    )
    .unwrap();
    assert!(matches!(
        retry(&request, &outside),
        Err(EnrollmentError::Protocol(_))
    ));

    // Normal transfer rebinds the request to the current physical route while
    // the immutable logical creation record remains unchanged.
    let mut destination = route.clone();
    destination.history.group = GroupId::new([12; 16]).unwrap();
    destination.history.incarnation = IncarnationId::new([13; 16]).unwrap();
    destination.epoch = route.epoch.checked_next().unwrap();
    destination.birth_digest = [14; 32];
    let mut moved = original.clone();
    moved.binding = entry(&destination).binding;
    assert_eq!(
        retry(&encoded(&moved, &destination), &destination),
        Ok(EnrollmentDecision::Existing(selected))
    );
    assert!(retry(&request, &destination).is_err());
}

#[test]
fn sealed_range_rejects_enrollment() {
    let route = route();
    let mut entry = entry(&route);
    let prepared_before_seal = encoded(&entry, &route);
    let selected = admitted(&route, &prepared_before_seal);
    for (phase, error) in [
        (EnrollmentPhase::Sealed, EnrollmentError::Sealed),
        (EnrollmentPhase::Retired, EnrollmentError::Retired),
    ] {
        for existing in [None, Some(&selected)] {
            assert_eq!(
                decide_enrollment(
                    &route,
                    phase,
                    &prepared_before_seal,
                    existing,
                    GroupEntryLimits::default(),
                ),
                Err(error)
            );
        }
    }
    entry.command = GroupCommand::Retire {
        transition: TransitionId::new([17; 16]).unwrap(),
        publication: [18; 32],
    };
    assert_eq!(
        decide_enrollment(
            &route,
            EnrollmentPhase::Active,
            &encoded(&entry, &route),
            None,
            GroupEntryLimits::default(),
        ),
        Err(EnrollmentError::NotEnrollment)
    );
}

#[test]
fn enrollment_record_is_bounded_canonical_data() {
    let route = route();
    let mut entry = entry(&route);
    let GroupCommand::Enroll { cell, .. } = &mut entry.command else {
        unreachable!()
    };
    cell.cell = CellId::new("\\".repeat(1024)).unwrap();
    let record = admitted(&route, &encoded(&entry, &route));
    let bytes = record.encode().unwrap();
    assert_eq!(EnrollmentRecord::decode(&bytes).unwrap(), record);
    assert_eq!(
        EnrollmentRecord::decode(&vec![0; 4097]),
        Err(EnrollmentError::Limit)
    );
    for end in 0..bytes.len() {
        assert!(EnrollmentRecord::decode(&bytes[..end]).is_err());
    }
    let mut spaced = bytes.clone();
    spaced.push(b' ');
    assert!(EnrollmentRecord::decode(&spaced).is_err());
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let replace_field = |field: &str, value: serde_json::Value| {
        // Preserve canonical field order so refusal must come from the changed
        // value, not from Value's map ordering when serializing the whole object.
        String::from_utf8(bytes.clone())
            .unwrap()
            .replacen(
                &format!("\"{field}\":{}", original[field]),
                &format!("\"{field}\":{value}"),
                1,
            )
            .into_bytes()
    };
    let another = replace_field("creation", serde_json::json!(vec![9; 16]));
    assert_eq!(
        EnrollmentRecord::decode(&another).unwrap().creation(),
        ProposalId::new([9; 16]).unwrap()
    );
    for (field, value) in [
        ("domain", serde_json::json!("cellar-cell-enrollment-2")),
        ("cluster", serde_json::json!(vec![0; 16])),
        ("creation", serde_json::json!(vec![0; 16])),
        ("initial_epoch", serde_json::json!(0)),
        ("initial_checksum", serde_json::json!(1)),
    ] {
        assert!(
            EnrollmentRecord::decode(&replace_field(field, value)).is_err(),
            "{field}"
        );
    }
    let mut unknown = bytes[..bytes.len() - 1].to_vec();
    unknown.extend_from_slice(b",\"extra\":true}");
    assert!(EnrollmentRecord::decode(&unknown).is_err());
    let mut damaged = encoded(&entry, &route);
    *damaged.last_mut().unwrap() ^= 1;
    assert!(matches!(
        decide_enrollment(
            &route,
            EnrollmentPhase::Active,
            &damaged,
            None,
            GroupEntryLimits::default()
        ),
        Err(EnrollmentError::Protocol(_))
    ));
}

#[tokio::test]
async fn routing_redirect_requires_catalog_confirmation() -> anyhow::Result<()> {
    directory::routing_authentication().await
}

#[tokio::test]
async fn routing_cached_requests_preserve_identity_without_catalog_reads() -> anyhow::Result<()> {
    directory::routing_cache_and_budget().await
}

#[tokio::test]
async fn routing_attempt_deadline_survives_redirect_and_cancellation() -> anyhow::Result<()> {
    directory::routing_deadline_and_cancellation().await
}
