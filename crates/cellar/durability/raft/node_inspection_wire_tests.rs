use super::*;

fn view() -> RaftHostObservation {
    let startup = RaftHostEnrollmentObservation {
        fleet: [80; 16],
        revision: 2,
        epoch: 1,
        physical_host: IncarnationId::new([90; 16]).unwrap(),
        state: FleetNodeState::Registered,
        capability_digest: [9; 32],
    };
    let current = RaftHostEnrollmentObservation {
        revision: 3,
        epoch: 2,
        state: FleetNodeState::RestartPending,
        ..startup.clone()
    };
    RaftHostObservation {
        node: NodeInspection {
            identity: NodeIdentity {
                node: NodeId::new([5; 16]).unwrap(),
                disk: DiskLineage::new([15; 16]).unwrap(),
                process: ProcessGeneration::new([25; 16]).unwrap(),
            },
            storage_instance: EngineInstanceId::new([35; 16]).unwrap(),
            profile: RaftNodeProfile {
                pool: "native".into(),
                region: "local".into(),
                zone: "shared".into(),
                group_capacity: 8,
                byte_capacity: 8192,
                bytes_per_group: 1024,
                leader_capacity: 8,
                draining: false,
            },
            groups: 0,
            reserved_bytes: 0,
        },
        startup_enrollment: Some(startup),
        current_enrollment: Some(current),
        fleet_trust_revision: Some(3),
        declared_namespaces: 1,
        registered_histories: 0,
        dormant_histories: 0,
        pending_history_registrations: 0,
        unattached_engine_histories: 0,
    }
}

#[test]
fn node_metadata_preserves_current_exclusion_without_rewriting_boot() {
    let view = view();
    assert_eq!(*decode(&encode(&view).unwrap()).unwrap(), view);
    let current = view.current_enrollment.unwrap();
    assert!(
        !current.state.eligibility().receive_replicas
            && !current.state.eligibility().evacuate_memberships
    );
}

#[test]
fn node_metadata_rejects_missing_invalid_and_unbounded_fields() {
    let original: serde_json::Value = serde_json::from_slice(&encode(&view()).unwrap()).unwrap();
    for field in [
        "format",
        "startup",
        "current",
        "profile",
        "pending_history_registrations",
    ] {
        let mut changed = original.clone();
        changed.as_object_mut().unwrap().remove(field);
        assert!(decode(&serde_json::to_vec(&changed).unwrap()).is_err());
    }
    for (field, value) in [
        ("format", 2),
        ("declared_namespaces", 65),
        ("pending_history_registrations", 2),
        ("dormant_histories", 1),
        ("unattached_engine_histories", 1),
    ] {
        let mut changed = original.clone();
        changed[field] = serde_json::json!(value);
        assert!(decode(&serde_json::to_vec(&changed).unwrap()).is_err());
    }
    for field in ["epoch", "revision"] {
        for value in ["0", "03", "18446744073709551615"] {
            let mut changed = original.clone();
            changed["current"][field] = serde_json::json!(value);
            assert!(decode(&serde_json::to_vec(&changed).unwrap()).is_err());
        }
    }
    let mut changed = original.clone();
    changed["current"]["state"] = serde_json::json!("Removed");
    assert!(decode(&serde_json::to_vec(&changed).unwrap()).is_err());
    let mut changed = original.clone();
    changed["profile"]["zone"] = serde_json::json!("x".repeat(65));
    assert!(decode(&serde_json::to_vec(&changed).unwrap()).is_err());
    let mut changed = original;
    changed["reserved_bytes"] = serde_json::json!("1");
    assert!(decode(&serde_json::to_vec(&changed).unwrap()).is_err());
    assert!(decode(&vec![0; MAX_BODY + 1]).is_err());
}
