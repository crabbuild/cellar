// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn canonical_population_partitions_complete_ranges() -> Result<()> {
    for occupancy in [1, 2, 32, 64, 128] {
        for total in [1, occupancy, occupancy + 1, 257] {
            let layout = Layout::new(total, occupancy, "Counter", 0)?;
            assert_eq!(layout.groups.len(), total.div_ceil(occupancy));
            assert_eq!(layout.groups.iter().map(|g| g.cells).sum::<usize>(), total);
            for (ordinal, group) in layout.groups.iter().enumerate() {
                assert_eq!(group.cells, (total - ordinal * occupancy).min(occupancy));
                if ordinal > 0 {
                    assert_eq!(
                        layout.groups[ordinal - 1].range.upper(),
                        group.range.lower()
                    );
                }
            }
            for cell in 0..total {
                let history = layout.history(cell)?;
                let key = CellKey::from_digest(
                    Sha256::digest(history.cell.routing_preimage(history.namespace)).into(),
                    history.cell,
                );
                assert!(layout.group(cell)?.range.contains(&key));
            }
        }
    }
    Ok(())
}

#[test]
fn logical_and_physical_histories_are_distinct_and_bounded() -> Result<()> {
    let old = Layout::new(33, 32, "Counter", 0)?;
    let fresh = Layout::new(33, 32, "Counter", 1)?;
    assert_eq!(old.assignment, fresh.assignment);
    for cell in 0..33 {
        assert_eq!(old.history(cell)?.cell, fresh.history(cell)?.cell);
        assert_ne!(
            old.history(cell)?.incarnation,
            fresh.history(cell)?.incarnation
        );
        assert_ne!(old.group(cell)?.history, fresh.group(cell)?.history);
    }
    for name in ["", "01", "-1", "33", "9999999999999999999999999999"] {
        assert!(old.named_history(name).is_err());
    }
    assert_eq!(old.named_history("32")?, old.history(32)?);
    assert!(old.group(33).is_err());
    assert!(Layout::new(0, 32, "Counter", 0).is_err());
    assert!(Layout::new(1, 0, "Counter", 0).is_err());
    assert!(Layout::new(1, 129, "Counter", 0).is_err());
    assert!(Layout::new(1, 32, "Other", 0).is_err());
    assert!(Layout::new(1, 32, "Counter", 2).is_err());
    Ok(())
}

#[test]
fn controller_vectors_match_native_routing_and_placement() -> Result<()> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!("raft_group_vectors.json"))?;
    for case in vectors.as_array().context("routing vectors")? {
        let layout = Layout::new(
            case["total"].as_u64().context("total")? as usize,
            case["occupancy"].as_u64().context("occupancy")? as usize,
            case["class_name"].as_str().context("class")?,
            case["generation"].as_u64().context("generation")? as u8,
        )?;
        assert_eq!(
            layout.json(Placement::new(
                case["nodes"].as_u64().context("nodes")? as u8
            )?),
            case["layout"]
        );
    }
    Ok(())
}
