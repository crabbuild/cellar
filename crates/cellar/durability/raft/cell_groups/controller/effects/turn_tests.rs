// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn independent_work_keeps_discovery_behind_an_owned_callback() -> anyhow::Result<()> {
    let class = RaftClass::new("Counter", 1, [1; 32])?.durable_callbacks()?;
    let callback = EffectSummary {
        next_alarm_unix_millis: Some(10),
        ..Default::default()
    };
    assert!(native_callback_only(&class, callback));
    for independent in [
        EffectSummary::default(),
        EffectSummary {
            next_call_unix_millis: Some(20),
            ..callback
        },
        EffectSummary {
            next_outbox_unix_millis: Some(20),
            ..callback
        },
        EffectSummary {
            pending_calls: 1,
            ..callback
        },
        EffectSummary {
            pending_outbox: 1,
            ..callback
        },
        EffectSummary {
            pending_inbox: 1,
            ..callback
        },
    ] {
        assert!(!native_callback_only(&class, independent));
    }
    Ok(())
}

#[test]
fn compiled_alarms_keep_discovery_behind_an_owned_callback() -> anyhow::Result<()> {
    let class = RaftClass::new("Counter", 1, [1; 32])?
        .durable_callbacks()?
        .alarms(Default::default())?;
    assert!(!native_callback_only(
        &class,
        EffectSummary {
            next_alarm_unix_millis: Some(10),
            ..Default::default()
        }
    ));
    Ok(())
}

#[test]
fn kv_payload_capabilities_keep_retirement_discovery() -> anyhow::Result<()> {
    use crate::application::{Application, Registry};
    let callback = EffectSummary {
        next_alarm_unix_millis: Some(10),
        ..Default::default()
    };
    for kv in [false, true] {
        let class = RaftClass::new("Counter", 1, [1; 32])?
            .initializer([2; 32], |_, _| Ok(()))?
            .json_cell_rpc("Run", [3; 32])?
            .durable_callbacks()?;
        let application = Registry::new(|_, _| {
            Box::pin(async { anyhow::bail!("discovery must not invoke the application") })
        })
        .raft_cell(class, |_, _| {
            anyhow::bail!("discovery must not construct a cell")
        })?;
        let options = crate::application::consensus::NativeCellConfiguration {
            do_classes: vec!["Counter".into()],
            kv_bindings: if kv {
                vec![("K".into(), "values".into())]
            } else {
                vec![]
            },
            ..Default::default()
        }
        .options("callback-discovery-guard");
        application.validate_configuration(&options)?;
        let configured = application.resolve_raft_class("Counter")?;
        assert_eq!(native_callback_only(&configured, callback), !kv);
        if kv {
            let service = application.resolve_raft_class(crate::deploy::KV_CLASS)?;
            assert!(!native_callback_only(&service, callback));
        }
    }
    Ok(())
}

#[test]
fn follower_turns_neither_delay_led_groups_nor_requeue() {
    use crate::raft::completion::RuntimeError;
    let follower = after_turn(&Err(Error::Runtime(RuntimeError::NotLeader)));
    assert_eq!(
        follower,
        AfterTurn {
            delay: false,
            dequeue: true
        }
    );
    assert_eq!(
        after_turn(&Ok(true)),
        AfterTurn {
            delay: false,
            dequeue: false
        }
    );
    for result in [
        Ok(false),
        Err(Error::Runtime(RuntimeError::NotReady)),
        Err(Error::Runtime(RuntimeError::Overloaded)),
        Err(Error::Invalid("other")),
    ] {
        assert_eq!(
            after_turn(&result),
            AfterTurn {
                delay: true,
                dequeue: false
            }
        );
    }
}

#[test]
fn dequeued_group_returns_on_its_next_observation() -> anyhow::Result<()> {
    use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, LogIndex, NamespaceId};
    let group = |byte| GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([byte; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    let (follower, led) = (group(5), group(6));
    let mut manager = Manager::default();
    let observed = |error: CellGroupControllerError| anyhow::anyhow!("{error}");
    manager
        .observe(follower, LogIndex::new(7), Some(10))
        .map_err(observed)?;
    manager
        .observe(led, LogIndex::new(7), Some(10))
        .map_err(observed)?;
    manager.ready.insert(follower);
    // The worker re-registers the completed group before completion; dequeue
    // must win over that same-position observation.
    manager
        .observe(follower, LogIndex::new(7), Some(10))
        .map_err(observed)?;
    manager.dequeue(&follower);
    assert!(!manager.ready.contains(&follower));
    assert!(manager
        .waiting
        .iter()
        .all(|(_, waiting)| *waiting != follower));
    assert!(manager.waiting.contains(&(10, led)));
    // A later synced publication re-observes the follower group.
    manager
        .observe(follower, LogIndex::new(8), Some(10))
        .map_err(observed)?;
    assert!(manager.waiting.contains(&(10, follower)));
    Ok(())
}
