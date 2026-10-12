use super::*;
use crate::application::Registry;
use crate::engine_api::{Compat, WorkerConfigOptions};
use crate::host_facets::{prepare_facet, FacetHost, FacetStart, LoadedFacet};
use crate::pool::Slot;
use crate::worker::Worker;
use cellar_native::clone::encode;
use std::sync::atomic::AtomicUsize;
use tokio::sync::{oneshot, Notify};

#[derive(Default)]
struct Observed {
    contexts: Mutex<Vec<Context>>,
    drops: Mutex<Vec<String>>,
    pending: Notify,
    pending_drops: AtomicUsize,
}
struct Probe {
    scope: String,
    observed: Arc<Observed>,
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.observed.drops.lock().unwrap().push(self.scope.clone());
    }
}
struct PendingProbe(Arc<Observed>);
impl Drop for PendingProbe {
    fn drop(&mut self) {
        self.0.pending_drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Cell for Probe {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            context.check_turn()?;
            let Invocation::Fetch(request) = invocation else {
                unreachable!()
            };
            if request.url.ends_with("pending") {
                let _probe = PendingProbe(self.observed.clone());
                self.observed.pending.notified().await;
                context.check_turn()?;
            }
            let storage = context.storage()?;
            assert_eq!(
                storage.set_alarm(1).unwrap_err().to_string(),
                "Facets currently cannot set alarms."
            );
            if let Some(saved) = storage.get_clone("props")? {
                assert_eq!(saved.value, context.facet().unwrap().props);
                assert!(!saved.stored_stubs);
            }
            let count = storage
                .get_json("calls")?
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
                + 1;
            storage.put_json("calls", &serde_json::json!(count))?;
            storage.put_clone("props", &context.facet().unwrap().props)?;
            Ok(Output::Http(HttpResponse {
                status: 200,
                body: count.to_string().into_bytes(),
                headers: Vec::new(),
                stream: None,
                websocket: None,
                write_position: None,
                observed_position: None,
            }))
        })
    }
}

fn config(observed: Arc<Observed>) -> Result<Arc<WorkerConfig>> {
    let application = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).cell(
        "Facet",
        move |context, identity| {
            context.check_turn()?;
            assert!(
                context.facet().is_some(),
                "startup is installed before construction"
            );
            observed.contexts.lock().unwrap().push(context);
            Ok(Arc::new(Probe {
                scope: identity.scope.clone(),
                observed: observed.clone(),
            }))
        },
    )?;
    Ok(Arc::new(
        WorkerConfig::new(WorkerConfigOptions {
            src: "this is deliberately not JavaScript".into(),
            script_name: "native-facet-conformance".into(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        })
        .with_native_application(Arc::new(application))
        .into_dynamic_worker(),
    ))
}

async fn identity(slot: &Arc<Slot>, scope: &str) -> Result<storage::StorageIdentity> {
    slot.turn(|worker| {
        let worker = worker.backend_mut::<NativeWorker>().unwrap();
        let _installed = worker.cells.install();
        storage::storage_identity(scope)?.context("missing test storage")
    })
    .await
}
async fn start(
    slot: &Arc<Slot>,
    parent_scope: &str,
    parent: storage::StorageIdentity,
    owner: &str,
    name: &str,
    id: &StructuredValue,
    props: &StructuredValue,
) -> Result<LoadedFacet> {
    prepare_facet(
        FacetHost::Own(slot.clone()),
        FacetStart {
            class_name: "Facet".into(),
            parent_scope: parent_scope.into(),
            owner: owner.into(),
            name: name.into(),
            id_sc: encode(id)?,
            props_sc: encode(props)?,
            parent,
        },
    )
    .await
    .map_err(anyhow::Error::msg)
}
async fn invoke(
    slot: &Arc<Slot>,
    scope: &str,
    path: &str,
) -> Result<(InFlight, Vec<Op>, oneshot::Receiver<Result<HttpResponse>>)> {
    let (reply, receive) = oneshot::channel();
    let (entry, ops) = slot
        .turn(|worker| {
            worker.turn_begin_cell(
                CellJob::Fetch {
                    scope: scope.into(),
                    name: None,
                    request_id: None,
                    url: format!("http://facet/{path}"),
                    method: "GET".into(),
                    body: Vec::new().into(),
                    headers: Vec::new(),
                    reply,
                    order: None,
                },
                None,
            )
        })
        .await;
    Ok((entry.context("facet event did not start")?, ops, receive))
}
async fn call(slot: &Arc<Slot>, scope: &str) -> Result<Vec<u8>> {
    let (mut entry, mut ops, receive) = invoke(slot, scope, "fetch").await?;
    while !entry.finished() {
        let (id, future) = ops.pop().context("pending event has no wake")?;
        let result = asyncrt::timeout(std::time::Duration::from_secs(2), future).await?;
        ops.extend(
            slot.turn(|worker| worker.turn_deliver(&mut entry, id, result))
                .await,
        );
    }
    let response = receive.await??;
    slot.turn(|worker| worker.turn_abandon_input_gates(&entry))
        .await;
    entry.abandon();
    Ok(response.body)
}

// Shares the single host runtime initialized by the native turn-driver test.
pub(crate) async fn check_native_facet_lifecycles() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root_file = directory.path().join("root.sqlite");
    let root_file = root_file.to_str().unwrap();
    let root = "Root:facet-conformance";
    let observed = Arc::new(Observed::default());
    let slot = Slot::loaded_worker(Worker::load_config(config(observed.clone())?)?);
    let retained_slot = Arc::downgrade(&slot);
    slot.turn(|worker| {
        worker.own_cell(
            root,
            Some(CellStorage {
                path: root_file,
                epoch: 7,
                replicated_wake: false,
                vfs: None,
            }),
        )
    })
    .await?;
    let mut parent = identity(&slot, root).await?;
    parent.root_position = 12;
    parent.root_observed = Some(11);
    let id = StructuredValue::from_json(serde_json::json!(["Root", "named-id", "alice"]))?;
    let literal_id = StructuredValue {
        root: Value::from("literal-id"),
        objects: Vec::new(),
    };
    let props = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                (Value::from("self"), Value::Reference(0)),
                (Value::from("bytes"), Value::Reference(1)),
            ]),
            Object::ArrayBuffer {
                bytes: vec![1, 2, 3],
                max_length: None,
                immutable: false,
            },
        ],
    };
    let facet = start(&slot, root, parent.clone(), "first", "child", &id, &props).await?;
    assert_eq!(call(&slot, &facet.scope).await?, b"1");
    let original_context = observed.contexts.lock().unwrap()[0].clone();
    assert_eq!(
        original_context.identity().unwrap().name.as_deref(),
        Some("alice")
    );
    assert_eq!(original_context.facet().unwrap().id, id);
    assert_eq!(original_context.facet().unwrap().props, props);
    assert_eq!(original_context.facet().unwrap().depth, 1);
    let root_gate = original_context.io.egress.lock().unwrap()[0]
        .root
        .clone()
        .unwrap();
    assert_eq!(
        (root_gate.epoch, root_gate.position, root_gate.observed),
        (7, 12, Some(11))
    );
    assert_eq!(root_gate.stream, facet.stream);

    // An already running facet keeps startup and its instance while refreshing
    // the root's durability sample on every call.
    parent.root_position = 18;
    parent.root_observed = Some(17);
    let repeated = start(
        &slot,
        root,
        parent.clone(),
        "first",
        "child",
        &literal_id,
        &StructuredValue::from_json(serde_json::json!({}))?,
    )
    .await?;
    assert_eq!(repeated.opening, facet.opening);
    assert_eq!(call(&slot, &facet.scope).await?, b"2");
    assert_eq!(observed.contexts.lock().unwrap().len(), 1);
    let refreshed = identity(&slot, &facet.scope).await?;
    assert_eq!(
        (refreshed.root_position, refreshed.root_observed),
        (18, Some(17))
    );
    let mut stale = parent.clone();
    stale.epoch = 8;
    let id_sc = encode(&id)?;
    let props_sc = encode(&props)?;
    assert!(slot
        .turn(|worker| worker.own_embedded_cell(
            &facet.scope,
            &stale,
            "child",
            id_sc,
            props_sc,
            (directory.path().join("unused.sqlite"), false),
        ))
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("retired root"));

    slot.turn(|worker| -> Result<()> {
        let worker = worker.backend_mut::<NativeWorker>().unwrap();
        let _installed = worker.cells.install();
        storage::transaction_control(root, "start", false, "").map_err(anyhow::Error::msg)?;
        storage::put_json(root, "rolled-back", &serde_json::json!(true))?;
        Ok(())
    })
    .await?;
    assert_eq!(call(&slot, &facet.scope).await?, b"3");
    slot.turn(|worker| -> Result<()> {
        let worker = worker.backend_mut::<NativeWorker>().unwrap();
        let _installed = worker.cells.install();
        storage::transaction_control(root, "rollback", false, "").map_err(anyhow::Error::msg)?;
        assert!(storage::get_stored(root, "rolled-back")?.is_none());
        Ok(())
    })
    .await?;

    let second = start(
        &slot,
        &facet.scope,
        refreshed,
        "nested",
        "second",
        &id,
        &props,
    )
    .await?;
    assert_eq!(call(&slot, &second.scope).await?, b"1");
    let third = start(
        &slot,
        &second.scope,
        identity(&slot, &second.scope).await?,
        "nested",
        "third",
        &id,
        &props,
    )
    .await?;
    assert_eq!(call(&slot, &third.scope).await?, b"1");
    assert!(start(
        &slot,
        &third.scope,
        identity(&slot, &third.scope).await?,
        "nested",
        "too-deep",
        &id,
        &props
    )
    .await
    .err()
    .unwrap()
    .to_string()
    .contains("depth limit"));
    let (closed, outcome) = slot
        .turn(|worker| worker.close_facet_tree(&facet.scope, root, &["child".into()]))
        .await;
    outcome?;
    assert_eq!(
        closed,
        vec![
            third.scope.clone(),
            second.scope.clone(),
            facet.scope.clone()
        ]
    );
    assert_eq!(*observed.drops.lock().unwrap(), closed);
    assert!(original_context
        .io
        .native_activation
        .as_ref()
        .unwrap()
        .resources
        .retired
        .load(Ordering::Acquire));

    // Replacing startup identity cannot change which database the facet name
    // selects. Reopening uses a new activation/opening and retained contents.
    let reopened = start(
        &slot,
        root,
        parent.clone(),
        "replacement",
        "child",
        &literal_id,
        &props,
    )
    .await?;
    assert_ne!(reopened.scope, facet.scope);
    assert_ne!(reopened.opening, facet.opening);
    assert_eq!(reopened.stream, facet.stream);
    assert_eq!(call(&slot, &reopened.scope).await?, b"4");
    let replacement_context = observed.contexts.lock().unwrap().last().unwrap().clone();
    assert_eq!(replacement_context.identity().unwrap().name, None);
    assert_eq!(replacement_context.facet().unwrap().id, literal_id);

    let cursor_child = start(
        &slot,
        &reopened.scope,
        identity(&slot, &reopened.scope).await?,
        "cursor",
        "cursor-child",
        &id,
        &props,
    )
    .await?;
    assert_eq!(call(&slot, &cursor_child.scope).await?, b"1");
    let cursor = slot
        .turn(|worker| -> Result<u64> {
            let worker = worker.backend_mut::<NativeWorker>().unwrap();
            let _installed = worker.cells.install();
            storage::sql_exec(
                &reopened.scope,
                "CREATE TABLE unfinished (value INTEGER)",
                &[],
            )
            .map_err(anyhow::Error::msg)?;
            let (cursor, _, row, _, _) = storage::sql_cursor_start(
                &reopened.scope,
                "INSERT INTO unfinished VALUES (1), (2) RETURNING value",
                &[],
            )
            .map_err(anyhow::Error::msg)?;
            assert!(row.is_some());
            assert!(storage::storage_identity(&reopened.scope).is_err());
            Ok(cursor)
        })
        .await?;
    slot.turn(|worker| worker.own_cell(&reopened.scope, None))
        .await?;
    assert!(
        identity(&slot, &cursor_child.scope).await.is_err(),
        "retirement must close children despite an unfinished write cursor"
    );
    slot.turn(|worker| {
        let worker = worker.backend_mut::<NativeWorker>().unwrap();
        let _installed = worker.cells.install();
        storage::sql_cursor_close(cursor);
    })
    .await;
    let old_opening = reopened.opening;
    let old_scope = reopened.scope.clone();
    drop(reopened);
    let reopened = start(
        &slot,
        root,
        parent.clone(),
        "replacement",
        "child",
        &literal_id,
        &props,
    )
    .await?;
    assert_eq!(reopened.scope, old_scope);
    assert_ne!(reopened.opening, old_opening);
    assert_eq!(call(&slot, &reopened.scope).await?, b"5");
    assert!(replacement_context
        .io
        .native_activation
        .as_ref()
        .unwrap()
        .resources
        .retired
        .load(Ordering::Acquire));

    let loaded_slot = Slot::loaded_worker(Worker::load_config(config(observed.clone())?)?);
    let retained_loaded_slot = Arc::downgrade(&loaded_slot);
    let loaded = start(
        &loaded_slot,
        root,
        parent.clone(),
        "loaded",
        "external",
        &id,
        &props,
    )
    .await?;
    assert_eq!(call(&loaded_slot, &loaded.scope).await?, b"1");
    let loaded_context = observed.contexts.lock().unwrap().last().unwrap().clone();

    let (mut pending, mut ops, receive) = invoke(&slot, &reopened.scope, "pending").await?;
    assert!(!pending.finished());
    let authority = slot
        .turn(|worker| {
            worker
                .backend_mut::<NativeWorker>()
                .unwrap()
                .activations
                .get(root)
                .unwrap()
                .clone()
        })
        .await;
    let late = crate::host_facets::prepare_native_facet(
        FacetHost::Own(slot.clone()),
        FacetStart {
            class_name: "Facet".into(),
            parent_scope: root.into(),
            owner: "late".into(),
            name: "late".into(),
            id_sc: encode(&id)?,
            props_sc: encode(&props)?,
            parent: parent.clone(),
        },
        authority.clone(),
    );
    let (kept_file, _) = crate::host_facets::open_facet_file(&parent, "child")
        .await
        .map_err(anyhow::Error::msg)?;
    assert!(asyncrt::fs().metadata(&kept_file).is_ok());
    let deletion = slot
        .turn(|worker| {
            let mut deletion = Box::pin(crate::application::facets::delete_native_facet(
                slot.clone(),
                authority,
                parent.clone(),
                "child".into(),
            ));
            // A host deletion cannot read or mutate the database while the
            // parent turn is held. Retire and reopen the same epoch before
            // releasing that turn: the old activation must never delete it.
            let mut task = std::task::Context::from_waker(futures_util::task::noop_waker_ref());
            assert!(std::future::Future::poll(deletion.as_mut(), &mut task).is_pending());
            worker.own_cell(root, None)?;
            worker.own_cell(
                root,
                Some(CellStorage {
                    path: root_file,
                    epoch: 7,
                    replicated_wake: false,
                    vfs: None,
                }),
            )?;
            Ok::<_, anyhow::Error>(deletion)
        })
        .await?;
    assert!(deletion
        .await
        .unwrap_err()
        .to_string()
        .contains("parent activation has retired"));
    assert!(
        asyncrt::fs().metadata(&kept_file).is_ok(),
        "retired deletion leaves the reopened activation's database intact"
    );
    slot.turn(|worker| worker.own_cell(root, None)).await?;
    assert!(identity(&slot, &reopened.scope).await.is_err());
    let (id, wake) = ops.pop().context("suspended facet has no wake")?;
    let result = asyncrt::timeout(std::time::Duration::from_secs(2), wake).await?;
    let more = slot
        .turn(|worker| worker.turn_deliver(&mut pending, id, result))
        .await;
    assert!(pending.finished());
    assert!(receive
        .await?
        .err()
        .unwrap()
        .to_string()
        .contains("activation has retired"));
    assert_eq!(observed.pending_drops.load(Ordering::SeqCst), 1);
    drop(ops);
    drop(more);
    slot.turn(|worker| worker.turn_abandon_input_gates(&pending))
        .await;
    pending.abandon();
    crate::host_facets::release_root_facets(root, 7, crate::host_facets::RootRelease::Stop)
        .await
        .map_err(anyhow::Error::msg)?;
    assert!(late
        .await
        .err()
        .unwrap()
        .contains("parent activation has retired"));
    assert!(identity(&loaded_slot, &loaded.scope).await.is_err());
    assert!(loaded_context
        .io
        .native_activation
        .as_ref()
        .unwrap()
        .resources
        .retired
        .load(Ordering::Acquire));
    drop(loaded);
    drop(loaded_slot);
    drop(pending);
    drop(reopened);
    drop(repeated);
    drop(facet);
    drop(second);
    drop(third);
    drop(cursor_child);
    drop(slot);
    assert!(retained_slot.upgrade().is_none());
    assert!(retained_loaded_slot.upgrade().is_none());
    assert!(!crate::host_facets::root_facets()
        .lock()
        .unwrap()
        .keys()
        .any(|(scope, _)| scope == root));
    assert!(installed_context().is_none());
    Ok(())
}
