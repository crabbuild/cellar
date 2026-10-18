use super::*;

#[test]
fn established_actor_name_needs_no_writer_lock() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cell.sqlite");
    let scope = "Counter:named";
    open(scope, path.to_str().unwrap())?;
    set_actor_name(scope, "counter")?;

    // WAL capture can hold the other connection's writer lock while an
    // ordinary named fetch or RPC starts. Checking identity must stay a read.
    let capture = Connection::open(&path)?;
    capture.execute_batch("BEGIN IMMEDIATE")?;
    with(scope, |connection| {
        connection.busy_timeout(std::time::Duration::ZERO)
    })
    .unwrap()?;
    let checked = set_actor_name(scope, "counter");
    capture.execute_batch("ROLLBACK")?;
    checked?;
    assert!(set_actor_name(scope, "different").is_err());
    assert_eq!(get_actor_name(scope)?.as_deref(), Some("counter"));

    delete_all_with_alarm(scope, true)?;
    close(scope);
    open(scope, path.to_str().unwrap())?;
    assert_eq!(get_actor_name(scope)?.as_deref(), Some("counter"));
    assert!(set_actor_name(scope, "different").is_err());
    set_actor_name(scope, "counter")?;
    close(scope);
    Ok(())
}

#[test]
fn warm_actor_identity_reuses_prepared_statements() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Counter:named";
    open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    let prepares = Arc::new(AtomicU64::new(0));
    let observed = prepares.clone();
    with(scope, |connection| {
        connection.authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
            observed.fetch_add(1, Ordering::Relaxed);
            authorize_sql(context)
        }));
    });
    set_actor_name(scope, "counter")?;
    assert_eq!(get_actor_name(scope)?.as_deref(), Some("counter"));
    let before = prepares.load(Ordering::Relaxed);
    for _ in 0..100 {
        set_actor_name(scope, "counter")?;
        assert_eq!(get_actor_name(scope)?.as_deref(), Some("counter"));
    }
    assert_eq!(prepares.load(Ordering::Relaxed), before);
    close(scope);
    Ok(())
}
