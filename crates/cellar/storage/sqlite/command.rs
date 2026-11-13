// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Restricted SQLite turns for the private Raft command image.

use anyhow::{ensure, Context as _, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{ffi, Connection, OpenFlags, Params, Row};
use std::cell::{Cell, OnceCell, Ref, RefCell};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use crate::application::storage::{
    RaftDispatch, RaftDispatchContext, RaftStorageAuthority, Resources,
};
use crate::application::{HandlerFuture, Storage};
use std::collections::VecDeque;
use std::task::{Context as TaskContext, Poll};

#[path = "command_capacity.rs"]
mod capacity;

/// Host-captured inputs for one command. Followers apply bytes, never rerun it.
#[derive(Clone, Copy, Debug)]
pub struct CommandInputs {
    pub now_unix_millis: u64,
    pub random: [u8; 32],
}

/// A callback can use SQL and captured inputs, but cannot retain or commit the
/// framework transaction, obtain its connection, or start asynchronous work.
pub struct CommandTransaction<'a> {
    frame: &'a CommandFrame,
    inputs: CommandInputs,
}

impl CommandTransaction<'_> {
    pub(crate) fn framework(&self) -> Result<Ref<'_, Connection>> {
        self.frame.connection()
    }

    pub fn inputs(&self) -> CommandInputs {
        self.inputs
    }

    pub fn execute(&self, sql: &str, params: impl Params) -> Result<usize> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        let connection = self.framework()?;
        super::as_user_sql(|| connection.execute(sql, params)).map_err(Into::into)
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        let connection = self.framework()?;
        super::as_user_sql(|| connection.execute_batch(sql)).map_err(Into::into)
    }

    pub fn query_row<T>(
        &self,
        sql: &str,
        params: impl Params,
        read: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<T> {
        super::check_sql_rename(sql).map_err(anyhow::Error::msg)?;
        let connection = self.framework()?;
        super::as_user_sql(|| connection.query_row(sql, params, read)).map_err(Into::into)
    }

    /// Open an ordinary Storage view within this private transaction. Its
    /// synchronous operations share the command's commit and durability.
    pub fn storage(&self) -> Result<Storage> {
        let connection = self.framework()?;
        if self.frame.storage.get().is_none() {
            super::as_framework_sql(|| connection.execute_batch(super::KV_SCHEMA))?;
        }
        let resources = self.frame.storage.get_or_init(|| {
            self.frame
                .activation
                .clone()
                .unwrap_or_else(|| Arc::new(Resources::default()))
        });
        Ok(Storage::raft_command(RaftStorageAuthority {
            resources: resources.clone(),
            invocation: Some(self.frame.invocation.clone()),
        }))
    }
}

struct CommandFrame {
    // Drop statements before the connection, including on unwinding paths.
    cursors: super::raft_cursors::RaftCursors,
    connection: RefCell<Option<Connection>>,
    storage: OnceCell<Arc<Resources>>,
    activation: Option<Arc<Resources>>,
    invocation: Arc<()>,
    failure: RefCell<Option<String>>,
    work: RefCell<Work>,
    dispatch: OnceCell<RaftDispatchContext>,
    binding_serial: Cell<u64>,
    parked_io: Cell<bool>,
}

struct Work {
    tasks: VecDeque<WorkTask>,
    active: usize,
    limit: usize,
    serial: u64,
    checkpoint: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
}

struct WorkTask {
    future: HandlerFuture<()>,
    external: Option<Arc<std::sync::atomic::AtomicBool>>,
    parked_io: Arc<std::sync::atomic::AtomicBool>,
}

thread_local! {
    static CURRENT_COMMAND: RefCell<Option<Rc<CommandFrame>>> = const { RefCell::new(None) };
}

struct InstalledCommand;

impl Drop for InstalledCommand {
    fn drop(&mut self) {
        CURRENT_COMMAND.with(|current| current.borrow_mut().take());
    }
}

impl CommandFrame {
    fn connection(&self) -> Result<Ref<'_, Connection>> {
        let connection = self
            .connection
            .try_borrow()
            .context("private SQLite is busy")?;
        let connection = Ref::filter_map(connection, Option::as_ref)
            .map_err(|_| anyhow::anyhow!("private SQLite is awaiting consensus"))?;
        ensure!(
            !connection.is_autocommit(),
            "private command transaction ended"
        );
        if let Some(error) = self.failure.borrow().as_ref() {
            anyhow::bail!("{error}");
        }
        Ok(connection)
    }
}

/// An open transaction whose connection belongs to one SQL worker. A parked
/// command owns its connection without installing it in that worker's TLS.
pub(crate) struct ActiveCommand {
    frame: Rc<CommandFrame>,
    inputs: CommandInputs,
    baseline: RefCell<(u64, i64)>,
}

impl ActiveCommand {
    fn install(&self) -> Result<InstalledCommand> {
        CURRENT_COMMAND.with(|current| {
            let mut current = current.borrow_mut();
            ensure!(current.is_none(), "nested Raft command");
            *current = Some(self.frame.clone());
            Ok(InstalledCommand)
        })
    }

    pub(crate) fn bind_dispatch(&self, context: RaftDispatchContext) -> Result<()> {
        self.frame
            .dispatch
            .set(context)
            .map_err(|_| anyhow::anyhow!("RPC dispatch already bound"))
    }

    pub(crate) fn turn<T>(
        &self,
        callback: impl FnOnce(&CommandTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let _installed = self.install()?;
        let _cursors = self.frame.cursors.install();
        self.frame.cursors.prune()?;
        drop(self.frame.connection()?);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let result = callback(&CommandTransaction {
                frame: &self.frame,
                inputs: self.inputs,
            })?;
            drop(self.frame.connection()?);
            Ok(result)
        }))
        .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
    }

    pub(crate) fn poll_work(&self, cx: &mut TaskContext<'_>) -> Result<bool> {
        self.poll_owned_work(cx, false)
    }

    pub(crate) fn can_poll_parked_io(&self) -> bool {
        let work = self.frame.work.borrow();
        work.active != 0
            && work.active == work.tasks.len()
            && work.checkpoint.is_none()
            && work.tasks.iter().all(|task| {
                task.parked_io.load(std::sync::atomic::Ordering::Acquire)
                    && task
                        .external
                        .as_ref()
                        .is_some_and(|proven| proven.load(std::sync::atomic::Ordering::Acquire))
            })
    }

    pub(crate) fn poll_parked_io(&self, cx: &mut TaskContext<'_>) -> Result<bool> {
        ensure!(
            self.can_poll_parked_io(),
            "parked work is not proven external I/O"
        );
        ensure!(
            self.frame.connection.borrow().is_none(),
            "parked I/O retains SQLite"
        );
        self.poll_owned_work(cx, true)
    }

    fn turn_parked_io<T>(&self, callback: impl FnOnce() -> Result<T>) -> Result<T> {
        let _installed = self.install()?;

        struct PollingIo<'a>(&'a Cell<bool>);

        impl Drop for PollingIo<'_> {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }
        self.frame.parked_io.set(true);
        let _polling = PollingIo(&self.frame.parked_io);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback))
            .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
    }

    fn poll_owned_work(&self, cx: &mut TaskContext<'_>, parked_io: bool) -> Result<bool> {
        let (count, serial) = {
            let work = self.frame.work.borrow();
            (work.tasks.len(), work.serial)
        };
        for _ in 0..count {
            let Some(mut task) = self.frame.work.borrow_mut().tasks.pop_front() else {
                break;
            };
            let mut poll = || {
                Ok(crate::application::consensus::RaftClass::poll_cell_work(
                    &mut task.future,
                    cx,
                ))
            };
            let polled = if parked_io {
                self.turn_parked_io(poll)
            } else {
                self.turn(|_| poll())
            }?;
            let mut work = self.frame.work.borrow_mut();
            match polled {
                Poll::Pending => work.tasks.push_back(task),
                Poll::Ready(result) => {
                    work.active -= 1;
                    result?;
                }
            }
        }
        let work = self.frame.work.borrow();
        if work.serial != serial {
            cx.waker().wake_by_ref();
        }
        Ok(work.active != 0)
    }

    pub(crate) fn can_park(&self) -> Result<bool> {
        self.frame.cursors.publishable()?;
        let work = self.frame.work.borrow();
        // Only a host-owned SDK task that has already proven its source root
        // can wait without SQLite. Transaction/sync tasks keep their writer.
        let external = work
            .tasks
            .iter()
            .filter(|task| {
                task.external
                    .as_ref()
                    .is_some_and(|waiting| waiting.load(std::sync::atomic::Ordering::Acquire))
            })
            .count();
        if work.active != external
            || work.checkpoint.is_some()
            || self
                .frame
                .storage
                .get()
                .is_some_and(|storage| !storage.raft_can_park())
        {
            return Ok(false);
        }
        let connection = self.frame.connection()?;
        if command_marker(&connection)? != *self.baseline.borrow() {
            return Ok(false);
        }
        // Temporary objects belong to this connection, rather than the durable
        // image. Preserve application objects by keeping their writer installed.
        // The sole exception is our protected dbstat module: open() recreates
        // it before installing the authorizer, and applications cannot alter it.
        super::as_framework_sql(|| {
            connection.query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM sqlite_temp_master WHERE name!='__sys_raft_capacity')",
                [],
                |row| row.get(0),
            )
        })
        .map_err(Into::into)
    }

    pub(crate) fn close_parked(self) -> Result<()> {
        self.cancel_work()?;
        ensure!(
            self.frame.connection.borrow().is_none(),
            "parked RPC retains SQLite"
        );
        self.frame.cursors.clear()?;
        Ok(())
    }

    pub(crate) fn retain_cursor_image(
        &self,
        database: *mut ffi::sqlite3,
        close: Box<dyn FnOnce() -> Result<()>>,
    ) -> Result<()> {
        self.frame.cursors.retain_image(database, close)
    }

    pub(crate) fn take_checkpoint(
        &self,
    ) -> Option<tokio::sync::oneshot::Sender<Result<(), String>>> {
        self.frame.work.borrow_mut().checkpoint.take()
    }

    pub(crate) fn cancel_work(&self) -> Result<()> {
        let mut tasks = {
            let mut work = self.frame.work.borrow_mut();
            work.active = 0;
            std::mem::take(&mut work.tasks)
        };
        let mut result = Ok(());
        while let Some(task) = tasks.pop_front() {
            if let Err(panic) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(task)))
            {
                result = Err(crate::native_worker::panic_error(panic));
            }
        }
        result
    }
}

fn command_marker(connection: &Connection) -> Result<(u64, i64)> {
    let schema = super::as_framework_sql(|| {
        connection.query_row("PRAGMA main.schema_version", [], |row| row.get(0))
    })?;
    Ok((connection.total_changes(), schema))
}

pub(super) fn with<T>(read: impl FnOnce(&Connection) -> T) -> Option<T> {
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let connection = current.as_ref()?.connection.try_borrow().ok()?;
        Some(read(connection.as_ref()?))
    })
}

/// Capture only a real ordinary RPC frame. Synchronous commands and immutable
/// queries cannot acquire an asynchronous dispatch permit.
pub(crate) fn capture_dispatch() -> Result<Option<RaftDispatch>> {
    ensure!(
        !super::immutable::active(),
        crate::application::consensus::CommandFailure::UnsupportedCapability
    );
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let Some(frame) = current.as_ref() else {
            return Ok(None);
        };
        let context = frame
            .dispatch
            .get()
            .cloned()
            .ok_or(crate::application::consensus::CommandFailure::UnsupportedCapability)?;
        ensure!(
            frame.work.borrow().limit != 0,
            crate::application::consensus::CommandFailure::UnsupportedCapability
        );
        let resources = frame
            .storage
            .get()
            .cloned()
            .context("RPC storage is not installed")?;
        Ok(Some(RaftDispatch::new(
            RaftStorageAuthority {
                resources,
                invocation: Some(frame.invocation.clone()),
            },
            context,
        )))
    })
}

pub(crate) fn binding_ordinal() -> Result<u64> {
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let frame = current
            .as_ref()
            .context("cell binding is outside its invocation")?;
        let work = frame.work.borrow();
        ensure!(
            work.limit != 0,
            crate::application::consensus::CommandFailure::UnsupportedCapability
        );
        ensure!(
            work.active < work.limit,
            crate::application::consensus::CommandFailure::Overloaded
        );
        let ordinal = frame
            .binding_serial
            .get()
            .checked_add(1)
            .filter(|value| *value <= 4096)
            .ok_or(crate::application::consensus::CommandFailure::Overloaded)?;
        frame.binding_serial.set(ordinal);
        Ok(ordinal)
    })
}

pub(super) fn with_mut<T>(write: impl FnOnce(&mut Connection) -> T) -> Option<T> {
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let mut connection = current.as_ref()?.connection.try_borrow_mut().ok()?;
        Some(write(connection.as_mut()?))
    })
}

pub(crate) fn require_storage(authority: &RaftStorageAuthority, healthy: bool) -> Result<()> {
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let frame = current
            .as_ref()
            .context("storage is outside its Raft command")?;
        ensure!(
            frame
                .storage
                .get()
                .is_some_and(|owner| Arc::ptr_eq(owner, &authority.resources))
                && authority
                    .invocation
                    .as_ref()
                    .is_none_or(|invocation| Arc::ptr_eq(invocation, &frame.invocation)),
            "storage belongs to another Raft command"
        );
        if healthy {
            ensure!(
                !authority
                    .resources
                    .retired
                    .load(std::sync::atomic::Ordering::Acquire),
                "Raft cell activation is retired"
            );
            frame.connection()?;
        }
        Ok(())
    })
}

/// A proven external task may poll without SQLite. This grants no Storage
/// access: ordinary handles still require an installed healthy connection.
pub(crate) fn require_dispatch_storage(authority: &RaftStorageAuthority) -> Result<()> {
    let parked_io = CURRENT_COMMAND.with(|current| {
        current
            .borrow()
            .as_ref()
            .is_some_and(|frame| frame.parked_io.get())
    });
    require_storage(authority, !parked_io)?;
    if parked_io {
        ensure!(
            !authority
                .resources
                .retired
                .load(std::sync::atomic::Ordering::Acquire),
            "Raft cell activation is retired"
        );
        CURRENT_COMMAND.with(|current| {
            if let Some(error) = current
                .borrow()
                .as_ref()
                .and_then(|frame| frame.failure.borrow().clone())
            {
                anyhow::bail!("{error}");
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// A framework task may request its writer while it is polling in the parked
/// I/O scope. Validate its original invocation even before the writer exists.
pub(crate) fn dispatch_has_writer(authority: &RaftStorageAuthority) -> Result<bool> {
    require_dispatch_storage(authority)?;
    CURRENT_COMMAND.with(|current| {
        Ok(current
            .borrow()
            .as_ref()
            .is_some_and(|frame| frame.connection.borrow().is_some()))
    })
}

/// Cached constructor Storage may enter subsequent invocations; cursors may not.
pub(crate) fn bind_storage(authority: &RaftStorageAuthority) -> Result<RaftStorageAuthority> {
    require_storage(authority, true)?;
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let frame = current
            .as_ref()
            .context("storage is outside its Raft command")?;
        Ok(RaftStorageAuthority {
            resources: authority.resources.clone(),
            invocation: Some(frame.invocation.clone()),
        })
    })
}

pub(crate) fn schedule_work(
    resources: &RaftStorageAuthority,
    task: HandlerFuture<()>,
) -> Result<()> {
    schedule(
        resources,
        task,
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
}

/// The flag becomes true only after exact source proof and release of the
/// ordered checkpoint slot. Application callbacks cannot mint this work kind.
pub(crate) fn schedule_dispatch_work(
    resources: &RaftStorageAuthority,
    task: HandlerFuture<()>,
    external: Arc<std::sync::atomic::AtomicBool>,
    parked_io: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    schedule(resources, task, Some(external), parked_io)
}

fn schedule(
    resources: &RaftStorageAuthority,
    task: HandlerFuture<()>,
    external: Option<Arc<std::sync::atomic::AtomicBool>>,
    parked_io: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    require_storage(resources, true)?;
    CURRENT_COMMAND.with(|current| {
        let current = current.borrow();
        let frame = current
            .as_ref()
            .context("storage is outside its Raft command")?;
        let mut work = frame.work.borrow_mut();
        ensure!(
            work.limit != 0,
            crate::application::consensus::CommandFailure::UnsupportedCapability
        );
        ensure!(
            work.active < work.limit,
            crate::application::consensus::CommandFailure::Overloaded
        );
        work.serial = work
            .serial
            .checked_add(1)
            .context("async work sequence exhausted")?;
        work.active += 1;
        work.tasks.push_back(WorkTask {
            future: task,
            external,
            parked_io,
        });
        Ok(())
    })
}

/// The root callback keeps its ordered slot and result until the SQL owner
/// proves this exact intermediate mutation. No application future commits SQL.
pub(crate) fn commit_work(resources: &RaftStorageAuthority) -> HandlerFuture<()> {
    let queued = (|| -> Result<_> {
        require_storage(resources, true)?;
        CURRENT_COMMAND.with(|current| {
            let current = current.borrow();
            let frame = current
                .as_ref()
                .context("storage is outside its Raft command")?;
            let mut work = frame.work.borrow_mut();
            ensure!(
                work.checkpoint.is_none(),
                "another RPC checkpoint is pending"
            );
            let (send, receive) = tokio::sync::oneshot::channel();
            work.serial = work
                .serial
                .checked_add(1)
                .context("async work sequence exhausted")?;
            work.checkpoint = Some(send);
            Ok(receive)
        })
    })();
    Box::pin(async move {
        queued?
            .await
            .context("RPC checkpoint owner was stopped")?
            .map_err(anyhow::Error::msg)
    })
}

pub(super) fn failure() -> Option<String> {
    CURRENT_COMMAND.with(|current| current.borrow().as_ref()?.failure.borrow().clone())
}

pub(super) fn poison(error: String) {
    CURRENT_COMMAND.with(|current| {
        if let Some(frame) = current.borrow().as_ref() {
            frame.failure.borrow_mut().get_or_insert(error);
        }
    });
}

pub(crate) struct CommandDatabase {
    connection: Option<Connection>,
}

impl CommandDatabase {
    /// Inspect the current framework-owned transaction before committing its
    /// captured bytes. This never visits application table payloads.
    pub(crate) fn capacity(connection: &Connection, quota: u64) -> Result<capacity::Usage> {
        capacity::measure(connection, quota)
    }

    /// Recheck after capture/checkpoint control writes. The private owner keeps
    /// every writer quiescent, and this read transaction binds all observations
    /// to one SQLite view without modifying the disposable image.
    pub(crate) fn frozen_capacity(&self, quota: u64) -> Result<capacity::Usage> {
        let connection = self.framework();
        ensure!(
            connection.is_autocommit(),
            "capacity inspection requires a quiescent writer"
        );
        super::as_storage_transaction(|| connection.execute_batch("BEGIN"))?;
        let result = capacity::measure(connection, quota);
        super::as_storage_transaction(|| connection.execute_batch("ROLLBACK"))
            .context("private capacity inspection rollback failed; rebuild is required")?;
        result
    }

    pub(crate) fn open(path: &Path, vfs: Option<&str>, max_image_bytes: u64) -> Result<Self> {
        let connection = match vfs {
            Some(vfs) => Connection::open_with_flags_and_vfs(path, OpenFlags::default(), vfs)?,
            None => Connection::open(path)?,
        };
        // This connection only mutates the disposable Raft working image. The
        // captured command and committed image have separate durability barriers;
        // recovery always discards this SQLite database and its sidecars.
        connection.execute_batch("PRAGMA synchronous=OFF; PRAGMA wal_autocheckpoint=0; PRAGMA temp_store=MEMORY; PRAGMA trusted_schema=OFF; PRAGMA cache_size=-64;")?;
        let page_size: u64 = connection.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        ensure!(
            cellar_ltx::ltx::is_valid_page_size(u32::try_from(page_size)?),
            "invalid command page size"
        );
        ensure!(
            max_image_bytes / page_size > 0,
            "command image limit is smaller than one page"
        );
        connection.pragma_update(None, "max_page_count", max_image_bytes / page_size)?;
        if let Some(limits) = crate::application::limits::current().sql.connection {
            // SAFETY: this live connection is exclusively owned and no statements
            // or transaction have been exposed. Limits do not mutate the image.
            unsafe {
                for (category, limit) in super::sqlite_limit_categories(limits) {
                    ffi::sqlite3_limit(connection.handle(), category, limit);
                }
            }
        }
        // SAFETY: the same exclusive, unused connection receives strict local
        // policies before any callback. No application can lower them through SQL.
        unsafe {
            ffi::sqlite3_limit(connection.handle(), ffi::SQLITE_LIMIT_ATTACHED, 0);
            ffi::sqlite3_limit(connection.handle(), ffi::SQLITE_LIMIT_WORKER_THREADS, 0);
            ensure!(
                ffi::sqlite3_db_config(
                    connection.handle(),
                    ffi::SQLITE_DBCONFIG_DEFENSIVE,
                    1,
                    std::ptr::null_mut::<i32>()
                ) == ffi::SQLITE_OK,
                "cannot enable defensive command SQLite"
            );
        }
        capacity::initialize(&connection)?;
        connection.authorizer(Some(authorize_command))?;
        Ok(Self {
            connection: Some(connection),
        })
    }

    /// Only fixed framework receipt/schema statements receive this connection.
    pub(crate) fn framework(&self) -> &Connection {
        self.connection
            .as_ref()
            .expect("working image owns its connection outside a command")
    }

    pub(crate) fn transaction<T>(
        &mut self,
        inputs: CommandInputs,
        callback: impl FnOnce(&CommandTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let active = self.begin(inputs, 0)?;
        let result = active.turn(callback);
        self.settle(active, result)
    }

    pub(crate) fn begin(
        &mut self,
        inputs: CommandInputs,
        max_tasks: usize,
    ) -> Result<ActiveCommand> {
        self.begin_with_activation(inputs, max_tasks, None)
    }

    pub(crate) fn begin_with_activation(
        &mut self,
        inputs: CommandInputs,
        max_tasks: usize,
        activation: Option<Arc<Resources>>,
    ) -> Result<ActiveCommand> {
        let connection = self
            .connection
            .as_ref()
            .context("private SQLite is unavailable")?;
        super::as_storage_transaction(|| connection.execute_batch("BEGIN IMMEDIATE"))?;
        let baseline = RefCell::new(command_marker(connection)?);
        Ok(ActiveCommand {
            baseline,
            frame: Rc::new(CommandFrame {
                cursors: super::raft_cursors::RaftCursors::default(),
                connection: RefCell::new(Some(
                    self.connection
                        .take()
                        .context("private SQLite is unavailable")?,
                )),
                storage: OnceCell::new(),
                activation,
                invocation: Arc::new(()),
                failure: RefCell::new(None),
                work: RefCell::new(Work {
                    tasks: VecDeque::new(),
                    active: 0,
                    limit: max_tasks,
                    serial: 0,
                    checkpoint: None,
                }),
                dispatch: OnceCell::new(),
                binding_serial: Cell::new(0),
                parked_io: Cell::new(false),
            }),
            inputs,
        })
    }

    /// Temporarily return the quiescent writer for LTX capture while the
    /// invocation retains its callbacks, Storage identity and transaction slots.
    pub(crate) fn checkpoint(&mut self, active: &ActiveCommand) -> Result<()> {
        ensure!(
            self.connection.is_none(),
            "private SQLite already belongs to capture"
        );
        active.frame.cursors.publishable()?;
        {
            let connection = active.frame.connection()?;
            super::as_storage_transaction(|| connection.execute_batch("COMMIT"))?;
        }
        self.connection = active.frame.connection.borrow_mut().take();
        ensure!(self.connection.is_some(), "checkpoint writer missing");
        Ok(())
    }

    pub(crate) fn continue_checkpoint(&mut self, active: &ActiveCommand) -> Result<()> {
        ensure!(
            active.frame.connection.borrow().is_none(),
            "RPC writer already installed"
        );
        let connection = self
            .connection
            .as_ref()
            .context("checkpoint writer missing")?;
        super::as_storage_transaction(|| connection.execute_batch("BEGIN IMMEDIATE"))?;
        *active.baseline.borrow_mut() = command_marker(connection)?;
        *active.frame.connection.borrow_mut() = self.connection.take();
        Ok(())
    }

    pub(crate) fn park(&mut self, active: &ActiveCommand) -> Result<()> {
        ensure!(
            self.connection.is_none() && active.can_park()?,
            "RPC writer is not quiescent"
        );
        // Idle cached statements also refer to this connection. A resumed RPC
        // may use a different working image, so none can cross a park.
        active.frame.cursors.park();
        {
            let connection = active.frame.connection()?;
            super::as_storage_transaction(|| connection.execute_batch("ROLLBACK"))?;
        }
        self.connection = active.frame.connection.borrow_mut().take();
        ensure!(self.connection.is_some(), "parked RPC writer missing");
        Ok(())
    }

    pub(crate) fn settle<T>(&mut self, active: ActiveCommand, result: Result<T>) -> Result<T> {
        let result = result.and_then(|value| {
            active.frame.cursors.publishable()?;
            active.frame.cursors.clear()?;
            let connection = active.frame.connection()?;
            super::as_storage_transaction(|| connection.execute_batch("COMMIT"))?;
            Ok(value)
        });
        self.restore(active, result.is_err())?;
        result
    }

    pub(crate) fn abort(&mut self, active: ActiveCommand) -> Result<()> {
        self.restore(active, true)
    }

    fn restore(&mut self, active: ActiveCommand, rollback: bool) -> Result<()> {
        // Storage handles retain only their authority, never the connection.
        let frame = Rc::try_unwrap(active.frame)
            .ok()
            .context("private SQLite is still installed")?;
        frame.cursors.clear()?;
        match (self.connection.is_some(), frame.connection.into_inner()) {
            (false, Some(connection)) => self.connection = Some(connection),
            (true, None) => (),
            _ => anyhow::bail!("private SQLite connection ownership differs"),
        }
        let connection = self.framework();
        if rollback && !connection.is_autocommit() {
            super::as_storage_transaction(|| connection.execute_batch("ROLLBACK"))
                .context("private command rollback failed; rebuild is required")?;
        }
        Ok(())
    }

    pub(crate) fn geometry(&self, max_image_bytes: u64) -> Result<(u64, u32)> {
        let connection = self.framework();
        ensure!(
            connection.is_autocommit(),
            "cannot freeze an active command transaction"
        );
        let size: u32 = connection.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let pages: u64 = connection.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let length = pages
            .checked_mul(size as u64)
            .context("image length overflow")?;
        ensure!(
            length > 0 && length <= max_image_bytes,
            "private image exceeds its bound"
        );
        Ok((length, size))
    }
}

fn authorize_command(context: AuthContext<'_>) -> Authorization {
    if super::authorize_sql(context) != Authorization::Allow {
        return Authorization::Deny;
    }
    if !super::USER_SQL.with(std::cell::Cell::get) {
        return Authorization::Allow;
    }
    match context.action {
        AuthAction::Pragma { .. } => Authorization::Deny,
        AuthAction::Function { function_name }
            if matches!(
                function_name.to_ascii_lowercase().as_str(),
                "random"
                    | "randomblob"
                    | "date"
                    | "time"
                    | "datetime"
                    | "julianday"
                    | "strftime"
                    | "unixepoch"
                    | "timediff"
                    | "current_date"
                    | "current_time"
                    | "current_timestamp"
            ) =>
        {
            Authorization::Deny
        }
        _ => Authorization::Allow,
    }
}
