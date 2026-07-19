//! SQL savepoints shared by standalone and pooled transactions.
//!
//! A savepoint name is interpolated into SQL because Db2 CLI does not bind
//! identifiers. The ASCII-only validator below is therefore a hard injection
//! boundary. The RAII guard follows the C++ `Savepoint` in
//! `include/halcyon/transaction.hpp`: it rolls back and releases on drop, and a
//! failed terminal operation poisons both the guard and its transaction.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::fmt;

use crate::error::{Error, ErrorCode, Result};

/// The per-transaction state shared by every live [`Savepoint`] guard.
///
/// Transactions are deliberately `!Sync`, so `Cell`/`RefCell` retain the
/// single-owner discipline without adding locking to every savepoint call.
pub(crate) struct SavepointState {
    counter: Cell<u64>,
    live_names: RefCell<BTreeSet<String>>,
}

impl SavepointState {
    pub(crate) fn new() -> Self {
        Self {
            counter: Cell::new(0),
            live_names: RefCell::new(BTreeSet::new()),
        }
    }

    fn next_name(&self) -> String {
        // The counter is only a name source. Wrapping keeps this public API
        // panic-free even after an impractically large number of savepoints.
        let next = self.counter.get().wrapping_add(1);
        self.counter.set(next);
        format!("halcyon_sp_{next}")
    }

    /// Reserves `name` until a guard terminally completes. Names are folded as
    /// Db2 folds unquoted identifiers, so case variants reserve the same slot.
    fn reserve(&self, name: &str) -> bool {
        self.live_names.borrow_mut().insert(normalize_name(name))
    }

    fn occupied(&self, name: &str) -> bool {
        self.live_names.borrow().contains(&normalize_name(name))
    }

    fn free(&self, name: &str) {
        self.live_names.borrow_mut().remove(&normalize_name(name));
    }
}

/// Narrow common seam for the two public transaction wrappers. It keeps all
/// savepoint validation, registry management, terminal SQL, and poison rules in
/// this module rather than duplicating them between `Transaction` and
/// `ScopedTransaction`.
pub(crate) trait SavepointOwner {
    fn execute_savepoint(&self, sql: &str) -> Result<i64>;
    fn savepoint_active(&self) -> bool;
    fn poison_from_savepoint(&self);
    fn savepoint_state(&self) -> &SavepointState;
}

/// One-shot RAII guard over a SQL savepoint in either transaction wrapper.
///
/// [`rollback`](Self::rollback) undoes work after the savepoint;
/// [`release`](Self::release) keeps it. Dropping a live guard executes both
/// `ROLLBACK TO SAVEPOINT` and `RELEASE SAVEPOINT`, in that order.
pub struct Savepoint<'a> {
    owner: &'a dyn SavepointOwner,
    name: String,
    active: Cell<bool>,
    poisoned: Cell<bool>,
}

impl Savepoint<'_> {
    /// The validated, unquoted identifier used in the savepoint SQL.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// True until the savepoint is rolled back, released, or dropped.
    pub fn active(&self) -> bool {
        self.active.get()
    }

    /// True if one of this guard's terminal SQL statements failed.
    pub fn poisoned(&self) -> bool {
        self.poisoned.get()
    }

    /// Rolls back to this savepoint. A second terminal call is a no-op.
    pub fn rollback(&self) -> Result<()> {
        if !self.deactivate() {
            return Ok(());
        }
        self.run_terminal(&format!("ROLLBACK TO SAVEPOINT {}", self.name))
    }

    /// Releases this savepoint while retaining the work that followed it. A
    /// second terminal call is a no-op.
    pub fn release(&self) -> Result<()> {
        if !self.deactivate() {
            return Ok(());
        }
        self.run_terminal(&format!("RELEASE SAVEPOINT {}", self.name))
    }

    fn deactivate(&self) -> bool {
        if !self.active.replace(false) {
            return false;
        }
        self.owner.savepoint_state().free(&self.name);
        true
    }

    fn run_terminal(&self, sql: &str) -> Result<()> {
        match self.owner.execute_savepoint(sql) {
            Ok(_) => Ok(()),
            Err(error) => {
                self.poison();
                Err(error)
            }
        }
    }

    fn poison(&self) {
        if !self.poisoned.replace(true) {
            crate::obs::emit_savepoint_poisoned(&self.name);
            self.owner.poison_from_savepoint();
        }
    }
}

impl Drop for Savepoint<'_> {
    fn drop(&mut self) {
        if !self.deactivate() {
            return;
        }

        // Run RELEASE even when ROLLBACK TO fails: this is the C++ destructor's
        // cleanup ordering and gives Db2 its best chance to release the server
        // savepoint before the enclosing transaction ends.
        let rollback = self
            .owner
            .execute_savepoint(&format!("ROLLBACK TO SAVEPOINT {}", self.name));
        let release = self
            .owner
            .execute_savepoint(&format!("RELEASE SAVEPOINT {}", self.name));
        if rollback.is_err() || release.is_err() {
            self.poison();
        }
    }
}

impl fmt::Debug for Savepoint<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Savepoint")
            .field("name", &self.name)
            .field("active", &self.active.get())
            .field("poisoned", &self.poisoned.get())
            .finish_non_exhaustive()
    }
}

pub(crate) fn auto_savepoint<'a>(owner: &'a dyn SavepointOwner) -> Result<Savepoint<'a>> {
    let state = owner.savepoint_state();
    // Skip generated names occupied by live explicit savepoints (e.g. a user's
    // own `halcyon_sp_1`). The live set is finite and each iteration advances
    // the counter, so a free numeric suffix is always reached.
    loop {
        let name = state.next_name();
        if !state.occupied(&name) {
            return make_savepoint(owner, name);
        }
    }
}

pub(crate) fn named_savepoint<'a>(
    owner: &'a dyn SavepointOwner,
    name: &str,
) -> Result<Savepoint<'a>> {
    if !valid_name(name) {
        return Err(Error::new(
            ErrorCode::InvalidArgument,
            format!(
                "invalid savepoint name '{name}': must match [A-Za-z_][A-Za-z0-9_]{{0,127}} and not begin with SYS"
            ),
        ));
    }
    make_savepoint(owner, name.to_owned())
}

pub(crate) fn nested_savepoint<T>(
    owner: &dyn SavepointOwner,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let savepoint = auto_savepoint(owner)?;
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(value)) => {
            savepoint.release()?;
            Ok(value)
        }
        Ok(Err(error)) => {
            // Preserve the caller's error, as the C++ `nested` does. A failed
            // rollback still poisons the guard and owner through `rollback`.
            let _ = savepoint.rollback();
            Err(error)
        }
        Err(panic) => {
            // Match C++ `nested`'s exception path: the explicit rollback
            // disarms the guard, so unwinding does not also issue RELEASE.
            let _ = savepoint.rollback();
            std::panic::resume_unwind(panic)
        }
    }
}

fn make_savepoint<'a>(owner: &'a dyn SavepointOwner, name: String) -> Result<Savepoint<'a>> {
    if !owner.savepoint_active() {
        return Err(Error::new(
            ErrorCode::InvalidState,
            "savepoint requires an active transaction",
        ));
    }
    if !owner.savepoint_state().reserve(&name) {
        return Err(Error::new(
            ErrorCode::InvalidArgument,
            format!("savepoint name '{name}' is already active in this transaction"),
        ));
    }

    if let Err(error) =
        owner.execute_savepoint(&format!("SAVEPOINT {name} ON ROLLBACK RETAIN CURSORS"))
    {
        owner.savepoint_state().free(&name);
        return Err(error);
    }

    Ok(Savepoint {
        owner,
        name,
        active: Cell::new(true),
        poisoned: Cell::new(false),
    })
}

/// `[A-Za-z_][A-Za-z0-9_]{0,127}`, ASCII-only, and not `SYS...` in any ASCII
/// case. This remains locale-independent so no process setting can weaken the
/// SQL-identifier injection boundary.
fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 128 {
        return false;
    }
    if !ascii_alpha(bytes[0]) && bytes[0] != b'_' {
        return false;
    }
    if !bytes[1..]
        .iter()
        .copied()
        .all(|byte| ascii_alnum(byte) || byte == b'_')
    {
        return false;
    }
    !(bytes.len() >= 3
        && ascii_upper(bytes[0]) == b'S'
        && ascii_upper(bytes[1]) == b'Y'
        && ascii_upper(bytes[2]) == b'S')
}

fn normalize_name(name: &str) -> String {
    name.bytes().map(|byte| ascii_upper(byte) as char).collect()
}

fn ascii_alpha(byte: u8) -> bool {
    byte.is_ascii_alphabetic()
}

fn ascii_alnum(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

fn ascii_upper(byte: u8) -> u8 {
    byte.to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use crate::core::Connection;
    use crate::driver::ConnectionParams;
    use crate::error::{Error, ErrorCode, Result};
    use crate::testing::MockCliDriver;
    use std::sync::Arc;

    fn params() -> ConnectionParams {
        ConnectionParams {
            connection_string: "DSN=mock".into(),
            isolation: None,
        }
    }

    fn connect(mock: &Arc<MockCliDriver>) -> Connection {
        Connection::open(mock.clone(), &params()).unwrap()
    }

    #[test]
    fn savepoint_named_rejects_sys_prefix_and_injection() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();

        for name in ["SYSFOO", "sysx", "a;drop", "", "\u{e9}"] {
            match tx.savepoint_named(name) {
                Err(error) => assert_eq!(error.code, ErrorCode::InvalidArgument),
                Ok(_) => panic!("{name:?} must be rejected"),
            }
        }

        assert!(mock.prepared_sql().is_empty());
    }

    #[test]
    fn savepoint_raii_rollback_on_drop() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();

        {
            let _savepoint = tx.savepoint_named("stage_one").unwrap();
        }

        assert_eq!(
            mock.prepared_sql(),
            vec![
                "SAVEPOINT stage_one ON ROLLBACK RETAIN CURSORS",
                "ROLLBACK TO SAVEPOINT stage_one",
                "RELEASE SAVEPOINT stage_one",
            ]
        );
    }

    #[test]
    fn nested_releases_on_ok_rolls_back_on_err() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();

        assert_eq!(tx.nested(|_| Ok::<_, Error>(7)).unwrap(), 7);
        let error = tx
            .nested(|_| Err::<(), _>(Error::new(ErrorCode::Constraint, "duplicate")))
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Constraint);
        assert_eq!(
            mock.prepared_sql(),
            vec![
                "SAVEPOINT halcyon_sp_1 ON ROLLBACK RETAIN CURSORS",
                "RELEASE SAVEPOINT halcyon_sp_1",
                "SAVEPOINT halcyon_sp_2 ON ROLLBACK RETAIN CURSORS",
                "ROLLBACK TO SAVEPOINT halcyon_sp_2",
            ]
        );
    }

    #[test]
    fn nested_panic_rolls_back_without_release() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _: Result<()> = tx.nested(|_| panic!("boom"));
        }));

        assert!(panic.is_err());
        assert_eq!(
            mock.prepared_sql(),
            vec![
                "SAVEPOINT halcyon_sp_1 ON ROLLBACK RETAIN CURSORS",
                "ROLLBACK TO SAVEPOINT halcyon_sp_1",
            ]
        );
    }

    #[test]
    fn live_name_reuse_is_invalid_argument_case_insensitively() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();
        let savepoint = tx.savepoint_named("stage_one").unwrap();

        match tx.savepoint_named("STAGE_ONE") {
            Err(error) => assert_eq!(error.code, ErrorCode::InvalidArgument),
            Ok(_) => panic!("a live savepoint name must not be reused"),
        }
        assert_eq!(mock.prepared_sql().len(), 1);

        drop(savepoint);
        tx.savepoint_named("STAGE_ONE").unwrap().release().unwrap();
    }

    #[test]
    fn failed_terminal_savepoint_operation_poisons_savepoint_and_transaction() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();
        let savepoint = tx.savepoint_named("stage_one").unwrap();
        mock.push_execute_error(Error::new(ErrorCode::Connection, "lost"));

        assert_eq!(savepoint.release().unwrap_err().code, ErrorCode::Connection);
        assert!(savepoint.poisoned());
        assert!(tx.poisoned());
    }

    #[test]
    fn automatic_names_skip_live_explicit_savepoints() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();

        // A live explicit savepoint occupies the first automatic name
        // (case-folded); generation must skip it, not fail.
        let explicit = tx.savepoint_named("HALCYON_SP_1").unwrap();
        let auto = tx.savepoint().unwrap();
        assert_eq!(auto.name(), "halcyon_sp_2");

        auto.release().unwrap();
        explicit.release().unwrap();
    }

    #[test]
    fn automatic_names_increment_per_transaction() {
        let mock = Arc::new(MockCliDriver::new());
        let conn = connect(&mock);
        let tx = conn.begin().unwrap();

        let first = tx.savepoint().unwrap();
        let second = tx.savepoint().unwrap();

        assert_eq!(first.name(), "halcyon_sp_1");
        assert_eq!(second.name(), "halcyon_sp_2");
    }
}
