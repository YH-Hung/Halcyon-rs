//! The streaming result cursor and its row view.
//!
//! [`ResultSet`] is a forward-only cursor over an executed statement, ported
//! from the C++ `ResultSet`. It is driven one row at a time by [`ResultSet::next`]
//! (a lending cursor — each [`Row`] borrows the set, so only one row is live at a
//! time and no per-row allocation is forced). A driver-side failure mid-scan —
//! whether a `fetch` or a column read — is surfaced, never collapsed into
//! end-of-cursor: the failing call returns the error and [`ResultSet::error`]
//! retains it, the signal the pool layer uses to discard a dropped connection.
//! A client-side mapping error (wrong target type, NULL into a non-`Option`) is
//! *not* recorded — it doesn't mean the connection is bad.
//!
//! For idiomatic iteration, [`ResultSet::query_map`] adapts the cursor into an
//! `Iterator` of owned `Result<T>` (sidestepping the lending borrow), and
//! [`ResultSet::collect_rows`] materializes all rows via [`FromRow`].

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;

use crate::core::column::FromColumn;
use crate::core::connection::Statement;
use crate::core::mapping;
use crate::driver::{CliDriver, StatementHandle};
use crate::error::{Error, Result};

/// Non-owning view of the cursor's current row. Read one column with
/// [`Row::column`], or map the whole row to a tuple with [`Row::get`]. Valid
/// only while it is the live row of its [`ResultSet`].
pub struct Row<'a> {
    driver: &'a dyn CliDriver,
    stmt: StatementHandle,
    columns: usize,
    // The owning cursor's error slot, so a driver-side read failure is recorded
    // there (mirrors the C++ `Row::record_read_error` -> `note_column_error`).
    errors: &'a RefCell<Option<Error>>,
}

impl Row<'_> {
    pub fn column_count(&self) -> usize {
        self.columns
    }

    /// Reads the 0-based `index` column as `T`. `Mapping` error on a bad index,
    /// a type mismatch, or NULL into a non-`Option` target.
    ///
    /// A driver-side read failure (e.g. a connection drop during `SQLGetData`) is
    /// recorded on the owning [`ResultSet`] — see [`ResultSet::error`] — as well
    /// as returned; a client-side mapping error is only returned.
    pub fn column<T: FromColumn>(&self, index: usize) -> Result<T> {
        if index >= self.columns {
            // Client-side misuse, not a driver failure: return but don't record.
            return Err(mapping(format!(
                "column index {index} out of range (row has {} columns)",
                self.columns
            )));
        }
        let value = match self.driver.get_column(self.stmt, index) {
            Ok(v) => v,
            Err(e) => {
                // Driver-side read failure: poison the cursor so the pool can
                // discard the connection. A `from_column` mapping error below is
                // client-side and deliberately left unrecorded (as in C++).
                let mut slot = self.errors.borrow_mut();
                if slot.is_none() {
                    *slot = Some(e.clone());
                }
                return Err(e);
            }
        };
        T::from_column(value)
    }

    /// Maps the whole row to `T` (a tuple of column types), checking arity.
    /// The Rust analog of the C++ `Row::as<Ts...>()`.
    pub fn get<T: FromRow>(&self) -> Result<T> {
        T::from_row(self)
    }
}

impl fmt::Debug for Row<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row")
            .field("stmt", &self.stmt)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

/// Maps a whole cursor row into `Self`. Implemented for tuples of [`FromColumn`]
/// types up to arity 12; `#[derive(FromRow)]` for structs arrives in Phase 2.
pub trait FromRow: Sized {
    fn from_row(row: &Row<'_>) -> Result<Self>;
}

macro_rules! impl_from_row_tuple {
    ($count:literal; $($T:ident $idx:tt),+) => {
        impl<$($T: FromColumn),+> FromRow for ($($T,)+) {
            fn from_row(row: &Row<'_>) -> Result<Self> {
                if row.column_count() != $count {
                    return Err(mapping(format!(
                        "row mapping expected {} column(s), got {}",
                        $count,
                        row.column_count()
                    )));
                }
                Ok(($(row.column::<$T>($idx)?,)+))
            }
        }
    };
}
impl_from_row_tuple!(1; A 0);
impl_from_row_tuple!(2; A 0, B 1);
impl_from_row_tuple!(3; A 0, B 1, C 2);
impl_from_row_tuple!(4; A 0, B 1, C 2, D 3);
impl_from_row_tuple!(5; A 0, B 1, C 2, D 3, E 4);
impl_from_row_tuple!(6; A 0, B 1, C 2, D 3, E 4, F 5);
impl_from_row_tuple!(7; A 0, B 1, C 2, D 3, E 4, F 5, G 6);
impl_from_row_tuple!(8; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
impl_from_row_tuple!(9; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
impl_from_row_tuple!(10; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
impl_from_row_tuple!(11; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
impl_from_row_tuple!(12; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

/// Forward-only cursor over an executed statement. Owns the statement, closing
/// its cursor and finalizing it on drop. Move-only.
#[derive(Debug)]
pub struct ResultSet<'c> {
    stmt: Statement<'c>,
    columns: usize,
    // `RefCell` so a `Row` (which holds only a shared `&self`) can record a
    // driver-side read failure here without the cursor being borrowed mutably.
    error: RefCell<Option<Error>>,
    done: bool,
}

impl<'c> ResultSet<'c> {
    /// Wraps a statement whose `execute` has already run and whose column count
    /// is known. Called by `Connection`/`Statement` after executing a query.
    pub(crate) fn from_executed(stmt: Statement<'c>, columns: usize) -> Self {
        ResultSet {
            stmt,
            columns,
            error: RefCell::new(None),
            done: false,
        }
    }

    pub fn column_count(&self) -> usize {
        self.columns
    }

    /// The driver-side failure that spoiled the scan, if any: empty after a clean
    /// end, set after a mid-stream `fetch` or column-read error. Distinguishes a
    /// clean end-of-cursor from a Db2 error, and is the signal the pool uses to
    /// discard the connection. Client-side mapping errors are not recorded here.
    pub fn error(&self) -> Option<Error> {
        self.error.borrow().clone()
    }

    /// Records a driver-side failure, keeping the first one seen.
    fn poison(&self, e: &Error) {
        let mut slot = self.error.borrow_mut();
        if slot.is_none() {
            *slot = Some(e.clone());
        }
    }

    /// Advances to the next row. `Ok(Some(row))` while rows remain, `Ok(None)` at
    /// a clean end, `Err` on a mid-stream fetch failure (also retained in
    /// [`error`](ResultSet::error)). Once it yields `None`/`Err` it stays done.
    ///
    /// Not an [`Iterator`]: the yielded [`Row`] borrows the cursor (a lending
    /// iterator), which `std::iter::Iterator` can't express — use `while let` here
    /// or [`query_map`](ResultSet::query_map) for owned-item iteration.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row<'_>>> {
        if self.done {
            return Ok(None);
        }
        let handle = self.stmt.handle();
        match self.stmt.driver_ref().fetch(handle) {
            Ok(true) => Ok(Some(Row {
                driver: self.stmt.driver_ref(),
                stmt: handle,
                columns: self.columns,
                errors: &self.error,
            })),
            Ok(false) => {
                self.done = true;
                Ok(None)
            }
            Err(e) => {
                self.done = true;
                self.poison(&e);
                Err(e)
            }
        }
    }

    /// Adapts the cursor into an `Iterator` of owned `Result<T>`, mapping each row
    /// with `f`. Because each item is owned, this composes with `for`/`.collect()`
    /// despite the lending cursor underneath. A mid-stream error appears as a
    /// final `Err` item, after which the iterator ends.
    pub fn query_map<T, F>(self, f: F) -> QueryMap<'c, T, F>
    where
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        QueryMap {
            rs: self,
            f,
            _out: PhantomData,
        }
    }

    /// Materializes all remaining rows via [`FromRow`]. A mid-stream error (fetch
    /// or mapping) short-circuits to `Err`.
    pub fn collect_rows<T: FromRow>(&mut self) -> Result<Vec<T>> {
        let mut out = Vec::new();
        while let Some(row) = self.next()? {
            out.push(T::from_row(&row)?);
        }
        Ok(out)
    }
}

/// Owned-item iterator produced by [`ResultSet::query_map`].
pub struct QueryMap<'c, T, F> {
    rs: ResultSet<'c>,
    f: F,
    _out: PhantomData<fn() -> T>,
}

impl<T, F> Iterator for QueryMap<'_, T, F>
where
    F: FnMut(&Row<'_>) -> Result<T>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rs.next() {
            Ok(Some(row)) => Some((self.f)(&row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
