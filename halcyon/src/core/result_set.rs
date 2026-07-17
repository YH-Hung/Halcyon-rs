//! The streaming result cursor and its row view.
//!
//! [`ResultSet`] is a forward-only lending cursor over an executed statement,
//! ported from the C++ `ResultSet`. It requests row-major [`Value`] blocks from
//! the portable driver seam and retains one block at a time. An empty block is
//! clean EOF; a driver failure is retained so the owning statement/connection
//! is poisoned on drop. Mapping a buffered value is client-side and never
//! poisons the connection.

use std::fmt;
use std::marker::PhantomData;

use crate::core::column::FromColumn;
use crate::core::connection::Statement;
use crate::core::mapping;
use crate::error::{Error, Result};
use crate::value::Value;

/// Rows requested on each portable driver call. Drivers may return a smaller
/// block for byte-budget or transport reasons; only an empty block is EOF.
pub(crate) const FETCH_BLOCK_ROWS: usize = 4096;

/// Non-owning view of one materialized cursor row. Read a column with
/// [`Row::column`], or map the whole row to a tuple with [`Row::get`]. Valid
/// only while it is the live row of its [`ResultSet`].
pub struct Row<'a> {
    cells: &'a [Value],
}

impl<'a> Row<'a> {
    /// Builds a view over already-materialized row values. Used by the core and
    /// pooled cursor implementations after successful `fetch_block` calls.
    pub(crate) fn new(cells: &'a [Value]) -> Self {
        Row { cells }
    }

    pub fn column_count(&self) -> usize {
        self.cells.len()
    }

    /// Reads the 0-based `index` column as `T`. Mapping errors are local to the
    /// caller: the driver already returned the entire block successfully.
    pub fn column<T: FromColumn>(&self, index: usize) -> Result<T> {
        let value = self.cells.get(index).cloned().ok_or_else(|| {
            mapping(format!(
                "column index {index} out of range (row has {} columns)",
                self.cells.len()
            ))
        })?;
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
            .field("columns", &self.cells.len())
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
    block: Vec<Vec<Value>>,
    position: usize,
    error: Option<Error>,
    done: bool,
}

impl<'c> ResultSet<'c> {
    /// Wraps a statement whose `execute` has already run and whose column count
    /// is known. Called by `Connection`/`Statement` after executing a query.
    pub(crate) fn from_executed(stmt: Statement<'c>, columns: usize) -> Self {
        ResultSet {
            stmt,
            columns,
            block: Vec::new(),
            position: 0,
            error: None,
            done: false,
        }
    }

    pub fn column_count(&self) -> usize {
        self.columns
    }

    /// The driver-side failure that spoiled the scan, if any: empty after a
    /// clean end, set after a mid-stream block-fetch failure. Client-side
    /// mapping errors are not recorded here.
    pub fn error(&self) -> Option<Error> {
        self.error.clone()
    }

    /// Records a driver-side failure, keeping the first one seen.
    fn poison(&mut self, e: Error) {
        if self.error.is_none() {
            self.error = Some(e);
        }
    }

    /// Advances to the next row. `Ok(Some(row))` while rows remain, `Ok(None)`
    /// at a clean end, `Err` on a block-fetch failure (also retained in
    /// [`error`](ResultSet::error)). Once it yields `None`/`Err` it stays done.
    ///
    /// Not an [`Iterator`]: the yielded [`Row`] borrows the cursor (a lending
    /// iterator), which `std::iter::Iterator` can't express — use `while let` or
    /// [`query_map`](ResultSet::query_map) for owned-item iteration.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row<'_>>> {
        if self.done {
            return Ok(None);
        }
        if self.position == self.block.len() {
            let handle = self.stmt.handle();
            match self.stmt.driver_ref().fetch_block(handle, FETCH_BLOCK_ROWS) {
                Ok(block) if block.is_empty() => {
                    self.done = true;
                    return Ok(None);
                }
                Ok(block) => {
                    self.block = block;
                    self.position = 0;
                }
                Err(e) => {
                    self.done = true;
                    self.poison(e.clone());
                    return Err(e);
                }
            }
        }

        let row = Row::new(&self.block[self.position]);
        self.position += 1;
        Ok(Some(row))
    }

    /// Adapts the cursor into an `Iterator` of owned `Result<T>`, mapping each
    /// row with `f`. A mid-stream error appears as a final `Err` item.
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

    /// Materializes all remaining rows via [`FromRow`]. A mid-stream driver
    /// error or client-side mapping error short-circuits to `Err`.
    pub fn collect_rows<T: FromRow>(&mut self) -> Result<Vec<T>> {
        let mut out = Vec::new();
        while let Some(row) = self.next()? {
            out.push(T::from_row(&row)?);
        }
        Ok(out)
    }
}

impl Drop for ResultSet<'_> {
    fn drop(&mut self) {
        // A driver-side block-fetch failure may leave the handle mid-cursor:
        // poison the statement so a cached entry is finalized, not reused.
        if self.error.is_some() {
            self.stmt.poison();
        }
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
