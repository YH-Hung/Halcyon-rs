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
use crate::core::connection::{Connection, Statement};
use crate::core::mapping;
use crate::driver::StatementHandle;
use crate::error::{Error, Result};
use crate::value::Value;

/// Rows requested on each portable driver call. Drivers may return a smaller
/// block for byte-budget or transport reasons; only an empty block is EOF.
pub(crate) const FETCH_BLOCK_ROWS: usize = 4096;

/// Non-owning view of one materialized row — normally the live row of a
/// [`ResultSet`], valid only until the cursor advances. Read a column with
/// [`Row::column`], or map the whole row to a tuple with [`Row::get`].
pub struct Row<'a> {
    cells: &'a [Value],
}

impl<'a> Row<'a> {
    /// Builds a view over row values in column order — what a cursor yields,
    /// and a way to unit-test a [`FromRow`] mapping with no driver:
    ///
    /// ```
    /// use halcyon::{FromRow, Row, Value};
    ///
    /// struct User {
    ///     id: i64,
    ///     name: String,
    /// }
    ///
    /// impl FromRow for User {
    ///     fn from_row(row: &Row<'_>) -> halcyon::Result<Self> {
    ///         Ok(User { id: row.column(0)?, name: row.column(1)? })
    ///     }
    /// }
    ///
    /// let cells = [Value::Int(7), Value::Text("ann".into())];
    /// let user = User::from_row(&Row::new(&cells))?;
    /// assert_eq!((user.id, user.name.as_str()), (7, "ann"));
    /// # Ok::<(), halcyon::Error>(())
    /// ```
    pub fn new(cells: &'a [Value]) -> Self {
        Row { cells }
    }

    /// Number of columns in the row.
    pub fn column_count(&self) -> usize {
        self.cells.len()
    }

    /// Borrows the buffered value at the 0-based `index` without copying, or
    /// `None` when the index is out of range. Use this for large cells (LOBs)
    /// where the owned conversion of [`column`](Row::column) would clone the
    /// buffered bytes.
    pub fn value(&self, index: usize) -> Option<&'a Value> {
        self.cells.get(index)
    }

    /// Reads the 0-based `index` column as `T`. Mapping errors are local to the
    /// caller: the driver already returned the entire block successfully.
    ///
    /// Returning owned data copies a variable-length cell once (the same cost
    /// as the C++ `TypeBinder::from_value`); [`value`](Row::value) borrows the
    /// buffered cell instead.
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
/// types up to arity 12, and for structs by `#[derive(FromRow)]` (feature
/// `derive`), which maps columns to fields by position.
pub trait FromRow: Sized {
    /// Builds `Self` from one row, failing with
    /// [`ErrorCode::Mapping`](crate::ErrorCode::Mapping) when the column count
    /// or a column's type does not match.
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

/// Block-buffered scan state shared by the borrowed [`ResultSet`] and the
/// pooled [`QueryResult`](crate::QueryResult), which differ only in how they
/// own the statement and its connection.
#[derive(Debug)]
pub(crate) struct BlockState {
    stmt: StatementHandle,
    columns: usize,
    block: Vec<Vec<Value>>,
    position: usize,
    error: Option<Error>,
    done: bool,
}

impl BlockState {
    pub(crate) fn new(stmt: StatementHandle, columns: usize) -> Self {
        BlockState {
            stmt,
            columns,
            block: Vec::new(),
            position: 0,
            error: None,
            done: false,
        }
    }

    pub(crate) fn column_count(&self) -> usize {
        self.columns
    }

    pub(crate) fn error(&self) -> Option<Error> {
        self.error.clone()
    }

    /// Advances to the next row, fetching a new block from `conn` when the
    /// buffered one is spent. A fetch failure is retained, retires `conn` if
    /// it reports the connection lost, and ends the scan.
    pub(crate) fn next(&mut self, conn: &Connection) -> Result<Option<Row<'_>>> {
        if self.done {
            return Ok(None);
        }
        if self.position == self.block.len() {
            match conn.driver_ref().fetch_block(self.stmt, FETCH_BLOCK_ROWS) {
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
                    conn.note_failure(&e);
                    self.error = Some(e.clone());
                    return Err(e);
                }
            }
        }

        let row = Row::new(&self.block[self.position]);
        self.position += 1;
        Ok(Some(row))
    }
}

/// Forward-only cursor over an executed statement. Owns the statement, closing
/// its cursor and finalizing it on drop. Move-only.
#[derive(Debug)]
pub struct ResultSet<'c> {
    stmt: Statement<'c>,
    state: BlockState,
}

impl<'c> ResultSet<'c> {
    /// Wraps a statement whose `execute` has already run and whose column count
    /// is known. Called by `Connection`/`Statement` after executing a query.
    pub(crate) fn from_executed(stmt: Statement<'c>, columns: usize) -> Self {
        let state = BlockState::new(stmt.handle(), columns);
        ResultSet { stmt, state }
    }

    /// Number of columns in the result set.
    pub fn column_count(&self) -> usize {
        self.state.column_count()
    }

    /// The driver-side failure that spoiled the scan, if any: empty after a
    /// clean end, set after a mid-stream block-fetch failure. Client-side
    /// mapping errors are not recorded here.
    pub fn error(&self) -> Option<Error> {
        self.state.error()
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
        self.state.next(self.stmt.connection())
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
        if self.state.error.is_some() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_value_borrows_the_buffered_cell_without_copying() {
        let cells = vec![Value::Int(7), Value::Bytes(vec![1, 2, 3])];
        let row = Row::new(&cells);
        // The same allocation, not a clone.
        assert!(std::ptr::eq(row.value(1).unwrap(), &cells[1]));
        assert_eq!(row.value(2), None);
    }
}
