//! Dedicated row-at-a-time cursor used by chunked LOB reads.

use std::fmt;

use crate::core::connection::Statement;
use crate::driver::{CliDriver, GetDataChunk, StatementHandle};
use crate::error::{Error, ErrorCode, Result};
use crate::lob::{StreamingAccess, StreamingRow};
use crate::value::Value;

pub(crate) struct StreamingState {
    stmt: StatementHandle,
    columns: usize,
    error: Option<Error>,
    done: bool,
    last_column: Option<usize>,
}

impl StreamingState {
    pub(crate) fn new(stmt: StatementHandle, columns: usize) -> Self {
        Self {
            stmt,
            columns,
            error: None,
            done: false,
            last_column: None,
        }
    }

    pub(crate) fn column_count(&self) -> usize {
        self.columns
    }

    pub(crate) fn error(&self) -> Option<Error> {
        self.error.clone()
    }

    pub(crate) fn next(&mut self, driver: &dyn CliDriver) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        match driver.fetch_next(self.stmt) {
            Ok(has_row) => {
                self.done = !has_row;
                self.last_column = None;
                Ok(has_row)
            }
            Err(error) => self.driver_error(error),
        }
    }

    fn access_column(&mut self, column: usize) -> Result<()> {
        if column >= self.columns {
            return Err(Error::new(
                ErrorCode::Mapping,
                format!(
                    "column index {column} out of range for {} columns",
                    self.columns
                ),
            ));
        }
        if self.last_column.is_some_and(|last| column <= last) {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "streaming columns must be accessed in strictly ascending order",
            ));
        }
        self.last_column = Some(column);
        Ok(())
    }

    pub(crate) fn get(&mut self, driver: &dyn CliDriver, column: usize) -> Result<Value> {
        self.access_column(column)?;
        match driver.get_value(self.stmt, column) {
            Ok(value) => Ok(value),
            Err(error) => self.driver_error(error),
        }
    }

    pub(crate) fn start_lob(&mut self, column: usize) -> Result<()> {
        self.access_column(column)
    }

    pub(crate) fn read_lob(
        &mut self,
        driver: &dyn CliDriver,
        column: usize,
        buffer: &mut [u8],
    ) -> Result<GetDataChunk> {
        match driver.get_data_chunk(self.stmt, column, buffer) {
            Ok(chunk) if chunk.bytes <= buffer.len() => Ok(chunk),
            Ok(_) => self.driver_error(Error::new(
                ErrorCode::Mapping,
                "driver returned an oversized LOB chunk",
            )),
            Err(error) => self.driver_error(error),
        }
    }

    fn driver_error<T>(&mut self, error: Error) -> Result<T> {
        self.done = true;
        if self.error.is_none() {
            self.error = Some(error.clone());
        }
        Err(error)
    }
}

/// A connection-borrowing, row-at-a-time streaming cursor.
pub struct StreamingResultSet<'c> {
    statement: Statement<'c>,
    state: StreamingState,
}

impl<'c> StreamingResultSet<'c> {
    pub(crate) fn from_executed(statement: Statement<'c>, columns: usize) -> Self {
        let state = StreamingState::new(statement.handle(), columns);
        Self { statement, state }
    }

    pub fn column_count(&self) -> usize {
        self.state.column_count()
    }

    pub fn error(&self) -> Option<Error> {
        self.state.error()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<StreamingRow<'_>>> {
        let driver = self.statement.driver_ref();
        if self.state.next(driver)? {
            Ok(Some(StreamingRow::new(self)))
        } else {
            Ok(None)
        }
    }
}

impl StreamingAccess for StreamingResultSet<'_> {
    fn streaming_column_count(&self) -> usize {
        self.state.column_count()
    }

    fn streaming_get(&mut self, column: usize) -> Result<Value> {
        let driver = self.statement.driver_ref();
        self.state.get(driver, column)
    }

    fn streaming_start_lob(&mut self, column: usize) -> Result<()> {
        self.state.start_lob(column)
    }

    fn streaming_read_lob(&mut self, column: usize, buffer: &mut [u8]) -> Result<GetDataChunk> {
        let driver = self.statement.driver_ref();
        self.state.read_lob(driver, column, buffer)
    }
}

impl Drop for StreamingResultSet<'_> {
    fn drop(&mut self) {
        if self.state.error.is_some() {
            self.statement.poison();
        }
    }
}

impl fmt::Debug for StreamingResultSet<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingResultSet")
            .field("columns", &self.column_count())
            .field("done", &self.state.done)
            .finish_non_exhaustive()
    }
}
