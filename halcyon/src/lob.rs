//! Public large-object bind sources and lending streaming row/read views.

use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::core::FromColumn;
use crate::driver::ParamStreamSource;
use crate::error::{Error, ErrorCode, Result};
use crate::value::Value;

pub type LobPull<'a> = dyn FnMut(&mut [u8]) -> Result<usize> + 'a;
pub type OwnedLobPull = dyn FnMut(&mut [u8]) -> Result<usize> + Send + 'static;

/// One positional parameter for a single-use streaming execute.
pub enum BindParam<'a> {
    /// An ordinary value, bound as with any other query.
    Value(Value),
    /// A BLOB or CLOB streamed in chunks during execution.
    Lob(LobSource<'a>),
}

impl From<Value> for BindParam<'_> {
    fn from(value: Value) -> Self {
        Self::Value(value)
    }
}

/// Borrowed, single-use LOB source for synchronous execution. Build one with
/// [`lob_file`], [`lob_reader`], or [`lob_callback`].
pub enum LobSource<'a> {
    /// Streams the contents of a file, whose size gives the length.
    File {
        /// File to read.
        path: PathBuf,
        /// Bind as `CLOB` instead of `BLOB`.
        as_clob: bool,
    },
    /// Streams from a reader until it returns end of file.
    Reader {
        /// Data source.
        reader: &'a mut (dyn Read + Send),
        /// Total length in bytes, when known.
        size_hint: Option<u64>,
        /// Bind as `CLOB` instead of `BLOB`.
        as_clob: bool,
    },
    /// Streams from a closure that fills a buffer and returns the number of
    /// bytes written, with `0` meaning the data is finished.
    Callback {
        /// Data source.
        pull: Box<LobPull<'a>>,
        /// Total length in bytes, when known.
        size_hint: Option<u64>,
        /// Bind as `CLOB` instead of `BLOB`.
        as_clob: bool,
    },
}

impl<'a> LobSource<'a> {
    /// Bind this source as character data (`CLOB`) instead of binary (`BLOB`).
    pub fn as_clob(mut self) -> Self {
        match &mut self {
            Self::File { as_clob, .. }
            | Self::Reader { as_clob, .. }
            | Self::Callback { as_clob, .. } => *as_clob = true,
        }
        self
    }

    pub(crate) fn into_stream(self, param_index: usize) -> ParamStreamSource<'a> {
        match self {
            Self::File { path, as_clob } => file_stream(path, param_index, as_clob),
            Self::Reader {
                reader,
                size_hint,
                as_clob,
            } => ParamStreamSource {
                param_index,
                pull: Box::new(move |buffer| reader.read(buffer).map_err(io_source_error)),
                size_hint,
                is_clob: as_clob,
            },
            Self::Callback {
                pull,
                size_hint,
                as_clob,
            } => ParamStreamSource {
                param_index,
                pull,
                size_hint,
                is_clob: as_clob,
            },
        }
    }
}

/// Owned, `Send + 'static`, single-use LOB source for async execution. Build
/// one with [`owned_lob_file`] or [`owned_lob_callback`].
pub enum OwnedLobSource {
    /// Streams the contents of a file, whose size gives the length.
    File {
        /// File to read.
        path: PathBuf,
        /// Bind as `CLOB` instead of `BLOB`.
        as_clob: bool,
    },
    /// Streams from a closure that fills a buffer and returns the number of
    /// bytes written, with `0` meaning the data is finished.
    Callback {
        /// Data source.
        pull: Box<OwnedLobPull>,
        /// Total length in bytes, when known.
        size_hint: Option<u64>,
        /// Bind as `CLOB` instead of `BLOB`.
        as_clob: bool,
    },
}

impl OwnedLobSource {
    /// Bind this source as character data (`CLOB`) instead of binary (`BLOB`).
    pub fn as_clob(mut self) -> Self {
        match &mut self {
            Self::File { as_clob, .. } | Self::Callback { as_clob, .. } => *as_clob = true,
        }
        self
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn into_borrowed(self) -> LobSource<'static> {
        match self {
            Self::File { path, as_clob } => LobSource::File { path, as_clob },
            Self::Callback {
                pull,
                size_hint,
                as_clob,
            } => LobSource::Callback {
                pull,
                size_hint,
                as_clob,
            },
        }
    }
}

/// Owned counterpart of [`BindParam`], used by the async facade.
pub enum OwnedBindParam {
    /// An ordinary value, bound as with any other query.
    Value(Value),
    /// A BLOB or CLOB streamed in chunks during execution.
    Lob(OwnedLobSource),
}

impl From<Value> for OwnedBindParam {
    fn from(value: Value) -> Self {
        Self::Value(value)
    }
}

impl OwnedBindParam {
    #[cfg(feature = "tokio")]
    pub(crate) fn into_borrowed(self) -> BindParam<'static> {
        match self {
            Self::Value(value) => BindParam::Value(value),
            Self::Lob(source) => BindParam::Lob(source.into_borrowed()),
        }
    }
}

/// Creates a lazily opened file LOB source.
pub fn lob_file(path: impl Into<PathBuf>) -> LobSource<'static> {
    LobSource::File {
        path: path.into(),
        as_clob: false,
    }
}

/// Creates a LOB source borrowing an already-open reader.
pub fn lob_reader<'a, R>(reader: &'a mut R, size_hint: Option<u64>) -> LobSource<'a>
where
    R: Read + Send + 'a,
{
    LobSource::Reader {
        reader,
        size_hint,
        as_clob: false,
    }
}

/// Creates a LOB source from a pull callback (`Ok(0)` means EOF).
pub fn lob_callback<'a, F>(pull: F, size_hint: Option<u64>) -> LobSource<'a>
where
    F: FnMut(&mut [u8]) -> Result<usize> + 'a,
{
    LobSource::Callback {
        pull: Box::new(pull),
        size_hint,
        as_clob: false,
    }
}

/// Creates an owned, lazily opened file source for async execution.
pub fn owned_lob_file(path: impl Into<PathBuf>) -> OwnedLobSource {
    OwnedLobSource::File {
        path: path.into(),
        as_clob: false,
    }
}

/// Creates an owned callback source for async execution.
pub fn owned_lob_callback<F>(pull: F, size_hint: Option<u64>) -> OwnedLobSource
where
    F: FnMut(&mut [u8]) -> Result<usize> + Send + 'static,
{
    OwnedLobSource::Callback {
        pull: Box::new(pull),
        size_hint,
        as_clob: false,
    }
}

fn io_source_error(error: std::io::Error) -> Error {
    Error::new(
        ErrorCode::Mapping,
        format!("LOB source read failed: {error}"),
    )
}

fn file_stream(path: PathBuf, param_index: usize, is_clob: bool) -> ParamStreamSource<'static> {
    let mut file: Option<File> = None;
    ParamStreamSource {
        param_index,
        pull: Box::new(move |buffer| {
            if file.is_none() {
                file = Some(File::open(&path).map_err(io_source_error)?);
            }
            match file.as_mut() {
                Some(file) => file.read(buffer).map_err(io_source_error),
                None => Err(Error::new(
                    ErrorCode::Mapping,
                    "LOB file source did not initialize",
                )),
            }
        }),
        size_hint: None,
        is_clob,
    }
}

/// Private cursor operations shared by core and facade streaming cursors.
pub(crate) trait StreamingAccess {
    fn streaming_column_count(&self) -> usize;
    fn streaming_get(&mut self, column: usize) -> Result<Value>;
    fn streaming_start_lob(&mut self, column: usize) -> Result<()>;
    fn streaming_read_lob(
        &mut self,
        column: usize,
        buffer: &mut [u8],
    ) -> Result<crate::driver::GetDataChunk>;
}

/// Lending view of the current row of a streaming result set.
pub struct StreamingRow<'a> {
    pub(crate) cursor: &'a mut dyn StreamingAccess,
}

impl StreamingRow<'_> {
    pub(crate) fn new(cursor: &mut dyn StreamingAccess) -> StreamingRow<'_> {
        StreamingRow { cursor }
    }

    /// Number of columns in the result set.
    pub fn column_count(&self) -> usize {
        self.cursor.streaming_column_count()
    }

    /// Reads a scalar column, enforcing ascending column access.
    pub fn get<T: FromColumn>(&mut self, column: usize) -> Result<T> {
        T::from_column(self.cursor.streaming_get(column)?)
    }

    /// Opens chunked access to one LOB column.
    pub fn lob(&mut self, column: usize) -> Result<LobReader<'_>> {
        self.cursor.streaming_start_lob(column)?;
        Ok(LobReader {
            cursor: self.cursor,
            column,
            done: false,
            is_null: false,
        })
    }
}

impl fmt::Debug for StreamingRow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingRow")
            .field("columns", &self.column_count())
            .finish_non_exhaustive()
    }
}

/// Chunked reader over one LOB cell. It borrows its row/cursor exclusively.
pub struct LobReader<'a> {
    cursor: &'a mut dyn StreamingAccess,
    column: usize,
    done: bool,
    is_null: bool,
}

impl LobReader<'_> {
    /// Reads the next chunk; `Ok(0)` means EOF (including SQL NULL).
    pub fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        if self.done {
            return Ok(0);
        }
        if buffer.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "LOB read buffer must not be empty",
            ));
        }
        let chunk = match self.cursor.streaming_read_lob(self.column, buffer) {
            Ok(chunk) => chunk,
            Err(error) => {
                self.done = true;
                return Err(error);
            }
        };
        self.done = chunk.done;
        self.is_null |= chunk.is_null;
        Ok(chunk.bytes)
    }

    /// Meaningful after the first read; a NULL LOB returns immediate EOF.
    pub fn is_null(&self) -> bool {
        self.is_null
    }

    /// Copies the remaining LOB to a writer in O(chunk) memory.
    pub fn copy_to(&mut self, writer: &mut dyn Write) -> Result<u64> {
        let mut stage = vec![0u8; 256 * 1024];
        let mut total = 0u64;
        loop {
            let n = self.read(&mut stage)?;
            if n == 0 {
                return Ok(total);
            }
            writer.write_all(&stage[..n]).map_err(io_sink_error)?;
            total += n as u64;
        }
    }

    /// Copies the remaining LOB to a newly created file in O(chunk) memory.
    pub fn copy_to_file(&mut self, path: impl AsRef<Path>) -> Result<u64> {
        let mut file = File::create(path).map_err(io_sink_error)?;
        self.copy_to(&mut file)
    }

    /// Materializes the remaining LOB into memory.
    pub fn to_vec(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.copy_to(&mut out)?;
        Ok(out)
    }

    /// Materializes the remaining LOB and decodes it as UTF-8.
    pub fn to_string(&mut self) -> Result<String> {
        String::from_utf8(self.to_vec()?)
            .map_err(|e| Error::new(ErrorCode::Mapping, format!("LOB is not UTF-8: {e}")))
    }
}

fn io_sink_error(error: std::io::Error) -> Error {
    Error::new(
        ErrorCode::Mapping,
        format!("LOB sink write failed: {error}"),
    )
}

impl fmt::Debug for LobReader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LobReader")
            .field("column", &self.column)
            .field("done", &self.done)
            .field("is_null", &self.is_null)
            .finish()
    }
}
