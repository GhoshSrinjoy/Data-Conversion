use std::fmt;
use std::path::PathBuf;

/// Errors surfaced by the native engine.  The Python binding maps each kind to the
/// matching `dataconv.errors` exception.
#[derive(Debug)]
pub enum Error {
    Io {
        source: std::io::Error,
        path: Option<PathBuf>,
    },
    Arrow(arrow::error::ArrowError),
    Parquet(parquet::errors::ParquetError),
    /// Unknown, unsupported or mismatched file format.
    Format(String),
    /// Malformed input data under a policy that does not tolerate it.
    Data(String),
    /// Invalid schema specification or a value that cannot take the requested type.
    Schema(String),
    /// Checksums do not match: corruption or truncation.
    Integrity(String),
    /// Destination exists and overwriting was not requested.
    Exists(PathBuf),
    /// Invalid options.
    Invalid(String),
    /// The caller asked to stop (e.g. Ctrl-C).
    Cancelled,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn io(source: std::io::Error, path: impl Into<PathBuf>) -> Self {
        Error::Io { source, path: Some(path.into()) }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Error::Io { .. } => "io",
            Error::Arrow(_) | Error::Parquet(_) => "data",
            Error::Format(_) => "format",
            Error::Data(_) => "data",
            Error::Schema(_) => "schema",
            Error::Integrity(_) => "integrity",
            Error::Exists(_) => "exists",
            Error::Invalid(_) => "invalid",
            Error::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io { source, path: Some(p) } => write!(f, "{}: {}", p.display(), source),
            Error::Io { source, path: None } => write!(f, "{source}"),
            Error::Arrow(e) => write!(f, "{e}"),
            Error::Parquet(e) => write!(f, "{e}"),
            Error::Format(m) | Error::Data(m) | Error::Schema(m) | Error::Integrity(m) | Error::Invalid(m) => {
                f.write_str(m)
            }
            Error::Exists(p) => write!(f, "{} already exists (use overwrite to replace it)", p.display()),
            Error::Cancelled => f.write_str("cancelled"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::Io { source, path: None }
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(e: arrow::error::ArrowError) -> Self {
        Error::Arrow(e)
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error::Parquet(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Invalid(format!("invalid JSON: {e}"))
    }
}
