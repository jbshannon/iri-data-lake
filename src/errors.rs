//! Typed error surface. CLI boundaries may bubble up `anyhow::Error`,
//! but the library code uses these typed enums so callers (tests, future
//! worker pool, future row-level reject mode) can pattern-match.

use std::path::PathBuf;

use crate::model::SourceIdentity;
use thiserror::Error;

pub type Result<T, E = IngestError> = std::result::Result<T, E>;

/// Top-level ingest error. Each variant carries enough context
/// (source path, byte offset, raw bytes) for the CLI to print a useful
/// diagnostic without consulting logs.
#[derive(Debug, Error)]
pub enum IngestError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid header at {path}: expected {expected:?}, got {actual:?}")]
    HeaderMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },

    #[error(
        "file {path} is not record-aligned: size={size}, header={header}, record={record} (remainder={remainder})"
    )]
    RecordAlignment {
        path: PathBuf,
        size: u64,
        header: usize,
        record: usize,
        remainder: u64,
    },

    #[error("invalid IRI_KEY in row {row} of {identity}: raw bytes {raw_bytes:?} not a u32")]
    InvalidIriKey {
        identity: Box<SourceIdentity>,
        row: u64,
        raw_bytes: String,
    },

    #[error(
        "invalid WEEK in row {row} of {identity}: raw bytes {raw_bytes:?} not a u16 (range must fit 1..=12*52+1=625 or similar)"
    )]
    InvalidWeek {
        identity: Box<SourceIdentity>,
        row: u64,
        raw_bytes: String,
    },

    #[error("invalid SY/GE/VEND/ITEM/UNITS/D/PR integer in row {row} of {identity}: field={field} raw={raw_bytes:?}")]
    InvalidIntegerField {
        identity: Box<SourceIdentity>,
        row: u64,
        field: &'static str,
        raw_bytes: String,
    },

    #[error("invalid DOLLARS in row {row} of {identity}: raw={raw_bytes:?}: {reason}")]
    InvalidDollars {
        identity: Box<SourceIdentity>,
        row: u64,
        raw_bytes: String,
        reason: MoneyErrorReason,
    },

    #[error("unknown feature code in row {row} of {identity}: raw={raw_bytes:?}")]
    UnknownFeatureCode {
        identity: Box<SourceIdentity>,
        row: u64,
        raw_bytes: String,
    },

    #[error("non-ASCII bytes in field {field} at row {row} of {identity}")]
    NonAscii {
        identity: Box<SourceIdentity>,
        row: u64,
        field: &'static str,
    },

    #[error("parquet write failed at {path}: {source}")]
    Parquet {
        path: PathBuf,
        #[source]
        source: parquet::errors::ParquetError,
    },

    #[error("arrow build failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("manifest error: {0}")]
    Manifest(String),

    #[error("io-rename atomic swap failed: tmp={tmp} -> dst={dst}: {source}")]
    AtomicRename {
        tmp: PathBuf,
        dst: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("discovery failed: {0}")]
    Discovery(String),

    #[error("config error: {0}")]
    Config(String),
}

impl IngestError {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub fn parquet(path: impl Into<PathBuf>, source: parquet::errors::ParquetError) -> Self {
        Self::Parquet {
            path: path.into(),
            source,
        }
    }

    pub fn manifest<S: Into<String>>(s: S) -> Self {
        Self::Manifest(s.into())
    }
}

/// Sub-reason for malformed monetary values, kept so callers can
/// distinguish empty fields from malformed numerics.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum MoneyErrorReason {
    #[error("field is empty or all spaces")]
    Empty,
    #[error("not a valid ASCII number")]
    NotANumber,
    #[error("contains more than 2 fractional digits")]
    TooManyFractionalDigits,
    #[error("more than one decimal point")]
    MultipleDecimalPoints,
    #[error("contains a sign character; only positive values expected")]
    UnexpectedSign,
    #[error("non-ASCII bytes in money field")]
    NonAscii,
}
