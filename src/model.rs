//! Domain types: identity of a source file, manifest records, ingest stats.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Logical channel of a sales file: either drug-store or grocery-store aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Drug,
    Groc,
}

impl Channel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Channel::Drug => "drug",
            Channel::Groc => "groc",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "drug" => Some(Channel::Drug),
            "groc" => Some(Channel::Groc),
            _ => None,
        }
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Inferred identity of a sales file: the only inputs needed for routing
/// the output into Hive-style partitions.
///
/// `category` is the raw token from the filename (`beer`, `toothpa`, …);
/// canonicalising it (`paptowl` ↔ `paptowls`) is intentionally a later
/// concern so the parser can ingest files whose canonical name has not
/// been decided yet.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceIdentity {
    /// Absolute path on disk.
    pub path: PathBuf,
    /// IRI academic-data year, 1..=12.
    pub year: u8,
    /// Raw category token from the filename (preserves the case-as-on-disk).
    pub category: String,
    /// Drug vs grocery.
    pub channel: Channel,
    /// IRI week number at the start of the file's range (inclusive).
    pub filename_week_start: u16,
    /// IRI week number at the end of the file's range (inclusive).
    pub filename_week_end: u16,
}

impl SourceIdentity {
    pub fn filename_week_range(&self) -> std::ops::RangeInclusive<u16> {
        self.filename_week_start..=self.filename_week_end
    }
}

impl fmt::Display for SourceIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (year={}, category={}, channel={}, weeks {}-{})",
            self.path.display(),
            self.year,
            self.category,
            self.channel,
            self.filename_week_start,
            self.filename_week_end,
        )
    }
}

/// Partition path derived from identity. Does not include the part-number suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPath {
    pub year: u8,
    pub category: String,
    pub channel: Channel,
}

impl PartitionPath {
    pub fn from_identity(id: &SourceIdentity) -> Self {
        Self {
            year: id.year,
            category: id.category.clone(),
            channel: id.channel,
        }
    }

    /// Hive-style partition directory relative to `bronze/iri_sales/`.
    pub fn relative(&self) -> PathBuf {
        PathBuf::from(format!("year={}", self.year))
            .join(format!("category={}", self.category))
            .join(format!("channel={}", self.channel))
    }
}

/// One record per successful or attempted ingestion of one source file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub run_id: String,
    pub source_path: PathBuf,
    pub source_size_bytes: u64,
    pub source_sha256: String,
    pub source_year: u8,
    pub category: String,
    pub channel: Channel,
    pub filename_week_start: u16,
    pub filename_week_end: u16,
    pub expected_rows: u64,
    pub written_rows: u64,
    pub rejected_rows: u64,
    pub parser_version: String,
    pub output_schema_version: u32,
    pub compression: String,
    pub batch_rows: usize,
    pub row_group_rows: usize,
    pub output_paths: Vec<PathBuf>,
    pub output_size_bytes: u64,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub duration_ms: Option<u64>,
    pub status: ManifestStatus,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestStatus {
    Success,
    Failed,
    /// Placeholder so a "started but not finished" record can be written
    /// without committing to success/failure. Currently unused.
    InProgress,
}

impl ManifestRecord {
    /// A source is skippable only if *every* output-affecting input matches
    /// a prior successful record. Any mismatch forces a re-run.
    pub fn matches_for_skip(&self, other: &ManifestRecord) -> bool {
        self.status == ManifestStatus::Success
            && other.status == ManifestStatus::Success
            && self.source_path == other.source_path
            && self.source_size_bytes == other.source_size_bytes
            && self.source_sha256 == other.source_sha256
            && self.parser_version == other.parser_version
            && self.output_schema_version == other.output_schema_version
            && self.compression == other.compression
            && self.batch_rows == other.batch_rows
            && self.row_group_rows == other.row_group_rows
    }
}

/// Stats produced by a single ingest run. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct IngestStats {
    pub source_path: PathBuf,
    pub source_size_bytes: u64,
    pub expected_rows: u64,
    pub written_rows: u64,
    pub rejected_rows: u64,
    /// True when the rejected trailing record was missing *data* bytes,
    /// as opposed to only its line terminator. Only that case is a real
    /// loss; see `fixed_width::TrailingDefect`.
    pub rejected_loses_data: bool,
    pub output_bytes: u64,
    pub elapsed: std::time::Duration,
    pub output_paths: Vec<PathBuf>,
}

impl IngestStats {
    pub fn raw_mib_per_second(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        (self.source_size_bytes as f64 / (1024.0 * 1024.0)) / secs
    }
    pub fn rows_per_second(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        self.written_rows as f64 / secs
    }
    pub fn compression_ratio(&self) -> f64 {
        if self.output_bytes == 0 {
            0.0
        } else {
            self.source_size_bytes as f64 / self.output_bytes as f64
        }
    }
}

/// Compatibility marker for the current output schema.
/// Bump whenever a field name or Arrow type changes; the manifest stores
/// this and `ingest-all --resume` will treat older versions as invalid.
///
/// Version history:
/// - v1: 14 columns including `source_year`, `category`, `channel`.
/// - v2: 13 columns; `source_year` removed (derivable from `week`).
/// - v3: 11 columns; `category` and `channel` removed too. All three
///   are partition-only now, following the modern lakehouse convention
///   where partition columns live in the table/directory metadata, not
///   in the data files.
pub const CURRENT_SCHEMA_VERSION: u32 = 3;

/// Marker for the parser code; bump when fixed-width offsets change.
pub const PARSER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Sentinel for the partition-root name within `data/lake/`.
pub const BRONZE_TABLE: &str = "iri_sales";

/// Sub-directory of `data/lake/` that holds non-partitioned metadata.
pub const METADATA_DIR: &str = "metadata";

/// Convenience: the canonical bronze root for sales facts.
pub fn bronze_root(output_root: &Path) -> PathBuf {
    output_root.join("bronze").join(BRONZE_TABLE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_path_is_hive_style() {
        let id = SourceIdentity {
            path: PathBuf::from("/x/y/z"),
            year: 12,
            category: "toothpa".to_string(),
            channel: Channel::Drug,
            filename_week_start: 1687,
            filename_week_end: 1739,
        };
        let p = PartitionPath::from_identity(&id);
        assert_eq!(
            p.relative(),
            PathBuf::from("year=12/category=toothpa/channel=drug")
        );
    }

    #[test]
    fn skip_matching_requires_all_fields() {
        let now = chrono::Utc::now();
        let a = ManifestRecord {
            run_id: "a".into(),
            source_path: PathBuf::from("/x"),
            source_size_bytes: 100,
            source_sha256: "abc".into(),
            source_year: 1,
            category: "beer".into(),
            channel: Channel::Drug,
            filename_week_start: 1,
            filename_week_end: 52,
            expected_rows: 10,
            written_rows: 10,
            rejected_rows: 0,
            parser_version: PARSER_VERSION.into(),
            output_schema_version: CURRENT_SCHEMA_VERSION,
            compression: "zstd".into(),
            batch_rows: 1_000_000,
            row_group_rows: 4_000_000,
            output_paths: vec![PathBuf::from("/x.parquet")],
            output_size_bytes: 50,
            started_at: now,
            completed_at: Some(now),
            duration_ms: Some(1),
            status: ManifestStatus::Success,
            error_message: None,
        };
        let mut b = a.clone();
        assert!(a.matches_for_skip(&b));
        b.batch_rows = 500_000;
        assert!(!a.matches_for_skip(&b));
        b.batch_rows = a.batch_rows;
        b.source_sha256 = "different".into();
        assert!(!a.matches_for_skip(&b));
    }
}
