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
///
/// Every field added for the non-sales datasets is `#[serde(default)]`
/// and optional, so a `manifest.jsonl` written before this change —
/// where the identity fields were mandatory — still deserialises, and
/// still produces the same skip decision. `dataset` defaults to
/// `Sales`, which is what every pre-existing line was.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub run_id: String,
    /// Which dataset class produced this output.
    #[serde(default = "DatasetKind::sales_default")]
    pub dataset: DatasetKind,
    pub source_path: PathBuf,
    pub source_size_bytes: u64,
    pub source_sha256: String,
    #[serde(default)]
    pub source_year: Option<u8>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub channel: Option<Channel>,
    #[serde(default)]
    pub filename_week_start: Option<u16>,
    #[serde(default)]
    pub filename_week_end: Option<u16>,
    /// True when one source file was written once and then referenced
    /// by several other source paths (content-hash dedup). See
    /// `docs/other_sources.md` § Duplicate handling.
    #[serde(default)]
    pub deduplicated_from: Option<String>,
    /// Identity of the *source's own* schema, for the classes whose
    /// column set is a property of the file rather than of the crate
    /// (`iri_product_attr`, the stubs, the demos).
    ///
    /// This is deliberately separate from `output_schema_version`.
    /// That field answers "has the code changed?", and it must be a
    /// constant per class so it can be compared *before* the source is
    /// parsed — which is what makes resume cheap. This field answers
    /// "did this file's columns change?", which can only be known after
    /// reading its header. Conflating them makes every per-file-schema
    /// class un-skippable, because the prospective record has to be
    /// built before the parser exists.
    #[serde(default)]
    pub source_schema_fingerprint: Option<String>,
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
            && self.dataset == other.dataset
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

/// The class of source a manifest record describes.
///
/// `Sales` is the original class and carries a dedicated, performance-
/// tuned pipeline ([`crate::discovery`], [`crate::ingest`]) with its
/// own partition scheme and gate suite. Every other class shares the
/// generic pipeline in [`crate::datasets`].
///
/// The variants beyond `Sales` exist because the corpus is not one data
/// source: alongside the 744 store-level sales files there are household
/// panel trips, product attribute dictionaries, store rosters, a
/// week→calendar dimension and several small cross-reference tables.
/// See `docs/other_sources.md` for the survey.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    /// Store × week × item aggregates, fixed-width. The original class.
    Sales,
    /// Household panel trip × item records. Four on-disk dialects.
    Panel,
    /// Household trip × store totals from the external trips files.
    PanelTrips,
    /// Per-panelist trip counts and static-panel flags.
    PanelStatic,
    /// Per-panelist demographics (year-directory `DEMOS.CSV`).
    PanelistDemos,
    /// Per-panelist demographics for the ad panel (`ads demo<n>.csv`).
    AdsDemos,
    /// Per-item attribute dictionary (`*_prod_attr`). Per-file schema.
    ProductAttr,
    /// Per-item product stub from the parsed-stub workbooks.
    ProductStub,
    /// Store roster (`Delivery_Stores`). Fixed-width.
    DeliveryStores,
    /// IRI week → calendar date dimension.
    WeekDimension,
    /// Masked chain cross-reference by year.
    ChainXref,
    /// Manual store → chain entry by year.
    ManualStoreEntry,
}

impl DatasetKind {
    /// serde's `default` for the `dataset` field. Every manifest line
    /// written before this field existed was a sales record.
    pub fn sales_default() -> Self {
        DatasetKind::Sales
    }

    /// Parse the kebab/snake forms used on the command line.
    pub fn parse(s: &str) -> Option<Self> {
        use DatasetKind::*;
        let key = s.trim().to_ascii_lowercase().replace(['_', '-'], "");
        Some(match key.as_str() {
            "sales" => Sales,
            "panel" => Panel,
            "paneltrips" | "trips" => PanelTrips,
            "panelstatic" | "static" => PanelStatic,
            "panelistdemos" | "demos" => PanelistDemos,
            "adsdemos" => AdsDemos,
            "productattr" | "prodattr" => ProductAttr,
            "productstub" | "stub" => ProductStub,
            "deliverystores" => DeliveryStores,
            "weekdimension" | "weeks" => WeekDimension,
            "chainxref" => ChainXref,
            "manualstoreentry" => ManualStoreEntry,
            _ => return None,
        })
    }

    /// Table name under `bronze/`.
    pub fn bronze_table(self) -> &'static str {
        use DatasetKind::*;
        match self {
            Sales => BRONZE_TABLE,
            Panel => "iri_panel",
            PanelTrips => "iri_panel_trips",
            PanelStatic => "iri_panel_static",
            PanelistDemos => "iri_panelist_demos",
            AdsDemos => "iri_ads_demos",
            ProductAttr => "iri_product_attr",
            ProductStub => "iri_product_stub",
            DeliveryStores => "iri_delivery_stores",
            WeekDimension => "iri_week_dimension",
            ChainXref => "iri_chain_xref",
            ManualStoreEntry => "iri_manual_store_entry",
        }
    }

    /// Sales is the only class that keeps the dedicated pipeline.
    pub fn is_sales(self) -> bool {
        matches!(self, DatasetKind::Sales)
    }

    /// Every non-sales class, in ingest order.
    ///
    /// The order is cheapest-and-most-reusable first so that a partial
    /// run still yields the tables other tables join against: the week
    /// dimension and the store roster are small, and `iri_sales` queries
    /// reference both.
    pub fn others() -> &'static [DatasetKind] {
        use DatasetKind::*;
        &[
            WeekDimension,
            DeliveryStores,
            PanelStatic,
            ChainXref,
            ManualStoreEntry,
            AdsDemos,
            PanelistDemos,
            PanelTrips,
            Panel,
            ProductAttr,
            ProductStub,
        ]
    }

    /// The declared output-schema version for this class.
    ///
    /// A constant per class, not per file, for the reason above: the
    /// skip decision is made before the source is parsed. Classes whose
    /// schema is genuinely per-file pair this with a
    /// `source_schema_fingerprint`.
    pub fn schema_version(self) -> u32 {
        use DatasetKind::*;
        match self {
            Sales => CURRENT_SCHEMA_VERSION,
            Panel => PANEL_SCHEMA_VERSION,
            DeliveryStores => DELIVERY_STORES_SCHEMA_VERSION,
            PanelTrips => PANEL_TRIPS_SCHEMA_VERSION,
            PanelStatic => PANEL_STATIC_SCHEMA_VERSION,
            WeekDimension => WEEK_DIMENSION_SCHEMA_VERSION,
            // The remaining three have a per-file schema; the version
            // tracks the writer, the fingerprint tracks the columns.
            ProductAttr | ProductStub | PanelistDemos | AdsDemos | ChainXref | ManualStoreEntry => {
                1
            }
        }
    }

    /// Whether this class's column set is a property of the source file
    /// rather than of the crate.
    ///
    /// The three attribute-shaped classes (`prod_attr`, the stubs, the
    /// demographics) each ship their own column list per file, so a
    /// single crate-wide schema would mean inventing null columns for
    /// the 29-attribute categories or dropping attributes for the
    /// 82-attribute ones.
    pub fn has_per_file_schema(self) -> bool {
        use DatasetKind::*;
        matches!(
            self,
            ProductAttr | ProductStub | PanelistDemos | AdsDemos | ChainXref | ManualStoreEntry
        )
    }

    /// Which identity fields become Hive partition keys, in order.
    ///
    /// Declared per class rather than inferred from "whatever the
    /// identity happens to carry", because the two differ in a way that
    /// matters. `iri_delivery_stores` carries a category on its
    /// identity — the directory it was found in — but partitioning by
    /// it would scatter one 2 053-row year-wide dimension across 31
    /// directories for no query benefit. Conversely `iri_panel`
    /// partitions by all three, because a PANEL file really is one
    /// (year, category, outlet) slice and every query filters on them.
    ///
    /// An empty list means a flat table: correct for the genuinely
    /// small ones (`iri_week_dimension`, 627 rows) and a warning sign
    /// for a large one, which is why the list is explicit.
    pub fn partition_keys(self) -> &'static [&'static str] {
        use DatasetKind::*;
        match self {
            Sales => &["year", "category", "channel"],
            Panel => &["year", "category", "outlet"],
            ProductAttr => &["year", "category"],
            PanelistDemos => &["year"],
            ProductStub => &["edition"],
            DeliveryStores => &["year"],
            PanelTrips => &["year"],
            PanelStatic | AdsDemos => &[],
            WeekDimension | ChainXref | ManualStoreEntry => &[],
        }
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_value(self)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| format!("{self:?}"));
        f.write_str(&s)
    }
}

/// Output-schema version for the `Panel` class.
///
/// v1: 8 columns — `panid`, `week`, `minute` (nullable), `units`,
/// `outlet`, `dollars_cents`, `iri_key`, `colupc`. The four on-disk
/// PANEL dialects all normalise onto this one schema; the pre-year-8
/// dialects have no MINUTE column at all, which is what the nullable
/// column represents.
pub const PANEL_SCHEMA_VERSION: u32 = 1;

/// Output-schema version for the `DeliveryStores` class.
///
/// v1: 7 columns — `iri_key`, `ou`, `est_acv`, `market_name`, `open_week`,
/// `closed_week`, `masked_name`. `open_week`/`closed_week` are the IRI
/// week numbers IRI uses for "first/last week this store delivered",
/// with `9998` as the open-ended sentinel; they are stored as the raw
/// `u16` rather than a date because the week→date join is exactly what
/// `iri_week_dimension` is for.
pub const DELIVERY_STORES_SCHEMA_VERSION: u32 = 1;

/// Output-schema version for the `PanelTrips` class.
///
/// v1: 8 columns — `panid`, `week`, `iri_key`, `iri_key2` (nullable),
/// `minute`, `cents998`, `cents999`, `kryscents` (nullable). The
/// year-8+ edition added `IRI_Key2` and `KRYSCENTS`; years 1–7 have
/// neither, which is what the two nullable columns represent.
pub const PANEL_TRIPS_SCHEMA_VERSION: u32 = 1;

/// Output-schema version for the `PanelStatic` class.
///
/// v1: 4 columns — `panid`, `trip_count`, `make_static`, `source_year`.
pub const PANEL_STATIC_SCHEMA_VERSION: u32 = 1;

/// Output-schema version for the `WeekDimension` class.
///
/// v1: 4 columns — `iri_week`, `week_start`, `week_end`, `source_year`
/// (nullable; only the authoritative workbook carries a `Year`).
///
/// The two date columns are `Date32` rather than a string or an
/// Excel serial: the whole point of this table is to attach calendar
/// dates to `iri_sales.week`, and doing that conversion at Bronze
/// means the conversion is applied exactly once and identically for
/// every consumer. `week_end` uses IRI's own week-end convention
/// (Sunday), which is what the workbook documents.
pub const WEEK_DIMENSION_SCHEMA_VERSION: u32 = 1;

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
            dataset: DatasetKind::Sales,
            source_path: PathBuf::from("/x"),
            source_size_bytes: 100,
            source_sha256: "abc".into(),
            source_year: Some(1),
            category: Some("beer".into()),
            channel: Some(Channel::Drug),
            filename_week_start: Some(1),
            filename_week_end: Some(52),
            deduplicated_from: None,
            source_schema_fingerprint: None,
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
