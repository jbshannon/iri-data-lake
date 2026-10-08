//! `iri_delivery_stores` — the per-year store roster.
//!
//! `Delivery_Stores` is the dimension that turns `iri_sales.iri_key`
//! into a place: outlet code, market, the weeks the store delivered, and
//! the masked chain name. It is small (2 053 rows per file, ~130 KB)
//! and it is the join every store-level analysis needs.
//!
//! # Fixed-width at 63 bytes, and the header lies about the offsets
//!
//! `docs/data_layout.md` describes this as a space-aligned table. It is
//! fixed-width: all 37 200 data rows across all 372 files are exactly
//! 63 bytes. The problem is that the **header's token positions do not
//! match the data's field positions**:
//!
//! ```text
//! header  IRI_KEY@0  OU@8  EST_ACV@11  Market_Name@20  Open@45  Clsd@50  MskdName@55
//! data     200039@1   GR@8  9.709999@11 BUFFALO/…@20     539@46   1219@50   Chain87@55
//! ```
//!
//! Slicing by the header's own offsets lands mid-field — `Market` would
//! start at byte 20 and `Open` at 45, but `539` starts at 46. The
//! right-padded `IRI_KEY` is what throws the first field off by one.
//! The layout below is measured, not inferred:
//!
//! ```text
//! IRI_KEY  [ 0.. 8)   OU   [ 8..11)   EST_ACV   [11..20)
//! Market   [20..45)   Open [45..50)   Clsd      [50..55)   MskdName [55..63)
//! ```
//!
//! # Heavy duplication
//!
//! 372 files hold ~62 distinct byte sequences (measured: 6/4/7/3/2/5/4/
//! 4/5/3/3/13 distinct per year). The 31 categories of a year share
//! 2–13 store editions between them. Content-hash dedup is a policy
//! decision rather than an implementation detail — see
//! `docs/other_sources.md` § Duplicate handling.
//!
//! # `Open`/`Clsd` are week numbers, not dates
//!
//! `Open`/`Clsd` are IRI week numbers in the same 1114–1739 space as
//! `iri_sales.week`, with `9998` as the "still open" sentinel. They are
//! stored as raw `u16` rather than resolved to dates because resolving
//! them is precisely the job of `iri_week_dimension` — and doing it at
//! Bronze would bake one join's answer into the source data.

use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::ArrayBuilder;
use arrow_array::builder::{Float64Builder, StringBuilder, UInt16Builder, UInt32Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::delimited;
use super::ingest::{BatchSink, DatasetParser, DatasetTuning, ParserFactory};
use crate::config::IngestConfig;
use crate::dataset::{infer_category, infer_year, DatasetFile, DatasetInventory, SourceRef};
use crate::errors::Result;
use crate::model::DatasetKind;

/// Total bytes per data row, header included. Verified as the *only*
/// row length present across all 372 files / 37 200 rows.
pub const RECORD_LEN: usize = 63;
/// Header line length, excluding CRLF. Measured from the corpus.
pub const HEADER_LEN: usize = 63;

/// Field offsets, measured (see the module docs).
pub mod offsets {
    pub const IRI_KEY: std::ops::Range<usize> = 0..8;
    pub const OU: std::ops::Range<usize> = 8..11;
    pub const EST_ACV: std::ops::Range<usize> = 11..20;
    pub const MARKET: std::ops::Range<usize> = 20..45;
    pub const OPEN: std::ops::Range<usize> = 45..50;
    pub const CLSD: std::ops::Range<usize> = 50..55;
    pub const MSKD_NAME: std::ops::Range<usize> = 55..63;
}

/// The sentinel `Open`/`Clsd` carries for "still trading".
pub const OPEN_ENDED: u16 = 9998;

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("iri_key", DataType::UInt32, false),
        Field::new("ou", DataType::Utf8, true),
        Field::new("est_acv", DataType::Float64, true),
        Field::new("market_name", DataType::Utf8, true),
        Field::new("open_week", DataType::UInt16, true),
        Field::new("closed_week", DataType::UInt16, true),
        Field::new("masked_name", DataType::Utf8, true),
    ]))
}

/// Column builders for the store roster.
#[derive(Debug, Default)]
pub struct StoreBuilders {
    iri_key: UInt32Builder,
    ou: StringBuilder,
    est_acv: Float64Builder,
    market: StringBuilder,
    open: UInt16Builder,
    closed: UInt16Builder,
    masked: StringBuilder,
    rows: u64,
}

impl StoreBuilders {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.iri_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Append one 63-byte row.
    ///
    /// Panics if `row` is not exactly [`RECORD_LEN`], because a
    /// short row means the offsets below would index out of bounds —
    /// and a layout surprise must be loud, not a panic three frames
    /// deeper with no source path.
    pub fn push_row(&mut self, row: &[u8]) {
        assert_eq!(
            row.len(),
            RECORD_LEN,
            "Delivery_Stores row must be exactly {RECORD_LEN} bytes"
        );
        match delimited::parse_int(&row[offsets::IRI_KEY]) {
            Some(v) if (0..=u32::MAX as i64).contains(&v) => self.iri_key.append_value(v as u32),
            _ => self.iri_key.append_null(),
        };
        self.ou.append_option(text(&row[offsets::OU]));
        match delimited::parse_f64(&row[offsets::EST_ACV]) {
            Some(v) => self.est_acv.append_value(v),
            None => self.est_acv.append_null(),
        }
        self.market.append_option(text(&row[offsets::MARKET]));
        match delimited::parse_int(&row[offsets::OPEN]) {
            Some(v) if (0..=u16::MAX as i64).contains(&v) => self.open.append_value(v as u16),
            _ => self.open.append_null(),
        }
        match delimited::parse_int(&row[offsets::CLSD]) {
            Some(v) if (0..=u16::MAX as i64).contains(&v) => self.closed.append_value(v as u16),
            _ => self.closed.append_null(),
        }
        self.masked.append_option(text(&row[offsets::MSKD_NAME]));
        self.rows += 1;
    }

    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(self.iri_key.finish()),
                Arc::new(self.ou.finish()),
                Arc::new(self.est_acv.finish()),
                Arc::new(self.market.finish()),
                Arc::new(self.open.finish()),
                Arc::new(self.closed.finish()),
                Arc::new(self.masked.finish()),
            ],
        )
        .expect("roster schema and build()'s column list are declared together")
    }
}

fn text(field: &[u8]) -> Option<String> {
    let t = delimited::trim_ascii_space(field);
    if t.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(t).into_owned())
    }
}

/// The roster parser for one source file.
#[derive(Debug)]
pub struct StoreParser {
    body: Vec<u8>,
    rows: u64,
}

impl StoreParser {
    pub fn open(path: &Path, _source: &SourceRef) -> Result<Self> {
        let bytes = delimited::read_file(path)?;
        let consumed = bytes
            .iter()
            .position(|&b| b == b'\n')
            .map(|i| i + 1)
            .unwrap_or(bytes.len());
        Ok(Self {
            body: bytes[consumed.min(bytes.len())..].to_vec(),
            rows: 0,
        })
    }
}

impl DatasetParser for StoreParser {
    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        let mut b = StoreBuilders::new();
        let mut skipped = 0u64;
        for line in delimited::lines(&self.body) {
            if line.is_empty() {
                continue;
            }
            if line.len() != RECORD_LEN {
                // Every row in the corpus is 63 bytes. A different
                // length means the layout assumption is wrong for this
                // file, and guessing at a new layout would silently
                // mis-slice a dimension table.
                skipped += 1;
                tracing::warn!(
                    bytes = line.len(),
                    expected = RECORD_LEN,
                    "Delivery_Stores row is not 63 bytes; skipped"
                );
                continue;
            }
            b.push_row(line);
        }
        self.rows = b.rows();
        if !b.is_empty() {
            sink.push(b.finish(), cfg)?;
        }
        if skipped > 0 {
            tracing::warn!(skipped, "Delivery_Stores rows skipped on length");
        }
        Ok(())
    }

    fn expected_rows(&self) -> Option<u64> {
        Some(self.rows)
    }
}

pub fn parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(StoreParser::open(path, source)?))
}

pub fn tuning() -> DatasetTuning {
    DatasetTuning::new(100_000, 100_000, parser as ParserFactory)
}

/// Discover every `Delivery_Stores` under `input_root`.
pub fn discover(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::DeliveryStores);
    for entry in walkdir::WalkDir::new(input_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !is_ignored(e.path(), input_root))
    {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.file_name() != "Delivery_Stores" {
            continue;
        }
        let path = entry.path();
        let Some(year) = infer_year(path, input_root) else {
            inv.skipped.push((path.to_path_buf(), "no_year_ancestor"));
            continue;
        };
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        // Partitioned by year only: the roster is a whole-year
        // dimension, and the 31 categories of a year share it. The
        // category is kept on the identity for provenance but does not
        // become a partition key — partitioning by it would scatter
        // one 2 053-row table across 31 directories.
        let source = SourceRef::new(path)
            .with_year(year)
            .with_category(infer_category(path, input_root).unwrap_or_default());
        inv.files.push(DatasetFile {
            kind: DatasetKind::DeliveryStores,
            source,
            size_bytes: size,
        });
    }
    Ok(inv)
}

fn is_ignored(path: &Path, input_root: &Path) -> bool {
    if !path.is_dir() || path == input_root {
        return false;
    }
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    if name.starts_with('.') {
        return true;
    }
    let lower = name.to_ascii_lowercase();
    lower.starts_with("parsed stub") || lower == "demos trips external"
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, StringArray, UInt16Array, UInt32Array};

    /// A verbatim row from `Year1/beer/Delivery_Stores`.
    const ROW: &[u8] = b" 200039 GR 9.709999 BUFFALO/ROCHESTER         539 1219 Chain87 ";

    #[test]
    fn the_sample_row_is_exactly_63_bytes() {
        assert_eq!(ROW.len(), RECORD_LEN);
    }

    #[test]
    fn slices_the_row_at_the_measured_offsets() {
        // The header's own token positions would give Open@45, which
        // slices `539` in half; 46 is where it actually starts.
        assert_eq!(&ROW[offsets::IRI_KEY], b" 200039 ");
        assert_eq!(&ROW[offsets::OU], b"GR ");
        assert_eq!(&ROW[offsets::EST_ACV], b"9.709999 ");
        assert_eq!(
            delimited::trim_ascii_space(&ROW[offsets::MARKET]),
            b"BUFFALO/ROCHESTER"
        );
        assert_eq!(delimited::trim_ascii_space(&ROW[offsets::OPEN]), b"539");
        assert_eq!(delimited::trim_ascii_space(&ROW[offsets::CLSD]), b"1219");
        assert_eq!(
            delimited::trim_ascii_space(&ROW[offsets::MSKD_NAME]),
            b"Chain87"
        );
    }

    #[test]
    fn parses_the_sample_row_into_the_schema() {
        let mut b = StoreBuilders::new();
        b.push_row(ROW);
        let batch = b.finish();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            200_039
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "GR"
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            9.709999
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "BUFFALO/ROCHESTER"
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(0),
            539
        );
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(0),
            1219
        );
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Chain87"
        );
    }

    #[test]
    fn open_ended_stores_keep_the_9998_sentinel_rather_than_null() {
        // 9998 is a real, meaningful value ("still delivering"), not a
        // missing one. Nulling it would lose the distinction between a
        // store that closed in week 9998 and one that is still open.
        let row: Vec<u8> = {
            let mut r = ROW.to_vec();
            r[offsets::CLSD].copy_from_slice(b" 9998");
            r
        };
        assert_eq!(row.len(), RECORD_LEN);
        let mut b = StoreBuilders::new();
        b.push_row(&row);
        let batch = b.finish();
        let clsd = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert!(!clsd.is_null(0));
        assert_eq!(clsd.value(0), OPEN_ENDED);
    }

    #[test]
    fn a_row_of_the_wrong_length_is_refused_not_resliced() {
        let short: Vec<u8> = ROW[..40].to_vec();
        let r = std::panic::catch_unwind(move || {
            let mut b = StoreBuilders::new();
            b.push_row(&short);
        });
        assert!(r.is_err(), "a short row must not be silently resliced");
    }

    #[test]
    fn partitions_by_year_not_by_category() {
        // 31 categories in a year share one roster. Partitioning by
        // category would scatter a 2 053-row dimension across 31
        // directories for no query benefit.
        let s = SourceRef::new("/raw/Year1/beer/Delivery_Stores")
            .with_year(1)
            .with_category("beer");
        assert_eq!(
            s.partition_dir_for(DatasetKind::DeliveryStores),
            Path::new("year=1")
        );
    }

    #[test]
    fn schema_seven_columns_with_open_and_closed_weeks() {
        let s = schema();
        assert_eq!(s.fields().len(), 7);
        assert_eq!(s.field(4).name(), "open_week");
        assert_eq!(s.field(5).name(), "closed_week");
    }
}
