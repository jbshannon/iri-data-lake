//! The genuinely quoted-CSV datasets, and the small cross-reference
//! tables.
//!
//! Four classes share one implementation shape:
//!
//! - `iri_panel_trips` — household trip × store totals
//! - `iri_panel_static` — per-panelist trip counts
//! - `iri_panelist_demos` — per-panelist demographics
//! - `iri_ads_demos` — the ad panel's demographics
//!
//! plus two small dimensions that are shaped differently enough to be
//! worth their own module: `iri_chain_xref` and
//! `iri_manual_store_entry`.
//!
//! # Typed schema vs. per-file schema
//!
//! `iri_panel_trips` and `iri_panel_static` have **fixed** schemas
//! declared in code: 8 and 4 columns respectively, stable across all
//! twelve years, with a nullable column for the two the year-8 edition
//! added (`IRI_Key2`, `KRYSCENTS`) — the same "one schema, nullable
//! column" decision the PANEL class makes for `MINUTE`.
//!
//! `iri_panelist_demos` and `iri_ads_demos` get a **per-file schema**:
//! the year directories ship 37 columns (years 3–5) and 45 (years 8–11)
//! with renamed headers (`HH_RACE` → `Household Head Race`), and the ad
//! panel has its own 38. Forcing those onto one schema would mean either
//! dropping attributes or inventing nulls.
//!
//! # Why the `csv` crate, not the byte-level reader
//!
//! These files have quoted fields containing commas and embedded
//! newlines (`"Panelist ID","Combined Pre-Tax Income of HH",…`), which
//! is exactly what the `csv` crate exists to get right. PANEL uses the
//! byte-level reader in [`super::delimited`] instead because its
//! delimiter is fixed at runtime and it never quotes.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, Decimal128Builder, Int32Builder, Int8Builder, StringBuilder, UInt16Builder,
    UInt32Builder,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use csv::StringRecord;

use super::delimited;
use super::ingest::{BatchSink, DatasetParser, DatasetTuning, ParserFactory};
use crate::config::IngestConfig;
use crate::dataset::{infer_year, DatasetFile, DatasetInventory, SourceRef};
use crate::errors::{IngestError, Result};
use crate::model::DatasetKind;

pub const CSV_BATCH_ROWS: usize = 250_000;
pub const DEMOS_BATCH_ROWS: usize = 100_000;

// ---------------------------------------------------------------------
// iri_panel_trips
// ---------------------------------------------------------------------

/// Trips columns, in the union of both editions' order.
///
/// Years 1–7 ship `PANID, WEEK, IRI_Key, MINUTE, CENTS998, CENTS999`;
/// years 8–12 add `IRI_Key2` and `KRYSCENTS` and rename `IRI_Key` to
/// `IRI_KEY`. Reading by name handles both, and the two added columns
/// are nullable for the years that lack them.
pub const TRIPS_COLUMNS: &[&str] = &[
    "PANID",
    "WEEK",
    "IRI_KEY",
    "IRI_KEY2",
    "MINUTE",
    "CENTS998",
    "CENTS999",
    "KRYSCENTS",
];

/// The trips columns, with the type each actually holds on disk.
///
/// `CENTS998` / `CENTS999` / `KRYSCENTS` are `Float64`-shaped
/// in the source — values like `4470.8359375` and `4600.316406`,
/// 32-bit float renderings of 2-decimal amounts (`float32(4470.84)` is
/// `4470.8359375`). Storing them as `Float64` would silently drop
/// information (`0.7299998474` cannot be represented exactly in `f64`'s
/// 52-bit mantissa *and* is not the value the source meant).
///
/// The Bronze record keeps the raw **digits and fractional scale** of
/// the source's decimal string in three columns per value:
/// `(cents998_digits, cents998_scale)`, and so on. The original bytes
/// are recovered exactly by `digits / 10^scale`, and Silver can fold
/// these into the integer cents `iri_sales` carries once the unit is
/// known (per the data description, `sense 998` is the register-tape
/// total as entered, `sense 999` is the sum of scanned-item cents, and
/// the third sense variable is generally the same as `sense 999`
/// "scrubbed a bit better" — they *should* be integer cents, but the
/// corpus carries them as strings of decimal digits).
///
/// `CENTS998` is genuinely empty in 92–97 % of rows, which is a fact
/// about the corpus, not a parse failure.
pub fn trips_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("panid", DataType::UInt32, true),
        Field::new("week", DataType::UInt16, true),
        Field::new("iri_key", DataType::UInt32, true),
        Field::new("iri_key2", DataType::UInt32, true),
        Field::new("minute", DataType::UInt16, true),
        // Bronze digits+scale. `Decimal128(38, 0)` carries up to 38
        // decimal digits; the fractional count rides in the sibling
        // `scale` column, so the *type* has a fixed scale and the rows
        // do not.
        Field::new("cents998_digits", DataType::Decimal128(38, 0), true),
        Field::new("cents998_scale", DataType::Int8, true),
        Field::new("cents999_digits", DataType::Decimal128(38, 0), true),
        Field::new("cents999_scale", DataType::Int8, true),
        Field::new("kryscents_digits", DataType::Decimal128(38, 0), true),
        Field::new("kryscents_scale", DataType::Int8, true),
    ]))
}

/// The trips parser. One CSV, one fixed schema.
#[derive(Debug)]
pub struct TripsParser {
    records: Vec<StringRecord>,
    header: Vec<String>,
}

impl TripsParser {
    pub fn open(path: &Path, _source: &SourceRef) -> Result<Self> {
        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_path(path)
            .map_err(|e| IngestError::Discovery(format!("open {}: {e}", path.display())))?;
        let header: Vec<String> = rdr
            .headers()
            .map_err(|e| IngestError::Discovery(format!("header {}: {e}", path.display())))?
            .iter()
            .map(|s| s.trim().to_string())
            .collect();
        let mut records = Vec::new();
        for rec in rdr.records() {
            match rec {
                Ok(r) => records.push(r),
                Err(e) => tracing::warn!(error = %e, "malformed trips row skipped"),
            }
        }
        Ok(Self { records, header })
    }

    pub fn edition(&self) -> String {
        if self
            .header
            .iter()
            .any(|h| h.eq_ignore_ascii_case("KRYSCENTS"))
        {
            "may13 (years 8-12: +IRI_Key2 +KRYSCENTS)".to_string()
        } else {
            "jul08 (years 1-7)".to_string()
        }
    }
}

impl DatasetParser for TripsParser {
    fn schema(&self) -> SchemaRef {
        trips_schema()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        // Normalise the header so `IRI_Key` (years 1-7) and `IRI_KEY`
        // (years 8-12) both land in the `iri_key` column.
        let idx = upper_index(&self.header);
        fn at<'r>(
            idx: &HashMap<String, usize>,
            rec: &'r StringRecord,
            name: &str,
        ) -> Option<&'r str> {
            idx.get(name).and_then(|i| rec.get(*i)).map(str::trim)
        }
        let mut b = TripBuilders::new();
        for rec in self.records.drain(..) {
            push_u32(&mut b.panid, at(&idx, &rec, "PANID"));
            push_u16(&mut b.week, at(&idx, &rec, "WEEK"));
            push_u32(&mut b.iri_key, at(&idx, &rec, "IRI_KEY"));
            push_u32(&mut b.iri_key2, at(&idx, &rec, "IRI_KEY2"));
            push_u16(&mut b.minute, at(&idx, &rec, "MINUTE"));
            push_decimal(
                &mut b.cents998_digits,
                &mut b.cents998_scale,
                at(&idx, &rec, "CENTS998"),
            );
            push_decimal(
                &mut b.cents999_digits,
                &mut b.cents999_scale,
                at(&idx, &rec, "CENTS999"),
            );
            push_decimal(
                &mut b.kryscents_digits,
                &mut b.kryscents_scale,
                at(&idx, &rec, "KRYSCENTS"),
            );
            if b.panid.len() >= CSV_BATCH_ROWS {
                sink.push(b.finish(), cfg)?;
                b = TripBuilders::new();
            }
        }
        if b.panid.len() > 0 {
            sink.push(b.finish(), cfg)?;
        }
        Ok(())
    }

    fn note(&self) -> Option<String> {
        Some(format!("edition={}", self.edition()))
    }
}

/// Column builders for `iri_panel_trips`.
///
/// Three pairs of `(digits, scale)` columns — one per money value —
/// hold the raw decimal the source wrote. `Decimal128(38, 0)` carries
/// up to 38 decimal digits; the *fractional* count rides in the
/// sibling `Int8` `scale` column so the type can be a fixed-scale
/// `Decimal128` while the rows vary.
#[derive(Default)]
struct TripBuilders {
    panid: UInt32Builder,
    week: UInt16Builder,
    iri_key: UInt32Builder,
    iri_key2: UInt32Builder,
    minute: UInt16Builder,
    cents998_digits: Decimal128Builder,
    cents998_scale: Int8Builder,
    cents999_digits: Decimal128Builder,
    cents999_scale: Int8Builder,
    kryscents_digits: Decimal128Builder,
    kryscents_scale: Int8Builder,
}

impl TripBuilders {
    fn new() -> Self {
        let digits_dt = DataType::Decimal128(38, 0);
        Self {
            panid: UInt32Builder::new(),
            week: UInt16Builder::new(),
            iri_key: UInt32Builder::new(),
            iri_key2: UInt32Builder::new(),
            minute: UInt16Builder::new(),
            cents998_digits: Decimal128Builder::new().with_data_type(digits_dt.clone()),
            cents998_scale: Int8Builder::new(),
            cents999_digits: Decimal128Builder::new().with_data_type(digits_dt.clone()),
            cents999_scale: Int8Builder::new(),
            kryscents_digits: Decimal128Builder::new().with_data_type(digits_dt),
            kryscents_scale: Int8Builder::new(),
        }
    }

    fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            trips_schema(),
            vec![
                Arc::new(self.panid.finish()),
                Arc::new(self.week.finish()),
                Arc::new(self.iri_key.finish()),
                Arc::new(self.iri_key2.finish()),
                Arc::new(self.minute.finish()),
                Arc::new(self.cents998_digits.finish()),
                Arc::new(self.cents998_scale.finish()),
                Arc::new(self.cents999_digits.finish()),
                Arc::new(self.cents999_scale.finish()),
                Arc::new(self.kryscents_digits.finish()),
                Arc::new(self.kryscents_scale.finish()),
            ],
        )
        .expect("trips schema and build()'s column list are declared together")
    }
}

/// Map upper-cased column name to its position.
///
/// Owned keys rather than borrowed: `to_ascii_uppercase` allocates, and
/// a `HashMap<&str, _>` borrowing from a temporary is the classic
/// dangling-reference bug. One small allocation per file is cheaper
/// than the mistake.
fn upper_index(header: &[String]) -> HashMap<String, usize> {
    header
        .iter()
        .enumerate()
        .map(|(i, h)| (h.to_ascii_uppercase(), i))
        .collect()
}

fn push_u32(b: &mut UInt32Builder, v: Option<&str>) {
    match v.and_then(|s| delimited::parse_int(s.as_bytes())) {
        Some(n) if (0..=u32::MAX as i64).contains(&n) => b.append_value(n as u32),
        _ => b.append_null(),
    }
}

fn push_u16(b: &mut UInt16Builder, v: Option<&str>) {
    match v.and_then(|s| delimited::parse_int(s.as_bytes())) {
        Some(n) if (0..=u16::MAX as i64).contains(&n) => b.append_value(n as u16),
        _ => b.append_null(),
    }
}

/// Append a trips money value to a `(digits, scale)` pair.
///
/// The parser in [`delimited::parse_decimal`] records the raw decimal
/// the source wrote as `(digits, scale)`; the value is
/// `digits / 10^scale`. Empty and malformed inputs null both columns
/// together, which is what every downstream consumer wants: a row is
/// missing its value, not missing half of it.
fn push_decimal(digits: &mut Decimal128Builder, scale: &mut Int8Builder, v: Option<&str>) {
    use delimited::DecimalValue;
    let parsed = match v {
        Some(s) => delimited::parse_decimal(s.as_bytes()),
        None => DecimalValue::Missing,
    };
    match parsed {
        DecimalValue::Missing | DecimalValue::Unparseable => {
            digits.append_null();
            scale.append_null();
        }
        DecimalValue::Value {
            digits: d,
            scale: s,
            ..
        } => {
            digits.append_value(d);
            scale.append_value(s);
        }
    }
}

pub fn trips_parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(TripsParser::open(path, source)?))
}

pub fn trips_tuning() -> DatasetTuning {
    DatasetTuning::new(
        CSV_BATCH_ROWS,
        CSV_BATCH_ROWS,
        trips_parser as ParserFactory,
    )
}

/// Discover the twelve external trips files.
///
/// Years 1–7 are `trips<n> jul08.csv`; years 8–12 are `trips<n> may13.csv`.
/// The `.zip` siblings of years 8–11 are compressed copies of the same
/// CSVs and are not sources.
pub fn discover_trips(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::PanelTrips);
    let external = input_root.join("demos trips external");
    if !external.is_dir() {
        return Ok(inv);
    }
    for entry in walkdir::WalkDir::new(&external).max_depth(1) {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let lower = name.to_ascii_lowercase();
        if lower.ends_with(".zip") {
            inv.skipped
                .push((entry.path().to_path_buf(), "archive_of_a_csv"));
            continue;
        }
        let Some(rest) = lower.strip_prefix("trips") else {
            continue;
        };
        let Some((year, tail)) = rest.split_once(' ') else {
            continue;
        };
        let Ok(year) = year.parse::<u8>() else {
            continue;
        };
        if !(1..=12).contains(&year) || !tail.ends_with(".csv") {
            continue;
        }
        let size = std::fs::metadata(entry.path())
            .map(|m| m.len())
            .unwrap_or(0);
        inv.files.push(DatasetFile {
            kind: DatasetKind::PanelTrips,
            source: SourceRef::new(entry.path())
                .with_year(year)
                .with_edition(tail.trim_end_matches(".csv")),
            size_bytes: size,
        });
    }
    Ok(inv)
}

// ---------------------------------------------------------------------
// iri_panel_static
// ---------------------------------------------------------------------

pub fn static_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("panid", DataType::UInt32, true),
        Field::new("trip_count", DataType::Int32, true),
        Field::new("make_static", DataType::Utf8, true),
        Field::new("source_year", DataType::UInt8, true),
    ]))
}

#[derive(Debug)]
pub struct StaticParser {
    records: Vec<StringRecord>,
    header: Vec<String>,
}

impl StaticParser {
    pub fn open(path: &Path, _source: &SourceRef) -> Result<Self> {
        let (records, header) = read_csv(path)?;
        Ok(Self { records, header })
    }
}

impl DatasetParser for StaticParser {
    fn schema(&self) -> SchemaRef {
        static_schema()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        let idx = upper_index(&self.header);
        fn at<'r>(
            idx: &HashMap<String, usize>,
            rec: &'r StringRecord,
            name: &str,
        ) -> Option<&'r str> {
            idx.get(name).and_then(|i| rec.get(*i)).map(str::trim)
        }
        let mut panid = UInt32Builder::with_capacity(CSV_BATCH_ROWS);
        let mut trips = Int32Builder::with_capacity(CSV_BATCH_ROWS);
        let mut make_static = StringBuilder::new();
        let mut year = arrow_array::builder::UInt8Builder::with_capacity(CSV_BATCH_ROWS);
        for rec in self.records.drain(..) {
            push_u32(&mut panid, at(&idx, &rec, "PANID"));
            match at(&idx, &rec, "TRIP_COUNT").and_then(|s| delimited::parse_int(s.as_bytes())) {
                Some(n) if (i32::MIN as i64..=i32::MAX as i64).contains(&n) => {
                    trips.append_value(n as i32)
                }
                _ => trips.append_null(),
            }
            match at(&idx, &rec, "MAKE_STATIC") {
                Some(s) if !s.is_empty() => make_static.append_value(s),
                _ => make_static.append_null(),
            }
            match at(&idx, &rec, "YEAR").and_then(|s| delimited::parse_int(s.as_bytes())) {
                Some(n) if (1..=12).contains(&n) => year.append_value(n as u8),
                _ => year.append_null(),
            }
        }
        let batch = RecordBatch::try_new(
            static_schema(),
            vec![
                Arc::new(panid.finish()),
                Arc::new(trips.finish()),
                Arc::new(make_static.finish()),
                Arc::new(year.finish()),
            ],
        )?;
        sink.push(batch, cfg)?;
        Ok(())
    }
}

pub fn static_parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(StaticParser::open(path, source)?))
}

pub fn static_tuning() -> DatasetTuning {
    DatasetTuning::new(
        CSV_BATCH_ROWS,
        CSV_BATCH_ROWS,
        static_parser as ParserFactory,
    )
}

/// Discover the `static*` files.
///
/// The three files are **nested subsets**: `static1_5.csv` ⊂
/// `static 1_7.csv` ⊂ `static 1_12.csv` (30 154 / 47 767 / 87 051
/// rows). Only the largest is ingested; the smaller two are reported as
/// skipped so the decision is visible. Ingesting all three would write
/// the same rows three times into one table.
pub fn discover_panel_static(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::PanelStatic);
    let external = input_root.join("demos trips external");
    let mut candidates: Vec<(std::path::PathBuf, u64)> = Vec::new();
    for entry in walkdir::WalkDir::new(&external).max_depth(1) {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        let lower = entry.file_name().to_string_lossy().to_ascii_lowercase();
        if !lower.starts_with("static") || !lower.ends_with(".csv") {
            continue;
        }
        let size = std::fs::metadata(entry.path())
            .map(|m| m.len())
            .unwrap_or(0);
        candidates.push((entry.path().to_path_buf(), size));
    }
    // The largest file is the superset.
    candidates.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
    if let Some((path, size)) = candidates.first().cloned() {
        inv.files.push(DatasetFile {
            kind: DatasetKind::PanelStatic,
            source: SourceRef::new(&path).with_edition("1_12"),
            size_bytes: size,
        });
    }
    for (path, _) in candidates.iter().skip(1) {
        inv.skipped
            .push((path.clone(), "subset_of_the_largest_static_file"));
    }
    Ok(inv)
}

// ---------------------------------------------------------------------
// iri_panelist_demos / iri_ads_demos (per-file schema)
// ---------------------------------------------------------------------

/// A CSV parsed with a schema derived from its own header.
///
/// Every column is a string except `Panelist ID`, which is the one
/// numeric key the corpus is consistent about. The demographic columns
/// are codes (`HH_RACE`, `HH_AGE`, `HH_EDU`, …) whose code lists are not
/// published in the corpus; typing them as integers would be an
/// inference that silently reinterprets a codebook we do not have.
#[derive(Debug)]
pub struct DynamicCsvParser {
    schema: SchemaRef,
    records: Vec<StringRecord>,
    header: Vec<String>,
    label: String,
}

/// The column that is typed across every dynamic-schema CSV class.
const PANELIST_KEY: &str = "Panelist ID";

impl DynamicCsvParser {
    pub fn open(path: &Path, source: &SourceRef, label: &str) -> Result<Self> {
        let (records, header) = read_csv(path)?;
        let columns: Vec<String> = header
            .iter()
            .enumerate()
            .map(|(i, h)| {
                if h.trim().is_empty() {
                    format!("col_{i:03}")
                } else {
                    h.trim().to_string()
                }
            })
            .collect();
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| {
                if c.eq_ignore_ascii_case(PANELIST_KEY) {
                    Field::new(c, DataType::UInt32, true)
                } else {
                    Field::new(c, DataType::Utf8, true)
                }
            })
            .collect();
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            records,
            header: columns,
            label: format!("{} {}", label, source.path.display()),
        })
    }
}

impl DatasetParser for DynamicCsvParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        let n = self.schema.fields().len();
        let key_at = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(PANELIST_KEY));
        let mut key = UInt32Builder::new();
        let mut strs: Vec<StringBuilder> = (0..n).map(|_| StringBuilder::new()).collect();
        for rec in self.records.drain(..) {
            for (i, builder) in strs.iter_mut().enumerate() {
                let cell = rec.get(i).map(str::trim).unwrap_or("");
                if Some(i) == key_at {
                    push_u32(&mut key, Some(cell));
                } else if cell.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(cell);
                }
            }
        }
        let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(n);
        for (i, mut s) in strs.into_iter().enumerate() {
            if Some(i) == key_at {
                cols.push(Arc::new(key.finish()));
            } else {
                cols.push(Arc::new(s.finish()));
            }
        }
        let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
        sink.push(batch, cfg)?;
        Ok(())
    }

    fn schema_fingerprint(&self) -> Option<String> {
        Some(super::ingest::fingerprint(
            self.schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<String>>(),
        ))
    }

    fn note(&self) -> Option<String> {
        Some(format!(
            "columns={} source={}",
            self.header.len(),
            self.label
        ))
    }
}

pub fn panel_demos_parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(DynamicCsvParser::open(path, source, "demos")?))
}

pub fn ads_demos_parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(DynamicCsvParser::open(path, source, "ads-demos")?))
}

pub fn demos_tuning(parser: ParserFactory) -> DatasetTuning {
    DatasetTuning::new(DEMOS_BATCH_ROWS, DEMOS_BATCH_ROWS, parser)
}

/// Discover the per-year `DEMOS.CSV` files.
///
/// 217 files across years 3, 4, 5, 8, 9, 10 and 11 — but only **7
/// distinct** (every category in a year ships the same bytes). The
/// dedup decision is `docs/other_sources.md` § Duplicate handling.
pub fn discover_panel_demos(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::PanelistDemos);
    for entry in walkdir::WalkDir::new(input_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !is_ignored(e.path(), input_root))
    {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        if !entry.path().file_name().is_some_and(|n| n == "DEMOS.CSV") {
            continue;
        }
        let path = entry.path();
        let Some(year) = infer_year(path, input_root) else {
            inv.skipped.push((path.to_path_buf(), "no_year_ancestor"));
            continue;
        };
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        inv.files.push(DatasetFile {
            kind: DatasetKind::PanelistDemos,
            source: SourceRef::new(path)
                .with_year(year)
                .with_category(infer_category(path, input_root).unwrap_or_default()),
            size_bytes: size,
        });
    }
    Ok(inv)
}

/// Discover the ad-panel demographics.
///
/// 12 years, and the corpus ships two spellings: `ads demo<n>.csv` for
/// years 1–8 and `ads demos<n>.csv` for years 9–12. The extension case
/// also varies per file (`ads demo6.CSV` but `ads demo7.csv`), so the
/// filter has to be case-insensitive — and then the lowercase twin
/// question arises, which is why this accepts both the `demo` and
/// `demos` stems rather than one of them.
///
/// On a case-insensitive filesystem (APFS, the default) the uppercase
/// twins are the same file, so 12 are discovered. On a case-sensitive
/// one there are 24 and the uppercase half is reported as skipped.
/// (`docs/data_layout.md` lists 12 files; the directory holds 24
/// entries.)
pub fn discover_ads_demos(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::AdsDemos);
    let external = input_root.join("demos trips external");
    if !external.is_dir() {
        return Ok(inv);
    }
    for entry in walkdir::WalkDir::new(&external).max_depth(1) {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let lower = name.to_ascii_lowercase();
        // Both stems occur: `ads demo<n>` (years 1-8) and
        // `ads demos<n>` (years 9-12). Matching only one silently drops
        // four years.
        // "demos" is tried first: it is the longer stem, and
        // "ads demos12.csv" would otherwise match "ads demo" and leave
        // a leading `s` where the year digits should be.
        let rest = lower
            .strip_prefix("ads demos")
            .or_else(|| lower.strip_prefix("ads demo"));
        let Some(rest) = rest else { continue };
        if !rest.ends_with(".csv") {
            continue;
        }
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        let Ok(year) = digits.parse::<u8>() else {
            continue;
        };
        if !(1..=12).contains(&year) {
            continue;
        }
        let path = entry.path();
        if name != lower {
            // The uppercase-extension twin of a file we already take.
            // On APFS this entry never appears; on a case-sensitive
            // filesystem it does.
            inv.skipped
                .push((path.to_path_buf(), "uppercase_twin_of_a_lowercase_file"));
            continue;
        }
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        inv.files.push(DatasetFile {
            kind: DatasetKind::AdsDemos,
            source: SourceRef::new(path).with_year(year),
            size_bytes: size,
        });
    }
    Ok(inv)
}

fn infer_category(path: &Path, input_root: &Path) -> Option<String> {
    crate::dataset::infer_category(path, input_root)
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

// ---------------------------------------------------------------------
// iri_chain_xref and iri_manual_store_entry
// ---------------------------------------------------------------------

/// The chain cross-reference: masked chain number by year.
///
/// The file is a wide grid — header `Year1,Year2,…,Year12`, one row per
/// chain, cells left empty where that chain has no mapping for that
/// year. An empty cell is a real "no mapping", so the cells are
/// nullable `UInt16` and not coerced to zero: chain 0 is not a chain.
pub fn chain_xref_schema(n_years: usize) -> SchemaRef {
    let mut fields = vec![Field::new("chain", DataType::UInt32, true)];
    for y in 1..=n_years {
        fields.push(Field::new(format!("year{y}"), DataType::UInt16, true));
    }
    Arc::new(Schema::new(fields))
}

#[derive(Debug)]
pub struct ChainXrefParser {
    n_years: usize,
    records: Vec<StringRecord>,
    header: Vec<String>,
}

impl ChainXrefParser {
    pub fn open(path: &Path, _source: &SourceRef) -> Result<Self> {
        let (records, header) = read_csv(path)?;
        Ok(Self {
            n_years: header.len(),
            records,
            header,
        })
    }
}

fn read_csv(path: &Path) -> Result<(Vec<StringRecord>, Vec<String>)> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(path)
        .map_err(|e| IngestError::Discovery(format!("open {}: {e}", path.display())))?;
    let header: Vec<String> = rdr
        .headers()
        .map_err(|e| IngestError::Discovery(format!("header {}: {e}", path.display())))?
        .iter()
        .map(|s| s.trim().to_string())
        .collect();
    let mut records = Vec::new();
    let mut malformed = 0usize;
    for rec in rdr.records() {
        match rec {
            Ok(r) => records.push(r),
            Err(e) => {
                // Counted and reported rather than silently dropped:
                // a malformed row is a fact about the source, and
                // "how many rows did not survive" is the first
                // question anyone asks of an ingest.
                malformed += 1;
                tracing::debug!(source = %path.display(), error = %e, "malformed CSV row skipped");
            }
        }
    }
    if malformed > 0 {
        tracing::warn!(
            source = %path.display(),
            malformed,
            kept = records.len(),
            "CSV rows were unreadable and were skipped"
        );
    }
    Ok((records, header))
}

impl DatasetParser for ChainXrefParser {
    fn schema(&self) -> SchemaRef {
        chain_xref_schema(self.n_years)
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        let mut chain = UInt32Builder::with_capacity(self.records.len());
        let mut cells: Vec<UInt16Builder> = (0..self.n_years)
            .map(|_| UInt16Builder::with_capacity(self.records.len()))
            .collect();
        for rec in self.records.drain(..) {
            // The grid is offset by one: cell 0 is the chain id and
            // cell i+1 is that chain's number in year i.
            push_u32(&mut chain, rec.get(0));
            for (i, cell) in cells.iter_mut().enumerate() {
                push_u16(cell, rec.get(i + 1));
            }
        }
        let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(self.n_years + 1);
        cols.push(Arc::new(chain.finish()));
        for mut c in cells {
            cols.push(Arc::new(c.finish()));
        }
        let batch = RecordBatch::try_new(self.schema(), cols)?;
        sink.push(batch, cfg)?;
        Ok(())
    }

    fn note(&self) -> Option<String> {
        Some(format!(
            "year_columns={} header={:?}",
            self.n_years, self.header
        ))
    }
}

pub fn chain_xref_parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(ChainXrefParser::open(path, source)?))
}

pub fn chain_xref_tuning() -> DatasetTuning {
    DatasetTuning::new(10_000, 10_000, chain_xref_parser as ParserFactory)
}

/// Discover the masked-chain cross-reference.
///
/// Four files exist and they are nested supersets by year span:
/// `masked_chain_xref.csv` (5 years), `masked_chain_xref1_7.csv` (7),
/// `masked_chain_xref1_12.csv` (12). Only the widest is ingested; the
/// narrower ones are reported as skipped.
pub fn discover_chain_xref(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::ChainXref);
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    collect_csv_matching(
        input_root,
        "demos trips external",
        "masked_chain_xref",
        &mut candidates,
    );
    // The top-level copy is the same content as the external one.
    collect_csv_matching(
        input_root,
        "",
        "masked_chains crossreference",
        &mut candidates,
    );
    pick_widest(input_root, &candidates, &mut inv, "masked_chain_xref");
    Ok(inv)
}

/// Discover the manual store-entry tables.
///
/// `manual store entry external.csv` (years 1–4) and
/// `manual store entry external 8_11.csv` (years 8–11) do **not**
/// overlap, so unlike the chain cross-reference both are ingested. The
/// top-level `manual store entry external 8_12.csv` is a near-duplicate
/// of the external copy and is skipped.
pub fn discover_manual_store_entry(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::ManualStoreEntry);
    let external = input_root.join("demos trips external");
    if !external.is_dir() {
        return Ok(inv);
    }
    for entry in walkdir::WalkDir::new(&external).max_depth(1) {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        let lower = entry.file_name().to_string_lossy().to_ascii_lowercase();
        if !lower.starts_with("manual store entry") || !lower.ends_with(".csv") {
            continue;
        }
        let path = entry.path();
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        // The two files cover different, non-overlapping year spans:
        // `… external.csv` is years 1–4, `… external 8_11.csv` is years
        // 8–11. The span is taken from the trailing digits so the two
        // land in separate partitions rather than colliding.
        let span = year_span(&lower).unwrap_or_else(|| "unknown".to_string());
        inv.files.push(DatasetFile {
            kind: DatasetKind::ManualStoreEntry,
            source: SourceRef::new(path).with_edition(span),
            size_bytes: size,
        });
    }
    // Report the top-level duplicate rather than silently dropping it.
    for entry in walkdir::WalkDir::new(input_root).max_depth(1) {
        let Ok(entry) = entry else { continue };
        if entry.file_type().is_file()
            && entry
                .file_name()
                .to_string_lossy()
                .to_ascii_lowercase()
                .starts_with("manual store entry")
        {
            inv.skipped
                .push((entry.path().to_path_buf(), "top_level_duplicate"));
        }
    }
    Ok(inv)
}

fn collect_csv_matching(root: &Path, sub: &str, needle: &str, out: &mut Vec<std::path::PathBuf>) {
    let dir = if sub.is_empty() {
        root.to_path_buf()
    } else {
        root.join(sub)
    };
    for entry in walkdir::WalkDir::new(&dir)
        .max_depth(1)
        .into_iter()
        .flatten()
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        if name.starts_with(needle) && name.ends_with(".csv") {
            out.push(entry.path().to_path_buf());
        }
    }
}

/// Ingest only the candidate with the most year columns.
fn pick_widest(
    root: &Path,
    candidates: &[std::path::PathBuf],
    inv: &mut DatasetInventory,
    needle: &str,
) {
    let mut scored: Vec<(usize, std::path::PathBuf)> = candidates
        .iter()
        .map(|p| {
            let n = delimited::read_file(p)
                .ok()
                .and_then(|b| delimited::first_line(&b).map(|l| l.split(|&b| b == b',').count()))
                .unwrap_or(0);
            (n, p.clone())
        })
        .collect();
    scored.sort_by_key(|(n, _)| std::cmp::Reverse(*n));
    if let Some((n, path)) = scored.first().cloned() {
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        inv.files.push(DatasetFile {
            kind: DatasetKind::ChainXref,
            source: SourceRef::new(&path).with_edition(format!("{n}_year_columns")),
            size_bytes: size,
        });
    }
    for (_, p) in scored.iter().skip(1) {
        inv.skipped.push((
            p.clone(),
            Box::leak(format!("narrower_than_the_widest_{needle}").into_boxed_str()),
        ));
    }
    let _ = root;
}

/// Extract a trailing year span like `1_4` or `8_11` from a filename.
///
/// `manual store entry external.csv` has no digits at all and covers
/// years 1–4; `manual store entry external 8_11.csv` declares its span
/// explicitly. Deriving the span from the filename (rather than a
/// hard-coded table) is what keeps the two from writing into the same
/// partition directory.
fn year_span(lower_name: &str) -> Option<String> {
    let stem = lower_name.trim_end_matches(".csv");
    let digits: String = stem
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit() || *c == '_')
        .collect();
    if digits.is_empty() {
        return None;
    }
    Some(
        digits
            .chars()
            .rev()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>(),
    )
}

pub fn manual_store_entry_tuning() -> DatasetTuning {
    DatasetTuning::new(10_000, 10_000, chain_xref_parser as ParserFactory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};

    fn write(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }
    #[test]
    fn reads_both_trips_editions_onto_one_schema() {
        // Years 1-7 and 8-12 differ in their column set, but both
        // editions normalise onto the same schema: Bronze is one
        // logical table with three money columns, each a
        // `(digits, scale)` pair.
        let dir = tempfile::tempdir().unwrap();
        let y1 = write(
            dir.path(),
            "trips1 jul08.csv",
            "PANID,WEEK,IRI_Key,MINUTE,CENTS998,CENTS999\r\n             1100016,1114,234140,9701,,4470.8359375\r\n",
        );
        let y8 = write(
            dir.path(),
            "trips8 may13.csv",
            "PANID,WEEK,IRI_KEY,IRI_Key2,MINUTE,CENTS998,CENTS999,KRYSCENTS\r\n             1100016,1508,8003043,,2406,,2531.82959,2531.82959\r\n",
        );

        let p = TripsParser::open(&y1, &SourceRef::new(&y1)).unwrap();
        assert_eq!(p.schema(), trips_schema());
        assert!(p.edition().starts_with("jul08"));
        let p8 = TripsParser::open(&y8, &SourceRef::new(&y8)).unwrap();
        assert_eq!(p8.schema(), trips_schema(), "one schema for both editions");
        assert!(p8.edition().starts_with("may13"));
        assert_eq!(trips_schema().fields().len(), 11);
    }

    #[test]
    fn trips_money_columns_carry_digits_and_scale_losslessly() {
        // The corpus holds `4470.8359375`, which is float32(4470.84).
        // Storing it as Float64 would be lossy in either direction:
        // an f64 cannot represent 0.7299998474 exactly, and the
        // "true" value is the float32 render of 0.73, not 0.73. The
        // digits+scale pair records the raw decimal the source wrote.
        let s = trips_schema();
        for prefix in ["cents998", "cents999", "kryscents"] {
            let fd = s
                .fields()
                .iter()
                .find(|f| f.name().as_str() == format!("{prefix}_digits").as_str())
                .unwrap();
            assert_eq!(
                fd.data_type(),
                &DataType::Decimal128(38, 0),
                "{prefix}_digits must be Decimal128(38, 0)"
            );
            let fs = s
                .fields()
                .iter()
                .find(|f| f.name().as_str() == format!("{prefix}_scale").as_str())
                .unwrap();
            assert_eq!(
                fs.data_type(),
                &DataType::Int8,
                "{prefix}_scale must be Int8"
            );
        }
        // And the value survives the round trip: 4470.8359375 ->
        // digits=44708359375, scale=10; 0/empty -> null.
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            "trips1 jul08.csv",
            "PANID,WEEK,IRI_Key,MINUTE,CENTS998,CENTS999\r\n             1100016,1114,234140,9701,,4470.8359375\r\n",
        );
        let mut parser = TripsParser::open(&p, &SourceRef::new(&p)).unwrap();
        let out = tempfile::tempdir().unwrap();
        let mut sink = BatchSink::new_test(out.path().to_path_buf(), trips_schema());
        let cfg = IngestConfig::for_test();
        parser.parse(&mut sink, &cfg).unwrap();
        sink.flush(&cfg).unwrap();
        let wb = sink.read_all(&cfg);
        // panid=0, week=1, iri_key=2, iri_key2=3, minute=4,
        // cents998_digits=5, cents998_scale=6,
        // cents999_digits=7, cents999_scale=8,
        // kryscents_digits=9, kryscents_scale=10.
        let c999_digits = wb
            .column(7)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert!(
            !c999_digits.is_null(0),
            "a populated CENTS999 must not be NULL"
        );
        assert_eq!(c999_digits.value(0), 44_708_359_375_i128);
        let c999_scale = wb
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Int8Array>()
            .unwrap();
        assert_eq!(
            c999_scale.value(0),
            7,
            "4470.8359375 -> 7 fractional digits"
        );
        // CENTS998 is empty -> both digits and scale null.
        assert!(wb
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap()
            .is_null(0));
        assert!(wb
            .column(6)
            .as_any()
            .downcast_ref::<arrow_array::Int8Array>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn the_static_file_has_four_columns() {
        let s = static_schema();
        assert_eq!(s.fields().len(), 4);
        assert_eq!(s.field(0).name(), "panid");
        assert_eq!(s.field(1).name(), "trip_count");
        assert_eq!(s.field(2).name(), "make_static");
        assert_eq!(s.field(3).name(), "source_year");
    }

    #[test]
    fn parses_the_static_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            "static 1_12.csv",
            "PANID,Trip_Count,make_static,year\r\n1100032,58,no,1\r\n1100156,84,yes,1\r\n",
        );
        let mut parser = StaticParser::open(&p, &SourceRef::new(&p)).unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let mut sink = BatchSink::new_test(dir2.path().to_path_buf(), static_schema());
        let cfg = IngestConfig::for_test();
        parser.parse(&mut sink, &cfg).unwrap();
        sink.flush(&cfg).unwrap();
        assert_eq!(sink.rows, 2);
        let wb = sink.read_all(&cfg);
        let trips = wb.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(trips.value(0), 58);
        // The year column: `1` for both rows.
        let years = wb
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .unwrap();
        assert_eq!(years.value(0), 1);
        let st = wb.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(st.value(1), "yes");
    }

    #[test]
    fn chain_xref_schema_grows_with_the_year_span() {
        assert_eq!(chain_xref_schema(12).fields().len(), 13);
        assert_eq!(chain_xref_schema(5).fields().len(), 6);
        assert_eq!(chain_xref_schema(12).field(12).name(), "year12");
    }

    #[test]
    fn an_empty_chain_cell_is_null_not_chain_zero() {
        // The grid leaves a cell empty where a chain has no mapping for
        // a year. Chain 0 is not a chain, so a blank must not become 0.
        let s = chain_xref_schema(3);
        assert!(s.field(1).is_nullable());
    }
}
