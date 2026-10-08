//! `iri_panel` — household panel trip × item records.
//!
//! The PANEL files are the household counterpart to the store-level
//! sales facts: where `iri_sales` says "store 200039 sold 3 units of
//! item 85674 in week 1114", PANEL says "household 1225359 bought 1
//! unit of that item at that store in that week". They are the join
//! that turns an aggregate into a panel, so they matter more than their
//! size suggests: 824 MB and ~1–2 M rows/year against 143 GB and 2.7 B.
//!
//! # Four dialects, not three
//!
//! `docs/data_layout.md` documents three (tab for years 1–2,
//! whitespace for 3–7, comma for 8–12). Sniffing all 1110 header lines
//! finds **four**:
//!
//! | files | header | years |
//! |---:|---|---|
//! | 461 | `PANID,WEEK,MINUTE,UNITS,OUTLET,DOLLARS,IRI_KEY,COLUPC` | 8–12 |
//! | 368 | `PANID WEEK UNITS OUTLET DOLLARS IRI_KEY COLUPC` | 4–7 |
//! | 276 | `PANID\tWEEK\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC` | 1–3 |
//! | **1** | `PANID\tWEEK\tMINUTE\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC` | **11** |
//!
//! The fourth is `Year11/diapers/diapers_PANEL_GK_1635_1686.DAT`: a
//! tab-delimited file carrying the year-8+ `MINUTE` column. "Years 8+
//! are comma-delimited" is wrong for exactly one file out of 1110, and
//! a delimiter inferred from the year mis-parses it *silently* — a
//! comma split of a tab-delimited line yields one field, which does not
//! look like an error, just a row with a single column.
//!
//! So the **header line is the dispatcher**, consulted per file. It has
//! to be anyway: the `MINUTE` column is present or absent on the same
//! line as the delimiter, and both facts are only knowable from the
//! bytes.
//!
//! # Two zero-byte files
//!
//! `Year1/beer/beer_PANEL_DR_1114_1165.dat` and one Year-2 equivalent
//! are genuinely empty: no header, no rows. A parser that requires a
//! header turns them into failures, and a failure is a permanent
//! manifest entry that retries forever. They are recorded as empty
//! successes instead — see `DatasetOutcome::Empty`.
//!
//! # One output schema for twelve years
//!
//! All four dialects normalise onto an 8-column schema with `minute`
//! **nullable**. The pre-year-8 dialects have no MINUTE column at all,
//! which is a real difference in the data rather than a missing value,
//! but modelling it as a nullable column is what lets one query span
//! all twelve years. The alternative — a 7-column schema for years 1–7
//! and an 8-column one for 8–12 — gives two Parquet schemas for one
//! logical table, which every downstream reader then has to know about.
//! Note that `minute = 0` would be *wrong* rather than merely imprecise:
//! it is indistinguishable from a real midnight trip.

use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::ArrayBuilder;
use arrow_array::builder::{
    Decimal128Builder, Int32Builder, StringBuilder, UInt16Builder, UInt32Builder,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::delimited;
use super::delimited::DecimalValue;
use super::ingest::{BatchSink, DatasetParser, DatasetTuning, ParserFactory};
use crate::config::IngestConfig;
use crate::dataset::{infer_category, infer_year, DatasetFile, DatasetInventory, SourceRef};
use crate::errors::Result;
use crate::model::DatasetKind;

/// Rows buffered in Arrow before a batch is flushed.
pub const BATCH_ROWS: usize = 500_000;
/// Max rows per output Parquet file. One batch per file: the largest
/// PANEL source is ~2 MB, and splitting it further would produce more
/// files than the manifest is worth.
pub const MAX_ROWS_PER_FILE: usize = 500_000;

/// The one schema every PANEL dialect normalises onto.
///
/// `units_digits`/`units_scale` and `dollars_digits`/`dollars_scale`
/// are a **(digits, scale)** pair that losslessly records the raw
/// decimal string. Bronze preserves everything the source held: a
/// `Float64` would round `0.7299998474` to `0.73` and
/// `4.3091992188` to `4.31` (or fail outright), whereas the digits+scale
/// pair recovers the exact bytes (`digits / 10^scale`). The integer
/// columns `units` and `dollars_cents` stay as a convenience for
/// joins against `iri_sales` and for queries that filter by a whole
/// number — a Bronze record is only well-typed when it is *both*
/// columns, and the digits+scale pair is the source of truth.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("panid", DataType::UInt32, true),
        Field::new("week", DataType::UInt16, true),
        Field::new("minute", DataType::UInt16, true),
        Field::new("units", DataType::Int32, true),
        // Bronze digits+scale. `Decimal128(38, 0)` carries up to 38
        // decimal digits; we use it with a fixed scale of 0 so the
        // *fractional* digits are recorded in the separate `scale`
        // column. The value is `digits / 10^scale`; for scale=0 it is
        // just `digits`.
        Field::new("units_digits", DataType::Decimal128(38, 0), true),
        Field::new("units_scale", DataType::Int8, true),
        Field::new("outlet", DataType::Utf8, true),
        Field::new("dollars_cents", DataType::Int64, true),
        Field::new("dollars_digits", DataType::Decimal128(38, 0), true),
        Field::new("dollars_scale", DataType::Int8, true),
        Field::new("iri_key", DataType::UInt32, true),
        Field::new("colupc", DataType::Utf8, true),
    ]))
}

/// How a PANEL file's bytes are laid out.
///
/// Two independent facts, both read from the header line:
///
/// - **the delimiter** (tab / whitespace / comma), and
/// - **whether a MINUTE column is present**.
///
/// They are independent because one file in the corpus is
/// tab-delimited *with* MINUTE (`Year11/diapers/
/// diapers_PANEL_GK_1635_1686.DAT`). A `Dialect` that bundled them
/// would have to invent a fifth variant for it, or — worse — infer the
/// delimiter from the MINUTE column and mis-split every row of that
/// file. Both facts belong on the value, decided per file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dialect {
    pub delim: Delim,
    pub has_minute: bool,
}

/// The byte a PANEL row is split on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delim {
    Tab,
    Whitespace,
    Comma,
}

impl Dialect {
    pub fn label(self) -> &'static str {
        match self.delim {
            Delim::Tab => "tab",
            Delim::Whitespace => "whitespace",
            Delim::Comma => "comma",
        }
    }

    /// Column names in on-disk order, decided by the two header facts.
    pub fn column_names(self) -> &'static [&'static [u8]] {
        if self.has_minute {
            &[
                b"PANID", b"WEEK", b"MINUTE", b"UNITS", b"OUTLET", b"DOLLARS", b"IRI_KEY",
                b"COLUPC",
            ]
        } else {
            &[
                b"PANID", b"WEEK", b"UNITS", b"OUTLET", b"DOLLARS", b"IRI_KEY", b"COLUPC",
            ]
        }
    }

    fn split(self, line: &[u8]) -> Vec<&[u8]> {
        match self.delim {
            Delim::Tab => delimited::split_delimited(line, b'\t'),
            Delim::Whitespace => delimited::split_whitespace(line),
            Delim::Comma => delimited::split_delimited(line, b','),
        }
    }
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}+minute", self.label())?;
        if !self.has_minute {
            write!(f, "+no-minute")?;
        }
        Ok(())
    }
}

/// Decide the dialect from a header line.
///
/// The decision is made on the bytes, never on the year, because the
/// year inference is wrong for exactly one file in the corpus and
/// because a file's own header is the only thing that cannot lie about
/// its own format.
///
/// Returns `None` for a source with no header at all, which means
/// "empty file", not "malformed file".
pub fn dialect_from_header(header: &[u8]) -> Option<Dialect> {
    // Tab wins over comma even when MINUTE is present: the year-11
    // outlier is exactly that case, and checking for the tab first is
    // what makes it parse correctly.
    let delim = if header.contains(&b'\t') {
        Delim::Tab
    } else if header.contains(&b',') {
        Delim::Comma
    } else {
        Delim::Whitespace
    };
    // MINUTE is looked for *after* the delimiter is known, using that
    // delimiter. Splitting the comma header on whitespace would yield
    // one token (`PANID,WEEK,MINUTE,…`) and never match.
    let fields = match delim {
        Delim::Tab => delimited::split_delimited(header, b'\t'),
        Delim::Comma => delimited::split_delimited(header, b','),
        Delim::Whitespace => delimited::split_whitespace(header),
    };
    let has_minute = fields
        .iter()
        .any(|f| delimited::trim_ascii_space(f) == b"MINUTE");
    Some(Dialect { delim, has_minute })
}

/// Pull one named column out of a row.
///
/// Dispatching on the *name* rather than the position is what makes the
/// four dialects one code path: `DOLLARS` is field 4 in the tab and
/// whitespace dialects and field 5 in the comma dialect, because MINUTE
/// was inserted. Three sets of positional lookups would drift the first
/// time a fifth dialect appears.
fn named<'a>(names: &[&'a [u8]], fields: &[&'a [u8]], want: &[u8]) -> Option<&'a [u8]> {
    names
        .iter()
        .position(|n| *n == want)
        .and_then(|i| fields.get(i).copied())
}

/// Column builders for one PANEL file.
///
/// Named `PanelBuilders` rather than being inline in `parse` so that the
/// row-level tests can push a single row and assert on the resulting
/// batch without going through a file, a mmap and a Parquet writer.
#[derive(Debug)]
pub struct PanelBuilders {
    panid: UInt32Builder,
    week: UInt16Builder,
    minute: UInt16Builder,
    units: Int32Builder,
    units_digits: Decimal128Builder,
    units_scale: arrow_array::builder::Int8Builder,
    outlet: StringBuilder,
    dollars_cents: arrow_array::builder::Int64Builder,
    dollars_digits: Decimal128Builder,
    dollars_scale: arrow_array::builder::Int8Builder,
    iri_key: UInt32Builder,
    colupc: StringBuilder,
    rows: u64,
    /// Rows where UNITS held a genuinely fractional count, which the
    /// integer column cannot represent (the digits+scale columns do).
    fractional_units: u64,
    /// Rows where DOLLARS could not be parsed in either encoding.
    unparseable_dollars: u64,
}

impl Default for PanelBuilders {
    fn default() -> Self {
        Self::new()
    }
}

impl PanelBuilders {
    pub fn new() -> Self {
        // Build with the schema's declared Decimal128 type so the
        // builder emits matching arrays. Scale is fixed at 0 in the
        // type and recorded per-row in the sibling `scale` column.
        let digits_dt = DataType::Decimal128(38, 0);
        Self {
            panid: UInt32Builder::new(),
            week: UInt16Builder::new(),
            minute: UInt16Builder::new(),
            units: Int32Builder::new(),
            units_digits: Decimal128Builder::new().with_data_type(digits_dt.clone()),
            units_scale: arrow_array::builder::Int8Builder::new(),
            outlet: StringBuilder::new(),
            dollars_cents: arrow_array::builder::Int64Builder::new(),
            dollars_digits: Decimal128Builder::new().with_data_type(digits_dt),
            dollars_scale: arrow_array::builder::Int8Builder::new(),
            iri_key: UInt32Builder::new(),
            colupc: StringBuilder::new(),
            rows: 0,
            fractional_units: 0,
            unparseable_dollars: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.panid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append one row already split into fields for `dialect`.
    ///
    /// `dialect.column_names()` is assumed to describe `fields`. A short
    /// row is rejected by the caller before it gets here.
    pub fn push_row(&mut self, dialect: Dialect, fields: &[&[u8]]) {
        let names = dialect.column_names();
        debug_assert!(
            fields.len() >= names.len(),
            "caller must reject short rows before push_row"
        );
        // MINUTE is absent from the pre-year-8 dialects. That absence
        // is the reason the column is nullable; `minute = 0` would be
        // indistinguishable from a real midnight trip.
        //
        // It is *not* a minute-of-day. Measured over the corpus the
        // values run 488..9943, so any "minutes since midnight" bound
        // (1440) would null out most of the column. It is an IRI time
        // bucket code; only the u16 range is enforced here, and the
        // codebook for it is not in the corpus.
        push_u16_at(&mut self.minute, named(names, fields, b"MINUTE"));
        push_u32(&mut self.panid, named(names, fields, b"PANID"));
        push_u16(&mut self.week, named(names, fields, b"WEEK"));
        // UNITS and DOLLARS both pass through `parse_decimal`, which
        // records the raw digits and their fractional count in
        // separate columns. The integer `units` and `dollars_cents`
        // stay so a query can join against `iri_sales` and filter by
        // a whole number; the digits+scale pair is the source of
        // truth and survives values the integer columns cannot.
        let units_field = named(names, fields, b"UNITS");
        match units_field {
            Some(f) => match delimited::parse_decimal(f) {
                DecimalValue::Missing => {
                    self.units_digits.append_null();
                    self.units_scale.append_null();
                    self.units.append_null();
                }
                DecimalValue::Unparseable => {
                    self.units_digits.append_null();
                    self.units_scale.append_null();
                    self.units.append_null();
                }
                DecimalValue::Value { digits, scale, .. } => {
                    self.units_digits.append_value(digits);
                    self.units_scale.append_value(scale);
                    // The integer column records whole numbers only.
                    // A genuinely fractional value stays in the
                    // digits+scale pair; the integer column is null
                    // and the row is counted.
                    if scale == 0 && digits >= 0 && digits <= i32::MAX as i128 {
                        self.units.append_value(digits as i32);
                    } else if scale > 0 {
                        self.units.append_null();
                        self.fractional_units += 1;
                    } else {
                        // Negative or beyond i32; digits+scale carry it.
                        self.units.append_null();
                    }
                }
            },
            None => {
                self.units_digits.append_null();
                self.units_scale.append_null();
                self.units.append_null();
            }
        }
        push_u32(&mut self.iri_key, named(names, fields, b"IRI_KEY"));
        self.outlet
            .append_option(text(named(names, fields, b"OUTLET")));
        // COLUPC keeps its leading zeros (`0011497450009`), which is
        // why it is a string rather than a number.
        self.colupc
            .append_option(text(named(names, fields, b"COLUPC")));
        let dollars_field = named(names, fields, b"DOLLARS");
        match dollars_field {
            Some(f) => match delimited::parse_decimal(f) {
                DecimalValue::Missing => {
                    self.dollars_cents.append_null();
                    self.dollars_digits.append_null();
                    self.dollars_scale.append_null();
                }
                DecimalValue::Unparseable => {
                    self.unparseable_dollars += 1;
                    self.dollars_cents.append_null();
                    self.dollars_digits.append_null();
                    self.dollars_scale.append_null();
                }
                DecimalValue::Value { digits, scale, .. } => {
                    self.dollars_digits.append_value(digits);
                    self.dollars_scale.append_value(scale);
                    // Sub-cent values round to integer cents; the
                    // round-trip is lossless because the digits+scale
                    // pair carries the exact value.
                    let cents = DecimalValue::Value {
                        digits,
                        scale,
                        kind: delimited::DecimalKind::Exact,
                    }
                    .cents_rounded();
                    match cents {
                        Some(c) => self.dollars_cents.append_value(c),
                        None => self.dollars_cents.append_null(),
                    }
                }
            },
            None => {
                self.dollars_cents.append_null();
                self.dollars_digits.append_null();
                self.dollars_scale.append_null();
            }
        }
        self.rows += 1;
    }

    /// Rows whose UNITS is genuinely fractional and so is *not* in the
    /// `units` integer column. Surfaced so the loss is visible.
    pub fn fractional_units(&self) -> u64 {
        self.fractional_units
    }

    /// Rows whose DOLLARS could not be read at all.
    pub fn unparseable_dollars(&self) -> u64 {
        self.unparseable_dollars
    }

    /// Append one row given as a raw line.
    pub fn push_line(&mut self, dialect: Dialect, line: &[u8]) {
        let fields = dialect.split(line);
        if fields.len() < dialect.column_names().len() {
            return;
        }
        self.push_row(dialect, &fields);
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn finish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(self.panid.finish()),
                Arc::new(self.week.finish()),
                Arc::new(self.minute.finish()),
                Arc::new(self.units.finish()),
                Arc::new(self.units_digits.finish()),
                Arc::new(self.units_scale.finish()),
                Arc::new(self.outlet.finish()),
                Arc::new(self.dollars_cents.finish()),
                Arc::new(self.dollars_digits.finish()),
                Arc::new(self.dollars_scale.finish()),
                Arc::new(self.iri_key.finish()),
                Arc::new(self.colupc.finish()),
            ],
        )
        .expect("PANEL schema and build()'s column list are declared together")
    }
}

fn push_u32(b: &mut UInt32Builder, field: Option<&[u8]>) {
    match field.and_then(delimited::parse_int) {
        Some(v) if (0..=u32::MAX as i64).contains(&v) => b.append_value(v as u32),
        _ => b.append_null(),
    }
}

fn push_u16(b: &mut UInt16Builder, field: Option<&[u8]>) {
    push_u16_at(b, field)
}

fn push_u16_at(b: &mut UInt16Builder, field: Option<&[u8]>) {
    match field.and_then(delimited::parse_int) {
        Some(v) if (0..=u16::MAX as i64).contains(&v) => b.append_value(v as u16),
        _ => b.append_null(),
    }
}

fn text(field: Option<&[u8]>) -> Option<String> {
    let t = delimited::trim_ascii_space(field?);
    if t.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(t).into_owned())
    }
}

/// The PANEL parser for one source file.
#[derive(Debug)]
pub struct PanelParser {
    /// `None` for a genuinely zero-byte source.
    dialect: Option<Dialect>,
    /// Everything after the header line, so the parse pass does not
    /// re-split the header.
    body: Vec<u8>,
    rows: u64,
    /// Carried across batch boundaries: the counters live on
    /// `PanelBuilders`, which is replaced every `BATCH_ROWS` rows.
    fractional_units: u64,
    unparseable_dollars: u64,
}

impl PanelParser {
    pub fn open(path: &Path, _source: &SourceRef) -> Result<Self> {
        let bytes = delimited::read_file(path)?;
        let (dialect, body) = match delimited::first_line(&bytes) {
            // `first_line` found the header: the body is everything
            // after that line's terminator.
            Some(header) => {
                let consumed = bytes
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|i| i + 1)
                    .unwrap_or(bytes.len());
                (
                    dialect_from_header(header),
                    bytes[consumed.min(bytes.len())..].to_vec(),
                )
            }
            None => (None, Vec::new()),
        };
        Ok(Self {
            dialect,
            body,
            rows: 0,
            fractional_units: 0,
            unparseable_dollars: 0,
        })
    }
}

impl DatasetParser for PanelParser {
    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        let Some(dialect) = self.dialect else {
            // Zero-byte source: no header, no rows, not an error.
            return Ok(());
        };
        let mut b = PanelBuilders::new();
        let mut short_rows = 0u64;
        for line in delimited::lines(&self.body) {
            if line.is_empty() {
                continue;
            }
            let fields = dialect.split(line);
            if fields.len() < dialect.column_names().len() {
                short_rows += 1;
                continue;
            }
            b.push_row(dialect, &fields);
            if b.len() >= BATCH_ROWS {
                self.rows += b.rows();
                self.fractional_units += b.fractional_units();
                self.unparseable_dollars += b.unparseable_dollars();
                let batch = b.finish();
                b = PanelBuilders::new();
                sink.push(batch, cfg)?;
            }
        }
        if !b.is_empty() {
            self.rows += b.rows();
            self.fractional_units += b.fractional_units();
            self.unparseable_dollars += b.unparseable_dollars();
            let batch = b.finish();
            sink.push(batch, cfg)?;
        }
        let fractional = self.fractional_units;
        let unparseable = self.unparseable_dollars;
        if short_rows > 0 {
            // Worth a warning: the corpus has none, so any non-zero
            // count means a dialect we mis-sniffed.
            tracing::warn!(
                dialect = dialect.label(),
                short_rows,
                "PANEL rows had fewer fields than the header declares"
            );
        }
        if fractional > 0 {
            // Measured on the corpus: `carbbev`'s PANEL files hold
            // some genuinely fractional unit counts. They are left out
            // of the `units` column rather than rounded, so this
            // warning is the record of what was left out.
            tracing::warn!(
                dataset = "iri_panel",
                fractional_units = fractional,
                "PANEL rows with a fractional UNITS were not written to the \
                 integer `units` column"
            );
        }
        if unparseable > 0 {
            tracing::warn!(
                dataset = "iri_panel",
                unparseable_dollars = unparseable,
                "PANEL rows whose DOLLARS matched no known encoding"
            );
        }
        Ok(())
    }

    fn expected_rows(&self) -> Option<u64> {
        Some(self.rows)
    }

    fn note(&self) -> Option<String> {
        Some(match self.dialect {
            Some(d) => format!(
                "dialect={} minute_column={}",
                d.label(),
                if d.has_minute { "present" } else { "absent" }
            ),
            None => "dialect=none source_is_zero_bytes".to_string(),
        })
    }
}

/// Parser entry point handed to the generic driver.
pub fn parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(PanelParser::open(path, source)?))
}

/// Tuning for the PANEL class.
pub fn tuning() -> DatasetTuning {
    DatasetTuning::new(BATCH_ROWS, MAX_ROWS_PER_FILE, parser as ParserFactory)
}

/// The outlet codes PANEL files use.
///
/// Year 8 adds `KK` (drugstore "combo"); years 8–12 rename `DR`→`DK`,
/// `GR`→`GK`, `MA`→`MK`. Taking the union keeps the matcher simple and
/// costs nothing — a code that never occurs simply never matches.
const OUTLET_CODES: &[&str] = &["DR", "GR", "MA", "DK", "GK", "MK", "KK"];

/// Discover the PANEL sources under `input_root`.
///
/// Matches `<category>_PANEL_<OUTLET>_<week_start>_<week_end>.{dat,DAT}`
/// anywhere under a `Year<N>` directory.
pub fn discover(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::Panel);
    for entry in walkdir::WalkDir::new(input_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !is_ignored(e.path(), input_root))
    {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            inv.skipped.push((path.to_path_buf(), "non_utf8_filename"));
            continue;
        };
        if let Some(reason) = noise_reason(name) {
            inv.skipped.push((path.to_path_buf(), reason));
            continue;
        }
        let Some(source) = parse_panel_path(path, input_root) else {
            inv.skipped
                .push((path.to_path_buf(), "not_a_panel_filename"));
            continue;
        };
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        inv.files.push(DatasetFile {
            kind: DatasetKind::Panel,
            source,
            size_bytes: size,
        });
    }
    Ok(inv)
}

/// Reject the corpus's known stragglers, by the same rules the sales
/// walker uses so the two can never disagree about what is noise.
fn noise_reason(name: &str) -> Option<&'static str> {
    if name.starts_with("~$") {
        return Some("lock_file");
    }
    let upper = name.to_ascii_uppercase();
    if upper.ends_with(".OLD") || upper.ends_with(".BAK") || upper.ends_with(".TMP") {
        return Some("backup_extension");
    }
    None
}

fn parse_panel_path(path: &Path, input_root: &Path) -> Option<SourceRef> {
    let name = path.file_name().and_then(|n| n.to_str())?;
    // Strip the extension: `.dat` for years 1–2, `.DAT` for 3+.
    let stem = name.rsplit_once('.').map(|(s, _)| s).unwrap_or(name);
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() < 5 {
        return None;
    }
    // `<category...>_PANEL_<OUTLET>_<w1>_<w2>`
    let n = parts.len();
    if parts[n - 4] != "PANEL" {
        return None;
    }
    let outlet = parts[n - 3];
    if !OUTLET_CODES.contains(&outlet) {
        return None;
    }
    let ws: u16 = parts[n - 2].parse().ok()?;
    let we: u16 = parts[n - 1].parse().ok()?;
    if ws == 0 || we < ws {
        return None;
    }
    let year = infer_year(path, input_root)?;
    let category = infer_category(path, input_root).unwrap_or_default();
    Some(
        SourceRef::new(path)
            .with_year(year)
            .with_category(category)
            .with_outlet(outlet)
            .with_weeks(ws, we),
    )
}

/// Directories beneath the input root that hold no PANEL files.
///
/// Only the stub editions and the external directory matter; every year
/// tree is descended.
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
    use arrow_array::{Array, Int32Array, Int64Array, StringArray, UInt16Array, UInt32Array};

    fn batch_of(line: &[u8], dialect: Dialect) -> RecordBatch {
        let mut b = PanelBuilders::new();
        b.push_line(dialect, line);
        b.finish()
    }

    /// The three dialects the corpus actually contains, spelled out so
    /// the tests read as the observed header lines they are.
    const DIALECT_TAB: Dialect = Dialect {
        delim: Delim::Tab,
        has_minute: false,
    };
    const DIALECT_WS: Dialect = Dialect {
        delim: Delim::Whitespace,
        has_minute: false,
    };
    const DIALECT_CSV: Dialect = Dialect {
        delim: Delim::Comma,
        has_minute: true,
    };
    /// The one-file outlier: year 8+ layout, year 1-3 delimiter.
    const DIALECT_TAB_MINUTE: Dialect = Dialect {
        delim: Delim::Tab,
        has_minute: true,
    };

    #[test]
    fn sniffs_all_four_observed_dialects() {
        assert_eq!(
            dialect_from_header(b"PANID\tWEEK\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC"),
            Some(DIALECT_TAB)
        );
        assert_eq!(
            dialect_from_header(b"PANID WEEK UNITS OUTLET DOLLARS IRI_KEY COLUPC"),
            Some(DIALECT_WS)
        );
        assert_eq!(
            dialect_from_header(b"PANID,WEEK,MINUTE,UNITS,OUTLET,DOLLARS,IRI_KEY,COLUPC"),
            Some(DIALECT_CSV)
        );
        // The one-file outlier: `Year11/diapers/
        // diapers_PANEL_GK_1635_1686.DAT` is tab-delimited *with* the
        // year-8+ MINUTE column. Both facts come from the bytes, which
        // is the only way to get this file right.
        assert_eq!(
            dialect_from_header(b"PANID\tWEEK\tMINUTE\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC"),
            Some(DIALECT_TAB_MINUTE)
        );
    }

    #[test]
    fn the_delimiter_and_the_minute_column_are_independent() {
        // A `Dialect` that bundled them would need a fifth variant, or
        // would infer the delimiter from MINUTE and mis-split this
        // file's rows silently: a comma split of a tab row yields one
        // field, which is not an error, just a row with no columns.
        assert_eq!(DIALECT_TAB.delim, Delim::Tab);
        assert_eq!(DIALECT_TAB_MINUTE.delim, Delim::Tab);
        // Same delimiter, different column set: this is the whole point.
        assert_ne!(
            DIALECT_TAB.column_names(),
            DIALECT_TAB_MINUTE.column_names()
        );
        assert_eq!(DIALECT_TAB_MINUTE.column_names().len(), 8);
        assert_eq!(DIALECT_TAB.column_names().len(), 7);
    }

    #[test]
    fn a_zero_byte_source_has_no_dialect_and_is_not_an_error() {
        // The corpus really contains these: `Year1/beer`'s PANEL_DR
        // and one Year-2 equivalent are zero bytes.
        assert_eq!(delimited::first_line(b""), None);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("beer_PANEL_DR_1114_1165.dat");
        std::fs::write(&p, b"").unwrap();
        let parser = PanelParser::open(&p, &SourceRef::new(&p)).unwrap();
        assert_eq!(parser.dialect, None);
        assert_eq!(parser.expected_rows(), Some(0));
    }

    #[test]
    fn parses_a_real_year1_tab_row() {
        // First data row of Year1/beer/beer_PANEL_GR_1114_1165.dat.
        let batch = batch_of(
            b"3315465\t1134\t1\tGR\t1.69\t228037\t10943900008",
            DIALECT_TAB,
        );
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            3_315_465
        );
        // No MINUTE in this dialect: null, not zero.
        assert!(batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .is_null(0));
        // The integer cents column reflects the rounded value.
        assert_eq!(
            batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            169,
            "1.69 dollars -> 169 cents, through the same parser iri_sales uses"
        );
        // The digits+scale pair carries the exact bytes: digits=169,
        // scale=2.
        let digits = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(digits.value(0), 169);
        assert_eq!(
            batch
                .column(9)
                .as_any()
                .downcast_ref::<arrow_array::Int8Array>()
                .unwrap()
                .value(0),
            2
        );
        // Units has the same shape: digits=1, scale=0.
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "GR"
        );
    }

    #[test]
    fn parses_a_real_year9_comma_row_with_minute() {
        // First data row of Year9/beer/beer_PANEL_GK_1531_1582.DAT,
        // including the trailing spaces the corpus pads with.
        let batch = batch_of(
            b"3822619 ,1536 ,9334 ,1 ,GK ,6.49 ,257871 ,0011497450009 ",
            DIALECT_CSV,
        );
        let minute = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert!(!minute.is_null(0));
        assert_eq!(minute.value(0), 9334);
        // Padded fields must trim: `3822619 ` -> 3822619.
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            3_822_619
        );
        assert_eq!(
            batch
                .column(11)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "0011497450009",
            "COLUPC keeps its leading zeros, which is why it is a string"
        );
    }

    #[test]
    fn parses_a_real_year5_whitespace_row() {
        // First data row of Year5/factiss/factiss_PANEL_MA_1322_1373.DAT.
        let batch = batch_of(b"3105502 1344 1 MA 0.98 690004 8839999850289", DIALECT_WS);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            3_105_502
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(0),
            1344
        );
        assert_eq!(
            batch
                .column(10)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            690_004
        );
    }

    #[test]
    fn all_four_dialects_land_on_the_same_eight_columns() {
        // The point of the shared schema: a query spans twelve years
        // without branching on which file a row came from.
        let rows = [
            (
                DIALECT_TAB,
                &b"3315465\t1134\t1\tGR\t1.69\t228037\t10943900008"[..],
            ),
            (
                DIALECT_WS,
                &b"3105502 1344 1 MA 0.98 690004 8839999850289"[..],
            ),
            (
                DIALECT_CSV,
                &b"3822619 ,1536 ,9334 ,1 ,GK ,6.49 ,257871 ,0011497450009 "[..],
            ),
            (
                DIALECT_TAB,
                &b"3822619\t1536\t9334\t1\tGK\t6.49\t257871\t0011497450009"[..],
            ),
        ];
        for (d, line) in rows {
            let batch = batch_of(line, d);
            assert_eq!(
                batch.schema(),
                schema(),
                "{:?} produced a different schema",
                d
            );
            assert_eq!(batch.num_rows(), 1);
        }
    }

    #[test]
    fn a_missing_optional_field_is_null_not_zero() {
        // A blank DOLLARS must not become 0 cents, which would be a
        // real zero-dollar trip in a fact table. The tab dialect can
        // express an empty DOLLARS; the
        // whitespace one cannot (a blank field is indistinguishable
        // from two spaces, which collapse).
        let batch = batch_of(b"1\t1114\t1\tGR\t\t228037\t10943900008", DIALECT_TAB);
        // A blank DOLLARS nulls all three dollars columns, never 1.
        assert!(batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_null(0));
        assert!(batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap()
            .is_null(0));
        assert!(batch
            .column(9)
            .as_any()
            .downcast_ref::<arrow_array::Int8Array>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn the_float32_encoded_rows_of_the_real_corpus_are_recovered() {
        // Verbatim from `Year1/beer/beer_PANEL_GR_1114_1165.dat`. IRI
        // stored these two fields through a 32-bit float and the writer
        // printed all 17 significant digits, so `DOLLARS` reads
        // `0.7299998474` (= float32(0.73)) and `UNITS` reads
        // `0.049999997` (= float32(0.05)).
        //
        // The strict integer-cent parser rejects both, which would null
        // out 74 000 real dollar amounts across the corpus — 0.45 % of
        // all PANEL rows, concentrated in `carbbev`'s panel files.
        let mut b = PanelBuilders::new();
        b.push_line(
            DIALECT_TAB,
            b"3347963\t1164\t0.049999997\tGR\t0.7299998474\t264075\t11820043550",
        );
        let fractional = b.fractional_units();
        let unparseable = b.unparseable_dollars();
        let batch = b.finish();
        assert_eq!(
            batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            73,
            "float32(0.73) must round back to 73 cents, not become NULL"
        );
        // The digits+scale pair recovers the exact value the source
        // wrote: `0.7299998474` -> digits=7299998474, scale=10.
        let dollars_digits = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(dollars_digits.value(0), 7_299_998_474);
        assert_eq!(
            batch
                .column(9)
                .as_any()
                .downcast_ref::<arrow_array::Int8Array>()
                .unwrap()
                .value(0),
            10
        );
        // UNITS is a genuinely fractional count (0.05): the integer
        // column is null, but the digits+scale pair carries it.
        assert!(batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .is_null(0));
        let units_digits = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(units_digits.value(0), 49_999_997);
        // 9 trailing digits is what 32-bit float can represent after the
        // decimal for values of this magnitude.
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<arrow_array::Int8Array>()
                .unwrap()
                .value(0),
            9
        );
        assert_eq!(fractional, 1);
        assert_eq!(unparseable, 0);
        assert_eq!(batch.num_rows(), 1, "the row is kept, not dropped");
    }

    #[test]
    fn an_unparseable_dollar_value_is_counted_not_silently_nulled() {
        // A DOLLARS that matches neither encoding must be visible, not
        // just a null column: the row is kept and the count is logged.
        let mut b = PanelBuilders::new();
        b.push_line(DIALECT_TAB, b"1\t1114\t1\tGR\tN/A\t228037\t10943900008");
        let unparseable = b.unparseable_dollars();
        let fractional = b.fractional_units();
        let batch = b.finish();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_null(0));
        // The digits+scale pair is null for an unparseable value.
        assert!(batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap()
            .is_null(0));
        assert_eq!(unparseable, 1);
        assert_eq!(fractional, 0);
    }

    #[test]
    fn matches_real_panel_filenames_and_rejects_stragglers() {
        let root = Path::new("/raw");

        let s = parse_panel_path(
            Path::new("/raw/Year9/beer/beer_PANEL_GK_1531_1582.DAT"),
            root,
        )
        .unwrap();
        assert_eq!(s.year, Some(9));
        assert_eq!(s.category.as_deref(), Some("beer"));
        assert_eq!(s.outlet.as_deref(), Some("GK"));
        assert_eq!(s.week_start, Some(1531));
        assert_eq!(s.week_end, Some(1582));
        assert_eq!(
            s.partition_dir_for(DatasetKind::Panel),
            Path::new("year=9/category=beer/outlet=GK")
        );

        // The Year-12 extra nesting level.
        let s = parse_panel_path(
            Path::new("/raw/Year12/toothpa/toothpa/toothpa_PANEL_DK_1687_1739.DAT"),
            root,
        )
        .unwrap();
        assert_eq!(s.year, Some(12));
        assert_eq!(s.category.as_deref(), Some("toothpa"));

        // Year 8's extra KK outlet.
        assert_eq!(
            parse_panel_path(
                Path::new("/raw/Year8/beer/beer_PANEL_KK_1479_1530.DAT"),
                root
            )
            .unwrap()
            .outlet
            .as_deref(),
            Some("KK")
        );

        // Stragglers are noise, not sources.
        assert_eq!(
            noise_reason("coldcer_PANEL_GR_1218_1269.DAT.bak"),
            Some("backup_extension")
        );
        assert_eq!(
            noise_reason("margbutr_PANEL_GR_1166_1217.OLD"),
            Some("backup_extension")
        );
        assert_eq!(noise_reason("~$prod_factiss.xlsx"), Some("lock_file"));

        // A sales file is not a panel file.
        assert!(parse_panel_path(Path::new("/raw/Year1/beer/beer_drug_1114_1165"), root).is_none());
        // An unknown outlet code is not a panel file either.
        assert!(parse_panel_path(
            Path::new("/raw/Year1/beer/beer_PANEL_ZZ_1114_1165.dat"),
            root
        )
        .is_none());
    }

    #[test]
    fn schema_carries_panell_and_money_digits_and_scale_columns() {
        let s = schema();
        assert_eq!(s.fields().len(), 12);
        assert_eq!(s.field(2).name(), "minute");
        assert!(s.field(2).is_nullable());
        assert_eq!(s.field(7).name(), "dollars_cents");
        assert_eq!(s.field(7).data_type(), &DataType::Int64);
    }

    #[test]
    fn a_batch_keeps_every_builder_in_step() {
        let mut b = PanelBuilders::new();
        for i in 0..37 {
            b.push_line(
                DIALECT_TAB,
                format!("{}\t1114\t1\tGR\t1.69\t228037\t1094390000{}", i, i).as_bytes(),
            );
        }
        assert_eq!(b.len(), 37);
        assert_eq!(b.rows(), 37);
        let batch = b.finish();
        assert_eq!(batch.num_rows(), 37);
        // Every column must have the same length or Arrow would have
        // rejected the batch; assert it explicitly anyway, because a
        // future column added to one builder only is exactly the bug
        // this catches.
        for (i, c) in batch.columns().iter().enumerate() {
            assert_eq!(c.len(), 37, "column {i} out of step");
        }
    }
}
