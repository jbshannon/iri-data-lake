//! Excel-backed datasets: the week→calendar dimension, the product
//! stubs, and the FIPS→IRI-market cross-reference.
//!
//! All three are `.xls` (BIFF8) or `.xlsx` (OOXML) workbooks, so they
//! need one crate — [`calamine`] — to read either. That is the reason
//! for the dependency and the reason it is scoped to this module: the
//! sales pipeline and the seven text-format classes do not touch it,
//! and a caller who removes the stubs never pulls Excel support out of
//! their build for the other eight classes.
//!
//! # Sheet selection is by content, not position
//!
//! `fips by IRI market.xls` puts a documentation sheet first and the
//! data second. `parsed stub files 2007/prod_beer.xlsx` has three
//! sheets, only the first of which is the stub. So nothing here reads
//! `sheets[0]` on faith: each class declares how it recognises its
//! sheet, and a workbook that matches nothing is an error naming the
//! sheets it did find.

use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{
    Date32Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt8Builder,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use calamine::{ExcelDateTimeType as XlDateType, Reader, Sheets};
use std::fs::File;

use super::delimited;
use super::ingest::{BatchSink, DatasetParser, DatasetTuning, ParserFactory};
use crate::config::IngestConfig;
use crate::dataset::{DatasetFile, DatasetInventory, SourceRef};
use crate::errors::{IngestError, Result};
use crate::model::{DatasetKind, WEEK_DIMENSION_SCHEMA_VERSION};

pub const WEEK_BATCH_ROWS: usize = 10_000;
pub const STUB_BATCH_ROWS: usize = 100_000;

// ---------------------------------------------------------------------
// iri_week_dimension
// ---------------------------------------------------------------------

/// The week dimension's one schema.
pub fn week_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("iri_week", DataType::UInt16, false),
        Field::new("week_start", DataType::Date32, true),
        Field::new("week_end", DataType::Date32, true),
        Field::new("source_year", DataType::UInt8, true),
    ]))
}

pub fn week_schema_version() -> u32 {
    WEEK_DIMENSION_SCHEMA_VERSION
}

/// Convert an Excel serial date to days since the Unix epoch.
///
/// This defers to calamine's `as_datetime`, which applies Excel's two
/// historical corrections — the phantom 1900-02-29 day and the 1462-day
/// gap between the 1900 and 1904 date systems — before doing anything
/// else. Reimplementing the serial arithmetic here would mean
/// reimplementing those corrections, and getting them wrong produces
/// dates that are off by one with no visible symptom.
///
/// The time-of-day component is truncated rather than rounded: a week
/// *starts* on a date, and rounding `2001-01-01T18:00` up to the 2nd
/// would move the whole week a day.
fn excel_serial_to_unix_days(serial: f64) -> Option<i32> {
    if !serial.is_finite() || serial < 1.0 {
        // Below 1 is Excel's phantom 1900-01-00/1900-02-29 region.
        return None;
    }
    let dt = calamine::ExcelDateTime::new(serial, XlDateType::DateTime, false).as_datetime()?;
    Some(
        dt.date()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp() as i32
            / 86_400,
    )
}

fn cell_date(cell: &calamine::Data) -> Option<i32> {
    match cell {
        calamine::Data::DateTime(dt) => dt
            .as_datetime()
            .map(|d| d.date().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as i32 / 86_400),
        calamine::Data::Float(v) => excel_serial_to_unix_days(*v),
        calamine::Data::Int(v) => excel_serial_to_unix_days(*v as f64),
        calamine::Data::String(s) => parse_iso_date(s),
        _ => None,
    }
}

fn parse_iso_date(s: &str) -> Option<i32> {
    let d = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    Some(d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as i32 / 86_400)
}

/// The week dimension parser.
///
/// Reads a workbook's `Sheet1`, whose header is
/// `IRI Week | Calendar week starting on | Calendar week ending on |
/// [Year] | Calendar date | IRI Week`. The trailing pair is an
/// unrelated week-1-based table that shares the sheet; only the first
/// three columns are this dimension.
#[derive(Debug)]
pub struct WeekParser {
    rows: Vec<WeekRow>,
    source_label: String,
}

/// One row of the week dimension: `(iri_week, start, end, year)`.
type WeekRow = (u16, Option<i32>, Option<i32>, Option<u8>);

impl WeekParser {
    pub fn open(path: &Path, _source: &SourceRef) -> Result<Self> {
        let mut wb = calamine::open_workbook_auto(path)
            .map_err(|e| IngestError::Discovery(format!("open {}: {e}", path.display())))?;
        let Some(sheet) = pick_week_sheet(&mut wb, path) else {
            return Err(IngestError::Discovery(format!(
                "{}: no sheet has an 'IRI Week' column",
                path.display()
            )));
        };
        let range = wb.worksheet_range(&sheet).map_err(|e| {
            IngestError::Discovery(format!("read {sheet:?} in {}: {e}", path.display()))
        })?;

        let mut rows = Vec::new();
        for (i, r) in range.rows().enumerate() {
            if i == 0 {
                continue; // header
            }
            let week = match r.first() {
                Some(calamine::Data::Float(v)) => *v as i64,
                Some(calamine::Data::Int(v)) => *v,
                _ => continue,
            };
            if !(1..=10_000).contains(&week) {
                continue;
            }
            // Column 4 is `Year` in the authoritative file and empty in
            // the per-year copies. Read it by position because its
            // *header* is what varies, and both layouts are known.
            let year = r.get(3).and_then(|c| match c {
                calamine::Data::Float(v) => Some(*v as u8),
                calamine::Data::Int(v) => Some(*v as u8),
                _ => None,
            });
            rows.push((
                week as u16,
                r.get(1).and_then(cell_date),
                r.get(2).and_then(cell_date),
                year.filter(|y| (1..=12).contains(y)),
            ));
        }
        rows.sort_by_key(|r| r.0);
        rows.dedup_by_key(|r| r.0);
        Ok(Self {
            rows,
            source_label: path.display().to_string(),
        })
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Recognise the week sheet by its header rather than its position.
///
/// Three of the corpus's workbook sets differ here: the authoritative
/// file's header carries a `Year` column the per-year copies lack, and
/// `fips by IRI market.xls` leads with a documentation sheet.
fn pick_week_sheet(wb: &mut Sheets<std::io::BufReader<File>>, path: &Path) -> Option<String> {
    for name in wb.sheet_names().to_vec() {
        let Ok(range) = wb.worksheet_range(&name) else {
            continue;
        };
        let Some(first) = range.rows().next() else {
            continue;
        };
        let joined: String = first
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        if joined.contains("iri week") {
            return Some(name);
        }
    }
    let _ = path;
    None
}

impl DatasetParser for WeekParser {
    fn schema(&self) -> SchemaRef {
        week_schema()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let mut week = UInt16Builder::with_capacity(self.rows.len());
        let mut start = Date32Builder::with_capacity(self.rows.len());
        let mut end = Date32Builder::with_capacity(self.rows.len());
        // The schema declares `source_year` as UInt8 (1..=12); the
        // builder must match or Arrow rejects the batch.
        let mut year = UInt8Builder::with_capacity(self.rows.len());
        let mut year_buf: Vec<Option<u8>> = Vec::with_capacity(self.rows.len());
        for (w, s, e, y) in self.rows.drain(..) {
            week.append_value(w);
            match s {
                Some(v) => start.append_value(v),
                None => start.append_null(),
            }
            match e {
                Some(v) => end.append_value(v),
                None => end.append_null(),
            }
            year_buf.push(y);
        }
        for y in year_buf {
            match y {
                Some(v) => year.append_value(v),
                None => year.append_null(),
            }
        }
        let batch = RecordBatch::try_new(
            week_schema(),
            vec![
                Arc::new(week.finish()),
                Arc::new(start.finish()),
                Arc::new(end.finish()),
                Arc::new(year.finish()),
            ],
        )?;
        sink.push(batch, cfg)?;
        Ok(())
    }

    fn expected_rows(&self) -> Option<u64> {
        Some(self.len() as u64)
    }

    fn note(&self) -> Option<String> {
        Some(format!("workbook={}", self.source_label))
    }
}

pub fn week_parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(WeekParser::open(path, source)?))
}

pub fn week_tuning() -> DatasetTuning {
    DatasetTuning::new(
        WEEK_BATCH_ROWS,
        WEEK_BATCH_ROWS,
        week_parser as ParserFactory,
    )
}

/// Discover the week-dimension workbooks.
///
/// **The authoritative file only**: `demos trips external/IRI week
/// translation.xls`, which covers weeks 1114–1739 with a `Year` column.
/// The 311 per-year copies are *subsets* of it — the Year-1 copy starts
/// at week 1138, not 1114 — so ingesting all 312 would write three
/// partial copies of one table and 312× the rows for a dimension of 627
/// rows. It is also the only copy that covers the year 6/7 gap the
/// survey flags, and the only one carrying `Year`.
///
/// The per-year copies are still *discovered and reported* as skipped,
/// with the reason, so the decision is visible rather than implicit.
pub fn discover_week_dimension(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::WeekDimension);
    let external = input_root.join("demos trips external");
    let authoritative = external.join("IRI week translation.xls");
    if authoritative.is_file() {
        let size = std::fs::metadata(&authoritative)
            .map(|m| m.len())
            .unwrap_or(0);
        inv.files.push(DatasetFile {
            kind: DatasetKind::WeekDimension,
            source: SourceRef::new(&authoritative).with_edition("full-1114-1739"),
            size_bytes: size,
        });
    }
    // Report the per-year copies we are deliberately not ingesting.
    for entry in walkdir::WalkDir::new(&external).max_depth(1) {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.to_ascii_lowercase().contains("week translation")
            && entry.path() != authoritative.as_path()
        {
            inv.skipped
                .push((entry.path().to_path_buf(), "subset_of_authoritative"));
        }
    }
    for entry in walkdir::WalkDir::new(input_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !is_ignored(e.path(), input_root))
    {
        let Ok(entry) = entry else { continue };
        if entry.file_type().is_file()
            && entry
                .file_name()
                .to_string_lossy()
                .to_ascii_lowercase()
                .contains("week translation")
            && entry.path() != authoritative.as_path()
        {
            inv.skipped
                .push((entry.path().to_path_buf(), "subset_of_authoritative"));
        }
    }
    Ok(inv)
}

// ---------------------------------------------------------------------
// iri_product_stub
// ---------------------------------------------------------------------

/// The stub columns that are typed. Everything else is a string.
///
/// `UPC` is a string because `08-01-94922-00101` is not a number, and
/// `VOL_EQ` is the only genuinely fractional column.
fn stub_typed_columns() -> &'static [&'static str] {
    &["Level", "SY", "GE", "VEND", "ITEM"]
}

/// Build the schema for a stub workbook's declared columns.
///
/// The first 12 columns are stable across all four editions
/// (`L1 L2 L3 L4 L5 L9 Level UPC SY GE VEND ITEM`); column 13 is the
/// stub spec and is named by its `*` prefix, which differs per edition
/// (`*STUBSPEC 1416IS…` vs `*AG C=1+ CATEGORY`); after that the
/// attribute columns are per-category and drift.
pub fn stub_schema(columns: &[String]) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    for name in columns {
        let dt = match stub_typed_columns().contains(&name.as_str()) {
            true => match name.as_str() {
                "Level" => DataType::UInt16,
                "SY" | "GE" => DataType::UInt8,
                _ => DataType::UInt32,
            },
            false => match name.as_str() {
                "VOL_EQ" => DataType::Float64,
                _ => DataType::Utf8,
            },
        };
        fields.push(Field::new(name, dt, true));
    }
    Arc::new(Schema::new(fields))
}

/// The stub parser for one workbook.
#[derive(Debug)]
pub struct StubParser {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    source_label: String,
}

impl StubParser {
    pub fn open(path: &Path, source: &SourceRef) -> Result<Self> {
        let mut wb = calamine::open_workbook_auto(path)
            .map_err(|e| IngestError::Discovery(format!("open {}: {e}", path.display())))?;
        let sheet = pick_stub_sheet(&mut wb).ok_or_else(|| {
            IngestError::Discovery(format!(
                "{}: no sheet looks like a product stub; sheets are {:?}",
                path.display(),
                wb.sheet_names()
            ))
        })?;
        let range = wb.worksheet_range(&sheet).map_err(|e| {
            IngestError::Discovery(format!("read {sheet:?} in {}: {e}", path.display()))
        })?;

        let mut rows = range.rows();
        let Some(header_row) = rows.next() else {
            return Err(IngestError::Discovery(format!(
                "{}: stub sheet {sheet:?} is empty",
                path.display()
            )));
        };
        let raw: Vec<String> = header_row.iter().map(|c| c.to_string()).collect();
        let columns = normalise_stub_columns(&raw);

        // One workbook is at most ~18 000 rows, so the whole thing
        // becomes one batch. Building builders row-by-row here rather
        // than through a generic sink keeps the per-column type
        // dispatch here, where the column list is already in hand.
        let mut builders: Vec<Builder> = columns.iter().map(|c| Builder::for_column(c)).collect();
        let mut row_count = 0u64;
        for r in rows {
            for (i, b) in builders.iter_mut().enumerate() {
                let cell = r.get(i).unwrap_or(&calamine::Data::Empty);
                b.push(cell);
            }
            // Rows shorter than the header leave trailing columns null
            // rather than being dropped.
            for b in builders.iter_mut().skip(r.len()) {
                b.push(&calamine::Data::Empty);
            }
            row_count += 1;
        }
        let arrays: Vec<Arc<dyn Array>> = builders.into_iter().map(|b| b.finish()).collect();
        let batch = RecordBatch::try_new(stub_schema(&columns), arrays)?;
        Ok(Self {
            schema: stub_schema(&columns),
            batches: if row_count == 0 {
                Vec::new()
            } else {
                vec![batch]
            },
            source_label: source.path.display().to_string(),
        })
    }

    pub fn rows(&self) -> u64 {
        self.batches.iter().map(|b| b.num_rows() as u64).sum()
    }
}

/// Recognise the stub sheet by looking for the `L1`/`UPC` columns.
///
/// `parsed stub files 2007/prod_beer.xlsx` has three sheets and only the
/// first is the stub, so position is not a safe test — but `Sheet1` is
/// where the stub lives in every edition, so it is tried first and the
/// content check is the fallback.
fn pick_stub_sheet(wb: &mut Sheets<std::io::BufReader<File>>) -> Option<String> {
    let names = wb.sheet_names().to_vec();
    if let Some(first) = names.first() {
        if sheet_looks_like_stub(wb, first) {
            return Some(first.clone());
        }
    }
    names.into_iter().find(|n| sheet_looks_like_stub(wb, n))
}

fn sheet_looks_like_stub(wb: &mut Sheets<std::io::BufReader<File>>, name: &str) -> bool {
    let Ok(range) = wb.worksheet_range(name) else {
        return false;
    };
    let Some(first) = range.rows().next() else {
        return false;
    };
    let names: Vec<String> = first.iter().map(|c| c.to_string()).collect();
    names.iter().any(|n| n == "UPC") && names.iter().any(|n| n == "L1")
}

/// Normalise a stub workbook's header into valid, unique column names.
///
/// Column 13 is the stub spec, whose name differs per edition
/// (`*STUBSPEC 1416IS …` vs `*AG C=1+ CATEGORY`) and is renamed to
/// `stubspec` so all four editions share one column name. Everything
/// else is normalised the same way the attribute columns are.
pub fn normalise_stub_columns(raw: &[String]) -> Vec<String> {
    let mut taken: Vec<String> = Vec::new();
    raw.iter()
        .enumerate()
        .map(|(i, name)| {
            if name.starts_with('*') {
                // The `*` prefix is the reliable marker for the stub
                // spec column across all four editions.
                return super::product_attr::normalise_attr_name(b"stubspec", i, &mut taken);
            }
            super::product_attr::normalise_attr_name(name.as_bytes(), i, &mut taken)
        })
        .collect()
}

/// A per-column value builder, chosen by column name.
///
/// Four variants cover every column in all four stub editions. A new
/// edition that adds a genuinely typed column adds a variant here and
/// in [`stub_schema`]; anything unrecognised lands in `Str`, which is
/// lossless.
enum Builder {
    U16(UInt16Builder),
    U8(UInt8Builder),
    U32(UInt32Builder),
    F64(arrow_array::builder::Float64Builder),
    Str(StringBuilder),
}

impl Builder {
    fn for_column(name: &str) -> Self {
        match name {
            "Level" => Builder::U16(UInt16Builder::new()),
            "SY" | "GE" => Builder::U8(UInt8Builder::new()),
            "VEND" | "ITEM" => Builder::U32(UInt32Builder::new()),
            "VOL_EQ" => Builder::F64(arrow_array::builder::Float64Builder::new()),
            _ => Builder::Str(StringBuilder::new()),
        }
    }

    fn push(&mut self, cell: &calamine::Data) {
        match self {
            Builder::U16(b) => match excel_int(cell) {
                Some(v) if (0..=u16::MAX as i64).contains(&v) => b.append_value(v as u16),
                _ => b.append_null(),
            },
            Builder::U8(b) => match excel_int(cell) {
                Some(v) if (0..=u8::MAX as i64).contains(&v) => b.append_value(v as u8),
                _ => b.append_null(),
            },
            Builder::U32(b) => match excel_int(cell) {
                Some(v) if (0..=u32::MAX as i64).contains(&v) => b.append_value(v as u32),
                _ => b.append_null(),
            },
            Builder::F64(b) => match excel_float(cell) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            // A numeric-looking cell in an unrecognised column becomes
            // its string form rather than being dropped. The corpus's
            // stub sheets have no such column today, but a future
            // edition might, and a string is the lossless landing
            // spot — silently discarding it would not be.
            Builder::Str(b) => match cell {
                calamine::Data::Empty => b.append_null(),
                calamine::Data::Float(v) => b.append_value(format!("{v}")),
                calamine::Data::Int(v) => b.append_value(v.to_string()),
                calamine::Data::String(s) if s.is_empty() => b.append_null(),
                other => b.append_value(other.to_string()),
            },
        }
    }

    fn finish(self) -> Arc<dyn Array> {
        match self {
            Builder::U16(mut b) => Arc::new(b.finish()),
            Builder::U8(mut b) => Arc::new(b.finish()),
            Builder::U32(mut b) => Arc::new(b.finish()),
            Builder::F64(mut b) => Arc::new(b.finish()),
            Builder::Str(mut b) => Arc::new(b.finish()),
        }
    }
}

fn excel_int(cell: &calamine::Data) -> Option<i64> {
    match cell {
        calamine::Data::Float(v) => {
            if v.is_finite() {
                Some(*v as i64)
            } else {
                None
            }
        }
        calamine::Data::Int(v) => Some(*v),
        calamine::Data::String(s) => delimited::parse_int(s.as_bytes()),
        _ => None,
    }
}

fn excel_float(cell: &calamine::Data) -> Option<f64> {
    match cell {
        calamine::Data::Float(v) => v.is_finite().then_some(*v),
        calamine::Data::Int(v) => Some(*v as f64),
        calamine::Data::String(s) => delimited::parse_f64(s.as_bytes()),
        _ => None,
    }
}

impl DatasetParser for StubParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        for b in self.batches.drain(..) {
            sink.push(b, cfg)?;
        }
        Ok(())
    }

    fn expected_rows(&self) -> Option<u64> {
        Some(self.rows())
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
            "workbook={} columns={}",
            self.source_label,
            self.schema.fields().len()
        ))
    }
}

pub fn stub_parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(StubParser::open(path, source)?))
}

pub fn stub_tuning() -> DatasetTuning {
    DatasetTuning::new(
        STUB_BATCH_ROWS,
        STUB_BATCH_ROWS,
        stub_parser as ParserFactory,
    )
}

/// The four stub edition directories and the filename prefix each uses.
///
/// ```text
/// parsed stub files/          years 1–6   prod_<code>.xls
/// parsed stub files 2007/     year 7      prod_<code>.xlsx
/// parsed stub files 2008-2011/ years 8–11 prod11_<code>.xlsx
/// parsed stub files 2012/     year 12     prod12_<code>.xlsx
/// ```
pub const STUB_EDITIONS: &[(&str, &str, &str)] = &[
    ("parsed stub files", "prod", "2001-2006"),
    ("parsed stub files 2007", "prod", "2007"),
    ("parsed stub files 2008-2011", "prod11", "2008-2011"),
    ("parsed stub files 2012", "prod12", "2012"),
];

/// Discover the product-stub workbooks.
///
/// The three `_sz` size variants (`prod_beer_sz.xls` etc.) are excluded:
/// they are alternate pack-size definitions for the same product, not
/// separate products, and the reference Julia pipeline excludes them
/// with the same rule.
pub fn discover_product_stub(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::ProductStub);
    for (dir, prefix, edition) in STUB_EDITIONS {
        let d = input_root.join(dir);
        if !d.is_dir() {
            continue;
        }
        for entry in walkdir::WalkDir::new(&d).max_depth(1) {
            let Ok(entry) = entry else { continue };
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("~$") {
                inv.skipped.push((entry.path().to_path_buf(), "lock_file"));
                continue;
            }
            let lower = name.to_ascii_lowercase();
            if !lower.ends_with(".xls") && !lower.ends_with(".xlsx") {
                continue;
            }
            if !name.starts_with(prefix) {
                inv.skipped
                    .push((entry.path().to_path_buf(), "not_a_stub_filename"));
                continue;
            }
            if name.contains("_sz.") {
                inv.skipped
                    .push((entry.path().to_path_buf(), "size_variant_excluded"));
                continue;
            }
            let size = std::fs::metadata(entry.path())
                .map(|m| m.len())
                .unwrap_or(0);
            inv.files.push(DatasetFile {
                kind: DatasetKind::ProductStub,
                // The stub *directory* encodes the edition and hence the
                // year range; the file itself carries no year. The
                // edition is the partition key so the four editions do
                // not overwrite each other.
                source: SourceRef::new(entry.path()).with_edition(*edition),
                size_bytes: size,
            });
        }
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
    name.starts_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_excel_serials_to_unix_days() {
        // IRI week 1114 starts 2001-01-01, which the corpus stores as
        // Excel serial 36892. 2001-01-01 is 11323 days after the Unix
        // epoch.
        assert_eq!(excel_serial_to_unix_days(36892.0), Some(11323));
        assert_eq!(excel_serial_to_unix_days(25569.0), Some(0), "1970-01-01");
        // Serials below 1 are Excel's 1900 phantom-day region.
        assert_eq!(excel_serial_to_unix_days(0.0), None);
        assert_eq!(excel_serial_to_unix_days(-1.0), None);
        assert_eq!(excel_serial_to_unix_days(f64::NAN), None);
        assert_eq!(excel_serial_to_unix_days(f64::INFINITY), None);
    }

    #[test]
    fn applies_excels_1900_leap_year_correction() {
        // Excel believes 1900 was a leap year. Serial 60 is the phantom
        // 1900-02-29 and serials after it are shifted by one day
        // against a correct calendar. calamine's `as_datetime` applies
        // the correction; a naive `serial - 25569` would be wrong here,
        // which is why this conversion is not reimplemented.
        let phantom = calamine::ExcelDateTime::new(60.0, XlDateType::DateTime, false)
            .as_datetime()
            .unwrap();
        assert_eq!(
            phantom.date(),
            chrono::NaiveDate::from_ymd_opt(1900, 2, 28).unwrap(),
            "serial 60 is the phantom day and lands on 1900-02-28"
        );
        // The first of March is the first serial that is off-by-one
        // against a naive calculation, and calamine gets it right.
        let after = calamine::ExcelDateTime::new(61.0, XlDateType::DateTime, false)
            .as_datetime()
            .unwrap();
        assert_eq!(
            after.date(),
            chrono::NaiveDate::from_ymd_opt(1900, 3, 1).unwrap()
        );
    }

    #[test]
    fn a_time_of_day_component_is_truncated_not_rounded() {
        // A week boundary is a date. Rounding 2001-01-01T18:00 up to
        // the 2nd would put the whole week a day out.
        let noon = excel_serial_to_unix_days(36892.0 + 0.5).unwrap();
        assert_eq!(noon, excel_serial_to_unix_days(36892.0).unwrap());
        let late = excel_serial_to_unix_days(36892.0 + 0.99).unwrap();
        assert_eq!(late, excel_serial_to_unix_days(36892.0).unwrap());
    }

    #[test]
    fn parses_iso_dates_from_text_cells() {
        assert_eq!(parse_iso_date("2001-01-01"), Some(11323));
        assert_eq!(parse_iso_date(" 2001-01-01 "), Some(11323));
        assert_eq!(parse_iso_date("not a date"), None);
    }

    #[test]
    fn stub_spec_columns_collapse_to_one_name_across_editions() {
        // The `*` prefix is the reliable marker; the rest of the name
        // differs per edition.
        let y1 = normalise_stub_columns(&[
            "L1".into(),
            "Level".into(),
            "UPC".into(),
            "*STUBSPEC 1416IS   00004".into(),
        ]);
        let y8 = normalise_stub_columns(&[
            "L1".into(),
            "Level".into(),
            "UPC".into(),
            "*AG C=1+ CATEGORY".into(),
        ]);
        assert_eq!(y1[3], "stubspec");
        assert_eq!(y8[3], "stubspec");
    }

    #[test]
    fn typed_stub_columns_keep_their_types_and_the_rest_are_strings() {
        let cols = vec![
            "L1".to_string(),
            "Level".to_string(),
            "UPC".to_string(),
            "SY".to_string(),
            "GE".to_string(),
            "VEND".to_string(),
            "ITEM".to_string(),
            "VOL_EQ".to_string(),
            "PRODUCT_TYPE".to_string(),
        ];
        let s = stub_schema(&cols);
        assert_eq!(s.field(1).data_type(), &DataType::UInt16, "Level");
        assert_eq!(s.field(3).data_type(), &DataType::UInt8, "SY");
        assert_eq!(s.field(4).data_type(), &DataType::UInt8, "GE");
        assert_eq!(s.field(5).data_type(), &DataType::UInt32, "VEND");
        assert_eq!(s.field(6).data_type(), &DataType::UInt32, "ITEM");
        assert_eq!(s.field(7).data_type(), &DataType::Float64, "VOL_EQ");
        // UPC must be a string: `08-01-94922-00101` is not a number,
        // and its leading zero is significant.
        assert_eq!(s.field(2).data_type(), &DataType::Utf8, "UPC");
        assert_eq!(s.field(8).data_type(), &DataType::Utf8);
    }

    #[test]
    fn the_week_dimension_schema_has_four_columns() {
        let s = week_schema();
        assert_eq!(s.fields().len(), 4);
        assert_eq!(s.field(0).name(), "iri_week");
        assert_eq!(s.field(0).data_type(), &DataType::UInt16);
        assert_eq!(s.field(1).name(), "week_start");
        assert_eq!(s.field(2).name(), "week_end");
        assert_eq!(s.field(3).name(), "source_year");
    }

    #[test]
    fn stub_editions_cover_all_four_years_ranges() {
        assert_eq!(STUB_EDITIONS.len(), 4);
        assert_eq!(STUB_EDITIONS[0].2, "2001-2006");
        assert_eq!(STUB_EDITIONS[1].2, "2007");
        assert_eq!(STUB_EDITIONS[2].2, "2008-2011");
        assert_eq!(STUB_EDITIONS[3].2, "2012");
        // Only the year-7 edition is `.xlsx` under a 2007 directory;
        // the prefix changes for years 8+.
        assert_eq!(STUB_EDITIONS[0].1, "prod");
        assert_eq!(STUB_EDITIONS[2].1, "prod11");
        assert_eq!(STUB_EDITIONS[3].1, "prod12");
    }
}
