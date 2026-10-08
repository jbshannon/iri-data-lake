//! `iri_product_attr` — the per-item attribute dictionary.
//!
//! `*_prod_attr` is the attribute side of the product master: for every
//! `(SY, GE, VEND, ITEM)` key that appears in `iri_sales`, a row of
//! free-text attributes ("SHELF STABLE", "PALE", "4.7 PERCENT"). It is
//! the join that lets a sales aggregate be described rather than just
//! counted — and it is the only place in the corpus that says what a
//! product *is*.
//!
//! # The 21-byte pitch
//!
//! These files are fixed-width, but not with the header's own token
//! positions. The layout is:
//!
//! ```text
//! SY [0..3) GE [3..6) VEND [6..12) ITEM [12..18) VOL_EQ [18..27)
//! then N attribute columns of 21 bytes each, the last truncated by 1
//! row_len = 27 + 21N + 20   ⟹   N = (row_len - 47) / 21
//! ```
//!
//! Verified across all 93 files: the arithmetic holds exactly, the four
//! lead fields parse as digits in every sampled row, and the 21-byte
//! slices carry the header names verbatim. `Year9/beer` yields 29
//! attributes at 656 bytes/row; `Year9/saltsnck` yields **81** at 1748;
//! `Year9/toitisu` yields **12** at 299.
//!
//! Whitespace-splitting these files — which is what the layout looks
//! like at a glance — gives 27–32 ragged fields per file and misaligns
//! silently: `MISSING` is exactly 21 characters including padding, so a
//! run of two adjacent missing attributes is 42 spaces and looks like a
//! single field boundary. The pitch is the only thing that recovers the
//! header names exactly.
//!
//! # Per-file schema, on purpose
//!
//! Each of the 93 files has its own attribute set. Forcing them onto
//! one schema means inventing 69 all-null columns for `toitisu` or
//! dropping attributes for everyone else, and a union schema would
//! change shape when a new category appeared — invalidating every
//! prior file's schema version. So each source file writes one Parquet
//! file with the schema its own header declares, and the manifest
//! records that schema's version.
//!
//! Three files (`Year9,10,11/photo/photo_prod_attr`) declare one
//! attribute with an **empty name**. That is a degenerate category
//! definition, not corruption; the column is named `attr_###` so the
//! Parquet schema stays valid.

use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::ArrayBuilder;
use arrow_array::builder::{Float64Builder, StringBuilder, UInt32Builder, UInt8Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::delimited;
use super::ingest::{fingerprint, BatchSink, DatasetParser, DatasetTuning, ParserFactory};
use crate::config::IngestConfig;
use crate::dataset::{infer_category, infer_year, DatasetFile, DatasetInventory, SourceRef};
use crate::errors::Result;
use crate::metrics::sha256_of_file;
use crate::model::DatasetKind;

pub const BATCH_ROWS: usize = 250_000;
pub const MAX_ROWS_PER_FILE: usize = 250_000;

/// Width of the fixed key prefix: `SY GE VEND ITEM VOL_EQ`.
pub const KEY_PREFIX_LEN: usize = 27;
/// Width of one attribute column.
pub const ATTR_WIDTH: usize = 21;

/// Field offsets within the key prefix, measured from all 93 files.
pub mod key_offsets {
    pub const SY: std::ops::Range<usize> = 0..3;
    pub const GE: std::ops::Range<usize> = 3..6;
    pub const VEND: std::ops::Range<usize> = 6..12;
    pub const ITEM: std::ops::Range<usize> = 12..18;
    pub const VOL_EQ: std::ops::Range<usize> = 18..27;
}

/// The attribute count implied by a row length.
///
/// `row_len = 27 + 21*(N-1) + last`, where the last column is
/// truncated to whatever remains. In every one of the 93 files the
/// remainder is exactly 20, so `last = row_len - 47 - 21*(N-1)` and
/// `N = (row_len - 47) / 21 + 1`.
///
/// The `+ 1` matters. An earlier reading of this layout used
/// `N = (row_len - 47) / 21` and silently dropped the final attribute
/// of every file — for `Year9/beer` that was `WINE/LIQUOR TYPE`, a
/// real attribute with real values, not trailing padding. Every
/// affected file still produced 20 trailing bytes that looked like
/// padding, which is exactly why the mistake was invisible.
///
/// Returns `None` when fewer bytes than one attribute are present,
/// which is the signal that the 21-byte pitch does not hold for this
/// file and that guessing would be worse than refusing.
pub fn attr_count(row_len: usize) -> Option<usize> {
    let body = row_len.checked_sub(KEY_PREFIX_LEN)?;
    if body < ATTR_WIDTH {
        return None;
    }
    Some(body.div_ceil(ATTR_WIDTH))
}

/// Width of attribute `i` for a source with `count` attributes and a
/// row (or header) of `line_len` bytes.
pub fn attr_width(i: usize, count: usize, line_len: usize) -> usize {
    if i + 1 == count {
        line_len.saturating_sub(KEY_PREFIX_LEN + i * ATTR_WIDTH)
    } else {
        ATTR_WIDTH
    }
}

/// Byte range of attribute `i` in a source with `count` attributes and a
/// row (or header) of `line_len` bytes.
///
/// `line_len` is passed in rather than derived because **the header and
/// the rows are different lengths**: `Year9/beer`'s header is 652 bytes
/// and its rows are 656. The header's last column is therefore 16
/// bytes and the row's is 20. Both are correct — the header holds a
/// 16-character name (`WINE/LIQUOR TYPE`) and the row a 21-wide value
/// field — and using one length for both would read four bytes past the
/// end of the header.
pub fn attr_range(i: usize, count: usize, line_len: usize) -> std::ops::Range<usize> {
    let start = KEY_PREFIX_LEN + i * ATTR_WIDTH;
    start..start + attr_width(i, count, line_len)
}

/// The `MISSING` sentinel the corpus uses for an absent attribute.
pub const MISSING: &str = "MISSING";

/// Build the Arrow schema for a source with `attrs` named attributes.
///
/// The first five columns are typed; every attribute is a string,
/// because the corpus's attribute values are free text ("SHELF
/// STABLE", "PALE", "LONG NECK BTL IN BOX") with no consistent
/// structure across 93 files. Typing them per-column would mean
/// encoding an inference per attribute name and getting a different
/// answer each time the corpus changed.
pub fn schema_for(attrs: &[String]) -> SchemaRef {
    let mut fields = vec![
        Field::new("sy", DataType::UInt8, true),
        Field::new("ge", DataType::UInt8, true),
        Field::new("vend", DataType::UInt32, true),
        Field::new("item", DataType::UInt32, true),
        Field::new("vol_eq", DataType::Float64, true),
    ];
    for name in attrs {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

/// Column builders matching [`schema_for`].
#[derive(Debug)]
pub struct AttrBuilders {
    sy: UInt8Builder,
    ge: UInt8Builder,
    vend: UInt32Builder,
    item: UInt32Builder,
    vol_eq: Float64Builder,
    attrs: Vec<StringBuilder>,
    rows: u64,
}

impl AttrBuilders {
    pub fn new(attr_count: usize) -> Self {
        Self {
            sy: UInt8Builder::new(),
            ge: UInt8Builder::new(),
            vend: UInt32Builder::new(),
            item: UInt32Builder::new(),
            vol_eq: Float64Builder::new(),
            attrs: (0..attr_count).map(|_| StringBuilder::new()).collect(),
            rows: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.sy.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Append one row of `row_len` bytes, using `count` attributes.
    pub fn push_row(&mut self, row: &[u8], count: usize) {
        push_u8(&mut self.sy, &row[key_offsets::SY]);
        push_u8(&mut self.ge, &row[key_offsets::GE]);
        push_u32(&mut self.vend, &row[key_offsets::VEND]);
        push_u32(&mut self.item, &row[key_offsets::ITEM]);
        match delimited::parse_f64(&row[key_offsets::VOL_EQ]) {
            Some(v) => self.vol_eq.append_value(v),
            None => self.vol_eq.append_null(),
        }
        for i in 0..count {
            let r = attr_range(i, count, row.len());
            self.attrs[i].append_option(text(&row[r]));
        }
        self.rows += 1;
    }

    pub fn finish(mut self, schema: SchemaRef) -> RecordBatch {
        let mut cols: Vec<std::sync::Arc<dyn arrow_array::Array>> = vec![
            Arc::new(self.sy.finish()),
            Arc::new(self.ge.finish()),
            Arc::new(self.vend.finish()),
            Arc::new(self.item.finish()),
            Arc::new(self.vol_eq.finish()),
        ];
        for mut b in self.attrs {
            cols.push(Arc::new(b.finish()));
        }
        RecordBatch::try_new(schema, cols)
            .expect("attr schema and build()'s column list are declared together")
    }
}

fn push_u8(b: &mut UInt8Builder, field: &[u8]) {
    match delimited::parse_int(field) {
        Some(v) if (0..=u8::MAX as i64).contains(&v) => b.append_value(v as u8),
        _ => b.append_null(),
    }
}

fn push_u32(b: &mut UInt32Builder, field: &[u8]) {
    match delimited::parse_int(field) {
        Some(v) if (0..=u32::MAX as i64).contains(&v) => b.append_value(v as u32),
        _ => b.append_null(),
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

/// Normalise an attribute name into a valid, reasonably unique column
/// name.
///
/// Header names are free text: `'ALCOHOLIC VS N/ALCHC'`, `'FLAVOR/SCENT'`,
/// `'WINE/LIQUOR TYPE'`, and in three files the empty string. Two fixes
/// are needed: Parquet field names cannot contain `/`, and an empty or
/// duplicate name would produce an invalid schema. So: non-alphanumerics
/// become `_`, an empty name becomes `attr_<index>`, and a name that
/// collides with an earlier one gets a numeric suffix.
pub fn normalise_attr_name(raw: &[u8], index: usize, taken: &mut Vec<String>) -> String {
    let mut base: String = String::from_utf8_lossy(raw)
        .trim()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    while base.contains("__") {
        base = base.replace("__", "_");
    }
    let base = base.trim_matches('_').to_string();
    let base = if base.is_empty() {
        format!("attr_{index:03}")
    } else {
        base
    };
    let mut candidate = base.clone();
    let mut n = 2;
    while taken.contains(&candidate) {
        candidate = format!("{base}_{n}");
        n += 1;
    }
    taken.push(candidate.clone());
    candidate
}

/// Derive the attribute names from a header line.
///
/// `count` comes from the *row* length (the rows are the wider of the
/// two) but the names are sliced from the *header*, so the ranges are
/// clamped to the header's length. Passing `count` derived from the
/// header's own length would give 30 columns of which the last has no
/// bytes at all; passing the row length with the header's ranges
/// without clamping reads past its end.
pub fn attr_names_from_header(header: &[u8], count: usize, row_len: usize) -> Vec<String> {
    let mut taken: Vec<String> = Vec::with_capacity(count);
    (0..count)
        .map(|i| {
            let r = attr_range(i, count, row_len.min(header.len()));
            // A degenerate category definition can leave a name empty;
            // `normalise_attr_name` turns that into `attr_NNN`.
            normalise_attr_name(header.get(r).unwrap_or(b""), i, &mut taken)
        })
        .collect()
}

/// The attribute dictionary parser for one source file.
#[derive(Debug)]
pub struct AttrParser {
    schema: SchemaRef,
    count: usize,
    body: Vec<u8>,
    row_len: usize,
    rows: u64,
}

impl AttrParser {
    pub fn open(path: &Path, _source: &SourceRef) -> Result<Self> {
        let bytes = delimited::read_file(path)?;
        let Some(header) = delimited::first_line(&bytes) else {
            // An empty attribute file is a real file with no content;
            // treat it as an empty table rather than a failure.
            return Ok(Self {
                schema: schema_for(&[]),
                count: 0,
                body: Vec::new(),
                row_len: 0,
                rows: 0,
            });
        };
        let consumed = bytes
            .iter()
            .position(|&b| b == b'\n')
            .map(|i| i + 1)
            .unwrap_or(bytes.len());
        let body = &bytes[consumed.min(bytes.len())..];

        // Every data row in a prod_attr file has the same length. Take
        // the first one and pin the layout to it.
        let row_len = delimited::lines(body)
            .find(|l| !l.is_empty())
            .map(|l| l.len())
            .unwrap_or(0);
        let count = if row_len == 0 {
            0
        } else {
            match attr_count(row_len) {
                Some(n) => n,
                None => {
                    return Err(crate::errors::IngestError::Discovery(format!(
                        "{}: row length {row_len} does not fit the 21-byte attribute pitch \
                         (expected at least {})",
                        path.display(),
                        KEY_PREFIX_LEN + ATTR_WIDTH
                    )))
                }
            }
        };
        let names = attr_names_from_header(header, count, row_len);
        Ok(Self {
            schema: schema_for(&names),
            count,
            body: body.to_vec(),
            row_len,
            rows: 0,
        })
    }

    /// The attribute column names this file's schema declares.
    pub fn attribute_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .skip(5)
            .map(|f| f.name().clone())
            .collect()
    }
}

impl DatasetParser for AttrParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()> {
        if self.count == 0 {
            return Ok(());
        }
        let mut b = AttrBuilders::new(self.count);
        let mut skipped = 0u64;
        for line in delimited::lines(&self.body) {
            if line.is_empty() {
                continue;
            }
            if line.len() != self.row_len {
                skipped += 1;
                continue;
            }
            b.push_row(line, self.count);
            if b.len() >= BATCH_ROWS {
                self.rows += b.rows();
                let batch = b.finish(self.schema.clone());
                b = AttrBuilders::new(self.count);
                sink.push(batch, cfg)?;
            }
        }
        if !b.is_empty() {
            self.rows += b.rows();
            let batch = b.finish(self.schema.clone());
            sink.push(batch, cfg)?;
        }
        if skipped > 0 {
            tracing::warn!(
                source = %self.row_len,
                skipped,
                "prod_attr rows whose length differed from the first row were skipped"
            );
        }
        Ok(())
    }

    fn expected_rows(&self) -> Option<u64> {
        Some(self.rows)
    }

    /// A digest of the column names this file's header declares.
    ///
    /// The column set is a property of *this file* (12 attributes for
    /// `toitisu`, 82 for `saltsnck`), so it cannot live in the
    /// crate-wide `output_schema_version`. Editing a source's header
    /// changes this fingerprint and forces a re-ingest; an unchanged
    /// header does not.
    fn schema_fingerprint(&self) -> Option<String> {
        Some(fingerprint(
            self.schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<String>>(),
        ))
    }

    fn note(&self) -> Option<String> {
        Some(format!(
            "attributes={} row_len={} columns={:?}",
            self.count,
            self.row_len,
            self.attribute_names()
        ))
    }
}

pub fn parser(path: &Path, source: &SourceRef) -> Result<Box<dyn DatasetParser>> {
    Ok(Box::new(AttrParser::open(path, source)?))
}

pub fn tuning() -> DatasetTuning {
    DatasetTuning::new(BATCH_ROWS, MAX_ROWS_PER_FILE, parser as ParserFactory)
}

/// Discover the `*_prod_attr` sources under `input_root`.
///
/// Years 9, 10 and 11 only (31 categories each). Every other year has
/// none, which is a fact about the corpus rather than a gap in the walk.
pub fn discover(input_root: &Path) -> Result<DatasetInventory> {
    let mut inv = DatasetInventory::new(DatasetKind::ProductAttr);
    for entry in walkdir::WalkDir::new(input_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !is_ignored(e.path(), input_root))
    {
        let Ok(entry) = entry else { continue };
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with("_prod_attr") {
            continue;
        }
        let path = entry.path();
        let Some(year) = infer_year(path, input_root) else {
            inv.skipped.push((path.to_path_buf(), "no_year_ancestor"));
            continue;
        };
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        inv.files.push(DatasetFile {
            kind: DatasetKind::ProductAttr,
            source: SourceRef::new(path)
                .with_year(year)
                .with_category(infer_category(path, input_root).unwrap_or_default()),
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

/// Content hash of a source, exposed for the dedup question.
pub fn content_hash(path: &Path) -> Result<String> {
    sha256_of_file(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, StringArray, UInt32Array, UInt8Array};

    /// `Year9/beer/beer_prod_attr`'s 30 attribute names, in order.
    ///
    /// Taken from the file rather than pasted as a literal, because the
    /// layout is byte-exact: a header row is 652 bytes and a data row is
    /// 656, and a literal with hand-trimmed trailing padding is
    /// indistinguishable from a real one until the widths stop adding
    /// up.
    const BEER_ATTRS: &[&str] = &[
        "FLAVOR/SCENT",
        "PACKAGE",
        "PRODUCT TYPE",
        "STORE LOCATION",
        "ALCOHOLIC VS N/ALCHC",
        "CALORIE LEVEL",
        "COLOR",
        "DRYNESS",
        "GEOGRAPH. REFERENCE",
        "IMPORTED VS DOMESTIC",
        "PROCESS",
        "ALCOHOL CONTENT",
        "CONSISTENCY",
        "SEASONAL INFO",
        "SIZE",
        "TYPE OF BEER/ALE",
        "PERCENTAGE OF JUICE",
        "TYPE OF SWEETENER",
        "ADDITIVES",
        "AGE/VINTAGE",
        "BIOLOGICAL INFO",
        "CAFFEINE INFO",
        "GRADE INFO",
        "LABEL INSTRUCTIONS",
        "PRESERVATIVE INFO",
        "REGION",
        "TYPE OF CIDER",
        "TYPE OF SODA",
        "TYPE OF SUGAR",
        "WINE/LIQUOR TYPE",
    ];

    /// `Year9/beer/beer_prod_attr`'s first data row, attribute by
    /// attribute, **as the 21-byte grid reads it**.
    ///
    /// Note `LONG NECK BTL IN BOX`. The real source value for
    /// `PACKAGE` is `LONG NECK BTL IN BOX BEER` — 25 bytes — and it
    /// physically overflows its 21-byte slot, so its last four bytes
    /// land at the start of the `PRODUCT TYPE` slot. That is a property
    /// of the corpus, not of the parser: the slot boundaries are still
    /// 21 bytes apart in every row (verified by requiring
    /// space-to-non-space at each offset in all 16 760 rows), and the
    /// overflow is what puts `BEER` in the next attribute. A parser
    /// that "fixed" this by re-flowing values would misalign every
    /// column after the first overflow.
    const BEER_VALUES: &[&str] = &[
        "MISSING",
        "LONG NECK BTL IN BOX",
        "BEER",
        "SHELF STABLE",
        "4.7 PERCENT",
        "MISSING",
        "PALE",
        "MISSING",
        "MISSING",
        "DOMESTIC",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "LAGER",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
        "MISSING",
    ];

    /// The key prefix of `Year9/beer/beer_prod_attr`'s first row, byte
    /// for byte: `SY=0 GE=1 VEND=1 ITEM=30233 VOL_EQ=0.1944`.
    const BEER_KEY: &[u8] = b" 0  1     1 30233   0.1944 ";

    /// The measured last-slot width of the header line.
    ///
    /// The header is 652 bytes and the rows 656. The header's last slot
    /// holds `WINE/LIQUOR TYPE` — 16 characters, no padding — while the
    /// row's last slot is the usual 21 minus one byte. So the header and
    /// the rows have **different lengths**, and one length cannot slice
    /// both.
    const BEER_HEADER_LEN: usize = 652;
    /// The measured length of `Year9/beer/beer_prod_attr`'s data rows.
    const BEER_ROW_LEN: usize = 656;

    /// Build a header and a matching data row from the attribute
    /// lists, at the measured widths.
    ///
    /// Both are assembled from the same list so the widths cannot
    /// disagree, and the last slot is sized from each line's own total
    /// length — which is what makes the 652/656 difference explicit
    /// rather than something the parser has to guess.
    fn beer_header_and_row() -> (Vec<u8>, Vec<u8>) {
        let count = BEER_ATTRS.len();
        assert_eq!(BEER_VALUES.len(), count);
        let mut hdr = BEER_KEY.to_vec();
        let mut row = BEER_KEY.to_vec();
        for i in 0..count {
            pad_into(
                &mut hdr,
                BEER_ATTRS[i].as_bytes(),
                attr_width(i, count, BEER_HEADER_LEN),
            );
            pad_into(
                &mut row,
                BEER_VALUES[i].as_bytes(),
                attr_width(i, count, BEER_ROW_LEN),
            );
        }
        assert_eq!(hdr.len(), BEER_HEADER_LEN);
        assert_eq!(row.len(), BEER_ROW_LEN);
        (hdr, row)
    }

    fn pad_into(out: &mut Vec<u8>, value: &[u8], width: usize) {
        assert!(value.len() <= width, "{value:?} does not fit {width} bytes");
        out.extend_from_slice(value);
        out.extend(std::iter::repeat(b' ').take(width - value.len()));
    }

    #[test]
    fn the_21_byte_pitch_recovers_30_attributes_from_a_656_byte_row() {
        let (hdr, row) = beer_header_and_row();
        // The header is 4 bytes narrower than the rows, because the
        // last slot holds a 16-character name and a 21-wide value.
        assert_eq!(hdr.len(), 652);
        assert_eq!(row.len(), 656);
        assert_eq!(attr_count(656), Some(30));
        assert_eq!(attr_width(29, 30, hdr.len()), 16);
        assert_eq!(attr_width(29, 30, row.len()), 20);
        let names = attr_names_from_header(&hdr, 30, 656);
        assert_eq!(names.len(), 30);
        assert_eq!(names[0], "FLAVOR_SCENT");
        assert_eq!(names[1], "PACKAGE");
        assert_eq!(names[2], "PRODUCT_TYPE");
        assert_eq!(names[3], "STORE_LOCATION");
        // Multi-word names survive because the pitch, not whitespace,
        // decides the boundary.
        assert_eq!(names[4], "ALCOHOLIC_VS_N_ALCHC");
        assert_eq!(names[5], "CALORIE_LEVEL");
        // The 30th is the truncated one, and it is a real attribute —
        // not trailing padding.
        assert_eq!(names[29], "WINE_LIQUOR_TYPE");
    }

    #[test]
    fn the_final_attribute_is_kept_not_dropped_as_padding() {
        // The regression this guards: `N = (row_len - 47) / 21` yields
        // 29 for a 656-byte row and throws away the last attribute,
        // which is `WINE/LIQUOR TYPE` with real values. The 20-byte
        // remainder looked exactly like padding, which is why the
        // mistake survived a survey of all 93 files.
        let (hdr, row) = beer_header_and_row();
        let count = attr_count(row.len()).unwrap();
        let names = attr_names_from_header(&hdr, count, row.len());
        let last_value = delimited::trim_ascii_space(&row[attr_range(count - 1, count, row.len())]);
        assert_eq!(names[count - 1], "WINE_LIQUOR_TYPE");
        assert_eq!(last_value, b"MISSING", "the dropped attribute had values");
        // And the dropped-attribute version would have produced one
        // fewer column.
        assert_eq!((656 - 47) / 21, 29);
        assert_ne!(count, (656 - 47) / 21);
    }

    #[test]
    fn slices_the_row_by_pitch_not_by_whitespace() {
        let (_, row) = beer_header_and_row();
        let count = attr_count(row.len()).unwrap();
        let vals: Vec<String> = (0..count)
            .map(|i| {
                String::from_utf8_lossy(delimited::trim_ascii_space(
                    &row[attr_range(i, count, row.len())],
                ))
                .into_owned()
            })
            .collect();
        assert_eq!(vals[0], "MISSING");
        assert_eq!(vals[1], "LONG NECK BTL IN BOX");
        assert_eq!(vals[2], "BEER");
        assert_eq!(vals[3], "SHELF STABLE");
        assert_eq!(vals[4], "4.7 PERCENT");
        assert_eq!(vals[5], "MISSING");
        assert_eq!(vals[6], "PALE");
        // Two adjacent MISSINGs are 42 bytes of padding. Whitespace
        // splitting collapses them into one field; the pitch does not.
        assert_eq!(vals[7], "MISSING");
        assert_eq!(vals[8], "MISSING");
        assert_eq!(vals[29], "MISSING");
    }

    #[test]
    fn a_value_that_overflows_its_slot_still_lands_in_the_right_attribute() {
        // The real `Year9/beer` row carries `LONG NECK BTL IN BOX BEER`
        // in `PACKAGE`, which is 25 bytes into a 21-byte slot. The grid
        // is unchanged, so `PACKAGE` reads as the truncated
        // `LONG NECK BTL IN BOX` and the overflowed `BEER` reads as
        // `PRODUCT TYPE` — which is what the real file's next column
        // says. Pinning this because "fixing" it by re-flowing would
        // misalign all 26 attributes after it.
        let (_, row) = beer_header_and_row();
        let count = attr_count(row.len()).unwrap();
        let read = |i: usize| {
            String::from_utf8_lossy(delimited::trim_ascii_space(
                &row[attr_range(i, count, row.len())],
            ))
            .into_owned()
        };
        assert_eq!(read(1), "LONG NECK BTL IN BOX");
        assert_eq!(read(2), "BEER");
        assert_eq!(read(3), "SHELF STABLE");
    }

    #[test]
    fn whitespace_splitting_would_misalign_the_row() {
        // The reason the pitch exists. `MISSING` padded to 21 bytes
        // means adjacent missing attributes look like one wide field.
        let (_, row) = beer_header_and_row();
        let naive: Vec<&[u8]> = delimited::split_whitespace(&row);
        assert_ne!(
            naive.len(),
            attr_count(row.len()).unwrap(),
            "if this ever matches, the pitch is not buying anything"
        );
    }

    #[test]
    fn typed_key_fields_and_attributes_are_strings() {
        let (hdr, row) = beer_header_and_row();
        let count = attr_count(row.len()).unwrap();
        let names = attr_names_from_header(&hdr, count, row.len());
        let mut b = AttrBuilders::new(count);
        b.push_row(&row, count);
        let batch = b.finish(schema_for(&names));
        assert_eq!(batch.num_columns(), 5 + count);
        let sy = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(sy.value(0), 0);
        let ge = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(ge.value(0), 1);
        let item = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(item.value(0), 30233);
        let vol = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(vol.value(0), 0.1944);
        let flavor = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(flavor.value(0), "MISSING");
        let pkg = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(pkg.value(0), "LONG NECK BTL IN BOX");
    }

    #[test]
    fn the_attribute_count_varies_by_category() {
        // Measured row lengths from the corpus, with the corrected
        // `ceil` count:
        //   Year9/toitisu     299 B ->  13 attributes
        //   Year9/beer        656 B ->  30
        //   Year11/coldcer   1328 B ->  62
        //   Year9/saltsnck   1748 B ->  82
        assert_eq!(attr_count(299), Some(13));
        assert_eq!(attr_count(656), Some(30));
        assert_eq!(attr_count(1328), Some(62));
        assert_eq!(attr_count(1748), Some(82));
    }

    #[test]
    fn every_row_length_leaves_exactly_20_bytes_of_last_column() {
        // In all 93 files the last attribute is truncated to 20 bytes.
        // If a future file left a different remainder the layout would
        // still work, so this is a measured observation rather than an
        // invariant — but it is worth pinning, because it is what makes
        // "20 trailing bytes" *look* like padding.
        for row_len in [299, 656, 929, 1328, 1748] {
            let n = attr_count(row_len).unwrap();
            assert_eq!(attr_width(n - 1, n, row_len), 20, "row_len={row_len}");
            for i in 0..n - 1 {
                assert_eq!(attr_width(i, n, row_len), ATTR_WIDTH);
            }
        }
    }

    #[test]
    fn a_row_too_short_for_one_attribute_is_refused() {
        // 26 bytes is the key prefix and nothing else.
        assert_eq!(attr_count(26), None);
        assert_eq!(attr_count(27), None);
        assert_eq!(attr_count(0), None);
        assert_eq!(attr_count(48), Some(1), "exactly one 21-byte attribute");
    }

    #[test]
    fn the_last_attribute_is_truncated_by_one_byte() {
        let count = attr_count(656).unwrap();
        let last = attr_range(count - 1, count, 656);
        assert_eq!(last.end, 656);
        assert_eq!(last.end - last.start, 20);
        // Every other attribute is a full 21.
        assert_eq!(attr_range(0, count, 656).len(), ATTR_WIDTH);
        // And the header's last slot is one byte narrower, which is
        // why the two lengths are passed separately.
        assert_eq!(attr_range(29, 30, 652).len(), 16);
    }

    #[test]
    fn an_empty_header_name_becomes_a_positional_column_name() {
        // `Year9,10,11/photo/photo_prod_attr` has one attribute with an
        // empty name. It must still produce a valid Parquet schema.
        let mut taken: Vec<String> = Vec::new();
        assert_eq!(normalise_attr_name(b"", 4, &mut taken), "attr_004");
        let names = attr_names_from_header(
            b"SY GE VEND  ITEM  VOL_EQ                        EXPOSURE             ",
            1,
            48,
        );
        assert_eq!(names.len(), 1);
        assert!(!names[0].is_empty());
    }

    #[test]
    fn duplicate_header_names_do_not_collide() {
        let mut taken = Vec::new();
        assert_eq!(normalise_attr_name(b"COLOR", 0, &mut taken), "COLOR");
        assert_eq!(normalise_attr_name(b"COLOR", 1, &mut taken), "COLOR_2");
        assert_eq!(normalise_attr_name(b"COLOR", 2, &mut taken), "COLOR_3");
        assert_eq!(taken.len(), 3);
    }

    #[test]
    fn the_fingerprint_tracks_the_declared_columns() {
        let (hdr, row) = beer_header_and_row();
        let a = schema_for(&attr_names_from_header(&hdr, 30, row.len()));
        let b = schema_for(&attr_names_from_header(&hdr, 30, row.len()));
        assert_eq!(a, b, "identical headers must yield an identical schema");
        let fp = |s: &std::sync::Arc<arrow_schema::Schema>| {
            fingerprint(
                s.fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<String>>(),
            )
        };
        assert_eq!(fp(&a), fp(&b));
        let c = schema_for(&attr_names_from_header(&hdr, 29, row.len()));
        assert_ne!(a, c, "a different attribute count is a different schema");
        assert_ne!(fp(&a), fp(&c));
    }
}
