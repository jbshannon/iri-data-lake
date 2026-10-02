//! Raw-bytes → Arrow column-builder parser.
//!
//! ## Design
//!
//! 1. The caller has already mmap'd (or otherwise loaded) the raw file
//!    bytes into memory and verified the header.
//! 2. `parse_records_into_builder` walks the body slice in record
//!    strides, slicing each field by its fixed byte range and appending
//!    the typed value to the matching Arrow builder.
//! 3. No per-record allocation of strings or row structs. Each builder
//!    pre-allocates capacity for one full batch.
//! 4. On any field-level error we return immediately with the offending
//!    row index and raw bytes; the current scaffold aborts the whole
//!    source file. A later "reject-invalid-rows" mode will instead
//!    route bad rows to `rejected_rows` without aborting.

use crate::arrow_output::SalesBuilders;
use crate::errors::{IngestError, MoneyErrorReason};
use crate::feature::{parse_feature, FeatureCode};
use crate::fixed_width::{
    self, CRLF_OFFSET, D, DOLLARS, F, GE, HEADER_LEN, IRI_KEY, ITEM, PR, RECORD_LEN, SY, UNITS,
    VEND, WEEK,
};
use crate::model::SourceIdentity;
use crate::money::parse_dollars_cents;

/// Validate a file-sized body for record alignment.
///
/// Returns the expected number of rows on success. The check is:
/// `(size - HEADER_LEN) % RECORD_LEN == 0`.
///
/// Note: `HEADER_LEN` includes the CRLF terminator on the header line,
/// and `RECORD_LEN` includes the CRLF terminator on every data row,
/// so a body of N×`RECORD_LEN` bytes yields exactly N rows.
pub fn expected_row_count(file_size: u64) -> Result<u64, IngestError> {
    fixed_width::expected_rows(file_size).ok_or_else(|| IngestError::RecordAlignment {
        path: std::path::PathBuf::new(),
        size: file_size,
        header: HEADER_LEN,
        record: RECORD_LEN,
        remainder: (file_size.saturating_sub(HEADER_LEN as u64)) % RECORD_LEN as u64,
    })
}

/// Append one batch's worth of rows from `body` into `builders`.
///
/// `body` is the file bytes **after** the header (i.e. starting at the
/// first byte of the first data row's CRLF-terminated content). It must
/// be exactly `record_count * RECORD_LEN` long; we trust the caller to
/// have validated that with `expected_row_count`.
///
/// `record_count` is the number of rows to append (≤ builders.capacity).
pub fn parse_records_into_builder(
    identity: &SourceIdentity,
    body: &[u8],
    start_row: u64,
    record_count: usize,
    builders: &mut SalesBuilders,
) -> Result<(), IngestError> {
    debug_assert!(body.len() >= record_count * RECORD_LEN);

    let mut off = 0usize;
    for row in start_row.. {
        if (off / RECORD_LEN) >= record_count {
            break;
        }
        let rec = &body[off..off + RECORD_LEN];
        parse_one(identity, rec, row, builders)?;
        off += RECORD_LEN;
    }
    Ok(())
}

#[inline]
fn parse_one(
    identity: &SourceIdentity,
    rec: &[u8],
    row_idx: u64,
    b: &mut SalesBuilders,
) -> Result<(), IngestError> {
    // Confirm the trailing CRLF of the row. This is the cheapest invariant
    // check: a file that passes `expected_row_count` will normally pass this
    // too, but a corrupt mid-file byte that shifts the row boundary would
    // be caught here.
    if rec.get(CRLF_OFFSET..CRLF_OFFSET + 2) != Some(b"\r\n") {
        return Err(IngestError::RecordAlignment {
            path: identity.path.clone(),
            size: rec.len() as u64,
            header: HEADER_LEN,
            record: RECORD_LEN,
            remainder: 1,
        });
    }

    // iri_key — UInt32, ASCII digits with possible leading space
    let iri_key = parse_uint::<u32>(iri_key_field(rec), "iri_key").map_err(|raw| {
        IngestError::InvalidIriKey {
            identity: Box::new(identity.clone()),
            row: row_idx,
            raw_bytes: raw,
        }
    })?;
    b.iri_key.append_value(iri_key);

    // week — UInt16, ASCII digits
    let week =
        parse_uint::<u16>(WEEK.slice(rec), "week").map_err(|raw| IngestError::InvalidWeek {
            identity: Box::new(identity.clone()),
            row: row_idx,
            raw_bytes: raw,
        })?;
    b.week.append_value(week);

    // sy, ge — UInt8
    let sy =
        parse_uint::<u8>(SY.slice(rec), "sy").map_err(|raw| IngestError::InvalidIntegerField {
            identity: Box::new(identity.clone()),
            row: row_idx,
            field: "sy",
            raw_bytes: raw,
        })?;
    b.sy.append_value(sy);

    let ge =
        parse_uint::<u8>(GE.slice(rec), "ge").map_err(|raw| IngestError::InvalidIntegerField {
            identity: Box::new(identity.clone()),
            row: row_idx,
            field: "ge",
            raw_bytes: raw,
        })?;
    b.ge.append_value(ge);

    // vend, item — UInt32
    let vend = parse_uint::<u32>(VEND.slice(rec), "vend").map_err(|raw| {
        IngestError::InvalidIntegerField {
            identity: Box::new(identity.clone()),
            row: row_idx,
            field: "vend",
            raw_bytes: raw,
        }
    })?;
    b.vend.append_value(vend);

    let item = parse_uint::<u32>(ITEM.slice(rec), "item").map_err(|raw| {
        IngestError::InvalidIntegerField {
            identity: Box::new(identity.clone()),
            row: row_idx,
            field: "item",
            raw_bytes: raw,
        }
    })?;
    b.item.append_value(item);

    // units — Int32 (sales can be negative? — observed values are ≥ 0,
    // but the schema allows negatives in case of returns.)
    let units = parse_int::<i32>(UNITS.slice(rec), "units").map_err(|raw| {
        IngestError::InvalidIntegerField {
            identity: Box::new(identity.clone()),
            row: row_idx,
            field: "units",
            raw_bytes: raw,
        }
    })?;
    b.units.append_value(units);

    // dollars — exactly parsed into cents
    let dollars_cents =
        parse_dollars_cents(DOLLARS.slice(rec)).map_err(|e| IngestError::InvalidDollars {
            identity: Box::new(identity.clone()),
            row: row_idx,
            raw_bytes: format_bytes(DOLLARS.slice(rec)),
            reason: e.reason(),
        })?;
    b.dollars_cents.append_value(dollars_cents);

    // feature code
    let feature = parse_feature(F.slice(rec)).map_err(|e| IngestError::UnknownFeatureCode {
        identity: Box::new(identity.clone()),
        row: row_idx,
        raw_bytes: format!("{:?} ({})", F.slice(rec), e.0),
    })?;
    b.feature_code.append_value(encode_feature(feature));

    // display — UInt8 (0/1 observed; allow 0..=255 to be safe)
    let display = parse_uint::<u8>(D.slice(rec), "display").map_err(|raw| {
        IngestError::InvalidIntegerField {
            identity: Box::new(identity.clone()),
            row: row_idx,
            field: "display",
            raw_bytes: raw,
        }
    })?;
    b.display.append_value(display);

    // price_reduction — Boolean (encoded as '0' or '1' ASCII)
    let pr_byte = PR.slice(rec);
    let pr_bool = match pr_byte {
        [b'0'] => false,
        [b'1'] => true,
        other => {
            return Err(IngestError::InvalidIntegerField {
                identity: Box::new(identity.clone()),
                row: row_idx,
                field: "price_reduction",
                raw_bytes: format!("{:?}", other),
            });
        }
    };
    b.price_reduction.append_value(pr_bool);

    // Low-cardinality source attributes are NOT appended here:
    // `source_year`, `category`, and `channel` are partition-only metadata.
    // They live in the Hive-style directory layout (`year=N/category=…/
    // channel=…/`) and are read back by Hive-aware engines (DuckDB with
    // `hive_partitioning=true`, PyArrow, Polars, Spark, Iceberg, Delta).

    Ok(())
}

#[inline]
fn iri_key_field(rec: &[u8]) -> &[u8] {
    IRI_KEY.slice(rec)
}

#[inline]
fn parse_uint<T>(field: &[u8], name: &'static str) -> Result<T, String>
where
    T: std::str::FromStr<Err = std::num::ParseIntError>,
{
    let s = match std::str::from_utf8(field) {
        Ok(s) => s,
        Err(_) => return Err(format!("{:02x?} (non-utf8 in {})", field, name)),
    };
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(format!("{:?} (empty {})", field, name));
    }
    trimmed
        .parse::<T>()
        .map_err(|e| format!("{:?} ({}: {:?})", field, name, e))
}

#[inline]
fn parse_int<T>(field: &[u8], name: &'static str) -> Result<T, String>
where
    T: std::str::FromStr<Err = std::num::ParseIntError>,
{
    parse_uint::<T>(field, name)
}

#[inline]
fn encode_feature(f: FeatureCode) -> u8 {
    f.as_u8()
}

#[inline]
fn format_bytes(field: &[u8]) -> String {
    format!("{:02x?}", field)
}

impl fixed_width::FieldSpec {
    #[inline]
    pub fn slice<'a>(&self, row: &'a [u8]) -> &'a [u8] {
        &row[self.start..self.end]
    }
}

/// Convenience: validate the file's header and alignment; return
/// expected row count.
pub fn validate_file_layout(path: &std::path::Path, file_size: u64) -> Result<u64, IngestError> {
    if file_size < HEADER_LEN as u64 {
        return Err(IngestError::HeaderMismatch {
            path: path.to_path_buf(),
            expected: std::str::from_utf8(fixed_width::HEADER_TEXT)
                .unwrap_or("")
                .to_string(),
            actual: format!("<only {} bytes>", file_size),
        });
    }
    fixed_width::expected_rows(file_size).ok_or_else(|| IngestError::RecordAlignment {
        path: path.to_path_buf(),
        size: file_size,
        header: HEADER_LEN,
        record: RECORD_LEN,
        remainder: file_size.saturating_sub(HEADER_LEN as u64) % RECORD_LEN as u64,
    })
}

/// Compatibility helper used by callers that need to construct
/// `MoneyErrorReason` directly (kept for the public surface).
#[allow(dead_code)]
pub fn money_reason_for(s: &str) -> MoneyErrorReason {
    match s {
        "empty" => MoneyErrorReason::Empty,
        "non_ascii" => MoneyErrorReason::NonAscii,
        "too_many_fractional_digits" => MoneyErrorReason::TooManyFractionalDigits,
        "multiple_decimal_points" => MoneyErrorReason::MultipleDecimalPoints,
        "unexpected_sign" => MoneyErrorReason::UnexpectedSign,
        _ => MoneyErrorReason::NotANumber,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow_output::schema;
    use crate::model::Channel;

    fn tiny_identity() -> SourceIdentity {
        SourceIdentity {
            path: std::path::PathBuf::from("/tmp/test"),
            year: 1,
            category: "test".to_string(),
            channel: Channel::Drug,
            filename_week_start: 1,
            filename_week_end: 52,
        }
    }

    #[test]
    fn expected_row_count_works() {
        // 57 (header) + N*56 (body) → N rows
        assert_eq!(expected_row_count(57).unwrap(), 0);
        assert_eq!(expected_row_count(57 + 56).unwrap(), 1);
        assert_eq!(expected_row_count(57 + 56 * 10).unwrap(), 10);
        // Misaligned
        assert!(expected_row_count(57 + 1).is_err());
        assert!(expected_row_count(57 + 56 + 1).is_err());
    }

    #[test]
    fn parses_one_record_to_a_batch() {
        // Construct one minimal data row:
        // IRI_KEY=1234567, WEEK=1114, SY=0, GE=2, VEND=18200,
        // ITEM=647, UNITS=1, DOLLARS=9.29, F=NONE, D=0, PR=0, + CRLF
        let mut row = Vec::new();
        row.extend_from_slice(b"1234567"); // IRI_KEY (7)
        row.push(b' ');
        row.extend_from_slice(b"1114"); // WEEK (4)
        row.push(b' ');
        row.extend_from_slice(b" 0"); // SY (2)
        row.push(b' ');
        row.extend_from_slice(b" 2"); // GE (2)
        row.push(b' ');
        row.extend_from_slice(b"18200"); // VEND (5)
        row.push(b' ');
        row.extend_from_slice(b"  647"); // ITEM (5)
        row.push(b' ');
        row.extend_from_slice(b"    1"); // UNITS (5)
        row.push(b' ');
        row.extend_from_slice(b"    9.29"); // DOLLARS (8) — 4 spaces + "9.29"
        row.push(b' ');
        row.extend_from_slice(b"NONE"); // F (4)
        row.push(b' ');
        row.push(b'0'); // D (1)
        row.push(b' ');
        row.push(b'0'); // PR (1)
        row.extend_from_slice(b"\r\n"); // CRLF (2)
        assert_eq!(row.len(), RECORD_LEN, "row bytes: {:?}", row);

        let mut b = SalesBuilders::with_capacity(1);
        parse_records_into_builder(&tiny_identity(), &row, 0, 1, &mut b).unwrap();
        let batch = b.finish(schema()).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let iri = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .unwrap()
            .value(0);
        let week = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt16Array>()
            .unwrap()
            .value(0);
        let dollars = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        let pr = batch
            .column(10)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .value(0);

        assert_eq!(iri, 1_234_567);
        assert_eq!(week, 1114);
        assert_eq!(dollars, 929);
        assert!(!pr);
        // `category` and `channel` are no longer physical columns —
        // they come back as virtual columns via Hive partitioning.
    }
}
