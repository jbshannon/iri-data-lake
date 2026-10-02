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

/// Parse a fixed-width ASCII integer field straight from bytes.
///
/// This replaces a `from_utf8 -> str::trim -> FromStr` chain that a
/// sampling profile put at ~18.6% of parse self time. Both steps were
/// wasted work on this format:
///
/// * `from_utf8` validated UTF-8 on fields that are ASCII digits by
///   construction — and any byte >= 0x80 fails the digit test below
///   anyway, so the validation was redundant.
/// * `str::trim` applies *Unicode* whitespace rules through
///   `char::is_whitespace`, dispatching on multi-byte code points, for
///   fields that are space-padded (0x20) ASCII. `trim_matches` was the
///   single largest frame in the parser.
///
/// Accepts optional leading/trailing ASCII spaces, an optional `+`/`-`
/// sign, and ASCII digits. Non-UTF-8 bytes now surface as "not a
/// digit" rather than "non-utf8"; a fixed-width ASCII format has no
/// legitimate non-UTF-8 field, so the distinction is not worth a
/// validation pass over every row.
#[inline]
fn parse_int64(field: &[u8], name: &str) -> Result<i64, String> {
    let n = field.len();
    let mut i = 0;
    while i < n && field[i] == b' ' {
        i += 1;
    }

    let mut negative = false;
    if i < n && (field[i] == b'-' || field[i] == b'+') {
        negative = field[i] == b'-';
        i += 1;
    }

    let mut acc: u64 = 0;
    let mut digits = 0usize;
    while i < n {
        let byte = field[i];
        if byte == b' ' {
            // Trailing padding. Everything after the first space must
            // also be a space, otherwise this is an interior blank.
            i += 1;
            while i < n {
                if field[i] != b' ' {
                    return Err(format!("{:?} (non-space after digits in {})", field, name));
                }
                i += 1;
            }
            break;
        }
        let d = byte.wrapping_sub(b'0');
        if d > 9 {
            return Err(format!("{:?} (non-digit in {})", field, name));
        }
        // The widest integer field in the layout is 8 bytes, so a u64
        // accumulator cannot overflow in 19 or fewer digits; check the
        // bound here rather than paying two checked ops per digit.
        // The guard must precede the multiply -- a 20-digit field would
        // otherwise overflow the accumulator itself (a panic in debug
        // builds) before any post-loop check could run.
        if digits == 19 {
            return Err(format!("{:?} (overflow in {})", field, name));
        }
        acc = acc * 10 + d as u64;
        digits += 1;
        i += 1;
    }

    if digits == 0 {
        return Err(format!("{:?} (empty {})", field, name));
    }
    if acc > i64::MAX as u64 {
        return Err(format!("{:?} (overflow in {})", field, name));
    }
    let v = acc as i64;
    Ok(if negative { -v } else { v })
}

#[inline]
fn parse_uint<T>(field: &[u8], name: &'static str) -> Result<T, String>
where
    T: TryFrom<i64>,
{
    parse_int64(field, name)?
        .try_into()
        .map_err(|_| format!("{:?} ({} out of range)", field, name))
}

/// `units` is signed: sales can be negative for returns.
#[inline]
fn parse_int<T>(field: &[u8], name: &'static str) -> Result<T, String>
where
    T: TryFrom<i64>,
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

    #[test]
    fn parse_int64_handles_padding_sign_and_errors() {
        // Space-padded ASCII, the actual on-disk shape.
        assert_eq!(parse_int64(b"  12345", "f").unwrap(), 12345);
        assert_eq!(parse_int64(b"  12345   ", "f").unwrap(), 12345);
        assert_eq!(parse_int64(b"0", "f").unwrap(), 0);
        assert!(parse_int64(b"     ", "f").is_err(), "empty after trim");

        // Signs: `+` was accepted by `FromStr` before, keep it.
        assert_eq!(parse_int64(b" +42", "f").unwrap(), 42);
        assert_eq!(parse_int64(b"-42", "f").unwrap(), -42);

        // Rejections.
        assert!(parse_int64(b"12 34", "f").is_err(), "interior blank");
        assert!(parse_int64(b"12a4", "f").is_err(), "non-digit");
        assert!(parse_int64(b"\xff\xfe", "f").is_err(), "non-ascii");
        assert!(parse_int64(b"", "f").is_err(), "empty field");

        // Overflow is caught once, at the end.
        assert!(parse_int64(b"99999999999999999999", "f").is_err());
        assert_eq!(parse_int64(b"9223372036854775807", "f").unwrap(), i64::MAX);
    }

    #[test]
    fn parse_uint_range_checks_the_target_type() {
        // A value that fits i64 but not the narrower target.
        assert_eq!(parse_uint::<u16>(b" 65535", "week").unwrap(), 65535);
        assert!(parse_uint::<u16>(b" 65536", "week").is_err(), "u16 overflow");
        assert_eq!(parse_uint::<u8>(b" 255", "ge").unwrap(), 255);
        assert!(parse_uint::<u8>(b" 256", "ge").is_err(), "u8 overflow");
        // Negative into an unsigned target must not wrap.
        assert!(parse_uint::<u32>(b"-1", "iri_key").is_err());
        // Signed target still accepts negatives.
        assert_eq!(parse_int::<i32>(b" -7", "units").unwrap(), -7);
    }
}
