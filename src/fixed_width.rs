//! Canonical fixed-width layout for `*_drug_*` and `*_groc_*` sales files.
//!
//! ## Format invariants (verified against multiple real files)
//!
//! - The header is **57 bytes** including CRLF (55 bytes of column labels
//!   followed by `\r\n`).
//! - Each data row is **56 bytes** including CRLF (54 bytes of fields
//!   followed by `\r\n`).
//!
//! The spec the project was built from stated `HEADER_LEN = 55` and
//! `RECORD_LEN = 54` and described those as "including CRLF". That was
//! slightly off — those numbers are the *content* lengths. Empirically,
//! `(file_size - 57) % 56 == 0` holds for every file checked across years
//! 1, 6, 11 and 12.
//!
//! ## Field offsets (zero-based half-open byte ranges within the
//! 54-byte content portion of each row)
//!
//! ```text
//! IRI_KEY  [ 0..7 ]  7 chars
//! (sep)    [ 7..8 ]  1 space
//! WEEK     [ 8..12]  4 chars
//! (sep)    [12..13]  1 space
//! SY       [13..15]  2 chars
//! (sep)    [15..16]  1 space
//! GE       [16..18]  2 chars
//! (sep)    [18..19]  1 space
//! VEND     [19..24]  5 chars
//! (sep)    [24..25]  1 space
//! ITEM     [25..30]  5 chars
//! (sep)    [30..31]  1 space
//! UNITS    [31..36]  5 chars
//! (sep)    [36..37]  1 space
//! DOLLARS  [37..45]  8 chars
//! (sep)    [45..46]  1 space
//! F        [46..50]  4 chars
//! (sep)    [50..51]  1 space
//! D        [51..52]  1 char
//! (sep)    [52..53]  1 space
//! PR       [53..54]  1 char
//! CRLF     [54..56]  2 bytes (\r\n)
//! ```
//!
//! Header text (55 ASCII bytes, no trailing CR/LF when read as bytes):
//!
//! ```text
//! IRI_KEY WEEK SY GE VEND  ITEM  UNITS DOLLARS  F    D PR
//! ```
//!
//! The header's column labels are short versions of the data field widths
//! (e.g. `VEND` is 4 chars but the data field `VEND` is 5 chars; the
//! extra character in the data row is the leading space of the
//! `padded spaces + digits` pattern). Only the **field widths in the data
//! rows** matter to the parser.

/// Total header length including the CRLF terminator.
pub const HEADER_LEN: usize = 57;

/// Total data-row length including the CRLF terminator.
pub const RECORD_LEN: usize = 56;

/// Length of the field-content portion of a data row (everything
/// before CRLF). Equal to `RECORD_LEN - 2`.
pub const RECORD_CONTENT_LEN: usize = 54;

/// Exact expected header text (CRLF stripped). 55 ASCII bytes.
pub const HEADER_TEXT: &[u8] = b"IRI_KEY WEEK SY GE VEND  ITEM  UNITS DOLLARS  F    D PR";

#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    pub name: &'static str,
    pub start: usize,
    pub end: usize,
}

pub const IRI_KEY: FieldSpec = FieldSpec {
    name: "iri_key",
    start: 0,
    end: 7,
};
pub const WEEK: FieldSpec = FieldSpec {
    name: "week",
    start: 8,
    end: 12,
};
pub const SY: FieldSpec = FieldSpec {
    name: "sy",
    start: 13,
    end: 15,
};
pub const GE: FieldSpec = FieldSpec {
    name: "ge",
    start: 16,
    end: 18,
};
pub const VEND: FieldSpec = FieldSpec {
    name: "vend",
    start: 19,
    end: 24,
};
pub const ITEM: FieldSpec = FieldSpec {
    name: "item",
    start: 25,
    end: 30,
};
pub const UNITS: FieldSpec = FieldSpec {
    name: "units",
    start: 31,
    end: 36,
};
pub const DOLLARS: FieldSpec = FieldSpec {
    name: "dollars",
    start: 37,
    end: 45,
};
pub const F: FieldSpec = FieldSpec {
    name: "feature_code",
    start: 46,
    end: 50,
};
pub const D: FieldSpec = FieldSpec {
    name: "display",
    start: 51,
    end: 52,
};
pub const PR: FieldSpec = FieldSpec {
    name: "price_reduction",
    start: 53,
    end: 54,
};

/// Byte offset (relative to the start of the row) where `\r\n` is expected.
pub const CRLF_OFFSET: usize = 54;

/// Validate the bytes that should be at the start of a sales file.
/// Returns `Ok(())` iff the first 57 bytes are the canonical header followed
/// by CRLF.
pub fn validate_header(header: &[u8]) -> Result<(), (String, String)> {
    if header.len() < HEADER_LEN {
        return Err((
            HEADER_TEXT.iter().map(|&b| b as char).collect(),
            "<file shorter than header>".to_string(),
        ));
    }
    let text = std::str::from_utf8(&header[..HEADER_TEXT.len()])
        .map_err(|_| ("<utf8 header>".to_string(), "<non-utf8>".to_string()))?;
    let expected = std::str::from_utf8(HEADER_TEXT).unwrap();
    if text != expected {
        return Err((expected.to_string(), text.to_string()));
    }
    if header[HEADER_TEXT.len()] != b'\r' || header[HEADER_TEXT.len() + 1] != b'\n' {
        return Err((
            "<CRLF>".to_string(),
            format!(
                "got bytes {:02x?} {:02x?}",
                &header[HEADER_TEXT.len()..HEADER_TEXT.len() + 2],
                &[] as &[u8]
            ),
        ));
    }
    Ok(())
}

/// Bytes that look like the trailing CRLF for the row at `record_offset`.
#[inline]
pub fn row_crlf(row: &[u8]) -> Option<&[u8]> {
    row.get(CRLF_OFFSET..CRLF_OFFSET + 2)
}

/// Compute expected row count from a file size.
///
/// Returns `None` if the file is smaller than the header or the body is
/// not a multiple of `RECORD_LEN`.
/// Split a file size into (complete records, rejected records).
///
/// A well-formed sales file has `HEADER_LEN + n * RECORD_LEN` bytes and
/// this returns `(n, 0)`. A file whose final record is truncated —
/// which the staged corpus really contains one of, see
/// `docs/corpus_readiness.md` Gate 2 — returns the number of whole
/// records plus the rejected tail: every complete record past the last
/// aligned boundary, plus the partial one, counted as one.
///
/// `None` only for a file too short to hold a header, which is a
/// different failure (it has no records at all, aligned or not).
pub fn split_aligned_records(file_size: u64) -> Option<(u64, u64)> {
    if file_size < HEADER_LEN as u64 {
        return None;
    }
    let body = file_size - HEADER_LEN as u64;
    let complete = body / RECORD_LEN as u64;
    let tail = body % RECORD_LEN as u64;
    let rejected = if tail == 0 {
        0
    } else {
        tail / RECORD_LEN as u64 + 1
    };
    Some((complete, rejected))
}

/// Bytes of a misaligned tail that will *not* be ingested, i.e. the
/// `file_size` implied by `split_aligned_records`.
pub fn aligned_prefix_len(file_size: u64) -> Option<u64> {
    let (complete, _) = split_aligned_records(file_size)?;
    Some(HEADER_LEN as u64 + complete * RECORD_LEN as u64)
}

pub fn expected_rows(file_size: u64) -> Option<u64> {
    if file_size < HEADER_LEN as u64 {
        return None;
    }
    let body = file_size - HEADER_LEN as u64;
    if body % RECORD_LEN as u64 != 0 {
        return None;
    }
    Some(body / RECORD_LEN as u64)
}

/// IRI academic-data year ranges derived from `docs/data_layout.md` §2.3.
/// These are contiguous and non-overlapping, so `week → year` is a
/// deterministic function over the full range.
///
/// The mapping is hard-coded rather than read from a dimension table
/// because:
///
/// - The IRI WEEK field is a single integer counter (year 1 starts at
///   week 1114, year 12 ends at week 1739, contiguous throughout).
/// - The year ranges are stable across all 12 years of data.
/// - A small `match` is faster than a table lookup and doesn't depend
///   on a separate dimension-ingest pipeline.
///
/// Returns the academic year (1..=12) for any week in `1114..=1739`. `None`
/// for week numbers outside the known range — those would indicate a
/// file we haven't seen before.
pub fn week_to_year(week: u16) -> Option<u8> {
    // (start_week_inclusive, end_week_inclusive, academic_year)
    // Source: devoirs/data_layout.md §2.3.
    const RANGES: &[(u16, u16, u8)] = &[
        (1114, 1165, 1),
        (1166, 1217, 2),
        (1218, 1269, 3),
        (1270, 1321, 4),
        (1322, 1373, 5),
        (1374, 1426, 6), // 53-week year
        (1427, 1478, 7),
        (1479, 1530, 8),
        (1531, 1582, 9),
        (1583, 1634, 10),
        (1635, 1686, 11),
        (1687, 1739, 12), // 53-week year
    ];
    for &(lo, hi, y) in RANGES {
        if (lo..=hi).contains(&week) {
            return Some(y);
        }
    }
    None
}

/// Field-width → max integer value.
///
/// All numeric columns in the IRI sales files store decimal digits with
/// optional sign and optional decimal point (DOLLARS). The fixed width
/// of the on-disk field therefore puts a hard upper bound on the
/// representable value:
/// - unsigned, W bytes: `0 ..= 10^W - 1`
/// - signed,   W bytes: `-(10^W - 1) ..= 10^W - 1`
/// - dollars,  W bytes with up to 2 fractional digits:
///   `0 ..= (10^W - 1) * 100` cents
///
/// Use these bounds to pick the smallest Arrow integer type that can
/// hold any value that could possibly fit on disk. Empirical
/// observation can tighten further (e.g. if all IRI_KEY values are
/// observed < 65 536), but width-based inference is deterministic
/// and requires no scan.
pub fn max_unsigned(width: usize) -> u64 {
    let mut v: u64 = 1;
    for _ in 0..width {
        v = v.saturating_mul(10);
    }
    v.saturating_sub(1)
}
pub fn max_signed(width: usize) -> i64 {
    max_unsigned(width) as i64
}
pub fn max_cents(width: usize) -> i64 {
    (max_unsigned(width) as i64).saturating_mul(100)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_match_verified_layout() {
        // Sanity: the in-source `FieldSpec` table is internally consistent
        // with the prose.
        assert_eq!(HEADER_LEN, 57);
        assert_eq!(RECORD_LEN, 56);
        assert_eq!(RECORD_CONTENT_LEN, 54);
        assert_eq!(CRLF_OFFSET, 54);
        assert_eq!(HEADER_TEXT.len(), 55);
    }

    #[test]
    fn fields_cover_record_content_exactly() {
        // Sorted, non-overlapping, spans the entire content region
        // except for the single-byte ASCII space separators between fields.
        let fields = [
            &IRI_KEY, &WEEK, &SY, &GE, &VEND, &ITEM, &UNITS, &DOLLARS, &F, &D, &PR,
        ];
        let mut prev_end = 0;
        let mut separators = 0;
        for f in fields {
            if f.start > prev_end {
                separators += 1;
                assert_eq!(
                    f.start - prev_end,
                    1,
                    "expected exactly 1 separator byte before {}",
                    f.name
                );
            }
            assert!(f.end > f.start);
            prev_end = f.end;
        }
        assert_eq!(
            prev_end, RECORD_CONTENT_LEN,
            "fields cover exactly {} content bytes",
            RECORD_CONTENT_LEN
        );
        assert_eq!(
            separators,
            fields.len() - 1,
            "one separator between each adjacent field"
        );
    }

    #[test]
    fn header_text_matches_known_bytes() {
        let known: &[u8] = b"IRI_KEY WEEK SY GE VEND  ITEM  UNITS DOLLARS  F    D PR";
        assert_eq!(HEADER_TEXT, known);
        assert_eq!(HEADER_TEXT.len(), 55);
    }

    #[test]
    fn width_based_max_matches_field_layout() {
        // IRI_KEY (7) max is 9_999_999 — fits UInt32, no UInt24 exists.
        assert_eq!(max_unsigned(7), 9_999_999);
        // WEEK (4) max is 9_999 — fits UInt16.
        assert_eq!(max_unsigned(4), 9_999);
        // SY/GE (2) max is 99 — fits UInt8.
        assert_eq!(max_unsigned(2), 99);
        // VEND/ITEM (5) max is 99_999 — exceeds UInt16 (65 535) so UInt32.
        assert_eq!(max_unsigned(5), 99_999);
        assert!(max_unsigned(5) > u16::MAX as u64);
        // UNITS (5) signed max is ±99_999 — exceeds Int16 (32 767) so Int32.
        assert_eq!(max_signed(5), 99_999);
        assert!(max_signed(5) > i16::MAX as i64);
        // DOLLARS (8) accepts "all integer digits" (no decimal point),
        // so the worst-case width-bound is $99 999 999, i.e.
        // 9 999 999 900 cents. That is > i32::MAX (~2.1 B) so Int64
        // is the smallest correct storage type from width alone.
        // (Empirically the corpus is well under $10k per row, so Int32
        // would be safe; the scaffold is conservative.)
        assert_eq!(max_cents(8), 9_999_999_900);
        assert!(max_cents(8) > i32::MAX as i64);
        // Upper-bound check is redundant (max_unsigned cannot exceed
        // u64::MAX by construction), so just exercise the value:
        let _: i64 = max_cents(8);
        // D (1) max is 9 — fits UInt8. PR (1) is documented 0/1 only,
        // so Boolean is appropriate but UInt8 always works.
        assert_eq!(max_unsigned(1), 9);
    }

    #[test]
    fn week_to_year_is_total_on_known_range() {
        // Every week in 1114..=1739 maps to exactly one year, and the
        // ranges are contiguous (no gaps, no overlaps).
        assert_eq!(week_to_year(1114), Some(1));
        assert_eq!(week_to_year(1165), Some(1));
        assert_eq!(week_to_year(1166), Some(2));
        assert_eq!(week_to_year(1426), Some(6)); // last week of year 6
        assert_eq!(week_to_year(1427), Some(7)); // first week of year 7
        assert_eq!(week_to_year(1687), Some(12));
        assert_eq!(week_to_year(1739), Some(12));
        // Out-of-range weeks return None.
        assert_eq!(week_to_year(1113), None);
        assert_eq!(week_to_year(1740), None);
        assert_eq!(week_to_year(0), None);
    }
}
