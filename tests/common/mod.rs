//! Shared helpers for the integration test suite.
//!
//! Every integration test starts by writing one or more fixture files
//! into a fresh tempdir, then runs assertions on them. The fixtures
//! are *real binary files* with CRLF line endings and the exact byte
//! layout the parser expects. We never bake pre-built binaries into
//! the repo: tests stay self-contained and deterministic.
//
// The fixture builders are referenced from individual integration
// tests in tests/*.rs; `cargo test --lib` (which compiles only the
// library unit tests) sees them as unused. Suppress the warning.
#![allow(dead_code)]

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use iri_lake::fixed_width::{HEADER_LEN, HEADER_TEXT, RECORD_LEN};

/// A single data row builder for tests.
pub struct RowBuilder {
    bytes: Vec<u8>,
}

impl RowBuilder {
    pub fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(RECORD_LEN),
        }
    }

    pub fn iri_key(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 0, 7, s);
        self
    }
    pub fn week(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 8, 12, s);
        self
    }
    pub fn sy(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 13, 15, s);
        self
    }
    pub fn ge(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 16, 18, s);
        self
    }
    pub fn vend(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 19, 24, s);
        self
    }
    pub fn item(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 25, 30, s);
        self
    }
    pub fn units(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 31, 36, s);
        self
    }
    pub fn dollars(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 37, 45, s);
        self
    }
    pub fn feature(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 46, 50, s);
        self
    }
    pub fn display(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 51, 52, s);
        self
    }
    pub fn price_reduction(mut self, s: &str) -> Self {
        write_padded(&mut self.bytes, 53, 54, s);
        self
    }

    /// Append the trailing CRLF and finalize the row.
    pub fn finish(mut self) -> Vec<u8> {
        self.bytes.extend_from_slice(b"\r\n");
        assert_eq!(
            self.bytes.len(),
            RECORD_LEN,
            "row length mismatch: got {}, expected {}",
            self.bytes.len(),
            RECORD_LEN
        );
        self.bytes
    }
}

fn write_padded(buf: &mut Vec<u8>, start: usize, end: usize, s: &str) {
    let width = end - start;
    assert!(
        s.len() <= width,
        "field too long: {} into width {}",
        s,
        width
    );
    let pad = width - s.len();
    if buf.len() < start {
        buf.resize(start, b' ');
    }
    buf.extend_from_slice(s.as_bytes());
    buf.extend(std::iter::repeat(b' ').take(pad));
}

/// Write `header + n_rows` rows under `dir/<year>/<cat>/<cat>_<chan>_<w1>_<w2>`.
pub fn write_sales_fixture(
    dir: &Path,
    year: u8,
    category: &str,
    channel: &str,
    weeks: (u16, u16),
    rows: &[Vec<u8>],
) -> PathBuf {
    let path = dir
        .join(format!("Year{}", year))
        .join(category)
        .join(format!("{}_{}_{}_{}", category, channel, weeks.0, weeks.1));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = File::create(&path).unwrap();
    f.write_all(HEADER_TEXT).unwrap();
    f.write_all(b"\r\n").unwrap();
    assert_eq!(HEADER_TEXT.len() + 2, HEADER_LEN, "header layout invariant");
    for row in rows {
        assert_eq!(row.len(), RECORD_LEN, "row layout invariant");
        f.write_all(row).unwrap();
    }
    path
}

/// Convenience: build N default rows for tests that just need *some*
/// records. Each row has the same canned values, with the unit counter
/// and price varying per index so values aren't all identical.
pub fn default_rows(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            RowBuilder::new()
                .iri_key(&format!("{:>7}", 1_000_000 + (i % 100)))
                .week(&format!("{:>4}", 1114 + (i / 100)))
                .sy(" 0")
                .ge(" 2")
                .vend("18200")
                .item(&format!("{:>5}", 647 + (i % 50)))
                .units(&format!("{:>5}", 1 + (i % 9)))
                .dollars(&format!("{:>8}", format!("{:.2}", (i as f64) * 0.99)))
                .feature(if i % 3 == 0 { "A   " } else { "NONE" })
                .display("0")
                .price_reduction(if i % 2 == 0 { "0" } else { "1" })
                .finish()
        })
        .collect()
}

/// Build a default fixture under `tmp` returning its path.
pub fn fixture_path(tmp: &Path) -> PathBuf {
    let rows = default_rows(64);
    write_sales_fixture(tmp, 1, "beer", "drug", (1114, 1165), &rows)
}

/// `tmp/` -> `Year12/toothpa/toothpa/<file>`. Exercises the nested case.
pub fn nested_year12_fixture(tmp: &Path, rows: usize) -> PathBuf {
    let rows = default_rows(rows);
    let path = tmp
        .join("Year12")
        .join("toothpa")
        .join("toothpa")
        .join("toothpa_drug_1687_1739");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = File::create(&path).unwrap();
    f.write_all(HEADER_TEXT).unwrap();
    f.write_all(b"\r\n").unwrap();
    for r in &rows {
        f.write_all(r).unwrap();
    }
    path
}

/// Write a fixture with a malformed DOLLARS field (alpha chars).
pub fn malformed_dollars_fixture(tmp: &Path) -> PathBuf {
    let path = tmp.join("Year1").join("beer").join("beer_drug_1114_1165");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = File::create(&path).unwrap();
    f.write_all(HEADER_TEXT).unwrap();
    f.write_all(b"\r\n").unwrap();
    let mut row = RowBuilder::new()
        .iri_key("1234567")
        .week("1114")
        .sy(" 0")
        .ge(" 2")
        .vend("18200")
        .item("  647")
        .units("    1")
        .feature("NONE")
        .display("0")
        .price_reduction("0")
        .finish();
    // Corrupt the DOLLARS field (37..45).
    for b in row[37..45].iter_mut() {
        *b = b'X';
    }
    f.write_all(&row).unwrap();
    path
}

/// Write a fixture with an unknown feature code.
pub fn unknown_feature_fixture(tmp: &Path) -> PathBuf {
    let path = tmp.join("Year1").join("beer").join("beer_drug_1114_1165");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = File::create(&path).unwrap();
    f.write_all(HEADER_TEXT).unwrap();
    f.write_all(b"\r\n").unwrap();
    let row = RowBuilder::new()
        .iri_key("1234567")
        .week("1114")
        .sy(" 0")
        .ge(" 2")
        .vend("18200")
        .item("  647")
        .units("    1")
        .dollars("   9.29")
        .feature("ZZZZ") // not in {NONE, A, A+, B, C}
        .display("0")
        .price_reduction("0")
        .finish();
    f.write_all(&row).unwrap();
    path
}

/// Fixture with a corrupt header text.
pub fn bad_header_fixture(tmp: &Path) -> PathBuf {
    let path = tmp.join("Year1").join("beer").join("beer_drug_1114_1165");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = File::create(&path).unwrap();
    // Same length as the canonical header (55 chars) but wrong content,
    // so alignment is preserved and the header check trips.
    f.write_all(b"XXXXXXX WEEK SY GE VEND  ITEM  UNITS DOLLARS  F    D PR\r\n")
        .unwrap();
    // Pad with a valid row so size is plausible.
    let row = RowBuilder::new()
        .iri_key("1234567")
        .week("1114")
        .sy(" 0")
        .ge(" 2")
        .vend("18200")
        .item("  647")
        .units("    1")
        .dollars("    9.29")
        .feature("NONE")
        .display("0")
        .price_reduction("0")
        .finish();
    f.write_all(&row).unwrap();
    path
}

/// Fixture whose body is mis-aligned (size not divisible by RECORD_LEN).
pub fn misaligned_fixture(tmp: &Path) -> PathBuf {
    let path = tmp.join("Year1").join("beer").join("beer_drug_1114_1165");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut f = File::create(&path).unwrap();
    f.write_all(HEADER_TEXT).unwrap();
    f.write_all(b"\r\n").unwrap();
    // Write one row plus one trailing byte.
    let row = RowBuilder::new()
        .iri_key("1234567")
        .week("1114")
        .sy(" 0")
        .ge(" 2")
        .vend("18200")
        .item("  647")
        .units("    1")
        .dollars("   9.29")
        .feature("NONE")
        .display("0")
        .price_reduction("0")
        .finish();
    f.write_all(&row).unwrap();
    f.write_all(b"X").unwrap();
    path
}
