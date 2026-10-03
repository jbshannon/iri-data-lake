//! `validate` command: confirm a single file matches the expected
//! fixed-width layout and report inferred identity / sizes without
//! performing a full ingest.

use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

use crate::discovery::parse_identity;
use crate::errors::{IngestError, Result};
use crate::fixed_width::{self, TrailingDefect, HEADER_LEN, RECORD_LEN};
use crate::model::SourceIdentity;

/// Result of validating a single file.
#[derive(Debug)]
pub struct ValidationReport {
    pub identity: SourceIdentity,
    pub size_bytes: u64,
    pub expected_rows: u64,
    pub header_matches: bool,
    /// Whether the file size is an exact multiple of the record length.
    ///
    /// A `false` here is not on its own a failure — see
    /// [`TrailingDefect`]. A file whose last record is missing only its
    /// CRLF terminator has lost no field value, and
    /// [`ValidationReport::is_ok`] says so.
    pub record_aligned: bool,
    pub sample_size: usize,
    pub sample_passed: usize,
    /// What is wrong with the bytes after the last whole record.
    pub trailing: TrailingDefect,
    /// Human-readable note for a benign trailing defect, else `None`.
    pub note: Option<String>,
}

impl ValidationReport {
    /// Whether the file is fit to ingest.
    ///
    /// Benign trailing defects pass. A truncated *record* does not: some
    /// of its fields are missing, so the file is reporting a data loss
    /// the operator needs to see as a failure.
    pub fn is_ok(&self) -> bool {
        self.header_matches && !self.trailing.loses_data() && self.sample_passed == self.sample_size
    }

    /// True when the file ends on a whole record.
    pub fn is_aligned(&self) -> bool {
        self.trailing == TrailingDefect::None
    }
}

/// Validate `path`. `sample` controls how many data rows to spot-check
/// after the header and alignment checks; pass `usize::MAX` for full
/// validation (`--full` flag in the CLI).
pub fn validate_file(
    path: &Path,
    input_root: &Path,
    sample: usize,
) -> Result<ValidationReport, IngestError> {
    let identity = parse_identity(path, input_root).map_err(|_| {
        IngestError::Discovery(format!(
            "file does not match eligible sales filename shape: {}",
            path.display()
        ))
    })?;

    let metadata = std::fs::metadata(path).map_err(|e| IngestError::io(path, e))?;
    let size = metadata.len();

    let mut file = File::open(path).map_err(|e| IngestError::io(path, e))?;
    let mut header = vec![0u8; HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| IngestError::io(path, e))?;
    let header_matches = fixed_width::validate_header(&header).is_ok();

    // Classify the tail rather than merely counting whole records. The
    // count that matters is the number of records we can actually check,
    // and for a file with a truncated terminator that is every record in
    // the file — the previous code reported `expected_rows = 0` for the
    // one real corpus file with this defect, and therefore sampled none
    // of its 11.4 million records.
    let alignment = fixed_width::classify_trailing(size);
    let expected_rows = alignment.map(|a| a.complete_rows).unwrap_or(0);
    let record_aligned = alignment.map(|a| a.is_aligned()).unwrap_or(false);
    let trailing = alignment
        .map(|a| a.defect)
        .unwrap_or(TrailingDefect::TruncatedRecord { content_bytes: 0 });
    let note = match trailing {
        TrailingDefect::None => None,
        defect if defect.is_benign() => Some(format!(
            "warning: {} — all {expected_rows} records and every field are intact; \
             ingest will write the {expected_rows} whole records",
            defect
        )),
        defect => Some(format!("error: {defect}")),
    };

    // Spot-check rows. Each row is RECORD_LEN bytes including CRLF.
    let sample_size = sample.min(expected_rows as usize);
    let mut sample_passed = 0usize;
    let mut body = vec![0u8; RECORD_LEN];
    let body_offset = HEADER_LEN as u64;
    let _ = file
        .seek(std::io::SeekFrom::Start(body_offset))
        .map_err(|e| IngestError::io(path, e))?;
    for i in 0..sample_size {
        file.read_exact(&mut body)
            .map_err(|e| IngestError::io(path, e))?;
        if fixed_width::row_crlf(&body) == Some(b"\r\n") {
            sample_passed += 1;
        }
        let _ = i;
    }

    Ok(ValidationReport {
        identity,
        size_bytes: size,
        expected_rows,
        header_matches,
        record_aligned,
        sample_size,
        sample_passed,
        trailing,
        note,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed_width::RECORD_LEN;
    use std::io::Write;

    fn write_valid_file(dir: &Path) -> std::path::PathBuf {
        let p = dir.join("Year1").join("beer").join("beer_drug_1114_1165");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let mut f = File::create(&p).unwrap();
        // Header
        f.write_all(fixed_width::HEADER_TEXT).unwrap();
        f.write_all(b"\r\n").unwrap();
        // Two data rows
        for _ in 0..2 {
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
            row.extend_from_slice(b"\r\n");
            assert_eq!(row.len(), RECORD_LEN);
            f.write_all(&row).unwrap();
        }
        p
    }

    #[test]
    fn validates_correct_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_valid_file(dir.path());
        let root = dir.path();
        let r = validate_file(&path, root, 2).unwrap();
        assert!(r.header_matches);
        assert!(r.record_aligned);
        assert_eq!(r.expected_rows, 2);
        assert_eq!(r.sample_passed, 2);
        assert_eq!(r.identity.year, 1);
        assert_eq!(r.identity.category, "beer");
    }

    #[test]
    fn detects_misaligned_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir
            .path()
            .join("Year1")
            .join("beer")
            .join("beer_drug_1114_1165");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let mut f = File::create(&p).unwrap();
        f.write_all(fixed_width::HEADER_TEXT).unwrap();
        f.write_all(b"\r\n").unwrap();
        // 1 row + 1 trailing byte (misalignment)
        f.write_all(&[b'0'; RECORD_LEN + 1]).unwrap();
        let root = dir.path();
        let r = validate_file(&p, root, 1).unwrap();
        assert!(!r.record_aligned);
    }
}
