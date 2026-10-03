//! Trailing-defect policy: a file that ends inside its last line's CRLF
//! is a warning, not a data loss; a file whose last record is missing
//! *field* bytes is a real loss and stays visible as one.
//!
//! The distinction is drawn once, in `fixed_width::classify_trailing`,
//! and these tests pin what each consumer does with it — the validator's
//! exit status, the ingest log level, and what actually reaches the lake.
//!
//! `Year12/soup/soup_groc_1687_1739` in the staged corpus is the real
//! terminator-only case; `tests/common::misaligned_fixture` is the
//! data-losing case.

mod common;

use iri_lake::config::IngestConfig;
use iri_lake::fixed_width::{self, TrailingDefect};
use iri_lake::ingest::{ingest_file, IngestOutcome};
use iri_lake::validation::validate_file;

fn cfg() -> IngestConfig {
    IngestConfig::for_test()
}

// ---------------------------------------------------------------- validator

#[test]
fn validator_passes_a_terminator_only_defect_with_a_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::terminator_only_fixture(tmp.path());

    let r = validate_file(&p, tmp.path(), usize::MAX).unwrap();

    // The defect is recorded, not hidden...
    assert!(!r.record_aligned, "size is not a multiple of RECORD_LEN");
    assert_eq!(r.trailing, TrailingDefect::TerminatorOnly { missing: 1 });
    assert!(!r.is_aligned());
    // ...but it does not fail the file, because no field is affected.
    assert!(r.is_ok(), "terminator-only defect must not fail validation");
    let note = r.note.expect("a benign defect should carry a note");
    assert!(note.contains("no field affected"), "note was: {note}");

    // And the complete records are counted and checked — not skipped.
    assert_eq!(r.expected_rows, 2);
    assert_eq!(r.sample_size, 2);
    assert_eq!(r.sample_passed, 2, "both whole records must be checked");
}

#[test]
fn validator_fails_when_the_last_record_is_missing_data_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::misaligned_fixture(tmp.path()); // 1 whole row + b"X"

    let r = validate_file(&p, tmp.path(), usize::MAX).unwrap();

    assert!(!r.record_aligned);
    assert!(
        matches!(
            r.trailing,
            TrailingDefect::TruncatedRecord { content_bytes: 1 }
        ),
        "expected a data-losing defect, got {:?}",
        r.trailing
    );
    assert!(r.trailing.loses_data());
    assert!(
        !r.is_ok(),
        "a record missing field bytes must fail validation"
    );
    assert!(r.note.unwrap().contains("error:"));
}

#[test]
fn validator_still_fails_a_bad_header_regardless_of_alignment() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::bad_header_fixture(tmp.path());
    let r = validate_file(&p, tmp.path(), usize::MAX).unwrap();
    assert!(!r.header_matches);
    assert!(!r.is_ok());
}

// -------------------------------------------------------------------- ingest

#[test]
fn ingest_writes_every_field_of_a_terminator_only_file() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::terminator_only_fixture(tmp.path());
    let lake = tmp.path().join("lake");

    let outcome = ingest_file(&p, tmp.path(), &lake, &cfg(), &Default::default()).unwrap();
    let stats = match outcome {
        IngestOutcome::Completed(_, s) => s,
        IngestOutcome::Skipped(_) => panic!("fresh ingest should complete"),
    };

    assert_eq!(stats.written_rows, 2, "both whole records land");
    assert_eq!(
        stats.rejected_rows, 1,
        "the unterminated row is not written"
    );
    assert!(
        !stats.rejected_loses_data,
        "a terminator-only defect loses no field, so it must not be \
         reported as a data loss"
    );
    assert!(stats.output_paths[0].exists());
}

#[test]
fn ingest_reports_a_data_losing_defect_as_a_loss() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::misaligned_fixture(tmp.path());
    let lake = tmp.path().join("lake");

    let outcome = ingest_file(&p, tmp.path(), &lake, &cfg(), &Default::default()).unwrap();
    let stats = outcome.stats().expect("completed");

    assert_eq!(stats.written_rows, 1);
    assert_eq!(stats.rejected_rows, 1);
    assert!(
        stats.rejected_loses_data,
        "a record missing content bytes must be flagged as a real loss"
    );
}

// --------------------------------------------------------------- the shape

#[test]
fn the_corpus_soup_file_is_a_terminator_only_defect() {
    // Pin the classification of the one real corpus file this policy is
    // about, by size alone: 637 922 152 bytes.
    //
    // Deliberately a pure arithmetic check on the known size rather than
    // reading 608 MB: the point is that this exact length classifies as
    // benign, so a future change to `classify_trailing` cannot silently
    // downgrade (or upgrade) how the corpus's one damaged file is treated.
    let size = 637_922_152u64;
    let a = fixed_width::classify_trailing(size).expect("sized file");
    assert_eq!(
        a.defect,
        TrailingDefect::TerminatorOnly { missing: 1 },
        "the soup file's final record is short one byte of terminator"
    );
    assert_eq!(a.complete_rows, 11_391_465);
    assert_eq!(a.rejected_rows, 1);
    assert!(a.trailing_is_benign());
}
