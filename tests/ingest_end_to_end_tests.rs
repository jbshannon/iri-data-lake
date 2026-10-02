//! End-to-end tests for the multi-file ingest path. We don't run
//! `ingest-all` through the CLI here — the multi-file logic is tested
//! at the library level via direct discovery + per-file ingest loops
//! that match the CLI's behaviour.

mod common;

use iri_lake::config::IngestConfig;
use iri_lake::discovery::discover;
use iri_lake::ingest::{ingest_file, IngestFilter, IngestOutcome};

#[test]
fn largest_first_processes_multiple_files() {
    let tmp = tempfile::tempdir().unwrap();
    // Three fixtures of different sizes — default_rows(n) scales the body.
    let p_small = common::write_sales_fixture(
        tmp.path(),
        1,
        "beer",
        "drug",
        (1114, 1165),
        &common::default_rows(4),
    );
    let _p_med = common::write_sales_fixture(
        tmp.path(),
        1,
        "coffee",
        "drug",
        (1114, 1165),
        &common::default_rows(32),
    );
    let p_large = common::write_sales_fixture(
        tmp.path(),
        1,
        "deod",
        "drug",
        (1114, 1165),
        &common::default_rows(128),
    );

    let inv = discover(tmp.path()).unwrap();
    let mut files = inv.files.clone();
    files.sort_by_key(|a| std::cmp::Reverse(a.size_bytes));
    assert_eq!(files[0].size_bytes, p_large.metadata().unwrap().len());
    assert_eq!(files[2].size_bytes, p_small.metadata().unwrap().len());

    let lake = tmp.path().join("lake");
    let cfg = IngestConfig::for_test();
    for f in &files {
        let _ = ingest_file(
            &f.identity.path,
            tmp.path(),
            &lake,
            &cfg,
            &IngestFilter::default(),
        )
        .unwrap();
    }

    // The manifest should have all three.
    let manifest_path = lake.join("metadata").join("manifest.jsonl");
    let s = std::fs::read_to_string(&manifest_path).unwrap();
    let count = s.lines().filter(|l| !l.trim().is_empty()).count();
    assert_eq!(count, 3);
}

#[test]
fn filtering_by_year_selects_only_that_year() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_sales_fixture(
        tmp.path(),
        1,
        "beer",
        "drug",
        (1114, 1165),
        &common::default_rows(8),
    );
    common::write_sales_fixture(
        tmp.path(),
        2,
        "beer",
        "drug",
        (1166, 1217),
        &common::default_rows(8),
    );
    let inv = discover(tmp.path()).unwrap();
    let year1: Vec<_> = inv
        .files
        .iter()
        .filter(|f| f.identity.year == 1)
        .cloned()
        .collect();
    let year2: Vec<_> = inv
        .files
        .iter()
        .filter(|f| f.identity.year == 2)
        .cloned()
        .collect();
    assert_eq!(year1.len(), 1);
    assert_eq!(year2.len(), 1);
}

#[test]
fn manifest_records_carry_inferred_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::write_sales_fixture(
        tmp.path(),
        3,
        "carbbev",
        "groc",
        (1218, 1269),
        &common::default_rows(8),
    );
    let lake = tmp.path().join("lake");
    let cfg = IngestConfig::for_test();
    let outcome = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    let stats = match outcome {
        IngestOutcome::Completed(_, s) => s,
        _ => panic!("expected Completed"),
    };
    assert_eq!(stats.written_rows, 8);
    let manifest_path = lake.join("metadata").join("manifest.jsonl");
    let s = std::fs::read_to_string(&manifest_path).unwrap();
    let rec: serde_json::Value = serde_json::from_str(s.lines().next().unwrap()).unwrap();
    assert_eq!(rec["source_year"], 3);
    assert_eq!(rec["category"], "carbbev");
    assert_eq!(rec["channel"], "groc");
    assert_eq!(rec["filename_week_start"], 1218);
    assert_eq!(rec["filename_week_end"], 1269);
    assert_eq!(rec["status"], "success");
}
