//! Manifest / resume tests.

mod common;

use std::path::Path;

use chrono::Utc;
use iri_lake::config::IngestConfig;
use iri_lake::ingest::{ingest_file, IngestFilter, IngestOutcome};
use iri_lake::manifest::{skip_decision, JsonlManifest, ManifestStore};
use iri_lake::model::{
    Channel, ManifestRecord, ManifestStatus, CURRENT_SCHEMA_VERSION, PARSER_VERSION,
};

fn fresh_record(path: &Path) -> ManifestRecord {
    ManifestRecord {
        run_id: "test".into(),
        source_path: path.to_path_buf(),
        source_size_bytes: 100,
        source_sha256: "abc".into(),
        source_year: 1,
        category: "beer".into(),
        channel: Channel::Drug,
        filename_week_start: 1,
        filename_week_end: 52,
        expected_rows: 10,
        written_rows: 10,
        rejected_rows: 0,
        parser_version: PARSER_VERSION.into(),
        output_schema_version: CURRENT_SCHEMA_VERSION,
        compression: "zstd".into(),
        batch_rows: 1_000_000,
        row_group_rows: 4_000_000,
        output_paths: vec![],
        output_size_bytes: 0,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        duration_ms: Some(1),
        status: ManifestStatus::Success,
        error_message: None,
    }
}

#[test]
fn jsonl_manifest_round_trips_records() {
    let tmp = tempfile::tempdir().unwrap();
    let store = JsonlManifest::open(tmp.path()).unwrap();
    let r = fresh_record(&tmp.path().join("src"));
    store.append(&r).unwrap();
    store.append(&r).unwrap();
    let last = store.last_for_path(&r.source_path).unwrap().unwrap();
    assert_eq!(last.source_path, r.source_path);
    assert_eq!(last.run_id, r.run_id);
    assert_eq!(store.all().unwrap().len(), 2);
}

#[test]
fn skip_decision_requires_success_status() {
    let r = fresh_record(Path::new("/x"));
    let mut failed = r.clone();
    failed.status = ManifestStatus::Failed;
    assert!(skip_decision(&r, Some(&failed)).unwrap().is_none());
}

#[test]
fn skip_decision_requires_matching_schema_version() {
    let r = fresh_record(Path::new("/x"));
    let mut bad = r.clone();
    bad.output_schema_version = CURRENT_SCHEMA_VERSION.wrapping_sub(1);
    assert!(skip_decision(&r, Some(&bad)).unwrap().is_none());
}

#[test]
fn skip_decision_requires_matching_sha() {
    let r = fresh_record(Path::new("/x"));
    let mut bad = r.clone();
    bad.source_sha256 = "different".into();
    assert!(skip_decision(&r, Some(&bad)).unwrap().is_none());
}

#[test]
fn skip_decision_requires_existing_outputs() {
    let tmp = tempfile::tempdir().unwrap();
    let r = fresh_record(&tmp.path().join("src"));
    let mut with_missing_outputs = r.clone();
    with_missing_outputs.output_paths = vec![tmp.path().join("does_not_exist.parquet")];
    assert!(skip_decision(&r, Some(&with_missing_outputs))
        .unwrap()
        .is_none());
}

#[test]
fn skip_decision_accepts_matching_success() {
    let tmp = tempfile::tempdir().unwrap();
    let output = tmp.path().join("kept.parquet");
    std::fs::write(&output, b"x").unwrap();
    let r = fresh_record(&tmp.path().join("src"));
    let mut ok = r.clone();
    ok.output_paths = vec![output];
    assert!(skip_decision(&r, Some(&ok)).unwrap().is_some());
}

#[test]
fn second_ingest_skips_via_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let lake = tmp.path().join("lake");
    let cfg = IngestConfig::for_test();
    let first = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    assert!(matches!(first, IngestOutcome::Completed(_, _)));
    let second = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    match second {
        IngestOutcome::Skipped(r) => {
            assert_eq!(r.status, ManifestStatus::Success);
            assert!(!r.output_paths.is_empty());
        }
        other => panic!("expected Skipped, got {:?}", other),
    }
}

#[test]
fn changed_config_forces_re_ingest() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let lake = tmp.path().join("lake");
    let mut cfg = IngestConfig::for_test();
    cfg.batch_rows = 8;
    let first = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    assert!(matches!(first, IngestOutcome::Completed(_, _)));

    // Change a config value that affects skip-decision.
    cfg.batch_rows = 16;
    // Overwrite the file (we wrote manifest with batch=8, now config says 16).
    // Manually delete the output so the new run is allowed to write.
    let manifest_path = lake.join("metadata").join("manifest.jsonl");
    let rec_line = std::fs::read_to_string(&manifest_path).unwrap();
    // Extract the output path from the manifest.
    let val: serde_json::Value = serde_json::from_str(rec_line.trim()).unwrap();
    let op = val["output_paths"][0].as_str().unwrap().to_string();
    std::fs::remove_file(&op).unwrap();

    let second = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    assert!(matches!(second, IngestOutcome::Completed(_, _)));
}
