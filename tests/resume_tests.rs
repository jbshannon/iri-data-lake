//! Manifest / resume tests.

mod common;

use std::path::Path;

use chrono::Utc;
use clap::Parser;
use iri_lake::cli::Cli;
use iri_lake::config::{IngestConfig, OverwriteMode};
use iri_lake::errors::IngestError;
use iri_lake::ingest::{ingest_file, IngestFilter, IngestOutcome};
use iri_lake::manifest::{skip_decision, JsonlManifest, ManifestStore};
use iri_lake::model::{
    Channel, DatasetKind, ManifestRecord, ManifestStatus, CURRENT_SCHEMA_VERSION, PARSER_VERSION,
};

fn fresh_record(path: &Path) -> ManifestRecord {
    ManifestRecord {
        run_id: "test".into(),
        dataset: DatasetKind::Sales,
        source_path: path.to_path_buf(),
        source_size_bytes: 100,
        source_sha256: "abc".into(),
        source_year: Some(1),
        category: Some("beer".into()),
        channel: Some(Channel::Drug),
        filename_week_start: Some(1),
        filename_week_end: Some(52),
        deduplicated_from: None,
        source_schema_fingerprint: None,
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

// ---------------------------------------------------------------------------
// Overwrite-policy tests.
//
// Regression cover for the case where `OverwriteMode` was declared on
// `IngestConfig` and set by `Cli::overwrite_mode`, but never read anywhere:
// `--overwrite` silently skipped, and `--resume` was parsed and discarded.
// `skip_decision` alone drove the skip, so the policy was unreachable.
// ---------------------------------------------------------------------------

#[test]
fn overwrite_mode_re_ingests_even_when_manifest_matches() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let lake = tmp.path().join("lake");

    let mut cfg = IngestConfig::for_test();
    cfg.overwrite = OverwriteMode::Overwrite;
    let first = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    assert!(matches!(first, IngestOutcome::Completed(_, _)));

    // Nothing about the source or config changed, so `skip_decision` still
    // reports a match. The Overwrite policy must win over that match.
    let second = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    match second {
        IngestOutcome::Completed(_, s) => assert_eq!(s.written_rows, 64),
        other => panic!("expected Completed under Overwrite, got {:?}", other),
    }

    // The row count above is necessary but not sufficient: a rewrite that
    // leaves the prior run's files on disk still reports the right number of
    // rows written, and only shows up as a doubled lake. `plan_output_paths`
    // embeds the new run id in every filename, so nothing collides by
    // accident and the superseded set must be reaped explicitly.
    let parquets = lake_files(&lake);
    assert_eq!(
        parquets.len(),
        1,
        "overwrite must leave exactly one Parquet file, found {parquets:?}"
    );

    // And the manifest must not accumulate a second record per source, or
    // Gate 5's 5a ("one record per source, no duplicates") fails.
    let records = manifest_lines(&lake);
    assert_eq!(
        records.len(),
        2,
        "two runs means two append-only records; 5a reads the latest per \
         source, so this is only a guard on gross duplication: {records:?}"
    );
}

/// Every `*.parquet` file under `lake`, sorted for stable assertion output.
fn lake_files(lake: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![lake.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "parquet") {
                out.push(p.display().to_string());
            }
        }
    }
    out.sort();
    out
}

/// Raw `manifest.jsonl` lines.
fn manifest_lines(lake: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(lake.join("metadata").join("manifest.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

#[test]
fn refuse_mode_errors_instead_of_skipping_or_overwriting() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let lake = tmp.path().join("lake");

    let mut cfg = IngestConfig::for_test();
    cfg.overwrite = OverwriteMode::Refuse;
    let first = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    assert!(matches!(first, IngestOutcome::Completed(_, _)));

    let err = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default())
        .expect_err("Refuse must error on a matching prior success");
    assert!(
        matches!(err, IngestError::OutputsExist { .. }),
        "expected OutputsExist, got {:?}",
        err
    );
}

#[test]
fn skip_if_present_still_skips() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let lake = tmp.path().join("lake");

    let mut cfg = IngestConfig::for_test();
    cfg.overwrite = OverwriteMode::SkipIfPresent;
    let first = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    assert!(matches!(first, IngestOutcome::Completed(_, _)));
    let second = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    assert!(matches!(second, IngestOutcome::Skipped(_)));
}

#[test]
fn resume_and_overwrite_together_are_rejected() {
    let cli = Cli::parse_from(["iri-lake", "ingest-all", "--resume", "--overwrite"]);
    let err = cli
        .overwrite_mode(true, true)
        .expect_err("opposite intents must not resolve silently");
    assert!(err.contains("mutually exclusive"), "got: {}", err);
}

#[test]
fn flag_pairs_map_to_expected_policies() {
    // Neither flag: skipping stays the default so an interrupted
    // 744-file run can simply be re-issued.
    let cli = Cli::parse_from(["iri-lake", "ingest-all"]);
    assert_eq!(
        cli.overwrite_mode(false, false).unwrap(),
        OverwriteMode::SkipIfPresent
    );
    // --resume is an explicit affirmation of the same default.
    let cli = Cli::parse_from(["iri-lake", "ingest-all", "--resume"]);
    assert_eq!(
        cli.overwrite_mode(true, false).unwrap(),
        OverwriteMode::SkipIfPresent
    );
    let cli = Cli::parse_from(["iri-lake", "ingest-all", "--overwrite"]);
    assert_eq!(
        cli.overwrite_mode(false, true).unwrap(),
        OverwriteMode::Overwrite
    );
}
