//! Discovery & filename parsing tests.

mod common;

use std::path::PathBuf;

use iri_lake::discovery::{discover, parse_identity, SkipReason};
use iri_lake::model::Channel;

fn id(p: &str) -> PathBuf {
    PathBuf::from(p)
}

#[test]
fn parses_simple_drug_filename() {
    let root = id("/raw");
    let path = id("/raw/Year3/beer/beer_drug_1218_1269");
    let identity = parse_identity(&path, &root).unwrap();
    assert_eq!(identity.year, 3);
    assert_eq!(identity.category, "beer");
    assert_eq!(identity.channel, Channel::Drug);
    assert_eq!(identity.filename_week_start, 1218);
    assert_eq!(identity.filename_week_end, 1269);
}

#[test]
fn parses_simple_groc_filename() {
    let root = id("/raw");
    let path = id("/raw/Year7/coffee/coffee_groc_1427_1478");
    let identity = parse_identity(&path, &root).unwrap();
    assert_eq!(identity.year, 7);
    assert_eq!(identity.channel, Channel::Groc);
}

#[test]
fn parses_nested_year12_toothpa_path() {
    let root = id("/raw");
    let path = id("/raw/Year12/toothpa/toothpa/toothpa_drug_1687_1739");
    let identity = parse_identity(&path, &root).unwrap();
    assert_eq!(identity.year, 12);
    assert_eq!(identity.category, "toothpa");
}

#[test]
fn rejects_panel_files() {
    // A PANEL filename is filtered out by the walker's filename-level
    // skip_reason check before parse_identity is even called. Calling
    // parse_identity directly returns UnparseableFilename because the
    // `.dat` suffix is not a numeric week range.
    let root = id("/raw");
    let path = id("/raw/Year1/beer/beer_PANEL_DR_1114_1165.dat");
    let err = parse_identity(&path, &root).unwrap_err();
    assert_eq!(err, iri_lake::discovery::SkipReason::UnparseableFilename);
}

#[test]
fn rejects_unknown_channel() {
    let root = id("/raw");
    let path = id("/raw/Year1/beer/beer_other_1114_1165");
    let err = parse_identity(&path, &root).unwrap_err();
    assert_eq!(err, SkipReason::InvalidChannel);
}

#[test]
fn rejects_out_of_range_year() {
    let root = id("/raw");
    let path = id("/raw/Year15/beer/beer_drug_1_52");
    let err = parse_identity(&path, &root).unwrap_err();
    assert_eq!(err, SkipReason::BadYearAncestor);
}

#[test]
fn rejects_zero_or_reversed_week_range() {
    let root = id("/raw");
    let path = id("/raw/Year1/beer/beer_drug_0_52");
    assert!(parse_identity(&path, &root).is_err());
    let path = id("/raw/Year1/beer/beer_drug_100_50");
    assert!(parse_identity(&path, &root).is_err());
}

#[test]
fn walker_discovers_a_real_fixture() {
    let tmp = tempfile::tempdir().unwrap();
    let _p = common::fixture_path(tmp.path());
    let inv = discover(tmp.path()).unwrap();
    assert_eq!(inv.total_files(), 1);
    assert!(inv.skipped.is_empty());
    let f = &inv.files[0];
    assert_eq!(f.identity.year, 1);
    assert_eq!(f.identity.category, "beer");
    assert_eq!(f.identity.channel, Channel::Drug);
    assert_eq!(f.identity.filename_week_start, 1114);
    assert_eq!(f.identity.filename_week_end, 1165);
    assert!(f.size_bytes >= 57 + 64 * 56);
}

#[test]
fn walker_finds_nested_year12_file() {
    let tmp = tempfile::tempdir().unwrap();
    let _p = common::nested_year12_fixture(tmp.path(), 16);
    let inv = discover(tmp.path()).unwrap();
    assert_eq!(inv.total_files(), 1);
    assert_eq!(inv.files[0].identity.year, 12);
    assert_eq!(inv.files[0].identity.category, "toothpa");
}

#[test]
fn walker_ignores_panel_and_excel_and_lockfiles() {
    let tmp = tempfile::tempdir().unwrap();
    // Eligible sales file
    common::fixture_path(tmp.path());
    // PANEL file
    let panel = tmp.path().join("Year1/beer/beer_PANEL_DR_1114_1165.dat");
    std::fs::create_dir_all(panel.parent().unwrap()).unwrap();
    std::fs::write(&panel, b"irrelevant").unwrap();
    // Lock file
    let lock = tmp.path().join("Year1/beer/~$beer_drug_1114_1165");
    std::fs::write(&lock, b"x").unwrap();
    // Old backup
    let old = tmp.path().join("Year1/beer/beer_drug_1114_1165.OLD");
    std::fs::write(&old, b"x").unwrap();
    // Excel
    let xls = tmp.path().join("Year1/beer/prod_attr.xls");
    std::fs::write(&xls, b"x").unwrap();

    let inv = discover(tmp.path()).unwrap();
    assert_eq!(inv.total_files(), 1, "only the canonical file should match");
    // We should have reported all four non-sales files as skipped.
    let reasons: Vec<_> = inv.skipped.iter().map(|s| s.reason).collect();
    assert!(reasons.contains(&SkipReason::PanelFile));
    assert!(reasons.contains(&SkipReason::LockFile));
    assert!(reasons.contains(&SkipReason::BackupExtension));
    assert!(reasons.contains(&SkipReason::StubOrExcel));
}

#[test]
fn walker_ignores_known_non_data_directories() {
    let tmp = tempfile::tempdir().unwrap();
    // Add an Excel file inside the top-level `parsed stub files/` directory.
    let stubs = tmp.path().join("parsed stub files");
    std::fs::create_dir_all(&stubs).unwrap();
    std::fs::write(stubs.join("prod_attr.xls"), b"x").unwrap();
    // And a sales file inside Year1.
    common::fixture_path(tmp.path());
    let inv = discover(tmp.path()).unwrap();
    assert_eq!(inv.total_files(), 1);
}

#[test]
fn walker_sorts_by_year_and_channel_for_summary() {
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
        1,
        "beer",
        "groc",
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
    assert_eq!(inv.total_files(), 3);
    let buckets = inv.by_year_channel();
    assert_eq!(buckets.len(), 3);
    assert!(buckets.contains_key(&(1, Channel::Drug)));
    assert!(buckets.contains_key(&(1, Channel::Groc)));
    assert!(buckets.contains_key(&(2, Channel::Drug)));
}
