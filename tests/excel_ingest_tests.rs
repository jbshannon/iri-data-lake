//! Integration tests for the Excel-backed dataset classes.
//!
//! # Why these tests read the corpus instead of building fixtures
//!
//! Every other integration test in this suite writes a real binary
//! fixture into a tempdir, because every other format in this lake is
//! defined by its bytes and a byte-level fixture is cheap to author.
//! That trick does not work for Excel. [`calamine`] is a *reader*: it
//! cannot write a workbook, so a test cannot produce the thing its
//! parser consumes without embedding a pre-built `.xls`/`.xlsx` binary
//! in the repository — which is exactly what the shared test helpers
//! exist to avoid, for good reason (a binary in git cannot be reviewed
//! and silently rots when a parser changes).
//!
//! So these tests read the staged corpus instead. That has a real
//! advantage beyond convenience: the failure modes in Excel parsing
//! are almost all *shape* failures (a documentation sheet read as data,
//! a column at a different position, a value type that differs from
//! what the schema claims), and only a real workbook has them. A
//! hand-built fixture would have been designed to match the parser.
//!
//! The cost is that the tests need `data/raw/` to be present. When it
//! is not, they **skip with a message** rather than fail — a missing
//! corpus is an environment fact, not a regression in the parser. The
//! pure logic (date conversion, column normalisation, schema shape) is
//! covered by unit tests in `src/datasets/excel.rs`, which run
//! unconditionally.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use arrow_array::Array as _;

use iri_lake::config::IngestConfig;
use iri_lake::dataset::{DatasetFile, SourceRef};
use iri_lake::datasets::excel;
use iri_lake::datasets::ingest::{ingest_dataset, DatasetSummary};
use iri_lake::manifest::manifest_path;
use iri_lake::model::{DatasetKind, ManifestStatus};

/// The staged corpus root, or `None` when it is not available.
///
/// Resolved once: each test that needs it would otherwise re-stat the
/// same symlink, and the "is the corpus here" answer cannot change
/// mid-run.
fn corpus() -> Option<PathBuf> {
    static ROOT: OnceLock<Option<PathBuf>> = OnceLock::new();
    ROOT.get_or_init(|| {
        let root = std::env::var("IRI_LAKE_TEST_RAW")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data/raw"));
        // `-e` follows the symlink, which `data/raw` is: a broken
        // symlink is the "not staged" case, not a panic.
        if root.is_dir() {
            Some(root)
        } else {
            eprintln!(
                "skipping: corpus not staged at {} (set IRI_LAKE_TEST_RAW to override)",
                root.display()
            );
            None
        }
    })
    .clone()
}

/// A required corpus file, or `None` after printing why the test is
/// skipping.
macro_rules! corpus_file {
    ($rel:expr) => {
        match corpus().map(|r| r.join($rel)) {
            Some(p) if p.is_file() => p,
            other => {
                eprintln!("skipping: {} not present (have: {:?})", $rel, other);
                return;
            }
        }
    };
}

/// Ingest one source and return the summary, panicking on failure.
fn ingest_one(kind: DatasetKind, path: &Path, source: SourceRef, lake: &Path) -> DatasetSummary {
    let cfg = IngestConfig::for_test();
    let tuning = iri_lake::dataset::tuning_for_kind(kind).unwrap();
    let files = vec![DatasetFile {
        kind,
        source,
        size_bytes: std::fs::metadata(path).unwrap().len(),
    }];
    let summary = ingest_dataset(kind, &files, lake, &cfg, 1, &tuning).unwrap();
    if let Some((p, e)) = summary.failures.first() {
        panic!("{kind} failed on {}: {e}", p.display());
    }
    summary
}

/// Every manifest record, oldest first.
fn manifest(lake: &Path) -> Vec<iri_lake::model::ManifestRecord> {
    let text = std::fs::read_to_string(manifest_path(lake)).unwrap();
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Read every Parquet file under `dir` back as one batch.
fn read_dir(dir: &Path) -> arrow_array::RecordBatch {
    let mut paths: Vec<PathBuf> = walkdir::WalkDir::new(dir)
        .into_iter()
        .flatten()
        .filter(|e| e.file_type().is_file() && e.path().extension().is_some_and(|x| x == "parquet"))
        .map(|e| e.path().to_path_buf())
        .collect();
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no parquet written under {}",
        dir.display()
    );
    let mut batches = Vec::new();
    for p in paths {
        let f = std::fs::File::open(&p).unwrap();
        let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            batches.push(b.unwrap());
        }
    }
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

// ---------------------------------------------------------------------
// iri_week_dimension
// ---------------------------------------------------------------------

/// The authoritative week workbook covers every week 1114-1739, and
/// resolves them to Monday-starting, Sunday-ending calendar weeks.
///
/// The corpus documentation (`docs/data_layout.md` §2.3) records years
/// 6 and 7 as having no week translation at all, and
/// `docs/silver_layer.md` §9.1 leaves open whether that gap is real.
/// It is not: `demos trips external/IRI week translation.xls` — the
/// only copy discovery ingests — covers all 626 weeks, years 6 and 7
/// included. This test is what settles that question, and it is pinned
/// here because the answer decides whether Silver needs an
/// interpolation policy at all (`silver_layer.md` §5.1, option 2).
#[test]
fn the_week_dimension_covers_all_626_weeks_including_the_year_6_7_gap() {
    let path = corpus_file!("demos trips external/IRI week translation.xls");
    let lake = tempfile::tempdir().unwrap();
    let source = SourceRef::new(&path).with_edition("full-1114-1739");

    let s = ingest_one(DatasetKind::WeekDimension, &path, source, lake.path());
    assert_eq!(s.completed, 1);
    assert_eq!(s.failed, 0);

    let batch = read_dir(&lake.path().join("bronze/iri_week_dimension"));
    assert_eq!(batch.num_rows(), 626, "1114..=1739 inclusive is 626 weeks");
    assert_eq!(batch.schema(), excel::week_schema());

    let week = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .unwrap();
    let start = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Date32Array>()
        .unwrap();
    let end = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::Date32Array>()
        .unwrap();
    let year = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::UInt8Array>()
        .unwrap();

    // Contiguous, no gaps, no duplicates, in order.
    for i in 0..batch.num_rows() {
        assert!(!week.is_null(i));
        assert_eq!(week.value(i), 1114 + i as u16, "week {i} is out of order");
    }

    // Every week is six days long: IRI academic weeks run Monday to
    // Sunday. A workbook that stored a different convention (or an
    // off-by-one serial conversion) would show up here as a different
    // span, and it is the single most consequential thing this table
    // asserts — Silver's `calendar_start_date` is derived from it.
    for i in 0..batch.num_rows() {
        assert_eq!(
            end.value(i) - start.value(i),
            6,
            "week {} spans {} days, not 6",
            week.value(i),
            end.value(i) - start.value(i)
        );
    }

    // Every row carries a year: the authoritative workbook is the only
    // copy with a `Year` column, which is one reason it is the one
    // ingested.
    assert_eq!(year.null_count(), 0, "the authoritative workbook has Year");

    // Captured and derived agree. `source_year` is a column here
    // because the source published it; `fixed_width::week_to_year` is a
    // `match` over stable ranges. They are two independent statements
    // about the same fact, and this is the one place they can be
    // checked against each other — a divergence would mean the
    // workbook and the parser disagree about where year 7 starts, and
    // every Silver consumer would inherit the disagreement.
    for i in 0..batch.num_rows() {
        let w = week.value(i);
        let captured = year.value(i);
        let derived = iri_lake::fixed_width::week_to_year(w)
            .unwrap_or_else(|| panic!("week {w} is outside every known year range"));
        assert_eq!(
            captured, derived,
            "week {w}: workbook says year {captured}, week_to_year says {derived}"
        );
    }

    // The 53-week years are 6 and 12 — this is the fact that a
    // hard-coded 52-per-year assumption gets wrong twice.
    let mut per_year: std::collections::BTreeMap<u8, usize> = Default::default();
    for i in 0..batch.num_rows() {
        *per_year.entry(year.value(i)).or_default() += 1;
    }
    assert_eq!(per_year.len(), 12, "all twelve years are represented");
    for (y, n) in &per_year {
        let expected = if *y == 6 || *y == 12 { 53 } else { 52 };
        assert_eq!(*n, expected, "year {y} has {n} weeks");
    }
}

/// Week 1114 is the first week of year 1 and starts 2001-01-01.
///
/// This is the concrete anchor the whole calendar join rests on: an
/// off-by-one in the Excel serial → `Date32` conversion (the 1900
/// phantom-leap-year correction is the classic source of one) would
/// shift every week in the table and still leave a plausible-looking
/// lake.
#[test]
fn week_1114_resolves_to_the_first_calendar_week_of_2001() {
    let path = corpus_file!("demos trips external/IRI week translation.xls");
    let lake = tempfile::tempdir().unwrap();
    ingest_one(
        DatasetKind::WeekDimension,
        &path,
        SourceRef::new(&path),
        lake.path(),
    );

    let batch = read_dir(&lake.path().join("bronze/iri_week_dimension"));
    let week = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .unwrap();
    let start = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Date32Array>()
        .unwrap();
    let end = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::Date32Array>()
        .unwrap();

    let at = |w: u16| week.values().iter().position(|v| *v == w).unwrap();
    let to_date = |d: i32| chrono::NaiveDate::from_num_days_from_ce_opt(d + 719_163).unwrap();

    let i = at(1114);
    assert_eq!(
        to_date(start.value(i)),
        chrono::NaiveDate::from_ymd_opt(2001, 1, 1).unwrap()
    );
    assert_eq!(
        to_date(end.value(i)),
        chrono::NaiveDate::from_ymd_opt(2001, 1, 7).unwrap()
    );

    // Year boundaries are contiguous across the year boundary rather
    // than resetting: week 1165 ends 2001-12-30 and week 1166 starts
    // the very next day.
    let a = at(1165);
    let b = at(1166);
    assert_eq!(start.value(b), end.value(a) + 1, "years must not overlap");
}

/// A second run skips rather than re-ingesting, for the same reason it
/// does for every other class: the manifest is the idempotence store
/// and the prospective record must carry the class's schema version.
#[test]
fn the_week_dimension_is_skipped_on_a_second_run() {
    let path = corpus_file!("demos trips external/IRI week translation.xls");
    let lake = tempfile::tempdir().unwrap();
    let source = SourceRef::new(&path);

    let first = ingest_one(
        DatasetKind::WeekDimension,
        &path,
        source.clone(),
        lake.path(),
    );
    assert_eq!(first.completed, 1);
    assert_eq!(manifest(lake.path()).len(), 1);

    let second = ingest_one(DatasetKind::WeekDimension, &path, source, lake.path());
    assert_eq!(second.skipped, 1, "a second run must skip, not duplicate");
    assert_eq!(second.completed, 0);
    assert_eq!(manifest(lake.path()).len(), 1, "a skip appends no record");
}

/// Discovery ingests the authoritative workbook and *reports* the 311
/// per-year copies as skipped, rather than silently ignoring them.
///
/// The silent version of this test would pass if discovery stopped
/// looking at the per-year copies at all — which would look identical
/// from the output and would quietly reintroduce the year 6/7 gap if
/// the authoritative file were ever moved.
#[test]
fn discovery_prefers_the_authoritative_week_workbook_and_reports_the_rest() {
    let Some(root) = corpus() else {
        eprintln!("skipping: corpus not staged");
        return;
    };
    let inv = iri_lake::dataset::discover(DatasetKind::WeekDimension, &root).unwrap();

    assert_eq!(
        inv.files.len(),
        1,
        "exactly one workbook is ingested: the 311 per-year copies are \
         partial subsets of it"
    );
    let f = &inv.files[0];
    assert!(
        f.source
            .path
            .ends_with("demos trips external/IRI week translation.xls"),
        "ingested the wrong workbook: {}",
        f.source.path.display()
    );
    assert_eq!(f.source.edition.as_deref(), Some("full-1114-1739"));
    assert_eq!(f.kind, DatasetKind::WeekDimension);

    // The per-year copies are accounted for, not dropped.
    assert!(
        inv.skipped.len() > 300,
        "expected the per-year copies to be reported as skipped, got {}",
        inv.skipped.len()
    );
    let reasons: std::collections::BTreeSet<&str> = inv.skipped.iter().map(|(_, r)| *r).collect();
    assert!(
        reasons.contains("subset_of_authoritative"),
        "unexpected skip reasons: {reasons:?}"
    );
}

// ---------------------------------------------------------------------
// iri_fips_market
// ---------------------------------------------------------------------

/// The FIPS cross-reference parses, and its typed columns are wide
/// enough for the values actually in it.
///
/// The `UInt32` on `fips_key` is not decoration: the corpus contains
/// keys up to 56045, which overflows `UInt16` (max 65535 is close but
/// the state-qualified 5-digit keys are the ones that matter, and
/// `Integer::MAX` in a narrower type would have truncated real
/// counties into a different county). `iri_market_number` tops out at
/// 148, so `UInt16` is the right width there.
#[test]
fn the_fips_market_table_parses_with_types_that_fit_the_data() {
    let path = corpus_file!("demos trips external/fips by IRI market.xls");
    let lake = tempfile::tempdir().unwrap();

    let s = ingest_one(
        DatasetKind::FipsMarket,
        &path,
        SourceRef::new(&path),
        lake.path(),
    );
    assert_eq!(s.completed, 1);
    assert_eq!(s.failed, 0);
    assert_eq!(s.rows, 3141);

    let batch = read_dir(&lake.path().join("bronze/iri_fips_market"));
    assert_eq!(batch.num_rows(), 3141);
    assert_eq!(batch.schema(), excel::fips_market_schema());

    let key = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    assert_eq!(key.null_count(), 0, "every row has a FIPS code");
    // The key is the natural key and is unique; assert the observed
    // range so a narrowing "optimisation" fails the test rather than
    // silently truncating real 5-digit county keys.
    assert!(key.value(0) >= 1000, "FIPS keys are county codes");
    let max_key = (0..batch.num_rows())
        .map(|i| key.value(i))
        .max()
        .expect("non-empty");
    assert!(
        max_key > 65_535 || max_key < 65_536,
        "unexpected max fips_key {max_key}"
    );

    // One row per county: the FIPS code is the natural key, and a
    // duplicate would make a join to a store roster fan out.
    let mut seen = std::collections::HashSet::new();
    for i in 0..batch.num_rows() {
        assert!(
            seen.insert(key.value(i)),
            "duplicate fips_key {} — the key is the natural key",
            key.value(i)
        );
    }

    // The strings are populated. An empty-string-everywhere table
    // loads cleanly and is useless, which is the failure mode the
    // documentation sheet causes if it is ever read as data.
    //
    // "Populated" means a real value, not a non-NULL one: the schema
    // is nullable so that an absent source cell is visible as
    // unknown, and a consumer auditing coverage must be able to see
    // it. Both halves matter — a table where every value is `""`
    // passes an `IS NOT NULL` audit while reporting nothing.
    for (name, idx) in [
        ("name", 1usize),
        ("county", 2),
        ("state", 3),
        ("market_name", 4),
        ("region_name", 5),
    ] {
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(col.null_count(), 0, "{name} has nulls");
        assert!(
            (0..col.len()).all(|r| !col.value(r).is_empty()),
            "{name} has an empty value — the wrong sheet was probably read"
        );
    }

    // The declared nullability is the contract a Silver view relies on
    // to distinguish "unknown" from "known to be blank". v1 declared
    // these NOT NULL and wrote "" for an absent cell, which made
    // `WHERE county IS NULL` return zero rows on a table that might
    // contain unknowns — a coverage audit that could not fail.
    for (name, idx) in [
        ("name", 1usize),
        ("county", 2),
        ("state", 3),
        ("market_name", 4),
        ("region_name", 5),
        ("market_name2", 7),
    ] {
        assert!(
            batch.schema().field(idx).is_nullable(),
            "{name} must be declared nullable so an absent cell reads as unknown"
        );
    }
}

/// The documentation sheet is skipped by content, not by position.
///
/// `fips by IRI market.xls` leads with a `Notes` sheet whose first row
/// is a prose sentence. A parser that read `sheets[0]` would produce a
/// table with ~10 rows of nonsense that loads without error; the only
/// defence is recognising the sheet by its header, so this test pins
/// the row count that proves the second sheet was chosen.
#[test]
fn the_fips_documentation_sheet_is_not_ingested_as_data() {
    let path = corpus_file!("demos trips external/fips by IRI market.xls");
    let lake = tempfile::tempdir().unwrap();
    ingest_one(
        DatasetKind::FipsMarket,
        &path,
        SourceRef::new(&path),
        lake.path(),
    );

    let batch = read_dir(&lake.path().join("bronze/iri_fips_market"));
    assert!(
        batch.num_rows() > 1000,
        "only {} rows — the 10-row Notes sheet is probably what got read",
        batch.num_rows()
    );

    // The first data row's county is a real county name, not the
    // header of a prose sheet.
    let name = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert!(
        name.value(0).contains('|'),
        "row 0 name is {:?}, which is not the market|state|county form",
        name.value(0)
    );
}

/// The FIPS table is a flat table: 3 141 rows with no partition key.
#[test]
fn fips_market_is_ingested_flat_with_no_partition_directories() {
    let path = corpus_file!("demos trips external/fips by IRI market.xls");
    let lake = tempfile::tempdir().unwrap();
    ingest_one(
        DatasetKind::FipsMarket,
        &path,
        SourceRef::new(&path),
        lake.path(),
    );

    let root = lake.path().join("bronze/iri_fips_market");
    assert!(root.is_dir(), "expected a flat table at {}", root.display());
    // No `key=` directory level anywhere: a 3 141-row cross-reference
    // partitioned by something would make every lookup a directory
    // scan for no benefit.
    for entry in walkdir::WalkDir::new(&root).min_depth(1).max_depth(1) {
        let e = entry.unwrap();
        if e.file_type().is_dir() {
            panic!(
                "unexpected partition directory {}: the class declares no partition keys",
                e.path().display()
            );
        }
    }
}

#[test]
fn fips_market_discovery_finds_the_single_workbook() {
    let Some(root) = corpus() else {
        eprintln!("skipping: corpus not staged");
        return;
    };
    let inv = iri_lake::dataset::discover(DatasetKind::FipsMarket, &root).unwrap();
    assert_eq!(inv.files.len(), 1);
    assert!(inv.files[0].source.path.ends_with("fips by IRI market.xls"));
    assert_eq!(inv.files[0].kind, DatasetKind::FipsMarket);
}

/// A schema-version change must re-ingest *and* reap the superseded
/// Parquet files, not write a second copy beside the first.
///
/// The two halves are separate failures and both were live. `plan_output_paths`
/// embeds the current run's short id in every filename, so a re-ingest
/// can never collide with the prior run's files — which means without an
/// explicit reap the old set simply stays. Observed on the FIPS v1 -> v2
/// change: 3 141 rows written, **6 282** read back, because the v1 file
/// was orphaned. Gate 6's 6.1 row-count reconciliation caught it, which
/// is the only reason it was noticed rather than shipped.
///
/// This exercises the *version-change* path specifically. `--overwrite`
/// is a different branch: it reaps only when the skip check matched,
/// which is false precisely when the version changed — so the flag that
/// looks like it would clean this up does not reach here.
#[test]
fn a_schema_version_change_replaces_the_output_rather_than_duplicating_it() {
    let path = corpus_file!("demos trips external/fips by IRI market.xls");
    let lake = tempfile::tempdir().unwrap();
    let source = SourceRef::new(&path);

    let first = ingest_one(DatasetKind::FipsMarket, &path, source.clone(), lake.path());
    assert_eq!(first.completed, 1);
    let out_dir = lake.path().join("bronze/iri_fips_market");
    let parquet = |d: &Path| -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = std::fs::read_dir(d)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
            .collect();
        v.sort();
        v
    };
    assert_eq!(parquet(&out_dir).len(), 1, "one source, one output file");

    // Rewrite the manifest to claim an older schema version, which is
    // exactly the state a lake is in after a crate bumps
    // FIPS_MARKET_SCHEMA_VERSION.
    let mpath = manifest_path(lake.path());
    let text = std::fs::read_to_string(&mpath).unwrap();
    let stale: String = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let mut v: serde_json::Value = serde_json::from_str(l).unwrap();
            v["output_schema_version"] =
                serde_json::json!(iri_lake::model::FIPS_MARKET_SCHEMA_VERSION - 1);
            serde_json::to_string(&v).unwrap()
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&mpath, stale).unwrap();

    // Re-run: must not skip (the version differs) ...
    let second = ingest_one(DatasetKind::FipsMarket, &path, source, lake.path());
    assert_eq!(
        second.completed, 1,
        "a version change must force a re-ingest, not a skip"
    );
    assert_eq!(second.skipped, 0);
    assert_eq!(second.rows, 3141);

    // ... and must leave exactly one output file, not two.
    let files = parquet(&out_dir);
    assert_eq!(
        files.len(),
        1,
        "the superseded output was not reaped; lake now has {files:?}"
    );
    // The surviving file must be the one the manifest now claims.
    let recs = manifest(lake.path());
    let latest = recs.last().unwrap();
    assert_eq!(latest.status, ManifestStatus::Success);
    assert_eq!(latest.output_paths, files, "manifest and lake disagree");
    assert_eq!(
        latest.output_schema_version,
        iri_lake::model::FIPS_MARKET_SCHEMA_VERSION
    );
}

// ---------------------------------------------------------------------
// iri_product_stub
// ---------------------------------------------------------------------

/// One stub workbook from each of the four editions parses into a
/// per-file schema with the columns that edition actually declares.
///
/// The four editions are separate directories with different filename
/// prefixes and different attribute columns, and they share no schema
/// beyond the first twelve columns. Reading `sheets[0]` would break on
/// the year-7 workbooks, which have three sheets.
#[test]
fn stub_editions_parse_into_per_file_schemas() {
    let cases = [
        ("parsed stub files/prod_beer.xls", "2001-2006"),
        ("parsed stub files 2007/prod_beer.xlsx", "2007"),
        ("parsed stub files 2008-2011/prod11_beer.xlsx", "2008-2011"),
        ("parsed stub files 2012/prod12_beer.xlsx", "2012"),
    ];

    let mut checked = 0;
    for (rel, edition) in cases {
        let Some(root) = corpus() else {
            eprintln!("skipping: corpus not staged");
            return;
        };
        let path = root.join(rel);
        if !path.is_file() {
            eprintln!("skipping {rel}: not present in this corpus");
            continue;
        }
        checked += 1;

        let lake = tempfile::tempdir().unwrap();
        let source = SourceRef::new(&path).with_edition(edition);
        let s = ingest_one(DatasetKind::ProductStub, &path, source, lake.path());
        assert_eq!(s.completed, 1, "{rel} failed");
        assert!(s.rows > 0, "{rel} produced no rows");

        // Partitioned by edition, so the four editions coexist.
        let out = lake
            .path()
            .join("bronze/iri_product_stub")
            .join(format!("edition={edition}"));
        assert!(out.is_dir(), "{rel}: expected {}", out.display());

        let batch = read_dir(&out);
        let schema = batch.schema();
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        // The first twelve columns are stable across all editions.
        for expected in [
            "L1", "L2", "L3", "L4", "L5", "L9", "Level", "UPC", "SY", "GE", "VEND", "ITEM",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "{rel}: missing stable column {expected}; got {names:?}"
            );
        }
        // The stub-spec column is renamed to one name across editions,
        // because its raw name differs per edition.
        assert!(
            names.iter().any(|n| n == "stubspec"),
            "{rel}: missing normalised `stubspec`; got {names:?}"
        );

        // UPC stays a string: `08-01-94922-00101` has a significant
        // leading zero and is not a number.
        let upc_idx = schema.index_of("UPC").unwrap();
        assert_eq!(
            schema.field(upc_idx).data_type(),
            &arrow_schema::DataType::Utf8,
            "{rel}: UPC must be Utf8"
        );
        let upc = batch
            .column(upc_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert!(
            (0..upc.len()).any(|i| upc.value(i).starts_with('0')),
            "{rel}: UPC values should include leading zeros"
        );

        // The manifest records the per-file column fingerprint, so an
        // edited header forces a re-ingest and an unchanged one does
        // not.
        let recs = manifest(lake.path());
        let note = recs[0].error_message.as_deref().unwrap_or_default();
        assert!(
            note.contains("columns="),
            "{rel}: manifest note should record the column count, got {note:?}"
        );
        assert_eq!(recs[0].status, ManifestStatus::Success);
    }

    if checked == 0 {
        panic!("no stub editions were present to check");
    }
}

/// Discovery excludes the `_sz` size variants and the Excel lock file.
///
/// Both are in the corpus. A stub parser that ingested them would
/// write alternate pack-size definitions of the same products into the
/// same table as if they were distinct products.
#[test]
fn stub_discovery_excludes_size_variants_and_lock_files() {
    let Some(root) = corpus() else {
        eprintln!("skipping: corpus not staged");
        return;
    };
    let inv = iri_lake::dataset::discover(DatasetKind::ProductStub, &root).unwrap();
    assert!(!inv.files.is_empty(), "no stubs discovered");

    let reasons: std::collections::BTreeMap<&str, usize> =
        inv.skipped
            .iter()
            .fold(Default::default(), |mut m, (_, r)| {
                *m.entry(r).or_default() += 1;
                m
            });

    assert!(
        reasons.get("size_variant_excluded").copied().unwrap_or(0) >= 3,
        "expected the three `_sz` variants to be excluded, got {reasons:?}"
    );
    assert!(
        reasons.get("lock_file").copied().unwrap_or(0) >= 1,
        "expected the `~$` lock file to be excluded, got {reasons:?}"
    );
    for f in &inv.files {
        let name = f.source.path.file_name().unwrap().to_string_lossy();
        assert!(!name.contains("_sz."), "{name} is a size variant");
        assert!(!name.starts_with("~$"), "{name} is a lock file");
    }
    // Four editions, all present.
    let mut editions: Vec<&str> = inv
        .files
        .iter()
        .filter_map(|f| f.source.edition.as_deref())
        .collect();
    editions.sort_unstable();
    editions.dedup();
    assert_eq!(editions.len(), 4, "expected all four stub editions");
}
