//! End-to-end tests for the non-sales datasets.
//!
//! These build **real binary fixtures** in a tempdir — the same rule the
//! sales tests follow — because every one of these formats is defined by
//! its bytes, not by a schema file. A test that fed the parser something
//! it had already parsed would pass against a parser that had been
//! subtly wrong all along.
//!
//! What each test pins:
//!
//! - the fixture is byte-identical in shape to the corpus file it
//!   stands for (real header, real widths, real delimiters),
//! - the class writes the schema it declares,
//! - a second run *skips* rather than duplicating, and
//! - the manifest records the outcome, so a lake's provenance is
//!   answerable from one file.

mod common;

use std::path::{Path, PathBuf};

use arrow_array::Array as _;

use iri_lake::config::IngestConfig;
use iri_lake::dataset::{DatasetFile, SourceRef};
use iri_lake::datasets::ingest::{ingest_dataset, DatasetSummary};
use iri_lake::datasets::{delivery_stores, panel, product_attr};
use iri_lake::manifest::{manifest_path, SharedManifest};
use iri_lake::model::{DatasetKind, ManifestStatus};

/// Ingest one file and return the manifest record it produced.
fn ingest_one(kind: DatasetKind, path: &Path, source: SourceRef, lake: &Path) -> DatasetSummary {
    let cfg = IngestConfig::for_test();
    let tuning = iri_lake::dataset::tuning_for_kind(kind).unwrap();
    let size = std::fs::metadata(path).unwrap().len();
    let files = vec![DatasetFile {
        kind,
        source,
        size_bytes: size,
    }];
    let summary = ingest_dataset(kind, &files, lake, &cfg, 1, &tuning).unwrap();
    if let Some((p, e)) = summary.failures.first() {
        panic!("{kind} failed on {}: {e}", p.display());
    }
    summary
}

/// Every record in the manifest, newest last.
fn manifest(lake: &Path) -> Vec<iri_lake::model::ManifestRecord> {
    let text = std::fs::read_to_string(manifest_path(lake)).unwrap();
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Read back every Parquet file under `dir` as one batch.
fn read_dir(dir: &Path) -> arrow_array::RecordBatch {
    let mut paths: Vec<PathBuf> = walkdir::WalkDir::new(dir)
        .into_iter()
        .flatten()
        .filter(|e| e.file_type().is_file())
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
// iri_panel
// ---------------------------------------------------------------------

/// Write a PANEL fixture whose header is `header` verbatim.
fn write_panel(dir: &Path, name: &str, header: &str, rows: &[&str]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, format!("{header}\r\n{}\r\n", rows.join("\r\n"))).unwrap();
    p
}

/// The year-1 header, verbatim.
const Y1_PANEL_HEADER: &str = "PANID\tWEEK\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC";
/// The year-9 header, verbatim.
const Y9_PANEL_HEADER: &str = "PANID,WEEK,MINUTE,UNITS,OUTLET,DOLLARS,IRI_KEY,COLUPC";
/// The one-file outlier: year-8 layout, year-1 delimiter.
const Y11_PANEL_HEADER: &str = "PANID\tWEEK\tMINUTE\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC";

#[test]
fn panel_round_trips_through_parquet_and_is_then_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");

    // Three rows: two exact, one the float32 encoding.
    let rows = [
        "3315465\t1134\t1\tGR\t1.69\t228037\t10943900008",
        "3135202\t1144\t2\tGR\t5.39\t257006\t10946803119",
        "3347963\t1164\t0.049999997\tGR\t0.7299998474\t264075\t11820043550",
    ];
    let p = write_panel(
        dir.path(),
        "beer_PANEL_GR_1114_1165.dat",
        Y1_PANEL_HEADER,
        &rows,
    );
    let source = SourceRef::new(&p)
        .with_year(1)
        .with_category("beer")
        .with_outlet("GR")
        .with_weeks(1114, 1165);

    let s = ingest_one(DatasetKind::Panel, &p, source.clone(), &lake);
    assert_eq!(s.completed, 1);
    assert_eq!(s.rows, 3);

    let out = lake.join("bronze/iri_panel/year=1/category=beer/outlet=GR");
    assert!(out.is_dir(), "Hive layout not created at {}", out.display());
    let batch = read_dir(&out);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.schema(), panel::schema());

    // Money: the digits+scale pair carries the raw source bytes; the
    // rounded cents column is derived.
    //   1.69      -> cents=169, digits=169,  scale=2
    //   5.39      -> cents=539, digits=539,  scale=2
    //   0.7299998 -> cents=73,  digits=7299998474, scale=7
    let dollars_cents = batch
        .column(7)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(dollars_cents.value(0), 169);
    assert_eq!(dollars_cents.value(1), 539);
    assert_eq!(
        dollars_cents.value(2),
        73,
        "float32(0.73) must round back to 73 cents"
    );
    let dollars_digits = batch
        .column(8)
        .as_any()
        .downcast_ref::<arrow_array::Decimal128Array>()
        .unwrap();
    let dollars_scale = batch
        .column(9)
        .as_any()
        .downcast_ref::<arrow_array::Int8Array>()
        .unwrap();
    assert_eq!(dollars_digits.value(0), 169);
    assert_eq!(dollars_scale.value(0), 2);
    assert_eq!(dollars_digits.value(2), 7299998474);
    assert_eq!(dollars_scale.value(2), 10);
    // Every column except `minute` is fully populated. `minute` is null
    // for all three rows because the year-1 dialect has no MINUTE
    // column at all — absence, not a missing value.
    assert_eq!(batch.num_rows(), 3);
    // Null counts per column:
    //   2 (minute):     3  (the year-1 dialect has no MINUTE column)
    //   3 (units int):  1  (the third row's 0.049999997 is genuinely
    //                       fractional, so the integer column is null;
    //                       the digits+scale pair below carries it)
    //   other:         0
    for (i, c) in batch.columns().iter().enumerate() {
        let expected = match i {
            2 => 3,
            3 => 1,
            _ => 0,
        };
        assert_eq!(c.null_count(), expected, "column {i} null count");
    }

    // COLUPC keeps its leading zeros.
    let colupc = batch
        .column(11)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(colupc.value(0), "10943900008");

    // The dialect reached the manifest, so a consumer can see it.
    let recs = manifest(&lake);
    assert_eq!(recs.len(), 1);
    let note = recs[0].error_message.as_deref().unwrap_or_default();
    assert!(
        note.contains("dialect=tab") && note.contains("minute_column=absent"),
        "manifest should record the dialect, got {note:?}"
    );

    // Second run skips: the manifest is the idempotence store.
    //
    // This is the regression that a per-class schema version guards.
    // The prospective record is built before the parser exists, so it
    // carries the *class's* version; the written record carried
    // whatever the parser reported. While those differed, no source
    // could ever be skipped, and a re-run silently re-ingested
    // everything into new files.
    let s2 = ingest_one(DatasetKind::Panel, &p, source, &lake);
    assert_eq!(
        s2.skipped, 1,
        "a second run must skip, not re-ingest; s2={s2:#?}"
    );
    assert_eq!(s2.completed, 0);
    assert_eq!(s2.rows, 0);
    assert_eq!(manifest(&lake).len(), 1, "a skip must not append a record");
}

#[test]
fn the_comma_dialect_with_minute_parses_and_skips() {
    // The year-8+ layout, verbatim from
    // `Year9/beer/beer_PANEL_GK_1531_1582.DAT`, including the padding
    // every field carries.
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");
    let p = write_panel(
        dir.path(),
        "beer_PANEL_GK_1531_1582.DAT",
        Y9_PANEL_HEADER,
        &[
            "3822619 ,1536 ,9334 ,1 ,GK ,6.49 ,257871 ,0011497450009 ",
            "3108126 ,1568 ,8262 ,1 ,GK ,5.29 ,257871 ,0011820000106 ",
        ],
    );
    let source = SourceRef::new(&p)
        .with_year(9)
        .with_category("beer")
        .with_outlet("GK")
        .with_weeks(1531, 1582);
    let s = ingest_one(DatasetKind::Panel, &p, source.clone(), &lake);
    assert_eq!(s.rows, 2);

    let batch = read_dir(&lake.join("bronze/iri_panel/year=9/category=beer/outlet=GK"));
    let minute = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .unwrap();
    assert!(!minute.is_null(0), "this dialect has a MINUTE column");
    assert_eq!(minute.value(0), 9334);
    let panid = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    assert_eq!(panid.value(0), 3_822_619, "padded fields must trim");
    let colupc = batch
        .column(11)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(colupc.value(0), "0011497450009", "leading zeros kept");
    for c in batch.columns() {
        assert_eq!(c.null_count(), 0, "this fixture has no optional blanks");
    }

    let s2 = ingest_one(DatasetKind::Panel, &p, source, &lake);
    assert_eq!(s2.skipped, 1, "a second run must skip");
}

#[test]
fn all_four_panel_dialects_normalise_to_one_schema() {
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");

    // One row per observed dialect, in each dialect's own header.
    let cases: [(&str, &str, &str); 4] = [
        (
            "a_drug_1114_1165.dat",
            Y1_PANEL_HEADER,
            "3315465\t1134\t1\tDR\t1.69\t228037\t10943900008",
        ),
        (
            "b_groc_1114_1165.dat",
            Y1_PANEL_HEADER,
            "3315465\t1134\t1\tGR\t1.69\t228037\t10943900008",
        ),
        (
            "c_PANEL_MA_1114_1165.dat",
            Y1_PANEL_HEADER,
            "3315465\t1134\t1\tMA\t1.69\t228037\t10943900008",
        ),
        // The year-11 outlier: tab-delimited with MINUTE.
        (
            "d_PANEL_GK_1635_1686.DAT",
            Y11_PANEL_HEADER,
            "3822619\t1536\t9334\t1\tGK\t6.49\t257871\t0011497450009",
        ),
    ];

    let mut schema_seen: Option<std::sync::Arc<arrow_schema::Schema>> = None;
    for (name, header, row) in cases {
        let p = write_panel(dir.path(), name, header, &[row]);
        let outlet = name
            .split('_')
            .nth(2)
            .and_then(|s| s.strip_suffix(".dat").or_else(|| s.strip_suffix(".DAT")))
            .unwrap_or("GK")
            .to_string();
        let source = SourceRef::new(&p)
            .with_year(1)
            .with_category("beer")
            .with_outlet(outlet)
            .with_weeks(1114, 1165);
        ingest_one(DatasetKind::Panel, &p, source, &lake);
        let batch = read_dir(&lake.join("bronze/iri_panel/year=1/category=beer"));
        match &schema_seen {
            None => schema_seen = Some(batch.schema()),
            Some(first) => assert_eq!(
                &batch.schema(),
                first,
                "{name} produced a different schema; one table must have one schema"
            ),
        }
    }

    // The tab+MINUTE file got its MINUTE; the plain tab files did not.
    let recs = manifest(&lake);
    assert_eq!(recs.len(), 4);
    let with_minute = recs
        .iter()
        .filter(|r| {
            r.error_message
                .as_deref()
                .unwrap_or_default()
                .contains("minute_column=present")
        })
        .count();
    assert_eq!(
        with_minute, 1,
        "exactly one fixture has a MINUTE column, and it is the tab-delimited one"
    );
}

#[test]
fn a_zero_byte_panel_file_is_an_empty_success_not_a_failure() {
    // The corpus really contains two: `Year1/beer`'s PANEL_DR and one
    // Year-2 equivalent. A failure here would be permanent — a failed
    // manifest record never matches a skip, so it would retry forever.
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");
    let p = dir.path().join("beer_PANEL_DR_1114_1165.dat");
    std::fs::write(&p, b"").unwrap();

    let source = SourceRef::new(&p)
        .with_year(1)
        .with_category("beer")
        .with_outlet("DR");
    let s = ingest_one(DatasetKind::Panel, &p, source, &lake);
    assert_eq!(s.empty, 1);
    assert_eq!(s.failed, 0);
    assert_eq!(s.rows, 0);

    let recs = manifest(&lake);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].status, ManifestStatus::Success);
    assert!(recs[0]
        .error_message
        .as_deref()
        .unwrap_or_default()
        .contains("zero_bytes"));
}

#[test]
fn a_panid_without_a_minute_column_gets_a_null_not_a_zero() {
    // Zero would be indistinguishable from a real midnight trip.
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");
    let p = write_panel(
        dir.path(),
        "beer_PANEL_DR_1114_1165.dat",
        Y1_PANEL_HEADER,
        &["3315465\t1134\t1\tDR\t1.69\t228037\t10943900008"],
    );
    let source = SourceRef::new(&p)
        .with_year(1)
        .with_category("beer")
        .with_outlet("DR");
    ingest_one(DatasetKind::Panel, &p, source, &lake);
    let batch = read_dir(&lake.join("bronze/iri_panel/year=1/category=beer/outlet=DR"));
    let minute = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .unwrap();
    assert!(minute.is_null(0));
}

// ---------------------------------------------------------------------
// iri_delivery_stores
// ---------------------------------------------------------------------

#[test]
fn delivery_stores_slices_the_measured_63_byte_layout() {
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");

    // Verbatim first two lines of Year1/beer/Delivery_Stores.
    let header = "IRI_KEY OU EST_ACV  Market_Name              Open Clsd MskdName";
    let rows = [
        " 200039 GR 9.709999 BUFFALO/ROCHESTER         539 1219 Chain87 ",
        " 200171 GR 27.69099 MILWAUKEE                 522 9998 Chain97 ",
    ];
    let p = write_panel(dir.path(), "Delivery_Stores", header, &rows);
    // 63-byte header + CRLF, then 63 bytes + CRLF per row. The real
    // file is 2053 rows of exactly this shape.
    assert_eq!(std::fs::metadata(&p).unwrap().len(), 65 + 2 * 65);
    assert_eq!(rows[0].len(), delivery_stores::RECORD_LEN);

    let source = SourceRef::new(&p).with_year(1).with_category("beer");
    let s = ingest_one(DatasetKind::DeliveryStores, &p, source, &lake);
    assert_eq!(s.completed, 1);
    assert_eq!(s.rows, 2);

    // Partitioned by year alone, not by the directory it was found in.
    let out = lake.join("bronze/iri_delivery_stores/year=1");
    assert!(
        out.is_dir(),
        "expected {out:?}; a per-category layout would scatter one \
         year-wide dimension across 31 directories"
    );
    assert!(!lake
        .join("bronze/iri_delivery_stores/year=1/category=beer")
        .exists());

    let batch = read_dir(&out);
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema(), delivery_stores::schema());

    let key = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    let ou = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let market = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let open = batch
        .column(4)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .unwrap();
    let closed = batch
        .column(5)
        .as_any()
        .downcast_ref::<arrow_array::UInt16Array>()
        .unwrap();
    let masked = batch
        .column(6)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    assert_eq!(key.value(0), 200_039);
    assert_eq!(ou.value(0), "GR");
    assert_eq!(market.value(0), "BUFFALO/ROCHESTER");
    assert_eq!(open.value(0), 539);
    assert_eq!(closed.value(0), 1219);
    assert_eq!(masked.value(0), "Chain87");
    // The 9998 "still trading" sentinel is a value, not a null.
    assert_eq!(closed.value(1), delivery_stores::OPEN_ENDED);
    assert!(!closed.is_null(1));
}

// ---------------------------------------------------------------------
// iri_product_attr
// ---------------------------------------------------------------------

#[test]
fn product_attr_writes_a_schema_derived_from_its_own_header() {
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");

    // A three-attribute file built at the measured pitch: 27-byte key
    // prefix then 21-byte attribute columns.
    let key = " 0  1     1 30233   0.1944 ";
    let attrs = ["FLAVOR/SCENT", "PACKAGE", "PRODUCT TYPE"];
    let values = ["MISSING", "LONG NECK BTL IN BOX", "BEER"];
    let mut header = String::from(key);
    let mut row = String::from(key);
    for (a, v) in attrs.iter().zip(values.iter()) {
        header.push_str(&format!("{a:<21}"));
        row.push_str(&format!("{v:<21}"));
    }
    let p = write_panel(dir.path(), "beer_prod_attr", &header, &[&row]);
    // The row's *content* length is what the parser derives the
    // attribute count from: a 27-byte key prefix plus N x 21 bytes.
    assert_eq!(
        row.len(),
        product_attr::KEY_PREFIX_LEN + 3 * product_attr::ATTR_WIDTH
    );
    assert_eq!(
        product_attr::attr_count(row.len()),
        Some(3),
        "the fixture must be exactly at the measured pitch"
    );

    let source = SourceRef::new(&p).with_year(9).with_category("beer");
    let s = ingest_one(DatasetKind::ProductAttr, &p, source, &lake);
    assert_eq!(s.completed, 1);
    assert_eq!(s.rows, 1);

    let out = lake.join("bronze/iri_product_attr/year=9/category=beer");
    let batch = read_dir(&out);
    // 5 typed key columns + 3 attributes.
    assert_eq!(batch.num_columns(), 8);
    let schema = batch.schema();
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(
        names,
        vec![
            "sy",
            "ge",
            "vend",
            "item",
            "vol_eq",
            "FLAVOR_SCENT",
            "PACKAGE",
            "PRODUCT_TYPE"
        ]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>()
    );
    let item = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    assert_eq!(item.value(0), 30233);
    let pkg = batch
        .column(6)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(pkg.value(0), "LONG NECK BTL IN BOX");

    // The manifest records the per-file schema's version, so a change
    // to this file's columns forces a re-ingest and an identical header
    // does not.
    let recs = manifest(&lake);
    let note = recs[0].error_message.as_deref().unwrap_or_default();
    assert!(note.contains("attributes=3"), "manifest note was {note:?}");
    assert_ne!(
        recs[0].output_schema_version,
        iri_lake::model::CURRENT_SCHEMA_VERSION,
        "a per-file schema must not claim the crate-wide version"
    );
}

// ---------------------------------------------------------------------
// manifest, resume and the dataset tag
// ---------------------------------------------------------------------

#[test]
fn the_manifest_records_the_dataset_class() {
    // "What class produced this file, and from which source" has to be
    // answerable from one file, or the lake's provenance is a guess.
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");
    let p = write_panel(
        dir.path(),
        "beer_PANEL_DR_1114_1165.dat",
        Y1_PANEL_HEADER,
        &["3315465\t1134\t1\tDR\t1.69\t228037\t10943900008"],
    );
    let source = SourceRef::new(&p)
        .with_year(1)
        .with_category("beer")
        .with_outlet("DR")
        .with_weeks(1114, 1165);
    ingest_one(DatasetKind::Panel, &p, source, &lake);

    let recs = manifest(&lake);
    assert_eq!(recs.len(), 1);
    let r = &recs[0];
    assert_eq!(r.dataset, DatasetKind::Panel);
    assert_eq!(r.source_year, Some(1));
    assert_eq!(r.category.as_deref(), Some("beer"));
    assert_eq!(r.filename_week_start, Some(1114));
    assert_eq!(r.filename_week_end, Some(1165));
    assert_eq!(r.written_rows, 1);
    assert_eq!(r.status, ManifestStatus::Success);
    assert!(!r.output_paths.is_empty());
    for p in &r.output_paths {
        assert!(p.exists(), "{} should exist", p.display());
    }
}

#[test]
fn a_pre_existing_manifest_still_deserialises() {
    // Every `manifest.jsonl` written before this change had mandatory
    // `source_year: 1` and no `dataset` field. Those lines must still
    // parse, and must still be honoured as *sales* records — otherwise
    // a re-run against a 744-file lake silently re-ingests all of it.
    let dir = tempfile::tempdir().unwrap();
    let path = manifest_path(dir.path());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let legacy = r#"{"run_id":"old","source_path":"/raw/Year1/beer/beer_drug_1114_1165","source_size_bytes":100,"source_sha256":"abc","source_year":1,"category":"beer","channel":"drug","filename_week_start":1114,"filename_week_end":1165,"expected_rows":10,"written_rows":10,"rejected_rows":0,"parser_version":"0.1.0","output_schema_version":3,"compression":"zstd","batch_rows":1000000,"row_group_rows":4000000,"output_paths":[],"output_size_bytes":50,"started_at":"2024-01-01T00:00:00Z","completed_at":"2024-01-01T00:00:01Z","duration_ms":1000,"status":"success","error_message":null}"#;
    std::fs::write(&path, format!("{legacy}\n")).unwrap();

    let store = SharedManifest::open(dir.path()).unwrap();
    let rec = store
        .last_for_path(Path::new("/raw/Year1/beer/beer_drug_1114_1165"))
        .unwrap()
        .expect("a legacy line must still be readable");
    assert_eq!(
        rec.dataset,
        DatasetKind::Sales,
        "defaults to the sales class"
    );
    assert_eq!(rec.source_year, Some(1));
    assert_eq!(rec.category.as_deref(), Some("beer"));
    assert_eq!(rec.deduplicated_from, None);
}

#[test]
fn an_empty_source_is_not_a_failure_for_any_class() {
    // The "empty success" contract, checked through the driver rather
    // than per parser: a real file with no content is a fact about the
    // corpus, not an error to retry forever.
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");

    let cases: Vec<(DatasetKind, &str, SourceRef)> = vec![
        (
            DatasetKind::Panel,
            "beer_PANEL_DR_1114_1165.dat",
            SourceRef::new("/x").with_year(1).with_category("beer"),
        ),
        (
            DatasetKind::DeliveryStores,
            "Delivery_Stores",
            SourceRef::new("/x").with_year(1),
        ),
        (
            DatasetKind::ProductAttr,
            "beer_prod_attr",
            SourceRef::new("/x").with_year(9).with_category("beer"),
        ),
    ];

    for (kind, name, source) in cases {
        let p = dir.path().join(name);
        std::fs::write(&p, b"").unwrap();
        let mut source = source.clone();
        source.path = p.clone();
        let s = ingest_one(kind, &p, source, &lake);
        assert_eq!(s.failed, 0, "{kind} must not fail on an empty source");
        assert_eq!(s.empty, 1, "{kind} should record an empty success");
    }
}

#[test]
fn a_broken_source_is_recorded_as_a_failure_and_the_run_continues() {
    // One unparseable source must not abandon the rest: the failure is
    // a value, and it lands in the manifest so a re-run retries it.
    let dir = tempfile::tempdir().unwrap();
    let lake = dir.path().join("lake");

    // A `prod_attr` whose row length cannot fit the 21-byte pitch.
    // Distinct names: two fixtures with the same name would leave only
    // the second on disk, and the test would silently check one file.
    // The broken one is 28 bytes — a 27-byte key prefix plus a single
    // trailing byte, which cannot hold even one 21-byte attribute.
    let p = write_panel(
        dir.path(),
        "broken_prod_attr",
        "SY GE VEND  ITEM  VOL_EQ  ",
        &[" 0  1     1 30233   0.1944 X"],
    );
    let good = write_panel(
        dir.path(),
        "good_prod_attr",
        "SY GE VEND  ITEM  VOL_EQ   FLAVOR/SCENT         ",
        &[" 0  1     1 30233   0.1944 MISSING              "],
    );
    assert_eq!(
        product_attr::attr_count(" 0  1     1 30233   0.1944 X".len()),
        None,
        "the fixture must be one the parser cannot explain"
    );

    let cfg = IngestConfig::for_test();
    let tuning = iri_lake::dataset::tuning_for_kind(DatasetKind::ProductAttr).unwrap();
    let files: Vec<DatasetFile> = [&p, &good]
        .iter()
        .map(|p| DatasetFile {
            kind: DatasetKind::ProductAttr,
            source: SourceRef::new(*p).with_year(9).with_category("beer"),
            size_bytes: std::fs::metadata(p).unwrap().len(),
        })
        .collect();

    let summary =
        ingest_dataset(DatasetKind::ProductAttr, &files, &lake, &cfg, 1, &tuning).unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.completed, 1, "the healthy source still ran");
    assert!(!summary.ok());

    let recs = manifest(&lake);
    let failed: Vec<_> = recs
        .iter()
        .filter(|r| r.status == ManifestStatus::Failed)
        .collect();
    assert_eq!(failed.len(), 1);
    assert!(failed[0].error_message.is_some());
    // A failed record must not satisfy a skip: it is retried next run.
    assert_eq!(failed[0].written_rows, 0);
}
