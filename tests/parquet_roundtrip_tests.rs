//! Parquet round-trip tests.
//!
//! Each test writes a tiny fixture through `ingest_file`, reads the
//! resulting Parquet file back through the Arrow/Parquet APIs, and
//! asserts that every field matches the original input byte-for-byte
//! in semantic terms. `dollars_cents` round-trip is the headline test:
//! no float precision loss.

mod common;

use std::fs::File;
use std::path::Path;

use arrow_array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt8Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use iri_lake::arrow_output::schema;
use iri_lake::config::IngestConfig;
use iri_lake::fixed_width::{HEADER_LEN, RECORD_LEN};
use iri_lake::ingest::{ingest_file, IngestFilter, IngestOutcome};
use iri_lake::parquet_output::write_parquet_atomic;

fn read_all_batches(path: &Path) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    builder.build().unwrap().map(|b| b.unwrap()).collect()
}

fn ingest_fixture(rows_in: usize) -> (tempfile::TempDir, std::path::PathBuf, usize) {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::write_sales_fixture(
        tmp.path(),
        1,
        "beer",
        "drug",
        (1114, 1165),
        &common::default_rows(rows_in),
    );
    let lake = tmp.path().join("lake");
    let cfg = IngestConfig::for_test();
    let outcome = ingest_file(&p, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    let stats = match outcome {
        IngestOutcome::Completed(_, s) => s,
        IngestOutcome::Skipped(_) => panic!("expected ingest"),
    };
    assert_eq!(stats.output_paths.len(), 1);
    (tmp, stats.output_paths[0].clone(), rows_in)
}

#[test]
fn parquet_round_trips_basic_schema() {
    let (tmp, path, rows) = ingest_fixture(32);
    let file_size = std::fs::metadata(tmp.path().join("Year1/beer/beer_drug_1114_1165"))
        .unwrap()
        .len();
    // Sanity: header + rows * RECORD_LEN
    assert_eq!(
        file_size,
        HEADER_LEN as u64 + (rows as u64) * RECORD_LEN as u64
    );

    let batches = read_all_batches(&path);
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), rows);

    // iri_key column: 1_000_000 + (i % 100) for i in 0..rows
    let iri = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    for i in 0..rows {
        let expected = 1_000_000u32 + ((i % 100) as u32);
        assert_eq!(iri.value(i), expected, "iri_key mismatch at row {}", i);
    }

    // dollars_cents exact integer round-trip
    let cents = batch
        .column(7)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..rows {
        let expected: i64 = ((i as f64) * 0.99 * 100.0).round() as i64;
        assert_eq!(
            cents.value(i),
            expected,
            "dollars_cents mismatch at row {}",
            i
        );
    }

    // Boolean round-trip
    let pr = batch
        .column(10)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    for i in 0..rows {
        let expected = i % 2 != 0; // 1 on odd indices per default_rows
        assert_eq!(
            pr.value(i),
            expected,
            "price_reduction mismatch at row {}",
            i
        );
    }

    // Channel string round-trip
    let ch = batch
        .column(12)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..rows {
        assert_eq!(ch.value(i), "drug");
    }

    // source_year is no longer a physical column; year is derivable
    // from week (or read from the partition directory). Sy, feature_code,
    // display, vend, item, week, category all stay at the same offsets.
    let sy = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    assert_eq!(sy.value(0), 0);
    let fc = batch
        .column(8)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    // i % 3 == 0 -> A (code 1), else NONE (code 0)
    for i in 0..rows {
        let expected = if i % 3 == 0 { 1u8 } else { 0u8 };
        assert_eq!(fc.value(i), expected);
    }
    let week = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    for i in 0..rows {
        let expected = 1114u16 + ((i / 100) as u16);
        assert_eq!(week.value(i), expected);
    }
    let cat = batch
        .column(11)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..rows {
        assert_eq!(cat.value(i), "beer");
    }
}

#[test]
fn parquet_writer_atomic_swap_leaves_no_tmp() {
    let tmp = tempfile::tempdir().unwrap();
    let mut b = iri_lake::arrow_output::SalesBuilders::with_capacity(2);
    b.iri_key.append_value(1);
    b.week.append_value(2);
    b.sy.append_value(3);
    b.ge.append_value(4);
    b.vend.append_value(5);
    b.item.append_value(6);
    b.units.append_value(7);
    b.dollars_cents.append_value(800);
    b.feature_code.append_value(0);
    b.display.append_value(0);
    b.price_reduction.append_value(true);
    b.category.append_value("c");
    b.channel.append_value("drug");
    b.iri_key.append_value(10);
    b.week.append_value(20);
    b.sy.append_value(30);
    b.ge.append_value(40);
    b.vend.append_value(50);
    b.item.append_value(60);
    b.units.append_value(70);
    b.dollars_cents.append_value(900);
    b.feature_code.append_value(1);
    b.display.append_value(1);
    b.price_reduction.append_value(false);
    b.category.append_value("d");
    b.channel.append_value("groc");
    let batch = b.finish(schema()).unwrap();

    let final_path = tmp.path().join("nested/dir/out.parquet");
    let cfg = IngestConfig::for_test();
    write_parquet_atomic(&final_path, schema(), &[batch], &cfg).unwrap();
    assert!(final_path.exists());

    let tmp_path: std::path::PathBuf = {
        let mut p = final_path.as_os_str().to_owned();
        p.push(".tmp");
        std::path::PathBuf::from(p)
    };
    assert!(!tmp_path.exists());
}

#[test]
fn partition_path_layout_is_hive_style() {
    use iri_lake::model::{Channel, PartitionPath, SourceIdentity};
    let id = SourceIdentity {
        path: std::path::PathBuf::from("/x"),
        year: 12,
        category: "toothpa".into(),
        channel: Channel::Drug,
        filename_week_start: 1687,
        filename_week_end: 1739,
    };
    let p = PartitionPath::from_identity(&id);
    assert_eq!(
        p.relative(),
        std::path::PathBuf::from("year=12/category=toothpa/channel=drug")
    );
}

#[test]
fn two_distinct_sources_do_not_collide_on_output() {
    // Different categories with the same apparent (year, channel) bucket
    // must end up at different paths.
    let tmp = tempfile::tempdir().unwrap();
    let p1 = common::write_sales_fixture(
        tmp.path(),
        1,
        "beer",
        "drug",
        (1114, 1165),
        &common::default_rows(8),
    );
    let p2 = common::write_sales_fixture(
        tmp.path(),
        1,
        "coffee",
        "drug",
        (1114, 1165),
        &common::default_rows(8),
    );
    let lake = tmp.path().join("lake");
    let cfg = IngestConfig::for_test();
    let o1 = ingest_file(&p1, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    let o2 = ingest_file(&p2, tmp.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
    let s1 = o1.stats().unwrap();
    let s2 = o2.stats().unwrap();
    assert_ne!(s1.output_paths[0], s2.output_paths[0]);
    assert!(s1.output_paths[0]
        .to_string_lossy()
        .contains("/category=beer/"));
    assert!(s2.output_paths[0]
        .to_string_lossy()
        .contains("/category=coffee/"));
}

#[test]
fn parquet_file_has_expected_column_metadata() {
    let (_tmp, path, rows) = ingest_fixture(8);
    let f = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let meta = builder.metadata().file_metadata();
    let n_rows = meta.num_rows();
    assert_eq!(n_rows, rows as i64);
    let schema = meta.schema_descr();
    let n_cols = schema.num_columns();
    assert_eq!(n_cols, 13);
}
