//! Criterion bench harness scaffold.
//!
//! Real benchmarks need a representative file (≈1 GB / 220 M rows is
//! the design point). The scaffold here runs against any path passed
//! via `BENCH_FILE=<path>`; if unset, a tiny in-memory fixture is built
//! so the bench compiles and runs on a fresh clone.
//!
//! Run with:
//!     BENCH_FILE=data/raw/Year12/coldcer/coldcer_groc_1687_1739 cargo bench --bench parse_sales
//!
//! The harness measures:
//!
//! 1. `parse_only` — mmap + parse_records_into_builder, no Arrow finish
//! 2. `parse_and_arrow` — full parse, finish `RecordBatch`
//! 3. `parse_and_parquet` — full parse, finish `RecordBatch`, write Parquet
//!
//! Use `BENCH_BATCH=<rows>`, `BENCH_ROW_GROUP=<rows>` and
//! `BENCH_COMPRESSION=<codec>` to vary parameters. Everything else stays
//! at `IngestConfig::defaults()`. See `BENCHMARKING.md` for the full
//! protocol and for measured results.

use std::path::PathBuf;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use arrow_array::RecordBatch;
use iri_lake::arrow_output::{schema, SalesBuilders};
use iri_lake::config::IngestConfig;
use iri_lake::fixed_width::{HEADER_LEN, RECORD_LEN};
use iri_lake::model::{Channel, SourceIdentity};
use iri_lake::parquet_output::write_parquet_atomic;
use iri_lake::parser::parse_records_into_builder;

const FIXTURE_HEADER: &[u8] = iri_lake::fixed_width::HEADER_TEXT;

fn bench_path() -> Option<PathBuf> {
    std::env::var("BENCH_FILE").ok().map(PathBuf::from)
}

fn build_tiny_fixture_bytes(rows: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(HEADER_LEN + rows * RECORD_LEN);
    v.extend_from_slice(FIXTURE_HEADER);
    v.extend_from_slice(b"\r\n");
    for i in 0..rows {
        let mut row = Vec::new();
        row.extend_from_slice(b"1234567");
        row.push(b' ');
        row.extend_from_slice(b"1114");
        row.push(b' ');
        row.extend_from_slice(b" 0");
        row.push(b' ');
        row.extend_from_slice(b" 2");
        row.push(b' ');
        row.extend_from_slice(b"18200");
        row.push(b' ');
        row.extend_from_slice(b"  647");
        row.push(b' ');
        row.extend_from_slice(format!("{:>5}", i + 1).as_bytes());
        row.push(b' ');
        row.extend_from_slice(format!("{:>8}", (i as f64) * 0.99).as_bytes());
        row.push(b' ');
        row.extend_from_slice(b"NONE");
        row.push(b' ');
        row.push(b'0');
        row.push(b' ');
        row.push(if i % 2 == 0 { b'0' } else { b'1' });
        row.extend_from_slice(b"\r\n");
        assert_eq!(row.len(), RECORD_LEN);
        v.extend_from_slice(&row);
    }
    v
}

fn tiny_identity() -> SourceIdentity {
    SourceIdentity {
        path: PathBuf::from("/tmp/bench"),
        year: 1,
        category: "bench".into(),
        channel: Channel::Drug,
        filename_week_start: 1,
        filename_week_end: 52,
    }
}

fn bench_parse_only(c: &mut Criterion) {
    let bytes = match bench_path() {
        Some(p) => std::fs::read(&p).expect("BENCH_FILE readable"),
        None => build_tiny_fixture_bytes(10_000),
    };
    let body = &bytes[HEADER_LEN..];
    let id = tiny_identity();
    let rows = body.len() / RECORD_LEN;

    let mut group = c.benchmark_group("parse_only");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
        b.iter(|| {
            let mut builders = SalesBuilders::with_capacity(rows);
            parse_records_into_builder(
                black_box(&id),
                black_box(body),
                0,
                black_box(rows),
                &mut builders,
            )
            .unwrap();
            black_box(builders.len());
        })
    });
    group.finish();
}

fn bench_parse_and_arrow(c: &mut Criterion) {
    let bytes = match bench_path() {
        Some(p) => std::fs::read(&p).expect("BENCH_FILE readable"),
        None => build_tiny_fixture_bytes(10_000),
    };
    let body = &bytes[HEADER_LEN..];
    let id = tiny_identity();
    let rows = body.len() / RECORD_LEN;

    let mut group = c.benchmark_group("parse_and_arrow");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
        b.iter(|| {
            let mut builders = SalesBuilders::with_capacity(rows);
            parse_records_into_builder(
                black_box(&id),
                black_box(body),
                0,
                black_box(rows),
                &mut builders,
            )
            .unwrap();
            let batch: RecordBatch = builders.finish(schema()).unwrap();
            black_box(batch.num_rows());
        })
    });
    group.finish();
}

fn bench_parse_and_parquet(c: &mut Criterion) {
    let bytes = match bench_path() {
        Some(p) => std::fs::read(&p).expect("BENCH_FILE readable"),
        None => build_tiny_fixture_bytes(10_000),
    };
    let body = &bytes[HEADER_LEN..];
    let id = tiny_identity();
    let rows = body.len() / RECORD_LEN;
    let batch_size: usize = std::env::var("BENCH_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    // Benchmark at *production* settings. `IngestConfig::for_test()` sets
    // `parquet_row_group_rows = 1024`, which would write one row group per
    // 1024 rows — hundreds of tiny row groups for a real file, and a badly
    // inflated `parse_and_parquet` number. Only `batch_rows` and the codec
    // are meant to be swept here.
    let mut cfg = IngestConfig::defaults();
    cfg.worker_threads = Some(1);
    if let Ok(codec) = std::env::var("BENCH_COMPRESSION") {
        cfg.compression = codec;
    }
    cfg.batch_rows = batch_size;
    if let Ok(rg) = std::env::var("BENCH_ROW_GROUP") {
        cfg.parquet_row_group_rows = rg.parse().unwrap();
    }

    let dir = tempfile::tempdir().unwrap();

    let mut group = c.benchmark_group("parse_and_parquet");
    group.throughput(Throughput::Elements(rows as u64));
    group.measurement_time(Duration::from_secs(5));
    group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
        b.iter(|| {
            let mut builders = SalesBuilders::with_capacity(rows);
            parse_records_into_builder(
                black_box(&id),
                black_box(body),
                0,
                black_box(rows),
                &mut builders,
            )
            .unwrap();
            let batch: RecordBatch = builders.finish(schema()).unwrap();
            let out_path = dir.path().join("bench_out.parquet");
            let _ = write_parquet_atomic(&out_path, schema(), std::slice::from_ref(&batch), &cfg)
                .unwrap();
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_parse_only,
    bench_parse_and_arrow,
    bench_parse_and_parquet
);
criterion_main!(benches);
