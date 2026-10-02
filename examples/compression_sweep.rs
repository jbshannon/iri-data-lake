//! Compression codec sweep for the ingest write path.
//!
//! Measures, for each Parquet codec:
//!
//!   * **write**  — encode a parsed `RecordBatch` to a new Parquet file
//!   * **size**   — resulting bytes, and bytes/row
//!   * **decode** — read the file back and materialise every batch
//!
//! The decode column matters as much as the write one: a lakehouse pays
//! the write cost once and the decode cost on every scan, so a codec that
//! wins on ingest but loses on read is a bad trade.
//!
//! Usage:
//!     cargo run --release --example compression_sweep -- <file> [<file>...]
//!     cargo run --release --example compression_sweep -- --iters 9 --codecs snappy,zstd-1 <file>
//!
//! All output goes to a scratch dir that is removed on exit; the input
//! files are never modified.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

use iri_lake::arrow_output::schema;
use iri_lake::config::IngestConfig;
use iri_lake::fixed_width::{HEADER_LEN, RECORD_LEN};
use iri_lake::model::{Channel, SourceIdentity};
use iri_lake::parquet_output::compression_from_config;
use iri_lake::parser::parse_records_into_builder;

const ALL_CODECS: &[&str] = &[
    "uncompressed",
    "snappy",
    "lz4",
    "lz4_raw",
    "zstd",
    "zstd-1",
    "zstd-3",
    "zstd-9",
];

fn parse_args() -> (usize, Vec<String>, Vec<String>) {
    let mut iters = 5usize;
    let mut codecs: Vec<String> = ALL_CODECS.iter().map(|s| s.to_string()).collect();
    let mut files = Vec::new();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--iters" => {
                iters = argv[i + 1].parse().expect("--iters N");
                i += 2;
            }
            "--codecs" => {
                codecs = argv[i + 1].split(',').map(|s| s.trim().into()).collect();
                i += 2;
            }
            other => {
                files.push(other.to_string());
                i += 1;
            }
        }
    }
    (iters, codecs, files)
}

/// Parse a source file into production-sized batches.
fn parse_file(path: &Path, batch_rows: usize) -> (Vec<RecordBatch>, u64) {
    let bytes = std::fs::read(path).expect("read source file");
    let body = &bytes[HEADER_LEN..];
    let total_rows = body.len() / RECORD_LEN;
    let identity = SourceIdentity {
        path: path.to_path_buf(),
        year: 1,
        category: "sweep".into(),
        channel: Channel::Drug,
        filename_week_start: 1114,
        filename_week_end: 1165,
    };
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < total_rows {
        let n = batch_rows.min(total_rows - start);
        let mut b = iri_lake::arrow_output::SalesBuilders::with_capacity(n);
        parse_records_into_builder(&identity, body, start as u64, n, &mut b).expect("parse batch");
        out.push(b.finish(schema()).expect("finish batch"));
        start += n;
    }
    (out, total_rows as u64)
}

fn write_once(batches: &[RecordBatch], cfg: &IngestConfig, out: &Path) -> Duration {
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(compression_from_config(cfg))
        .set_max_row_group_size(cfg.parquet_row_group_rows)
        .build();
    let t = Instant::now();
    {
        let f = std::fs::File::create(out).unwrap();
        let mut w =
            ArrowWriter::try_new(std::io::BufWriter::new(f), schema(), Some(props)).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.close().unwrap();
    }
    t.elapsed()
}

/// Read a Parquet file back, materialising every batch so every page is
/// actually decompressed rather than lazily skipped.
fn decode_once(path: &Path) -> (Duration, u64) {
    let t = Instant::now();
    let f = std::fs::File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let reader = builder.build().unwrap();
    let mut rows = 0u64;
    for batch in reader {
        rows += batch.unwrap().num_rows() as u64;
    }
    (t.elapsed(), rows)
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort_by_key(|d| d.as_nanos());
    v[v.len() / 2]
}

fn main() {
    let (iters, codecs, files) = parse_args();
    if files.is_empty() {
        eprintln!("usage: compression_sweep [--iters N] [--codecs a,b,c] <file>...");
        std::process::exit(2);
    }
    let scratch = tempfile::tempdir().expect("scratch dir");

    for f in &files {
        let path = PathBuf::from(f);
        let input_bytes = std::fs::metadata(&path).unwrap().len();
        let cfg = IngestConfig::defaults();
        let (batches, rows) = parse_file(&path, cfg.batch_rows);

        println!();
        println!("{}", f);
        println!(
            "  input {:.1} MiB · {rows} rows · {} batches · batch_rows={} · row_group_rows={}",
            input_bytes as f64 / 2f64.powi(20),
            batches.len(),
            cfg.batch_rows,
            cfg.parquet_row_group_rows
        );
        println!(
            "  {:<14} {:>10} {:>10} {:>10} {:>12} {:>10}",
            "codec", "write_ms", "MiB/s", "decode_ms", "size_MiB", "B/row"
        );
        println!("  {}", "-".repeat(70));

        for codec in &codecs {
            let mut c = cfg.clone();
            c.compression = codec.clone();
            let out = scratch
                .path()
                .join(format!("{}.parquet", codec.replace(['/', '-'], "_")));

            // parquet-rs returns Err (and panics internally) when a codec's
            // cargo feature was compiled out. Silence the default hook for
            // the duration so the sweep table isn't buried in backtraces,
            // then report the codec as UNSUPPORTED.
            let write_result = {
                let prev = std::panic::take_hook();
                std::panic::set_hook(Box::new(|_| {}));
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut ts = Vec::new();
                    for _ in 0..iters {
                        ts.push(write_once(&batches, &c, &out));
                    }
                    ts
                }));
                std::panic::set_hook(prev);
                r
            };

            let ts = match write_result {
                Ok(ts) => ts,
                Err(_) => {
                    println!("  {:<14} {:>10}", codec, "UNSUPPORTED");
                    continue;
                }
            };

            let size = std::fs::metadata(&out).unwrap().len();
            let mut ds = Vec::new();
            let mut decoded_rows = 0;
            for _ in 0..iters.min(3) {
                let (d, r) = decode_once(&out);
                ds.push(d);
                decoded_rows = r;
            }
            assert_eq!(decoded_rows, rows, "round-trip row count mismatch");

            let w = median(ts);
            let d = median(ds);
            let write_mib_s = (input_bytes as f64 / 2f64.powi(20)) / w.as_secs_f64();
            println!(
                "  {:<14} {:>10.1} {:>10.1} {:>10.1} {:>12.2} {:>10.2}",
                codec,
                w.as_secs_f64() * 1e3,
                write_mib_s,
                d.as_secs_f64() * 1e3,
                size as f64 / 2f64.powi(20),
                size as f64 / rows as f64,
            );
        }
        println!();
    }
}
