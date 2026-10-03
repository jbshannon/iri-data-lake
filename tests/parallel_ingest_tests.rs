//! G1: file-level parallelism.
//!
//! These tests pin the properties the parallel `ingest-all` has to
//! hold, all of which are checked on fixtures rather than the corpus:
//!
//! 1. **Output equivalence.** N workers must produce byte-identical
//!    Parquet to 1 worker, in any order. If this ever fails, the
//!    parallelism changed the data.
//! 2. **Manifest integrity.** Every source appears exactly once, every
//!    line parses, and a second run skips everything.
//! 3. **Failure isolation.** One bad source is counted and logged; the
//!    other files still complete. The run does not abort.
//! 4. **Accounting.** Rows/bytes in the summary equal the sum over the
//!    fixtures, and match the sequential run's.
//!
//! The byte-equivalence check is the important one: `docs/parallelism.md`
//! reports it as measured on the real corpus too, but a test is what
//! keeps it true as the parser changes.

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use iri_lake::config::IngestConfig;
use iri_lake::discovery::discover;
use iri_lake::ingest::ingest_all;
use iri_lake::manifest::{manifest_path, JsonlManifest, ManifestStore};

use common::{default_rows, write_sales_fixture};

/// Distinct category per fixture, so every file lands in its own
/// partition directory (and never collides on filename, which is
/// derived from category + channel).
fn category_for(i: usize) -> String {
    format!("cat{i:02}")
}

/// Build a corpus of `n` fixtures with deliberately uneven sizes, so
/// scheduling order actually matters to the run.
fn build_corpus(root: &Path, n: usize) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    for i in 0..n {
        let cat = category_for(i);
        // 40, 400, 4000 rows: a 100x spread, like the real corpus
        // (0.02 GiB drug files next to 1.4 GiB groc files).
        let rows = 40 * 10usize.pow((i % 3) as u32);
        let channel = if i % 2 == 0 { "drug" } else { "groc" };
        paths.push(write_sales_fixture(
            root,
            1,
            &cat,
            channel,
            (1114, 1165),
            &default_rows(rows),
        ));
    }
    paths
}

fn total_rows(paths: &[std::path::PathBuf]) -> u64 {
    paths
        .iter()
        .map(|p| {
            iri_lake::fixed_width::expected_rows(std::fs::metadata(p).unwrap().len()).unwrap_or(0)
        })
        .sum()
}

/// sha256 of every Parquet file, keyed by *partition directory* (not
/// filename, which carries a per-run UUID).
fn partition_digests(lake: &Path) -> BTreeMap<String, Vec<String>> {
    use sha2::{Digest, Sha256};
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    fn walk(dir: &Path, lake: &Path, out: &mut BTreeMap<String, Vec<String>>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            if p.is_dir() {
                walk(&p, lake, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let rel = p.parent().unwrap().to_path_buf();
                let key = rel
                    .strip_prefix(lake.join("bronze").join("iri_sales"))
                    .unwrap_or(&rel)
                    .to_string_lossy()
                    .into_owned();
                let mut h = Sha256::new();
                h.update(std::fs::read(&p).unwrap());
                out.entry(key).or_default().push(hex::encode(h.finalize()));
            }
        }
    }
    walk(&lake.join("bronze"), lake, &mut out);
    for v in out.values_mut() {
        v.sort();
    }
    out
}

fn cfg() -> IngestConfig {
    let mut c = IngestConfig::for_test();
    // Production-ish: bigger batches so the Parquet writer's row-group
    // behaviour is exercised rather than one group per 1024 rows.
    c.batch_rows = 1024;
    c.parquet_row_group_rows = 4096;
    c.overwrite = iri_lake::config::OverwriteMode::SkipIfPresent;
    c
}

#[test]
fn parallel_output_is_byte_identical_to_sequential() {
    let dir = tempfile::tempdir().unwrap();
    let paths = build_corpus(dir.path(), 12);
    let expected_rows = total_rows(&paths);
    let inv = discover(dir.path()).unwrap();
    assert_eq!(inv.files.len(), 12);

    // 1 worker.
    let seq_lake = dir.path().join("lake-seq");
    let seq = ingest_all(&inv.files, dir.path(), &seq_lake, &cfg(), 1).unwrap();
    assert_eq!(seq.completed, 12);
    assert_eq!(seq.failed, 0);
    assert_eq!(seq.rows, expected_rows);

    // 8 workers, and the reverse file order, to prove the ordering is
    // not what makes the bytes match.
    let mut reversed = inv.files.clone();
    reversed.reverse();
    let par_lake = dir.path().join("lake-par");
    let par = ingest_all(&reversed, dir.path(), &par_lake, &cfg(), 8).unwrap();
    assert_eq!(par.completed, 12);
    assert_eq!(par.failed, 0);
    assert_eq!(par.rows, expected_rows);
    assert_eq!(par.bytes_in, seq.bytes_in);
    assert_eq!(par.bytes_out, seq.bytes_out);

    let a = partition_digests(&seq_lake);
    let b = partition_digests(&par_lake);
    assert!(!a.is_empty(), "no parquet produced");
    assert_eq!(a, b, "parallel output differs from sequential output");
}

#[test]
fn manifest_is_one_intact_record_per_source() {
    let dir = tempfile::tempdir().unwrap();
    build_corpus(dir.path(), 12);
    let inv = discover(dir.path()).unwrap();
    let lake = dir.path().join("lake");
    let s = ingest_all(&inv.files, dir.path(), &lake, &cfg(), 6).unwrap();
    assert_eq!(s.completed, 12);

    // Every line parses (no torn interleaved writes).
    let store = JsonlManifest::open(&lake).unwrap();
    let all = store.all().unwrap();
    assert_eq!(all.len(), 12, "expected exactly one record per source");
    let mut seen = std::collections::HashSet::new();
    for r in &all {
        assert!(
            seen.insert(r.source_path.clone()),
            "duplicate record for {}",
            r.source_path.display()
        );
        assert_eq!(r.status, iri_lake::model::ManifestStatus::Success);
    }

    // Re-running the same scope skips everything and writes nothing.
    let s2 = ingest_all(&inv.files, dir.path(), &lake, &cfg(), 6).unwrap();
    assert_eq!(s2.skipped, 12);
    assert_eq!(s2.completed, 0);
    assert_eq!(store.all().unwrap().len(), 12, "resume appended records");
    assert!(manifest_path(&lake).exists());
}

#[test]
fn a_bad_source_does_not_abort_the_run() {
    let dir = tempfile::tempdir().unwrap();
    build_corpus(dir.path(), 6);
    // A seventh source with a corrupt header: discovered (the filename
    // is well-formed) but rejected at header validation.
    common::bad_header_fixture(dir.path());
    let inv = discover(dir.path()).unwrap();
    assert_eq!(inv.files.len(), 7);

    let lake = dir.path().join("lake");
    let s = ingest_all(&inv.files, dir.path(), &lake, &cfg(), 4).unwrap();
    assert_eq!(s.completed, 6);
    assert_eq!(s.failed, 1, "the bad file should be counted, not fatal");
    assert_eq!(s.failures.len(), 1);
    assert!(matches!(
        s.failures[0].1,
        iri_lake::errors::IngestError::HeaderMismatch { .. }
    ));

    // The good files are all on disk; the failure is recorded by count,
    // and a re-run retries it (no stale success to skip it).
    let s2 = ingest_all(&inv.files, dir.path(), &lake, &cfg(), 4).unwrap();
    assert_eq!(s2.skipped, 6);
    assert_eq!(s2.failed, 1);
}

#[test]
fn a_truncated_trailing_record_is_counted_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    build_corpus(dir.path(), 4);
    // A fifth source with a valid header and one whole record plus a
    // partial one — the shape of the real `Year12/soup` defect.
    common::misaligned_fixture(dir.path());
    let inv = discover(dir.path()).unwrap();
    assert_eq!(inv.files.len(), 5);

    let lake = dir.path().join("lake");
    let s = ingest_all(&inv.files, dir.path(), &lake, &cfg(), 4).unwrap();
    assert_eq!(s.completed, 5, "a truncated tail must not fail the file");
    assert_eq!(s.failed, 0);
    assert_eq!(s.rejected_rows, 1);

    // The rejection is in the manifest, on a *successful* record: the
    // lake has the good rows and the loss is auditable.
    let store = JsonlManifest::open(&lake).unwrap();
    let all = store.all().unwrap();
    assert_eq!(all.len(), 5);
    let rejected: Vec<_> = all.iter().filter(|r| r.rejected_rows > 0).collect();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].rejected_rows, 1);
    assert_eq!(
        rejected[0].status,
        iri_lake::model::ManifestStatus::Success,
        "a partial tail is a successful ingest of the prefix"
    );
}

#[test]
fn worker_pool_can_be_run_at_several_widths() {
    let dir = tempfile::tempdir().unwrap();
    let paths = build_corpus(dir.path(), 9);
    let expected_rows = total_rows(&paths);
    let inv = discover(dir.path()).unwrap();

    // 0 workers is a config error, not a panic.
    assert!(ingest_all(&inv.files, dir.path(), &dir.path().join("l0"), &cfg(), 0).is_err());

    for workers in [1usize, 2, 3, 8] {
        let lake = dir.path().join(format!("lake-{workers}"));
        let s = ingest_all(&inv.files, dir.path(), &lake, &cfg(), workers).unwrap();
        assert_eq!(s.completed, 9, "workers={workers}");
        assert_eq!(s.failed, 0, "workers={workers}");
        assert_eq!(s.rows, expected_rows, "workers={workers}");
    }
}
