//! End-to-end ingest of a single sales file.
//!
//! Pipeline:
//!
//! ```text
//!   parse_identity()
//!      ↓
//!   mmap file → validate header + alignment
//!      ↓
//!   SHA-256 (parallelizable with mmap)
//!      ↓
//!   loop in batches of N rows:
//!      parse_records_into_builder() → RecordBatch → ArrowWriter
//!      ↓
//!   close writer → atomic rename .tmp → .parquet
//!      ↓
//!   write ManifestRecord
//! ```

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use chrono::Utc;
use memmap2::Mmap;
use uuid::Uuid;

use crate::arrow_output::{schema, SalesBuilders};
use crate::config::IngestConfig;
use crate::discovery::parse_identity;
use crate::errors::{IngestError, Result};
use crate::fixed_width::{self, HEADER_LEN, RECORD_LEN};
use crate::manifest::{skip_decision, JsonlManifest, ManifestStore};
use crate::metrics::{sha256_of_file, Timer};
use crate::model::{
    bronze_root, Channel, IngestStats, ManifestRecord, ManifestStatus, SourceIdentity,
    CURRENT_SCHEMA_VERSION, PARSER_VERSION,
};
use crate::parquet_output::write_parquet_atomic;
use crate::parser::parse_records_into_builder;

/// Optional filter applied during `ingest-all` to limit which source
/// files are processed.
#[derive(Debug, Clone, Default)]
pub struct IngestFilter {
    pub year: Option<u8>,
    pub category: Option<String>,
    pub channel: Option<Channel>,
}

/// Result of attempting to ingest a file (whether skipped, failed, or succeeded).
#[derive(Debug)]
pub enum IngestOutcome {
    Skipped(ManifestRecord),
    Completed(ManifestRecord, IngestStats),
}

impl IngestOutcome {
    pub fn stats(&self) -> Option<&IngestStats> {
        match self {
            IngestOutcome::Skipped(_) => None,
            IngestOutcome::Completed(_, s) => Some(s),
        }
    }
}

/// Plan the output filename for one source file. The plan depends on
/// the run's UUID so that two ingest runs of the same source — if any
/// output overlap is allowed — never collide on disk.
///
/// The naming scheme is deterministic per run-id:
///
/// ```text
/// part-<run-id-short>-<sequence>.parquet
/// ```
pub fn plan_output_paths(
    output_root: &Path,
    identity: &SourceIdentity,
    run_id: &str,
    batch_rows: usize,
    expected_rows: u64,
) -> Vec<PathBuf> {
    let partition = crate::model::PartitionPath::from_identity(identity).relative();
    let base = bronze_root(output_root).join(partition);
    let run_short = &run_id[..run_id.len().min(8)];
    let mut out = Vec::new();
    let n_batches = if expected_rows == 0 {
        0
    } else {
        expected_rows.div_ceil(batch_rows as u64) as usize
    };
    if n_batches <= 1 {
        out.push(base.join(format!("part-{}-0001.parquet", run_short)));
    } else {
        for i in 1..=n_batches {
            out.push(base.join(format!("part-{}-{:04}.parquet", run_short, i)));
        }
    }
    out
}

/// Ingest a single source file end-to-end.
///
/// Steps:
///
/// 1. Parse identity, validate size, compute expected row count.
/// 2. SHA-256 the source file.
/// 3. Check manifest for a prior success record; if present and the
///    output files still exist, return `IngestOutcome::Skipped`.
/// 4. mmap the file, validate header bytes, walk rows in batches,
///    write Parquet, atomically rename.
/// 5. Write a `ManifestRecord` to the JSONL store.
#[allow(unsafe_code)]
pub fn ingest_file(
    path: &Path,
    input_root: &Path,
    output_root: &Path,
    config: &IngestConfig,
    filter: &IngestFilter,
) -> Result<IngestOutcome> {
    // ---- 1. Identity & filter -----------------------------------------
    let identity = parse_identity(path, input_root).map_err(|_| {
        IngestError::Discovery(format!(
            "file does not match eligible sales filename shape: {}",
            path.display()
        ))
    })?;
    if let Some(y) = filter.year {
        if identity.year != y {
            return Err(IngestError::Config(format!(
                "filter requires year={}, got {} for {}",
                y,
                identity.year,
                path.display()
            )));
        }
    }
    if let Some(c) = &filter.category {
        if &identity.category != c {
            return Err(IngestError::Config(format!(
                "filter requires category={}, got {} for {}",
                c,
                identity.category,
                path.display()
            )));
        }
    }
    if let Some(ch) = filter.channel {
        if identity.channel != ch {
            return Err(IngestError::Config(format!(
                "filter requires channel={}, got {} for {}",
                ch,
                identity.channel,
                path.display()
            )));
        }
    }

    let metadata = std::fs::metadata(path).map_err(|e| IngestError::io(path, e))?;
    let size = metadata.len();
    let expected_rows =
        fixed_width::expected_rows(size).ok_or_else(|| IngestError::RecordAlignment {
            path: path.to_path_buf(),
            size,
            header: HEADER_LEN,
            record: RECORD_LEN,
            remainder: size.saturating_sub(HEADER_LEN as u64) % RECORD_LEN as u64,
        })?;

    // ---- 2. SHA-256 -----------------------------------------------------
    let source_sha256 = sha256_of_file(path)?;

    // ---- 3. Run-id + planned outputs -----------------------------------
    let run_id = Uuid::new_v4().to_string();
    let output_paths = plan_output_paths(
        output_root,
        &identity,
        &run_id,
        config.batch_rows,
        expected_rows,
    );

    // Skip-or-rewrite semantics. We refuse to overwrite if a prior
    // success record exists but the policy says "Refuse".
    let store = JsonlManifest::open(output_root)?;
    let prior = store.last_for_path(path)?;

    let started_at = Utc::now();
    let timer = Timer::start();

    // Compose the prospective ManifestRecord so skip_decision() can match.
    let prospective = ManifestRecord {
        run_id: run_id.clone(),
        source_path: path.to_path_buf(),
        source_size_bytes: size,
        source_sha256: source_sha256.clone(),
        source_year: identity.year,
        category: identity.category.clone(),
        channel: identity.channel,
        filename_week_start: identity.filename_week_start,
        filename_week_end: identity.filename_week_end,
        expected_rows,
        written_rows: 0, // placeholder
        rejected_rows: 0,
        parser_version: PARSER_VERSION.into(),
        output_schema_version: CURRENT_SCHEMA_VERSION,
        compression: config.compression.clone(),
        batch_rows: config.batch_rows,
        row_group_rows: config.parquet_row_group_rows,
        output_paths: output_paths.clone(),
        output_size_bytes: 0,
        started_at,
        completed_at: None,
        duration_ms: None,
        status: ManifestStatus::InProgress,
        error_message: None,
    };

    if let Some(prior_ok) = skip_decision(&prospective, prior.as_ref())? {
        return Ok(IngestOutcome::Skipped(prior_ok));
    }

    // ---- 4. mmap + validate header + parse in batches ------------------
    let file = File::open(path).map_err(|e| IngestError::io(path, e))?;
    // SAFETY: this is the only `unsafe` in the crate. `memmap2::Mmap::map`
    // is `unsafe fn` and exposes no safe wrapper. We hand it a `File` we
    // just opened read-only and never mutate it; the Mmap is dropped at
    // the end of this function while the `File` is still in scope, which
    // memmap2 requires. See `lib.rs` for the rationale.
    let mmap = unsafe { Mmap::map(&file) }.map_err(|e| IngestError::io(path, e))?;

    let mut header = [0u8; HEADER_LEN];
    header.copy_from_slice(&mmap[..HEADER_LEN]);
    fixed_width::validate_header(&header).map_err(|_| IngestError::HeaderMismatch {
        path: path.to_path_buf(),
        expected: std::str::from_utf8(fixed_width::HEADER_TEXT)
            .unwrap_or("")
            .to_string(),
        actual: String::from_utf8_lossy(&header[..HEADER_TEXT_LEN]).into_owned(),
    })?;

    let body = &mmap[HEADER_LEN..];
    let total_rows = body.len() / RECORD_LEN;
    let batch_rows = config.batch_rows;
    let mut written_rows: u64 = 0;
    let mut output_paths_written: Vec<PathBuf> = Vec::new();
    let mut total_output_bytes: u64 = 0;

    // Helper closure: write a slice of rows as one Parquet file.
    let mut write_batch =
        |rows_start: u64, rows_in_batch: usize, output_path: PathBuf| -> Result<()> {
            let start_off = (rows_start as usize) * RECORD_LEN;
            let end_off = start_off + rows_in_batch * RECORD_LEN;
            let body_slice = &body[start_off..end_off];
            let mut builders = SalesBuilders::with_capacity(rows_in_batch);
            parse_records_into_builder(
                &identity,
                body_slice,
                rows_start,
                rows_in_batch,
                &mut builders,
            )?;
            let batch: RecordBatch = builders.finish(schema())?;
            let bytes = write_parquet_atomic(&output_path, schema(), &[batch], config)?;
            total_output_bytes += bytes;
            output_paths_written.push(output_path);
            Ok(())
        };

    if total_rows == 0 {
        // 0-row file: write a single empty Parquet file to keep the
        // directory layout consistent. Some files may be header-only
        // by mistake — but a 0-row Parquet is still valid.
        let path0 = output_paths
            .first()
            .cloned()
            .unwrap_or_else(|| output_root.join("empty.parquet"));
        let batch: RecordBatch = SalesBuilders::with_capacity(0).finish(schema())?;
        let bytes = write_parquet_atomic(&path0, schema(), &[batch], config)?;
        total_output_bytes += bytes;
        output_paths_written.push(path0);
    } else {
        let mut rows_done = 0usize;
        let mut seq = 0usize;
        while rows_done < total_rows {
            let rows_in_batch = (total_rows - rows_done).min(batch_rows);
            let output_path = output_paths.get(seq).cloned().unwrap_or_else(|| {
                output_root.join(format!("part-{}-{:04}.parquet", &run_id[..8], seq + 1))
            });
            write_batch(rows_done as u64, rows_in_batch, output_path)?;
            written_rows += rows_in_batch as u64;
            rows_done += rows_in_batch;
            seq += 1;
        }
    }

    let elapsed = timer.elapsed();
    let stats = IngestStats {
        source_path: path.to_path_buf(),
        source_size_bytes: size,
        expected_rows,
        written_rows,
        rejected_rows: 0,
        output_bytes: total_output_bytes,
        elapsed,
        output_paths: output_paths_written.clone(),
    };

    let final_record = ManifestRecord {
        run_id: run_id.clone(),
        source_path: path.to_path_buf(),
        source_size_bytes: size,
        source_sha256,
        source_year: identity.year,
        category: identity.category.clone(),
        channel: identity.channel,
        filename_week_start: identity.filename_week_start,
        filename_week_end: identity.filename_week_end,
        expected_rows,
        written_rows,
        rejected_rows: 0,
        parser_version: PARSER_VERSION.into(),
        output_schema_version: CURRENT_SCHEMA_VERSION,
        compression: config.compression.clone(),
        batch_rows: config.batch_rows,
        row_group_rows: config.parquet_row_group_rows,
        output_paths: output_paths_written,
        output_size_bytes: total_output_bytes,
        started_at,
        completed_at: Some(Utc::now()),
        duration_ms: Some(elapsed.as_millis() as u64),
        status: ManifestStatus::Success,
        error_message: None,
    };
    store.append(&final_record)?;

    Ok(IngestOutcome::Completed(final_record, stats))
}

// Re-export for the unsafe block above (kept private to this module).
const HEADER_TEXT_LEN: usize = 55;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed_width::{HEADER_TEXT, RECORD_LEN};
    use std::io::Write;

    /// Build a fixture file under `dir` with the given number of rows.
    fn make_fixture(dir: &Path, rows: usize) -> (PathBuf, SourceIdentity) {
        let p = dir.join("Year1").join("beer").join("beer_drug_1114_1165");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let mut f = File::create(&p).unwrap();
        f.write_all(HEADER_TEXT).unwrap();
        f.write_all(b"\r\n").unwrap();
        for i in 0..rows {
            let mut row = Vec::new();
            row.extend_from_slice(b"1234567"); // IRI_KEY
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
            // Use {:.2} to keep DOLLARS at exactly 8 chars regardless of f64 noise.
            let dollars = format!("{:.2}", (i as f64) * 0.99);
            row.extend_from_slice(format!("{:>8}", dollars).as_bytes());
            row.push(b' ');
            row.extend_from_slice(b"NONE");
            row.push(b' ');
            row.push(b'0');
            row.push(b' ');
            row.push(if i % 2 == 0 { b'0' } else { b'1' });
            row.extend_from_slice(b"\r\n");
            assert_eq!(
                row.len(),
                RECORD_LEN,
                "row build failed at i={}: got {} bytes",
                i,
                row.len()
            );
            f.write_all(&row).unwrap();
        }
        let id = SourceIdentity {
            path: p.clone(),
            year: 1,
            category: "beer".to_string(),
            channel: Channel::Drug,
            filename_week_start: 1114,
            filename_week_end: 1165,
        };
        (p, id)
    }

    #[test]
    fn ingests_a_small_fixture() {
        let dir = tempfile::tempdir().unwrap();
        let (p, _) = make_fixture(dir.path(), 32);
        let cfg = IngestConfig::for_test();
        let lake = dir.path().join("lake");
        let outcome = ingest_file(&p, dir.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
        let stats = match outcome {
            IngestOutcome::Completed(_, s) => s,
            IngestOutcome::Skipped(_) => panic!("expected fresh ingest"),
        };
        assert_eq!(stats.written_rows, 32);
        assert!(stats.output_paths[0].exists());
    }

    #[test]
    fn second_ingest_skips_via_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let (p, _) = make_fixture(dir.path(), 16);
        let cfg = IngestConfig::for_test();
        let lake = dir.path().join("lake");
        let _ = ingest_file(&p, dir.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
        let second = ingest_file(&p, dir.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
        match second {
            IngestOutcome::Skipped(r) => {
                assert_eq!(r.status, ManifestStatus::Success);
            }
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn refuses_to_overwrite_when_outputs_remain() {
        let dir = tempfile::tempdir().unwrap();
        let (p, _) = make_fixture(dir.path(), 8);
        let cfg = IngestConfig::for_test();
        let lake = dir.path().join("lake");
        let _ = ingest_file(&p, dir.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
        // Second run with --resume semantics should skip.
        let second = ingest_file(&p, dir.path(), &lake, &cfg, &IngestFilter::default()).unwrap();
        assert!(matches!(second, IngestOutcome::Skipped(_)));
    }
}
