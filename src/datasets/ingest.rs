//! Generic ingest driver for the non-sales datasets.
//!
//! Every class in [`crate::dataset`] differs in how bytes become rows,
//! and shares everything else: the SHA-256 that makes the skip decision
//! trustworthy, the manifest lookup and append, the atomic Parquet
//! write, and the failure-is-a-value policy that keeps one bad source
//! from abandoning the rest of a run.
//!
//! That shared part is here. The varying part is a [`DatasetParser`],
//! which the per-dataset modules implement.
//!
//! # The contract a parser implements
//!
//! ```text
//! open(source) -> Parser                    read the header, decide the schema
//! Parser::parse(&mut self, rows: &mut RecordBatchSink) -> Result<()>
//! Parser::finish(self) -> Result<()>
//! ```
//!
//! A parser pushes finished `RecordBatch`es into the sink; the driver
//! decides where each one is written. This split exists because the
//! *row shape* is what varies: `iri_week_dimension` yields one
//! 627-row batch from a workbook, while `iri_product_attr` streams
//! 10 MB files and needs a batch every million rows.
//!
//! The alternative — a `Vec<u8>` in, a `RecordBatch` out, per file —
//! would force the small classes to materialise their whole source and
//! the large ones to be re-parsed per batch. Neither is a constraint
//! any of these eleven formats actually imposes.

use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use chrono::Utc;
use rayon::prelude::*;
use uuid::Uuid;

use crate::config::{IngestConfig, OverwriteMode};
use crate::dataset::DatasetFile;
use crate::errors::{IngestError, Result};
use crate::manifest::{skip_decision, SharedManifest};
use crate::metrics::{sha256_of_file, Timer};
use crate::model::{DatasetKind, ManifestRecord, ManifestStatus, PARSER_VERSION};
use crate::parquet_output::write_parquet_atomic;

/// Where a parser's batches go, and how they are named.
///
/// The driver owns this rather than the parser so that the
/// `part-<run>-NNNN.parquet` naming, the per-file output list, and the
/// atomic-rename discipline are identical across all eleven classes.
/// One naming scheme means `find data/lake -name '*.parquet'` finds
/// everything and nothing looks foreign.
///
/// Batches accumulate until they reach `max_rows_per_file` and are
/// then written as one Parquet file. A parser that emits batches
/// smaller than that gets them coalesced, which is what makes it safe
/// for a CSV parser to flush every 64 K rows regardless of dataset:
/// the file boundary is the driver's decision, not the parser's.
#[derive(Debug)]
pub struct BatchSink {
    output_dir: PathBuf,
    run_short: String,
    schema: SchemaRef,
    max_rows_per_file: usize,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    next_seq: usize,
    /// Output paths written so far, in order.
    pub written: Vec<PathBuf>,
    pub bytes: u64,
    pub rows: u64,
    pub files: usize,
}

impl BatchSink {
    fn new(
        output_dir: PathBuf,
        run_short: String,
        schema: SchemaRef,
        max_rows_per_file: usize,
    ) -> Self {
        Self {
            output_dir,
            run_short,
            schema,
            max_rows_per_file: max_rows_per_file.max(1),
            pending: Vec::new(),
            pending_rows: 0,
            next_seq: 0,
            written: Vec::new(),
            bytes: 0,
            rows: 0,
            files: 0,
        }
    }

    /// Accept one batch from the parser.
    ///
    /// A batch larger than the whole-file budget is split on the row
    /// axis with `RecordBatch::slice`, which is a zero-copy view over
    /// the same buffers — so a parser that hands over one enormous
    /// batch for a 10 MB source still produces readable files instead
    /// of a single Parquet row group larger than any reader wants.
    pub fn push(&mut self, batch: RecordBatch, cfg: &IngestConfig) -> Result<()> {
        let n = batch.num_rows();
        self.rows += n as u64;
        if n > self.max_rows_per_file {
            self.flush(cfg)?;
            let mut offset = 0;
            while offset < n {
                let take = (n - offset).min(self.max_rows_per_file);
                let part = batch.slice(offset, take);
                self.pending_rows += part.num_rows();
                self.pending.push(part);
                if self.pending_rows >= self.max_rows_per_file {
                    self.flush(cfg)?;
                }
                offset += take;
            }
            return Ok(());
        }
        self.pending_rows += n;
        self.pending.push(batch);
        if self.pending_rows >= self.max_rows_per_file {
            self.flush(cfg)?;
        }
        Ok(())
    }

    /// Write everything buffered. Callers must call this once the
    /// parser is done, so the last partial file is not lost.
    pub fn flush(&mut self, cfg: &IngestConfig) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        let path = self.next_path();
        let size = write_parquet_atomic(&path, self.schema.clone(), &batches, cfg)?;
        self.bytes += size;
        self.written.push(path);
        Ok(())
    }

    fn next_path(&mut self) -> PathBuf {
        self.next_seq += 1;
        self.output_dir.join(format!(
            "part-{}-{:04}.parquet",
            self.run_short, self.next_seq
        ))
    }

    /// Test-only constructor: a sink with no Parquet output, so a test
    /// can drive a parser and inspect the batches it produced.
    #[cfg(test)]
    pub(crate) fn new_test(output_dir: PathBuf, schema: SchemaRef) -> Self {
        Self::new(output_dir, "test".into(), schema, 1_000_000)
    }

    /// Test-only: concatenate everything written back into one batch,
    /// so a test can assert on column values without opening Parquet.
    #[cfg(test)]
    pub(crate) fn read_all(&self, _cfg: &IngestConfig) -> RecordBatch {
        let mut batches: Vec<RecordBatch> = Vec::new();
        for p in &self.written {
            let f = std::fs::File::open(p).unwrap();
            let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .build()
                .unwrap();
            for b in r {
                batches.push(b.unwrap());
            }
        }
        if batches.is_empty() {
            return RecordBatch::new_empty(self.schema.clone());
        }
        arrow::compute::concat_batches(&self.schema, &batches).unwrap()
    }
}

/// A per-class row parser.
pub trait DatasetParser: Send {
    /// The Arrow schema this parser will produce. Must be decided
    /// before any batch is pushed, because the sink writes each file
    /// with it.
    fn schema(&self) -> SchemaRef;

    /// Parse rows into the sink. Called once per source file.
    fn parse(&mut self, sink: &mut BatchSink, cfg: &IngestConfig) -> Result<()>;

    /// Row count the source implied, for the manifest. Defaults to
    /// whatever the sink actually wrote.
    fn expected_rows(&self) -> Option<u64> {
        None
    }

    /// Free-form note recorded on the manifest record — e.g. the
    /// dialect a PANEL file turned out to be, or the columns a
    /// per-file schema turned out to have. This is provenance that
    /// would otherwise only exist in a log.
    fn note(&self) -> Option<String> {
        None
    }

    /// Identity of the source's own schema, recorded on the manifest.
    ///
    /// `None` for the classes whose schema is the class's (a constant,
    /// already carried by `output_schema_version`). The per-file-schema
    /// classes return a digest of their declared column names, so
    /// editing a source's header forces a re-ingest and an unchanged
    /// header does not.
    fn schema_fingerprint(&self) -> Option<String> {
        None
    }
}

/// Open the parser for one source file.
pub type ParserFactory = fn(&Path, &crate::dataset::SourceRef) -> Result<Box<dyn DatasetParser>>;

/// One file's result plus its wall time.
///
/// The elapsed time rides alongside the outcome rather than inside it:
/// `DatasetOutcome` carries a `ManifestRecord`, which is the persisted
/// artefact, and a measurement taken for the run summary does not belong
/// on disk.
#[derive(Debug)]
pub struct FileResult {
    pub outcome: Result<DatasetOutcome>,
    pub elapsed: std::time::Duration,
}

/// Outcome of ingesting one non-sales source file.
#[derive(Debug)]
pub enum DatasetOutcome {
    /// A prior successful run already produced this output.
    Skipped(ManifestRecord),
    Completed(ManifestRecord),
    /// The source was recognised but has no rows (a zero-byte PANEL
    /// file, an empty workbook sheet). Not a failure: the file is real
    /// and its emptiness is a fact about the corpus, recorded as a
    /// success with zero rows so it never retries.
    Empty(ManifestRecord),
}

/// Per-class tuning, supplied by the class module rather than guessed
/// centrally: a 12-row cross-reference and a 11 MB attribute file want
/// very different batch sizes.
#[derive(Debug, Clone, Copy)]
pub struct DatasetTuning {
    /// Rows buffered in Arrow before a batch is flushed.
    pub batch_rows: usize,
    /// Max rows per output Parquet file. A batch larger than this is
    /// split on the row axis.
    pub max_rows_per_file: usize,
    pub parser: ParserFactory,
}

impl DatasetTuning {
    pub const fn new(batch_rows: usize, max_rows_per_file: usize, parser: ParserFactory) -> Self {
        Self {
            batch_rows,
            max_rows_per_file,
            parser,
        }
    }
}

/// Ingest one non-sales source file end-to-end.
///
/// Mirrors `ingest::ingest_file` step for step — identity, SHA-256,
/// skip decision, parse, atomic write, manifest append — because the
/// guarantees a caller gets must not depend on which pipeline ran.
pub fn ingest_dataset_file(
    file: &DatasetFile,
    output_root: &Path,
    config: &IngestConfig,
    tuning: &DatasetTuning,
    store: &SharedManifest,
) -> Result<DatasetOutcome> {
    let path = &file.source.path;
    let metadata = std::fs::metadata(path).map_err(|e| IngestError::io(path, e))?;
    let size = metadata.len();

    // ---- 1. SHA-256 --------------------------------------------------
    // Before the skip decision, because the hash is what makes the
    // decision trustworthy. Same policy as the sales pipeline.
    let source_sha256 = sha256_of_file(path)?;

    // ---- 2. Prospective record + skip decision -----------------------
    let run_id = Uuid::new_v4().to_string();
    let run_short = run_id[..8].to_string();
    let prior = store.last_for_path(path)?;

    // The class's declared schema version, not `CURRENT_SCHEMA_VERSION`
    // and not the parser's: the prospective record is built *before*
    // the parser exists, and both records must carry the same value or
    // no source is ever skippable.
    let schema_version = file.kind.schema_version();
    let prospective = ManifestRecord {
        run_id: run_id.clone(),
        dataset: file.kind,
        source_path: path.clone(),
        source_size_bytes: size,
        source_sha256: source_sha256.clone(),
        source_year: file.source.year,
        category: file.source.category.clone(),
        // `outlet` has no dedicated column; it rides in `category`
        // only when there is no category, so a PANEL record keeps both
        // its category and its outlet.
        channel: None,
        filename_week_start: file.source.week_start,
        filename_week_end: file.source.week_end,
        deduplicated_from: None,
        source_schema_fingerprint: None,
        expected_rows: 0,
        written_rows: 0,
        rejected_rows: 0,
        parser_version: PARSER_VERSION.into(),
        output_schema_version: schema_version,
        compression: config.compression.clone(),
        batch_rows: tuning.batch_rows,
        row_group_rows: config.parquet_row_group_rows,
        output_paths: Vec::new(),
        output_size_bytes: 0,
        started_at: Utc::now(),
        completed_at: None,
        duration_ms: None,
        status: ManifestStatus::InProgress,
        error_message: None,
    };

    if let Some(prior_ok) = skip_decision(&prospective, prior.as_ref())? {
        match config.overwrite {
            OverwriteMode::SkipIfPresent => return Ok(DatasetOutcome::Skipped(prior_ok)),
            OverwriteMode::Refuse => {
                return Err(IngestError::OutputsExist {
                    path: path.clone(),
                    run_id: prior_ok.run_id,
                })
            }
            OverwriteMode::Overwrite => {
                tracing::info!(
                    source = %path.display(),
                    prior_run = %prior_ok.run_id,
                    "overwriting prior output (--overwrite)"
                );
                // Reap only after the replacement is durably recorded.
                let _ = reap(&prior_ok.output_paths);
            }
        }
    }

    // ---- 3. Parse + write -------------------------------------------
    let timer = Timer::start();
    let mut parser = (tuning.parser)(path, &file.source)?;
    let schema = parser.schema();
    let output_dir = crate::dataset::bronze_root_for(file.kind, output_root)
        .join(file.source.partition_dir_for(file.kind));
    let mut sink = BatchSink::new(
        output_dir,
        run_short.clone(),
        schema,
        tuning.max_rows_per_file,
    );
    parser.parse(&mut sink, config)?;
    // Flush the trailing partial file. A parser is not required to
    // leave the sink empty-handed: forgetting this is the one way to
    // lose rows, so the driver does it rather than trusting 11
    // parsers not to.
    sink.flush(config)?;

    let elapsed = timer.elapsed();
    let rows = sink.rows;
    let record = ManifestRecord {
        run_id,
        dataset: file.kind,
        source_path: path.clone(),
        source_size_bytes: size,
        source_sha256,
        source_year: file.source.year,
        category: file.source.category.clone(),
        channel: None,
        filename_week_start: file.source.week_start,
        filename_week_end: file.source.week_end,
        deduplicated_from: None,
        source_schema_fingerprint: parser.schema_fingerprint(),
        expected_rows: parser.expected_rows().unwrap_or(rows),
        written_rows: rows,
        rejected_rows: 0,
        parser_version: PARSER_VERSION.into(),
        output_schema_version: schema_version,
        compression: config.compression.clone(),
        batch_rows: tuning.batch_rows,
        row_group_rows: config.parquet_row_group_rows,
        output_paths: sink.written.clone(),
        output_size_bytes: sink.bytes,
        started_at: prospective.started_at,
        completed_at: Some(Utc::now()),
        duration_ms: Some(elapsed.as_millis() as u64),
        status: ManifestStatus::Success,
        error_message: parser.note(),
    };
    store.append(&record)?;

    if rows == 0 {
        Ok(DatasetOutcome::Empty(record))
    } else {
        Ok(DatasetOutcome::Completed(record))
    }
}

/// Delete output paths a `--overwrite` run has superseded.
///
/// Best-effort, like the tmp sweep and like the sales pipeline's own
/// reap: these are inert files, and failing to remove one must not fail
/// an otherwise-successful ingest. It does silently double that
/// source's footprint, so each failure is logged.
fn reap(paths: &[PathBuf]) -> Result<()> {
    for p in paths {
        match std::fs::remove_file(p) {
            Ok(()) => tracing::debug!(path = %p.display(), "removed superseded output"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(
                path = %p.display(),
                error = %e,
                "failed to remove superseded output; it stays in the lake"
            ),
        }
    }
    Ok(())
}

/// Summary of one dataset class's run.
#[derive(Debug, Default)]
pub struct DatasetSummary {
    pub kind: Option<DatasetKind>,
    pub completed: usize,
    pub skipped: usize,
    pub empty: usize,
    pub failed: usize,
    pub rows: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub wall: std::time::Duration,
    /// Per-file wall time of the slowest parse+write. Same floor
    /// semantics as the sales summary: a run cannot beat it.
    pub slowest_file: std::time::Duration,
    pub failures: Vec<(PathBuf, IngestError)>,
}

impl DatasetSummary {
    pub fn ok(&self) -> bool {
        self.failed == 0
    }
}

/// Ingest every file of one class, with `workers` rayon threads.
///
/// Runs **sequentially per class** by the umbrella, not across classes.
/// The eleven classes have batch sizes spanning four orders of
/// magnitude and one of them (the stubs) holds an open workbook per
/// file; interleaving them would make the memory profile of the whole
/// run depend on which classes happen to be co-resident. Sequential
/// per class also means a class that exhausts memory costs one class,
/// not eleven.
pub fn ingest_dataset(
    kind: DatasetKind,
    files: &[DatasetFile],
    output_root: &Path,
    config: &IngestConfig,
    workers: usize,
    tuning: &DatasetTuning,
) -> Result<DatasetSummary> {
    if workers == 0 {
        return Err(IngestError::Config("workers must be >= 1".into()));
    }
    let store = SharedManifest::open(output_root)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|i| format!("iri-lake-{i}"))
        .build()
        .map_err(|e| IngestError::Config(format!("build worker pool: {e}")))?;

    let started_at = Utc::now();
    let started = std::time::Instant::now();
    // Largest-first, then reversed, so the long poles are handed out
    // early by rayon's work-stealing deque — the same reasoning and the
    // same measurement as the sales pipeline (docs/parallelism.md).
    let mut ordered: Vec<&DatasetFile> = files.iter().collect();
    ordered.sort_by_key(|f| std::cmp::Reverse(f.size_bytes));
    ordered.reverse();

    let results: Vec<FileResult> = pool.install(|| {
        ordered
            .par_iter()
            .map(|f| {
                let t = Timer::start();
                let outcome = ingest_dataset_file(f, output_root, config, tuning, &store);
                FileResult {
                    outcome,
                    elapsed: t.elapsed(),
                }
            })
            .collect()
    });
    let wall = started.elapsed();

    let mut summary = DatasetSummary {
        kind: Some(kind),
        wall,
        ..Default::default()
    };
    for (f, FileResult { outcome, elapsed }) in ordered.iter().zip(results) {
        match outcome {
            Ok(DatasetOutcome::Skipped(_)) => summary.skipped += 1,
            Ok(DatasetOutcome::Completed(rec)) => {
                summary.completed += 1;
                summary.rows += rec.written_rows;
                summary.bytes_out += rec.output_size_bytes;
                summary.bytes_in += f.size_bytes;
                summary.slowest_file = summary.slowest_file.max(elapsed);
                tracing::info!(
                    dataset = %kind,
                    source = %f.source.path.display(),
                    rows = rec.written_rows,
                    bytes_out = rec.output_size_bytes,
                    elapsed_s = elapsed.as_secs_f64(),
                    "ingested"
                );
            }
            Ok(DatasetOutcome::Empty(rec)) => {
                summary.empty += 1;
                summary.bytes_in += f.size_bytes;
                summary.slowest_file = summary.slowest_file.max(elapsed);
                tracing::info!(
                    dataset = %kind,
                    source = %f.source.path.display(),
                    rows = rec.written_rows,
                    "source has no rows; recorded as an empty success"
                );
                debug_assert_eq!(rec.written_rows, 0);
            }
            Err(e) => {
                summary.failed += 1;
                tracing::warn!(
                    dataset = %kind,
                    source = %f.source.path.display(),
                    error = ?e,
                    "ingest failed"
                );
                // Record the failure, exactly as the sales pipeline
                // does: stdout is what a re-run loses.
                let record = failure_record(f, &e, config, tuning, started_at);
                if let Err(write_err) = store.append(&record) {
                    tracing::error!(
                        source = %f.source.path.display(),
                        error = ?write_err,
                        "could not record the failure in the manifest"
                    );
                }
                summary.failures.push((f.source.path.clone(), e));
            }
        }
    }
    Ok(summary)
}

/// The `status: "failed"` record for a source that could not be
/// ingested. Mirrors `ingest::failure_record`, including its two
/// deliberate blanks: the hash is empty (hashing either failed or was
/// never reached) and `written_rows` is 0. Neither can affect a skip
/// decision, because only a `Success` is ever honoured.
fn failure_record(
    f: &DatasetFile,
    error: &IngestError,
    config: &IngestConfig,
    tuning: &DatasetTuning,
    started_at: chrono::DateTime<chrono::Utc>,
) -> ManifestRecord {
    ManifestRecord {
        run_id: Uuid::new_v4().to_string(),
        dataset: f.kind,
        source_path: f.source.path.clone(),
        source_size_bytes: f.size_bytes,
        source_sha256: String::new(),
        source_year: f.source.year,
        category: f.source.category.clone(),
        channel: None,
        filename_week_start: f.source.week_start,
        filename_week_end: f.source.week_end,
        deduplicated_from: None,
        source_schema_fingerprint: None,
        expected_rows: 0,
        written_rows: 0,
        rejected_rows: 0,
        parser_version: PARSER_VERSION.into(),
        output_schema_version: f.kind.schema_version(),
        compression: config.compression.clone(),
        batch_rows: tuning.batch_rows,
        row_group_rows: config.parquet_row_group_rows,
        output_paths: Vec::new(),
        output_size_bytes: 0,
        started_at,
        completed_at: Some(Utc::now()),
        duration_ms: Some(
            Utc::now()
                .signed_duration_since(started_at)
                .num_milliseconds()
                .max(0) as u64,
        ),
        status: ManifestStatus::Failed,
        error_message: Some(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, UInt32Builder};

    fn schema() -> SchemaRef {
        std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("iri_key", arrow_schema::DataType::UInt32, false),
            arrow_schema::Field::new("n", arrow_schema::DataType::Int32, false),
        ]))
    }

    fn batch(rows: usize) -> RecordBatch {
        let mut k = UInt32Builder::with_capacity(rows);
        let mut n = Int32Builder::with_capacity(rows);
        for i in 0..rows {
            k.append_value(i as u32);
            n.append_value(i as i32);
        }
        RecordBatch::try_new(
            schema(),
            vec![
                std::sync::Arc::new(k.finish()),
                std::sync::Arc::new(n.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn small_batches_coalesce_into_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = IngestConfig::for_test();
        let mut sink = BatchSink::new(dir.path().to_path_buf(), "run".into(), schema(), 100);
        for _ in 0..10 {
            sink.push(batch(10), &cfg).unwrap();
        }
        sink.flush(&cfg).unwrap();
        assert_eq!(sink.rows, 100);
        assert_eq!(
            sink.written.len(),
            1,
            "100 rows at a 100-row budget is one file"
        );
        assert!(sink.written[0]
            .to_string_lossy()
            .ends_with("part-run-0001.parquet"));
    }

    #[test]
    fn oversized_batch_is_split_on_the_row_axis() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = IngestConfig::for_test();
        let mut sink = BatchSink::new(dir.path().to_path_buf(), "run".into(), schema(), 100);
        // One batch of 250 rows against a 100-row file budget.
        sink.push(batch(250), &cfg).unwrap();
        sink.flush(&cfg).unwrap();
        assert_eq!(sink.rows, 250, "splitting must not lose or double rows");
        assert_eq!(sink.written.len(), 3);
        for (i, p) in sink.written.iter().enumerate() {
            assert!(p.exists(), "{p:?} should exist");
            assert!(p
                .to_string_lossy()
                .ends_with(&format!("part-run-{:04}.parquet", i + 1)));
        }
    }

    #[test]
    fn flush_on_an_empty_sink_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = IngestConfig::for_test();
        let mut sink = BatchSink::new(dir.path().to_path_buf(), "run".into(), schema(), 100);
        sink.flush(&cfg).unwrap();
        assert!(sink.written.is_empty());
        assert_eq!(sink.rows, 0);
    }
}
/// A short, stable digest of a list of column names.
///
/// Used as the `source_schema_fingerprint` for the classes whose schema
/// is a property of the source file. `sha256` truncated to 16 hex
/// characters is far more than enough to distinguish two column lists
/// and short enough to read in a manifest line.
pub fn fingerprint(columns: impl IntoIterator<Item = String>) -> String {
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    for c in columns {
        sha2::Digest::update(&mut hasher, c.as_bytes());
        // A separator so ["ab", "c"] and ["a", "bc"] differ.
        sha2::Digest::update(&mut hasher, b"\x00");
    }
    hex::encode(sha2::Digest::finalize(hasher))[..16].to_string()
}

#[cfg(test)]
mod fingerprint_tests {
    use super::fingerprint;

    fn fp(v: &[&str]) -> String {
        fingerprint(v.iter().map(|s| s.to_string()))
    }

    #[test]
    fn is_stable_and_order_sensitive() {
        assert_eq!(fp(&["a", "b"]), fp(&["a", "b"]));
        assert_ne!(fp(&["a", "b"]), fp(&["b", "a"]));
        // Column *order* is part of the schema, so it must matter.
        assert_ne!(fp(&["sy", "ge", "item"]), fp(&["sy", "item", "ge"]));
        // And the separator prevents an ambiguous split.
        assert_ne!(fp(&["ab", "c"]), fp(&["a", "bc"]));
    }

    #[test]
    fn is_short_enough_to_read_in_a_manifest_line() {
        assert_eq!(fp(&["week"]).len(), 16);
        assert!(fp(&["week"]).chars().all(|c| c.is_ascii_hexdigit()));
    }
}
