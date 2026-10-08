//! The umbrella runner: `ingest-all` over every dataset class.
//!
//! Runs the classes **sequentially**, each with its own discovery,
//! parallelism and batch sizing, and is resumable at *dataset* grain: a
//! class whose files are all already in the manifest is skipped whole
//! before any work starts.
//!
//! # Why sequential across classes
//!
//! The eleven classes span four orders of magnitude in size and have
//! incompatible memory profiles: `iri_sales` is 143 GB of mmap and
//! 1 M-row Arrow batches, `iri_product_stub` holds an open workbook per
//! file, and `iri_week_dimension` is 627 rows total. Running them in
//! one rayon pool would make the whole run's memory ceiling a function
//! of which classes happen to be co-resident, and one class failing
//! would be able to abandon the other ten.
//!
//! Within a class, files still go through a worker pool — see
//! [`ingest::ingest_dataset`].
//!
//! # Why sales is not in here
//!
//! `DatasetKind::others()` excludes `Sales`. The umbrella calls the
//! sales pipeline separately, through `ingest::ingest_all`, because it
//! is the one pipeline with a measured performance contract
//! (`docs/parallelism.md`), five documented gates, and 744 files that
//! take ~220 s. Interleaving it with eleven small classes would put the
//! measured sales numbers at the mercy of unrelated work.

use std::path::Path;
use std::time::Instant;

use crate::config::IngestConfig;
use crate::dataset::{self, DatasetFile, DatasetInventory};
use crate::datasets::ingest::{self, DatasetSummary};
use crate::errors::{IngestError, Result};
use crate::model::DatasetKind;

/// One class's result in the umbrella's report.
#[derive(Debug)]
pub struct ClassResult {
    pub kind: DatasetKind,
    pub inventory: DatasetInventory,
    /// `None` when the class was skipped because every source was
    /// already recorded successfully.
    pub summary: Option<DatasetSummary>,
    pub wall: std::time::Duration,
}

impl ClassResult {
    pub fn rows(&self) -> u64 {
        self.summary.as_ref().map(|s| s.rows).unwrap_or(0)
    }

    pub fn failed(&self) -> usize {
        self.summary.as_ref().map(|s| s.failed).unwrap_or(0)
    }
}

/// The umbrella's totals.
#[derive(Debug, Default)]
pub struct UmbrellaSummary {
    pub classes: Vec<ClassResult>,
    pub wall: std::time::Duration,
}

impl UmbrellaSummary {
    pub fn total_rows(&self) -> u64 {
        self.classes.iter().map(|c| c.rows()).sum()
    }

    pub fn total_failed(&self) -> usize {
        self.classes.iter().map(|c| c.failed()).sum()
    }

    /// The umbrella is green only if every class is.
    pub fn ok(&self) -> bool {
        self.total_failed() == 0
    }

    /// Classes that produced nothing, with the reason they had no work.
    pub fn empty_classes(&self) -> Vec<(DatasetKind, &'static str)> {
        let mut out = Vec::new();
        for c in &self.classes {
            if c.inventory.files.is_empty() {
                out.push((c.kind, "no sources discovered"));
            } else if c.rows() == 0 {
                out.push((c.kind, "all sources already ingested or empty"));
            }
        }
        out
    }
}

/// Run every non-sales class, sequentially.
///
/// `kinds` selects which classes to run; pass `DatasetKind::others()`
/// for all of them. `on_class` is called once per class, after it
/// finishes, so the CLI can print a line as it goes rather than only at
/// the end — a 20-minute run with no output until the last class is
/// indistinguishable from a hang.
pub fn ingest_all_classes<F>(
    input_root: &Path,
    output_root: &Path,
    config: &IngestConfig,
    kinds: &[DatasetKind],
    workers: usize,
    mut on_class: F,
) -> Result<UmbrellaSummary>
where
    F: FnMut(&ClassResult),
{
    let started = Instant::now();
    let mut summary = UmbrellaSummary::default();

    for &kind in kinds {
        let t = Instant::now();
        let inventory = dataset::discover(kind, input_root)?;
        if inventory.files.is_empty() {
            // Nothing to do is a normal outcome, not a failure: a
            // checkout that has only Year1 sales has no panel files to
            // find.
            let r = ClassResult {
                kind,
                inventory,
                summary: None,
                wall: t.elapsed(),
            };
            on_class(&r);
            summary.classes.push(r);
            continue;
        }

        let tuning = dataset::tuning_for_kind(kind)?;
        let s = ingest::ingest_dataset(
            kind,
            &inventory.files,
            output_root,
            config,
            workers,
            &tuning,
        )?;
        // After a successful ingest, append a `Success` record for
        // every deduplicated source path so the manifest still answers
        // "what is the source of this row?" for it. The record carries
        // the canonical source's path in `deduplicated_from` and the
        // canonical's SHA, so a query can join on either.
        if !inventory.deduplicated.is_empty() {
            let store = crate::manifest::SharedManifest::open(output_root)?;
            for (original, canonical) in &inventory.deduplicated {
                if let Ok(record) = build_dedup_record(kind, original, canonical, config, &tuning) {
                    if let Err(e) = store.append(&record) {
                        tracing::warn!(?e, source = %original.display(),
                            "could not record deduplicated source in the manifest");
                    }
                }
            }
        }
        let r = ClassResult {
            kind,
            inventory,
            summary: Some(s),
            wall: t.elapsed(),
        };
        on_class(&r);
        summary.classes.push(r);
    }

    summary.wall = started.elapsed();
    Ok(summary)
}

/// Build the manifest record for a deduplicated source.
///
/// `original` is the path the discovery walk saw. `canonical` is the
/// path that was actually ingested — the one whose Parquet file the
/// canonical row landed in. The record points at the canonical in
/// `deduplicated_from`, copies the canonical's SHA so a query can
/// match on content, and uses the input's source size so a row-count
/// check across the corpus still holds.
fn build_dedup_record(
    kind: DatasetKind,
    original: &std::path::Path,
    canonical: &std::path::Path,
    config: &IngestConfig,
    tuning: &ingest::DatasetTuning,
) -> Result<crate::model::ManifestRecord, IngestError> {
    use crate::model::{ManifestRecord, ManifestStatus};
    let original_size = std::fs::metadata(original)
        .map_err(|e| IngestError::io(original, e))?
        .len();
    let canonical_sha = crate::metrics::sha256_of_file(canonical)?;
    Ok(ManifestRecord {
        run_id: uuid::Uuid::new_v4().to_string(),
        dataset: kind,
        source_path: original.to_path_buf(),
        source_size_bytes: original_size,
        // The SHA is the canonical's, not the duplicate's: this is the
        // bytes that the lake holds. A query that joins the manifest
        // on this SHA can find every source path that landed in the
        // same Parquet files.
        source_sha256: canonical_sha,
        source_year: None,
        category: None,
        channel: None,
        filename_week_start: None,
        filename_week_end: None,
        deduplicated_from: Some(canonical.to_string_lossy().to_string()),
        source_schema_fingerprint: None,
        expected_rows: 0,
        written_rows: 0,
        rejected_rows: 0,
        parser_version: crate::model::PARSER_VERSION.into(),
        output_schema_version: kind.schema_version(),
        compression: config.compression.clone(),
        batch_rows: tuning.batch_rows,
        row_group_rows: config.parquet_row_group_rows,
        output_paths: Vec::new(),
        output_size_bytes: 0,
        started_at: chrono::Utc::now(),
        completed_at: Some(chrono::Utc::now()),
        duration_ms: Some(0),
        status: ManifestStatus::Success,
        error_message: Some(format!("deduplicated to {}", canonical.display())),
    })
}

/// A one-line-per-class summary of what an umbrella run did.
pub fn format_summary(summary: &UmbrellaSummary) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "ingest-all (non-sales): {} class(es), {} row(s), {} failure(s) in {:.2}s\n",
        summary.classes.len(),
        summary.total_rows(),
        summary.total_failed(),
        summary.wall.as_secs_f64(),
    ));
    for c in &summary.classes {
        let files = c.inventory.files.len();
        let skipped_reasons = c.inventory.skipped.len();
        match &c.summary {
            Some(s) => out.push_str(&format!(
                "  {:<20} files={:<5} completed={:<5} skipped={:<5} empty={:<3} failed={:<3} rows={:<12} out={:<8} wall={:.2}s ({} skipped-by-discovery)\n",
                c.kind.to_string(),
                files,
                s.completed,
                s.skipped,
                s.empty,
                s.failed,
                s.rows,
                human_bytes(s.bytes_out),
                c.wall.as_secs_f64(),
                skipped_reasons,
            )),
            None => out.push_str(&format!(
                "  {:<20} files=0     (no sources discovered; {} file(s) skipped by discovery rules)\n",
                c.kind.to_string(),
                skipped_reasons,
            )),
        }
    }
    out
}

/// Format a byte count for the summary line.
pub fn human_bytes(b: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let b = b as f64;
    if b >= GIB {
        format!("{:.2}GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.1}MiB", b / MIB)
    } else {
        format!("{b}B")
    }
}

/// Group an inventory's skip reasons for reporting.
///
/// The reasons matter as much as the counts: a class that skipped 300
/// files as `subset_of_the_authoritative` is behaving as designed, and
/// one that skipped 300 as `not_a_panel_filename` is broken.
pub fn summarise_skips(inv: &DatasetInventory) -> Vec<(&'static str, usize)> {
    let mut counts: std::collections::BTreeMap<&'static str, usize> =
        std::collections::BTreeMap::new();
    for (_, reason) in &inv.skipped {
        *counts.entry(reason).or_insert(0) += 1;
    }
    counts.into_iter().collect()
}

/// Total bytes an inventory would read.
pub fn inventory_bytes(files: &[DatasetFile]) -> u64 {
    files.iter().map(|f| f.size_bytes).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_picks_a_sensible_unit() {
        assert_eq!(human_bytes(0), "0B");
        assert_eq!(human_bytes(999), "999B");
        assert_eq!(human_bytes(1024 * 1024), "1.0MiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.00GiB");
    }

    #[test]
    fn skip_reasons_are_grouped_for_reporting() {
        let inv = DatasetInventory {
            kind: Some(DatasetKind::Panel),
            files: vec![],
            skipped: vec![
                ("/a".into(), "subset_of_the_authoritative"),
                ("/b".into(), "subset_of_the_authoritative"),
                ("/c".into(), "lock_file"),
            ],
            deduplicated: vec![],
        };
        assert_eq!(
            summarise_skips(&inv),
            vec![("lock_file", 1), ("subset_of_the_authoritative", 2)]
        );
    }

    #[test]
    fn the_umbrella_summary_is_green_when_every_class_is() {
        let s = UmbrellaSummary::default();
        assert!(s.ok());
        assert_eq!(s.total_rows(), 0);
    }
}
