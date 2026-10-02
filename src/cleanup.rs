//! Cleanup of stale `*.tmp` siblings left by an interrupted ingest.
//!
//! `parquet_output::write_parquet_atomic` writes every Parquet file to a
//! `.tmp` sibling and only renames it into place once the writer has
//! closed cleanly. A run that is killed mid-write therefore leaves a
//! `.tmp` file behind and — because `ManifestStatus::InProgress` is
//! never written (gap G5 in `docs/corpus_readiness.md`) — no manifest
//! line marking the interrupted file. The leftover is inert, but it
//! accumulates and it confuses anyone globbing the lake.
//!
//! This module sweeps those leftovers automatically. It is deliberately
//! conservative:
//!
//! - Only files whose extension is exactly `tmp` are considered, so a
//!   real `.parquet` is never a candidate.
//! - Only files older than `max_age` are removed. A `.tmp` younger than
//!   that may belong to a *concurrent* run writing to the same output
//!   root; deleting it would corrupt an in-flight ingest. The age gate
//!   is what makes it safe to call this at the start of every run.
//! - Every removal and every failure is reported, never silent.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use walkdir::WalkDir;

/// Default minimum age of a `*.tmp` file before it is considered stale.
///
/// 24 hours is comfortably longer than the longest plausible single-file
/// ingest (a 1.3 GB / 220 M row file), so a healthy run never sees its
/// own in-flight temporary file deleted.
pub const DEFAULT_TMP_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// Outcome of one sweep of an output root.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// `*.tmp` files found, regardless of age.
    pub found: usize,
    /// Files deleted.
    pub removed: Vec<PathBuf>,
    /// Files left alone because they were younger than the age gate.
    pub too_recent: usize,
    /// Bytes reclaimed by the deletions.
    pub bytes_reclaimed: u64,
    /// Files that could not be inspected or removed, with the reason.
    pub failures: Vec<(PathBuf, String)>,
}

impl CleanupReport {
    /// True when the sweep found nothing to do.
    pub fn is_empty(&self) -> bool {
        self.found == 0
    }

    /// One-line summary suitable for logging or stdout.
    pub fn summary(&self) -> String {
        format!(
            "found={} removed={} too_recent={} bytes_reclaimed={} failures={}",
            self.found,
            self.removed.len(),
            self.too_recent,
            self.bytes_reclaimed,
            self.failures.len()
        )
    }
}

/// True when `path` is a candidate for removal: a file with a `.tmp`
/// extension. The check is on the extension only, so it holds for both
/// `part-1a2b3c4d-0001.parquet.tmp` and a bare `scratch.tmp`.
fn is_tmp_file(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("tmp"))
}

/// Age of `path` by modification time.
///
/// Returns `None` if the metadata cannot be read. A modification time in
/// the future (clock skew, or a filesystem with a coarse timestamp) is
/// reported as age zero rather than a negative duration, so the caller
/// treats it as recent and leaves the file alone.
fn age_of(path: &Path) -> Option<Duration> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    Some(
        SystemTime::now()
            .duration_since(modified)
            .unwrap_or(Duration::ZERO),
    )
}

/// Remove stale `*.tmp` files from beneath `output_root`.
///
/// Files older than `max_age` are deleted; younger ones are counted in
/// `too_recent` and left in place. A missing `output_root` is not an
/// error — there is simply nothing to clean — so this is safe to call on
/// a fresh output root before any ingest has run.
pub fn cleanup_stale_tmp(output_root: &Path, max_age: Duration) -> CleanupReport {
    let mut report = CleanupReport::default();
    if !output_root.exists() {
        return report;
    }

    for entry in WalkDir::new(output_root)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if !is_tmp_file(path) {
            continue;
        }
        report.found += 1;

        let Some(age) = age_of(path) else {
            report
                .failures
                .push((path.to_path_buf(), "cannot read mtime".to_string()));
            continue;
        };
        if age < max_age {
            report.too_recent += 1;
            continue;
        }

        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(path) {
            Ok(()) => {
                report.bytes_reclaimed += size;
                report.removed.push(path.to_path_buf());
            }
            Err(e) => report.failures.push((path.to_path_buf(), e.to_string())),
        }
    }

    report
}

/// Convenience wrapper around [`cleanup_stale_tmp`] using
/// [`DEFAULT_TMP_MAX_AGE`].
pub fn cleanup_stale_tmp_default(output_root: &Path) -> CleanupReport {
    cleanup_stale_tmp(output_root, DEFAULT_TMP_MAX_AGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn touch(path: &Path, contents: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = File::create(path).unwrap();
        f.write_all(contents).unwrap();
        f.sync_all().unwrap();
    }

    /// Backdate `path` by `age` so the age gate sees it as stale.
    fn backdate(path: &Path, age: Duration) {
        let when = SystemTime::now() - age;
        let f = File::options().write(true).open(path).unwrap();
        f.set_modified(when).unwrap();
    }

    fn tmpdir() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn removes_stale_tmp_in_nested_dirs_only() {
        let dir = tmpdir();
        let root = dir.path();
        touch(
            &root.join("bronze/iri_sales/year=1/category=beer/part-a-0001.parquet.tmp"),
            b"x",
        );
        touch(
            &root.join("bronze/iri_sales/year=1/category=beer/part-a-0001.parquet"),
            b"keep",
        );
        touch(&root.join("metadata/manifest.jsonl"), b"keep");
        for p in [
            root.join("bronze/iri_sales/year=1/category=beer/part-a-0001.parquet.tmp"),
            root.join("bronze/iri_sales/year=1/category=beer/part-a-0001.parquet"),
            root.join("metadata/manifest.jsonl"),
        ] {
            backdate(&p, Duration::from_secs(48 * 3600));
        }

        let r = cleanup_stale_tmp(root, DEFAULT_TMP_MAX_AGE);
        assert_eq!(r.found, 1);
        assert_eq!(r.removed.len(), 1);
        assert_eq!(r.bytes_reclaimed, 1);
        assert!(r.failures.is_empty());
        assert!(!root
            .join("bronze/iri_sales/year=1/category=beer/part-a-0001.parquet.tmp")
            .exists());
        // Siblings untouched.
        assert!(root
            .join("bronze/iri_sales/year=1/category=beer/part-a-0001.parquet")
            .exists());
        assert!(root.join("metadata/manifest.jsonl").exists());
    }

    #[test]
    fn keeps_recent_tmp() {
        let dir = tmpdir();
        let p = dir.path().join("part-a-0001.parquet.tmp");
        touch(&p, b"in flight");
        backdate(&p, Duration::from_secs(60));

        let r = cleanup_stale_tmp(dir.path(), DEFAULT_TMP_MAX_AGE);
        assert_eq!(r.found, 1);
        assert_eq!(r.too_recent, 1);
        assert!(r.removed.is_empty());
        assert!(
            p.exists(),
            "a young .tmp may be another run's in-flight write"
        );
    }

    #[test]
    fn missing_output_root_is_not_an_error() {
        let dir = tmpdir();
        let r = cleanup_stale_tmp(&dir.path().join("does/not/exist"), DEFAULT_TMP_MAX_AGE);
        assert!(r.is_empty());
        assert!(r.failures.is_empty());
    }

    #[test]
    fn age_gate_is_honoured() {
        let dir = tmpdir();
        let old = dir.path().join("old.parquet.tmp");
        let young = dir.path().join("young.parquet.tmp");
        touch(&old, b"aaaa");
        touch(&young, b"aaaa");
        backdate(&old, Duration::from_secs(2 * 3600));
        backdate(&young, Duration::from_secs(30));

        let r = cleanup_stale_tmp(dir.path(), Duration::from_secs(3600));
        assert_eq!(r.removed, vec![old.clone()]);
        assert_eq!(r.too_recent, 1);
        assert!(young.exists());
    }
}
