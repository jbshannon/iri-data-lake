//! Runtime configuration: defaults, env overrides, CLI overrides.
//!
//! The defaults below are tuned for a local NVMe machine and must be
//! benchmarked — see `BENCHMARKING.md`.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Number of rows to buffer in memory before flushing to Parquet.
    pub batch_rows: usize,
    /// Target row-group size in the Parquet writer. Often a multiple of
    /// `batch_rows`; the writer splits at flush boundaries anyway.
    pub parquet_row_group_rows: usize,
    /// Compression codec name: `zstd`, `zstd-N`, `snappy`, `lz4`,
    /// `lz4_raw`, `uncompressed`.
    pub compression: String,
    /// Number of worker threads. `None` = "let rayon decide".
    pub worker_threads: Option<usize>,
    /// Default input root for `inventory` and `ingest-all`.
    pub input_root: PathBuf,
    /// Default output root for `ingest` and `ingest-all`.
    pub output_root: PathBuf,
    /// Behaviour when an output Parquet file already exists.
    pub overwrite: OverwriteMode,
    /// Behaviour for unexpected feature codes (`F` field).
    pub unknown_feature: UnknownFeaturePolicy,
    /// When true, sample-validate that each row's `WEEK` lies within the
    /// filename's declared range. Disable for full-corpus runs where
    /// mismatches are known to be absent.
    pub week_range_strict: bool,
    /// Minimum age, in hours, a `*.tmp` file under the output root must
    /// reach before the automatic pre-run cleanup deletes it (G5). The
    /// age gate keeps the sweep from deleting a `.tmp` that a concurrent
    /// run is still writing.
    pub tmp_max_age_hours: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OverwriteMode {
    /// Refuse to overwrite an existing output file.
    Refuse,
    /// Overwrite (delete and replace) without warning.
    Overwrite,
    /// Skip the source file if the matching output already exists.
    SkipIfPresent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnknownFeaturePolicy {
    /// Fail the entire source file with a clear error.
    Fail,
    /// Emit a warning and write `0` (NONE) for the offending rows.
    WarnAndTreatAsNone,
    /// Emit a warning and write `255` to mark unrecognised values.
    WarnAndTreatAsUnknown,
}

impl IngestConfig {
    /// Reasonable defaults for a local NVMe machine.
    pub fn defaults() -> Self {
        Self {
            batch_rows: 1_000_000,
            parquet_row_group_rows: 4_000_000,
            compression: "zstd".to_string(),
            worker_threads: Some(num_cpus()),
            input_root: PathBuf::from("data/raw"),
            output_root: PathBuf::from("data/lake"),
            overwrite: OverwriteMode::Refuse,
            unknown_feature: UnknownFeaturePolicy::Fail,
            week_range_strict: false,
            tmp_max_age_hours: crate::cleanup::DEFAULT_TMP_MAX_AGE.as_secs() / 3600,
        }
    }

    /// Used by unit tests — gives a deterministic config that doesn't
    /// depend on the host machine's CPU count.
    ///
    /// `overwrite` mirrors the effective CLI default (`SkipIfPresent`),
    /// which is what `ingest` / `ingest-all` resolve to when neither
    /// `--resume` nor `--overwrite` is passed. `defaults()` declares
    /// `Refuse` for library callers that have not chosen a policy.
    pub fn for_test() -> Self {
        let mut c = Self::defaults();
        c.worker_threads = Some(2);
        c.batch_rows = 1024;
        c.parquet_row_group_rows = 1024;
        c.overwrite = OverwriteMode::SkipIfPresent;
        c
    }

    pub fn with_overrides(mut self, overrides: ConfigOverrides) -> Self {
        if let Some(batch_rows) = overrides.batch_rows {
            self.batch_rows = batch_rows;
        }
        if let Some(rg) = overrides.parquet_row_group_rows {
            self.parquet_row_group_rows = rg;
        }
        if let Some(c) = overrides.compression {
            self.compression = c;
        }
        if let Some(w) = overrides.worker_threads {
            self.worker_threads = Some(w);
        }
        if let Some(p) = overrides.input_root {
            self.input_root = p;
        }
        if let Some(p) = overrides.output_root {
            self.output_root = p;
        }
        if let Some(o) = overrides.overwrite {
            self.overwrite = o;
        }
        if let Some(u) = overrides.unknown_feature {
            self.unknown_feature = u;
        }
        if let Some(h) = overrides.tmp_max_age_hours {
            self.tmp_max_age_hours = h;
        }
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    pub batch_rows: Option<usize>,
    pub parquet_row_group_rows: Option<usize>,
    pub compression: Option<String>,
    pub worker_threads: Option<usize>,
    pub input_root: Option<PathBuf>,
    pub output_root: Option<PathBuf>,
    pub overwrite: Option<OverwriteMode>,
    pub unknown_feature: Option<UnknownFeaturePolicy>,
    pub tmp_max_age_hours: Option<u64>,
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(16) // cap so an oversubscribed CI box doesn't go wild
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = IngestConfig::defaults();
        assert_eq!(c.tmp_max_age_hours, 24);
        assert_eq!(c.batch_rows, 1_000_000);
        assert_eq!(c.parquet_row_group_rows, 4_000_000);
        assert_eq!(c.compression, "zstd");
        assert!(c.worker_threads.unwrap_or(0) >= 1);
    }

    #[test]
    fn overrides_apply() {
        let c = IngestConfig::defaults().with_overrides(ConfigOverrides {
            batch_rows: Some(250_000),
            compression: Some("snappy".into()),
            input_root: Some(PathBuf::from("/tmp/raw")),
            ..Default::default()
        });
        assert_eq!(c.batch_rows, 250_000);
        assert_eq!(c.compression, "snappy");
        assert_eq!(c.input_root, PathBuf::from("/tmp/raw"));
        // unchanged
        assert_eq!(c.parquet_row_group_rows, 4_000_000);
    }
}
