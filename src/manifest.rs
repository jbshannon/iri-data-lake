//! Manifest abstraction. The first implementation writes a JSONL file
//! under `<output_root>/metadata/manifest.jsonl`. A later commit will
//! swap in a Parquet-backed store behind the same trait — schema
//! versions and field names are kept identical so the on-disk data
//! can be re-ingested cleanly.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::errors::IngestError;
use crate::model::{ManifestRecord, ManifestStatus, CURRENT_SCHEMA_VERSION};

/// Abstract store. Implementations may use JSONL, SQLite, or Parquet.
///
/// The interface is deliberately minimal: append one record, look up
/// the most recent record for a source path, list all records. Any
/// other query (counts, summaries) is built on top of these primitives
/// at the call site.
pub trait ManifestStore: Send + Sync {
    fn append(&self, record: &ManifestRecord) -> Result<(), IngestError>;
    fn last_for_path(&self, source_path: &Path) -> Result<Option<ManifestRecord>, IngestError>;
    fn all(&self) -> Result<Vec<ManifestRecord>, IngestError>;
}

/// On-disk path for the default JSONL manifest.
pub fn manifest_path(output_root: &Path) -> PathBuf {
    output_root.join("metadata").join("manifest.jsonl")
}

/// JSONL-backed manifest. Each line is one `ManifestRecord`.
#[derive(Debug, Clone)]
pub struct JsonlManifest {
    path: PathBuf,
}

impl JsonlManifest {
    pub fn open(output_root: &Path) -> Result<Self, IngestError> {
        let path = manifest_path(output_root);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| IngestError::io(parent, e))?;
        }
        // Touch the file so subsequent appends succeed.
        if !path.exists() {
            File::create(&path).map_err(|e| IngestError::io(&path, e))?;
        }
        Ok(Self { path })
    }

    fn parse_line(line: &str) -> Result<Option<ManifestRecord>, IngestError> {
        if line.trim().is_empty() {
            return Ok(None);
        }
        serde_json::from_str::<ManifestRecord>(line)
            .map(Some)
            .map_err(|e| IngestError::manifest(format!("malformed manifest line: {}", e)))
    }
}

impl ManifestStore for JsonlManifest {
    fn append(&self, record: &ManifestRecord) -> Result<(), IngestError> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| IngestError::io(&self.path, e))?;
        let line = serde_json::to_string(record)
            .map_err(|e| IngestError::manifest(format!("serialise: {}", e)))?;
        writeln!(f, "{}", line).map_err(|e| IngestError::io(&self.path, e))?;
        Ok(())
    }

    fn last_for_path(&self, source_path: &Path) -> Result<Option<ManifestRecord>, IngestError> {
        let f = File::open(&self.path).map_err(|e| IngestError::io(&self.path, e))?;
        let reader = BufReader::new(f);
        let mut last: Option<ManifestRecord> = None;
        for line in reader.lines() {
            let line = line.map_err(|e| IngestError::io(&self.path, e))?;
            if let Some(r) = Self::parse_line(&line)? {
                if r.source_path == source_path {
                    last = Some(r);
                }
            }
        }
        Ok(last)
    }

    fn all(&self) -> Result<Vec<ManifestRecord>, IngestError> {
        let f = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(IngestError::io(&self.path, e)),
        };
        let reader = BufReader::new(f);
        let mut out = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|e| IngestError::io(&self.path, e))?;
            if let Some(r) = Self::parse_line(&line)? {
                out.push(r);
            }
        }
        Ok(out)
    }
}

/// Decide whether `current` is skippable given a previously-recorded
/// `prior` manifest entry.
///
/// Returns `Some(prior)` when the source can be skipped, `None` when a
/// re-run is required.
pub fn skip_decision(
    current: &ManifestRecord,
    prior: Option<&ManifestRecord>,
) -> Result<Option<ManifestRecord>, IngestError> {
    let Some(prior) = prior else {
        return Ok(None);
    };
    if prior.status != ManifestStatus::Success {
        return Ok(None);
    }
    if prior.output_schema_version != CURRENT_SCHEMA_VERSION {
        return Ok(None);
    }
    // The prospective record is still InProgress at decision time; check
    // the relevant config-affecting fields directly rather than calling
    // `matches_for_skip` (which is intended for two finalised records).
    if prior.source_path != current.source_path
        || prior.source_size_bytes != current.source_size_bytes
        || prior.source_sha256 != current.source_sha256
        || prior.parser_version != current.parser_version
        || prior.output_schema_version != current.output_schema_version
        || prior.compression != current.compression
        || prior.batch_rows != current.batch_rows
        || prior.row_group_rows != current.row_group_rows
    {
        return Ok(None);
    }
    // The previous run succeeded AND claimed to have written rows;
    // sanity-check the output files still exist on disk.
    for p in &prior.output_paths {
        if !p.exists() {
            return Ok(None);
        }
    }
    Ok(Some(prior.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Channel, CURRENT_SCHEMA_VERSION};
    use chrono::Utc;

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn dummy_record(path: &Path, status: ManifestStatus) -> ManifestRecord {
        ManifestRecord {
            run_id: "test".into(),
            source_path: path.to_path_buf(),
            source_size_bytes: 100,
            source_sha256: "abc".into(),
            source_year: 1,
            category: "beer".into(),
            channel: Channel::Drug,
            filename_week_start: 1,
            filename_week_end: 52,
            expected_rows: 10,
            written_rows: 10,
            rejected_rows: 0,
            parser_version: crate::model::PARSER_VERSION.into(),
            output_schema_version: CURRENT_SCHEMA_VERSION,
            compression: "zstd".into(),
            batch_rows: 1_000_000,
            row_group_rows: 4_000_000,
            output_paths: vec![],
            output_size_bytes: 0,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            duration_ms: Some(1),
            status,
            error_message: None,
        }
    }

    #[test]
    fn round_trip_jsonl() {
        let dir = tempdir();
        let store = JsonlManifest::open(dir.path()).unwrap();
        let r = dummy_record(&dir.path().join("source"), ManifestStatus::Success);
        store.append(&r).unwrap();
        store.append(&r).unwrap();
        let last = store.last_for_path(&r.source_path).unwrap().unwrap();
        assert_eq!(last.source_path, r.source_path);
        let all = store.all().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn skip_decision_matches_only_when_all_equal() {
        let dir = tempdir();
        let store = JsonlManifest::open(dir.path()).unwrap();
        let r = dummy_record(&dir.path().join("src"), ManifestStatus::Success);
        store.append(&r).unwrap();

        // Same record → skip
        let prior = store.last_for_path(&r.source_path).unwrap();
        assert!(skip_decision(&r, prior.as_ref()).unwrap().is_some());

        // Different batch_rows → do not skip
        let mut r2 = r.clone();
        r2.batch_rows += 1;
        let prior = store.last_for_path(&r2.source_path).unwrap();
        assert!(skip_decision(&r2, prior.as_ref()).unwrap().is_none());

        // Failed prior → do not skip
        let mut r3 = r.clone();
        r3.status = ManifestStatus::Failed;
        store.append(&r3).unwrap();
        let prior = store.last_for_path(&r3.source_path).unwrap().unwrap();
        assert_eq!(prior.status, ManifestStatus::Failed);
        let cur = r.clone();
        assert!(skip_decision(&cur, Some(&prior)).unwrap().is_none());
    }
}
