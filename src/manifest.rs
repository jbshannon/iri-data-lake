//! Manifest abstraction. The first implementation writes a JSONL file
//! under `<output_root>/metadata/manifest.jsonl`. A later commit will
//! swap in a Parquet-backed store behind the same trait — schema
//! versions and field names are kept identical so the on-disk data
//! can be re-ingested cleanly.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::errors::IngestError;
use crate::model::{ManifestRecord, ManifestStatus};

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

/// Concurrency-safe manifest for multi-worker ingest (G1).
///
/// Two things the single-threaded [`JsonlManifest`] cannot do safely
/// when several workers ingest files at once:
///
/// 1. **Append atomicity.** `JsonlManifest::append` opens and closes the
///    file per record and issues a single `write_all` per line. That is
///    *probably* atomic on APFS because of `O_APPEND`, but "probably" is
///    not a property an idempotence store should have. Here one handle
///    is opened once and every append is serialised behind a mutex.
/// 2. **Lookup cost.** `last_for_path` re-reads and re-parses the whole
///    file for every lookup, which is O(records²) per run. Here the
///    manifest is read exactly once at open time into an in-memory index
///    keyed by `source_path`; later lookups are a hash lookup under a
///    short-lived lock.
///
/// The index is loaded *before* any worker starts, and every subsequent
/// mutation goes through `append`, so the in-memory view is exactly what
/// a re-read of the file would show — including records written by
/// sibling workers earlier in this same run.
pub struct SharedManifest {
    state: Mutex<ManifestState>,
}

struct ManifestState {
    /// Append-only handle opened once, `O_APPEND`.
    file: File,
    /// Where `file` lives, for error messages.
    path: PathBuf,
    /// `source_path` → most recent record. Updated on every append.
    index: HashMap<PathBuf, ManifestRecord>,
}

impl std::fmt::Debug for SharedManifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `File` is not `Debug`-printable in a useful way and the
        // index can be large; report shape only.
        f.debug_struct("SharedManifest").finish_non_exhaustive()
    }
}

impl SharedManifest {
    /// Open (and read, once) the manifest under `output_root`.
    pub fn open(output_root: &Path) -> Result<Self, IngestError> {
        let path = manifest_path(output_root);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| IngestError::io(parent, e))?;
        }
        let bootstrap = JsonlManifest::open(output_root)?;
        let mut index: HashMap<PathBuf, ManifestRecord> = HashMap::new();
        for record in bootstrap.all()? {
            index.insert(record.source_path.clone(), record);
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| IngestError::io(&path, e))?;
        Ok(Self {
            state: Mutex::new(ManifestState { file, path, index }),
        })
    }

    /// The most recent record for `source_path`, or `None`.
    ///
    /// Clones the record because `skip_decision` needs an owned value
    /// and the lock must not be held across a multi-millisecond
    /// ingest.
    pub fn last_for_path(&self, source_path: &Path) -> Result<Option<ManifestRecord>, IngestError> {
        let state = self.state.lock().map_err(poisoned)?;
        Ok(state.index.get(source_path).cloned())
    }

    /// The path of the JSONL file this handle appends to.
    pub fn path(&self) -> PathBuf {
        self.state
            .lock()
            .map(|s| s.path.clone())
            .unwrap_or_else(|_| PathBuf::from("metadata/manifest.jsonl"))
    }

    /// Append `record` and make it visible to later `last_for_path` calls.
    pub fn append(&self, record: &ManifestRecord) -> Result<(), IngestError> {
        let line = serde_json::to_string(record)
            .map_err(|e| IngestError::manifest(format!("serialise: {}", e)))?;
        let mut state = self.state.lock().map_err(poisoned)?;
        let path = state.path.clone();
        // `write_all` on an O_APPEND handle emits one `write(2)` for the
        // whole line in practice; the mutex makes that guarantee hold
        // regardless of how the stream chunks it.
        writeln!(state.file, "{}", line).map_err(|e| IngestError::io(&path, e))?;
        state.file.flush().map_err(|e| IngestError::io(&path, e))?;
        state
            .index
            .insert(record.source_path.clone(), record.clone());
        Ok(())
    }
}

/// A poisoned manifest lock means a previous append panicked mid-write.
/// The file may have a torn last line, so treat it as a hard error
/// rather than silently continuing on a corrupt idempotence store.
fn poisoned<T>(_: std::sync::PoisonError<T>) -> IngestError {
    IngestError::manifest("manifest lock poisoned by a panicking writer")
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
    // The prospective record is still InProgress at decision time; check
    // the relevant config-affecting fields directly rather than calling
    // `matches_for_skip` (which is intended for two finalised records).
    //
    // `output_schema_version` is compared **record against record**, not
    // against the crate-wide `CURRENT_SCHEMA_VERSION`. The sales class
    // happens to be the crate's version, so hard-coding it was invisible
    // there — but every other class declares its own, and a check
    // against `CURRENT_SCHEMA_VERSION` rejects all of them, so no
    // non-sales source could ever be skipped and a re-run silently
    // re-ingested all of them into new files.
    if prior.source_path != current.source_path
        || prior.dataset != current.dataset
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
            dataset: crate::model::DatasetKind::Sales,
            source_path: path.to_path_buf(),
            source_size_bytes: 100,
            source_sha256: "abc".into(),
            source_year: Some(1),
            category: Some("beer".into()),
            channel: Some(Channel::Drug),
            filename_week_start: Some(1),
            filename_week_end: Some(52),
            deduplicated_from: None,
            source_schema_fingerprint: None,
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
    fn shared_manifest_round_trips_under_concurrency() {
        let dir = tempdir();
        let store = SharedManifest::open(dir.path()).unwrap();
        let records: Vec<ManifestRecord> = (0..64)
            .map(|i| {
                let mut r = dummy_record(
                    &dir.path().join(format!("src-{i}")),
                    ManifestStatus::Success,
                );
                r.run_id = format!("run-{i}");
                r
            })
            .collect();

        std::thread::scope(|s| {
            for chunk in records.chunks(8) {
                let store = &store;
                s.spawn(move || {
                    for r in chunk {
                        store.append(r).unwrap();
                    }
                });
            }
        });

        // Every record must be readable back, and the on-disk file must
        // contain exactly 64 parseable lines (no torn interleaving).
        for r in &records {
            assert_eq!(
                store.last_for_path(&r.source_path).unwrap().unwrap().run_id,
                r.run_id
            );
        }
        let on_disk = std::fs::read_to_string(manifest_path(dir.path())).unwrap();
        let lines: Vec<&str> = on_disk.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), records.len());
        for line in lines {
            JsonlManifest::parse_line(line)
                .unwrap()
                .expect("no torn line");
        }
    }

    #[test]
    fn shared_manifest_sees_earlier_appends_as_prior_records() {
        let dir = tempdir();
        let store = SharedManifest::open(dir.path()).unwrap();
        let r = dummy_record(&dir.path().join("src"), ManifestStatus::Success);
        store.append(&r).unwrap();
        let prior = store.last_for_path(&r.source_path).unwrap();
        assert!(skip_decision(&r, prior.as_ref()).unwrap().is_some());
        // A second, independently-opened store must agree.
        let reopened = SharedManifest::open(dir.path()).unwrap();
        assert_eq!(
            reopened
                .last_for_path(&r.source_path)
                .unwrap()
                .unwrap()
                .run_id,
            r.run_id
        );
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
