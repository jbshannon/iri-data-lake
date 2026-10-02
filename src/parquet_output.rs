//! Thin wrapper around `parquet::arrow::ArrowWriter`.
//!
//! This module is intentionally small — the only behaviours that live
//! here are:
//!
//! 1. Picking a compression codec from our `Config` (Zstd default,
//!    optional Snappy, Lz4 — anything the underlying crate supports).
//! 2. Encoding the row-group size into the writer properties.
//! 3. Atomic write: every Parquet file goes to a `.tmp` sibling first,
//!    then `rename` flips it into place. A consumer that observes a
//!    `*.parquet` filename can trust the bytes are complete.
//!
//! Parquet-side metadata writing for the manifest tables lives in
//! `manifest.rs` and intentionally stays JSONL for the first version.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::config::IngestConfig;
use crate::errors::IngestError;

/// Resolve the user's chosen compression codec into a Parquet value.
pub fn compression_from_config(cfg: &IngestConfig) -> Compression {
    match cfg.compression.to_ascii_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "zstd-1" | "zstd1" => Compression::ZSTD(ZstdLevel::try_new(1).unwrap_or_default()),
        "zstd-3" | "zstd3" => Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()),
        "zstd-9" | "zstd9" => Compression::ZSTD(ZstdLevel::try_new(9).unwrap_or_default()),
        "snappy" => Compression::SNAPPY,
        "lz4" | "lz4_raw" => Compression::LZ4,
        "gzip" => Compression::GZIP(parquet::basic::GzipLevel::default()),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => {
            // Unknown codec: default to zstd so a typo in the config
            // doesn't silently produce massive files.
            tracing::warn!(requested = %other, "unknown compression, falling back to zstd");
            Compression::ZSTD(ZstdLevel::default())
        }
    }
}

/// Write a single Parquet file at `final_path`, atomically.
///
/// `batches` is a sequence of already-built `RecordBatch` values that
/// together form the file's row groups. The writer is closed and the
/// temp file is renamed to `final_path` only after the last batch has
/// been written successfully.
///
/// Returns the on-disk byte size of the final file.
pub fn write_parquet_atomic(
    final_path: &Path,
    schema: SchemaRef,
    batches: &[RecordBatch],
    cfg: &IngestConfig,
) -> Result<u64, IngestError> {
    // Build the temp path next to the final path. We never delete an
    // existing final_path until the rename succeeds, so an interrupted
    // run leaves at most a `.tmp` sibling that the next run will
    // overwrite.
    let tmp_path: PathBuf = {
        let mut p = final_path.as_os_str().to_owned();
        p.push(".tmp");
        PathBuf::from(p)
    };

    if let Some(parent) = final_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| IngestError::io(parent, e))?;
    }

    // Scope the writer so its File handle is dropped before the rename.
    {
        let file = File::create(&tmp_path).map_err(|e| IngestError::io(&tmp_path, e))?;
        let props = WriterProperties::builder()
            .set_compression(compression_from_config(cfg))
            .set_max_row_group_size(cfg.parquet_row_group_rows)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| IngestError::parquet(&tmp_path, e))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| IngestError::parquet(&tmp_path, e))?;
        }
        writer
            .close()
            .map_err(|e| IngestError::parquet(&tmp_path, e))?;
    }

    let size = std::fs::metadata(&tmp_path)
        .map_err(|e| IngestError::io(&tmp_path, e))?
        .len();

    std::fs::rename(&tmp_path, final_path).map_err(|e| IngestError::AtomicRename {
        tmp: tmp_path,
        dst: final_path.to_path_buf(),
        source: e,
    })?;
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow_output::{schema, SalesBuilders};
    use arrow_array::RecordBatch;

    #[test]
    fn compression_round_trips_for_known_codecs() {
        let mut cfg = IngestConfig::for_test();
        for c in [
            "zstd",
            "zstd-1",
            "zstd-3",
            "zstd-9",
            "snappy",
            "lz4",
            "uncompressed",
        ] {
            cfg.compression = c.into();
            let _ = compression_from_config(&cfg);
        }
    }

    #[test]
    fn atomic_write_produces_readable_file() {
        let mut b = SalesBuilders::with_capacity(2);
        b.iri_key.append_value(1);
        b.week.append_value(2);
        b.sy.append_value(3);
        b.ge.append_value(4);
        b.vend.append_value(5);
        b.item.append_value(6);
        b.units.append_value(7);
        b.dollars_cents.append_value(800);
        b.feature_code.append_value(0);
        b.display.append_value(0);
        b.price_reduction.append_value(true);
        b.source_year.append_value(1);
        b.category.append_value("c");
        b.channel.append_value("drug");
        b.iri_key.append_value(10);
        b.week.append_value(20);
        b.sy.append_value(30);
        b.ge.append_value(40);
        b.vend.append_value(50);
        b.item.append_value(60);
        b.units.append_value(70);
        b.dollars_cents.append_value(900);
        b.feature_code.append_value(1);
        b.display.append_value(1);
        b.price_reduction.append_value(false);
        b.source_year.append_value(2);
        b.category.append_value("d");
        b.channel.append_value("groc");
        let batch: RecordBatch = b.finish(schema()).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let final_path = dir.path().join("out.parquet");
        let cfg = IngestConfig::for_test();
        let size = write_parquet_atomic(&final_path, schema(), &[batch], &cfg).unwrap();
        assert!(size > 0);
        assert!(final_path.exists());
        // The .tmp sibling should be gone after rename.
        let tmp_path = {
            let mut p = final_path.as_os_str().to_owned();
            p.push(".tmp");
            PathBuf::from(p)
        };
        assert!(!tmp_path.exists(), "tmp file should have been renamed");
    }
}
