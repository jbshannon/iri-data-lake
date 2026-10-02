//! Clap definitions for `iri-lake`.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::config::{IngestConfig, OverwriteMode, UnknownFeaturePolicy};

#[derive(Debug, Parser)]
#[command(
    name = "iri-lake",
    about = "High-throughput IRI weekly store-sales fixed-width → Parquet ingest",
    version,
    propagate_version = true
)]
pub struct Cli {
    /// Override batch size in rows.
    #[arg(long, global = true, env = "IRI_LAKE_BATCH_ROWS")]
    pub batch_rows: Option<usize>,

    /// Override Parquet row-group size in rows.
    #[arg(long, global = true, env = "IRI_LAKE_ROW_GROUP_ROWS")]
    pub row_group_rows: Option<usize>,

    /// Override compression codec (zstd, zstd-1, zstd-3, zstd-9,
    /// snappy, lz4, uncompressed). Anything else warns and uses zstd.
    #[arg(long, global = true, env = "IRI_LAKE_COMPRESSION")]
    pub compression: Option<String>,

    /// Override worker thread count.
    #[arg(long, global = true, env = "IRI_LAKE_WORKER_THREADS")]
    pub worker_threads: Option<usize>,

    /// Output root for Parquet files. Defaults to `data/lake`.
    #[arg(long, global = true, env = "IRI_LAKE_OUTPUT_ROOT")]
    pub output_root: Option<PathBuf>,

    /// Input root for source files. Defaults to `data/raw`.
    #[arg(long, global = true, env = "IRI_LAKE_INPUT_ROOT")]
    pub input_root: Option<PathBuf>,
    /// Minimum age in hours a stale `*.tmp` file under the output root
    /// must reach before the pre-run cleanup deletes it. `0` removes
    /// every `*.tmp`, which is only safe when no other run shares the
    /// output root.
    #[arg(long, global = true, env = "IRI_LAKE_TMP_MAX_AGE_HOURS")]
    pub tmp_max_age_hours: Option<u64>,

    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Debug, Subcommand)]
pub enum Cmd {
    /// Walk the input tree and report eligible sales files.
    Inventory {
        /// Override the input root for this command.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
    /// Validate the header and record alignment of one sales file.
    Validate {
        /// Path to the sales file.
        path: PathBuf,
        /// Validate every record (slow).
        #[arg(long)]
        full: bool,
        /// Sample size when not `--full`.
        #[arg(long, default_value_t = 100)]
        sample: usize,
    },
    /// Convert exactly one sales file into Parquet.
    Ingest {
        /// Path to the sales file.
        path: PathBuf,
        /// Output root override.
        #[arg(long)]
        output_root: Option<PathBuf>,
        /// Skip if a prior successful record exists.
        #[arg(long)]
        resume: bool,
    },
    /// Discover and ingest every eligible sales file.
    IngestAll {
        /// Input root override.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Output root override.
        #[arg(long)]
        output_root: Option<PathBuf>,
        /// Skip files whose manifest already shows a successful run.
        #[arg(long)]
        resume: bool,
        /// Overwrite any existing Parquet outputs.
        #[arg(long)]
        overwrite: bool,
        /// Discover but don't write any output.
        #[arg(long)]
        dry_run: bool,
        /// Stop after processing N files.
        #[arg(long)]
        max_files: Option<usize>,
        /// Only process this year.
        #[arg(long)]
        year: Option<u8>,
        /// Only process this category.
        #[arg(long)]
        category: Option<String>,
        /// Only process this channel.
        #[arg(long)]
        channel: Option<String>,
        /// Worker threads (defaults to config).
        #[arg(long)]
        workers: Option<usize>,
    },
    /// Run parser / Arrow / Parquet micro-benchmarks on one file.
    Benchmark {
        /// Path to the sales file.
        path: PathBuf,
        /// Number of rows per batch.
        #[arg(long)]
        batch_rows: Option<usize>,
        /// Compression codec.
        #[arg(long)]
        compression: Option<String>,
        /// Worker threads.
        #[arg(long)]
        workers: Option<usize>,
        /// Repeat each measurement this many times.
        #[arg(long, default_value_t = 3)]
        repeats: usize,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
}

impl Cli {
    pub fn build_config(&self) -> IngestConfig {
        let mut cfg = IngestConfig::defaults();
        cfg.batch_rows = self.batch_rows.unwrap_or(cfg.batch_rows);
        cfg.parquet_row_group_rows = self.row_group_rows.unwrap_or(cfg.parquet_row_group_rows);
        cfg.compression = self.compression.clone().unwrap_or(cfg.compression);
        cfg.worker_threads = self.worker_threads.or(cfg.worker_threads);
        cfg.input_root = self.input_root.clone().unwrap_or(cfg.input_root);
        cfg.output_root = self.output_root.clone().unwrap_or(cfg.output_root);
        cfg.tmp_max_age_hours = self.tmp_max_age_hours.unwrap_or(cfg.tmp_max_age_hours);
        cfg
    }

    pub fn overwrite_mode(
        &self,
        cli_resume: bool,
        cli_overwrite: bool,
    ) -> Result<OverwriteMode, String> {
        // `--resume` and `--overwrite` are opposite intents. Clap cannot
        // express this as a conflict (both are meaningful on their own),
        // so reject the combination rather than letting `--overwrite` win
        // silently -- a caller asking to resume is not asking to destroy
        // completed output.
        if cli_resume && cli_overwrite {
            return Err(
                "--resume and --overwrite are mutually exclusive: --resume keeps \
                 existing outputs, --overwrite deletes and rewrites them"
                    .to_string(),
            );
        }
        // Skipping completed sources is the default, so an interrupted
        // 744-file run can simply be re-issued. `--resume` states that
        // intent explicitly; it is an affirmation, not an opt-in.
        Ok(if cli_overwrite {
            OverwriteMode::Overwrite
        } else {
            OverwriteMode::SkipIfPresent
        })
    }

    pub fn unknown_feature_policy(&self) -> UnknownFeaturePolicy {
        UnknownFeaturePolicy::Fail
    }
}
