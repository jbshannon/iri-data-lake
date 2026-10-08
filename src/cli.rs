//! Clap definitions for `iri-lake`.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::config::{IngestConfig, OverwriteMode, UnknownFeaturePolicy};
use crate::discovery;
use crate::model::DatasetKind;

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
    /// snappy, lz4, lz4_raw, uncompressed). Anything else warns and
    /// uses zstd.
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
    /// Discover and ingest every eligible **sales** file.
    ///
    /// This is the original pipeline, unchanged: 744 files, 143 GB,
    /// the fixed-width sales parser, the measured parallelism. See
    /// `docs/parallelism.md` and `docs/corpus_readiness.md`.
    ///
    /// It is separate from [`Cmd::IngestAll`] because it is the one
    /// pipeline with a measured performance contract and five
    /// documented gates behind it, and folding eleven small datasets
    /// into it would put those measurements at the mercy of unrelated
    /// work.
    IngestSales {
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
        /// Worker threads for file-level parallelism. Defaults to
        /// `config.worker_threads` (available parallelism, capped at
        /// 16). 1 restores the sequential behaviour.
        #[arg(long)]
        workers: Option<usize>,
        /// Order in which files are handed to workers.
        #[arg(long, value_enum, default_value_t = Order::SmallestFirst)]
        order: Order,
        /// Ingest only shard `IDX` of `N`, e.g. `--shard 0/4`.
        ///
        /// The file list is split into `N` contiguous, largest-first
        /// chunks of equal total bytes, so each shard carries ~1/N of
        /// the corpus work. Meant for running several processes over
        /// one output root — see `docs/parallelism.md`.
        #[arg(long, value_parser = parse_shard)]
        shard: Option<(usize, usize)>,
    },
    /// Ingest every dataset class: sales plus all ten non-sales ones.
    ///
    /// Classes run **sequentially**, each with its own discovery,
    /// parallelism and batch sizing, and each is resumable at dataset
    /// grain. Sales runs first, through the dedicated pipeline.
    ///
    /// `--only` / `--skip` restrict which classes run. `--dry-run`
    /// discovers and reports without writing anything.
    IngestAll {
        /// Input root override.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Output root override.
        #[arg(long)]
        output_root: Option<PathBuf>,
        /// Only run these dataset classes. Repeatable. Omit for all.
        #[arg(long = "only", value_name = "CLASS")]
        only: Vec<String>,
        /// Skip these dataset classes. Repeatable.
        #[arg(long = "skip", value_name = "CLASS")]
        skip: Vec<String>,
        /// Overwrite any existing Parquet outputs.
        #[arg(long)]
        overwrite: bool,
        /// Discover and report, but write nothing.
        #[arg(long)]
        dry_run: bool,
        /// Include the sales class (default: yes, use --skip sales).
        #[arg(long)]
        no_sales: bool,
        /// Worker threads within each class.
        #[arg(long)]
        workers: Option<usize>,
        /// Show each class's skip reasons, not just counts.
        #[arg(long)]
        explain_skips: bool,
    },
    /// Walk the input tree and report the non-sales datasets.
    InventoryOther {
        /// Override the input root.
        #[arg(long)]
        input: Option<PathBuf>,
        /// Output format.
        #[arg(long, value_enum, default_value_t = OtherInventoryFormat::Table)]
        format: OtherInventoryFormat,
        /// Show each class's skip reasons, not just counts.
        #[arg(long)]
        explain_skips: bool,
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

/// Parse `--shard IDX/N`. Returns `(idx, n)` with `idx < n`.
fn parse_shard(s: &str) -> Result<(usize, usize), String> {
    let (a, b) = s
        .split_once('/')
        .ok_or_else(|| format!("expected IDX/N, got {s:?}"))?;
    let idx: usize = a
        .trim()
        .parse()
        .map_err(|_| format!("shard index is not a number: {a:?}"))?;
    let n: usize = b
        .trim()
        .parse()
        .map_err(|_| format!("shard count is not a number: {b:?}"))?;
    if n == 0 {
        return Err("shard count must be >= 1".to_string());
    }
    if idx >= n {
        return Err(format!("shard index {idx} must be < count {n}"));
    }
    Ok((idx, n))
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    /// One discovered source path per line, sorted, on stdout.
    ///
    /// Exists so that discovery's *decisions* can be consumed by other
    /// tools rather than re-derived from scratch: the Gate 2
    /// validation sweep pipes this straight into `xargs`, and it is the
    /// only supported way to enumerate what `ingest-all` would touch.
    /// Anything that greps filenames itself will drift from
    /// `discovery.rs` — the skips are 2 733 files across six reasons,
    /// and PANEL/stub/backup files are not reliably filterable from the
    /// shell.
    Paths,
}

/// Scheduling order for `ingest-all`'s work list.
///
/// The list is always built largest-file-first; these strategies decide
/// what to do with that ordering. **The default is `SmallestFirst`**,
/// which is counter-intuitive and was measured, not guessed: rayon's
/// work-stealing deque hands work out from the *back* of the slice, so
/// a largest-first list leaves the 1.4 GB file at index 0 to be picked
/// up last, by whichever worker happens to be free at the end. On the
/// Year-1 scope that cost 23% of wall time at 8 workers (24.9 s vs
/// 20.3 s). See `docs/parallelism.md` for the full table.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum Order {
    /// Biggest files first. Worst case for the rayon pool: the largest
    /// file is reached last.
    LargestFirst,
    /// Smallest files first, so the long poles are handed out early by
    /// the splitting workers and the tail is a pile of short jobs.
    /// This is the default.
    SmallestFirst,
    /// Round-robin over the largest-first list, so the N largest files
    /// are dealt to N different workers up front. Measures the same as
    /// `LargestFirst`; kept because it is the intuitive formulation of
    /// what `SmallestFirst` achieves.
    Striped,
}

impl std::fmt::Display for Order {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Order::LargestFirst => "largest-first",
            Order::SmallestFirst => "smallest-first",
            Order::Striped => "striped",
        };
        f.write_str(s)
    }
}

impl Order {
    /// Re-order a largest-first-sorted file list.
    ///
    /// `workers` only matters for [`Order::Striped`].
    pub fn apply(&self, files: &mut Vec<discovery::DiscoveredFile>, workers: usize) {
        match self {
            Order::LargestFirst => {}
            Order::SmallestFirst => files.reverse(),
            Order::Striped => stripe(files, workers.max(1)),
        }
    }
}

/// Deal the largest-first list out in strides of `n`: the first `n`
/// outputs are the n largest files, so each worker starts on a big one
/// instead of one worker inheriting all of them.
fn stripe(files: &mut Vec<discovery::DiscoveredFile>, n: usize) {
    let len = files.len();
    let mut out: Vec<Option<discovery::DiscoveredFile>> = (0..len).map(|_| None).collect();
    for (i, f) in files.drain(..).enumerate() {
        let target = (i / n) * n + (i % n);
        // For a list shorter than n, or one whose length is not a
        // multiple of n, `i / n` steps in strides and `target` can run
        // past the end; fall back to appending in that case.
        if target < len {
            out[target] = Some(f);
        } else {
            out.push(None);
            let last = out.len() - 1;
            out[last] = Some(f);
        }
    }
    // Compact away the `None` holes produced by ragged strides.
    *files = out.into_iter().flatten().collect();
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OtherInventoryFormat {
    /// One line per class: files, bytes, and a skip count.
    Table,
    Json,
    /// One discovered source path per line, sorted.
    ///
    /// The same contract as [`OutputFormat::Paths`]: the work list
    /// comes from `dataset::discover`, so a caller cannot drift from
    /// the filters the ingest path actually applies.
    Paths,
}

/// Resolve `--only` / `--skip` into an ordered class list.
///
/// Errors name the offending token and list what is valid, because
/// `--only panle` is a typo and an error that just says "unknown
/// dataset" sends the reader hunting through the source.
pub fn resolve_classes(
    only: &[String],
    skip: &[String],
    include_sales: bool,
) -> std::result::Result<Vec<DatasetKind>, String> {
    let all: Vec<DatasetKind> = if include_sales {
        let mut v = vec![DatasetKind::Sales];
        v.extend_from_slice(DatasetKind::others());
        v
    } else {
        DatasetKind::others().to_vec()
    };
    let known: Vec<String> = all.iter().map(|k| k.to_string()).collect();

    if !only.is_empty() {
        let mut picked = Vec::new();
        for token in only {
            let k = DatasetKind::parse(token)
                .ok_or_else(|| format!("unknown dataset {token:?}; known: {}", known.join(", ")))?;
            if !all.contains(&k) {
                return Err(format!(
                    "dataset {k} is not available here; known: {}",
                    known.join(", ")
                ));
            }
            if !picked.contains(&k) {
                picked.push(k);
            }
        }
        // Preserve the canonical order rather than the CLI's order, so
        // `--only stub,panel` and `--only panel,stub` do the same work
        // in the same sequence.
        return Ok(all.into_iter().filter(|k| picked.contains(k)).collect());
    }

    let mut out = all;
    for token in skip {
        let k = DatasetKind::parse(token)
            .ok_or_else(|| format!("unknown dataset {token:?}; known: {}", known.join(", ")))?;
        out.retain(|x| *x != k);
    }
    Ok(out)
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
