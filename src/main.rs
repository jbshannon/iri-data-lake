//! CLI entry point. All non-trivial logic lives in the library crate so
//! the bench targets and integration tests can exercise the same paths.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use iri_lake::cleanup::{cleanup_stale_tmp, CleanupReport};
use iri_lake::cli::{Cli, Cmd, OutputFormat};
use iri_lake::discovery;
use iri_lake::errors::IngestError;
use iri_lake::ingest::{ingest_all, ingest_file, IngestAllSummary, IngestFilter, IngestOutcome};
use iri_lake::model::Channel;
use iri_lake::validation::validate_file;

fn main() -> ExitCode {
    init_tracing();

    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = ?err, "iri-lake failed");
            eprintln!("error: {:#}", err);
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("iri_lake=info,warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn run(cli: Cli) -> Result<()> {
    let mut config = cli.build_config();
    // Resolve the skip/overwrite policy once, up front, so that a
    // contradictory pair of flags fails before any work starts.
    let resume = matches!(&cli.cmd, Cmd::Ingest { resume, .. } if *resume)
        || matches!(&cli.cmd, Cmd::IngestAll { resume, .. } if *resume);
    let overwrite = matches!(&cli.cmd, Cmd::IngestAll { overwrite, .. } if *overwrite);
    config.overwrite = cli
        .overwrite_mode(resume, overwrite)
        .map_err(IngestError::Config)
        .map_err(anyhow::Error::from)?;
    match cli.cmd {
        Cmd::Inventory { input, format } => {
            let root = input.unwrap_or_else(|| config.input_root.clone());
            let inv = discovery::discover(&root).context("inventory walk")?;
            match format {
                OutputFormat::Table => print_inventory_table(&inv),
                OutputFormat::Json => print_inventory_json(&inv),
                OutputFormat::Paths => print_inventory_paths(&inv),
            }
            Ok(())
        }
        Cmd::Validate { path, full, sample } => {
            let root = config.input_root.clone();
            let sample = if full { usize::MAX } else { sample };
            let r = validate_file(&path, &root, sample).context("validate")?;
            print_validation(&r);
            if r.is_ok() {
                Ok(())
            } else {
                anyhow::bail!(
                    "validation failed: header_matches={}, record_aligned={}, sample {}/{}",
                    r.header_matches,
                    r.record_aligned,
                    r.sample_passed,
                    r.sample_size
                )
            }
        }
        Cmd::Ingest {
            path,
            output_root,
            resume: _,
        } => {
            let out = output_root.unwrap_or_else(|| config.output_root.clone());
            let _ = sweep_tmp(&out, &config);
            let started = Instant::now();
            let outcome = ingest_file(
                &path,
                &config.input_root,
                &out,
                &config,
                &IngestFilter::default(),
            )
            .context("ingest")?;
            match outcome {
                IngestOutcome::Skipped(prior) => {
                    println!(
                        "SKIP: prior run {} already produced this output.",
                        prior.run_id
                    );
                }
                IngestOutcome::Completed(_, stats) => {
                    let wall = started.elapsed();
                    println!(
                        "OK   rows={} bytes_in={} bytes_out={} elapsed={:.2}s raw_MiB/s={:.1} rows/s={:.0} compression_ratio={:.2}x",
                        stats.written_rows,
                        stats.source_size_bytes,
                        stats.output_bytes,
                        wall.as_secs_f64(),
                        stats.raw_mib_per_second(),
                        stats.rows_per_second(),
                        stats.compression_ratio(),
                    );
                    for p in &stats.output_paths {
                        println!("  -> {}", p.display());
                    }
                }
            }
            Ok(())
        }
        Cmd::IngestAll {
            input,
            output_root,
            resume: _,
            overwrite,
            dry_run,
            max_files,
            year,
            category,
            channel,
            workers,
            order,
            shard,
        } => {
            let in_root = input.unwrap_or_else(|| config.input_root.clone());
            let out_root = output_root.unwrap_or_else(|| config.output_root.clone());
            let channel_filter: Option<Channel> = channel.as_deref().and_then(Channel::parse);

            let inv = discovery::discover(&in_root).context("ingest-all discovery")?;
            // Largest-first ordering for tail-latency smoothing.
            let mut files: Vec<_> = inv
                .files
                .iter()
                .filter(|f| match year {
                    Some(y) => f.identity.year == y,
                    None => true,
                })
                .filter(|f| match &category {
                    Some(c) => &f.identity.category == c,
                    None => true,
                })
                .filter(|f| match channel_filter {
                    Some(c) => f.identity.channel == c,
                    None => true,
                })
                .cloned()
                .collect();
            files.sort_by_key(|a| std::cmp::Reverse(a.size_bytes));
            if let Some(n) = max_files {
                files.truncate(n);
            }
            let workers = workers.or(config.worker_threads).unwrap_or(1).max(1);
            order.apply(&mut files, workers);
            if let Some((idx, n)) = shard {
                files = take_shard(files, idx, n);
            }

            // Report the *selected* bytes/rows, not the whole
            // inventory's: with `--shard`, `--year` or `--max-files`
            // the two differ by an order of magnitude and the old
            // line made a 4-file shard look like the full corpus.
            let sel_bytes: u64 = files.iter().map(|f| f.size_bytes).sum();
            let sel_rows: u64 = files
                .iter()
                .map(|f| iri_lake::fixed_width::expected_rows(f.size_bytes).unwrap_or(0))
                .sum();
            println!(
                "ingest-all: {} file(s) selected ({:.2} GiB raw, ~{:.3}B rows) of {} discovered ({:.2} GiB); overwrite={} dry_run={} workers={} order={} shard={:?}",
                files.len(),
                sel_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                sel_rows as f64 / 1e9,
                inv.total_files(),
                inv.total_bytes() as f64 / (1024.0 * 1024.0 * 1024.0),
                overwrite,
                dry_run,
                workers,
                order,
                shard,
            );
            if dry_run {
                for f in files.iter().take(20) {
                    println!(
                        "  DRY  year={} category={} channel={} bytes={}",
                        f.identity.year, f.identity.category, f.identity.channel, f.size_bytes
                    );
                }
                if files.len() > 20 {
                    println!("  ...and {} more", files.len() - 20);
                }
                println!("(dry-run; no output written.)");
                return Ok(());
            }

            let _ = sweep_tmp(&out_root, &config);

            tracing::info!(
                workers,
                order = %order,
                shard = ?shard,
                files = files.len(),
                "ingest-all starting"
            );

            let summary =
                ingest_all(&files, &in_root, &out_root, &config, workers).context("ingest-all")?;

            let IngestAllSummary {
                completed,
                skipped,
                failed,
                bytes_in,
                bytes_out,
                rows,
                rejected_rows,
                wall,
                slowest_file,
                failures: _,
            } = summary;
            let mib = 1024.0 * 1024.0;
            println!(
                "ingest-all done: completed={} skipped={} failed={} workers={} wall={:.2}s",
                completed,
                skipped,
                failed,
                workers,
                wall.as_secs_f64()
            );
            if completed > 0 {
                println!(
                    "  throughput: raw={:.1} MiB/s rows={:.2} M/s out={:.2} GiB bytes={} rows={} rejected_rows={} slowest_file={:.1}s",
                    bytes_in as f64 / mib / wall.as_secs_f64(),
                    rows as f64 / 1e6 / wall.as_secs_f64(),
                    bytes_out as f64 / (1024.0 * 1024.0 * 1024.0),
                    bytes_in,
                    rows,
                    rejected_rows,
                    slowest_file.as_secs_f64(),
                );
            }
            if rejected_rows > 0 {
                println!(
                    "  note: {} row(s) rejected as incomplete records (trailing bytes of a truncated source)",
                    rejected_rows
                );
            }
            if failed > 0 {
                tracing::warn!(failed, "ingest-all finished with failures");
            }
            Ok(())
        }
        Cmd::Benchmark {
            path,
            batch_rows,
            compression,
            workers: _,
            repeats,
        } => {
            let mut cfg = config;
            if let Some(b) = batch_rows {
                cfg.batch_rows = b;
            }
            if let Some(c) = compression {
                cfg.compression = c;
            }
            run_benchmark(&path, &cfg, repeats)
        }
    }
}

/// Delete stale `*.tmp` leftovers under `output_root` before a run
/// writes anything (G5). Interrupted runs leave a `.tmp` sibling with
/// no manifest line, so nothing else would ever reap them.
///
/// Best-effort by design: a failure here is reported but does not stop
/// the ingest, because the files are inert leftovers. The age gate in
/// `cleanup` means a concurrent run's in-flight `.tmp` is never a
/// candidate.
fn sweep_tmp(output_root: &Path, config: &iri_lake::config::IngestConfig) -> CleanupReport {
    let report = cleanup_stale_tmp(
        output_root,
        std::time::Duration::from_secs(config.tmp_max_age_hours * 3600),
    );
    if !report.is_empty() {
        tracing::info!(
            output_root = %output_root.display(),
            "{}",
            report.summary()
        );
        for (path, err) in &report.failures {
            tracing::warn!(path = %path.display(), error = %err, "tmp cleanup failed");
        }
        for path in &report.removed {
            tracing::debug!(path = %path.display(), "removed stale tmp");
        }
    }
    report
}

/// Take shard `idx` of `n` from a largest-first-sorted file list.
/// Boundaries are cut on **cumulative bytes**, not on file count. With
/// a largest-first list the first few files dominate the total, so
/// equal-count splits are wildly unbalanced (measured: a 4-way
/// equal-count split of Year 1 left shard 0 with 55% of the bytes and
/// a makespan 1.5x the whole-run time of an 8-thread pool). Cutting
/// where the running byte total crosses `idx/n` of the grand total
/// keeps every shard within one file of `1/n` of the work.
///
/// The list is contiguous per shard, so within a shard the same
/// largest-first property holds for the internal (rayon) split.
fn take_shard(
    files: Vec<discovery::DiscoveredFile>,
    idx: usize,
    n: usize,
) -> Vec<discovery::DiscoveredFile> {
    if n <= 1 || files.is_empty() {
        return files;
    }
    let total: u64 = files.iter().map(|f| f.size_bytes).sum();
    if total == 0 {
        return files;
    }
    let mut out = Vec::new();
    let mut cumulative = 0u64;
    for f in files.into_iter() {
        let before = cumulative;
        cumulative += f.size_bytes;
        // A file belongs to shard `k` where k/total falls in
        // [before, cumulative): k = floor(before * n / total). A file
        // bigger than total/n spans several shard boundaries and lands
        // wholly in the first one it touches — a small imbalance we
        // accept rather than split a source file across processes.
        let lo = before * n as u64 / total;
        if lo == idx as u64 {
            out.push(f);
        }
    }
    out
}

fn print_inventory_table(inv: &discovery::Inventory) {
    println!(
        "{:>5} {:>8} {:>14} {:>6} {:>14}",
        "year", "channel", "files", "GiB", "rows"
    );
    for ((year, channel), b) in inv.by_year_channel() {
        println!(
            "{:>5} {:>8} {:>14} {:>6.2} {:>14}",
            year,
            channel,
            b.file_count,
            b.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            b.total_expected_rows,
        );
    }
    println!(
        "\nTOTAL files={} bytes={:.2} GiB expected_rows={}",
        inv.total_files(),
        inv.total_bytes() as f64 / (1024.0 * 1024.0 * 1024.0),
        inv.total_expected_rows()
    );
    println!(
        "skipped={} (top reasons: {})",
        inv.skipped.len(),
        summarise_skip_reasons(&inv.skipped),
    );
}

fn summarise_skip_reasons(skipped: &[discovery::SkippedFile]) -> String {
    use std::collections::BTreeMap;
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    for s in skipped {
        *counts.entry(s.reason.label()).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_inventory_json(inv: &discovery::Inventory) {
    let mut by_yc: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for ((year, channel), b) in inv.by_year_channel() {
        by_yc.insert(
            format!("{}-{}", year, channel),
            serde_json::json!({
                "files": b.file_count,
                "bytes": b.total_bytes,
                "rows": b.total_expected_rows,
                "categories": b.categories,
            }),
        );
    }
    let out = serde_json::json!({
        "total_files": inv.total_files(),
        "total_bytes": inv.total_bytes(),
        "total_expected_rows": inv.total_expected_rows(),
        "by_year_channel": by_yc,
        "skipped_count": inv.skipped.len(),
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}

/// One discovered source path per line, sorted.
///
/// The point is that this list comes from `discovery.rs`, not from a
/// shell re-implementation of its filter. Gate 2 pipes it into
/// `xargs -P` to validate every file; see `scripts/gate2_validate.sh`.
fn print_inventory_paths(inv: &discovery::Inventory) {
    let mut paths: Vec<&std::path::Path> = inv
        .files
        .iter()
        .map(|f| f.identity.path.as_path())
        .collect();
    paths.sort_unstable();
    for p in paths {
        println!("{}", p.display());
    }
}

fn print_validation(r: &iri_lake::validation::ValidationReport) {
    println!("file:        {}", r.identity.path.display());
    println!(
        "identity:    year={} category={} channel={} weeks={}-{}",
        r.identity.year,
        r.identity.category,
        r.identity.channel,
        r.identity.filename_week_start,
        r.identity.filename_week_end
    );
    println!("size_bytes:  {}", r.size_bytes);
    println!("expected_rows: {}", r.expected_rows);
    println!("header_matches: {}", r.header_matches);
    println!("record_aligned: {}", r.record_aligned);
    println!("sample_passed:  {}/{}", r.sample_passed, r.sample_size);
}

/// Benchmark a single ingest of `path`, `repeats` times.
///
/// Each repeat writes to a *fresh* temporary output root. That is
/// deliberate: `ingest_file` consults the manifest and returns
/// `IngestOutcome::Skipped` when a prior success record matches the
/// source hash and the current config. That idempotence guard is exactly
/// what we want for `ingest`/`ingest-all`, but it makes repeats 2..N
/// silently measure nothing. Isolating the output root per repeat keeps
/// the measurement honest without weakening the guard itself.
///
/// The temporary root is removed on drop, so benchmarking never
/// pollutes the configured `--output-root`.
fn run_benchmark(path: &Path, cfg: &iri_lake::config::IngestConfig, repeats: usize) -> Result<()> {
    anyhow::ensure!(repeats >= 1, "--repeats must be at least 1");

    let scratch = tempfile::tempdir().context("create scratch output root for benchmark")?;

    let mut measurements: Vec<serde_json::Value> = Vec::new();
    for i in 0..repeats {
        let repeat_root = scratch.path().join(format!("run-{i}"));
        let started = Instant::now();
        let outcome = ingest_file(
            path,
            &cfg.input_root,
            &repeat_root,
            cfg,
            &IngestFilter::default(),
        )?;
        let elapsed = started.elapsed();
        let stats = match outcome {
            IngestOutcome::Completed(_, stats) => stats,
            IngestOutcome::Skipped(_) => {
                // Each repeat gets a private output root, so the manifest
                // should be empty and this branch unreachable. Fail loudly
                // rather than emit a `"skipped": true` entry that would be
                // silently averaged as a measurement.
                anyhow::bail!(
                    "repeat {i} was skipped by the manifest; \
                     this is a benchmark-harness bug, not a measurement"
                );
            }
        };
        measurements.push(serde_json::json!({
            "repeat": i,
            "rows": stats.written_rows,
            "bytes_in": stats.source_size_bytes,
            "bytes_out": stats.output_bytes,
            "elapsed_s": elapsed.as_secs_f64(),
            "raw_MiB_s": stats.raw_mib_per_second(),
            "rows_s": stats.rows_per_second(),
            "compression_ratio": stats.compression_ratio(),
            "batch_rows": cfg.batch_rows,
            "row_group_rows": cfg.parquet_row_group_rows,
            "compression": cfg.compression,
        }));
    }

    // The first repeat pays for cold page cache; the median of the
    // repeats is the headline number, the best is the least-noisy upper
    // bound. Both are emitted so callers can plot the spread.
    let perfs: Vec<f64> = measurements
        .iter()
        .map(|m| m["raw_MiB_s"].as_f64().unwrap_or_default())
        .collect();
    let mut sorted = perfs.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if sorted.is_empty() {
        f64::NAN
    } else {
        sorted[sorted.len() / 2]
    };

    let report = serde_json::json!({
        "file": path.display().to_string(),
        "bytes_in": measurements
            .first()
            .and_then(|m| m["bytes_in"].as_u64())
            .unwrap_or(0),
        "rows": measurements
            .first()
            .and_then(|m| m["rows"].as_u64())
            .unwrap_or(0),
        "repeats": measurements,
        "summary": {
            "raw_MiB_s_median": median,
            "raw_MiB_s_best": sorted.last().copied().unwrap_or(f64::NAN),
            "raw_MiB_s_worst": sorted.first().copied().unwrap_or(f64::NAN),
        },
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
