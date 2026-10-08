//! `iri-lake` library — exposed mostly so the bench targets and integration
//! tests can call into the same code paths the CLI uses.
//!
//! Modules are organised so that the data path itself
//! (discovery → parsing → Arrow → Parquet) is independent of the CLI
//! scaffolding (clap, tracing, anyhow, manifest I/O orchestration).
//
// `#![forbid(unsafe_code)]` would be ideal but memmap2's `Mmap::map` is
// an `unsafe fn` with no safe wrapper in the current crate API. We relax
// to `#![deny(unsafe_code)]` and localise the one allowed `unsafe` block
// in `src/ingest.rs` where the memory map is constructed. Any new `unsafe`
// surface should require a similar explanation.
#![deny(unsafe_code)]
#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod arrow_output;
pub mod cleanup;
pub mod cli;
pub mod config;
pub mod dataset;
pub mod datasets;
pub mod discovery;
pub mod errors;
pub mod feature;
pub mod fixed_width;
pub mod ingest;
pub mod manifest;
pub mod metrics;
pub mod model;
pub mod money;
pub mod parquet_output;
pub mod parser;
pub mod validation;

pub use errors::{IngestError, Result};
