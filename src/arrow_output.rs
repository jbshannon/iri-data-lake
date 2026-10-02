//! Arrow schema and column-builder wrapper for sales facts.
//!
//! The schema contains only the **raw data columns** — the eleven fields
//! parsed out of the fixed-width record:
//!
//! ```text
//! iri_key, week, sy, ge, vend, item, units, dollars_cents,
//! feature_code, display, price_reduction
//! ```
//!
//! `source_year`, `category`, and `channel` are **deliberately omitted**
//! from the physical columns. They are encoded in the Hive-style
//! partition directory layout (`year=N/category=…/channel=…/`) and any
//! Hive-aware engine (DuckDB with `hive_partitioning=true`, PyArrow,
//! Polars, Spark, Iceberg, Delta Lake) reads them as virtual columns
//! from the directory names. This is the modern lakehouse convention
//! (Iceberg/Delta) and avoids:
//!
//! - redundancy between file content and directory metadata
//! - duplicate-column conflicts in readers that auto-detect Hive
//!   partitioning and merge partition columns into the schema
//! - wasted bytes on what is essentially a constant per file
//!
//! `source_year` is additionally derivable from `week` via
//! `fixed_width::week_to_year`; `category` and `channel` are not
//! derivable from row content but are still partition-only because
//! they are constant per file.

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, UInt16Builder, UInt32Builder,
    UInt8Builder,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// Bumped when the schema or column ordering changes. The manifest
/// stores `CURRENT_SCHEMA_VERSION` per record; resume skips only if
/// the on-disk manifest schema_version matches.
///
/// Version history:
/// - v1: 14 columns including `source_year`, `category`, `channel`.
/// - v2: 13 columns; `source_year` removed (derivable from `week`).
/// - v3: 11 columns; `category` and `channel` removed too. All three
///   are partition-only now, following the modern lakehouse convention
///   (Iceberg/Delta) where partition columns live in the table
///   metadata, not in the data files.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("iri_key", DataType::UInt32, false),
        Field::new("week", DataType::UInt16, false),
        Field::new("sy", DataType::UInt8, false),
        Field::new("ge", DataType::UInt8, false),
        Field::new("vend", DataType::UInt32, false),
        Field::new("item", DataType::UInt32, false),
        Field::new("units", DataType::Int32, false),
        Field::new("dollars_cents", DataType::Int64, false),
        Field::new("feature_code", DataType::UInt8, false),
        Field::new("display", DataType::UInt8, false),
        Field::new("price_reduction", DataType::Boolean, false),
    ]))
}

/// Pre-allocated column builders sized to fit one batch in memory.
///
/// Memory budget for a 1M-row batch (upper bound):
/// - 6 × 4-byte ints           = 24 MB
/// - 1 × 8-byte int (cents)    =  8 MB  (Int64; field width bounds max
///   cents to 9_999_999_900, exceeds i32::MAX, so Int64 is required)
/// - 4 × 1-byte ints           =  4 MB
/// - 1 × 1-byte bool (bit-packed) ≈ 1 MB
/// - 2 × variable string       ≈ 8 MB (low cardinality, dictionary-able)
/// - Arrow validity/null bitmaps: 0 bytes (no nulls in this schema)
///
/// Total ≈ 47 MB per 1M rows. Comfortably bounded.
#[derive(Debug)]
pub struct SalesBuilders {
    pub iri_key: UInt32Builder,
    pub week: UInt16Builder,
    pub sy: UInt8Builder,
    pub ge: UInt8Builder,
    pub vend: UInt32Builder,
    pub item: UInt32Builder,
    pub units: Int32Builder,
    pub dollars_cents: Int64Builder,
    pub feature_code: UInt8Builder,
    pub display: UInt8Builder,
    pub price_reduction: BooleanBuilder,
}

impl SalesBuilders {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            iri_key: UInt32Builder::with_capacity(capacity),
            week: UInt16Builder::with_capacity(capacity),
            sy: UInt8Builder::with_capacity(capacity),
            ge: UInt8Builder::with_capacity(capacity),
            vend: UInt32Builder::with_capacity(capacity),
            item: UInt32Builder::with_capacity(capacity),
            units: Int32Builder::with_capacity(capacity),
            dollars_cents: Int64Builder::with_capacity(capacity),
            feature_code: UInt8Builder::with_capacity(capacity),
            display: UInt8Builder::with_capacity(capacity),
            price_reduction: BooleanBuilder::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.iri_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(mut self, schema: SchemaRef) -> Result<RecordBatch, arrow_schema::ArrowError> {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.iri_key.finish()),
                Arc::new(self.week.finish()),
                Arc::new(self.sy.finish()),
                Arc::new(self.ge.finish()),
                Arc::new(self.vend.finish()),
                Arc::new(self.item.finish()),
                Arc::new(self.units.finish()),
                Arc::new(self.dollars_cents.finish()),
                Arc::new(self.feature_code.finish()),
                Arc::new(self.display.finish()),
                Arc::new(self.price_reduction.finish()),
            ],
        )
    }

    /// Reset all builders so the same struct can be reused for the next batch.
    /// The original capacity is forgotten (the new capacity is the configured
    /// batch size); callers that care about preserving capacity should call
    /// `SalesBuilders::with_capacity(batch_rows)` themselves.
    pub fn reset(&mut self, capacity: usize) {
        self.iri_key = UInt32Builder::with_capacity(capacity);
        self.week = UInt16Builder::with_capacity(capacity);
        self.sy = UInt8Builder::with_capacity(capacity);
        self.ge = UInt8Builder::with_capacity(capacity);
        self.vend = UInt32Builder::with_capacity(capacity);
        self.item = UInt32Builder::with_capacity(capacity);
        self.units = Int32Builder::with_capacity(capacity);
        self.dollars_cents = Int64Builder::with_capacity(capacity);
        self.feature_code = UInt8Builder::with_capacity(capacity);
        self.display = UInt8Builder::with_capacity(capacity);
        self.price_reduction = BooleanBuilder::with_capacity(capacity);
    }
}

/// Test helper that scans every builder length and confirms they agree.
pub fn assert_builders_consistent(b: &SalesBuilders) {
    let n = b.len();
    let all = [
        ("iri_key", b.iri_key.len()),
        ("week", b.week.len()),
        ("sy", b.sy.len()),
        ("ge", b.ge.len()),
        ("vend", b.vend.len()),
        ("item", b.item.len()),
        ("units", b.units.len()),
        ("dollars_cents", b.dollars_cents.len()),
        ("feature_code", b.feature_code.len()),
        ("display", b.display.len()),
        ("price_reduction", b.price_reduction.len()),
    ];
    for (name, len) in all {
        assert_eq!(len, n, "builder {} out of sync: {} vs {}", name, len, n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_stable() {
        let s = schema();
        assert_eq!(s.fields().len(), 11);
        assert_eq!(s.field(0).name(), "iri_key");
        assert_eq!(s.field(0).data_type(), &DataType::UInt32);
        assert_eq!(s.field(7).name(), "dollars_cents");
        assert_eq!(s.field(7).data_type(), &DataType::Int64);
        assert_eq!(s.field(10).name(), "price_reduction");
        assert_eq!(s.field(10).data_type(), &DataType::Boolean);
        // After v3, only the 11 raw-data columns remain. source_year,
        // category, channel are partition-only.
        assert_eq!(s.field(0).name(), "iri_key");
        assert_eq!(s.field(10).name(), "price_reduction");
    }

    #[test]
    fn builders_can_produce_a_round_trippable_batch() {
        let mut b = SalesBuilders::with_capacity(4);
        b.iri_key.append_value(1234567);
        b.week.append_value(1114);
        b.sy.append_value(0);
        b.ge.append_value(2);
        b.vend.append_value(18200);
        b.item.append_value(647);
        b.units.append_value(1);
        b.dollars_cents.append_value(929);
        b.feature_code.append_value(0);
        b.display.append_value(0);
        b.price_reduction.append_value(false);
        assert_builders_consistent(&b);
        let batch = b.finish(schema()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);
        // Sanity: schema version 3 once they are partition-only.
        assert_eq!(crate::model::CURRENT_SCHEMA_VERSION, 3);
    }
}
