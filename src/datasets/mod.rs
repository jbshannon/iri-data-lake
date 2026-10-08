//! The non-sales dataset classes: one module per class family, plus
//! the generic driver they all share.
//!
//! | module | classes |
//! |---|---|
//! | [`panel`] | `iri_panel` |
//! | [`csv`] | `iri_panel_trips`, `iri_panel_static`, `iri_panelist_demos`, `iri_ads_demos`, `iri_chain_xref`, `iri_manual_store_entry` |
//! | [`delivery_stores`] | `iri_delivery_stores` |
//! | [`product_attr`] | `iri_product_attr` |
//! | [`excel`] | `iri_week_dimension`, `iri_product_stub` |
//! | [`delimited`] | shared byte-level line parsing |
//! | [`ingest`] | the generic driver: sha → skip → parse → Parquet → manifest |
//! | [`umbrella`] | the `ingest-all` runner that sequences the classes |
//!
//! The registry that maps a [`crate::model::DatasetKind`] to a
//! discovery walk and an ingest tuning lives in [`crate::dataset`].
//!
//! See `docs/other_sources.md` for the corpus survey behind all of
//! this, including the places where the on-disk data contradicts
//! `docs/data_layout.md`.

pub mod csv;
pub mod delimited;
pub mod delivery_stores;
pub mod excel;
pub mod ingest;
pub mod panel;
pub mod product_attr;
pub mod umbrella;

use std::path::Path;

use crate::errors::IngestError;
use crate::model::DatasetKind;

use ingest::DatasetTuning;

/// The per-class ingest tuning: batch size, output file size, and the
/// parser entry point.
pub fn tuning_for(kind: DatasetKind) -> Result<DatasetTuning, IngestError> {
    use DatasetKind::*;
    Ok(match kind {
        Panel => panel::tuning(),
        DeliveryStores => delivery_stores::tuning(),
        ProductAttr => product_attr::tuning(),
        PanelTrips => csv::trips_tuning(),
        PanelStatic => csv::static_tuning(),
        PanelistDemos => csv::demos_tuning(csv::panel_demos_parser as ingest::ParserFactory),
        AdsDemos => csv::demos_tuning(csv::ads_demos_parser as ingest::ParserFactory),
        WeekDimension => excel::week_tuning(),
        ProductStub => excel::stub_tuning(),
        ChainXref => csv::chain_xref_tuning(),
        ManualStoreEntry => csv::manual_store_entry_tuning(),
        Sales => {
            return Err(IngestError::Discovery(
                "the sales class uses ingest::ingest_all, not this driver".into(),
            ))
        }
    })
}

/// `bronze/<table>` for a class.
pub fn bronze_root(kind: DatasetKind, output_root: &Path) -> std::path::PathBuf {
    output_root.join("bronze").join(kind.bronze_table())
}
