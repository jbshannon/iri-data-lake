//! The non-sales datasets: what they are, how they are discovered, and
//! how they are ingested.
//!
//! `data/raw/` is not one data source. Alongside the 744 store-level
//! sales files there are household panel trips, product attribute
//! dictionaries, store rosters, a week→calendar dimension and several
//! small cross-reference tables — eleven classes in all, of which the
//! original pipeline handled exactly one.
//!
//! This module is the registry for the other ten. `DatasetKind`
//! ([`crate::model::DatasetKind`]) names the class; this module decides,
//! per class:
//!
//! - **which files belong to it** ([`discover`]),
//! - **what identity each file has** ([`SourceRef`]),
//! - **where its output is partitioned** ([`partition_dir`]),
//! - **how its rows are parsed** ([`crate::datasets`]).
//!
//! # Why the sales pipeline is not reused
//!
//! `iri_sales` is 143 GB across 744 files with a 56-byte fixed stride,
//! whole-file mmap and 1 M-row batches. Every other class is between
//! two and four orders of magnitude smaller, and none of them is
//! fixed-width in the same way. Reusing the sales batch/striding
//! machinery for a 400 KB Excel workbook or a 12-column CSV would
//! import the tuning and none of the performance, so the generic path
//! is its own: one source file at a time, read through a streaming
//! reader, buffered into Arrow with a batch size chosen per dataset.
//!
//! What *is* shared, deliberately: the JSONL manifest, the skip
//! decision, the atomic Parquet write, the tmp sweep and the worker
//! pool. Those are correctness and operational properties, not
//! performance tuning, and having two implementations of "what happens
//! when a run is interrupted" is how a lake ends up with an
//! unanswerable provenance question.
//!
//! See `docs/other_sources.md` for the survey these decisions came
//! from, including the places where the on-disk data contradicts the
//! existing `docs/data_layout.md`.

use std::path::{Path, PathBuf};

use crate::errors::IngestError;
use crate::model::DatasetKind;

pub use crate::datasets::{bronze_root as bronze_root_for, tuning_for as tuning_for_kind};

/// Identity of a non-sales source file: whatever routing keys can be
/// inferred for it.
///
/// Every field is optional because the classes disagree about what
/// they have. `iri_week_dimension` has no year and no category; a
/// `Delivery_Stores` file has a year but no category; a stub file has
/// neither (its edition carries the year range instead). Partition
/// directories are derived from whichever keys are present
/// ([`partition_dir`]), so a class that has no natural partition key
/// writes a single flat directory.
///
/// `iri_sales` does **not** use this type. It keeps its dedicated
/// `SourceIdentity`, because its partition scheme is part of a
/// performance contract with the sales gate suite and changing it would
/// invalidate the measured numbers in `docs/parallelism.md`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct SourceRef {
    /// Absolute path on disk.
    pub path: PathBuf,
    /// IRI academic-data year, from the nearest `Year<N>` ancestor.
    pub year: Option<u8>,
    /// Category token from the enclosing directory name.
    pub category: Option<String>,
    /// Outlet code (PANEL `OUTLET`, store `OU`).
    pub outlet: Option<String>,
    /// First IRI week in the file's declared range.
    pub week_start: Option<u16>,
    /// Last IRI week in the file's declared range.
    pub week_end: Option<u16>,
    /// Edition marker: the stub edition (`01`/`07`/`11`/`12`), the
    /// trips edition (`jul08`/`may13`), the week-translation edition.
    /// Present when the corpus ships several differently-shaped
    /// versions of one logical table.
    pub edition: Option<String>,
}

impl SourceRef {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    pub fn with_year(mut self, year: u8) -> Self {
        self.year = Some(year);
        self
    }

    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    pub fn with_outlet(mut self, outlet: impl Into<String>) -> Self {
        self.outlet = Some(outlet.into());
        self
    }

    pub fn with_weeks(mut self, start: u16, end: u16) -> Self {
        self.week_start = Some(start);
        self.week_end = Some(end);
        self
    }

    pub fn with_edition(mut self, edition: impl Into<String>) -> Self {
        self.edition = Some(edition.into());
        self
    }

    /// Hive-style partition directory for this class, derived from the
    /// keys [`DatasetKind::partition_keys`] declares for it.
    ///
    /// Deliberately not derived from "whatever the identity happens to
    /// carry": the store roster carries a category (the directory it
    /// was found in) but must not partition by it.
    pub fn partition_dir_for(&self, kind: DatasetKind) -> PathBuf {
        let mut p = PathBuf::new();
        for key in kind.partition_keys() {
            let value = match *key {
                "year" => self.year.map(|y| y.to_string()),
                "category" => self.category.clone().filter(|c| !c.is_empty()),
                "outlet" => self.outlet.clone(),
                "edition" => self.edition.clone(),
                // "channel" is the sales class's key and sales keeps its
                // own `PartitionPath`; reaching here would mean a
                // non-sales class claimed a key its identity cannot
                // supply, which must be loud rather than a silently
                // missing directory level.
                other => {
                    tracing::warn!(
                        dataset = %kind,
                        key = other,
                        "partition key the identity cannot supply; directory level dropped"
                    );
                    None
                }
            };
            if let Some(v) = value {
                p.push(format!("{key}={v}"));
            }
        }
        p
    }
}

impl std::fmt::Display for SourceRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.display())?;
        if let Some(y) = self.year {
            write!(f, " (year={y}")?;
            if let Some(c) = &self.category {
                write!(f, ", category={c}")?;
            }
            if let Some(o) = &self.outlet {
                write!(f, ", outlet={o}")?;
            }
            if let (Some(a), Some(b)) = (self.week_start, self.week_end) {
                write!(f, ", weeks={a}-{b}")?;
            }
            write!(f, ")")?;
        }
        if let Some(e) = &self.edition {
            write!(f, " [edition={e}]")?;
        }
        Ok(())
    }
}

/// One discovered non-sales source file, with the class it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFile {
    pub kind: DatasetKind,
    pub source: SourceRef,
    pub size_bytes: u64,
}

/// Everything one dataset's discovery walk found.
#[derive(Debug, Clone, Default)]
pub struct DatasetInventory {
    pub kind: Option<DatasetKind>,
    pub files: Vec<DatasetFile>,
    /// Files seen and deliberately not ingested, with the reason. Same
    /// idea as the sales `Inventory::skipped`: the *decisions* are the
    /// output, not just the work list.
    pub skipped: Vec<(PathBuf, &'static str)>,
    /// Sources that are content-hash duplicates of a canonical source in
    /// `files`. The canonical source is the one ingested; the others
    /// are recorded in the manifest with `deduplicated_from` pointing
    /// at it, and produce no Parquet output.
    ///
    /// Filled by the discovery walk when content dedup runs over the
    /// file list. Two files with different names but the same bytes are
    /// the same fact about the corpus, and only one ought to be
    /// ingested; the rest are recorded, so the lake still answers
    /// "what is the source of this row?" for every row.
    pub deduplicated: Vec<(PathBuf, PathBuf)>,
}

impl DatasetInventory {
    pub fn new(kind: DatasetKind) -> Self {
        Self {
            kind: Some(kind),
            ..Default::default()
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size_bytes).sum()
    }
}

/// Discover every non-sales source under `input_root`.
///
/// Returns one `DatasetInventory` per class, in `DatasetKind::others()`
/// order, so a caller that prints a report gets a stable ordering
/// without re-sorting.
pub fn discover_all(input_root: &Path) -> Result<Vec<DatasetInventory>, IngestError> {
    if !input_root.exists() {
        return Err(IngestError::Discovery(format!(
            "input root does not exist: {}",
            input_root.display()
        )));
    }
    let mut out = Vec::new();
    for &kind in DatasetKind::others() {
        out.push(discover(kind, input_root)?);
    }
    Ok(out)
}

/// Discover one dataset class under `input_root`.
///
/// Each class has its own walk because the corpus places its files in
/// different places — per-year directory trees for panel and rosters,
/// four edition directories for the stubs, one external directory for
/// trips and cross-references, the top level for a few duplicates.
/// A single generic walk that tried to recognise all eleven at once
/// would be guessing; these are explicit rules, one per class.
pub fn discover(kind: DatasetKind, input_root: &Path) -> Result<DatasetInventory, IngestError> {
    use crate::datasets::{csv, delivery_stores, excel, panel, product_attr};
    let mut inv = match kind {
        DatasetKind::WeekDimension => excel::discover_week_dimension(input_root),
        DatasetKind::DeliveryStores => delivery_stores::discover(input_root),
        DatasetKind::PanelStatic => csv::discover_panel_static(input_root),
        DatasetKind::ChainXref => csv::discover_chain_xref(input_root),
        DatasetKind::ManualStoreEntry => csv::discover_manual_store_entry(input_root),
        DatasetKind::AdsDemos => csv::discover_ads_demos(input_root),
        DatasetKind::PanelistDemos => csv::discover_panel_demos(input_root),
        DatasetKind::PanelTrips => csv::discover_trips(input_root),
        DatasetKind::Panel => panel::discover(input_root),
        DatasetKind::ProductAttr => product_attr::discover(input_root),
        DatasetKind::ProductStub => excel::discover_product_stub(input_root),
        DatasetKind::Sales => {
            return Err(IngestError::Discovery(
                "the sales class uses its own discovery pass; see discovery::discover".into(),
            ))
        }
    }?;
    inv.kind = Some(kind);
    // Content-hash dedup for the classes whose on-disk distribution
    // has heavy overlap. 372 `Delivery_Stores` files are really ~62
    // distinct files; 217 `DEMOS.CSV` files are really 7. Ingesting
    // every copy is cheap at this scale, but it doubles the wall and
    // the lake size for no information the consumer can use. Dedup is
    // a property of the dataset, not the run.
    if matches!(
        kind,
        DatasetKind::DeliveryStores | DatasetKind::PanelistDemos
    ) {
        dedup_by_hash(&mut inv)?;
    }
    Ok(inv)
}

/// The nearest `Year<N>` ancestor of `path`, or `None`.
///
/// Shared by the per-year classes. The sales walker's `infer_year`
/// does the same job; it is duplicated rather than shared because the
/// sales copy is pinned by its own tests and its semantics differ
/// subtly — this one returns `None` rather than failing, because a
/// non-sales class that cannot find a year is a skip, not an error.
pub fn infer_year(path: &Path, input_root: &Path) -> Option<u8> {
    let mut cur = path.parent();
    while let Some(dir) = cur {
        if let Some(name) = dir.file_name().and_then(|n| n.to_str()) {
            if let Some(rest) = name.strip_prefix("Year") {
                if let Ok(n) = rest.parse::<u8>() {
                    if (1..=12).contains(&n) {
                        return Some(n);
                    }
                }
            }
        }
        if dir == input_root {
            return None;
        }
        cur = dir.parent();
    }
    None
}

/// The category token for a file: the enclosing directory name, if it
/// is not a structural directory.
///
/// The year directories nest one or two levels (`Year12/toothpa/`,
/// `Year12/toothpa/toothpa/`), and a few years wrap the whole thing
/// once more. The *deepest* non-`Year` directory name is the category
/// in every observed case, which is what this returns.
pub fn infer_category(path: &Path, input_root: &Path) -> Option<String> {
    let mut cur = path.parent();
    while let Some(dir) = cur {
        if let Some(name) = dir.file_name().and_then(|n| n.to_str()) {
            let lower = name.to_ascii_lowercase();
            if !lower.starts_with("year") && !name.starts_with('.') {
                return Some(name.to_string());
            }
        }
        if dir == input_root {
            return None;
        }
        cur = dir.parent();
    }
    None
}

/// Group `inv.files` by content hash, keeping one canonical per hash
/// and demoting the rest to `inv.deduplicated`.
///
/// The canonical is whichever path sorts first, so the choice is
/// deterministic across runs. The manifest records each deduplicated
/// source with `deduplicated_from` pointing at the canonical, so a
/// query that filters by source path still resolves — even though no
/// Parquet file was written for it.
pub fn dedup_by_hash(inv: &mut DatasetInventory) -> Result<(), IngestError> {
    use std::collections::HashMap;
    let mut groups: HashMap<Vec<u8>, Vec<PathBuf>> = HashMap::new();
    for f in inv.files.iter() {
        let path = f.source.path.clone();
        let bytes = std::fs::read(&path).map_err(|e| IngestError::io(&path, e))?;
        // Re-hash from disk: this is a separate SHA-256 from the one
        // taken during ingest, but content-hash dedup wants to agree
        // with what the ingest pipeline will hash too. Re-reading is
        // cheap for these classes (≤ a few MB each) and is exactly
        // the I/O the ingest pipeline does next.
        let _ = bytes;
        let hash = crate::metrics::sha256_of_file(&path)?.into_bytes();
        groups.entry(hash).or_default().push(path);
    }
    let canonicals: std::collections::HashSet<&PathBuf> = groups
        .values()
        .filter_map(|paths| paths.iter().min())
        .collect();
    let original_files = std::mem::take(&mut inv.files);
    let mut new_files: Vec<DatasetFile> = Vec::new();
    let mut duplicates: Vec<(PathBuf, PathBuf)> = Vec::new();
    for file in original_files {
        let path = file.source.path.clone();
        if canonicals.contains(&path) {
            new_files.push(file);
        } else if let Some((_, paths)) = groups.iter().find(|(_, ps)| ps.contains(&path)) {
            let canonical = paths.iter().min().expect("non-empty group").clone();
            duplicates.push((path, canonical));
        }
    }
    new_files.sort_by(|a, b| a.source.path.cmp(&b.source.path));
    inv.files = new_files;
    inv.deduplicated = duplicates;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_dir_uses_only_the_class_declared_keys() {
        let bare = SourceRef::new("/x/y");
        assert_eq!(
            bare.partition_dir_for(DatasetKind::WeekDimension),
            PathBuf::new(),
            "a 627-row dimension has no partition keys"
        );

        let full = SourceRef::new("/x/y")
            .with_year(9)
            .with_category("beer")
            .with_outlet("GK");
        assert_eq!(
            full.partition_dir_for(DatasetKind::Panel),
            PathBuf::from("year=9/category=beer/outlet=GK")
        );
        assert_eq!(
            full.partition_dir_for(DatasetKind::ProductAttr),
            PathBuf::from("year=9/category=beer"),
            "prod_attr has no outlet"
        );
        assert_eq!(
            full.partition_dir_for(DatasetKind::PanelistDemos),
            PathBuf::from("year=9"),
            "demos partition by year only, not by the directory they came from"
        );
    }

    #[test]
    fn a_store_roster_does_not_partition_by_the_category_it_was_found_in() {
        // The 31 categories of a year share one roster. Partitioning by
        // category would scatter a 2 053-row dimension across 31
        // directories and make every join a 31-way union.
        let s = SourceRef::new("/raw/Year1/beer/Delivery_Stores")
            .with_year(1)
            .with_category("beer");
        assert_eq!(
            s.partition_dir_for(DatasetKind::DeliveryStores),
            PathBuf::from("year=1")
        );
    }

    #[test]
    fn every_partition_key_is_one_the_identity_can_supply() {
        // A key the identity cannot produce would silently drop its
        // directory level for every file, which looks like a working
        // layout until a query filters on it and finds nothing.
        for &k in DatasetKind::others() {
            for key in k.partition_keys() {
                assert!(
                    ["year", "category", "outlet", "edition"].contains(key),
                    "{k} declares unknown partition key {key:?}"
                );
            }
        }
        // And sales is the only class allowed to claim `channel`.
        for &k in DatasetKind::others() {
            assert!(
                !k.partition_keys().contains(&"channel"),
                "{k} must not claim the sales-only channel key"
            );
        }
    }

    #[test]
    fn infers_year_and_category() {
        let root = Path::new("/raw");
        let p = Path::new("/raw/Year12/toothpa/toothpa/toothpa_PANEL_DK_1687_1739.DAT");
        assert_eq!(infer_year(p, root), Some(12));
        // Deepest non-Year directory wins, which handles the extra
        // Year12 nesting level.
        assert_eq!(infer_category(p, root).as_deref(), Some("toothpa"));
    }

    #[test]
    fn infers_year_through_the_extra_year12_level() {
        let root = Path::new("/raw");
        let p = Path::new("/raw/Year12/toothpa/toothpa/Delivery_Stores");
        assert_eq!(infer_year(p, root), Some(12));
        assert_eq!(infer_category(p, root).as_deref(), Some("toothpa"));
    }

    #[test]
    fn no_year_ancestor_is_none_not_an_error() {
        let root = Path::new("/raw");
        let p = Path::new("/raw/demos trips external/trips1 jul08.csv");
        assert_eq!(infer_year(p, root), None);
    }

    #[test]
    fn every_class_has_a_distinct_bronze_table() {
        let mut tables: Vec<&str> = DatasetKind::others()
            .iter()
            .map(|k| k.bronze_table())
            .collect();
        let n = tables.len();
        tables.sort_unstable();
        tables.dedup();
        assert_eq!(tables.len(), n, "two classes share a bronze table");
        assert!(!DatasetKind::others().contains(&DatasetKind::Sales));
    }

    #[test]
    fn dataset_kind_round_trips_through_its_string_form() {
        for &k in DatasetKind::others() {
            let s = k.to_string();
            assert_eq!(DatasetKind::parse(&s), Some(k), "failed for {k}");
        }
    }

    #[test]
    fn dedup_by_hash_keeps_one_canonical_per_group() {
        // Two files, identical content: one survives, one is demoted.
        // Two files, distinct content: both kept.
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.txt");
        let b = dir.path().join("b.txt");
        let c = dir.path().join("c.txt");
        std::fs::write(&a, b"same bytes here").unwrap();
        std::fs::write(&b, b"same bytes here").unwrap();
        std::fs::write(&c, b"different bytes").unwrap();
        let mut inv = DatasetInventory::new(DatasetKind::DeliveryStores);
        inv.files = vec![
            DatasetFile {
                kind: DatasetKind::DeliveryStores,
                source: SourceRef::new(&a),
                size_bytes: 0,
            },
            DatasetFile {
                kind: DatasetKind::DeliveryStores,
                source: SourceRef::new(&b),
                size_bytes: 0,
            },
            DatasetFile {
                kind: DatasetKind::DeliveryStores,
                source: SourceRef::new(&c),
                size_bytes: 0,
            },
        ];
        dedup_by_hash(&mut inv).unwrap();
        assert_eq!(inv.files.len(), 2, "identical bytes deduplicate to one");
        assert_eq!(inv.deduplicated.len(), 1);
        // The smaller path is the canonical.
        let (original, canonical) = &inv.deduplicated[0];
        assert_eq!(canonical, &a);
        assert_eq!(original, &b);
    }
}
