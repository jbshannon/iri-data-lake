//! Discovery of eligible sales files.
//!
//! The source corpus is laid out roughly as
//!
//! ```text
//! data/raw/
//!   Year1/<cat>/<cat>_(drug|groc)_<w1>_<w2>
//!   Year2/...
//!   ...
//!   Year12/<cat>/<cat>/<cat>_(drug|groc)_<w1>_<w2>   <-- nested one extra level
//! ```
//!
//! The walker therefore:
//!
//! - recurses into every directory beneath `input_root`
//! - skips known noise (Office lock files, `.OLD`, `.bak`, hidden dirs,
//!   the `parsed stub files*`, `demos trips external`, `Academic …`,
//!   `Pacesetters external`, `TNS advertising data*` directories at the
//!   top level, plus any file that does not match the sales filename shape)
//! - infers year from the nearest `Year<N>` ancestor
//! - infers category, channel, and week range from the filename
//!
//! No fixed list of category names is assumed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use crate::errors::IngestError;
use crate::model::{Channel, SourceIdentity};

/// A file that was found, with its inferred identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredFile {
    pub identity: SourceIdentity,
    pub size_bytes: u64,
}

/// Top-level inventory: file count, raw bytes, expected-row totals.
#[derive(Debug, Clone, Default)]
pub struct Inventory {
    pub files: Vec<DiscoveredFile>,
    /// Files that were skipped (and why) for reporting.
    pub skipped: Vec<SkippedFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedFile {
    pub path: PathBuf,
    pub reason: SkipReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    LockFile,
    BackupExtension,
    IgnoredDirectory,
    NotSalesFilename,
    PanelFile,
    StubOrExcel,
    NonUtf8Filename,
    BadYearAncestor,
    InvalidChannel,
    UnparseableFilename,
}

impl SkipReason {
    pub fn label(self) -> &'static str {
        match self {
            SkipReason::LockFile => "lock_file",
            SkipReason::BackupExtension => "backup_extension",
            SkipReason::IgnoredDirectory => "ignored_directory",
            SkipReason::NotSalesFilename => "not_sales_filename",
            SkipReason::PanelFile => "panel_file",
            SkipReason::StubOrExcel => "stub_or_excel",
            SkipReason::NonUtf8Filename => "non_utf8_filename",
            SkipReason::BadYearAncestor => "bad_year_ancestor",
            SkipReason::InvalidChannel => "invalid_channel",
            SkipReason::UnparseableFilename => "unparseable_filename",
        }
    }
}

impl Inventory {
    pub fn total_files(&self) -> usize {
        self.files.len()
    }
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size_bytes).sum()
    }
    /// Expected rows = `size_bytes / RECORD_LEN - 1` (subtract the header).
    pub fn total_expected_rows(&self) -> u64 {
        self.files
            .iter()
            .map(|f| crate::fixed_width::expected_rows(f.size_bytes).unwrap_or(0))
            .sum()
    }

    /// Bucket by (year, channel). Useful for the inventory table.
    pub fn by_year_channel(&self) -> BTreeMap<(u8, Channel), YearChannelBucket> {
        let mut out: BTreeMap<(u8, Channel), YearChannelBucket> = BTreeMap::new();
        for f in &self.files {
            let key = (f.identity.year, f.identity.channel);
            let entry = out.entry(key).or_default();
            entry.file_count += 1;
            entry.total_bytes += f.size_bytes;
            entry.total_expected_rows +=
                crate::fixed_width::expected_rows(f.size_bytes).unwrap_or(0);
            entry.categories.insert(f.identity.category.clone());
        }
        out
    }
}

#[derive(Debug, Clone, Default)]
pub struct YearChannelBucket {
    pub file_count: usize,
    pub total_bytes: u64,
    pub total_expected_rows: u64,
    pub categories: std::collections::BTreeSet<String>,
}

/// Directories beneath `input_root` that we know are not sales data.
/// Matched case-insensitively against the directory name (not the full path).
const IGNORED_DIR_NAMES: &[&str] = &[
    "academic data set file and field description",
    "parsed stub files",
    "parsed stub files 2007",
    "parsed stub files 2008-2011",
    "parsed stub files 2012",
    "demos trips external",
    "pacesetters external",
    "tns advertising data2",
];

/// Discover eligible sales files under `input_root`.
pub fn discover(input_root: &Path) -> Result<Inventory, IngestError> {
    if !input_root.exists() {
        return Err(IngestError::Discovery(format!(
            "input root does not exist: {}",
            input_root.display()
        )));
    }

    let mut inv = Inventory::default();

    for entry in WalkDir::new(input_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !is_ignored_dir(e.path(), input_root))
    {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                // Don't abort the whole walk for one bad entry; record and continue.
                inv.skipped.push(SkippedFile {
                    path: err.path().map(|p| p.to_path_buf()).unwrap_or_default(),
                    reason: SkipReason::UnparseableFilename,
                });
                continue;
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        if let Some(reason) = skip_reason_for_filename(path) {
            inv.skipped.push(SkippedFile {
                path: path.to_path_buf(),
                reason,
            });
            continue;
        }

        // Try to interpret as an eligible sales file.
        match parse_identity(path, input_root) {
            Ok(identity) => {
                let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                inv.files.push(DiscoveredFile {
                    identity,
                    size_bytes: size,
                });
            }
            Err(SkipReason::InvalidChannel)
            | Err(SkipReason::NotSalesFilename)
            | Err(SkipReason::UnparseableFilename)
            | Err(SkipReason::BadYearAncestor)
            | Err(SkipReason::NonUtf8Filename) => {
                inv.skipped.push(SkippedFile {
                    path: path.to_path_buf(),
                    reason: SkipReason::NotSalesFilename,
                });
            }
            Err(other) => {
                inv.skipped.push(SkippedFile {
                    path: path.to_path_buf(),
                    reason: other,
                });
            }
        }
    }

    Ok(inv)
}

fn is_ignored_dir(path: &Path, input_root: &Path) -> bool {
    // Only inspect directory entries (filter_entry is called for everything,
    // so guard with metadata).
    let is_dir = path.is_dir();
    if !is_dir {
        // Don't descend into a regular file's "tree" (which is empty anyway).
        return false;
    }
    // Don't ignore input_root itself.
    if path == input_root {
        return false;
    }
    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
        if name.starts_with('.') {
            return true;
        }
        let lower = name.to_ascii_lowercase();
        if IGNORED_DIR_NAMES.iter().any(|d| *d == lower) {
            return true;
        }
    }
    false
}

fn skip_reason_for_filename(path: &Path) -> Option<SkipReason> {
    let name = path.file_name().and_then(|n| n.to_str())?;
    // Office lock files: `~$foo`
    if name.starts_with("~$") {
        return Some(SkipReason::LockFile);
    }
    // Backup extensions — case-insensitive.
    let upper = name.to_ascii_uppercase();
    if upper.ends_with(".OLD") || upper.ends_with(".BAK") || upper.ends_with(".TMP") {
        return Some(SkipReason::BackupExtension);
    }
    // Anything with .xls, .xlsx, .doc, .docx, .csv, .pdf, .zip is not sales
    if upper.ends_with(".XLS")
        || upper.ends_with(".XLSX")
        || upper.ends_with(".DOC")
        || upper.ends_with(".DOCX")
        || upper.ends_with(".CSV")
        || upper.ends_with(".PDF")
        || upper.ends_with(".ZIP")
    {
        return Some(SkipReason::StubOrExcel);
    }
    // PANEL files (handled by a separate pipeline; never sales)
    if name.contains("PANEL_") || name.contains("_PANEL") {
        return Some(SkipReason::PanelFile);
    }
    None
}

/// Parse a sales-file path into a `SourceIdentity`.
///
/// Expected filename shape: `<category>_(drug|groc)_(<digits>)_(<digits>)`
/// (no extension required). Year is inferred from the closest ancestor
/// directory whose name is `Year<digit>+`.
pub fn parse_identity(path: &Path, input_root: &Path) -> Result<SourceIdentity, SkipReason> {
    // File must live beneath input_root.
    if !path.starts_with(input_root) {
        return Err(SkipReason::NotSalesFilename);
    }

    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(s) => s,
        None => return Err(SkipReason::NonUtf8Filename),
    };

    // Split on underscores. Sales files always have at least 4 segments.
    let parts: Vec<&str> = file_name.split('_').collect();
    if parts.len() < 4 {
        return Err(SkipReason::NotSalesFilename);
    }

    // Last two segments must be numeric week numbers.
    let week_end_str = parts[parts.len() - 1];
    let week_start_str = parts[parts.len() - 2];
    let week_start: u16 = week_start_str
        .parse()
        .map_err(|_| SkipReason::UnparseableFilename)?;
    let week_end: u16 = week_end_str
        .parse()
        .map_err(|_| SkipReason::UnparseableFilename)?;
    if week_start == 0 || week_end < week_start {
        return Err(SkipReason::UnparseableFilename);
    }

    // Channel is the third-to-last segment.
    let channel_str = parts[parts.len() - 3];
    let channel = match channel_str {
        "drug" => Channel::Drug,
        "groc" => Channel::Groc,
        _ => return Err(SkipReason::InvalidChannel),
    };

    // Everything before the channel forms the category token, joined back
    // with underscores. (Category names do not contain underscores in the
    // observed data, but be robust: collapse them.)
    let category_parts = &parts[..parts.len() - 3];
    let category = category_parts.join("_");
    if category.is_empty() {
        return Err(SkipReason::UnparseableFilename);
    }

    // Walk up looking for a Year<N> ancestor.
    let year = infer_year(path, input_root).ok_or(SkipReason::BadYearAncestor)?;
    if !(1..=12).contains(&year) {
        return Err(SkipReason::BadYearAncestor);
    }

    Ok(SourceIdentity {
        path: path.to_path_buf(),
        year,
        category,
        channel,
        filename_week_start: week_start,
        filename_week_end: week_end,
    })
}

/// Walk up from `path` toward `input_root`, looking for a directory whose
/// name matches `Year\d+`. Returns the parsed integer.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn id(p: &str) -> PathBuf {
        PathBuf::from(p)
    }

    #[test]
    fn parses_simple_filename() {
        let root = id("/raw");
        let path = id("/raw/Year3/beer/beer_drug_1218_1269");
        let identity = parse_identity(&path, &root).unwrap();
        assert_eq!(identity.year, 3);
        assert_eq!(identity.category, "beer");
        assert_eq!(identity.channel, Channel::Drug);
        assert_eq!(identity.filename_week_start, 1218);
        assert_eq!(identity.filename_week_end, 1269);
    }

    #[test]
    fn parses_nested_year12_toothpa() {
        let root = id("/raw");
        let path = id("/raw/Year12/toothpa/toothpa/toothpa_drug_1687_1739");
        let identity = parse_identity(&path, &root).unwrap();
        assert_eq!(identity.year, 12);
        assert_eq!(identity.category, "toothpa");
        assert_eq!(identity.channel, Channel::Drug);
    }

    #[test]
    fn rejects_panel_filename_via_skip_reason() {
        // skip_reason_for_filename rejects PANEL files before parse_identity
        // sees them. The end-to-end walker behaviour is covered by the
        // integration tests in tests/discovery_tests.rs.
        let reason = skip_reason_for_filename(std::path::Path::new(
            "/raw/Year1/beer/beer_PANEL_DR_1114_1165.dat",
        ));
        assert_eq!(reason, Some(SkipReason::PanelFile));
    }

    #[test]
    fn parse_identity_rejects_malformed_panels_directly() {
        // Called directly with a PANEL filename, parse_identity returns
        // UnparseableFilename because the `.dat` extension is not a numeric
        // week range. The PANEL filtering happens upstream in the walker.
        let root = id("/raw");
        let path = id("/raw/Year1/beer/beer_PANEL_DR_1114_1165.dat");
        let err = parse_identity(&path, &root).unwrap_err();
        assert_eq!(err, SkipReason::UnparseableFilename);
    }

    #[test]
    fn rejects_lock_file() {
        assert_eq!(
            skip_reason_for_filename(Path::new("/x/~$beer_drug_1_52")),
            Some(SkipReason::LockFile)
        );
    }

    #[test]
    fn rejects_backup_extensions() {
        assert_eq!(
            skip_reason_for_filename(Path::new("/x/beer_drug_1_52.OLD")),
            Some(SkipReason::BackupExtension)
        );
        assert_eq!(
            skip_reason_for_filename(Path::new("/x/beer_drug_1_52.BAK")),
            Some(SkipReason::BackupExtension)
        );
    }

    #[test]
    fn rejects_excel_and_csv() {
        assert_eq!(
            skip_reason_for_filename(Path::new("/x/prod_attr.xls")),
            Some(SkipReason::StubOrExcel)
        );
        assert_eq!(
            skip_reason_for_filename(Path::new("/x/DEMOS.CSV")),
            Some(SkipReason::StubOrExcel)
        );
    }

    #[test]
    fn rejects_bad_week_range() {
        let root = id("/raw");
        let path = id("/raw/Year1/beer/beer_drug_0_52");
        assert!(parse_identity(&path, &root).is_err());
    }

    #[test]
    fn rejects_unknown_channel() {
        let root = id("/raw");
        let path = id("/raw/Year1/beer/beer_other_1114_1165");
        let err = parse_identity(&path, &root).unwrap_err();
        assert_eq!(err, SkipReason::InvalidChannel);
    }

    #[test]
    fn rejects_out_of_range_year() {
        let root = id("/raw");
        let path = id("/raw/Year15/beer/beer_drug_1_52");
        let err = parse_identity(&path, &root).unwrap_err();
        assert_eq!(err, SkipReason::BadYearAncestor);
    }
}
