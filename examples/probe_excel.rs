//! Probe the real Excel files in the corpus to understand their structure.
//!
//! Run with: cargo run --release --example probe_excel

use calamine::{Data, Reader};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // `--scan` reports, per cell variant, how many cells of each kind a
    // workbook holds. The FIPS parser flattens `Empty`, an absent cell,
    // an empty string and an `Error` (which is what an Excel `#REF!` or
    // `#DIV/0!` reads as) all to `""`, so this is the only way to tell
    // whether any of them actually occur in a given workbook.
    if args.iter().any(|a| a == "--scan") {
        for p in args.iter().skip(1).filter(|a| !a.starts_with("--")) {
            scan_cells(std::path::Path::new(p));
        }
        return;
    }
    let base = std::path::Path::new("data/raw");

    // Probe the authoritative week translation file
    let week_path = base.join("demos trips external/IRI week translation.xls");
    if week_path.exists() {
        probe_workbook(&week_path);
    } else {
        println!("Week translation file not found at {}", week_path.display());
    }

    // Probe a per-year week translation copy
    let per_year = base.join("Year1/beer/IRI week translation.xls");
    if per_year.exists() {
        probe_workbook(&per_year);
    } else {
        // Try to find one
        for year in 1..=5 {
            let p = base.join(format!("Year{}/beer/IRI week translation.xls", year));
            if p.exists() {
                probe_workbook(&p);
                break;
            }
        }
    }

    // Probe the FIPS market file
    let fips_path = base.join("demos trips external/fips by IRI market.xls");
    if fips_path.exists() {
        probe_workbook(&fips_path);
    }

    // Probe a product stub
    let stub_path = base.join("parsed stub files/prod_beer.xls");
    if stub_path.exists() {
        probe_workbook(&stub_path);
    }

    // Probe the panel complete store list
    let panel_path = base.join("demos trips external/panel complete store list with chains.xlsx");
    if panel_path.exists() {
        probe_workbook(&panel_path);
    }
}

/// Count each `calamine::Data` variant across every sheet of a workbook.
fn scan_cells(path: &std::path::Path) {
    println!("\n=== scan {} ===", path.display());
    let Ok(mut wb) = calamine::open_workbook_auto(path) else {
        println!("  cannot open");
        return;
    };
    let mut counts: std::collections::BTreeMap<&'static str, usize> = Default::default();
    for name in wb.sheet_names().to_vec() {
        let Ok(range) = wb.worksheet_range(&name) else {
            continue;
        };
        for (row_idx, row) in range.rows().enumerate() {
            for cell in row {
                let k = match cell {
                    Data::Empty => "Empty",
                    Data::String(s) if s.is_empty() => "String(empty)",
                    Data::String(_) => "String(value)",
                    Data::Float(_) => "Float",
                    Data::Int(_) => "Int",
                    Data::Bool(_) => "Bool",
                    Data::Error(_) => "Error  <-- #REF!/#N/A/#DIV/0!",
                    Data::DateTime(_) => "DateTime",
                    Data::DateTimeIso(_) => "DateTimeIso",
                    Data::DurationIso(_) => "DurationIso",
                };
                *counts.entry(k).or_default() += 1;
                if matches!(cell, Data::Empty) {
                    println!(
                        "     Empty at row {} col {}",
                        row_idx,
                        row.iter()
                            .position(|x| std::ptr::eq(x, cell))
                            .unwrap_or(999)
                    );
                }
            }
        }
    }
    for (k, v) in &counts {
        println!("  {:<36} {}", k, v);
    }
}

fn probe_workbook(path: &std::path::Path) {
    println!("\n=== {} ===", path.display());
    let mut wb = match calamine::open_workbook_auto(path) {
        Ok(wb) => wb,
        Err(e) => {
            println!("  ERROR opening: {e}");
            return;
        }
    };

    let names = wb.sheet_names();
    println!("  Sheets: {:?}", names);

    for name in &names {
        match wb.worksheet_range(name) {
            Ok(range) => {
                println!(
                    "  Sheet '{name}': {} rows x {} cols",
                    range.height(),
                    range.width()
                );
                // Print first 5 rows
                for (i, row) in range.rows().take(5).enumerate() {
                    let cells: Vec<String> = row
                        .iter()
                        .map(|c| match c {
                            Data::Empty => "".to_string(),
                            Data::Float(v) => format!("F:{v}"),
                            Data::Int(v) => format!("I:{v}"),
                            Data::String(s) => format!("S:{s}"),
                            Data::Bool(v) => format!("B:{v}"),
                            Data::Error(e) => format!("E:{e}"),
                            Data::DateTime(dt) => {
                                format!(
                                    "D:{}",
                                    dt.as_datetime().map(|d| d.to_string()).unwrap_or_default()
                                )
                            }
                            Data::DateTimeIso(s) => format!("DI:{s}"),
                            Data::DurationIso(s) => format!("DU:{s}"),
                        })
                        .collect();
                    println!("    Row {i}: {:?}", cells);
                }
            }
            Err(e) => println!("  Sheet '{name}': ERROR: {e}"),
        }
    }
}
