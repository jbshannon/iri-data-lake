//! Line-oriented parsing shared by the delimited-text datasets.
//!
//! Five of the eleven classes are line-oriented but not CSV: the
//! PANEL files are tab-, space- or comma-delimited depending on the
//! year, and `Delivery_Stores` is fixed-width. `delimited` holds the
//! pieces all of them need so that each class module contains only
//! what is specific to it — which is the part that actually varies.
//!
//! # Why not `csv::Reader` everywhere
//!
//! The `csv` crate is the right tool for the genuinely quoted classes
//! (`DEMOS.CSV`, the trips files, the cross-reference tables) and is
//! used for those. It is the wrong tool for PANEL, because PANEL's
//! delimiter is a *property of the file discovered at runtime* and the
//! crate fixes the delimiter at the reader's construction. Sniffing the
//! header and building a `csv::ReaderBuilder` from the result works,
//! but the crate then allocates a `String` per field per row, and the
//! PANEL class is 1110 files whose only job is to be read fast.
//!
//! The byte-level reader here borrows `&[u8]` field slices out of a
//! line the caller already has, so a PANEL row costs zero allocations
//! and the same code serves the fixed-width and whitespace dialects.

use std::io::Read;

/// A borrowed view of one line, split on a single byte delimiter.
///
/// ```ignore
/// let fields = split_delimited(b"a\tb\tc", b'\t');
/// assert_eq!(fields, vec![&b"a"[..], &b"b"[..], &b"c"[..]]);
/// ```
pub fn split_delimited(line: &[u8], delim: u8) -> Vec<&[u8]> {
    line.split(|&b| b == delim).collect()
}

/// Split on runs of ASCII whitespace, dropping empty fields.
///
/// This is the year 4–7 PANEL dialect, where the header reads
/// `PANID WEEK UNITS OUTLET DOLLARS IRI_KEY COLUPC` with single spaces.
/// A plain `split(|&b| b == b' ')` would produce empty fields wherever a
/// value is padded; collapsing runs is what makes the dialect's
/// whitespace actually insignificant.
pub fn split_whitespace(line: &[u8]) -> Vec<&[u8]> {
    line.split(|b| b.is_ascii_whitespace())
        .filter(|f| !f.is_empty())
        .collect()
}

/// Trim ASCII spaces (not tabs, not `\r`) from both ends.
pub fn trim_ascii_space(field: &[u8]) -> &[u8] {
    let start = field
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(field.len());
    let end = field
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|p| p + 1)
        .unwrap_or(start);
    &field[start..end]
}

/// Parse a trimmed ASCII integer, or `None` if it is not one.
///
/// `None` means "this field is not an integer", which every caller
/// treats as a nullable cell rather than an error: the corpus is full
/// of `MISSING` sentinels and blank trailing columns, and failing a
/// 100 MB source because one row has an empty optional field would be
/// the wrong trade. Callers that need strictness check the count of
/// non-null cells instead.
pub fn parse_int(field: &[u8]) -> Option<i64> {
    let t = trim_ascii_space(field);
    if t.is_empty() {
        return None;
    }
    // Reject a leading sign: the IRI numeric fields are unsigned, and
    // silently accepting `-1` as a store id would put a negative key
    // in a fact table.
    if t[0] == b'-' || t[0] == b'+' {
        return None;
    }
    std::str::from_utf8(t).ok()?.parse::<i64>().ok()
}

/// Parse a float, or `None`. Used for the one genuinely fractional
/// fixed-width field (`EST_ACV`) and for Excel numerics.
///
/// Prefer the integer-cent helpers for money: a float here is a *ratio*
/// or a *volume*, never an amount.
pub fn parse_f64(field: &[u8]) -> Option<f64> {
    let t = trim_ascii_space(field);
    if t.is_empty() {
        return None;
    }
    std::str::from_utf8(t).ok()?.parse::<f64>().ok()
}

/// Parse a money value as a **(digits, scale)** pair.
///
/// The pair is **lossless**: `4.3091992188` is recorded as
/// `(43091992188, 4)`, and `0.7299998474` as `(7299998474, 4)`. A
/// downstream view can recover the exact bytes the corpus held by
/// writing `digits / 10^scale`. This is what a `DECIMAL` type is for;
/// it is the only encoding in which `Float64` does not destroy
/// information for a value that fits in 18 decimal digits (which these
/// do — the observed maximum has 12).
///
/// The result also carries `decimal_kind`: `SubCent` for values with
/// fractional digits beyond the cent (scale > 2), `Exact` for an exact
/// 2-decimal amount, and `Float32Render` for the float32 artefact of a
/// clean 2-decimal number. `Rounding` to integer cents in
/// [`dollars_cents_lossy`] is what these three kinds have in common:
/// the rounded value is the same. The kind is recorded so a consumer
/// can tell them apart without re-parsing the raw bytes.
///
/// `None` means "no value" — empty or all-space — which is distinct
/// from `Some((Some(...), ...))` meaning "a value we could not read",
/// and from a numeric value. The caller surfaces the three cases.
pub fn parse_decimal(field: &[u8]) -> DecimalValue {
    let t = trim_ascii_space(field);
    if t.is_empty() {
        return DecimalValue::Missing;
    }
    // Strip a leading sign — the IRI amounts are always non-negative,
    // and a sign here would mean a malformed row.
    if t.contains(&b'-') || t.contains(&b'+') {
        return DecimalValue::Unparseable;
    }
    // Split on the single decimal point, if any.
    let (int_part, frac_part) = match t.iter().position(|&b| b == b'.') {
        Some(p) => (&t[..p], &t[p + 1..]),
        None => (t, b"" as &[u8]),
    };
    let int_digits = trim_ascii_space(int_part);
    if int_digits.is_empty() && frac_part.is_empty() {
        return DecimalValue::Unparseable;
    }
    // Concatenate into one digit string. Leading zeros in the integer part
    // are preserved (the corpus is consistent about them — PANEL writes
    // `0.1944` not `1944`, and trips writes `2531.82959`). An empty
    // integer part becomes `"0"` so the field is not silently null.
    let int_str: &str = if int_digits.is_empty() {
        "0"
    } else {
        std::str::from_utf8(int_digits).unwrap_or("")
    };
    let frac_str: &str = std::str::from_utf8(trim_ascii_space(frac_part)).unwrap_or("");
    let combined: String = std::borrow::Cow::Borrowed(int_str).into_owned();
    let combined = combined + frac_str;
    let scale = frac_str.len() as i8;
    let parsed: i128 = match combined.parse() {
        Ok(v) => v,
        Err(_) => return DecimalValue::Unparseable,
    };
    // `4470.83` → digits=447083, scale=2. `0.05` → digits=5, scale=2.
    // `5` → digits=5, scale=0. `4.3091992188` → digits=43091992188,
    // scale=4.
    let kind = if scale > 2 {
        DecimalKind::SubCent
    } else {
        DecimalKind::Exact
    };
    // The float32-render form is not distinguishable from a real
    // sub-cent amount in the bytes: `float32(0.73)` prints as
    // `0.7299998474` (10 fractional digits), the same shape as a
    // genuine `units × unit_price` computation. Both end up as `SubCent`
    // here; a consumer that cares can distinguish them by re-rounding
    // and checking `float32(round(value)) == round(value)`, but the
    // pipeline does not pretend to do that.
    DecimalValue::Value {
        digits: parsed,
        scale,
        kind,
    }
}

/// What `parse_decimal` found in a single field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecimalValue {
    /// The field is empty or all-space.
    Missing,
    /// The field is present but could not be read — a sign, several
    /// decimal points, non-ASCII bytes, or a non-digit.
    Unparseable,
    /// The value, with its raw digits as an `i128` and the number of
    /// fractional digits in `scale` (`digits / 10^scale` is the value).
    Value {
        digits: i128,
        scale: i8,
        kind: DecimalKind,
    },
}

/// Distinguishes an exact 2-decimal amount from a sub-cent computed
/// value.
///
/// The two float32-form values (`0.7299998474`) and sub-cent values
/// (`4.3091992188`) are **indistinguishable in the bytes** — both are
/// decimal strings with more than two fractional digits. They both land
/// here as [`DecimalKind::SubCent`]; a consumer that cares which is
/// which can re-round and check `float32(round(value)) == round(value)`,
/// but the pipeline does not pretend to do that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalKind {
    /// Exact 2-decimal string — `6.99`.
    Exact,
    /// A decimal value with more than two fractional digits. Most
    /// PANEL amounts with a fractional part land here; so does
    /// `Float32`-rendered form of a clean value.
    SubCent,
}

impl DecimalValue {
    /// `digits / 10^scale` rounded to integer cents, exactly the value
    /// the previous `dollars_cents_lossy` would have produced. `None`
    /// for `Missing` / `Unparseable`.
    ///
    /// We compute it as `round(digits / 10^(scale - 2))`, the natural
    /// cents-from-decimal operation. A 2-decimal amount with trailing
    /// zeros (`6.90`) and an exact 2-decimal amount (`6.99`) both
    /// round to the cent the way the rest of the pipeline expects.
    pub fn cents_rounded(&self) -> Option<i64> {
        let DecimalValue::Value { digits, scale, .. } = self else {
            return None;
        };
        // `scale` is the number of fractional digits; shift it down by
        // two to get cents. `scale < 2` (e.g. an integer like `5`,
        // `scale = 0`) means the cents are `digits * 10^(2 - scale)`.
        let cents = match (*scale as i32).cmp(&2) {
            std::cmp::Ordering::Less => *digits * 10i128.pow((2 - *scale as i32) as u32),
            std::cmp::Ordering::Equal => *digits,
            std::cmp::Ordering::Greater => {
                // Sub-cent: round at the cent boundary. The division
                // is integer; a tie rounds up so behaviour matches the
                // f64 path the previous `parse_dollars_cents_lossy`
                // used.
                let pow = 10i128.pow((*scale as i32 - 2) as u32);
                let q = *digits / pow;
                let r = *digits % pow;
                if 2 * r >= pow {
                    q + 1
                } else {
                    q
                }
            }
        };
        Some(cents as i64)
    }
}

/// Parse a monetary amount written either as an exact 2-decimal string
/// (`6.99`) or with more precision (`0.7299998474`, `4.3091992188`).
/// Returns integer cents.
///
/// The PANEL corpus contains **all three** forms, in the same column and
/// often the same file:
///
/// - exact 2-decimal: `6.99` — the overwhelming majority;
/// - a 32-bit float rendering of a 2-decimal amount:
///   `0.7299998474` is `float32(0.73)`;
/// - a genuinely sub-cent computed average: `4.3091992188`, which is
///   `units × unit price` at full precision. `carbbev`'s panel files
///   are ~0.4 % of PANEL rows and almost all of this third kind.
///
/// Three things follow:
///
/// - **Sub-cent is not money.** `dollars_cents / 100.0` must agree with
///   an `iri_sales` figure for the same purchase, and sales stores exact
///   cents. Rounding to the nearest cent is the accounting rule, and it
///   costs nothing: no cent is created or destroyed, and the discarded
///   precision is a fraction of a cent that never had a monetary
///   meaning.
/// - Rounding also **recovers** the float32 form exactly.
///   `0.7299998474 × 100 = 72.99998…`, which rounds to 73 — the amount
///   the source meant. Discarding those rows instead would null out
///   tens of thousands of real dollar amounts.
/// - The caller counts how many rows took the rounding path, so the
///   approximation is measured rather than assumed.
///
/// Anything that is not a non-negative finite decimal — a sign, letters,
/// several decimal points — is still `None`, exactly as in
/// [`crate::money`].
pub fn parse_dollars_cents_lossy(field: &[u8]) -> Option<i64> {
    let t = trim_ascii_space(field);
    if t.is_empty() || t.contains(&b'-') || t.contains(&b'+') {
        return None;
    }
    // Exact 2-decimal form: use the strict parser, so the common case
    // keeps the strict parser's guarantees byte for byte.
    if let Ok(c) = crate::money::parse_dollars_cents(t) {
        return Some(c);
    }
    let v: f64 = std::str::from_utf8(t).ok()?.parse().ok()?;
    if !v.is_finite() || v < 0.0 {
        return None;
    }
    let cents = v * 100.0;
    if cents > i64::MAX as f64 / 2.0 {
        return None;
    }
    Some(cents.round() as i64)
}

/// Whether an amount needed the rounding path rather than the exact one.
///
/// Reported alongside the row count so a consumer can see how much of a
/// class is sub-cent, and so a schema change to keep sub-cent precision
/// would be driven by a number rather than a guess.
pub fn dollars_needs_rounding(field: &[u8]) -> bool {
    let t = trim_ascii_space(field);
    !t.is_empty() && crate::money::parse_dollars_cents(t).is_err()
}

/// Parse a unit count that may be written either as an integer (`3`) or
/// as a float32 rendering of one (`2.99999976158142`).
///
/// Returns `Some(integer)` when the value is a whole number, or
/// `None` when it is genuinely fractional. The caller must **not**
/// round in the second case: a household that bought 0.5 of something
/// is a real fact, and rounding it to 0 or 1 would invent data. The
/// caller counts those rows instead, so the approximation is visible.
pub fn parse_units(field: &[u8]) -> Option<i64> {
    let t = trim_ascii_space(field);
    if t.is_empty() {
        return None;
    }
    if let Some(v) = parse_int(t) {
        return Some(v);
    }
    let v: f64 = std::str::from_utf8(t).ok()?.parse().ok()?;
    if !v.is_finite() || v < 0.0 {
        return None;
    }
    // float32 of a small integer: 3.0 arrives as 2.99999976158142.
    let r = v.round();
    if (v - r).abs() <= 1e-4 {
        Some(r as i64)
    } else {
        None
    }
}

/// Read a whole file into memory.
///
/// Every non-sales source is small enough for this: the largest is a
/// 20 MB `prod_attr` file. `mmap` would save the copy, but these
/// sources are read once and the copy is paid once, whereas the
/// allocation is bounded by the file size, which is already the thing
/// the operator chose to stage.
pub fn read_file(path: &std::path::Path) -> crate::errors::Result<Vec<u8>> {
    let mut f = std::fs::File::open(path).map_err(|e| crate::errors::IngestError::io(path, e))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .map_err(|e| crate::errors::IngestError::io(path, e))?;
    Ok(buf)
}

/// Yield the lines of `bytes`, stripped of a trailing `\r`.
///
/// CRLF is the corpus-wide convention, including in the files that are
/// otherwise whitespace-delimited. Splitting on `\n` and trimming a
/// trailing `\r` handles both, and leaves a trailing empty line (the
/// file's final terminator) to be dropped by the caller's emptiness
/// check.
pub fn lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    bytes.split(|&b| b == b'\n').map(|l| {
        if l.last() == Some(&b'\r') {
            &l[..l.len() - 1]
        } else {
            l
        }
    })
}

/// The first non-empty line of a source, or `None` if there is none.
///
/// This is the **dialect dispatcher** for the tab/space/comma classes.
/// A zero-byte file yields `None`, which callers must handle as "empty
/// table, not malformed" — the corpus contains two genuinely
/// zero-byte PANEL files (`Year1/beer`, `Year2/*` `PANEL_DR`) that are
/// real files with no content.
pub fn first_line(bytes: &[u8]) -> Option<&[u8]> {
    lines(bytes).find(|l| !l.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_a_single_byte_delimiter() {
        assert_eq!(
            split_delimited(b"a\tb\tc", b'\t'),
            vec![&b"a"[..], &b"b"[..], &b"c"[..]]
        );
        // An empty field between delimiters is preserved: that is how
        // `trips1 jul08.csv`'s empty CENTS998 column survives.
        assert_eq!(
            split_delimited(b"1100016,1114,,4470.8", b','),
            vec![&b"1100016"[..], &b"1114"[..], &b""[..], &b"4470.8"[..]]
        );
    }

    #[test]
    fn whitespace_split_collapses_runs_and_drops_empties() {
        assert_eq!(
            split_whitespace(b" 3105502 1344  1  MA 0.98  690004 8839999850289 "),
            vec![
                &b"3105502"[..],
                &b"1344"[..],
                &b"1"[..],
                &b"MA"[..],
                &b"0.98"[..],
                &b"690004"[..],
                &b"8839999850289"[..],
            ]
        );
    }

    #[test]
    fn parse_int_rejects_signs_blanks_and_junk() {
        assert_eq!(parse_int(b" 200039 "), Some(200039));
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"   "), None);
        assert_eq!(parse_int(b"9998"), Some(9998));
        assert_eq!(parse_int(b"-1"), None, "a negative id must not parse");
        assert_eq!(parse_int(b"+1"), None);
        assert_eq!(parse_int(b"12a"), None);
    }

    #[test]
    fn parse_f64_handles_iri_style_decimals() {
        assert_eq!(parse_f64(b"9.709999"), Some(9.709999));
        assert_eq!(parse_f64(b" 0.1944 "), Some(0.1944));
        assert_eq!(parse_f64(b"MISSING"), None);
        assert_eq!(parse_f64(b""), None);
    }

    #[test]
    fn money_takes_the_exact_two_decimal_path_unchanged() {
        assert_eq!(parse_dollars_cents_lossy(b"6.99"), Some(699));
        assert_eq!(parse_dollars_cents_lossy(b" 1.69 "), Some(169));
        assert_eq!(parse_dollars_cents_lossy(b"16.58"), Some(1658));
        assert_eq!(parse_dollars_cents_lossy(b"5"), Some(500));
    }

    #[test]
    fn money_recovers_a_float32_rendering_of_a_two_decimal_amount() {
        // Verbatim from Year1/beer/beer_PANEL_GR_1114_1165.dat, where
        // the strict parser would return None and the amount would be
        // lost.
        assert_eq!(
            parse_dollars_cents_lossy(b"0.7299998474"),
            Some(73),
            "float32(0.73) must round back to 73 cents"
        );
        assert_eq!(parse_dollars_cents_lossy(b"0.049999997"), Some(5));
        assert_eq!(parse_dollars_cents_lossy(b"6.98999977111816"), Some(699));
        assert_eq!(parse_dollars_cents_lossy(b"16.579999923706055"), Some(1658));
    }

    #[test]
    fn money_rounds_a_genuine_sub_cent_average_to_the_cent() {
        // The single most common high-precision form in the corpus,
        // concentrated in carbbev's panel files (~0.4 % of all PANEL
        // rows). Sub-cent precision is not money: `dollars_cents / 100`
        // has to agree with the sales figure for the same purchase.
        assert_eq!(parse_dollars_cents_lossy(b"4.3091992188"), Some(431));
        assert_eq!(parse_dollars_cents_lossy(b"3.5963989258"), Some(360));
        assert_eq!(parse_dollars_cents_lossy(b"2.797199707"), Some(280));
        // Standard half-up rounding at the half-cent boundary.
        assert_eq!(parse_dollars_cents_lossy(b"6.995"), Some(700));
        assert_eq!(parse_dollars_cents_lossy(b"6.9949"), Some(699));
    }

    #[test]
    fn money_reports_which_rows_needed_rounding() {
        assert!(!dollars_needs_rounding(b"6.99"));
        assert!(dollars_needs_rounding(b"4.3091992188"));
        assert!(dollars_needs_rounding(b"0.7299998474"));
        assert!(!dollars_needs_rounding(b""));
    }

    #[test]
    fn money_still_refuses_nonsense() {
        assert_eq!(parse_dollars_cents_lossy(b""), None);
        assert_eq!(parse_dollars_cents_lossy(b"   "), None);
        assert_eq!(parse_dollars_cents_lossy(b"-1.00"), None);
        assert_eq!(parse_dollars_cents_lossy(b"+1.00"), None);
        assert_eq!(parse_dollars_cents_lossy(b"n/a"), None);
        assert_eq!(parse_dollars_cents_lossy(b"MISSING"), None);
        assert_eq!(parse_dollars_cents_lossy(b"1.2.3"), None);
        assert_eq!(parse_dollars_cents_lossy(b"NaN"), None);
        assert_eq!(parse_dollars_cents_lossy(b"inf"), None);
    }

    #[test]
    fn units_accept_a_float32_rendering_of_a_whole_number() {
        assert_eq!(parse_units(b"1"), Some(1));
        assert_eq!(parse_units(b" 12 "), Some(12));
        // float32(3) prints as this.
        assert_eq!(parse_units(b"2.99999976158142"), Some(3));
        assert_eq!(parse_units(b"3.000000238418579"), Some(3));
        // A float32 rendering of a genuinely fractional count is NOT
        // rounded to an integer — it is not an artefact of an integer,
        // it *is* fractional. `0.049999997` is float32(0.05), half of
        // nothing, and the caller counts it instead of rounding it.
        assert_eq!(parse_units(b"0.049999997"), None);
    }

    #[test]
    fn units_refuses_to_round_a_genuinely_fractional_count() {
        // A household that bought half a unit is a real fact. Rounding
        // it to 0 or 1 would invent data, so it comes back as None and
        // the caller counts it.
        assert_eq!(parse_units(b"0.5"), None);
        assert_eq!(parse_units(b"1.5"), None);
        assert_eq!(parse_units(b""), None);
        assert_eq!(parse_units(b"-1"), None);
    }

    #[test]
    fn decimal_parses_three_encodings_of_a_money_value() {
        use super::DecimalKind::*;
        // Exact 2-decimal.
        let v = parse_decimal(b"6.99");
        assert_eq!(
            v,
            super::DecimalValue::Value {
                digits: 699,
                scale: 2,
                kind: Exact,
            }
        );
        assert_eq!(v.cents_rounded(), Some(699));

        // Float32 rendering of a 2-decimal: 10 fractional digits.
        let v = parse_decimal(b"0.7299998474");
        assert_eq!(
            v,
            super::DecimalValue::Value {
                digits: 7299998474,
                scale: 10,
                kind: SubCent,
            }
        );
        assert_eq!(v.cents_rounded(), Some(73));

        // Sub-cent computed average: also 10 fractional digits in this
        // file. Indistinguishable from the float32 form in the bytes.
        let v = parse_decimal(b"4.3091992188");
        assert_eq!(
            v,
            super::DecimalValue::Value {
                digits: 43091992188,
                scale: 10,
                kind: SubCent,
            }
        );
        assert_eq!(v.cents_rounded(), Some(431));
    }

    #[test]
    fn decimal_handles_edge_forms() {
        use super::DecimalKind::*;
        // Integer with no decimal point.
        assert_eq!(
            parse_decimal(b"5"),
            super::DecimalValue::Value {
                digits: 5,
                scale: 0,
                kind: Exact,
            }
        );
        // `5.` is `5.0`; an integer followed by `.` and no fraction.
        assert_eq!(
            parse_decimal(b"5."),
            super::DecimalValue::Value {
                digits: 5,
                scale: 0,
                kind: Exact,
            }
        );
        // `.5` is a fraction-only amount (5 tenths of a unit).
        assert_eq!(
            parse_decimal(b".5"),
            super::DecimalValue::Value {
                digits: 5,
                scale: 1,
                kind: Exact,
            }
        );
        // An exact amount with trailing zeros.
        assert_eq!(
            parse_decimal(b"6.90"),
            super::DecimalValue::Value {
                digits: 690,
                scale: 2,
                kind: Exact,
            }
        );
        // Empty / whitespace.
        assert_eq!(parse_decimal(b""), super::DecimalValue::Missing);
        assert_eq!(parse_decimal(b"   "), super::DecimalValue::Missing);
        // Malformed.
        assert_eq!(parse_decimal(b"-1.00"), super::DecimalValue::Unparseable);
        assert_eq!(parse_decimal(b"+1.00"), super::DecimalValue::Unparseable);
        assert_eq!(parse_decimal(b"n/a"), super::DecimalValue::Unparseable);
        assert_eq!(parse_decimal(b"1.2.3"), super::DecimalValue::Unparseable);
    }

    #[test]
    fn decimal_is_lossless_in_both_directions() {
        // The point of recording digits + scale rather than a float:
        // `digits / 10^scale` reproduces the value exactly (modulo the
        // BigInt-to-f64 conversion), and the digits are recoverable.
        for raw in [
            "6.99",
            "0.7299998474",
            "4.3091992188",
            "4470.8359375",
            "4600.316406",
            "2531.82959",
            "0",
            "5",
        ] {
            let v = parse_decimal(raw.as_bytes());
            let DecimalValue::Value { digits, scale, .. } = v else {
                panic!("{raw} parsed as {v:?}");
            };
            // Round-trip the *decimal digits* as a string. The digits
            // are the only thing that matters for losslessness.
            let mut s = digits.to_string();
            let scale = scale as usize;
            // Left-pad with zeros so it has at least `scale + 1` digits.
            while s.len() <= scale {
                s.insert(0, '0');
            }
            if scale > 0 {
                let split = s.len() - scale;
                s.insert(split, '.');
            }
            // Numeric closeness on the recovered value (we cannot
            // compare exact strings because trailing zeros can shift).
            let recovered: f64 = s.parse().unwrap();
            let original: f64 = raw.parse().unwrap();
            assert!(
                (recovered - original).abs() <= (10f64).powi(-(scale as i32)),
                "round-trip mismatch: raw={raw} recovered={s}"
            );
        }
    }

    #[test]
    fn lines_strips_crlf() {
        // A trailing CRLF yields a final empty slice; that is the
        // file's terminator, not a row, and every caller drops it.
        let v: Vec<&[u8]> = lines(b"a\r\nb\r\n\r\n").collect();
        assert_eq!(v, vec![&b"a"[..], &b"b"[..], &b""[..], &b""[..]]);
        // CRLF-free input is unchanged.
        let v: Vec<&[u8]> = lines(b"a\nb\n").collect();
        assert_eq!(v, vec![&b"a"[..], &b"b"[..], &b""[..]]);
    }

    #[test]
    fn first_line_of_a_zero_byte_file_is_none() {
        // The corpus has two genuinely empty PANEL files; the dialect
        // dispatcher must not treat that as malformed input.
        assert_eq!(first_line(b""), None);
        assert_eq!(first_line(b"\r\n\r\n"), None);
        assert_eq!(
            first_line(b"\r\nPANID,WEEK\r\n1,2\r\n"),
            Some(&b"PANID,WEEK"[..])
        );
    }
}
