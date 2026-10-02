//! Exact integer-cent parsing of the `DOLLARS` field.
//!
//! The IRI sales data stores dollar amounts as ASCII text with a
//! possibly-implicit decimal point:
//!
//! - `9.29`        → 929 cents
//! - ` 9.29`       → 929 cents (left-padded with spaces)
//! - `9.2`         → 920 cents (single fractional digit is treated as tenths)
//! - `9`           → 900 cents (no decimal point = integer dollars)
//! - ` 9`          → 900 cents
//! - `   9.29  `   → 929 cents (right-padded with spaces)
//!
//! What is rejected:
//! - Empty fields
//! - Non-ASCII bytes
//! - More than one decimal point
//! - More than two fractional digits (`9.291`)
//! - A leading `-` or `+` sign (sales values are always ≥ 0)
//! - Anything that is not a finite ASCII decimal
//!
//! Downstream views may expose `dollars_cents / 100.0` as a decimal-like
//! amount; storage never stores a binary float.

use crate::errors::MoneyErrorReason;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoneyError {
    Empty,
    NotANumber,
    TooManyFractionalDigits,
    MultipleDecimalPoints,
    UnexpectedSign,
    NonAscii,
}

impl MoneyError {
    pub fn reason(self) -> MoneyErrorReason {
        match self {
            MoneyError::Empty => MoneyErrorReason::Empty,
            MoneyError::NotANumber => MoneyErrorReason::NotANumber,
            MoneyError::TooManyFractionalDigits => MoneyErrorReason::TooManyFractionalDigits,
            MoneyError::MultipleDecimalPoints => MoneyErrorReason::MultipleDecimalPoints,
            MoneyError::UnexpectedSign => MoneyErrorReason::UnexpectedSign,
            MoneyError::NonAscii => MoneyErrorReason::NonAscii,
        }
    }
}

/// Parse a (possibly padded) ASCII dollar amount into integer cents.
///
/// The input slice is exactly the on-disk field bytes (typically 8 bytes).
/// Whitespace (spaces) inside the slice is ignored.
///
/// Returns `Result<i64, MoneyError>` because the IRI DOLLARS field is 8
/// bytes wide and a parser that accepts "no decimal point" amounts (e.g.
/// `parse_dollars_cents(b"9")`) implies a worst-case width-bound of
/// `$99 999 999 = 9 999 999 900` cents, which exceeds `i32::MAX`. We
/// keep the result in `i64` to match the storage type in the canonical
/// schema (`dollars_cents: Int64`). Empirical observation of the corpus
/// would let us tighten to `Int32` (realistic max cents is well under
/// 10⁶); see `ARCHITECTURE.md` for the analysis.
pub fn parse_dollars_cents(field: &[u8]) -> Result<i64, MoneyError> {
    let mut had_dot = false;
    let mut int_part: i64 = 0;
    let mut frac_part: i64 = 0;
    let mut frac_digits: usize = 0;
    let mut any_digit = false;

    for &b in field {
        match b {
            b' ' => continue,
            b'-' | b'+' => return Err(MoneyError::UnexpectedSign),
            b'.' => {
                if had_dot {
                    return Err(MoneyError::MultipleDecimalPoints);
                }
                had_dot = true;
            }
            b'0'..=b'9' => {
                any_digit = true;
                let digit = (b - b'0') as i64;
                if !had_dot {
                    int_part = int_part
                        .checked_mul(10)
                        .and_then(|x| x.checked_add(digit))
                        .ok_or(MoneyError::NotANumber)?;
                } else {
                    if frac_digits >= 2 {
                        return Err(MoneyError::TooManyFractionalDigits);
                    }
                    frac_part = frac_part
                        .checked_mul(10)
                        .and_then(|x| x.checked_add(digit))
                        .ok_or(MoneyError::NotANumber)?;
                    frac_digits += 1;
                }
            }
            // anything else (control bytes, letters, etc.) is not a money char.
            _ => return Err(MoneyError::NonAscii),
        }
    }

    if !any_digit {
        return Err(MoneyError::Empty);
    }

    // Pad fractional part: "9.2" → 920, not 92.
    if frac_digits == 0 {
        frac_part = 0;
    } else if frac_digits == 1 {
        frac_part *= 10;
    }

    let cents = int_part
        .checked_mul(100)
        .and_then(|x| x.checked_add(frac_part))
        .ok_or(MoneyError::NotANumber)?;
    Ok(cents)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_whole_dollars() {
        assert_eq!(parse_dollars_cents(b" 9 ").unwrap(), 900);
        assert_eq!(parse_dollars_cents(b"9").unwrap(), 900);
        assert_eq!(parse_dollars_cents(b"  123").unwrap(), 12300);
        assert_eq!(parse_dollars_cents(b"0").unwrap(), 0);
    }

    #[test]
    fn parses_two_digit_fraction() {
        assert_eq!(parse_dollars_cents(b"9.29").unwrap(), 929);
        assert_eq!(parse_dollars_cents(b"  9.29").unwrap(), 929);
        assert_eq!(parse_dollars_cents(b"  9.29  ").unwrap(), 929);
        assert_eq!(parse_dollars_cents(b"1234.56").unwrap(), 123456);
    }

    #[test]
    fn pads_single_digit_fraction() {
        // "9.2" is interpreted as 9 dollars 20 cents.
        assert_eq!(parse_dollars_cents(b"9.2").unwrap(), 920);
        assert_eq!(parse_dollars_cents(b"  9.2").unwrap(), 920);
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(
            parse_dollars_cents(b"        "),
            Err(MoneyError::Empty)
        ));
        assert!(matches!(parse_dollars_cents(b""), Err(MoneyError::Empty)));
    }

    #[test]
    fn rejects_non_ascii() {
        assert!(matches!(
            parse_dollars_cents(b"9.2a"),
            Err(MoneyError::NonAscii)
        ));
        // Non-ASCII byte
        assert!(matches!(
            parse_dollars_cents(&[b'1', b'.', 0xff, b'2']),
            Err(MoneyError::NonAscii)
        ));
    }

    #[test]
    fn rejects_too_many_fractional_digits() {
        assert!(matches!(
            parse_dollars_cents(b"9.291"),
            Err(MoneyError::TooManyFractionalDigits)
        ));
    }

    #[test]
    fn rejects_multiple_dots() {
        assert!(matches!(
            parse_dollars_cents(b"9.2.9"),
            Err(MoneyError::MultipleDecimalPoints)
        ));
    }

    #[test]
    fn rejects_sign() {
        assert!(matches!(
            parse_dollars_cents(b"-9.29"),
            Err(MoneyError::UnexpectedSign)
        ));
        assert!(matches!(
            parse_dollars_cents(b"+9.29"),
            Err(MoneyError::UnexpectedSign)
        ));
    }

    #[test]
    fn rejects_letters_and_punctuation() {
        assert!(matches!(
            parse_dollars_cents(b"9,29"),
            Err(MoneyError::NonAscii)
        ));
    }
}
