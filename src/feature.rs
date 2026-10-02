//! Promotion-feature coding for the `F` field.
//!
//! The IRI spec allows the values `NONE`, `A`, `A+`, `B`, `C`.
//! Real files have not been observed with other tokens, but we fail
//! loudly (rather than silently dropping or remapping) because losing
//! a feature flag silently is a data-quality bug.
//!
//! The encoding is stable and small:
//!
//! | token | code |
//! |-------|-----:|
//! | (empty/all spaces) | 0 |
//! | `NONE` | 0 |
//! | `A`    | 1 |
//! | `A+`   | 2 |
//! | `B`    | 3 |
//! | `C`    | 4 |

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureCode {
    None = 0,
    A = 1,
    APlus = 2,
    B = 3,
    C = 4,
}

impl FeatureCode {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureError(pub String);

impl std::fmt::Display for FeatureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown promotion feature token {:?}", self.0)
    }
}

impl std::error::Error for FeatureError {}

/// Parse the (trimmed) feature token. Returns the typed enum.
///
/// Trim semantics: leading and trailing spaces are dropped before matching;
/// `   A   ` parses the same as `A`. A field that is *all* spaces is
/// treated as `NONE` (code 0).
pub fn parse_feature(field: &[u8]) -> Result<FeatureCode, FeatureError> {
    // Trim spaces — ASCII byte 0x20 only; non-ASCII bytes fall through.
    let start = match field.iter().position(|&b| b != b' ') {
        Some(s) => s,
        None => return Ok(FeatureCode::None), // all spaces → NONE
    };
    let end = field
        .iter()
        .rposition(|&b| b != b' ')
        .map(|p| p + 1)
        .unwrap_or(start);
    let trimmed = &field[start..end];

    let s = std::str::from_utf8(trimmed).map_err(|_| FeatureError(format!("{:02x?}", field)))?;

    match s {
        "" | "NONE" => Ok(FeatureCode::None),
        "A" => Ok(FeatureCode::A),
        "A+" => Ok(FeatureCode::APlus),
        "B" => Ok(FeatureCode::B),
        "C" => Ok(FeatureCode::C),
        other => Err(FeatureError(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_spaces_is_none() {
        assert_eq!(parse_feature(b"    ").unwrap(), FeatureCode::None);
        assert_eq!(parse_feature(b"").unwrap(), FeatureCode::None);
    }

    #[test]
    fn parses_known_codes() {
        assert_eq!(parse_feature(b"NONE").unwrap(), FeatureCode::None);
        assert_eq!(parse_feature(b"A").unwrap(), FeatureCode::A);
        assert_eq!(parse_feature(b"A+").unwrap(), FeatureCode::APlus);
        assert_eq!(parse_feature(b"B").unwrap(), FeatureCode::B);
        assert_eq!(parse_feature(b"C").unwrap(), FeatureCode::C);
    }

    #[test]
    fn trims_padding() {
        assert_eq!(parse_feature(b"  A  ").unwrap(), FeatureCode::A);
        assert_eq!(parse_feature(b" A+  ").unwrap(), FeatureCode::APlus);
        assert_eq!(parse_feature(b"  NONE ").unwrap(), FeatureCode::None);
    }

    #[test]
    fn rejects_unknown() {
        assert!(parse_feature(b"D").is_err());
        assert!(parse_feature(b"X").is_err());
        assert!(parse_feature(b"a").is_err()); // case-sensitive
    }

    #[test]
    fn codes_match_documented_values() {
        assert_eq!(FeatureCode::None.as_u8(), 0);
        assert_eq!(FeatureCode::A.as_u8(), 1);
        assert_eq!(FeatureCode::APlus.as_u8(), 2);
        assert_eq!(FeatureCode::B.as_u8(), 3);
        assert_eq!(FeatureCode::C.as_u8(), 4);
    }
}
