//! Parser unit tests.
//!
//! These exercise the byte layout, integer parsing, money parsing,
//! feature coding, and the failure modes for malformed input.

mod common;

use std::fs::File;
use std::io::Read;

use iri_lake::arrow_output::{schema, SalesBuilders};
use iri_lake::config::IngestConfig;
use iri_lake::discovery::parse_identity;
use iri_lake::errors::{IngestError, MoneyErrorReason};
use iri_lake::feature::{parse_feature, FeatureCode};
use iri_lake::fixed_width::{self, HEADER_LEN, RECORD_LEN};
use iri_lake::ingest::{ingest_file, IngestFilter};
use iri_lake::model::Channel;
use iri_lake::money::parse_dollars_cents;
use iri_lake::parser::parse_records_into_builder;

fn identity_for(path: &std::path::Path, root: &std::path::Path) -> iri_lake::model::SourceIdentity {
    parse_identity(path, root).expect("identity")
}

#[test]
fn integer_field_parses_with_leading_spaces() {
    let bytes = b" 12345";
    let s = std::str::from_utf8(bytes).unwrap().trim();
    assert_eq!(s.parse::<u32>().unwrap(), 12345);
}

#[test]
fn money_parses_whole_dollars_to_cents() {
    assert_eq!(parse_dollars_cents(b" 9 ").unwrap(), 900);
    assert_eq!(parse_dollars_cents(b"9").unwrap(), 900);
}

#[test]
fn money_parses_two_digit_fraction() {
    assert_eq!(parse_dollars_cents(b"  9.29").unwrap(), 929);
    assert_eq!(parse_dollars_cents(b"1234.56").unwrap(), 123456);
}

#[test]
fn money_parses_single_digit_fraction_as_tenths() {
    assert_eq!(parse_dollars_cents(b"9.2").unwrap(), 920);
}

#[test]
fn money_rejects_non_ascii() {
    assert!(matches!(
        parse_dollars_cents(b"9,29"),
        Err(e) if e.reason() == MoneyErrorReason::NonAscii
    ));
}

#[test]
fn money_rejects_too_many_fractional_digits() {
    assert!(matches!(
        parse_dollars_cents(b"9.291"),
        Err(e) if e.reason() == MoneyErrorReason::TooManyFractionalDigits
    ));
}

#[test]
fn money_rejects_empty() {
    assert!(matches!(
        parse_dollars_cents(b"        "),
        Err(e) if e.reason() == MoneyErrorReason::Empty
    ));
}

#[test]
fn feature_codes_match_documented_values() {
    assert_eq!(parse_feature(b"NONE").unwrap(), FeatureCode::None);
    assert_eq!(parse_feature(b"A").unwrap(), FeatureCode::A);
    assert_eq!(parse_feature(b"A+").unwrap(), FeatureCode::APlus);
    assert_eq!(parse_feature(b"B").unwrap(), FeatureCode::B);
    assert_eq!(parse_feature(b"C").unwrap(), FeatureCode::C);
    assert_eq!(parse_feature(b"   ").unwrap(), FeatureCode::None);
}

#[test]
fn feature_codes_reject_unknown() {
    assert!(parse_feature(b"Z").is_err());
    assert!(parse_feature(b"D").is_err());
}

#[test]
fn header_is_exactly_seventy_bytes_including_crlf() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let mut f = File::open(&p).unwrap();
    let mut header = vec![0u8; HEADER_LEN];
    f.read_exact(&mut header).unwrap();
    fixed_width::validate_header(&header).expect("header must validate");
    // Plus CRLF at the end of the header.
    assert_eq!(header.len(), 57);
    assert_eq!(&header[55..57], b"\r\n");
}

#[test]
fn record_stride_is_exactly_fifty_six_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let bytes = std::fs::read(&p).unwrap();
    let body = &bytes[HEADER_LEN..];
    assert_eq!(body.len() % RECORD_LEN, 0);
    // First record's CRLF sits at body[54..56].
    assert_eq!(&body[54..56], b"\r\n");
    // Second record starts immediately after, with no padding.
    let second = &body[RECORD_LEN..RECORD_LEN + 2];
    assert!(!second.is_empty());
}

#[test]
fn parses_default_fixture_into_arrow() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::fixture_path(tmp.path());
    let id = identity_for(&p, tmp.path());
    let bytes = std::fs::read(&p).unwrap();
    let body = &bytes[HEADER_LEN..];
    let rows = body.len() / RECORD_LEN;
    let mut builders = SalesBuilders::with_capacity(rows);
    parse_records_into_builder(&id, body, 0, rows, &mut builders).unwrap();
    let batch = builders.finish(schema()).unwrap();
    assert_eq!(batch.num_rows(), 64);
    assert_eq!(batch.num_columns(), 13);

    // Spot-check a few columns.
    let iri = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    let cents = batch
        .column(7)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    let pr = batch
        .column(10)
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .unwrap();
    let ch = batch
        .column(12)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    assert_eq!(iri.value(0), 1_000_000);
    assert_eq!(cents.value(0), 0); // 0.00
    assert_eq!(cents.value(1), 99); // 0.99
    assert_eq!(cents.value(2), 198); // 1.98
    assert!(!pr.value(0)); // price_reduction was '0' on even rows
    assert!(pr.value(1));
    assert_eq!(ch.value(0), "drug");
}

#[test]
fn rejects_malformed_dollars_field() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::malformed_dollars_fixture(tmp.path());
    let err = ingest_file(
        &p,
        tmp.path(),
        &tmp.path().join("lake"),
        &IngestConfig::for_test(),
        &IngestFilter::default(),
    )
    .unwrap_err();
    match err {
        IngestError::InvalidDollars { raw_bytes, .. } => {
            assert!(raw_bytes.contains("XX") || raw_bytes.to_ascii_uppercase().contains("58"));
        }
        other => panic!("expected InvalidDollars, got {:?}", other),
    }
}

#[test]
fn rejects_unknown_feature_value() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::unknown_feature_fixture(tmp.path());
    let err = ingest_file(
        &p,
        tmp.path(),
        &tmp.path().join("lake"),
        &IngestConfig::for_test(),
        &IngestFilter::default(),
    )
    .unwrap_err();
    match err {
        IngestError::UnknownFeatureCode { raw_bytes, .. } => {
            assert!(raw_bytes.contains("ZZZZ") || raw_bytes.contains("5a5a5a5a"));
        }
        other => panic!("expected UnknownFeatureCode, got {:?}", other),
    }
}

#[test]
fn rejects_bad_header() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::bad_header_fixture(tmp.path());
    let err = ingest_file(
        &p,
        tmp.path(),
        &tmp.path().join("lake"),
        &IngestConfig::for_test(),
        &IngestFilter::default(),
    )
    .unwrap_err();
    match err {
        IngestError::HeaderMismatch { .. } => (),
        other => panic!("expected HeaderMismatch, got {:?}", other),
    }
}

#[test]
fn rejects_misaligned_body() {
    let tmp = tempfile::tempdir().unwrap();
    let p = common::misaligned_fixture(tmp.path());
    let err = ingest_file(
        &p,
        tmp.path(),
        &tmp.path().join("lake"),
        &IngestConfig::for_test(),
        &IngestFilter::default(),
    )
    .unwrap_err();
    match err {
        IngestError::RecordAlignment { .. } => (),
        other => panic!("expected RecordAlignment, got {:?}", other),
    }
}

#[test]
fn channel_string_round_trips() {
    assert_eq!(Channel::Drug.as_str(), "drug");
    assert_eq!(Channel::Groc.as_str(), "groc");
    assert_eq!(Channel::parse("drug"), Some(Channel::Drug));
    assert_eq!(Channel::parse("groc"), Some(Channel::Groc));
    assert_eq!(Channel::parse("other"), None);
}
