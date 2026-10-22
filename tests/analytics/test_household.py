"""Synthetic-only tests for the explicitly invoked restricted household domain."""
import hashlib
import json
import unittest

import duckdb

from scripts.silver.household import register, validate
from scripts.silver.keys import register_functions


def stable_id(payload):
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class HouseholdRegistrationTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        register_functions(self.con)
        self.con.execute("""
            CREATE TEMP VIEW bronze_panel AS SELECT * FROM (VALUES
              (7::UINTEGER, 2::UTINYINT, 44::USMALLINT, NULL::USMALLINT, 1::VARCHAR,
               9::UINTEGER, '00012345678'::VARCHAR, NULL::INTEGER, 12345678901234567890::DECIMAL(38,0),
               7::TINYINT, NULL::BIGINT, 987654321012345678901234::DECIMAL(38,0), 12::TINYINT,
               'panel.parquet'::VARCHAR, 0::UBIGINT),
              (7::UINTEGER, 2::UTINYINT, 44::USMALLINT, NULL::USMALLINT, 1::VARCHAR,
               9::UINTEGER, '00012345678'::VARCHAR, NULL::INTEGER, 12345678901234567890::DECIMAL(38,0),
               7::TINYINT, NULL::BIGINT, 987654321012345678901234::DECIMAL(38,0), 12::TINYINT,
               'panel.parquet'::VARCHAR, 1::UBIGINT),
              (NULL::UINTEGER, 2::UTINYINT, 44::USMALLINT, 0::USMALLINT, NULL::VARCHAR,
               NULL::UINTEGER, NULL::VARCHAR, NULL::INTEGER, NULL::DECIMAL(38,0), NULL::TINYINT,
               NULL::BIGINT, NULL::DECIMAL(38,0), NULL::TINYINT, 'panel.parquet'::VARCHAR, 2::UBIGINT)
            ) AS t(panid, source_year, week, minute, outlet, iri_key, colupc, units,
                   units_digits, units_scale, dollars_cents, dollars_digits, dollars_scale,
                   _source_file, _source_row)
        """)
        self.con.execute("""
            CREATE TEMP VIEW bronze_panel_trips AS SELECT * FROM (VALUES
              (7::UINTEGER, 2::UTINYINT, 44::USMALLINT, 9::UINTEGER, NULL::UINTEGER, NULL::USMALLINT,
               123456789012345678901::DECIMAL(38,0), 7::TINYINT,
               NULL::DECIMAL(38,0), NULL::TINYINT, 99999999999999999999::DECIMAL(38,0), 8::TINYINT,
               'trips.parquet'::VARCHAR, 0::UBIGINT),
              (7::UINTEGER, 2::UTINYINT, 44::USMALLINT, 9::UINTEGER, NULL::UINTEGER, NULL::USMALLINT,
               123456789012345678901::DECIMAL(38,0), 7::TINYINT,
               NULL::DECIMAL(38,0), NULL::TINYINT, 99999999999999999999::DECIMAL(38,0), 8::TINYINT,
               'trips.parquet'::VARCHAR, 1::UBIGINT)
            ) AS t(panid, source_year, week, iri_key, iri_key2, minute, cents998_digits, cents998_scale,
                   cents999_digits, cents999_scale, kryscents_digits, kryscents_scale, _source_file, _source_row)
        """)
        self.con.execute("""
            CREATE TEMP VIEW bronze_panelist_demos AS SELECT * FROM (VALUES
              (7::UINTEGER, 2::UTINYINT, '05'::VARCHAR, NULL::VARCHAR, 'demos.parquet'::VARCHAR, 0::UBIGINT),
              (7::UINTEGER, 2::UTINYINT, '06'::VARCHAR, NULL::VARCHAR, 'demos.parquet'::VARCHAR, 1::UBIGINT),
              (7::UINTEGER, 3::UTINYINT, '05'::VARCHAR, NULL::VARCHAR, 'demos.parquet'::VARCHAR, 2::UBIGINT)
            ) AS t(panid, source_year, income_code, age_code, _source_file, _source_row)
        """)
        self.con.execute("""
            CREATE TEMP VIEW bronze_panel_static AS SELECT * FROM (VALUES
              (7::UINTEGER, 2::UTINYINT, 15::UINTEGER, 'Y'::VARCHAR, 'static.parquet'::VARCHAR, 0::UBIGINT)
            ) AS t(panid, source_year, trip_count, make_static, _source_file, _source_row)
        """)

    def tearDown(self):
        self.con.close()

    def test_registration_without_household_inputs_creates_no_domain_objects(self):
        con = duckdb.connect()
        register_functions(con)
        self.assertEqual(register(con), {})
        self.assertEqual(con.execute("SELECT count(*) FROM information_schema.views WHERE table_name LIKE 'silver_%'").fetchone()[0], 0)
        con.close()

    def test_records_preserve_multiplicity_missing_keys_and_exact_pairs(self):
        metadata = register(self.con)
        rows = self.con.execute("""
            SELECT purchase_record_id, panid, minute_raw, units_digits, units_scale,
                   dollars_digits, dollars_scale, colupc_raw, upc, product_mapping_status
            FROM silver_panel_purchase_v ORDER BY source_row
        """).fetchall()
        self.assertEqual(len(rows), 3)
        self.assertNotEqual(rows[0][0], rows[1][0])
        self.assertEqual(rows[0][3:7], (12345678901234567890, 7, 987654321012345678901234, 12))
        self.assertIsNone(rows[0][2])  # missing minute remains distinct from zero
        self.assertEqual(rows[0][8], None)  # COLUPC correspondence is unresolved
        self.assertEqual(rows[0][9], "unresolved")
        self.assertIsNone(rows[2][1])
        self.assertIsNone(self.con.execute("SELECT household_id FROM silver_panel_purchase_v WHERE source_row=2").fetchone()[0])
        self.assertTrue(all(v["restricted"] for v in metadata.values()))
        self.assertTrue(all(c["ok"] for c in validate(self.con)))

    def test_trip_pairs_and_linkage_remain_exact_and_ambiguous_unresolved(self):
        register(self.con)
        trip = self.con.execute("""
            SELECT cents998_digits, cents998_scale, cents999_digits, cents999_scale,
                   kryscents_digits, kryscents_scale, minute_raw
            FROM silver_panel_trip_record_v ORDER BY source_row LIMIT 1
        """).fetchone()
        self.assertEqual(trip, (123456789012345678901, 7, None, None, 99999999999999999999, 8, None))
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_purchase_trip_link_v WHERE link_status='unresolved' AND trip_record_id IS NULL").fetchone()[0], 3)
        # Two trip records share all currently available candidate fields; neither is assigned.
        candidates = self.con.execute("SELECT count(*) FROM silver_panel_trip_record_v WHERE panid=7 AND source_year=2 AND week=44 AND iri_key=9 AND minute_raw IS NULL").fetchone()[0]
        self.assertEqual(candidates, 2)
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_panel_trip_record_v").fetchone()[0], 2)

    def test_annual_demo_conflicts_do_not_choose_a_value_or_decode_codes(self):
        register(self.con)
        conflicts = self.con.execute("""
            SELECT value, status, evidence_count FROM silver_household_attribute_v
            WHERE panid=7 AND source_year=2 AND attribute_name='income_code'
        """).fetchone()
        self.assertEqual(conflicts, (None, "conflict", 2))
        known_year = self.con.execute("""
            SELECT value, status FROM silver_household_attribute_v
            WHERE panid=7 AND source_year=3 AND attribute_name='income_code'
        """).fetchone()
        self.assertEqual(known_year, ("05", "known"))
        years = self.con.execute("SELECT source_year, has_purchase, has_demo, has_static FROM silver_household_year_v WHERE panid=7 ORDER BY source_year").fetchall()
        self.assertEqual(years, [(2, True, True, True), (3, False, True, False)])

    def test_demographic_observations_and_ad_population_are_separate(self):
        self.con.execute("CREATE TEMP VIEW bronze_ads_demos AS SELECT 7::UINTEGER panid, 2::UTINYINT source_year, 'ads.parquet'::VARCHAR _source_file, 0::UBIGINT _source_row, 'x'::VARCHAR ad_code")
        metadata = register(self.con)
        self.assertIn("silver_ads_demographic_observation_v", metadata)
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_ads_demographic_observation_v").fetchone()[0], 1)
        self.assertEqual(self.con.execute("SELECT panid FROM silver_ads_demographic_observation_v").fetchone()[0], 7)
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_household_demographic_observation_v").fetchone()[0], 3)
        self.assertTrue(all(item["ok"] for item in validate(self.con)))

    def test_actual_panelist_id_year_header_collapses_payload_and_keeps_lineage(self):
        self.con.execute("DROP VIEW bronze_panelist_demos")
        self.con.execute('''CREATE TEMP VIEW bronze_panelist_demos AS SELECT * FROM (VALUES
            (21::UINTEGER, '03'::VARCHAR, '05'::VARCHAR, '06'::VARCHAR, 'demo-a'::VARCHAR, 0::UBIGINT),
            (21::UINTEGER, '03'::VARCHAR, '05'::VARCHAR, '06'::VARCHAR, 'demo-b'::VARCHAR, 0::UBIGINT),
            (21::UINTEGER, '03'::VARCHAR, '07'::VARCHAR, '06'::VARCHAR, 'demo-c'::VARCHAR, 0::UBIGINT)
        ) t("Panelist ID", "Year", income_code, age_code, _source_file, _source_row)''')
        register(self.con)
        obs = self.con.execute("SELECT panid, source_year, source_record_count FROM silver_household_demographic_observation_v ORDER BY source_record_count DESC").fetchall()
        self.assertEqual(obs, [(21, 3, 2), (21, 3, 1)])
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_household_demographic_observation_v_lineage_v").fetchone()[0], 3)
        ids = self.con.execute("SELECT household_year_id FROM silver_household_year_v WHERE panid=21 AND source_year=3").fetchone()[0]
        purchase_id = self.con.execute("SELECT silver_id(json_array('household-year:v1', 3, 21)::VARCHAR)").fetchone()[0]
        self.assertEqual(ids, purchase_id)


if __name__ == "__main__":
    unittest.main()
