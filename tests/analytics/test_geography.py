import unittest

import duckdb

from scripts.silver import geography, stores
from scripts.silver.keys import register_functions

class GeographyViewsTest(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        register_functions(self.con)
        self.con.execute("""
          CREATE TABLE bronze_delivery_stores(
            source_year UTINYINT, iri_key UINTEGER, ou VARCHAR, est_acv DOUBLE,
            market_name VARCHAR, open_week USMALLINT, closed_week USMALLINT,
            masked_name VARCHAR, _source_file VARCHAR, _source_row UBIGINT)
        """)
        self.con.executemany("INSERT INTO bronze_delivery_stores VALUES (?,?,?,?,?,?,?,?,?,?)", [
            (1, 1, 'GR', 1.0, 'ALPHA', 1114, None, 'Chain7', 'r.parquet', 0),
            (1, 2, 'GR', 1.0, 'DUPLICATE', 1114, None, 'Chain8', 'r.parquet', 1),
            (1, 3, 'GR', 1.0, 'NOWHERE', 1114, None, 'chain?', 'r.parquet', 2),
            (1, 4, 'GR', 1.0, 'ALPHA', 1114, None, 'Chain7', 'r.parquet', 3),
            (1, 4, 'GR', 1.0, 'DUPLICATE', 1114, None, 'Chain7', 'r.parquet', 4),
        ])
        self.con.execute("""
          CREATE TABLE bronze_chain_xref(chain UINTEGER, year1 USMALLINT, year2 USMALLINT,
                                 _source_file VARCHAR, _source_row UBIGINT)
        """)
        self.con.executemany("INSERT INTO bronze_chain_xref VALUES (?,?,?,?,?)", [
            (70, 7, 9, 'chain.csv', 0), (71, 8, 10, 'chain.csv', 1),
            (72, 7, 11, 'chain.csv', 2)])
        self.con.execute("""
          CREATE TABLE bronze_fips_market(
            fips_key UINTEGER, name VARCHAR, county VARCHAR, state VARCHAR,
            market_name VARCHAR, region_name VARCHAR, iri_market_number USMALLINT, market_name2 VARCHAR)
        """)
        self.con.executemany("INSERT INTO bronze_fips_market VALUES (?,?,?,?,?,?,?,?)", [
            (1, 'x', 'c1', 's', 'ALPHA', 'East', 0, 'A-ALT'),
            (2, 'y', 'c2', 's', 'ALPHA', 'West', 0, None),
            (3, 'z', 'c3', 's', 'OTHER', 'East', 2, 'DUPLICATE'),
            (4, 'w', 'c4', 's', 'DUPLICATE', 'East', 3, None),
        ])
        stores.register(self.con)
        geography.register(self.con)

    def tearDown(self):
        self.con.close()

    def test_chain_unpivot_and_annual_nullable_assignments(self):
        self.assertEqual(self.con.execute("select count(*) from silver_chain_year_xref_v").fetchone()[0], 5)
        rows = self.con.execute("SELECT iri_key, chain_id, mapping_status FROM silver_store_chain_v JOIN silver_store_v USING(store_id) ORDER BY iri_key").fetchall()
        self.assertEqual(rows, [(1, None, 'ambiguous'), (2, 71, 'known'), (3, None, 'unmatched'), (4, None, 'ambiguous')])
        self.assertEqual(self.con.execute("select mapping_status from silver_chain_year_xref_v where source_year=1 and masked_chain_number=7").fetchone()[0], 'ambiguous')
        self.assertEqual(self.con.execute("select count(*) from silver_store_chain_v").fetchone()[0], 4)

    def test_market_zero_regions_and_ambiguous_exact_name(self):
        self.assertIn((0,), self.con.execute("SELECT iri_market_number FROM silver_market_v ORDER BY 1").fetchall())
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_market_region_v WHERE iri_market_number=0").fetchone()[0], 2)
        self.assertEqual(self.con.execute("SELECT mapping_status FROM silver_market_name_xref_v WHERE source_label='DUPLICATE'").fetchone()[0], 'ambiguous')
        assignments = self.con.execute("SELECT iri_key, iri_market_number, mapping_status FROM silver_store_market_v JOIN silver_store_v USING(store_id) ORDER BY iri_key").fetchall()
        self.assertEqual(assignments, [(1, 0, 'known'), (2, None, 'ambiguous'), (3, None, 'unmatched'), (4, None, 'conflict')])

    def test_market_region_is_bridge_and_checks_pass(self):
        checks = geography.validate(self.con)
        self.assertTrue(checks, 'geographic fixture should produce validation checks')
        self.assertTrue(all(c['ok'] for c in checks), checks)
        self.assertEqual(self.con.execute("select fips_code from silver_fips_market_v where fips_key=1").fetchone()[0], '00001')


if __name__ == '__main__':
    unittest.main()
