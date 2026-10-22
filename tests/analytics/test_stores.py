import unittest

import duckdb

from scripts.silver import codes, stores
from scripts.silver.keys import register_functions

class StoreViewsTest(unittest.TestCase):
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
            (1, 44, 'GR', 10.0, 'ALPHA', 1114, None, 'Chain7', 'a.parquet', 0),
            (1, 44, 'GR', 10.0, 'ALPHA', 1114, None, 'Chain7', 'b.parquet', 0),
            (1, 44, 'GR', 12.0, 'ALPHA', 1114, None, 'Chain7', 'b.parquet', 1),
            (2, 44, None, 10.0, 'ALPHA', 1114, None, 'Chain7', 'c.parquet', 0),
            (1, 45, 'DR', None, None, None, None, None, 'a.parquet', 1),
        ])
        stores.register(self.con)

    def tearDown(self):
        self.con.close()

    def test_annual_identity_observations_and_per_field_consensus(self):
        self.assertEqual(self.con.execute("select count(*) from silver_store_v").fetchone()[0], 3)
        row = self.con.execute("""
          SELECT observation_count, source_record_count, channel_code, est_acv
          FROM silver_store_v WHERE source_year=1 AND iri_key=44
        """).fetchone()
        self.assertEqual(row, (2, 3, 'groc', None))
        status = dict(self.con.execute("""
          SELECT attribute_name, status FROM silver_store_attribute_status_v
          WHERE store_id=(SELECT store_id FROM silver_store_v WHERE source_year=1 AND iri_key=44)
        """).fetchall())
        self.assertEqual(status['est_acv'], 'conflict')
        self.assertEqual(status['ou'], 'known')
        self.assertEqual(self.con.execute("select count(*) from silver_store_observation_lineage_v").fetchone()[0], 5)

    def test_reuse_in_another_year_is_separate_identity(self):
        ids = self.con.execute("SELECT store_id FROM silver_store_v WHERE iri_key=44 ORDER BY source_year").fetchall()
        self.assertEqual(len(set(ids)), 2)

    def test_unknown_roster_code_has_explicit_resolved_and_dictionary_status(self):
        self.con.execute("""INSERT INTO bronze_delivery_stores(source_year,iri_key,ou,_source_file,_source_row)
                            VALUES (1,46,'XX','unknown.parquet',0)""")
        codes.register(self.con)
        self.assertEqual(self.con.execute("""SELECT channel_code,channel_mapping_status,resolution_status
                            FROM silver_store_v WHERE source_year=1 AND iri_key=46""").fetchone(),
                         (None, "unmatched", "partial"))
        self.assertEqual(self.con.execute("""SELECT channel_code,mapping_status FROM silver_channel_code_v
                            WHERE code_system='roster' AND raw_code='XX' AND source_year=1""").fetchone(),
                         (None, "unmatched"))
        self.assertEqual(self.con.execute("""SELECT channel_code,channel_mapping_status FROM silver_store_v
                            WHERE source_year=1 AND iri_key=44""").fetchone(), ("groc", "known"))
        self.assertTrue(all(check["ok"] for check in stores.validate(self.con)))

    def test_validate_conserves_observations_and_annual_keys(self):
        checks = stores.validate(self.con)
        self.assertEqual(len(checks), 5)
        self.assertTrue(all(c['ok'] for c in checks), checks)


if __name__ == '__main__':
    unittest.main()
