import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.silver.product_attributes import register, validate


class ProductAttributeTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.con.execute("""CREATE TABLE bronze_product_stub(
          upc VARCHAR,sy INTEGER,ge INTEGER,vend INTEGER,item INTEGER,edition VARCHAR,
          Color VARCHAR, SIZE VARCHAR,_source_id VARCHAR,_source_file VARCHAR,_source_row UBIGINT)""")
        self.con.execute("""CREATE TABLE bronze_product_attr(
          sy INTEGER,ge INTEGER,vend INTEGER,item INTEGER,source_year INTEGER,category VARCHAR,
          Color VARCHAR,_source_id VARCHAR,_source_file VARCHAR,_source_row UBIGINT)""")
        self.con.execute("""CREATE VIEW silver_product_key_xref_v AS SELECT
          1::UTINYINT sy,2::UTINYINT ge,3::UINTEGER vend,4::UINTEGER item,'0000123'::VARCHAR upc""")
        self.con.execute("""INSERT INTO bronze_product_stub VALUES
          ('0000123',1,2,3,4,'2008-2011',' blue ','MISSING','stub-a','/stub/a.xlsx',1),
          ('0000123',1,2,3,4,'2008-2011',' blue ','MISSING','stub-copy','/stub/copy.xlsx',1)""")
        self.con.execute("""INSERT INTO bronze_product_attr VALUES
          (1,2,3,4,9,'GROC','blue','attr-a','/attr/a.parquet',0),
          (1,2,3,4,9,'GROC',NULL,'attr-b','/attr/b.parquet',0),
          (1,2,3,4,10,'GROC','MISSING','attr-c','/attr/c.parquet',0)""")

    def tearDown(self):
        self.con.close()

    def test_evidence_consensus_lineage_and_missingness(self):
        mapping = register(self.con)
        observations = self.con.execute("""SELECT source_family,source_year,attribute_name,raw_value,value_status
          FROM silver_product_attribute_observation_v ORDER BY 1,2,3,4""").fetchall()
        self.assertIn(('stub', None, 'Color', ' blue ', 'known'), observations)
        self.assertEqual(self.con.execute("SELECT count(*) FROM _silver_product_attribute_annual_v WHERE source_family='stub' AND attribute_name='Color'").fetchone()[0], 4)
        self.assertIn(('product_attr', 9, 'Color', None, 'source_null'), observations)
        self.assertIn(('product_attr', 10, 'Color', 'MISSING', 'source_missing'), observations)
        self.assertIn('silver_product_attribute_lineage_v', mapping)
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_product_attribute_v WHERE source_year=9 AND status='partial' AND value IS NULL").fetchone()[0], 1)
        self.assertEqual(self.con.execute("SELECT sum(source_record_count) FROM silver_product_attribute_lineage_v WHERE source_family='stub' AND attribute_name='Color'").fetchone()[0], 2)
        self.assertTrue(all(x['ok'] for x in validate(self.con)))

    def test_conflicting_annual_values_resolve_null(self):
        self.con.execute("INSERT INTO bronze_product_attr VALUES (1,2,3,4,9,'GROC','green','attr-d','/attr/d.parquet',0)")
        register(self.con)
        result = self.con.execute("SELECT value,status FROM silver_product_attribute_v WHERE source_year=9 AND attribute_name='Color'").fetchone()
        self.assertEqual(result, (None, 'conflict'))

    def test_file_schema_catalog_distinguishes_not_collected(self):
        self.con.execute("CREATE TABLE silver_input_column_v(_source_id VARCHAR,column_name VARCHAR,data_type VARCHAR,column_ordinal INTEGER)")
        self.con.execute("INSERT INTO silver_input_column_v VALUES ('stub-a','Color','VARCHAR',1),('attr-a','Color','VARCHAR',1),('attr-b','Color','VARCHAR',1),('attr-c','Color','VARCHAR',1)")
        register(self.con)
        statuses = self.con.execute("SELECT DISTINCT value_status FROM silver_product_attribute_observation_v WHERE source_family='stub' AND attribute_name='SIZE'").fetchall()
        self.assertEqual(statuses, [('not_collected',)])
        annual_gap = self.con.execute("SELECT count(*) FROM silver_product_attribute_v WHERE source_year=11 AND attribute_name='Color'").fetchone()[0]
        self.assertEqual(annual_gap, 1)  # Edition evidence is in scope for year 11.

    def test_reviewed_category_mapping_and_conflict(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'stub_categories.csv'
            path.write_text('edition,source_workbook,category\n2008-2011,a.xlsx,GROC\n', encoding='utf-8')
            register(self.con, reference_root=Path(d))
            self.assertEqual(self.con.execute('SELECT category FROM silver_stub_category_xref_v').fetchone()[0], 'GROC')
            with path.open('a', encoding='utf-8') as f:
                f.write('2008-2011,a.xlsx,DRUG\n')
            with self.assertRaisesRegex(ValueError, 'conflicting reviewed'):
                register(self.con, reference_root=Path(d))

    def test_no_upc_annual_source_uses_full_tuple_crosswalk(self):
        self.con.execute("INSERT INTO bronze_product_attr VALUES (1,2,3,99,9,'GROC','odd','unknown','/attr/unknown.parquet',0)")
        register(self.con)
        rows = self.con.execute("SELECT upc,product_mapping_status FROM silver_product_attribute_observation_v WHERE source_family='product_attr' AND observation_id IN (SELECT observation_id FROM silver_product_attribute_lineage_v WHERE _source_id='unknown')").fetchall()
        self.assertEqual(rows, [(None, 'unmatched_tuple')])
        self.assertEqual(self.con.execute("SELECT count(*) FROM silver_product_attribute_lineage_v WHERE _source_id='unknown'").fetchone()[0], 1)

    def test_validation_detects_actual_duplicate_primary_key_rows(self):
        register(self.con)
        self.con.execute("CREATE TEMP TABLE duplicate_observations AS SELECT * FROM silver_product_attribute_observation_v")
        self.con.execute("CREATE OR REPLACE TEMP VIEW silver_product_attribute_observation_v AS SELECT * FROM duplicate_observations UNION ALL SELECT * FROM duplicate_observations")
        checks = {item['check_id']: item for item in validate(self.con)}
        self.assertFalse(checks['product_attribute_observation_keys']['ok'])

    def test_raw_observation_key_collapses_duplicate_rows_and_edition_years(self):
        self.con.execute("INSERT INTO bronze_product_stub VALUES ('0000123',1,2,3,4,'2008-2011',' blue ','MISSING','stub-a','/stub/a.xlsx',1)")
        register(self.con)
        row = self.con.execute("SELECT observation_id,source_record_count,source_year FROM silver_product_attribute_observation_v WHERE source_family='stub' AND attribute_name='Color'").fetchone()
        self.assertEqual(row[1:], (3, None))
        self.assertEqual(self.con.execute("SELECT sum(source_record_count) FROM silver_product_attribute_lineage_v WHERE observation_id=?", [row[0]]).fetchone()[0], 3)
        self.assertEqual(self.con.execute("SELECT count(*) FROM (SELECT observation_id FROM silver_product_attribute_observation_v GROUP BY observation_id HAVING count(*)>1)").fetchone()[0], 0)

    def test_absent_null_and_missing_are_distinct_identities_and_coverage(self):
        self.con.execute("CREATE TABLE silver_input_column_v(_source_id VARCHAR,column_name VARCHAR,data_type VARCHAR,column_ordinal INTEGER)")
        self.con.execute("INSERT INTO silver_input_column_v VALUES ('stub-a','Color','VARCHAR',1),('stub-copy','Color','VARCHAR',1),('attr-a','Color','VARCHAR',1),('attr-b','Color','VARCHAR',1),('attr-c','Color','VARCHAR',1)")
        self.con.execute("INSERT INTO bronze_product_attr VALUES (1,2,3,4,10,'GROC','blue','attr-a','/attr/a.parquet',1)")
        register(self.con)
        rows = self.con.execute("SELECT value_status,observation_id FROM silver_product_attribute_observation_v WHERE attribute_name='Color' AND source_family='product_attr' ORDER BY value_status").fetchall()
        statuses = {r[0] for r in rows}
        self.assertEqual(statuses, {'known','source_null','source_missing'})
        ids_by_status = {status: {identity for value_status, identity in rows if value_status == status} for status in statuses}
        self.assertNotEqual(next(iter(ids_by_status['source_null'])), next(iter(ids_by_status['source_missing'])))
        coverage = self.con.execute("SELECT collection_status FROM silver_coverage_v WHERE source_family='stub' AND attribute_name='SIZE'").fetchone()[0]
        self.assertEqual(coverage, 'not_collected')

    def test_fullpath_and_basename_reference_ambiguity_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'stub_categories.csv'
            path.write_text('edition,source_workbook,category\n2008-2011,a.xlsx,GROC\n2008-2011,/stub/a.xlsx,DRUG\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'ambiguous reviewed'):
                register(self.con, reference_root=Path(d))


if __name__ == '__main__':
    unittest.main()
