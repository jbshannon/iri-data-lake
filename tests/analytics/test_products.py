import unittest

import duckdb

from scripts.silver.products import register


class ProductTests(unittest.TestCase):
    def test_global_crosswalk_sales_grain_and_exact_sums(self):
        con = duckdb.connect()
        con.execute('''CREATE TEMP TABLE bronze_product_stub(
            sy UTINYINT, ge UTINYINT, vend UINTEGER, item UINTEGER, upc VARCHAR)''')
        con.executemany('INSERT INTO bronze_product_stub VALUES (?,?,?,?,?)', [
            (1, 2, 3, 4, '00-ABC'), (1, 2, 3, 4, '00-ABC'),
            (1, 2, 3, 5, '00-XYZ')])
        con.execute('''CREATE TEMP TABLE bronze_sales(
          source_year UTINYINT, category VARCHAR, channel VARCHAR, iri_key UINTEGER,
          week USMALLINT, sy UTINYINT, ge UTINYINT, vend UINTEGER, item UINTEGER,
          units BIGINT, dollars_cents BIGINT, feature_code UTINYINT, display UTINYINT, price_reduction UTINYINT)''')
        con.executemany('INSERT INTO bronze_sales VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [
            (1,'F','groc',10,1114,1,2,3,4,2,123,0,1,0),
            (1,'D','drug',11,1114,1,2,3,4,-1,-9,1,0,0),
            (1,'F','groc',10,1115,1,2,3,5,0,0,0,0,0)])
        metadata = register(con)
        self.assertIn('silver_sales_v', metadata)
        self.assertEqual(con.execute('SELECT upc,week,units,revenue_cents,sales_rows FROM silver_product_week_v ORDER BY week').fetchall(), [
            ('00-ABC',1114,1,114,2), ('00-XYZ',1115,0,0,1)])
        self.assertEqual(con.execute('SELECT count(*) FROM silver_sales_v').fetchone()[0], 3)
        self.assertEqual(con.execute('DESCRIBE silver_product_week_v').fetchall()[0][1], 'VARCHAR')
        con.close()

    def test_actual_format_edition_and_public_sales_projection(self):
        con = duckdb.connect()
        con.execute('''CREATE TEMP TABLE bronze_product_stub(
          upc VARCHAR, sy UTINYINT, ge UTINYINT, vend UINTEGER, item UINTEGER,
          edition VARCHAR, L1 VARCHAR, "Panel description" VARCHAR)''')
        con.execute("INSERT INTO bronze_product_stub VALUES ('001',1,2,3,4,'2012','Food','Name')")
        con.execute('''CREATE TEMP TABLE bronze_sales(source_year UTINYINT,category VARCHAR,channel VARCHAR,
          iri_key UINTEGER,week USMALLINT,sy UTINYINT,ge UTINYINT,vend UINTEGER,item UINTEGER,
          units BIGINT,dollars_cents BIGINT, feature_code UTINYINT, display UTINYINT, price_reduction UTINYINT, _source_file VARCHAR)''')
        con.execute("INSERT INTO bronze_sales VALUES (12,'F','groc',1,1687,1,2,3,4,0,0,0,1,0,'hidden')")
        register(con)
        self.assertEqual(con.execute("SELECT edition,category_status FROM silver_product_edition_v").fetchall(), [('2012','unresolved')])
        self.assertEqual(con.execute("SELECT count(*) FROM silver_sales_v").fetchone()[0],1)
        self.assertEqual([r[0] for r in con.execute('DESCRIBE silver_sales_v').fetchall()], ['iri_key','week','sy','ge','vend','item','units','dollars_cents','feature_code','display','price_reduction','source_year','category','channel'])
        self.assertNotIn('silver_product_attribute_observation_v', [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()])
        self.assertTrue(all(x['ok'] for x in __import__('scripts.silver.products',fromlist=['validate']).validate(con)))
        con.close()

    def test_sales_projection_exists_without_stub(self):
        con = duckdb.connect()
        con.execute('''CREATE TEMP TABLE bronze_sales(source_year UTINYINT,category VARCHAR,channel VARCHAR,
          iri_key UINTEGER,week USMALLINT,sy UTINYINT,ge UTINYINT,vend UINTEGER,item UINTEGER,
          units BIGINT,dollars_cents BIGINT,feature_code UTINYINT,display UTINYINT,price_reduction UTINYINT,
          _source_file VARCHAR)''')
        register(con)
        cols=[r[0] for r in con.execute('DESCRIBE silver_sales_v').fetchall()]
        self.assertEqual(cols,['iri_key','week','sy','ge','vend','item','units','dollars_cents','feature_code','display','price_reduction','source_year','category','channel'])
        self.assertNotIn('_source_file',cols)
        self.assertEqual(con.execute('SELECT count(*) FROM silver_sales_v').fetchone()[0],0)
        con.close()

    def test_tuple_conflict_fails_before_crosswalk(self):
        con = duckdb.connect()
        con.execute('CREATE TEMP TABLE bronze_product_stub(sy UTINYINT,ge UTINYINT,vend UINTEGER,item UINTEGER,upc VARCHAR)')
        con.executemany('INSERT INTO bronze_product_stub VALUES (1,2,3,4,?)', [('A',),('B',)])
        with self.assertRaises(ValueError):
            register(con)
        con.close()

    def test_validate_rechecks_persisted_aggregate_corruption(self):
        from scripts.silver.products import validate
        con=duckdb.connect()
        con.execute('CREATE TEMP TABLE bronze_product_stub(sy UTINYINT,ge UTINYINT,vend UINTEGER,item UINTEGER,upc VARCHAR)')
        con.execute("INSERT INTO bronze_product_stub VALUES (1,2,3,4,'A')")
        con.execute('''CREATE TEMP TABLE bronze_sales(source_year UTINYINT,category VARCHAR,channel VARCHAR,
          iri_key UINTEGER,week USMALLINT,sy UTINYINT,ge UTINYINT,vend UINTEGER,item UINTEGER,
          units BIGINT,dollars_cents BIGINT,feature_code UTINYINT,display UTINYINT,price_reduction UTINYINT)''')
        con.execute("INSERT INTO bronze_sales VALUES (1,'F','groc',1,1114,1,2,3,4,1,10,0,0,0)")
        register(con)
        self.assertTrue(all(c['ok'] for c in validate(con)))
        con.execute('CREATE TEMP TABLE corrupt_week AS SELECT * FROM silver_product_week_v')
        con.execute('UPDATE corrupt_week SET revenue_cents=revenue_cents+1')
        con.execute('DROP VIEW silver_product_week_v')
        con.execute('CREATE TEMP VIEW silver_product_week_v AS SELECT * FROM corrupt_week')
        self.assertFalse(next(c for c in validate(con) if c['check_id']=='product_week_global_and_year_conservation')['ok'])
        con.close()

    def test_unmatched_sales_fails_without_dropping_fact(self):
        con = duckdb.connect()
        con.execute('CREATE TEMP TABLE bronze_product_stub(sy UTINYINT,ge UTINYINT,vend UINTEGER,item UINTEGER,upc VARCHAR)')
        con.execute("INSERT INTO bronze_product_stub VALUES (1,2,3,4,'A')")
        con.execute('''CREATE TEMP TABLE bronze_sales(source_year UTINYINT,category VARCHAR,channel VARCHAR,
          iri_key UINTEGER,week USMALLINT,sy UTINYINT,ge UTINYINT,vend UINTEGER,item UINTEGER,
          units BIGINT,dollars_cents BIGINT,feature_code UTINYINT,display UTINYINT,price_reduction UTINYINT)''')
        con.execute("INSERT INTO bronze_sales VALUES (1,'F','groc',1,1114,1,2,3,5,1,10,0,0,0)")
        with self.assertRaisesRegex(ValueError, 'preflight'):
            register(con)
        con.close()


if __name__ == '__main__':
    unittest.main()
