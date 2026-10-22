import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.silver.inputs import select_inputs, retain_inputs, register_bronze


def entry(path, *, status="success", year=None, dedup=None, dataset="delivery_stores", sha="abc", output=None, rows=0, category="beer"):
    return {"run_id":"r", "dataset":dataset, "source_path":str(path), "source_sha256":sha,
            "source_year":year, "category":category, "deduplicated_from":dedup,
            "output_paths":[str(output)] if output else [], "status":status,
            "written_rows":rows,"output_schema_version":1,"parser_version":"test"}


class InputTests(unittest.TestCase):
    def setup_lake(self, root, records):
        (root / "metadata").mkdir(parents=True, exist_ok=True)
        (root / "metadata/manifest.jsonl").write_text("".join(json.dumps(x)+"\n" for x in records))

    def parquet(self, path, query):
        path.parent.mkdir(parents=True, exist_ok=True)
        con=duckdb.connect()
        con.execute(f"COPY ({query}) TO '{str(path).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET)")
        con.close()
        return path

    def test_append_latest_failed_is_not_hidden(self):
        with tempfile.TemporaryDirectory() as temp:
            lake=Path(temp); out=self.parquet(lake/"x.parquet", "select 1 a")
            self.setup_lake(lake,[entry("a",output=out,rows=1),entry("a",status="failed",output=out,rows=1)])
            with self.assertRaisesRegex(ValueError,"latest attempt failed"):
                select_inputs(lake,required_datasets=["delivery_stores"])

    def test_actual_parquet_selection_retention_and_registration(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"; source=lake/"bronze/iri_sales/year=1/category=beer/channel=drug/part.parquet"
            self.parquet(source,"select 1::BIGINT as week, '00123'::VARCHAR as upc, 4::INTEGER as units, 123::BIGINT as dollars_cents")
            source_path=root/"raw/Year1/input.csv"
            rec=entry(source_path,year=1,dataset="sales",output=source,rows=1)
            self.setup_lake(lake,[rec])
            selected=select_inputs(lake,required_datasets=["sales"])
            retained=retain_inputs(selected,root/"retained")
            source.unlink()
            con=duckdb.connect()
            register_bronze(con,retained)
            row=con.execute("select source_year, upc, _source_row, _source_id, _source_file, category, channel from bronze_sales").fetchone()
            self.assertEqual(row[0:3],(1,"00123",0))
            self.assertEqual(row[5:7],("beer","drug"))
            self.assertEqual(row[3],con.execute("select _source_id from silver_source_v").fetchone()[0])
            self.assertEqual(row[4],str(source.resolve()))
            self.assertEqual(con.execute("select count(*) from silver_input_column_v where column_name='upc'").fetchone()[0],1)
            con.close()

    def test_null_year_alias_restores_only_year_scope_once(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"; out=self.parquet(lake/"bronze/roster.parquet", "select 9::INTEGER as store_code")
            canonical=entry(root/"Year1"/"stores.csv",dataset="delivery_stores",output=out,rows=1,year=None,sha="same")
            alias=entry(root/"Year2"/"stores.csv",dataset="delivery_stores",output=out,rows=1,year=None,dedup=str(canonical["source_path"]),sha="same",category="grocery")
            self.setup_lake(lake,[canonical,alias])
            snap=select_inputs(lake,required_datasets=["delivery_stores"])
            self.assertEqual({a["source_year"] for a in snap["aliases"]},{1,2})
            retained=retain_inputs(snap,root/"retained")
            con=duckdb.connect(); register_bronze(con,retained)
            self.assertEqual(con.execute("select source_year, count(*) from bronze_delivery_stores group by 1 order by 1").fetchall(),[(1,1),(2,1)])
            self.assertEqual(con.execute("select count(*) from bronze_delivery_stores").fetchone()[0],2)
            con.close()

    def test_successful_zero_rows_without_parquet_is_legitimate(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"
            self.setup_lake(lake,[entry(root/"empty.csv",dataset="week_dimension",rows=0)])
            snap=select_inputs(lake,required_datasets=["week_dimension"])
            self.assertTrue(snap["scope"]["complete"])
            self.assertEqual(snap["scope"]["empty_datasets"],["week_dimension"])
            con=duckdb.connect()
            try:
                register_bronze(con,snap)
                self.assertEqual(con.execute("select count(*) from silver_source_alias_v").fetchone()[0],1)
                self.assertEqual(con.execute("select canonical_path from silver_source_alias_v").fetchone()[0],None)
                self.assertEqual(con.execute("select count(*) from duckdb_views() where view_name='bronze_week_dimension'").fetchone()[0],0)
            finally:
                con.close()

    def test_empty_parquet_and_typed_empty_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"
            out=self.parquet(lake/"empty.parquet","select 1::INTEGER as marker where false")
            self.setup_lake(lake,[entry(root/"empty.csv",dataset="week_dimension",output=out,rows=0)])
            snap=select_inputs(lake,required_datasets=["week_dimension"])
            con=duckdb.connect(); register_bronze(con,snap)
            self.assertEqual(con.execute("select count(*) from bronze_week_dimension").fetchone()[0],0)
            self.assertEqual(con.execute("describe silver_source_v").fetchall()[0][1],"VARCHAR")
            self.assertEqual(con.execute("describe silver_source_alias_v").fetchall()[3][1],"UTINYINT")
            con.close()

    def test_count_mismatch_and_corrupt_parquet_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"; out=self.parquet(lake/"x.parquet","select 1 a")
            self.setup_lake(lake,[entry(root/"a",output=out,rows=2)])
            with self.assertRaisesRegex(ValueError,"written_rows mismatch"): select_inputs(lake,required_datasets=["delivery_stores"])
            out.write_bytes(b"corrupt")
            self.setup_lake(lake,[entry(root/"a",output=out,rows=1)])
            with self.assertRaises(Exception): select_inputs(lake,required_datasets=["delivery_stores"])

    def test_scope_and_selection_exclude_unrequested_sensitive_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"; public=self.parquet(lake/"a.parquet","select 1 a"); private=self.parquet(lake/"b.parquet","select 1 b")
            self.setup_lake(lake,[entry(root/"a",dataset="week_dimension",output=public,rows=1),entry(root/"panel",dataset="panel",output=private,rows=1)])
            snap=select_inputs(lake,required_datasets=["week_dimension"])
            self.assertEqual(list(snap["files"]),["week_dimension"])

    def test_flattened_retention_restores_hive_scopes_and_multiple_file_schemas(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"
            one=self.parquet(lake/"bronze/product_stub/edition=2001-2006"/"a.parquet","select 1::INTEGER as item, 'a'::VARCHAR as name")
            two=self.parquet(lake/"bronze/product_stub/edition=2008-2011"/"b.parquet","select 2::INTEGER as item, 99::BIGINT as feature")
            source=root/"raw"/"stub.csv"
            self.setup_lake(lake,[entry(source,dataset="product_stub",output=one,rows=1),
                                  {**entry(source,dataset="product_stub",output=two,rows=1),"output_paths":[str(one),str(two)],"written_rows":2}])
            snap=select_inputs(lake,required_datasets=["product_stub"])
            retained=retain_inputs(snap,root/"retained")
            one.unlink(); two.unlink()
            con=duckdb.connect()
            try:
                register_bronze(con,retained)
                rows=con.execute("select edition, _source_id from bronze_product_stub order by item").fetchall()
                self.assertEqual([r[0] for r in rows],["2001-2006","2008-2011"])
                self.assertNotEqual(rows[0][1],rows[1][1])
                catalog=con.execute("select _source_id, column_name from silver_input_column_v where column_name in ('name','feature') order by column_name").fetchall()
                self.assertEqual(len(catalog),2)
                self.assertEqual(con.execute("select count(*) from bronze_product_stub where edition='2001-2006'").fetchone()[0],1)
            finally:
                con.close()

    def test_panelist_header_alias_preserves_unsigned_integer_type(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"; out=self.parquet(lake/"demos.parquet","select 42::UINTEGER as \"Panelist ID\", '1'::VARCHAR as Year")
            self.setup_lake(lake,[entry(root/"Year1"/"demo.csv",dataset="panelist_demos",output=out,rows=1)])
            snap=select_inputs(lake,required_datasets=["panelist_demos"])
            with duckdb.connect() as con:
                register_bronze(con,snap)
                self.assertEqual(con.execute("select panid, source_year from bronze_panelist_demos").fetchone(),(42,1))
                schema={r[0]:r[1] for r in con.execute("describe bronze_panelist_demos").fetchall()}
                self.assertEqual(schema["panid"],"UINTEGER")

    def test_invalid_year_is_rejected_not_silently_cast_to_null(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); lake=root/"lake"; out=self.parquet(lake/"x.parquet","select 'not-a-year'::VARCHAR as Year")
            self.setup_lake(lake,[entry(root/"source",dataset="week_dimension",output=out,rows=1)])
            snap=select_inputs(lake,required_datasets=["week_dimension"])
            with duckdb.connect() as con:
                with self.assertRaisesRegex(ValueError,"invalid source year"):
                    register_bronze(con,snap)

    def test_manifest_source_and_output_year_scopes_must_agree(self):
        cases = [(1, "Year1/a", "year=2"), (None, "Year1/a", "year=2"),
                 (2, "Year1/a", "plain"), (None, "a", "year=13"),
                 (None, "a", "year=oops"), (None, "a", "year=1/year=2")]
        for year, source, partition in cases:
            with self.subTest(year=year, source=source, partition=partition), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                lake = root / "lake"
                output = self.parquet(lake / "bronze" / partition / "part.parquet", "SELECT 42::UINTEGER iri_key")
                self.setup_lake(lake, [entry(root / source, year=year, output=output, rows=1)])
                with self.assertRaisesRegex(ValueError, "year"):
                    select_inputs(lake, required_datasets=["delivery_stores"])

    def test_cross_year_dedup_alias_keeps_scope_with_canonical_hive_year(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lake = root / "lake"
            output = self.parquet(lake / "bronze" / "year=1" / "part.parquet", "SELECT 42::UINTEGER iri_key")
            canonical = entry(root / "Year1" / "roster", year=1, output=output, rows=1)
            alias = entry(root / "Year2" / "roster", year=2, output=output, rows=1,
                          dedup=canonical["source_path"])
            self.setup_lake(lake, [canonical, alias])
            snapshot = select_inputs(lake, required_datasets=["delivery_stores"])
            with duckdb.connect() as con:
                register_bronze(con, snapshot)
                self.assertEqual(con.execute("SELECT source_year,count(*) FROM bronze_delivery_stores GROUP BY 1 ORDER BY 1").fetchall(),
                                 [(1, 1), (2, 1)])

    def test_retention_integrity_and_original_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); src=self.parquet(root/"source.parquet","select 'fixture'::VARCHAR as fixture_value")
            snap={"files":{"sales":[str(src)]},"manifest_sha256":"h","records":[],"aliases":[],"scope":{}}
            retained=retain_inputs(snap,root/"retained")
            item=retained["integrity"][0]
            self.assertEqual(item["original_path"],str(src)); self.assertEqual(item["sha256"],hashlib.sha256(src.read_bytes()).hexdigest())
            self.assertTrue(Path(item["retained_path"]).is_file())


if __name__ == "__main__": unittest.main()
