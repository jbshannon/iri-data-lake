"""Auditable product attribute observations and conservative annual consensus."""
from __future__ import annotations

import csv
from pathlib import Path


def _exists(con, name: str) -> bool:
    return bool(con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0])


def _columns(con, name: str) -> dict[str, str]:
    return {r[0].lower(): r[0] for r in con.execute(f'DESCRIBE "{name}"').fetchall()}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _register_category_reference(con, reference_root: Path | None) -> None:
    con.execute("CREATE OR REPLACE TEMP TABLE _stub_category_ref(edition VARCHAR, source_workbook VARCHAR, category VARCHAR)")
    con.execute("CREATE OR REPLACE TEMP VIEW silver_stub_category_xref_v AS SELECT CAST(NULL AS VARCHAR) edition, CAST(NULL AS VARCHAR) source_workbook, CAST(NULL AS VARCHAR) category, CAST(NULL AS VARCHAR) mapping_status WHERE FALSE")
    if reference_root is None:
        return
    path = Path(reference_root) / "stub_categories.csv"
    if not path.exists():
        return
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        expected = {"edition", "source_workbook", "category"}
        if reader.fieldnames is None or not expected.issubset(reader.fieldnames):
            raise ValueError("stub_categories.csv requires edition,source_workbook,category columns")
        rows = [(r["edition"], r["source_workbook"], r["category"]) for r in reader]
    mappings: dict[tuple[str, str], set[str]] = {}
    for edition, workbook, category in rows:
        key = ((edition or "").strip(), (workbook or "").strip())
        if not all(key) or not (category or "").strip():
            continue
        mappings.setdefault(key, set()).add(category.strip())
    collisions = [k for k, values in mappings.items() if len(values) != 1]
    if collisions:
        raise ValueError(f"conflicting reviewed stub category mappings: {collisions!r}")
    values = [(e, w, next(iter(c))) for (e, w), c in mappings.items()]
    if values:
        con.executemany("INSERT INTO _stub_category_ref VALUES (?, ?, ?)", values)
    con.execute("CREATE OR REPLACE TEMP VIEW silver_stub_category_xref_v AS SELECT edition,source_workbook,category,'matched'::VARCHAR mapping_status FROM _stub_category_ref")


def register(con, *, reference_root: Path | None = None) -> dict[str, dict]:
    """Register raw observations, locator lineage, annual resolutions, and coverage."""
    _register_category_reference(con, reference_root)
    # Use the shared versioned identity codec. Never substitute a SQL-specific hash.
    from .keys import register_functions
    register_functions(con)

    reg: dict[str, dict] = {}
    sources: list[tuple[str, str]] = []
    for view, family in (("bronze_product_stub", "stub"), ("bronze_product_attr", "product_attr")):
        if _exists(con, view):
            sources.append((view, family))

    fragments: list[str] = []
    coverage_parts: list[str] = []
    for view, family in sources:
        cols = _columns(con, view)
        needed = {"sy", "ge", "vend", "item"}
        if not needed.issubset(cols):
            raise ValueError(f"{view} missing product tuple columns {sorted(needed-set(cols))}")
        edition_col = cols.get("edition") or cols.get("product_edition")
        year_col = cols.get("source_year")
        cat_col = cols.get("category")
        file_col, id_col, row_col = (cols.get("_source_file"), cols.get("_source_id"), cols.get("_source_row"))
        if family == "stub" and not edition_col:
            raise ValueError("product stub edition must be supplied as an edition column")
        excluded = {"upc", "sy", "ge", "vend", "item", "source_year", "year", "category", "edition", "product_edition", "_source_id", "_source_file", "_source_row", "_source_year", "_source_category"}
        attrs = [r[0] for r in con.execute(f'DESCRIBE "{view}"').fetchall() if r[0].lower() not in excluded and not r[0].startswith("_")]
        has_catalog = _exists(con, "silver_input_column_v") and id_col is not None
        for attr in attrs:
            q = _q(attr)
            sid = f"CAST(b.{_q(id_col)} AS VARCHAR)" if id_col else "CAST(NULL AS VARCHAR)"
            file = f"CAST(b.{_q(file_col)} AS VARCHAR)" if file_col else "CAST(NULL AS VARCHAR)"
            row = f"CAST(b.{_q(row_col)} AS UBIGINT)" if row_col else "CAST(NULL AS UBIGINT)"
            edition = f"CAST(b.{_q(edition_col)} AS VARCHAR)" if edition_col else "CAST(NULL AS VARCHAR)"
            year = f"CAST(b.{_q(year_col)} AS UTINYINT)" if year_col else "CAST(NULL AS UTINYINT)"
            source_category = f"CAST(b.{_q(cat_col)} AS VARCHAR)" if cat_col else "CAST(NULL AS VARCHAR)"
            category = source_category
            mapping_status = "'source'::VARCHAR" if cat_col else "'unmapped'::VARCHAR"
            if family == "stub" and file_col:
                basename = f"regexp_extract({file}, '[^/\\\\]+$')"
                candidates = f"SELECT DISTINCT r.category FROM _stub_category_ref r WHERE r.edition={edition} AND r.source_workbook IN ({file},{basename})"
                con.execute(f"""CREATE OR REPLACE TEMP VIEW _attribute_mapping_conflicts AS
                    SELECT DISTINCT b.{_q(edition_col)} edition,b.{_q(file_col)} source_file,a.category
                    FROM \"{view}\" b CROSS JOIN LATERAL ({candidates}) a""")
                conflict = con.execute("SELECT count(*) FROM (SELECT edition,source_file,count(DISTINCT category) n FROM _attribute_mapping_conflicts GROUP BY edition,source_file HAVING n>1)").fetchone()[0]
                if conflict:
                    raise ValueError("ambiguous reviewed stub category mapping between basename and full path")
                category = f"coalesce({source_category},(SELECT min(r.category) FROM _stub_category_ref r WHERE r.edition={edition} AND r.source_workbook IN ({file},{basename})) )"
                mapping_status = f"CASE WHEN {source_category} IS NOT NULL THEN 'source' WHEN {category} IS NOT NULL THEN 'matched' ELSE 'unmapped' END"
            catalog = "TRUE" if not has_catalog else f"EXISTS (SELECT 1 FROM silver_input_column_v c WHERE c._source_id={sid} AND c.column_name={attr!r})"
            physical_raw = f"CAST(b.{q} AS VARCHAR)"
            status = f"CASE WHEN b.{q} IS NULL THEN 'source_null' WHEN trim({physical_raw})='MISSING' THEN 'source_missing' ELSE 'known' END"
            final_status = f"CASE WHEN NOT ({catalog}) THEN 'not_collected' ELSE {status} END"
            raw = f"CASE WHEN ({catalog}) THEN {physical_raw} ELSE NULL END"
            normalized = f"CASE WHEN {final_status}='known' THEN trim({raw}) ELSE NULL END"
            upc_col = cols.get("upc")
            if upc_col:
                upc = f"CAST(b.{_q(upc_col)} AS VARCHAR)"
                mapping = "'direct'::VARCHAR"
            else:
                # LEFT mapping retains source tuple evidence even where no UPC mapping exists.
                upc = "CAST(x.upc AS VARCHAR)"
                mapping = "CASE WHEN x.upc IS NULL THEN 'unmatched_tuple' ELSE 'matched_tuple' END"
            join = "" if upc_col else f"LEFT JOIN silver_product_key_xref_v x ON x.sy=b.{_q(cols['sy'])} AND x.ge=b.{_q(cols['ge'])} AND x.vend=b.{_q(cols['vend'])} AND x.item=b.{_q(cols['item'])}"
            payload = ("to_json(struct_pack(codec := 'product_attribute_observation_v1', "
                       f"source_family := {family!r}, sy := CAST(b.{_q(cols['sy'])} AS UTINYINT), ge := CAST(b.{_q(cols['ge'])} AS UTINYINT), "
                       f"vend := CAST(b.{_q(cols['vend'])} AS UINTEGER), item := CAST(b.{_q(cols['item'])} AS UINTEGER), edition := {edition}, "
                       f"source_year := {year}, category := {category}, attribute_name := {attr!r}, raw_value := {raw}, value_status := {final_status}))")
            base = f"SELECT {upc} upc,CAST(b.{_q(cols['sy'])} AS UTINYINT) sy,CAST(b.{_q(cols['ge'])} AS UTINYINT) ge,CAST(b.{_q(cols['vend'])} AS UINTEGER) vend,CAST(b.{_q(cols['item'])} AS UINTEGER) item,{family!r} source_family,{edition} edition,{year} source_year,{category} category,{mapping_status} category_mapping_status,{mapping} product_mapping_status,{attr!r} attribute_name,{attr!r} source_attribute_name,{raw} raw_value,{normalized} normalized_value,{final_status} value_status,silver_id({payload}) observation_id,{sid} _source_id,{file} _source_file,{row} _source_row,({catalog}) collected"
            fragments.append(base + f" FROM \"{view}\" b {join}")
            coverage_parts.append(f"SELECT {family!r} source_family,{edition} edition,{year} source_year,{category} category,{mapping_status} category_mapping_status,{attr!r} attribute_name,{sid} _source_id,{file} _source_file,({catalog}) collected,count(*) represented_rows FROM \"{view}\" b {join} GROUP BY ALL")
    raw_union = " UNION ALL ".join(fragments)
    if raw_union:
        con.execute(f"""CREATE OR REPLACE TEMP VIEW _silver_product_attribute_raw_v AS {raw_union}""")
        con.execute("""CREATE OR REPLACE TEMP VIEW silver_product_attribute_observation_v AS
          SELECT upc,sy,ge,vend,item,source_family,edition,source_year,category,category_mapping_status,product_mapping_status,
            attribute_name,source_attribute_name,raw_value,normalized_value,value_status,observation_id,
            count(*) source_record_count
          FROM _silver_product_attribute_raw_v
          GROUP BY ALL""")
        con.execute("""CREATE OR REPLACE TEMP VIEW silver_product_attribute_lineage_v AS
          SELECT observation_id,_source_id,_source_file,_source_row,source_family,edition,source_year,category,attribute_name,source_attribute_name,
            count(*) source_record_count
          FROM _silver_product_attribute_raw_v WHERE _source_file IS NOT NULL
          GROUP BY ALL""")
        years = "CASE WHEN edition='2001-2006' THEN range(1,7) WHEN edition='2007' THEN range(7,8) WHEN edition='2008-2011' THEN range(8,12) WHEN edition='2012' THEN range(12,13) ELSE []::INTEGER[] END"
        con.execute(f"""CREATE OR REPLACE TEMP VIEW _silver_product_attribute_annual_v AS
          SELECT o.*,CAST(y.applicable_year AS UTINYINT) resolution_year
          FROM silver_product_attribute_observation_v o
          CROSS JOIN LATERAL (SELECT unnest(CASE WHEN source_family='stub' THEN {years} ELSE [source_year::INTEGER] END) applicable_year) y""")
        con.execute("""CREATE OR REPLACE TEMP VIEW silver_product_attribute_v AS
          WITH p AS (SELECT upc,resolution_year source_year,attribute_name,
            count(DISTINCT normalized_value) FILTER(WHERE value_status='known') known_values,
            count(DISTINCT observation_id) observations,
            count(DISTINCT observation_id) FILTER(WHERE value_status='known') known_count,
            count(DISTINCT observation_id) FILTER(WHERE value_status='source_missing') missing_count,
            count(DISTINCT observation_id) FILTER(WHERE value_status='source_null') null_count,
            count(DISTINCT observation_id) FILTER(WHERE value_status='not_collected') absent_count,
            min(normalized_value) FILTER(WHERE value_status='known') sole_value,
            string_agg(DISTINCT source_family,',' ORDER BY source_family) source_families
            FROM _silver_product_attribute_annual_v WHERE upc IS NOT NULL AND resolution_year IS NOT NULL GROUP BY upc,resolution_year,attribute_name)
          SELECT upc,source_year,attribute_name,
            CASE WHEN known_values=1 AND known_count=observations THEN sole_value ELSE NULL END AS "value",
            CASE WHEN known_values>1 THEN 'conflict' WHEN known_values=1 AND known_count<observations THEN 'partial' WHEN known_values=1 THEN 'known' WHEN missing_count>0 AND null_count=0 AND absent_count=0 THEN 'source_missing' WHEN null_count>0 AND missing_count=0 AND absent_count=0 THEN 'unknown' ELSE 'not_collected' END status,
            source_families,observations evidence_count
          FROM p""")
        con.execute(f"""CREATE OR REPLACE TEMP VIEW silver_coverage_v AS
          SELECT source_family,CAST(y.applicable_year AS UTINYINT) source_year,category,category_mapping_status,attribute_name,
            bool_or(collected) collected,CASE WHEN bool_or(collected) THEN 'collected' ELSE 'not_collected' END collection_status,
            sum(represented_rows) represented_rows,count(DISTINCT _source_id) FILTER(WHERE collected) source_files
          FROM ({' UNION ALL '.join(coverage_parts)}) c
          CROSS JOIN LATERAL (SELECT unnest(CASE WHEN source_family='stub' THEN {years} ELSE [source_year::INTEGER] END) applicable_year) y
          GROUP BY ALL""")
    else:
        return {}  # No attribute family was selected; do not invent an empty domain.
    for name, key in (("silver_product_attribute_observation_v", ["observation_id"]), ("silver_product_attribute_lineage_v", ["observation_id", "_source_id", "_source_file", "_source_row"]), ("silver_product_attribute_v", ["upc", "source_year", "attribute_name"]), ("silver_coverage_v", ["source_family", "source_year", "category", "category_mapping_status", "attribute_name"])):
        reg[name] = {"schema_version": 1, "primary_key": key, "materialize": False, "restricted": False}
    reg["silver_stub_category_xref_v"] = {"schema_version": 1, "primary_key": ["edition", "source_workbook"], "materialize": False, "restricted": False}
    return reg


def coalesce_sql(value: str) -> str:
    return f"coalesce(CAST({value} AS VARCHAR),'[NULL]')"


def validate(con) -> list[dict]:
    """Check real declared keys, complete lineage, and consensus against raw evidence."""
    checks: list[dict] = []
    if not _exists(con, "silver_product_attribute_observation_v"):
        return [{"check_id": "product_attribute_views_registered", "ok": False, "severity": "error", "actual": False, "expected": True}]
    dup, nulls = con.execute("""SELECT
      (SELECT count(*) FROM (SELECT observation_id FROM silver_product_attribute_observation_v GROUP BY observation_id HAVING count(*)<>1)),
      (SELECT count(*) FROM silver_product_attribute_observation_v WHERE observation_id IS NULL OR attribute_name IS NULL OR source_attribute_name IS NULL OR source_record_count<1)""").fetchone()
    checks.append({"check_id": "product_attribute_observation_keys", "ok": dup == 0 and nulls == 0, "severity": "error", "actual": {"duplicates": dup, "null_keys": nulls}, "expected": {"duplicates": 0, "null_keys": 0}})
    lineage = con.execute("""SELECT count(*) FROM silver_product_attribute_observation_v o
      WHERE o.source_record_count<>coalesce((SELECT sum(l.source_record_count) FROM silver_product_attribute_lineage_v l WHERE l.observation_id=o.observation_id),0)""").fetchone()[0]
    checks.append({"check_id": "product_attribute_lineage", "ok": lineage == 0, "severity": "error", "actual": lineage, "expected": 0})
    bad_status = con.execute("SELECT count(*) FROM silver_product_attribute_observation_v WHERE value_status NOT IN ('known','source_null','source_missing','not_collected')").fetchone()[0]
    checks.append({"check_id": "product_attribute_status", "ok": bad_status == 0, "severity": "error", "actual": bad_status, "expected": 0})
    bad = con.execute("""WITH expected AS (
      SELECT upc,resolution_year source_year,attribute_name,
        CASE WHEN count(DISTINCT normalized_value) FILTER(WHERE value_status='known')=1
          AND count(DISTINCT observation_id) FILTER(WHERE value_status='known')=count(DISTINCT observation_id)
          THEN min(normalized_value) FILTER(WHERE value_status='known') ELSE NULL END AS "value",
        CASE WHEN count(DISTINCT normalized_value) FILTER(WHERE value_status='known')>1 THEN 'conflict'
          WHEN count(DISTINCT normalized_value) FILTER(WHERE value_status='known')=1 AND count(DISTINCT observation_id) FILTER(WHERE value_status='known')<count(DISTINCT observation_id) THEN 'partial'
          WHEN count(DISTINCT normalized_value) FILTER(WHERE value_status='known')=1 THEN 'known'
          WHEN count(DISTINCT observation_id) FILTER(WHERE value_status='source_missing')>0 AND count(DISTINCT observation_id) FILTER(WHERE value_status='source_null')=0 AND count(DISTINCT observation_id) FILTER(WHERE value_status='not_collected')=0 THEN 'source_missing'
          WHEN count(DISTINCT observation_id) FILTER(WHERE value_status='source_null')>0 AND count(DISTINCT observation_id) FILTER(WHERE value_status='source_missing')=0 AND count(DISTINCT observation_id) FILTER(WHERE value_status='not_collected')=0 THEN 'unknown'
          ELSE 'not_collected' END status
      FROM _silver_product_attribute_annual_v WHERE upc IS NOT NULL AND resolution_year IS NOT NULL GROUP BY upc,resolution_year,attribute_name)
      SELECT count(*) FROM expected e FULL JOIN silver_product_attribute_v a USING(upc,source_year,attribute_name)
      WHERE e.status IS DISTINCT FROM a.status OR e.value IS DISTINCT FROM a.value OR e.status IS NULL""").fetchone()[0]
    checks.append({"check_id": "product_attribute_resolution", "ok": bad == 0, "severity": "error", "actual": bad, "expected": 0})
    bad_cov = con.execute("SELECT count(*) FROM silver_coverage_v WHERE represented_rows<0 OR source_files<0 OR (collected AND collection_status<>'collected') OR (NOT collected AND collection_status<>'not_collected')").fetchone()[0]
    checks.append({"check_id": "product_attribute_coverage", "ok": bad_cov == 0, "severity": "error", "actual": bad_cov, "expected": 0})
    return checks
