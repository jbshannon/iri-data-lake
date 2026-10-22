"""Product identity, edition evidence, and exact UPC-week sales relations."""
from pathlib import Path


def _has_view(con, name: str) -> bool:
    return bool(con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0])


def _columns(con, name: str) -> list[str]:
    return [r[0] for r in con.execute(f'DESCRIBE "{name}"').fetchall()]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# IRI weeks are globally unique counters. These ranges are authoritative and are
# intentionally not interpreted as calendar week-of-year values.
_YEAR_RANGES = ((1,1114,1165),(2,1166,1217),(3,1218,1269),(4,1270,1321),
                (5,1322,1373),(6,1374,1426),(7,1427,1478),(8,1479,1530),
                (9,1531,1582),(10,1583,1634),(11,1635,1686),(12,1687,1739))

def _year_sql(week: str) -> str:
    return "CASE " + " ".join(f"WHEN {week} BETWEEN {lo} AND {hi} THEN {year}" for year,lo,hi in _YEAR_RANGES) + " END"


def register(con, *, reference_root: Path | None = None) -> dict[str, dict]:
    """Register product and sales views supported by normalized Bronze inputs."""
    del reference_root
    result: dict[str, dict] = {}
    has_stub, has_sales = _has_view(con, 'bronze_product_stub'), _has_view(con, 'bronze_sales')
    if has_stub:
        cols = {c.lower(): c for c in _columns(con, 'bronze_product_stub')}
        required = ('sy','ge','vend','item','upc')
        if not all(x in cols for x in required):
            raise ValueError('bronze_product_stub missing full product identity columns')
        ident = ','.join(f'CAST({_q(cols[x])} AS {typ}) AS {x}' for x,typ in [('sy','UTINYINT'),('ge','UTINYINT'),('vend','UINTEGER'),('item','UINTEGER')])
        upc = f'CAST({_q(cols["upc"])} AS VARCHAR)'
        conflict = con.execute(f"""
          WITH c AS (SELECT {ident}, {upc} upc FROM bronze_product_stub
             WHERE {_q(cols['sy'])} IS NOT NULL AND {_q(cols['ge'])} IS NOT NULL AND {_q(cols['vend'])} IS NOT NULL AND {_q(cols['item'])} IS NOT NULL AND {_q(cols['upc'])} IS NOT NULL),
          a AS (SELECT sy,ge,vend,item,count(DISTINCT upc) n FROM c GROUP BY ALL),
          b AS (SELECT upc,count(DISTINCT (sy,ge,vend,item)) n FROM c GROUP BY upc)
          SELECT (SELECT count(*) FROM a WHERE n<>1)+(SELECT count(*) FROM b WHERE n<>1)
        """).fetchone()[0]
        if conflict:
            raise ValueError(f'product identity crosswalk is not globally bijective ({conflict} conflicts)')
        con.execute(f"""CREATE OR REPLACE TEMP VIEW silver_product_key_xref_v AS
          SELECT {ident}, {upc} upc FROM bronze_product_stub
          WHERE {_q(cols['sy'])} IS NOT NULL AND {_q(cols['ge'])} IS NOT NULL AND {_q(cols['vend'])} IS NOT NULL AND {_q(cols['item'])} IS NOT NULL AND {_q(cols['upc'])} IS NOT NULL GROUP BY ALL""")
        result['silver_product_key_xref_v']={'schema_version':1,'primary_key':['sy','ge','vend','item'],'materialize':True,'restricted':False}

        edition_col = next((cols[x] for x in ('edition','product_edition') if x in cols), None)
        if edition_col:
            edition = f'CAST({_q(edition_col)} AS VARCHAR)'
            bad_edition=con.execute(f"SELECT count(*) FROM bronze_product_stub WHERE {edition} IS NOT NULL AND {edition} NOT IN ('2001-2006','2007','2008-2011','2012')").fetchone()[0]
            if bad_edition: raise ValueError(f'unsupported product edition values ({bad_edition} rows)')
            payload = [x for x in ('sy','ge','vend','item','l1','l2','l3','l4','l5','l9','level','stubspec','vol_eq') if x in cols]
            payload_sql = ','.join(f'CAST({_q(cols[x])} AS VARCHAR)' for x in payload)
            conflict = con.execute(f"""SELECT count(*) FROM (
              SELECT {_q(cols['upc'])} upc, {edition} edition, count(DISTINCT ({payload_sql})) n
              FROM bronze_product_stub WHERE {_q(cols['upc'])} IS NOT NULL AND {edition} IS NOT NULL GROUP BY 1,2 HAVING n>1)""").fetchone()[0]
            if conflict:
                raise ValueError(f'conflicting edition descriptive payloads ({conflict} conflicts)')
            fields=[]
            for x in ('l1','l2','l3','l4','l5','l9','stubspec'):
                fields.append(f"CAST({_q(cols[x])} AS VARCHAR) {x}" if x in cols else f'CAST(NULL AS VARCHAR) {x}')
            level=f"CAST({_q(cols['level'])} AS USMALLINT)" if 'level' in cols else 'CAST(NULL AS USMALLINT)'
            vol=f"CAST({_q(cols['vol_eq'])} AS DOUBLE)" if 'vol_eq' in cols else 'CAST(NULL AS DOUBLE)'
            con.execute(f"""CREATE OR REPLACE TEMP VIEW silver_product_edition_v AS
              SELECT {upc} upc, {edition} edition, {ident}, {','.join(fields)}, {level} AS "level", {vol} AS vol_eq,
                     CAST(NULL AS VARCHAR) category, 'unresolved'::VARCHAR category_status
              FROM bronze_product_stub WHERE {_q(cols['upc'])} IS NOT NULL AND {edition} IS NOT NULL GROUP BY ALL""")
            con.execute("""CREATE OR REPLACE TEMP VIEW silver_product_v AS
              SELECT e.upc,CAST(y.source_year AS UTINYINT) source_year,e.edition,'edition'::VARCHAR attribute_scope,e.sy,e.ge,e.vend,e.item,
                e.l1,e.l2,e.l3,e.l4,e.l5,e.l9 AS item_description,e.level,e.stubspec,e.vol_eq,e.category,e.category_status
              FROM silver_product_edition_v e CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) y(source_year)
              WHERE (y.source_year BETWEEN 1 AND 6 AND e.edition='2001-2006') OR (y.source_year=7 AND e.edition='2007')
                 OR (y.source_year BETWEEN 8 AND 11 AND e.edition='2008-2011') OR (y.source_year=12 AND e.edition='2012')""")
            result.update({'silver_product_edition_v':{'schema_version':1,'primary_key':['upc','edition'],'materialize':False,'restricted':False},
                           'silver_product_v':{'schema_version':1,'primary_key':['upc','source_year'],'materialize':False,'restricted':False}})

    if has_sales:
        sales_cols={c.lower():c for c in _columns(con,'bronze_sales')}
        raw=('iri_key','week','sy','ge','vend','item','units','dollars_cents','feature_code','display','price_reduction')
        scope=('source_year','category','channel')
        missing=[x for x in raw+scope if x not in sales_cols]
        if missing:
            raise ValueError('bronze_sales missing normalized contract fields: '+', '.join(missing))
        projection=[]
        for x in raw+scope:
            c=sales_cols[x]
            typ={'source_year':'UTINYINT','iri_key':'UINTEGER','week':'USMALLINT','sy':'UTINYINT','ge':'UTINYINT','vend':'UINTEGER','item':'UINTEGER'}.get(x)
            projection.append((f'CAST({_q(c)} AS {typ})' if typ else _q(c))+f' AS {x}')
        con.execute('CREATE OR REPLACE TEMP VIEW silver_sales_v AS SELECT '+','.join(projection)+' FROM bronze_sales')
        result['silver_sales_v']={'schema_version':1,'primary_key':[],'materialize':False,'restricted':False}
        # Validate source scope before building. Unknown/out-of-range week and mismatched
        # authoritative source year are hard errors, never silently discarded.
        yr=_year_sql('week')
        bad=con.execute(f'SELECT count(*) FROM silver_sales_v WHERE source_year NOT BETWEEN 1 AND 12 OR ({yr}) IS NULL OR source_year <> ({yr})').fetchone()[0]
        if bad: raise ValueError(f'sales source_year/week disagreement ({bad} rows)')
        if not has_stub:
            return result
        con.execute("""CREATE OR REPLACE TEMP VIEW silver_product_week_v AS
          WITH mapped AS (SELECT x.upc,s.week,s.units,s.dollars_cents FROM silver_sales_v s
            JOIN silver_product_key_xref_v x USING(sy,ge,vend,item)), a AS (
            SELECT upc,CAST(week AS USMALLINT) AS "week",sum(CAST(units AS HUGEINT)) u,
                   sum(CAST(dollars_cents AS HUGEINT)) r,count(*) n FROM mapped GROUP BY upc,week)
          SELECT upc,"week",CAST(u AS BIGINT) units,CAST(r AS BIGINT) revenue_cents,CAST(n AS BIGINT) sales_rows FROM a""")
        # Catch unmatched records and accumulator-to-public-type overflow. Conversion is checked,
        # and no aggregate group is filtered out to make a gate pass.
        unmatched=con.execute('SELECT count(*) FROM silver_sales_v s ANTI JOIN silver_product_key_xref_v x USING(sy,ge,vend,item)').fetchone()[0]
        overflow=con.execute('''SELECT count(*) FROM (SELECT sum(CAST(units AS HUGEINT)) u,sum(CAST(dollars_cents AS HUGEINT)) r,count(*) n FROM
          (SELECT x.upc,s.week,s.units,s.dollars_cents FROM silver_sales_v s JOIN silver_product_key_xref_v x USING(sy,ge,vend,item)) GROUP BY upc,week)
          WHERE u NOT BETWEEN -9223372036854775808 AND 9223372036854775807 OR r NOT BETWEEN -9223372036854775808 AND 9223372036854775807 OR n>9223372036854775807''').fetchone()[0]
        if unmatched or overflow: raise ValueError(f'product-week preflight failed: unmatched={unmatched}, overflow={overflow}')
        result['silver_product_week_v']={'schema_version':1,'primary_key':['upc','week'],'materialize':True,'restricted':False}
        failed=[x for x in validate(con) if not x['ok']]
        if failed: raise ValueError(f'product-week conservation failed: {failed}')
    return result


def validate(con) -> list[dict]:
    checks=[]
    if _has_view(con,'silver_product_key_xref_v'):
        dup=con.execute('SELECT count(*) FROM (SELECT sy,ge,vend,item,count(*) n FROM silver_product_key_xref_v GROUP BY ALL HAVING n<>1)').fetchone()[0]
        inverse=con.execute('SELECT count(*) FROM (SELECT upc,count(*) n FROM silver_product_key_xref_v GROUP BY upc HAVING n<>1)').fetchone()[0]
        checks.append({'check_id':'product_key_xref_bijection','ok':dup==0 and inverse==0,'severity':'error','actual':{'tuple_conflicts':dup,'upc_conflicts':inverse},'expected':0})
    if _has_view(con,'silver_product_edition_v'):
        d=con.execute('SELECT count(*) FROM (SELECT upc,edition,count(*) n FROM silver_product_edition_v GROUP BY ALL HAVING n<>1)').fetchone()[0]
        checks.append({'check_id':'product_edition_key','ok':d==0,'severity':'error','actual':d,'expected':0})
    if _has_view(con,'silver_product_week_v') and _has_view(con,'silver_sales_v'):
        src=con.execute('SELECT count(*),coalesce(sum(CAST(units AS HUGEINT)),0),coalesce(sum(CAST(dollars_cents AS HUGEINT)),0) FROM silver_sales_v').fetchone()
        out=con.execute('SELECT coalesce(sum(CAST(sales_rows AS HUGEINT)),0),coalesce(sum(CAST(units AS HUGEINT)),0),coalesce(sum(CAST(revenue_cents AS HUGEINT)),0) FROM silver_product_week_v').fetchone()
        dup=con.execute('SELECT count(*) FROM (SELECT upc,week,count(*) n FROM silver_product_week_v GROUP BY ALL HAVING n<>1)').fetchone()[0]
        src_year=con.execute('SELECT source_year,count(*),coalesce(sum(CAST(units AS HUGEINT)),0),coalesce(sum(CAST(dollars_cents AS HUGEINT)),0) FROM silver_sales_v GROUP BY source_year').fetchall()
        yr=_year_sql('week')
        out_year=con.execute(f'SELECT {yr} source_year,coalesce(sum(CAST(sales_rows AS HUGEINT)),0),coalesce(sum(CAST(units AS HUGEINT)),0),coalesce(sum(CAST(revenue_cents AS HUGEINT)),0) FROM silver_product_week_v GROUP BY 1').fetchall()
        mismatches=[]
        expected={r[0]:tuple(r[1:]) for r in src_year}; actual={r[0]:tuple(r[1:]) for r in out_year}
        for y in set(expected)|set(actual):
            if expected.get(y,(0,0,0)) != actual.get(y,(0,0,0)): mismatches.append(y)
        checks.append({'check_id':'product_week_global_and_year_conservation','ok':tuple(src)==tuple(out) and dup==0 and not mismatches,'severity':'error','actual':{'source':src,'output':out,'duplicate_keys':dup,'year_mismatches':mismatches},'expected':'exact source count/units/cents globally and by authoritative output week-year; unique UPC-week'})
    if _has_view(con,'silver_sales_v'):
        columns=_columns(con,'silver_sales_v')
        expected=['iri_key','week','sy','ge','vend','item','units','dollars_cents','feature_code','display','price_reduction','source_year','category','channel']
        checks.append({'check_id':'sales_public_projection','ok':columns==expected,'severity':'error','actual':columns,'expected':expected})
    return checks
