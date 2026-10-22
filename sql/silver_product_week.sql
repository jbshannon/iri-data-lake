-- Silver product-week sales building block.
--
-- Grain: one row per (UPC, IRI week), summed across stores, categories, and
-- sales channels. Product identity is resolved through the separate
-- silver_product_key_xref_v, which preserves the original (SY, GE, VEND, ITEM)
-- key alongside UPC. The crosswalk is canonicalized across stub editions only
-- after validating that the key<->UPC mapping is one-to-one in this corpus.
--
-- The runner opens an in-memory DuckDB connection, so these views are session
-- scoped. CREATE VIEW itself is lazy and is not a useful runtime measurement.
-- The final SELECT consumes the complete aggregate and checks that mapped fact
-- rows equal successful sales rows in the latest manifest; run_sql.py reports
-- elapsed time for that evaluation. Run only after the full Bronze ingestion
-- and Gates 5/6 complete.

-- Profile the source mapping before using it. Incomplete, all-null stub rows
-- are counted and reported; conflicting key mappings or non-invertible UPCs
-- fail the gate rather than being silently collapsed.
WITH stub_rows AS (
    SELECT SY AS sy, GE AS ge, VEND AS vend, ITEM AS item, UPC AS upc
    FROM read_parquet(
        '${LAKE}/bronze/iri_product_stub/**/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
),
key_to_upc AS (
    SELECT sy, ge, vend, item, count(DISTINCT upc) AS upc_count
    FROM stub_rows
    WHERE sy IS NOT NULL AND ge IS NOT NULL
      AND vend IS NOT NULL AND item IS NOT NULL AND upc IS NOT NULL
    GROUP BY sy, ge, vend, item
),
upc_to_key AS (
    SELECT upc, count(DISTINCT (sy, ge, vend, item)) AS key_count
    FROM stub_rows
    WHERE sy IS NOT NULL AND ge IS NOT NULL
      AND vend IS NOT NULL AND item IS NOT NULL AND upc IS NOT NULL
    GROUP BY upc
)
SELECT
    (SELECT count(*) FROM key_to_upc WHERE upc_count <> 1) = 0 AS product_key_to_upc_unique_ok,
    (SELECT count(*) FROM upc_to_key WHERE key_count <> 1) = 0 AS upc_to_product_key_unique_ok,
    (SELECT count(*) FROM stub_rows
     WHERE sy IS NULL OR ge IS NULL OR vend IS NULL OR item IS NULL OR upc IS NULL)
        AS stub_rows_without_complete_mapping;

-- Canonical crosswalk: one row per complete IRI key tuple and its UPC.
-- Repeated, identical edition rows collapse here; conflicting mappings were
-- rejected by the preceding uniqueness checks. This relation can be joined in
-- either direction without losing the original item key.
CREATE OR REPLACE TEMP VIEW silver_product_key_xref_v AS
SELECT
    sy,
    ge,
    vend,
    item,
    min(upc) AS upc
FROM (
    SELECT SY AS sy, GE AS ge, VEND AS vend, ITEM AS item, UPC AS upc
    FROM read_parquet(
        '${LAKE}/bronze/iri_product_stub/**/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
)
WHERE sy IS NOT NULL AND ge IS NOT NULL
  AND vend IS NOT NULL AND item IS NOT NULL AND upc IS NOT NULL
GROUP BY sy, ge, vend, item
HAVING count(DISTINCT upc) = 1;

-- A compact mapping summary; should be a bijection on complete stub rows.
SELECT
    count(*) AS product_key_mappings,
    count(DISTINCT upc) AS distinct_upcs,
    count(DISTINCT (sy, ge, vend, item)) AS distinct_iri_keys,
    count(*) = count(DISTINCT upc)
        AND count(*) = count(DISTINCT (sy, ge, vend, item)) AS product_key_xref_bijective_ok
FROM silver_product_key_xref_v;

CREATE OR REPLACE TEMP VIEW silver_product_week_v AS
SELECT
    p.upc,
    s.week,
    CAST(sum(s.units) AS BIGINT) AS units,
    CAST(sum(s.dollars_cents) AS BIGINT) AS revenue_cents,
    count(*) AS sales_rows
FROM read_parquet('${BRONZE}/**/*.parquet', hive_partitioning = true) AS s
JOIN silver_product_key_xref_v AS p
  ON p.sy = s.sy
 AND p.ge = s.ge
 AND p.vend = s.vend
 AND p.item = s.item
GROUP BY p.upc, s.week;

-- Forces evaluation of the lazy view and returns a compact result summary.
-- Its elapsed time is the full sales scan/join/aggregation time, excluding
-- Parquet materialization. The manifest comparison verifies that no fact rows
-- were lost to an unmatched product or inflated by the product join.
WITH latest_sales AS (
    SELECT
        written_rows,
        status,
        row_number() OVER (
            PARTITION BY source_path ORDER BY started_at DESC, run_id DESC
        ) AS rn
    FROM read_json('${LAKE}/metadata/manifest.jsonl')
    WHERE coalesce(dataset, 'sales') = 'sales'
),
expected AS (
    SELECT sum(written_rows) AS expected_sales_rows
    FROM latest_sales
    WHERE rn = 1 AND status = 'success'
)
SELECT
    count(*) AS product_week_rows,
    sum(sales_rows) AS matched_sales_rows,
    max(expected_sales_rows) AS expected_sales_rows,
    coalesce(sum(sales_rows) = max(expected_sales_rows), false) AS product_coverage_ok,
    sum(units) AS total_units,
    sum(revenue_cents) AS total_revenue_cents
FROM silver_product_week_v
CROSS JOIN expected;
