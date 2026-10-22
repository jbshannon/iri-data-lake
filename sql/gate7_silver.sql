-- Additional public sales-profile checks after release-pinned bootstrap.
-- Run with: uv run python scripts/run_sql.py sql/gate7_silver.sql \
--   --silver-root /tmp/fixture-silver [--release-id ID]
-- The authoritative complete Gate 7 is `make gate7`: it replays ALL archived
-- domain assertions, including optional selected domains. This SQL profile
-- additionally requires sales, product identity, product-week and calendar.

SELECT count(*) = 626 AND count(DISTINCT week) = 626
       AND min(week) = 1114 AND max(week) = 1739
       AND bool_and(dayofweek(calendar_start_date) = 1 AND calendar_end_date - calendar_start_date = 6)
       AS calendar_ok
FROM silver_week_dimension_v;

SELECT NOT EXISTS (
    SELECT 1 FROM silver_product_week_v
    WHERE upc IS NULL OR week IS NULL OR units IS NULL
       OR revenue_cents IS NULL OR sales_rows IS NULL OR sales_rows < 0
) AND NOT EXISTS (
    SELECT 1 FROM silver_product_week_v GROUP BY upc,week HAVING count(*) > 1
) AS product_week_key_ok;

SELECT NOT EXISTS (
    SELECT 1 FROM silver_product_key_xref_v
    WHERE upc IS NULL OR sy IS NULL OR ge IS NULL OR vend IS NULL OR item IS NULL
) AND NOT EXISTS (
    SELECT 1 FROM silver_product_key_xref_v GROUP BY sy,ge,vend,item HAVING count(*) > 1
) AND NOT EXISTS (
    SELECT 1 FROM silver_product_key_xref_v GROUP BY upc HAVING count(*) > 1
) AS product_mapping_ok;

SELECT (SELECT count(*) FROM bronze_sales) =
       (SELECT coalesce(sum(sales_rows),0) FROM silver_product_week_v)
   AND (SELECT coalesce(sum(units::HUGEINT),0) FROM bronze_sales) =
       (SELECT coalesce(sum(units::HUGEINT),0) FROM silver_product_week_v)
   AND (SELECT coalesce(sum(dollars_cents::HUGEINT),0) FROM bronze_sales) =
       (SELECT coalesce(sum(revenue_cents::HUGEINT),0) FROM silver_product_week_v)
   AS global_conservation_ok;

WITH calendar AS (
    SELECT week, CASE
        WHEN week BETWEEN 1114 AND 1165 THEN 1 WHEN week BETWEEN 1166 AND 1217 THEN 2
        WHEN week BETWEEN 1218 AND 1269 THEN 3 WHEN week BETWEEN 1270 AND 1321 THEN 4
        WHEN week BETWEEN 1322 AND 1373 THEN 5 WHEN week BETWEEN 1374 AND 1426 THEN 6
        WHEN week BETWEEN 1427 AND 1478 THEN 7 WHEN week BETWEEN 1479 AND 1530 THEN 8
        WHEN week BETWEEN 1531 AND 1582 THEN 9 WHEN week BETWEEN 1583 AND 1634 THEN 10
        WHEN week BETWEEN 1635 AND 1686 THEN 11 WHEN week BETWEEN 1687 AND 1739 THEN 12
    END source_year FROM silver_week_dimension_v
), source AS (
    SELECT source_year,count(*)::HUGEINT n,sum(units::HUGEINT) units,
           sum(dollars_cents::HUGEINT) cents
    FROM bronze_sales GROUP BY source_year
), output AS (
    SELECT c.source_year,sum(p.sales_rows::HUGEINT) n,sum(p.units::HUGEINT) units,
           sum(p.revenue_cents::HUGEINT) cents
    FROM silver_product_week_v p JOIN calendar c USING(week)
    GROUP BY c.source_year
)
SELECT NOT EXISTS (
    SELECT 1 FROM source FULL OUTER JOIN output USING(source_year)
    WHERE source.n IS DISTINCT FROM output.n
       OR source.units IS DISTINCT FROM output.units
       OR source.cents IS DISTINCT FROM output.cents
) AS annual_conservation_ok;

SELECT NOT EXISTS (
    SELECT 1 FROM silver_sales_v s ANTI JOIN silver_week_dimension_v c USING(week)
) AND (SELECT count(*) FROM silver_sales_v) = (SELECT count(*) FROM bronze_sales)
AS sales_projection_ok;
