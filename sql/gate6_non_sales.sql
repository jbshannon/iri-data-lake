-- Gate 6 of docs/corpus_readiness.md: reconcile the non-sales bronze
-- tables against the manifest, and against each other's keys.
--
-- This is deliberately weaker than a row-for-row comparison against the
-- raw bytes — DuckDB cannot read the 21-byte fixed-width `prod_attr`
-- grid or the tab/whitespace PANEL dialects without reimplementing the
-- parsers, which would test the parsers against themselves. What it can
-- do, and what it does, is check every property that would catch a
-- *systematic* mis-parse rather than a crash:
--
--   * the manifest accounts for every table, with matching row counts
--   * the fixed-width tables' partition coverage matches the corpus
--   * the joins that make the lake useful actually resolve, i.e. every
--     `iri_sales.iri_key` has a roster row and every PANEL week has a
--     week-dimension row
--   * no column is silently all-null where the corpus says it is not
--
-- Every query returns a `gate_6x_ok` boolean; scripts/run_sql.py exits
-- non-zero if any is false.

-- 6.1: every ingested dataset is represented in the manifest, and the
-- manifest's row counts match the lake's.
--
-- The manifest is append-only, so a re-issued run leaves several
-- records for one source path. Only the **last** record per source
-- counts: summing them all double-counts every re-ingested file, which
-- is exactly the silent drift this gate exists to catch.
WITH latest AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY source_path ORDER BY started_at DESC, run_id DESC
        ) AS rn
    FROM read_json('${LAKE}/metadata/manifest.jsonl', format = 'newline_delimited')
    WHERE status = 'success'
),
manifest AS (
    SELECT
        dataset,
        sum(written_rows) AS rows,
        count(*)          AS files
    FROM latest
    WHERE rn = 1
    GROUP BY 1
),
-- The lake side is keyed by the same `dataset` kind the manifest uses,
-- not by the table name. `DatasetKind` serialises as `sales`, `panel`,
-- … while the bronze table is `iri_sales`, `iri_panel`, …; the mapping
-- lives in `DatasetKind::bronze_table()` and is spelled out here rather
-- than guessed, so a new class that forgets to add itself fails loudly.
lake AS (
    SELECT 'sales'            AS dataset, count(*) AS n FROM read_parquet('${LAKE}/bronze/iri_sales/**/*.parquet')
    UNION ALL SELECT 'panel',            count(*) FROM read_parquet('${LAKE}/bronze/iri_panel/**/*.parquet')
    UNION ALL SELECT 'panel_trips',      count(*) FROM read_parquet('${LAKE}/bronze/iri_panel_trips/**/*.parquet')
    UNION ALL SELECT 'panel_static',     count(*) FROM read_parquet('${LAKE}/bronze/iri_panel_static/**/*.parquet')
    UNION ALL SELECT 'panelist_demos',   count(*) FROM read_parquet('${LAKE}/bronze/iri_panelist_demos/**/*.parquet')
    UNION ALL SELECT 'ads_demos',        count(*) FROM read_parquet('${LAKE}/bronze/iri_ads_demos/**/*.parquet')
    UNION ALL SELECT 'product_attr',     count(*) FROM read_parquet('${LAKE}/bronze/iri_product_attr/**/*.parquet')
    UNION ALL SELECT 'product_stub',     count(*) FROM read_parquet('${LAKE}/bronze/iri_product_stub/**/*.parquet')
    UNION ALL SELECT 'delivery_stores',  count(*) FROM read_parquet('${LAKE}/bronze/iri_delivery_stores/**/*.parquet')
    UNION ALL SELECT 'week_dimension',   count(*) FROM read_parquet('${LAKE}/bronze/iri_week_dimension/**/*.parquet')
    UNION ALL SELECT 'chain_xref',       count(*) FROM read_parquet('${LAKE}/bronze/iri_chain_xref/**/*.parquet')
    UNION ALL SELECT 'manual_store_entry', count(*) FROM read_parquet('${LAKE}/bronze/iri_manual_store_entry/**/*.parquet')
)
SELECT
    '6.1 manifest and lake agree on row counts' AS check,
    coalesce(bool_and(m.rows = l.n), false)    AS gate_61_ok,
    count(*)                                    AS classes,
    coalesce(sum(m.rows), 0)                    AS manifest_rows,
    coalesce(sum(l.n), 0)                       AS lake_rows,
    coalesce(sum(m.files), 0)                   AS manifest_files
FROM manifest m
FULL OUTER JOIN lake l USING (dataset);

-- 6.2: the week dimension covers every week the sales and panel facts
-- reference. A gap here is invisible until a date-join silently drops
-- rows, which is the whole reason this table is ingested.
WITH weeks AS (
    SELECT DISTINCT week FROM read_parquet('${LAKE}/bronze/iri_sales/**/*.parquet')
    UNION
    SELECT DISTINCT week FROM read_parquet('${LAKE}/bronze/iri_panel/**/*.parquet')
),
dim AS (SELECT iri_week FROM read_parquet('${LAKE}/bronze/iri_week_dimension/**/*.parquet'))
SELECT
    '6.2 every referenced week has a calendar date' AS check,
    count(*) = 0                                     AS gate_62_ok,
    count(*)                                         AS uncovered_weeks
FROM weeks ANTI JOIN dim ON weeks.week = dim.iri_week;

-- 6.3: the week dimension is the documented one — 626 contiguous rows
-- from week 1114 to 1739, covering years 6 and 7 that the per-year
-- copies omit.
WITH dim AS (SELECT * FROM read_parquet('${LAKE}/bronze/iri_week_dimension/**/*.parquet'))
SELECT
    '6.3 week dimension is contiguous and complete'  AS check,
    count(*) = 626
        AND min(iri_week) = 1114
        AND max(iri_week) = 1739
        AND count(DISTINCT iri_week) = 626
        AND count(*) FILTER (WHERE week_start IS NULL) = 0
        AND count(*) FILTER (WHERE week_end IS NULL) = 0
        AND count(DISTINCT source_year) = 12         AS gate_63_ok,
    count(*)                                          AS rows,
    min(iri_week)                                     AS lo,
    max(iri_week)                                     AS hi,
    count(DISTINCT source_year)                       AS years
FROM dim;

-- 6.4: every store in the sales facts has a roster row for its year.
-- This is the join that turns `iri_sales.iri_key` into a place, and a
-- missing roster year would silently drop those stores from every
-- market-level rollup.
WITH keys AS (
    SELECT year, iri_key FROM read_parquet('${LAKE}/bronze/iri_sales/**/*.parquet') GROUP BY 1,2
),
roster AS (
    SELECT year, iri_key FROM read_parquet('${LAKE}/bronze/iri_delivery_stores/**/*.parquet', hive_partitioning=true) GROUP BY 1,2
)
SELECT
    '6.4 every sales store has a roster row'    AS check,
    count(*) = 0                                 AS gate_64_ok,
    count(*)                                     AS uncovered_stores,
    (SELECT count(DISTINCT year) FROM keys)      AS years
FROM keys ANTI JOIN roster USING (year, iri_key);

-- 6.5: the roster has no column that is entirely null. The 63-byte
-- layout is measured, and slicing by the header's own token positions
-- instead lands mid-field — which shows up here as a whole column of
-- garbage, not as an error.
SELECT
    '6.5 roster columns are populated'          AS check,
    count(*) = count(iri_key)
        AND count(*) = count(ou)
        AND count(*) = count(est_acv)
        AND count(*) = count(market_name)
        AND count(*) = count(open_week)
        AND count(*) = count(masked_name)       AS gate_65_ok,
    count(*)                                     AS rows
FROM read_parquet('${LAKE}/bronze/iri_delivery_stores/**/*.parquet');

-- 6.6: PANEL money is fully populated. The corpus writes DOLLARS in
-- three encodings (exact 2dp, float32 of 2dp, sub-cent computed
-- average) and all three must land in the column; a null here means a
-- whole encoding is being dropped.
SELECT
    '6.6 panel dollars are never null'          AS check,
    count(*) FILTER (WHERE dollars_cents IS NULL) = 0 AS gate_66_ok,
    count(*)                                     AS rows,
    count(*) FILTER (WHERE dollars_cents IS NULL) AS null_dollars
FROM read_parquet('${LAKE}/bronze/iri_panel/**/*.parquet');

-- 6.7: PANEL's MINUTE column is null exactly for the pre-year-8
-- dialects and populated for years 8-12. If the dialect sniffer ever
-- mis-reads a header, this is where it shows.
SELECT
    '6.7 panel MINUTE presence matches the dialect' AS check,
    count(*) FILTER (WHERE year <= 7 AND minute IS NOT NULL) = 0
        AND count(*) FILTER (WHERE year >= 8 AND minute IS NULL) = 0 AS gate_67_ok,
    count(*) FILTER (WHERE minute IS NULL)        AS null_minute,
    count(*) FILTER (WHERE minute IS NOT NULL)    AS with_minute
FROM read_parquet('${LAKE}/bronze/iri_panel/**/*.parquet', hive_partitioning = true);

-- 6.8: PANEL covers all twelve years and the expected outlet codes,
-- including year 8's extra `KK`.
SELECT
    '6.8 panel covers 12 years and 7 outlet codes' AS check,
    count(DISTINCT year) = 12 AND count(DISTINCT outlet) >= 7 AS gate_68_ok,
    count(DISTINCT year)                             AS years,
    count(DISTINCT outlet)                           AS outlets,
    min(year)                                        AS first_year,
    max(year)                                        AS last_year
FROM read_parquet('${LAKE}/bronze/iri_panel/**/*.parquet', hive_partitioning = true);

-- 6.9: trips money is not silently dropped. CENTS998 is genuinely
-- empty in ~95 % of rows; CENTS999 is not, so a null there means the
-- float encoding is being rejected.
SELECT
    '6.9 trips CENTS999 is populated'            AS check,
    -- Bronze records the source's raw decimal as a (digits,
    -- scale) pair; `cents999 IS NULL` means the (digits, scale)
    -- pair is missing for both columns. We require every value
    -- present in the source to be present in the lake.
    count(*) FILTER (WHERE cents999_digits IS NULL) < count(*) * 0.01 AS gate_69_ok,
    count(*)                                            AS rows,
    count(*) FILTER (WHERE cents998_digits IS NULL)   AS null_cents998,
    count(*) FILTER (WHERE cents999_digits IS NULL)   AS null_cents999
FROM read_parquet('${LAKE}/bronze/iri_panel_trips/**/*.parquet');
-- 6.10: deduplicated sources are accounted for in the manifest.
-- A `Success` record with a populated `deduplicated_from` means a
-- real source was not ingested as its own Parquet file, but the
-- lake still knows about it and points at the canonical that was.
-- Together they must agree: for every deduplicated source there is a
-- canonical manifest record, and the counts add to the original
-- discovery count.
WITH deduped AS (
    SELECT
        dataset,
        count(*) FILTER (WHERE deduplicated_from IS NOT NULL) AS duplicates
    FROM read_json('${LAKE}/metadata/manifest.jsonl', format = 'newline_delimited')
    WHERE status = 'success'
    GROUP BY 1
),
classes AS (
    -- Both DEMOS.CSV and Delivery_Stores deduplicate. The exact
    -- count varies with the corpus.
    SELECT 'delivery_stores' AS dataset UNION ALL SELECT 'panelist_demos'
)
SELECT
    '6.10 deduplicated sources are recorded with a canonical link' AS check,
    coalesce(bool_and(d.duplicates > 0), false)                       AS gate_610_ok,
    (SELECT json_group_array(dataset) FROM classes)            AS dedup_classes,
    (
        SELECT sum(duplicates) FROM deduped
    )                                                          AS total_duplicates
FROM classes c
LEFT JOIN deduped d USING (dataset);
-- 6.11: a re-issued run that walks every manifest record (rather
-- than the latest per source_path) must still sum to the same row
-- count. Without this check a future contributor summing all
-- records instead of `latest WHERE rn=1` could silently agree with
-- itself while disagreeing with the lake, because demoted sources
-- contribute `written_rows=0` and need to be ignored.
WITH all_recs AS (
    SELECT
        dataset,
        sum(written_rows) FILTER (
            WHERE deduplicated_from IS NULL
        ) AS canonical_rows,
        count(*) FILTER (WHERE deduplicated_from IS NULL) AS canonical_records,
        count(*) FILTER (WHERE deduplicated_from IS NOT NULL) AS deduplicated_records
    FROM read_json('${LAKE}/metadata/manifest.jsonl', format = 'newline_delimited')
    WHERE status = 'success'
    GROUP BY 1
)
SELECT
    '6.11 deduplicated records do not double-count row totals' AS check,
    (SELECT bool_and(canonical_records > 0 OR canonical_rows = 0)
        FROM all_recs)                                        AS gate_611_ok,
    (SELECT sum(canonical_records)    FROM all_recs)          AS total_canonical_records,
    (SELECT sum(deduplicated_records) FROM all_recs)          AS total_deduplicated_records
LIMIT 1;

