/* =====================================================================
   Exploratory & Validation Queries
   ---------------------------------------------------------------------
   Ad-hoc queries used during development to profile the raw DLD data,
   verify dimension builds, and validate the data warehouse structure.

   Every specific data-quality number quoted in 01_data_warehouse_
   pipeline.sql's comments and in the main README (the 50-70% null
   rate on NEAREST_METRO/MALL/LANDMARK, 99.6% null on MASTER_PROJECT_EN,
   the 4,478 duplicate TRANSACTION_NUMBERs, etc.) is reproducible by
   running the queries below against RAW.TRANSACTIONS_STAGING -- this
   file is that evidence, not just a narrated conclusion.
   ===================================================================== */

USE DATABASE DUBAI_REAL_ESTATE;

-- Quick schema inventory
SHOW SCHEMAS IN DATABASE DUBAI_REAL_ESTATE;

-- Raw staging row count (baseline for every % figure below)
SELECT COUNT(*) AS TOTAL_ROWS
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING;

-- Sample raw records
SELECT * FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING LIMIT 10;


/* ---------------------------------------------------------------------
   1. Null-rate profiling for the three excluded location columns
   Source of the "50-70% null" claim used to justify dropping
   NEAREST_METRO_EN / NEAREST_MALL_EN / NEAREST_LANDMARK_EN.
   --------------------------------------------------------------------- */
SELECT
    COUNT(*)                                                          AS TOTAL_ROWS,
    COUNT(NEAREST_METRO_EN)                                           AS METRO_POPULATED,
    ROUND(100 - COUNT(NEAREST_METRO_EN)    / COUNT(*) * 100, 2)       AS METRO_NULL_PCT,
    COUNT(NEAREST_MALL_EN)                                            AS MALL_POPULATED,
    ROUND(100 - COUNT(NEAREST_MALL_EN)     / COUNT(*) * 100, 2)       AS MALL_NULL_PCT,
    COUNT(NEAREST_LANDMARK_EN)                                        AS LANDMARK_POPULATED,
    ROUND(100 - COUNT(NEAREST_LANDMARK_EN) / COUNT(*) * 100, 2)       AS LANDMARK_NULL_PCT
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING;

-- 2. MASTER_PROJECT_EN — source of the "99.6% null / 503 of 142,112" claim
SELECT
    COUNT(*)                                                    AS TOTAL_ROWS,
    COUNT(MASTER_PROJECT_EN)                                    AS MASTER_PROJECT_POPULATED,
    ROUND(100 - COUNT(MASTER_PROJECT_EN) / COUNT(*) * 100, 2)   AS MASTER_PROJECT_NULL_PCT
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING;

-- 3. PROP_SB_TYPE_EN blank-cell count — source of the "1,812 blank cells" claim
SELECT COUNT(*) AS PROP_SB_TYPE_BLANK_COUNT
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
WHERE TRIM(COALESCE(PROP_SB_TYPE_EN, '')) = '';

-- 4. PROJECT_EN blank-cell count — source of the "18,153 blank cells" claim
SELECT COUNT(*) AS PROJECT_EN_BLANK_COUNT
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
WHERE TRIM(COALESCE(PROJECT_EN, '')) = '';

-- 5. DIM_TRANSACTION_TYPE source columns — confirm 0 nulls across all 5
SELECT
    COUNT(*) - COUNT(GROUP_EN)        AS GROUP_EN_NULLS,
    COUNT(*) - COUNT(PROCEDURE_EN)    AS PROCEDURE_EN_NULLS,
    COUNT(*) - COUNT(IS_OFFPLAN_EN)   AS IS_OFFPLAN_EN_NULLS,
    COUNT(*) - COUNT(IS_FREE_HOLD_EN) AS IS_FREE_HOLD_EN_NULLS,
    COUNT(*) - COUNT(USAGE_EN)        AS USAGE_EN_NULLS
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING;

-- 6. AREA_EN — confirm 0 nulls
SELECT COUNT(*) - COUNT(AREA_EN) AS AREA_EN_NULLS
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING;

/* ---------------------------------------------------------------------
   7. ROOMS_EN blank-vs-literal-"NA" split — source of the
   16,561 blank / 3,613 literal-"NA" claim, and the property-type
   correlation used to justify treating them as two different root
   causes ("Not Applicable" vs "Data Not Captured").
   --------------------------------------------------------------------- */
SELECT
    CASE
        WHEN TRIM(COALESCE(ROOMS_EN, '')) = '' THEN 'True Blank'
        WHEN TRIM(ROOMS_EN) = 'NA'             THEN 'Literal NA'
        ELSE 'Populated'
    END AS ROOMS_EN_PATTERN,
    COUNT(*)                                                        AS ROW_COUNT,
    COUNT(CASE WHEN PROP_TYPE_EN = 'Land' THEN 1 END)               AS OF_WHICH_LAND,
    COUNT(CASE WHEN PROP_TYPE_EN = 'Unit' THEN 1 END)               AS OF_WHICH_UNIT
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
GROUP BY 1
ORDER BY ROW_COUNT DESC;

-- 8. TRANSACTION_NUMBER duplicates — source of the "4,478 duplicates" claim
-- (expected and by design: one TRANSACTION_NUMBER can span multiple
-- property line-items, which is exactly why FACT_TRANSACTIONS grains
-- on line-item, not transaction, and needs its own surrogate FACT_ID).
SELECT COUNT(*) AS DUPLICATE_TRANSACTION_NUMBER_COUNT
FROM (
    SELECT TRANSACTION_NUMBER
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
    GROUP BY TRANSACTION_NUMBER
    HAVING COUNT(*) > 1
);

/* ---------------------------------------------------------------------
   9. Case-duplicate detection — same real-world value appearing under
   more than one capitalization in the source file. Source of the
   "5 areas affected" / "2 projects affected" claims documented in
   02_orchestration_and_automation.sql's text-normalization policy.
   --------------------------------------------------------------------- */
SELECT UPPER(TRIM(AREA_EN)) AS NORMALIZED_AREA, COUNT(DISTINCT AREA_EN) AS CASING_VARIANTS
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
GROUP BY 1
HAVING COUNT(DISTINCT AREA_EN) > 1;

SELECT UPPER(COALESCE(NULLIF(TRIM(PROJECT_EN), ''), 'Not Specified')) AS NORMALIZED_PROJECT,
       COUNT(DISTINCT PROJECT_EN) AS CASING_VARIANTS
FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
GROUP BY 1
HAVING COUNT(DISTINCT PROJECT_EN) > 1;

-- 10. Post-build sanity check: verify DIM_ROOMS collapsed correctly
USE SCHEMA DW;
SELECT * FROM DIM_ROOMS;
