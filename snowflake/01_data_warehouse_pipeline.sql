/* =====================================================================
   01_data_warehouse_pipeline.sql
   =====================================================================
   Dubai Real Estate — One-Time Baseline DDL
   
   Creates the DUBAI_REAL_ESTATE database, RAW staging schema,
   DW (data warehouse) schema, five dimension tables, and the
   FACT_TRANSACTIONS fact table in a Kimball star-schema design.
   
   Source: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
          (loaded from DLD government CSV exports via the Python
           incremental loader in automation/load_new_dld_data.py)
   ===================================================================== */


-- ─── Foundation ────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS DUBAI_REAL_ESTATE;
CREATE SCHEMA IF NOT EXISTS DUBAI_REAL_ESTATE.RAW;


/* =====================================================================
   Star-Schema Design Rationale
   ---------------------------------------------------------------------
   Dimension              Built from                                    Why
   ─────────────────────  ────────────────────────────────────────────  ───────────────────────────────────────
   DIM_AREA               AREA_EN (272 values)                         Classic geography dimension
   DIM_PROPERTY_TYPE      PROP_TYPE_EN + PROP_SB_TYPE_EN               Subtype only makes sense attached to its parent type
   DIM_TRANSACTION_TYPE   GROUP_EN, PROCEDURE_EN, IS_OFFPLAN_EN,       These 5 together describe
                          IS_FREE_HOLD_EN, USAGE_EN                    "what kind of deal this was"
   DIM_ROOMS              ROOMS_EN                                     Simple category dimension
   DIM_PROJECT            PROJECT_EN                                   Project name

   Two deliberate exclusions — real DA judgment calls, not oversights:

   NEAREST_METRO_EN, NEAREST_MALL_EN, NEAREST_LANDMARK_EN — 50–70% null
   across the board, dropped. A column that's mostly empty makes a weak,
   unreliable dimension.

   MASTER_PROJECT_EN — 99.6% null (only 503 of 142,112 rows have it).
   Dropped entirely.

   PARKING — kept, but not as a dimension. Its values are near-unique
   free text (spot numbers like P-132, G-11, G-12), not a real category.
   This is called a degenerate dimension — it stays as a plain text
   column inside the Fact table itself instead of getting its own table.
   ===================================================================== */


--- DATA WAREHOUSE STRUCTURE ----
CREATE SCHEMA IF NOT EXISTS DUBAI_REAL_ESTATE.DW;

/* =====================================================================
   DIM_AREA
   ---------------------------------------------------------------------
   Purpose......: Geography dimension — the community/area a property
                   sits in (e.g. Business Bay, Palm Deira).
   Grain........: One row per distinct AREA_EN value.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Cleaning.....: None needed — profiling confirmed 0 nulls in AREA_EN.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_AREA AS
WITH distinct_areas AS (
    SELECT DISTINCT AREA_EN
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT ROW_NUMBER() OVER (ORDER BY AREA_EN) AS AREA_ID,
       AREA_EN AS AREA_NAME
FROM distinct_areas;


/* =====================================================================
   DIM_PROPERTY_TYPE
   ---------------------------------------------------------------------
   Purpose......: What kind of property this is — top-level type
                   (Unit/Building/Land) plus its subtype (Flat, Shop,
                   Villa, Office, etc).
   Grain........: One row per distinct (PROP_TYPE_EN, PROP_SB_TYPE_EN)
                   combination — subtype only makes sense attached to
                   its parent type.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Cleaning.....: PROP_SB_TYPE_EN had 1,812 blank cells (source loaded
                   as '' not NULL via Snowflake's CSV wizard) —
                   NULLIF(TRIM(...), '') converts them to true NULL
                   before COALESCE can catch them.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_PROPERTY_TYPE AS
WITH distinct_types AS (
    SELECT DISTINCT PROP_TYPE_EN, PROP_SB_TYPE_EN
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT ROW_NUMBER() OVER (ORDER BY PROP_TYPE_EN, PROP_SB_TYPE_EN) AS PROPERTY_TYPE_ID,
       PROP_TYPE_EN AS PROPERTY_TYPE,
       COALESCE(NULLIF(TRIM(PROP_SB_TYPE_EN), ''), 'Not Specified') AS PROPERTY_SUBTYPE
FROM distinct_types;


/* =====================================================================
   DIM_TRANSACTION_TYPE
   ---------------------------------------------------------------------
   Purpose......: Describes "what kind of deal this was" — sale vs
                   mortgage vs gift, off-plan vs ready, freehold status,
                   residential vs commercial usage.
   Grain........: One row per distinct combination of GROUP_EN,
                   PROCEDURE_EN, IS_OFFPLAN_EN, IS_FREE_HOLD_EN,
                   USAGE_EN — these 5 fields together define one
                   transaction "type", not any single field alone.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Cleaning.....: None needed — 0 nulls confirmed across all 5 fields
                   during profiling.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_TRANSACTION_TYPE AS
WITH distinct_txn_types AS (
    SELECT DISTINCT GROUP_EN, PROCEDURE_EN, IS_OFFPLAN_EN, IS_FREE_HOLD_EN, USAGE_EN
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT ROW_NUMBER() OVER (ORDER BY GROUP_EN, PROCEDURE_EN, IS_OFFPLAN_EN, IS_FREE_HOLD_EN, USAGE_EN) AS TRANSACTION_TYPE_ID,
       GROUP_EN AS TRANSACTION_GROUP,
       PROCEDURE_EN AS PROCEDURE_NAME,
       IS_OFFPLAN_EN AS IS_OFFPLAN,
       IS_FREE_HOLD_EN AS IS_FREEHOLD,
       USAGE_EN AS USAGE_TYPE
FROM distinct_txn_types;


/* =====================================================================
   DIM_ROOMS
   ---------------------------------------------------------------------
   Purpose......: Room configuration of the property (Studio, 1 B/R,
                   2 B/R, etc).
   Grain........: One row per distinct cleaned ROOMS_EN value.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Cleaning.....: Two DIFFERENT missing-data patterns found during
                   profiling, kept as separate categories (not merged)
                   because they have different root causes:
                     - True blank (16,561 rows, 99.97% property type
                       = Land): room count structurally doesn't apply
                       to raw land -> "Not Applicable"
                     - Literal text "NA" (3,613 rows, 99.8% property
                       type = Unit): units normally DO have a room
                       count, so this is a genuine data capture gap,
                       not a structural non-applicability
                       -> "Data Not Captured"
                   Note: pandas' default CSV reader silently treats
                   literal "NA" as null — this split was only visible
                   after re-profiling with keep_default_na=False.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_ROOMS AS
WITH distinct_rooms AS (
    SELECT DISTINCT
        CASE
            WHEN TRIM(COALESCE(ROOMS_EN, '')) = '' THEN 'Not Applicable'
            WHEN TRIM(ROOMS_EN) = 'NA' THEN 'Data Not Captured'
            ELSE TRIM(ROOMS_EN)
        END AS ROOM_TYPE
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT ROW_NUMBER() OVER (ORDER BY ROOM_TYPE) AS ROOM_ID,
       ROOM_TYPE
FROM distinct_rooms;


/* =====================================================================
   DIM_PROJECT
   ---------------------------------------------------------------------
   Purpose......: Named development/project a property belongs to
                   (e.g. "THE CRESTMARK").
   Grain........: One row per distinct project name.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Cleaning.....: PROJECT_EN had 18,153 blank cells -> NULLIF(TRIM..)
                   + COALESCE pattern, same as DIM_PROPERTY_TYPE.
   Excluded col.: MASTER_PROJECT_EN dropped from this model — 99.6%
                   null (only 503 of 142,112 rows populated), too
                   sparse to be a reliable attribute.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_PROJECT AS
WITH distinct_projects AS (
    SELECT DISTINCT PROJECT_EN
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT ROW_NUMBER() OVER (ORDER BY PROJECT_EN) AS PROJECT_ID,
       COALESCE(NULLIF(TRIM(PROJECT_EN), ''), 'Not Specified') AS PROJECT_NAME
FROM distinct_projects;


/* =====================================================================
   FACT_TRANSACTIONS
   ---------------------------------------------------------------------
   Grain........: One row = one property line-item within a DLD
                   transaction (NOT one transaction). A single
                   TRANSACTION_NUMBER can span multiple properties
                   (e.g. one bulk mortgage covering 3 units = 3 rows).
                   Confirmed during profiling: TRANSACTION_NUMBER had
                   4,478 duplicates in the source file.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Surrogate key: FACT_ID — TRANSACTION_NUMBER can't be a unique key
                   at this grain, so a generated key is required.
   Type safety..: All numeric/date casts use TRY_TO_* functions, not
                   raw CAST — one malformed row returns NULL instead
                   of failing the entire table build.
   ===================================================================== */

CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.INSTANCE_DATE) AS FACT_ID,

    -- Degenerate dimension: kept for traceability back to the source
    -- transaction; not used for grouping/filtering in reports.
    s.TRANSACTION_NUMBER,

    -- Dates arrived as text in staging (by design). TRY_TO_TIMESTAMP
    -- converts safely — a malformed date becomes NULL, not a failed build.
    TRY_TO_TIMESTAMP(s.INSTANCE_DATE, 'YYYY-MM-DD HH24:MI:SS') AS TRANSACTION_DATE,

    -- Core measures, cast from text now that we're past staging.
    TRY_TO_NUMBER(s.TRANS_VALUE)     AS TRANSACTION_VALUE_AED,
    TRY_TO_NUMBER(s.PROCEDURE_AREA)  AS PROCEDURE_AREA_SQM,
    TRY_TO_NUMBER(s.ACTUAL_AREA)     AS ACTUAL_AREA_SQM,
    TRY_TO_NUMBER(s.TOTAL_BUYER)     AS TOTAL_BUYERS,
    TRY_TO_NUMBER(s.TOTAL_SELLER)    AS TOTAL_SELLERS,

    -- Degenerate dimension: near-unique free text (spot numbers), not
    -- a real category. Same NA-literal cleanup found earlier in DIM_ROOMS.
    CASE
        WHEN TRIM(COALESCE(s.PARKING, '')) = '' THEN 'Not Applicable'
        WHEN TRIM(s.PARKING) IN ('NA','N/A') THEN 'Data Not Captured'
        ELSE TRIM(s.PARKING)
    END AS PARKING,

    -- Foreign keys. Each join condition replicates the EXACT cleaning
    -- logic used to build that dimension — mismatch here silently
    -- produces orphan rows with NULL foreign keys.
    area.AREA_ID,
    prop.PROPERTY_TYPE_ID,
    txn.TRANSACTION_TYPE_ID,
    room.ROOM_ID,
    proj.PROJECT_ID

FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING s

LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_AREA area
    ON s.AREA_EN = area.AREA_NAME

LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_PROPERTY_TYPE prop
    ON s.PROP_TYPE_EN = prop.PROPERTY_TYPE
    AND COALESCE(NULLIF(TRIM(s.PROP_SB_TYPE_EN), ''), 'Not Specified') = prop.PROPERTY_SUBTYPE

LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_TRANSACTION_TYPE txn
    ON s.GROUP_EN = txn.TRANSACTION_GROUP
    AND s.PROCEDURE_EN = txn.PROCEDURE_NAME
    AND s.IS_OFFPLAN_EN = txn.IS_OFFPLAN
    AND s.IS_FREE_HOLD_EN = txn.IS_FREEHOLD
    AND s.USAGE_EN = txn.USAGE_TYPE

LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_ROOMS room
    ON CASE
           WHEN TRIM(COALESCE(s.ROOMS_EN, '')) = '' THEN 'Not Applicable'
           WHEN TRIM(s.ROOMS_EN) = 'NA' THEN 'Data Not Captured'
           ELSE TRIM(s.ROOMS_EN)
       END = room.ROOM_TYPE

LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_PROJECT proj
    ON COALESCE(NULLIF(TRIM(s.PROJECT_EN), ''), 'Not Specified') = proj.PROJECT_NAME;