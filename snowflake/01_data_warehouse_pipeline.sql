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

   Key strategy: every surrogate key below is a deterministic
   HASH(UPPER(TRIM(...))) of the natural business value — not a
   ROW_NUMBER() sequence. This is deliberate: 02_orchestration_and_
   automation.sql refreshes these same tables incrementally, and an
   incremental MERGE can only recognize "this row already exists"
   if the same input always produces the same key, on this baseline
   load and on every future incremental run alike. A ROW_NUMBER()
   key would be baseline-load-order-dependent and impossible to
   reproduce inside a MERGE, which is why the two scripts must share
   one key-generation formula, not two.
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

   Note on text casing: every dimension label below is stored as
   UPPER(TRIM(...)) — not the source's original casing. This is the
   same normalization policy documented in detail in
   02_orchestration_and_automation.sql (profiling found the same
   source value appearing in multiple capitalizations — e.g. "Business
   Bay" and "BUSINESS BAY" — which would otherwise silently create
   duplicate dimension rows). Storing the canonical form here means
   the Power BI layer can apply its own display-casing (e.g. Proper
   Case via a Power Query step) without that formatting choice leaking
   into the warehouse's matching logic.
   ===================================================================== */


--- DATA WAREHOUSE STRUCTURE ----
CREATE SCHEMA IF NOT EXISTS DUBAI_REAL_ESTATE.DW;

/* =====================================================================
   DIM_AREA
   ---------------------------------------------------------------------
   Purpose......: Geography dimension — the community/area a property
                   sits in (e.g. Business Bay, Palm Deira).
   Grain........: One row per distinct normalized AREA_EN value.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Cleaning.....: UPPER(TRIM(...)) applied — profiling found the same
                   area name in multiple capitalizations (5 areas
                   affected, e.g. "Business Bay" / "BUSINESS BAY").
   Key..........: HASH(AREA_NAME) — deterministic, matches the
                   incremental MERGE key in 02_orchestration_and_
                   automation.sql exactly.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_AREA AS
WITH distinct_areas AS (
    SELECT DISTINCT UPPER(TRIM(AREA_EN)) AS AREA_EN
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT HASH(AREA_EN) AS AREA_ID,
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
                   before COALESCE can catch them. UPPER(TRIM()) applied
                   throughout per the universal normalization policy.
   Key..........: HASH(PROPERTY_TYPE, PROPERTY_SUBTYPE) — deterministic,
                   matches 02_orchestration_and_automation.sql exactly.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_PROPERTY_TYPE AS
WITH distinct_types AS (
    SELECT DISTINCT
        UPPER(TRIM(PROP_TYPE_EN)) AS PROP_TYPE_EN,
        UPPER(COALESCE(NULLIF(TRIM(PROP_SB_TYPE_EN), ''), 'Not Specified')) AS PROPERTY_SUBTYPE
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT HASH(PROP_TYPE_EN, PROPERTY_SUBTYPE) AS PROPERTY_TYPE_ID,
       PROP_TYPE_EN AS PROPERTY_TYPE,
       PROPERTY_SUBTYPE
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
   Cleaning.....: 0 nulls confirmed across all 5 fields during profiling;
                   UPPER(TRIM()) applied anyway per the universal policy.
   Key..........: HASH() of all 5 normalized fields — deterministic,
                   matches 02_orchestration_and_automation.sql exactly.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_TRANSACTION_TYPE AS
WITH distinct_txn_types AS (
    SELECT DISTINCT
        UPPER(TRIM(GROUP_EN))        AS GROUP_EN,
        UPPER(TRIM(PROCEDURE_EN))    AS PROCEDURE_EN,
        UPPER(TRIM(IS_OFFPLAN_EN))   AS IS_OFFPLAN_EN,
        UPPER(TRIM(IS_FREE_HOLD_EN)) AS IS_FREE_HOLD_EN,
        UPPER(TRIM(USAGE_EN))        AS USAGE_EN
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT HASH(GROUP_EN, PROCEDURE_EN, IS_OFFPLAN_EN, IS_FREE_HOLD_EN, USAGE_EN) AS TRANSACTION_TYPE_ID,
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
   Grain........: One row per distinct cleaned, normalized ROOMS_EN value.
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
   Key..........: HASH(ROOM_TYPE) — deterministic, matches
                   02_orchestration_and_automation.sql exactly.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_ROOMS AS
WITH distinct_rooms AS (
    SELECT DISTINCT
        UPPER(
            CASE
                WHEN TRIM(COALESCE(ROOMS_EN, '')) = '' THEN 'Not Applicable'
                WHEN UPPER(TRIM(ROOMS_EN)) = 'NA'        THEN 'Data Not Captured'
                ELSE TRIM(ROOMS_EN)
            END
        ) AS ROOM_TYPE
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT HASH(ROOM_TYPE) AS ROOM_ID,
       ROOM_TYPE
FROM distinct_rooms;


/* =====================================================================
   DIM_PROJECT
   ---------------------------------------------------------------------
   Purpose......: Named development/project a property belongs to
                   (e.g. "THE CRESTMARK").
   Grain........: One row per distinct normalized project name.
   Source.......: DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
   Cleaning.....: PROJECT_EN had 18,153 blank cells -> NULLIF(TRIM..)
                   + COALESCE pattern, same as DIM_PROPERTY_TYPE.
                   UPPER() applied — profiling found the same project
                   under multiple capitalizations (2 projects affected,
                   e.g. "Crystal Tower" / "CRYSTAL TOWER").
   Excluded col.: MASTER_PROJECT_EN dropped from this model — 99.6%
                   null (only 503 of 142,112 rows populated), too
                   sparse to be a reliable attribute.
   Key..........: HASH(PROJECT_NAME) — deterministic, matches
                   02_orchestration_and_automation.sql exactly.
   ===================================================================== */
CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.DIM_PROJECT AS
WITH distinct_projects AS (
    SELECT DISTINCT UPPER(COALESCE(NULLIF(TRIM(PROJECT_EN), ''), 'Not Specified')) AS PROJECT_NAME
    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
)
SELECT HASH(PROJECT_NAME) AS PROJECT_ID,
       PROJECT_NAME
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
   Surrogate key: FACT_ID — an AUTOINCREMENT identity column, not a
                   computed value. TRANSACTION_NUMBER can't be a unique
                   key at this grain, and unlike the dimension keys
                   above, the fact row itself never needs to be looked
                   up by a deterministic key — only SOURCE_ROW_HASH
                   does, for incremental dedup (see below). Declaring
                   FACT_ID as IDENTITY lets both this baseline load and
                   02_orchestration_and_automation.sql's incremental
                   INSERT share one table definition without either
                   script needing to supply or compute FACT_ID itself.
   Fingerprint..: SOURCE_ROW_HASH covers every retained business column
                   via HASH(), with every TEXT input normalized through
                   UPPER(TRIM()) first — without that, the same real
                   transaction re-submitted in a future file with
                   different capitalization would produce a different
                   hash and get wrongly inserted as a duplicate row.
                   This is what 02_orchestration_and_automation.sql's
                   incremental MERGE keys on to decide "already loaded."
   Dedup........: QUALIFY collapses true duplicate rows arriving in the
                   same source batch — a plain INSERT alone doesn't
                   catch that, only rows already present in the target.
   Type safety..: All numeric/date casts use TRY_TO_* functions, not
                   raw CAST — one malformed row returns NULL instead
                   of failing the entire table build.
   ===================================================================== */

CREATE OR REPLACE TABLE DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS (
    FACT_ID                 NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    TRANSACTION_NUMBER      STRING,
    TRANSACTION_DATE        TIMESTAMP_NTZ,
    TRANSACTION_VALUE_AED   NUMBER,
    PROCEDURE_AREA_SQM      NUMBER,
    ACTUAL_AREA_SQM         NUMBER,
    TOTAL_BUYERS            NUMBER,
    TOTAL_SELLERS           NUMBER,
    PARKING                 STRING,
    AREA_ID                 NUMBER,
    PROPERTY_TYPE_ID        NUMBER,
    TRANSACTION_TYPE_ID     NUMBER,
    ROOM_ID                 NUMBER,
    PROJECT_ID              NUMBER,
    SOURCE_ROW_HASH         NUMBER
);

INSERT INTO DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS (
    TRANSACTION_NUMBER, TRANSACTION_DATE, TRANSACTION_VALUE_AED, PROCEDURE_AREA_SQM,
    ACTUAL_AREA_SQM, TOTAL_BUYERS, TOTAL_SELLERS, PARKING, AREA_ID, PROPERTY_TYPE_ID,
    TRANSACTION_TYPE_ID, ROOM_ID, PROJECT_ID, SOURCE_ROW_HASH
)
SELECT * FROM (
    SELECT
        -- Dates arrived as text in staging (by design). TRY_TO_TIMESTAMP
        -- converts safely — a malformed date becomes NULL, not a failed build.
        TRIM(s.TRANSACTION_NUMBER) AS TRANSACTION_NUMBER,
        TRY_TO_TIMESTAMP(s.INSTANCE_DATE, 'YYYY-MM-DD HH24:MI:SS') AS TRANSACTION_DATE,

        -- Core measures, cast from text now that we're past staging.
        TRY_TO_NUMBER(s.TRANS_VALUE)     AS TRANSACTION_VALUE_AED,
        TRY_TO_NUMBER(s.PROCEDURE_AREA)  AS PROCEDURE_AREA_SQM,
        TRY_TO_NUMBER(s.ACTUAL_AREA)     AS ACTUAL_AREA_SQM,
        TRY_TO_NUMBER(s.TOTAL_BUYER)     AS TOTAL_BUYERS,
        TRY_TO_NUMBER(s.TOTAL_SELLER)    AS TOTAL_SELLERS,

        -- Degenerate dimension: near-unique free text (spot numbers), not
        -- a real category. Same NA-literal cleanup found earlier in DIM_ROOMS.
        UPPER(
            CASE
                WHEN TRIM(COALESCE(s.PARKING, '')) = '' THEN 'Not Applicable'
                WHEN UPPER(TRIM(s.PARKING)) IN ('NA','N/A') THEN 'Data Not Captured'
                ELSE TRIM(s.PARKING)
            END
        ) AS PARKING,

        -- Foreign keys. Each join condition replicates the EXACT cleaning
        -- + normalization logic used to build that dimension — mismatch
        -- here silently produces orphan rows with NULL foreign keys.
        area.AREA_ID, prop.PROPERTY_TYPE_ID, txn.TRANSACTION_TYPE_ID, room.ROOM_ID, proj.PROJECT_ID,

        -- Fingerprint for incremental dedup — identical formula to the one
        -- 02_orchestration_and_automation.sql uses on every future refresh.
        HASH(TRIM(s.TRANSACTION_NUMBER), s.INSTANCE_DATE,
             UPPER(TRIM(s.GROUP_EN)), UPPER(TRIM(s.PROCEDURE_EN)), UPPER(TRIM(s.IS_OFFPLAN_EN)),
             UPPER(TRIM(s.IS_FREE_HOLD_EN)), UPPER(TRIM(s.USAGE_EN)), UPPER(TRIM(s.AREA_EN)),
             s.TRANS_VALUE, s.PROCEDURE_AREA, s.ACTUAL_AREA, UPPER(TRIM(s.ROOMS_EN)),
             UPPER(TRIM(s.PARKING)), s.TOTAL_BUYER, s.TOTAL_SELLER, UPPER(TRIM(s.PROP_TYPE_EN)),
             UPPER(TRIM(s.PROP_SB_TYPE_EN)), UPPER(TRIM(s.PROJECT_EN))) AS SOURCE_ROW_HASH

    FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING s

    LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_AREA area
        ON UPPER(TRIM(s.AREA_EN)) = area.AREA_NAME

    LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_PROPERTY_TYPE prop
        ON UPPER(TRIM(s.PROP_TYPE_EN)) = prop.PROPERTY_TYPE
        AND UPPER(COALESCE(NULLIF(TRIM(s.PROP_SB_TYPE_EN), ''), 'Not Specified')) = prop.PROPERTY_SUBTYPE

    LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_TRANSACTION_TYPE txn
        ON UPPER(TRIM(s.GROUP_EN)) = txn.TRANSACTION_GROUP
        AND UPPER(TRIM(s.PROCEDURE_EN)) = txn.PROCEDURE_NAME
        AND UPPER(TRIM(s.IS_OFFPLAN_EN)) = txn.IS_OFFPLAN
        AND UPPER(TRIM(s.IS_FREE_HOLD_EN)) = txn.IS_FREEHOLD
        AND UPPER(TRIM(s.USAGE_EN)) = txn.USAGE_TYPE

    LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_ROOMS room
        ON UPPER(
               CASE
                   WHEN TRIM(COALESCE(s.ROOMS_EN, '')) = '' THEN 'Not Applicable'
                   WHEN UPPER(TRIM(s.ROOMS_EN)) = 'NA'        THEN 'Data Not Captured'
                   ELSE TRIM(s.ROOMS_EN)
               END
           ) = room.ROOM_TYPE

    LEFT JOIN DUBAI_REAL_ESTATE.DW.DIM_PROJECT proj
        ON UPPER(COALESCE(NULLIF(TRIM(s.PROJECT_EN), ''), 'Not Specified')) = proj.PROJECT_NAME
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_ROW_HASH ORDER BY TRANSACTION_NUMBER) = 1;
