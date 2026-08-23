/* ================================================================================
   PROJECT   : Dubai Real Estate Analytics — Enterprise BI Pipeline (Portfolio)
   LAYER     : Orchestration & Automation (Snowflake native scheduling)
   AUTHOR    : Mirza Ishtiyaq Baig
   DEPENDS ON: 01_data_warehouse_pipeline.sql (Section 1 must already exist —
               this script assumes DIM_AREA, DIM_PROPERTY_TYPE, DIM_ROOMS,
               DIM_TRANSACTION_TYPE, DIM_PROJECT and FACT_TRANSACTIONS have
               already been created as empty/baseline tables.)

   WHAT THIS FILE DOES
   --------------------------------------------------------------------------------
   Wraps the manual "Section 2 / Section 3" refresh-and-validate routine from
   the main pipeline script into two callable stored procedures, then wires
   them into a scheduled, self-chaining pair of Snowflake TASKs — so the
   Data Warehouse layer refreshes and self-checks automatically every time
   new data lands in RAW.TRANSACTIONS_STAGING, with zero manual SQL execution.

   WHY A STORED PROCEDURE AT ALL
   --------------------------------------------------------------------------------
   A Snowflake TASK can only execute a single SQL statement. The refresh logic
   is 6 sequential MERGE statements that must run in a strict order (all 5
   Dimensions before the Fact table, since Fact's JOINs depend on Dimension
   rows already existing). Wrapping that sequence in a procedure turns "6
   ordered statements" into "1 callable unit" that a TASK can actually run.

   UNIVERSAL TEXT NORMALIZATION POLICY (applies to every MERGE below)
   --------------------------------------------------------------------------------
   Every text value that becomes a dimension label, or a degenerate-dimension
   value in Fact (PARKING), is wrapped in UPPER(TRIM(...)) — consistently,
   with no exceptions, including columns that currently profile as clean.
   This was deliberately widened from a narrower fix after profiling found
   the same root cause (inconsistent source capitalization) in 3 separate
   columns (AREA, PROJECT, PARKING) — not just the one caught first. Rather
   than patch each column as it's individually discovered broken, every
   text field is normalized the same way, so a future capitalization
   inconsistency in ANY column — including ones never manually audited — is
   structurally prevented rather than caught after the fact. This same
   UPPER(TRIM()) transformation is also applied to every text field that
   feeds SOURCE_ROW_HASH (the Fact fingerprint) — without that, the same
   real-world transaction re-submitted in a future file with different
   capitalization would produce a different hash and get wrongly inserted
   as a duplicate row, silently defeating the idempotency this whole
   pipeline was built around.

   RUN FREQUENCY
   --------------------------------------------------------------------------------
   SP_REFRESH_DW()   -> called automatically, daily, by TASK_REFRESH_DW
   SP_VALIDATE_DW()  -> called automatically, immediately after, by
                        TASK_VALIDATE_DW (chained — only fires if the
                        refresh task succeeded)
   Both can also be called manually at any time for ad-hoc testing:
       CALL DUBAI_REAL_ESTATE.DW.SP_REFRESH_DW();
       CALL DUBAI_REAL_ESTATE.DW.SP_VALIDATE_DW();
   ================================================================================ */


/* ================================================================================
   PROCEDURE 1 of 2 — SP_REFRESH_DW
   --------------------------------------------------------------------------------
   Purpose : Incrementally refreshes all 5 Dimension tables and the Fact table
             from whatever currently sits in RAW.TRANSACTIONS_STAGING.
   Self-reporting : Captures the row count of every MERGE via Snowflake
             Scripting's built-in SQLROWCOUNT variable (which always holds
             the rows-affected count of the DML statement that ran
             immediately before it) and returns a full per-table breakdown.
             This matters because calling a procedure collapses 6 separate
             MERGE results into a single client response — without this,
             the only way to see what actually changed would be querying
             QUERY_HISTORY separately after every run.
   Safe to rerun : Yes — every MERGE only INSERTs rows that don't already
             exist (matched by a deterministic key). Rerunning against
             unchanged staging data inserts zero new rows, which will show
             as all-zero counts in the returned summary. This was verified
             manually before automation was added — see README for the
             verification log.
   NOT safe for : Schema changes. Adding/removing a tracked column requires
             updating this procedure's SELECT list AND the target table's
             DDL (in 01_data_warehouse_pipeline.sql) together — see
             TROUBLESHOOTING section at the bottom of this file.
   ================================================================================ */
CREATE OR REPLACE PROCEDURE DUBAI_REAL_ESTATE.DW.SP_REFRESH_DW()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    area_inserted              INT DEFAULT 0;
    property_type_inserted     INT DEFAULT 0;
    transaction_type_inserted  INT DEFAULT 0;
    rooms_inserted             INT DEFAULT 0;
    project_inserted           INT DEFAULT 0;
    fact_inserted               INT DEFAULT 0;
    summary                    STRING;
BEGIN
    -- ---- Dimension 1: Area (geography) ----------------------------------------
    -- UPPER(TRIM(...)) normalizes case so "Business Bay" and "BUSINESS BAY"
    -- from the source collapse into a single row instead of two separate
    -- HASH() keys (case-sensitivity issue found during profiling: 5 areas
    -- affected).
    MERGE INTO DUBAI_REAL_ESTATE.DW.DIM_AREA AS target
    USING (
        SELECT DISTINCT
            UPPER(TRIM(AREA_EN)) AS AREA_EN,
            HASH(UPPER(TRIM(AREA_EN))) AS AREA_ID
        FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
    ) AS source
    ON target.AREA_ID = source.AREA_ID
    WHEN NOT MATCHED THEN
        INSERT (AREA_ID, AREA_NAME) VALUES (source.AREA_ID, source.AREA_EN);
    area_inserted := SQLROWCOUNT;

    -- ---- Dimension 2: Property Type (type + subtype, cleaned) -----------------
    -- Profiled clean, but UPPER(TRIM()) applied anyway per the universal policy.
    MERGE INTO DUBAI_REAL_ESTATE.DW.DIM_PROPERTY_TYPE AS target
    USING (
        SELECT DISTINCT
            UPPER(TRIM(PROP_TYPE_EN)) AS PROP_TYPE_EN,
            UPPER(COALESCE(NULLIF(TRIM(PROP_SB_TYPE_EN), ''), 'Not Specified')) AS PROPERTY_SUBTYPE,
            HASH(UPPER(TRIM(PROP_TYPE_EN)), UPPER(COALESCE(NULLIF(TRIM(PROP_SB_TYPE_EN), ''), 'Not Specified'))) AS PROPERTY_TYPE_ID
        FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
    ) AS source
    ON target.PROPERTY_TYPE_ID = source.PROPERTY_TYPE_ID
    WHEN NOT MATCHED THEN
        INSERT (PROPERTY_TYPE_ID, PROPERTY_TYPE, PROPERTY_SUBTYPE)
        VALUES (source.PROPERTY_TYPE_ID, source.PROP_TYPE_EN, source.PROPERTY_SUBTYPE);
    property_type_inserted := SQLROWCOUNT;

    -- ---- Dimension 3: Transaction Type (junk dimension, 5 flags bundled) ------
    -- Profiled clean, but UPPER(TRIM()) applied to all 5 columns anyway,
    -- per the universal policy.
    MERGE INTO DUBAI_REAL_ESTATE.DW.DIM_TRANSACTION_TYPE AS target
    USING (
        SELECT DISTINCT
            UPPER(TRIM(GROUP_EN))        AS GROUP_EN,
            UPPER(TRIM(PROCEDURE_EN))    AS PROCEDURE_EN,
            UPPER(TRIM(IS_OFFPLAN_EN))   AS IS_OFFPLAN_EN,
            UPPER(TRIM(IS_FREE_HOLD_EN)) AS IS_FREE_HOLD_EN,
            UPPER(TRIM(USAGE_EN))        AS USAGE_EN,
            HASH(UPPER(TRIM(GROUP_EN)), UPPER(TRIM(PROCEDURE_EN)), UPPER(TRIM(IS_OFFPLAN_EN)),
                 UPPER(TRIM(IS_FREE_HOLD_EN)), UPPER(TRIM(USAGE_EN))) AS TRANSACTION_TYPE_ID
        FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
    ) AS source
    ON target.TRANSACTION_TYPE_ID = source.TRANSACTION_TYPE_ID
    WHEN NOT MATCHED THEN
        INSERT (TRANSACTION_TYPE_ID, TRANSACTION_GROUP, PROCEDURE_NAME, IS_OFFPLAN, IS_FREEHOLD, USAGE_TYPE)
        VALUES (source.TRANSACTION_TYPE_ID, source.GROUP_EN, source.PROCEDURE_EN,
                source.IS_OFFPLAN_EN, source.IS_FREE_HOLD_EN, source.USAGE_EN);
    transaction_type_inserted := SQLROWCOUNT;

    -- ---- Dimension 4: Rooms (blank vs literal "NA" kept distinct) -------------
    -- Whole CASE result wrapped in UPPER() so generated labels and real
    -- values ("Studio" etc.) are normalized consistently.
    MERGE INTO DUBAI_REAL_ESTATE.DW.DIM_ROOMS AS target
    USING (
        SELECT ROOM_TYPE, HASH(ROOM_TYPE) AS ROOM_ID
        FROM (
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
    ) AS source
    ON target.ROOM_ID = source.ROOM_ID
    WHEN NOT MATCHED THEN
        INSERT (ROOM_ID, ROOM_TYPE) VALUES (source.ROOM_ID, source.ROOM_TYPE);
    rooms_inserted := SQLROWCOUNT;

    -- ---- Dimension 5: Project -------------------------------------------------
    -- Same case-normalization fix as DIM_AREA — "Crystal Tower" / "CRYSTAL
    -- TOWER" were found as separate rows during profiling (2 projects affected).
    MERGE INTO DUBAI_REAL_ESTATE.DW.DIM_PROJECT AS target
    USING (
        SELECT PROJECT_NAME, HASH(PROJECT_NAME) AS PROJECT_ID
        FROM (
            SELECT DISTINCT UPPER(COALESCE(NULLIF(TRIM(PROJECT_EN), ''), 'Not Specified')) AS PROJECT_NAME
            FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING
        )
    ) AS source
    ON target.PROJECT_ID = source.PROJECT_ID
    WHEN NOT MATCHED THEN
        INSERT (PROJECT_ID, PROJECT_NAME) VALUES (source.PROJECT_ID, source.PROJECT_NAME);
    project_inserted := SQLROWCOUNT;

    -- ---- Fact table: must run LAST — depends on all 5 dimensions above --------
    -- Fingerprint (SOURCE_ROW_HASH) covers every retained business column, not
    -- a subset — a narrower fingerprint was tried first and found to collide
    -- on rows that were genuinely different transactions (same property/value/
    -- date, different deal type). Every TEXT input to the fingerprint is now
    -- also wrapped in UPPER(TRIM()) — without this, the same real transaction
    -- re-submitted in a future file with different capitalization would
    -- produce a different hash and be wrongly inserted as a new duplicate row.
    -- QUALIFY guards against true duplicate rows arriving together in a
    -- single source batch, which MERGE's WHEN NOT MATCHED alone does not
    -- catch (it only checks against the existing target, not for duplicates
    -- within the incoming batch itself).
    MERGE INTO DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS AS target
    USING (
        SELECT * FROM (
            SELECT
                TRIM(s.TRANSACTION_NUMBER) AS TRANSACTION_NUMBER,
                TRY_TO_TIMESTAMP(s.INSTANCE_DATE, 'YYYY-MM-DD HH24:MI:SS') AS TRANSACTION_DATE,
                TRY_TO_NUMBER(s.TRANS_VALUE)     AS TRANSACTION_VALUE_AED,
                TRY_TO_NUMBER(s.PROCEDURE_AREA)  AS PROCEDURE_AREA_SQM,
                TRY_TO_NUMBER(s.ACTUAL_AREA)     AS ACTUAL_AREA_SQM,
                TRY_TO_NUMBER(s.TOTAL_BUYER)     AS TOTAL_BUYERS,
                TRY_TO_NUMBER(s.TOTAL_SELLER)    AS TOTAL_SELLERS,
                UPPER(
                    CASE
                        WHEN TRIM(COALESCE(s.PARKING, '')) = '' THEN 'Not Applicable'
                        WHEN UPPER(TRIM(s.PARKING)) IN ('NA','N/A') THEN 'Data Not Captured'
                        ELSE TRIM(s.PARKING)
                    END
                ) AS PARKING,
                area.AREA_ID, prop.PROPERTY_TYPE_ID, txn.TRANSACTION_TYPE_ID, room.ROOM_ID, proj.PROJECT_ID,
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
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_ROW_HASH ORDER BY TRANSACTION_NUMBER) = 1
    ) AS source
    ON target.SOURCE_ROW_HASH = source.SOURCE_ROW_HASH
    WHEN NOT MATCHED THEN
        INSERT (TRANSACTION_NUMBER, TRANSACTION_DATE, TRANSACTION_VALUE_AED, PROCEDURE_AREA_SQM,
                ACTUAL_AREA_SQM, TOTAL_BUYERS, TOTAL_SELLERS, PARKING, AREA_ID, PROPERTY_TYPE_ID,
                TRANSACTION_TYPE_ID, ROOM_ID, PROJECT_ID, SOURCE_ROW_HASH)
        VALUES (source.TRANSACTION_NUMBER, source.TRANSACTION_DATE, source.TRANSACTION_VALUE_AED,
                source.PROCEDURE_AREA_SQM, source.ACTUAL_AREA_SQM, source.TOTAL_BUYERS, source.TOTAL_SELLERS,
                source.PARKING, source.AREA_ID, source.PROPERTY_TYPE_ID, source.TRANSACTION_TYPE_ID,
                source.ROOM_ID, source.PROJECT_ID, source.SOURCE_ROW_HASH);
    fact_inserted := SQLROWCOUNT;

    -- All-zero counts here is the expected, healthy signature of a rerun
    -- against unchanged data — that IS the idempotency proof, visible now
    -- on every single run without needing a separate manual test.
    summary := 'DW refresh completed.' || CHR(10) ||
        'New rows inserted -> Area: ' || area_inserted ||
        ', Property Type: ' || property_type_inserted ||
        ', Transaction Type: ' || transaction_type_inserted ||
        ', Rooms: ' || rooms_inserted ||
        ', Project: ' || project_inserted ||
        ', Fact: ' || fact_inserted || '.';

    RETURN summary;
END;
$$;


/* ================================================================================
   PROCEDURE 2 of 2 — SP_VALIDATE_DW
   --------------------------------------------------------------------------------
   Purpose : Automated data-quality gate, run immediately after every refresh.
             Mirrors the manual 5-check sign-off checklist used during initial
             development, condensed into 3 hard-fail structural checks plus a
             business sanity check.
   Design note on Check 2 (row-count reconciliation): during manual testing
             this was a strict "difference must equal 0" gate. Once QUALIFY
             was added to correctly collapse true duplicate source rows,
             Staging and Fact counts legitimately differ by a small, variable
             amount on any run where the source file contains duplicates —
             that is expected behavior now, not a failure. Automating the
             original strict version would cause false failures on every
             future run, so it was deliberately dropped as a hard gate here.
   Failure mode : Uses RAISE to throw a real Snowflake exception on failure —
             not just a returned string — so a failed check makes the TASK
             itself show up as FAILED in TASK_HISTORY, rather than silently
             succeeding with a message nobody reads.
   ================================================================================ */
CREATE OR REPLACE PROCEDURE DUBAI_REAL_ESTATE.DW.SP_VALIDATE_DW()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    dup_key_count INT;
    orphan_count INT;
    fingerprint_dupe_count INT;
    txn_count INT;
    avg_value FLOAT;
    dq_failure EXCEPTION (-20001, 'Data quality gate failed - inspect DW tables before trusting this refresh');
BEGIN
    -- CHECK 1: Dimension key integrity — no duplicate keys in any dimension.
    SELECT COUNT(*) INTO :dup_key_count FROM (
        SELECT AREA_ID FROM DUBAI_REAL_ESTATE.DW.DIM_AREA GROUP BY AREA_ID HAVING COUNT(*) > 1
        UNION ALL
        SELECT PROPERTY_TYPE_ID FROM DUBAI_REAL_ESTATE.DW.DIM_PROPERTY_TYPE GROUP BY PROPERTY_TYPE_ID HAVING COUNT(*) > 1
        UNION ALL
        SELECT TRANSACTION_TYPE_ID FROM DUBAI_REAL_ESTATE.DW.DIM_TRANSACTION_TYPE GROUP BY TRANSACTION_TYPE_ID HAVING COUNT(*) > 1
        UNION ALL
        SELECT ROOM_ID FROM DUBAI_REAL_ESTATE.DW.DIM_ROOMS GROUP BY ROOM_ID HAVING COUNT(*) > 1
        UNION ALL
        SELECT PROJECT_ID FROM DUBAI_REAL_ESTATE.DW.DIM_PROJECT GROUP BY PROJECT_ID HAVING COUNT(*) > 1
    );

    -- CHECK 2: Orphan foreign keys — every FK on every Fact row must resolve
    -- to a real dimension row. A non-zero count here means a cleaning rule
    -- in this procedure has drifted out of sync with a dimension's rule.
    SELECT
        SUM(CASE WHEN AREA_ID IS NULL THEN 1 ELSE 0 END)
        + SUM(CASE WHEN PROPERTY_TYPE_ID IS NULL THEN 1 ELSE 0 END)
        + SUM(CASE WHEN TRANSACTION_TYPE_ID IS NULL THEN 1 ELSE 0 END)
        + SUM(CASE WHEN ROOM_ID IS NULL THEN 1 ELSE 0 END)
        + SUM(CASE WHEN PROJECT_ID IS NULL THEN 1 ELSE 0 END)
    INTO :orphan_count
    FROM DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS;

    -- CHECK 3: Fingerprint uniqueness — no source row inserted twice.
    SELECT COUNT(*) INTO :fingerprint_dupe_count FROM (
        SELECT SOURCE_ROW_HASH FROM DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS
        GROUP BY SOURCE_ROW_HASH HAVING COUNT(*) > 1
    );

    -- CHECK 4: Business sanity — table isn't empty and values aren't nonsensical.
    SELECT COUNT(*), AVG(TRANSACTION_VALUE_AED) INTO :txn_count, :avg_value
    FROM DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS;

    IF (dup_key_count > 0 OR orphan_count > 0 OR fingerprint_dupe_count > 0
        OR txn_count = 0 OR avg_value IS NULL OR avg_value <= 0) THEN
        RAISE dq_failure;
    END IF;

    RETURN 'All checks passed - ' || txn_count || ' rows, avg value ' || ROUND(avg_value, 0) || ' AED';
END;
$$;


/* ================================================================================
   SCHEDULED TASKS — chained: validation only fires if refresh succeeds
   --------------------------------------------------------------------------------
   Tasks are created SUSPENDED by default in Snowflake (a deliberate safety
   default). They must be explicitly resumed, and — because of the parent/
   child dependency — the CHILD must be resumed before the PARENT.
   ================================================================================ */
CREATE OR REPLACE TASK DUBAI_REAL_ESTATE.DW.TASK_REFRESH_DW
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 6 * * * Asia/Dubai'   -- daily, 6 AM Dubai time
AS
    CALL DUBAI_REAL_ESTATE.DW.SP_REFRESH_DW();

CREATE OR REPLACE TASK DUBAI_REAL_ESTATE.DW.TASK_VALIDATE_DW
    WAREHOUSE = COMPUTE_WH
    AFTER DUBAI_REAL_ESTATE.DW.TASK_REFRESH_DW        -- chained, not scheduled
AS
    CALL DUBAI_REAL_ESTATE.DW.SP_VALIDATE_DW();

-- Activation order matters: child before parent.
ALTER TASK DUBAI_REAL_ESTATE.DW.TASK_VALIDATE_DW RESUME;
ALTER TASK DUBAI_REAL_ESTATE.DW.TASK_REFRESH_DW  RESUME;

-- Manual trigger, for testing without waiting for the 6 AM schedule:
-- EXECUTE TASK DUBAI_REAL_ESTATE.DW.TASK_REFRESH_DW;

-- Daily monitoring query — confirms last night's automated run succeeded:
-- SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
-- FROM TABLE(DUBAI_REAL_ESTATE.INFORMATION_SCHEMA.TASK_HISTORY())
-- ORDER BY SCHEDULED_TIME DESC LIMIT 10;


/* ================================================================================
   TROUBLESHOOTING / DEBUGGING GUIDE
   --------------------------------------------------------------------------------
   Scenario: A dimension MERGE inserts 0 rows when new areas/projects/etc.
             were genuinely expected.
   Check   : Confirm the new CSV actually landed in RAW.TRANSACTIONS_STAGING
             first — `SELECT COUNT(*) FROM RAW.TRANSACTIONS_STAGING;` and
             compare to the expected new row count. This procedure only ever
             reads from staging; if staging wasn't refreshed, there's nothing
             new for it to find.

   Scenario: SP_VALIDATE_DW raises "Data quality gate failed."
   Check   : Run the 4 individual SELECT queries from inside the procedure
             body manually to see which specific check is non-zero, then
             cross-reference against the CHECK comments in
             01_data_warehouse_pipeline.sql Section 3 for what each one
             means and how it was diagnosed previously.

   Scenario: A NEW column needs to be tracked (e.g. a future LAND_NUMBER
             field appears in DLD's export).
   Steps   : (1) ALTER the target Dimension or Fact table in
                 01_data_warehouse_pipeline.sql to add the column.
             (2) Add the column to this procedure's relevant SELECT list
                 AND its INSERT column list — both must be updated together,
                 or the MERGE will error on a column-count mismatch.
             (3) If the new column is free text (not a numeric/date/flag),
                 wrap it in UPPER(TRIM(...)) immediately, per the universal
                 text normalization policy — do not wait for a future
                 profiling pass to catch a case-duplicate in it.
             (4) Re-run the full SECTION 3 validation checklist manually
                 before trusting the next automated run.

   Scenario: Hash collisions reappear (Check 3 in SP_VALIDATE_DW fails).
   Meaning : Two genuinely different transactions are producing the same
             SOURCE_ROW_HASH — the fingerprint is missing a column that
             actually distinguishes real records. This happened once during
             development (GROUP_EN was initially left out of the hash).
   Fix     : Identify which column differs between the colliding rows
             (query FACT_TRANSACTIONS grouped by SOURCE_ROW_HASH with
             COUNT(*) > 1, then trace back to RAW.TRANSACTIONS_STAGING to
             compare full rows), then add that column into the HASH() call
             inside this procedure's Fact MERGE — remembering to wrap any
             text column in UPPER(TRIM()) first, consistent with every
             other hash input.

   Scenario: A dimension shows more rows than expected after a refresh
             (e.g. DIM_AREA above 267, or DIM_PROJECT above 2,963).
   Meaning : A case-duplicate has reappeared — either a genuinely new area/
             project name, or a normalization gap in a column not yet
             covered by UPPER(TRIM()).
   Check   : SELECT AREA_NAME, COUNT(*) FROM DIM_AREA GROUP BY AREA_NAME
             HAVING COUNT(*) > 1 — should always return zero rows, since
             AREA_NAME is itself the post-normalization value. If it ever
             returns rows, the normalization applied at load time did not
             fully collapse a variant — re-check the exact source value
             that caused it in RAW.TRANSACTIONS_STAGING.

   Scenario: Need to test changes safely without affecting the real Task
             schedule.
   Approach: Call the procedures manually first — `CALL SP_REFRESH_DW();`
             then `CALL SP_VALIDATE_DW();` — and inspect results before
             ever touching the TASK definitions or their schedule.
   ================================================================================ */


SELECT * 
FROM DUBAI_REAL_ESTATE.DW.FACT_TRANSACTIONS
WHERE YEAR(TRANSACTION_DATE) = 2026 AND MONTH(TRANSACTION_DATE) = 8;