/* =====================================================================
   Exploratory & Validation Queries
   ---------------------------------------------------------------------
   Ad-hoc queries used during development to profile the raw DLD data,
   verify dimension builds, and validate the data warehouse structure.
   ===================================================================== */

-- Quick schema inventory
SHOW SCHEMAS IN DATABASE DUBAI_REAL_ESTATE;

-- Raw staging row count
SELECT COUNT(*) FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING;

-- Sample raw records
SELECT * FROM DUBAI_REAL_ESTATE.RAW.TRANSACTIONS_STAGING LIMIT 10;

-- Verify DIM_ROOMS build (check distinct room types)
USE DATABASE DUBAI_REAL_ESTATE;
USE SCHEMA DW;

SELECT * FROM DIM_ROOMS;
