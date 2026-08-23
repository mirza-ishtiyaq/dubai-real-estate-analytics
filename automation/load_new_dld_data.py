"""
Load New DLD Transaction Data into Snowflake
=============================================
Pipeline: Read DLD transactions CSV → Clean & Validate → Push to Snowflake
Target:   DUBAI_REAL_ESTATE.RAW.RAW_TRANSACTIONS
Source:   Dubai Land Department (DLD) Open Data Portal

Converted from load_new_dld_data.ipynb for use in automated pipelines.

Usage:
    export SNOWFLAKE_ACCOUNT='your_account_identifier'
    export SNOWFLAKE_USER='your_username'
    export SNOWFLAKE_PASSWORD='your_password'
    python load_new_dld_data.py
"""

# ─── Imports ────────────────────────────────────────────────────────────
import pandas as pd
import snowflake.connector
import os
import sys
import getpass
from datetime import datetime


def get_config():
    """Build Snowflake connection config from environment variables."""
    config = {
        "account":   os.environ.get("SNOWFLAKE_ACCOUNT", "YOUR_SNOWFLAKE_ACCOUNT"),
        "user":      os.environ.get("SNOWFLAKE_USER", "YOUR_USERNAME"),
        "password":  os.environ.get("SNOWFLAKE_PASSWORD", ""),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  "DUBAI_REAL_ESTATE",
        "schema":    "RAW",
    }

    # Prompt for password if not set in environment
    if not config["password"]:
        config["password"] = getpass.getpass("Enter Snowflake password: ")

    # Validate required fields
    if config["account"] == "YOUR_SNOWFLAKE_ACCOUNT":
        print("⚠️  SNOWFLAKE_ACCOUNT not set. Export it or update the default.")
        sys.exit(1)
    if config["user"] == "YOUR_USERNAME":
        print("⚠️  SNOWFLAKE_USER not set. Export it or update the default.")
        sys.exit(1)

    return config


def resolve_csv_path(csv_filename):
    """Locate the CSV file across common project directory layouts."""
    candidate_paths = [
        os.path.abspath(os.path.join(os.getcwd(), "..", "Raw_Data", csv_filename)),
        os.path.abspath(os.path.join(os.getcwd(), "Raw_Data", csv_filename)),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Raw_Data", csv_filename)),
    ]
    csv_path = next((p for p in candidate_paths if os.path.exists(p)), None)
    if csv_path is None:
        print(f"❌ CSV file not found: {csv_filename}")
        print("   Searched:")
        for p in candidate_paths:
            print(f"     - {p}")
        sys.exit(1)
    return csv_path


def load_and_validate_csv(csv_path):
    """Load CSV with BOM handling and perform basic validation."""
    print(f"\n{'─' * 60}")
    print(f"  Loading CSV: {os.path.basename(csv_path)}")
    print(f"{'─' * 60}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    print(f"  Rows loaded     : {len(df):,}")
    print(f"  Columns         : {len(df.columns)}")
    print(f"  File size       : {os.path.getsize(csv_path) / (1024*1024):.2f} MB")

    # ─── Null summary ──────────────────────────────────────────────────
    null_summary = df.isnull().sum()
    null_pct = (null_summary / len(df) * 100).round(2)
    null_df = pd.DataFrame({"Null Count": null_summary, "Null %": null_pct})
    nulls_present = null_df[null_df["Null Count"] > 0]
    if not nulls_present.empty:
        print(f"\n  ─── Null Value Summary ───")
        print(nulls_present.to_string())
        print(f"\n  Total null entries: {null_summary.sum():,}")

    # ─── Duplicate check ───────────────────────────────────────────────
    dup_count = df.duplicated(subset=["TRANSACTION_NUMBER"]).sum()
    if dup_count > 0:
        print(f"\n  ℹ️  CSV contains {dup_count} duplicate TRANSACTION_NUMBERs.")

    return df


def upload_to_snowflake(config, csv_path, df, target_table="RAW_TRANSACTIONS"):
    """Upload CSV to Snowflake via PUT + COPY INTO."""
    csv_filename = os.path.basename(csv_path)
    stage_name = "DLD_IMPORT_STAGE"

    print(f"\n{'─' * 60}")
    print(f"  Connecting to Snowflake")
    print(f"{'─' * 60}")
    print(f"  Account   : {config['account']}")
    print(f"  User      : {config['user']}")
    print(f"  Database  : {config['database']}")
    print(f"  Schema    : {config['schema']}")
    print(f"  Warehouse : {config['warehouse']}")

    conn = snowflake.connector.connect(**config)
    cur = conn.cursor()
    print("  ✅ Connected successfully.")

    try:
        # ── Set context ────────────────────────────────────────────────
        cur.execute(f"USE WAREHOUSE {config['warehouse']}")
        cur.execute(f"USE DATABASE {config['database']}")
        cur.execute(f"USE SCHEMA {config['schema']}")

        # ── Ensure target table exists ─────────────────────────────────
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            TRANSACTION_NUMBER  VARCHAR(100),
            INSTANCE_DATE       TIMESTAMP_NTZ,
            GROUP_EN            VARCHAR(100),
            PROCEDURE_EN        VARCHAR(150),
            IS_OFFPLAN_EN       VARCHAR(50),
            IS_FREE_HOLD_EN     VARCHAR(50),
            USAGE_EN            VARCHAR(100),
            AREA_EN             VARCHAR(150),
            PROP_TYPE_EN        VARCHAR(100),
            PROP_SB_TYPE_EN     VARCHAR(100),
            TRANS_VALUE         NUMBER(18, 2),
            PROCEDURE_AREA      NUMBER(18, 2),
            ACTUAL_AREA         NUMBER(18, 2),
            ROOMS_EN            VARCHAR(100),
            PARKING             VARCHAR(100),
            NEAREST_METRO_EN    VARCHAR(150),
            NEAREST_MALL_EN     VARCHAR(150),
            NEAREST_LANDMARK_EN VARCHAR(150),
            TOTAL_BUYER         NUMBER(38, 0),
            TOTAL_SELLER        NUMBER(38, 0),
            MASTER_PROJECT_EN   VARCHAR(250),
            PROJECT_EN          VARCHAR(250)
        );
        """
        cur.execute(create_table_sql)
        print(f"  ✅ Table {target_table} verified/created.")

        # ── Pre-load row count ─────────────────────────────────────────
        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        rows_before = cur.fetchone()[0]
        print(f"  Current rows: {rows_before:,}")

        # ── PUT: upload to stage ───────────────────────────────────────
        cur.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
        put_sql = f"PUT 'file://{csv_path}' @{stage_name}/{target_table}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        print(f"\n  Uploading {csv_filename} ({os.path.getsize(csv_path) / 1024:.1f} KB)...")
        cur.execute(put_sql)
        put_result = cur.fetchall()
        if put_result:
            print(f"  PUT status: {put_result[0][6]}")

        # ── COPY INTO: load from stage ─────────────────────────────────
        copy_sql = f"""
            COPY INTO {target_table}
            FROM @{stage_name}/{target_table}/
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                EMPTY_FIELD_AS_NULL = TRUE
                FIELD_DELIMITER = ','
                RECORD_DELIMITER = '\\n'
                ENCODING = 'UTF8'
            )
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE
        """
        print(f"  Loading into {target_table}...")
        cur.execute(copy_sql)
        copy_result = cur.fetchall()

        if copy_result:
            rows_parsed = copy_result[0][2]
            rows_loaded = copy_result[0][3]
            errors_seen = copy_result[0][5]
            first_error = copy_result[0][6]

            print(f"  Rows parsed  : {rows_parsed:,}")
            print(f"  Rows loaded  : {rows_loaded:,}")
            print(f"  Errors seen  : {errors_seen}")
            if first_error:
                print(f"  ⚠️  First error: {first_error}")

        # ── Post-load verification ─────────────────────────────────────
        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        rows_after = cur.fetchone()[0]
        rows_added = rows_after - rows_before

        print(f"\n{'═' * 60}")
        print(f"  LOAD SUMMARY")
        print(f"{'═' * 60}")
        print(f"  Table          : {config['database']}.{config['schema']}.{target_table}")
        print(f"  Rows before    : {rows_before:,}")
        print(f"  Rows after     : {rows_after:,}")
        print(f"  Rows added     : {rows_added:,}")
        print(f"  Source file    : {csv_filename}")
        print(f"  CSV row count  : {len(df):,}")
        print(f"{'═' * 60}")

        # ── Sample preview ─────────────────────────────────────────────
        cur.execute(
            f"SELECT TRANSACTION_NUMBER, INSTANCE_DATE, AREA_EN, TRANS_VALUE, ROOMS_EN, PARKING "
            f"FROM {target_table} ORDER BY INSTANCE_DATE DESC LIMIT 5"
        )
        columns = [desc[0] for desc in cur.description]
        sample_df = pd.DataFrame(cur.fetchall(), columns=columns)
        print("\n  ─── Latest 5 rows in Snowflake ───")
        print(sample_df.to_string(index=False))

    finally:
        cur.close()
        conn.close()
        print("\n  ✅ Snowflake connection closed.")


def main():
    print(f"{'═' * 60}")
    print(f"  DLD Transaction Loader")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 60}")

    csv_filename = "transactions-2026-08-23.csv"

    config = get_config()
    csv_path = resolve_csv_path(csv_filename)
    df = load_and_validate_csv(csv_path)
    upload_to_snowflake(config, csv_path, df)

    print(f"\n🏁 Pipeline complete — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
