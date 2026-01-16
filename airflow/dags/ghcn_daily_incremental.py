"""
GHCN Daily Incremental Load DAG

Incrementally loads NOAA GHCN daily observations into Snowflake.
Runs daily at 1 AM EST (6 AM UTC) after NOAA updates their data.

Flow:
1. Load current year data into staging table
2. MERGE only new records into RAW table
3. Update load tracking
4. Truncate staging table

Schedule: Daily at 1:00 AM EST (6:00 AM UTC)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


# DAG configuration
default_args = {
    'owner': 'skyra-atmos',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Snowflake connection ID (configure in Airflow UI: Admin > Connections)
SNOWFLAKE_CONN_ID = 'snowflake_default'

# Current year for loading
CURRENT_YEAR = datetime.now().year


# ============================================================
# SQL STATEMENTS
# ============================================================

SQL_SET_CONTEXT = """
USE WAREHOUSE SKYRA_ATMOS;
USE DATABASE WEATHER_ANALYTICS;
USE SCHEMA RAW;
"""

SQL_TRUNCATE_STAGING = """
TRUNCATE TABLE STG_DAILY_OBSERVATIONS;
"""

SQL_LOAD_TO_STAGING = f"""
COPY INTO STG_DAILY_OBSERVATIONS (ID, DATE, ELEMENT, DATA_VALUE, M_FLAG, Q_FLAG, S_FLAG, OBS_TIME, YEAR)
FROM (
    SELECT
        $1:ID::VARCHAR(11) AS ID,
        $1:DATE::VARCHAR(8) AS DATE,
        $1:ELEMENT::VARCHAR(4) AS ELEMENT,
        $1:DATA_VALUE::INTEGER AS DATA_VALUE,
        $1:M_FLAG::VARCHAR(1) AS M_FLAG,
        $1:Q_FLAG::VARCHAR(1) AS Q_FLAG,
        $1:S_FLAG::VARCHAR(1) AS S_FLAG,
        $1:OBS_TIME::VARCHAR(4) AS OBS_TIME,
        {CURRENT_YEAR} AS YEAR
    FROM @STG_NOAA_GHCN/parquet/by_year/YEAR={CURRENT_YEAR}/
)
FILE_FORMAT = FF_GHCN_PARQUET
FORCE = TRUE
ON_ERROR = 'CONTINUE';
"""

SQL_MERGE_TO_RAW = f"""
MERGE INTO RAW_DAILY_OBSERVATIONS AS target
USING STG_DAILY_OBSERVATIONS AS source
ON target.ID = source.ID
   AND target.DATE = source.DATE
   AND target.ELEMENT = source.ELEMENT
   AND target.YEAR = source.YEAR
WHEN NOT MATCHED THEN
    INSERT (ID, DATE, ELEMENT, DATA_VALUE, M_FLAG, Q_FLAG, S_FLAG, OBS_TIME, YEAR, _LOADED_AT)
    VALUES (source.ID, source.DATE, source.ELEMENT, source.DATA_VALUE,
            source.M_FLAG, source.Q_FLAG, source.S_FLAG, source.OBS_TIME,
            source.YEAR, CURRENT_TIMESTAMP());
"""

SQL_UPDATE_TRACKING = f"""
INSERT INTO LOAD_TRACKING (TABLE_NAME, LAST_LOAD_DATE, ROWS_LOADED)
SELECT
    'RAW_DAILY_OBSERVATIONS',
    CURRENT_DATE(),
    COUNT(*)
FROM RAW_DAILY_OBSERVATIONS
WHERE YEAR = {CURRENT_YEAR}
  AND DATE(LOADED_AT) = CURRENT_DATE();
"""

SQL_VERIFY_LOAD = f"""
SELECT
    'STAGING' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MAX(DATE) AS LATEST_DATE
FROM STG_DAILY_OBSERVATIONS
UNION ALL
SELECT
    'RAW (Current Year)',
    COUNT(*),
    MAX(DATE)
FROM RAW_DAILY_OBSERVATIONS
WHERE YEAR = {CURRENT_YEAR};
"""

SQL_CLEANUP_STAGING = """
TRUNCATE TABLE STG_DAILY_OBSERVATIONS;
"""


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id='ghcn_daily_incremental',
    default_args=default_args,
    description='Incremental daily load of GHCN weather data to Snowflake',
    schedule_interval='0 6 * * *',  # 6 AM UTC = 1 AM EST
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ghcn', 'snowflake', 'weather', 'incremental'],
    doc_md=__doc__,
) as dag:

    # Task 1: Set Snowflake context
    set_context = SnowflakeOperator(
        task_id='set_context',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_SET_CONTEXT,
    )

    # Task 2: Clear staging table
    truncate_staging = SnowflakeOperator(
        task_id='truncate_staging',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_TRUNCATE_STAGING,
    )

    # Task 3: Load current year data to staging
    load_to_staging = SnowflakeOperator(
        task_id='load_to_staging',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_LOAD_TO_STAGING,
    )

    # Task 4: Merge new records to RAW table
    merge_to_raw = SnowflakeOperator(
        task_id='merge_to_raw',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_MERGE_TO_RAW,
    )

    # Task 5: Verify load results
    verify_load = SnowflakeOperator(
        task_id='verify_load',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_VERIFY_LOAD,
    )

    # Task 6: Cleanup staging
    cleanup_staging = SnowflakeOperator(
        task_id='cleanup_staging',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_CLEANUP_STAGING,
    )

    # Task dependencies
    set_context >> truncate_staging >> load_to_staging >> merge_to_raw >> verify_load >> cleanup_staging
