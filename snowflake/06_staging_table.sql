-- ============================================================
-- GHCN Daily Data - Staging Table for Incremental Load
-- ============================================================
-- Creates staging table used by Airflow for daily incremental loads

USE WAREHOUSE SKYRA_ATMOS;
USE DATABASE WEATHER_ANALYTICS;
USE SCHEMA RAW;

-- ============================================================
-- STAGING TABLE
-- ============================================================

-- Staging table for daily observations (temporary landing zone)
CREATE OR REPLACE TABLE STG_DAILY_OBSERVATIONS (
    ID              VARCHAR(11),
    DATE            VARCHAR(8),
    ELEMENT         VARCHAR(4),
    DATA_VALUE      INTEGER,
    M_FLAG          VARCHAR(1),
    Q_FLAG          VARCHAR(1),
    S_FLAG          VARCHAR(1),
    OBS_TIME        VARCHAR(4),
    YEAR            INTEGER
);

-- ============================================================
-- LOAD TRACKING TABLE
-- ============================================================

-- Track when data was last loaded (for monitoring)
CREATE OR REPLACE TABLE LOAD_TRACKING (
    TABLE_NAME      VARCHAR(100),
    LAST_LOAD_DATE  DATE,
    ROWS_LOADED     INTEGER,
    LOAD_TIMESTAMP  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- VERIFY
-- ============================================================

SHOW TABLES LIKE '%STG%' IN SCHEMA RAW;
SHOW TABLES LIKE '%TRACKING%' IN SCHEMA RAW;
