-- ============================================================
-- GHCN Daily Data - Raw Tables
-- ============================================================
-- Creates raw tables for GHCN metadata and daily observations

USE WAREHOUSE SKYRA_ATMOS;
USE DATABASE WEATHER_ANALYTICS;
USE SCHEMA RAW;

-- ============================================================
-- METADATA TABLES
-- ============================================================

-- Countries lookup table
-- Source: ghcnd-countries.txt (fixed-width: 2-char code + country name)
CREATE OR REPLACE TABLE RAW_COUNTRIES (
    COUNTRY_CODE    VARCHAR(2),
    COUNTRY_NAME    VARCHAR(100),
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- States/Provinces lookup table
-- Source: ghcnd-states.txt (fixed-width: 2-char code + state name)
CREATE OR REPLACE TABLE RAW_STATES (
    STATE_CODE      VARCHAR(2),
    STATE_NAME      VARCHAR(100),
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stations metadata
-- Source: ghcnd-stations.txt (fixed-width format)
CREATE OR REPLACE TABLE RAW_STATIONS (
    STATION_ID      VARCHAR(11),    -- Columns 1-11
    LATITUDE        FLOAT,          -- Columns 13-20
    LONGITUDE       FLOAT,          -- Columns 22-30
    ELEVATION       FLOAT,          -- Columns 32-37 (meters)
    STATE           VARCHAR(2),     -- Columns 39-40
    STATION_NAME    VARCHAR(30),    -- Columns 42-71
    GSN_FLAG        VARCHAR(3),     -- Columns 73-75 (GCOS Surface Network)
    HCN_CRN_FLAG    VARCHAR(3),     -- Columns 77-79 (Historical Climatology Network)
    WMO_ID          VARCHAR(5),     -- Columns 81-85 (World Meteorological Org)
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Inventory table (data availability per station/element)
-- Source: ghcnd-inventory.txt (fixed-width format)
CREATE OR REPLACE TABLE RAW_INVENTORY (
    STATION_ID      VARCHAR(11),    -- Columns 1-11
    LATITUDE        FLOAT,          -- Columns 13-20
    LONGITUDE       FLOAT,          -- Columns 22-30
    ELEMENT         VARCHAR(4),     -- Columns 32-35 (TMAX, TMIN, PRCP, etc.)
    FIRST_YEAR      INTEGER,        -- Columns 37-40
    LAST_YEAR       INTEGER,        -- Columns 42-45
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DAILY OBSERVATIONS TABLE
-- ============================================================

-- Daily climate observations
-- Source: parquet/by_year/ (Parquet format, partitioned by year)
CREATE OR REPLACE TABLE RAW_DAILY_OBSERVATIONS (
    ID              VARCHAR(11),    -- Station ID
    DATE            VARCHAR(8),     -- YYYYMMDD format
    ELEMENT         VARCHAR(4),     -- Observation type (TMAX, TMIN, PRCP, etc.)
    DATA_VALUE      INTEGER,        -- Observation value (units vary by element)
    M_FLAG          VARCHAR(1),     -- Measurement flag
    Q_FLAG          VARCHAR(1),     -- Quality flag
    S_FLAG          VARCHAR(1),     -- Source flag
    OBS_TIME        VARCHAR(4),     -- Observation time (HHMM, often null)
    YEAR            INTEGER,        -- Partition year (derived from file path)
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- CLUSTERING (for query performance)
-- ============================================================

-- Cluster daily observations by commonly filtered columns
ALTER TABLE RAW_DAILY_OBSERVATIONS CLUSTER BY (YEAR, ELEMENT, ID);

-- ============================================================
-- VERIFY TABLES
-- ============================================================

SHOW TABLES IN SCHEMA RAW;
