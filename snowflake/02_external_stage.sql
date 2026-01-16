-- ============================================================
-- GHCN Daily Data - External Stage Setup
-- ============================================================
-- Creates external stage pointing to NOAA public S3 bucket
-- No credentials needed for public bucket access

USE WAREHOUSE SKYRA_ATMOS;
USE DATABASE WEATHER_ANALYTICS;
USE SCHEMA RAW;

-- ============================================================
-- FILE FORMATS
-- ============================================================

-- File format for fixed-width metadata files (loaded as single column, parsed later)
CREATE OR REPLACE FILE FORMAT FF_GHCN_TEXT
    TYPE = 'CSV'
    FIELD_DELIMITER = NONE
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    TRIM_SPACE = FALSE;

-- File format for Parquet files (daily observations)
CREATE OR REPLACE FILE FORMAT FF_GHCN_PARQUET
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';

-- ============================================================
-- EXTERNAL STAGE
-- ============================================================

-- Create external stage for NOAA GHCN public S3 bucket
-- This bucket is publicly accessible (no credentials required)
CREATE OR REPLACE STAGE STG_NOAA_GHCN
    URL = 's3://noaa-ghcn-pds/'
    FILE_FORMAT = FF_GHCN_TEXT;

-- Verify stage was created
SHOW STAGES;

-- ============================================================
-- TEST STAGE ACCESS
-- ============================================================

-- List metadata files
LIST @STG_NOAA_GHCN/ghcnd-countries.txt;
LIST @STG_NOAA_GHCN/ghcnd-states.txt;
LIST @STG_NOAA_GHCN/ghcnd-stations.txt;
LIST @STG_NOAA_GHCN/ghcnd-inventory.txt;

-- List parquet partitions (sample)
LIST @STG_NOAA_GHCN/parquet/by_year/YEAR=2023/;
