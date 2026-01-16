-- ============================================================
-- GHCN Daily Data - Load Metadata Tables
-- ============================================================
-- Loads metadata from fixed-width text files
-- Uses SUBSTR to parse fixed-width columns

USE WAREHOUSE SKYRA_ATMOS;
USE DATABASE WEATHER_ANALYTICS;
USE SCHEMA RAW;

-- ============================================================
-- LOAD COUNTRIES
-- ============================================================
-- Format: 2-char code + space + country name
-- Example: "AC Antigua and Barbuda"

COPY INTO RAW_COUNTRIES (COUNTRY_CODE, COUNTRY_NAME)
FROM (
    SELECT
        SUBSTR($1, 1, 2) AS COUNTRY_CODE,
        TRIM(SUBSTR($1, 4)) AS COUNTRY_NAME
    FROM @STG_NOAA_GHCN/ghcnd-countries.txt
)
FILE_FORMAT = FF_GHCN_TEXT
ON_ERROR = 'CONTINUE';

-- Verify
SELECT COUNT(*) AS row_count FROM RAW_COUNTRIES;
SELECT * FROM RAW_COUNTRIES LIMIT 10;

-- ============================================================
-- LOAD STATES
-- ============================================================
-- Format: 2-char code + space + state name
-- Example: "AL ALABAMA"

COPY INTO RAW_STATES (STATE_CODE, STATE_NAME)
FROM (
    SELECT
        SUBSTR($1, 1, 2) AS STATE_CODE,
        TRIM(SUBSTR($1, 4)) AS STATE_NAME
    FROM @STG_NOAA_GHCN/ghcnd-states.txt
)
FILE_FORMAT = FF_GHCN_TEXT
ON_ERROR = 'CONTINUE';

-- Verify
SELECT COUNT(*) AS row_count FROM RAW_STATES;
SELECT * FROM RAW_STATES LIMIT 10;

-- ============================================================
-- LOAD STATIONS
-- ============================================================
-- Fixed-width format:
-- Cols 1-11:  Station ID
-- Cols 13-20: Latitude
-- Cols 22-30: Longitude
-- Cols 32-37: Elevation
-- Cols 39-40: State
-- Cols 42-71: Name
-- Cols 73-75: GSN flag
-- Cols 77-79: HCN/CRN flag
-- Cols 81-85: WMO ID

COPY INTO RAW_STATIONS (STATION_ID, LATITUDE, LONGITUDE, ELEVATION, STATE, STATION_NAME, GSN_FLAG, HCN_CRN_FLAG, WMO_ID)
FROM (
    SELECT
        SUBSTR($1, 1, 11) AS STATION_ID,
        TRY_TO_DOUBLE(TRIM(SUBSTR($1, 13, 8))) AS LATITUDE,
        TRY_TO_DOUBLE(TRIM(SUBSTR($1, 22, 9))) AS LONGITUDE,
        TRY_TO_DOUBLE(TRIM(SUBSTR($1, 32, 6))) AS ELEVATION,
        TRIM(SUBSTR($1, 39, 2)) AS STATE,
        TRIM(SUBSTR($1, 42, 30)) AS STATION_NAME,
        TRIM(SUBSTR($1, 73, 3)) AS GSN_FLAG,
        TRIM(SUBSTR($1, 77, 3)) AS HCN_CRN_FLAG,
        TRIM(SUBSTR($1, 81, 5)) AS WMO_ID
    FROM @STG_NOAA_GHCN/ghcnd-stations.txt
)
FILE_FORMAT = FF_GHCN_TEXT
ON_ERROR = 'CONTINUE';

-- Verify
SELECT COUNT(*) AS row_count FROM RAW_STATIONS;
SELECT * FROM RAW_STATIONS LIMIT 10;

-- ============================================================
-- LOAD INVENTORY
-- ============================================================
-- Fixed-width format:
-- Cols 1-11:  Station ID
-- Cols 13-20: Latitude
-- Cols 22-30: Longitude
-- Cols 32-35: Element
-- Cols 37-40: First year
-- Cols 42-45: Last year

COPY INTO RAW_INVENTORY (STATION_ID, LATITUDE, LONGITUDE, ELEMENT, FIRST_YEAR, LAST_YEAR)
FROM (
    SELECT
        SUBSTR($1, 1, 11) AS STATION_ID,
        TRY_TO_DOUBLE(TRIM(SUBSTR($1, 13, 8))) AS LATITUDE,
        TRY_TO_DOUBLE(TRIM(SUBSTR($1, 22, 9))) AS LONGITUDE,
        TRIM(SUBSTR($1, 32, 4)) AS ELEMENT,
        TRY_TO_NUMBER(TRIM(SUBSTR($1, 37, 4))) AS FIRST_YEAR,
        TRY_TO_NUMBER(TRIM(SUBSTR($1, 42, 4))) AS LAST_YEAR
    FROM @STG_NOAA_GHCN/ghcnd-inventory.txt
)
FILE_FORMAT = FF_GHCN_TEXT
ON_ERROR = 'CONTINUE';

-- Verify
SELECT COUNT(*) AS row_count FROM RAW_INVENTORY;
SELECT * FROM RAW_INVENTORY LIMIT 10;

-- ============================================================
-- METADATA LOAD SUMMARY
-- ============================================================

SELECT 'RAW_COUNTRIES' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW_COUNTRIES
UNION ALL
SELECT 'RAW_STATES', COUNT(*) FROM RAW_STATES
UNION ALL
SELECT 'RAW_STATIONS', COUNT(*) FROM RAW_STATIONS
UNION ALL
SELECT 'RAW_INVENTORY', COUNT(*) FROM RAW_INVENTORY;
