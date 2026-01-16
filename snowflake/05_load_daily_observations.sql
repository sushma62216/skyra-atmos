-- ============================================================
-- GHCN Daily Data - Load Daily Observations
-- ============================================================
-- Loads daily climate observations from Parquet files
-- Data is partitioned by year in S3

USE WAREHOUSE SKYRA_ATMOS;
USE DATABASE WEATHER_ANALYTICS;
USE SCHEMA RAW;

-- ============================================================
-- OPTION 1: LOAD A SINGLE YEAR
-- ============================================================
-- Use this to test loading or load specific years

-- Example: Load year 2023
COPY INTO RAW_DAILY_OBSERVATIONS (ID, DATE, ELEMENT, DATA_VALUE, M_FLAG, Q_FLAG, S_FLAG, OBS_TIME, YEAR)
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
        2023 AS YEAR
    FROM @STG_NOAA_GHCN/parquet/by_year/YEAR=2023/
)
FILE_FORMAT = FF_GHCN_PARQUET
ON_ERROR = 'CONTINUE';

-- Verify
SELECT COUNT(*) AS row_count, MIN(DATE) AS min_date, MAX(DATE) AS max_date
FROM RAW_DAILY_OBSERVATIONS
WHERE YEAR = 2023;

-- ============================================================
-- OPTION 2: LOAD MULTIPLE YEARS (Procedure)
-- ============================================================
-- Creates a stored procedure to load a range of years

CREATE OR REPLACE PROCEDURE LOAD_GHCN_YEARS(START_YEAR INTEGER, END_YEAR INTEGER)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var results = [];
for (var year = START_YEAR; year <= END_YEAR; year++) {
    var sql = `
        COPY INTO RAW_DAILY_OBSERVATIONS (ID, DATE, ELEMENT, DATA_VALUE, M_FLAG, Q_FLAG, S_FLAG, OBS_TIME, YEAR)
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
                ${year} AS YEAR
            FROM @STG_NOAA_GHCN/parquet/by_year/YEAR=${year}/
        )
        FILE_FORMAT = FF_GHCN_PARQUET
        ON_ERROR = 'CONTINUE'
    `;
    try {
        var stmt = snowflake.createStatement({sqlText: sql});
        var res = stmt.execute();
        res.next();
        results.push("Year " + year + ": loaded successfully");
    } catch (err) {
        results.push("Year " + year + ": " + err.message);
    }
}
return results.join("\n");
$$;

-- ============================================================
-- LOAD EXAMPLES
-- ============================================================

-- Load last 5 years (2019-2023)
-- CALL LOAD_GHCN_YEARS(2019, 2023);

-- Load last 10 years (2014-2023)
-- CALL LOAD_GHCN_YEARS(2014, 2023);

-- Load all years (WARNING: This is ~224 years of data, will take time)
-- CALL LOAD_GHCN_YEARS(1800, 2023);

-- ============================================================
-- VERIFY LOAD
-- ============================================================

-- Check row counts by year
SELECT
    YEAR,
    COUNT(*) AS ROW_COUNT,
    COUNT(DISTINCT ID) AS STATION_COUNT,
    COUNT(DISTINCT ELEMENT) AS ELEMENT_COUNT
FROM RAW_DAILY_OBSERVATIONS
GROUP BY YEAR
ORDER BY YEAR DESC;

-- Total summary
SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT ID) AS TOTAL_STATIONS,
    COUNT(DISTINCT ELEMENT) AS TOTAL_ELEMENTS,
    MIN(YEAR) AS MIN_YEAR,
    MAX(YEAR) AS MAX_YEAR
FROM RAW_DAILY_OBSERVATIONS;
