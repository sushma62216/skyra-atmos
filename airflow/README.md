# Airflow Setup for GHCN Daily Refresh

## Overview

This Airflow DAG incrementally loads NOAA GHCN daily weather data into Snowflake.

**Schedule:** Daily at 1:00 AM EST (6:00 AM UTC)

## Flow

```
S3 (NOAA) → Staging Table → MERGE → RAW Table → Cleanup
```

1. Truncate staging table
2. Load current year from S3 to staging
3. MERGE only new records into RAW
4. Verify load
5. Cleanup staging

## Setup

### 1. Install Airflow

```bash
pip install -r requirements.txt
```

### 2. Configure Snowflake Connection

In Airflow UI: **Admin > Connections > Add**

| Field | Value |
|-------|-------|
| Connection Id | `snowflake_default` |
| Connection Type | Snowflake |
| Account | Your account identifier |
| Login | Your username |
| Password | Your password |
| Warehouse | `SKYRA_ATMOS` |
| Database | `WEATHER_ANALYTICS` |
| Schema | `RAW` |
| Role | Your role |

### 3. Create Staging Table in Snowflake

Run `snowflake/06_staging_table.sql` in Snowflake first.

### 4. Deploy DAG

Copy `dags/ghcn_daily_incremental.py` to your Airflow DAGs folder:

```bash
cp dags/ghcn_daily_incremental.py $AIRFLOW_HOME/dags/
```

### 5. Enable DAG

In Airflow UI, toggle the `ghcn_daily_incremental` DAG to ON.

## Monitoring

Check load results:

```sql
SELECT * FROM WEATHER_ANALYTICS.RAW.LOAD_TRACKING
ORDER BY LOAD_TIMESTAMP DESC;
```
