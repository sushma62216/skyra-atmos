# weather_analytics dbt Project

Transforms raw NOAA GHCN climate data from Snowflake RAW schema into
analytics-ready models in the STAGING and ANALYTICS schemas.

## Setup

1. Install dependencies: `dbt deps`
2. Ensure `~/.dbt/profiles.yml` has a `weather_analytics` profile (see design spec)
3. Load env vars: `source .env` from the repo root
4. Verify connection: `dbt debug`

## Schema Layout

| Schema     | Purpose                        | Materialization |
|------------|--------------------------------|-----------------|
| RAW        | Source data (managed by Airflow) | External        |
| STAGING    | Cleaned, typed, documented     | Views           |
| ANALYTICS  | Dimensional models, fact tables | Tables          |

## Commands

```bash
dbt deps              # install packages
dbt debug             # test Snowflake connection
dbt source freshness  # check RAW table recency
dbt compile           # validate all SQL compiles
dbt run               # run all models
dbt test              # run all tests
dbt docs generate && dbt docs serve  # browse documentation
```

## Sources

All source tables live in the `RAW` schema and are declared in
`models/staging/sources.yml`. They are loaded by the Airflow DAG
`ghcn_daily_incremental`.
