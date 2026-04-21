# dbt Project Setup — Design Spec
**Date:** 2026-04-21
**Status:** Approved

## Overview

Initialize a dbt project (`weather_analytics`) inside `skyra-atmos/dbt/` that connects to the existing Snowflake instance and declares the 5 RAW tables as dbt sources. No models are written in this step — the deliverable is a working, connected, compilable scaffold.

## Project Structure

```
skyra-atmos/
└── dbt/
    └── weather_analytics/
        ├── dbt_project.yml         # Project config, schema mappings
        ├── packages.yml            # dbt packages (dbt-utils)
        ├── models/
        │   ├── staging/            # STAGING schema output (models added in Step 2)
        │   │   └── sources.yml     # Declares RAW tables as dbt sources
        │   └── analytics/          # ANALYTICS schema output (models added in Step 3)
        ├── tests/                  # Custom data tests (added in Step 2+)
        ├── macros/                 # Reusable SQL macros (added later)
        └── README.md
```

`profiles.yml` lives at `~/.dbt/profiles.yml` — never committed to git (contains Snowflake credentials via env vars).

## Configuration

### `dbt_project.yml`
- Project name: `weather_analytics`
- Profile: `weather_analytics`
- `staging/` folder → `STAGING` schema, materialized as **views**
- `analytics/` folder → `ANALYTICS` schema, materialized as **tables**

### `~/.dbt/profiles.yml`
- Target: `dev`
- Type: `snowflake`
- Credentials via `env_var()` (pulls from `.env`): `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`
- Database: `WEATHER_ANALYTICS`
- Warehouse: `SKYRA_ATMOS`
- Default schema: `STAGING`

### `models/staging/sources.yml`
Declares all 5 existing RAW tables as dbt sources:
- `RAW_STATIONS`
- `RAW_COUNTRIES`
- `RAW_STATES`
- `RAW_INVENTORY`
- `RAW_DAILY_OBSERVATIONS`

## Success Criteria

Step 1 is complete when all three commands pass:

```bash
cd skyra-atmos/dbt/weather_analytics

dbt debug           # connection is valid
dbt source freshness  # all 5 RAW sources visible
dbt compile         # no compilation errors
```

## Out of Scope (covered in later steps)

- STAGING models (Step 2)
- ANALYTICS dimensional models / fact tables (Step 3)
- Data marts (Step 4)
- dbt tests and documentation (Step 2+)
- dbt macros (as needed)

## Tech Stack

- dbt-core + dbt-snowflake (already installed)
- Snowflake: `WEATHER_ANALYTICS` database, `SKYRA_ATMOS` warehouse
- Schemas: RAW (source), STAGING (output), ANALYTICS (output)
