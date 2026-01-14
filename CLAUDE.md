# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

skyra-atmos is a data analytics project focused on exploring and processing NOAA Global Historical Climatology Network (GHCN) weather data. The project uses public S3 datasets, with planned integration for Snowflake data warehousing and Claude AI capabilities.

## Technology Stack

- **Python 3.x** with Jupyter notebooks for data exploration
- **Data handling**: pandas, pyarrow, s3fs
- **Data source**: NOAA GHCN public dataset on S3 (`s3://noaa-ghcn-pds/`)
- **Planned integrations**: Snowflake, dbt, Anthropic Claude API

## Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install core dependencies
pip install pandas pyarrow s3fs fsspec

# Optional for full stack
pip install snowflake-connector-python anthropic
```

Copy `.env.example` to `.env` and configure credentials.

## Data Architecture

### NOAA GHCN Dataset Structure
- **Metadata files**: Station info, country/state codes, inventory
- **Time-series data**: Parquet files partitioned by year (1800s-2023)
- **Key elements**: TMAX, TMIN, TAVG, PRCP, SNOW, SNWD, AWND
- **Quality flags**: M_FLAG (measurement), Q_FLAG (quality), S_FLAG (source)

### S3 Data Access Pattern
```python
import pandas as pd
# Metadata (CSV)
df = pd.read_csv('s3://noaa-ghcn-pds/ghcnd-stations.txt', ...)
# Time-series (Parquet)
df = pd.read_parquet('s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/')
```

## Project Structure

- `notebooks/` - Jupyter notebooks for data exploration
  - `01_metadata_exploration.ipynb` - Station metadata and inventory analysis
  - `02_by_year_exploration.ipynb` - Time-series climate data analysis
- `.env.example` - Environment variables template (AWS, Snowflake, Anthropic)

## Future Development

The .gitignore indicates planned dbt integration (`dbt/target/`, `dbt/logs/`, `dbt/dbt_packages/`) for data transformations.
