-- ============================================================
-- GHCN Daily Data - Snowflake Database Setup
-- ============================================================
-- Run this script first to create the database and schemas

-- Create database
CREATE DATABASE IF NOT EXISTS WEATHER_ANALYTICS;

USE DATABASE WEATHER_ANALYTICS;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;          -- Raw data as-is from source
CREATE SCHEMA IF NOT EXISTS STAGING;      -- Intermediate transformations
CREATE SCHEMA IF NOT EXISTS ANALYTICS;    -- Final analytics-ready tables

-- Grant usage (adjust roles as needed)
-- GRANT USAGE ON DATABASE WEATHER_ANALYTICS TO ROLE <your_role>;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE WEATHER_ANALYTICS TO ROLE <your_role>;

-- Verify setup
SHOW SCHEMAS IN DATABASE WEATHER_ANALYTICS;
