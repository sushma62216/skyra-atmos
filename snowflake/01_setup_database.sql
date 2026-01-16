-- ============================================================
-- GHCN Daily Data - Snowflake Database Setup
-- ============================================================
-- Run this script first to create the warehouse, database, and schemas

-- ============================================================
-- WAREHOUSE
-- ============================================================

-- Create warehouse for compute
CREATE WAREHOUSE IF NOT EXISTS SKYRA_ATMOS
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60           -- Suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE          -- Resume automatically when queries run
    INITIALLY_SUSPENDED = TRUE; -- Start suspended to avoid charges

-- Use the warehouse
USE WAREHOUSE SKYRA_ATMOS;

-- ============================================================
-- DATABASE
-- ============================================================

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
