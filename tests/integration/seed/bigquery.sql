-- BigQuery seed data for integration tests
-- Run this against your GCP project to create the test dataset and table.
--
-- Usage:
--   bq query --use_legacy_sql=false < tests/integration/seed/bigquery.sql
--
-- Or paste into the BigQuery console SQL editor.
--
-- Requires: GCP project with BigQuery API enabled
-- Creates: dataset `analytics` with table `events`
--
-- The dataset name matches the table prefix in the view files (table: analytics.events)
-- so queries resolve correctly when the project is set as default.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE TABLE analytics.events (
    event_id STRING NOT NULL,
    event_type STRING NOT NULL,
    user_id STRING NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    country STRING,
    platform STRING NOT NULL,
    revenue_cents INT64 DEFAULT 0
);

INSERT INTO analytics.events (event_id, event_type, user_id, created_at, country, platform, revenue_cents) VALUES
('e001', 'page_view', 'u1', '2025-01-15 10:00:00+00', 'US', 'web', 0),
('e002', 'click',     'u1', '2025-01-15 10:05:00+00', 'US', 'web', 0),
('e003', 'purchase',  'u1', '2025-01-15 10:10:00+00', 'US', 'web', 4999),
('e004', 'page_view', 'u2', '2025-01-15 11:00:00+00', 'UK', 'ios', 0),
('e005', 'purchase',  'u2', '2025-01-15 11:05:00+00', 'UK', 'ios', 2500),
('e006', 'signup',    'u3', '2025-01-16 09:00:00+00', 'DE', 'android', 0),
('e007', 'page_view', 'u3', '2025-01-16 09:05:00+00', 'DE', 'android', 0),
('e008', 'click',     'u4', '2025-01-16 14:00:00+00', 'US', 'web', 0),
('e009', 'purchase',  'u4', '2025-01-16 14:30:00+00', 'US', 'web', 9999),
('e010', 'page_view', 'u5', '2025-01-17 08:00:00+00', 'JP', 'web', 0),
('e011', 'purchase',  'u5', '2025-01-17 08:15:00+00', 'JP', 'web', 1500),
('e012', 'click',     'u1', '2025-01-17 16:00:00+00', 'US', 'ios', 0);
