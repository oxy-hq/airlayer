CREATE SCHEMA IF NOT EXISTS workspace.airlayer_test;

CREATE OR REPLACE TABLE workspace.airlayer_test.events (
    event_id STRING,
    event_type STRING NOT NULL,
    user_id STRING NOT NULL,
    created_at TIMESTAMP,
    country STRING,
    platform STRING NOT NULL,
    revenue_cents INT
);

INSERT INTO workspace.airlayer_test.events VALUES
('e001', 'page_view', 'u1', '2025-01-15 10:00:00', 'US', 'web', 0),
('e002', 'click',     'u1', '2025-01-15 10:05:00', 'US', 'web', 0),
('e003', 'purchase',  'u1', '2025-01-15 10:10:00', 'US', 'web', 4999),
('e004', 'page_view', 'u2', '2025-01-15 11:00:00', 'UK', 'ios', 0),
('e005', 'purchase',  'u2', '2025-01-15 11:05:00', 'UK', 'ios', 2500),
('e006', 'signup',    'u3', '2025-01-16 09:00:00', 'DE', 'android', 0),
('e007', 'page_view', 'u3', '2025-01-16 09:05:00', 'DE', 'android', 0),
('e008', 'click',     'u4', '2025-01-16 14:00:00', 'US', 'web', 0),
('e009', 'purchase',  'u4', '2025-01-16 14:30:00', 'US', 'web', 9999),
('e010', 'page_view', 'u5', '2025-01-17 08:00:00', 'JP', 'web', 0),
('e011', 'purchase',  'u5', '2025-01-17 08:15:00', 'JP', 'web', 1500),
('e012', 'click',     'u1', '2025-01-17 16:00:00', 'US', 'ios', 0);
