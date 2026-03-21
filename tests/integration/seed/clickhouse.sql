-- Seed data for ClickHouse integration tests

CREATE DATABASE IF NOT EXISTS analytics;

-- Events table (multi-dialect)
CREATE TABLE IF NOT EXISTS analytics.events (
    event_id String,
    event_type String,
    user_id String,
    created_at DateTime DEFAULT now(),
    country Nullable(String),
    platform String,
    revenue_cents Int32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY event_id;

INSERT INTO analytics.events (event_id, event_type, user_id, created_at, country, platform, revenue_cents) VALUES
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

-- Orders table
CREATE TABLE IF NOT EXISTS analytics.orders (
    guid String,
    restaurant_id String,
    business_date String,
    opened_date DateTime DEFAULT now(),
    closed_date Nullable(DateTime),
    source String,
    approval_status String DEFAULT 'APPROVED',
    voided UInt8 DEFAULT 0,
    deleted UInt8 DEFAULT 0,
    number_of_guests Int32 DEFAULT 1,
    duration Int32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY guid;

INSERT INTO analytics.orders (guid, restaurant_id, business_date, opened_date, closed_date, source, approval_status, voided, deleted, number_of_guests, duration) VALUES
('ord001', 'rest01', '2025-01-15', '2025-01-15 12:00:00', '2025-01-15 13:00:00', 'POS',         'APPROVED', 0, 0, 4, 3600),
('ord002', 'rest01', '2025-01-15', '2025-01-15 18:00:00', '2025-01-15 19:30:00', 'POS',         'APPROVED', 0, 0, 2, 5400),
('ord003', 'rest02', '2025-01-15', '2025-01-15 12:30:00', '2025-01-15 13:15:00', 'Online',      'APPROVED', 0, 0, 1, 2700),
('ord004', 'rest02', '2025-01-16', '2025-01-16 19:00:00', '2025-01-16 20:00:00', 'Mobile',      'APPROVED', 1, 0, 3, 3600),
('ord005', 'rest03', '2025-01-16', '2025-01-16 11:00:00', '2025-01-16 11:45:00', 'Third Party', 'APPROVED', 0, 0, 2, 2700),
('ord006', 'rest01', '2025-01-17', '2025-01-17 13:00:00', '2025-01-17 14:00:00', 'POS',         'APPROVED', 0, 1, 5, 3600),
('ord007', 'rest03', '2025-01-17', '2025-01-17 20:00:00', '2025-01-17 21:30:00', 'Online',      'APPROVED', 0, 0, 4, 5400),
('ord008', 'rest02', '2025-01-18', '2025-01-18 12:00:00', '2025-01-18 13:00:00', 'POS',         'APPROVED', 0, 0, 2, 3600);

-- Restaurants table
CREATE TABLE IF NOT EXISTS analytics.restaurants (
    guid String,
    name String,
    location_name String DEFAULT '',
    location_code String DEFAULT '',
    city String DEFAULT '',
    state_code String DEFAULT '',
    zip_code String DEFAULT '',
    country String DEFAULT 'US',
    time_zone String DEFAULT 'America/New_York',
    latitude Float64 DEFAULT 0,
    longitude Float64 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY guid;

INSERT INTO analytics.restaurants (guid, name, location_name, location_code, city, state_code, zip_code, country) VALUES
('rest01', 'The Italian Place',  'Downtown',  'DT-001', 'New York',     'NY', '10001', 'US'),
('rest02', 'Sushi Garden',       'Midtown',   'MT-001', 'New York',     'NY', '10019', 'US'),
('rest03', 'Taco Express',       'West Side', 'WS-001', 'Los Angeles',  'CA', '90001', 'US');

-- Sales daily metrics
CREATE TABLE IF NOT EXISTS analytics.sales_daily_metrics (
    restaurant_id String,
    business_date String,
    gross_sales Decimal(12,2) DEFAULT 0,
    net_sales Decimal(12,2) DEFAULT 0,
    total_tax Decimal(12,2) DEFAULT 0,
    total_discounts Decimal(12,2) DEFAULT 0,
    total_tips Decimal(12,2) DEFAULT 0,
    order_count Int32 DEFAULT 0,
    guest_count Int32 DEFAULT 0,
    avg_check_amount Decimal(12,2) DEFAULT 0,
    avg_order_value Decimal(12,2) DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (restaurant_id, business_date);

INSERT INTO analytics.sales_daily_metrics VALUES
('rest01', '2025-01-15', 2500.00, 2200.00, 200.00, 100.00, 350.00, 25, 60, 88.00, 100.00),
('rest01', '2025-01-16', 1800.00, 1600.00, 150.00,  50.00, 250.00, 18, 40, 88.89, 100.00),
('rest01', '2025-01-17', 3200.00, 2900.00, 250.00,  50.00, 480.00, 30, 75, 96.67, 106.67),
('rest02', '2025-01-15', 1500.00, 1350.00, 120.00,  30.00, 200.00, 15, 30, 90.00, 100.00),
('rest02', '2025-01-16', 2000.00, 1800.00, 160.00,  40.00, 280.00, 20, 45, 90.00, 100.00),
('rest02', '2025-01-18', 2800.00, 2500.00, 220.00,  80.00, 400.00, 28, 65, 89.29, 100.00),
('rest03', '2025-01-16', 1200.00, 1100.00, 100.00,   0.00, 150.00, 12, 25, 91.67, 100.00),
('rest03', '2025-01-17', 1600.00, 1450.00, 130.00,  20.00, 220.00, 16, 35, 90.63, 100.00);
