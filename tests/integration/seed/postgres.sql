-- Seed data for PostgreSQL integration tests
-- Covers: multi-dialect events, snowflake-style MRR/deals/sales_reps

CREATE SCHEMA IF NOT EXISTS analytics;

-- Multi-dialect: events table
CREATE TABLE analytics.events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    country TEXT,
    platform TEXT NOT NULL,
    revenue_cents INTEGER DEFAULT 0
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

-- Snowflake-compatible: MRR data (using lowercase for postgres)
CREATE TABLE analytics.segment_mrr (
    deal_key TEXT NOT NULL,
    crm_deal_id TEXT,
    employee_id TEXT,
    employee_month_key TEXT,
    mrr_source TEXT,
    report_month DATE,
    change_reason TEXT,
    change_category_group TEXT,
    customer_type TEXT,
    product_line TEXT,
    product_edition TEXT,
    net_mrr_per_deal NUMERIC(12,2),
    is_self_service BOOLEAN DEFAULT FALSE,
    geography TEXT,
    segment TEXT,
    delta_mrr NUMERIC(12,2),
    is_qualified BOOLEAN DEFAULT TRUE
);

INSERT INTO analytics.segment_mrr VALUES
('dk001', 'crm001', 'emp01', 'emp01_2025-01', 'PAST MRR', '2025-01-01', 'New Business', 'Rep Driven', 'New', 'Core', 'Professional', 5000, FALSE, 'North America', 'Mid Market', 5000, TRUE),
('dk002', 'crm002', 'emp02', 'emp02_2025-01', 'PAST MRR', '2025-01-01', 'Expansion',   'Rep Driven', 'Install Base', 'Core', 'Enterprise', 12000, FALSE, 'EMEA', 'Corporate', 3000, TRUE),
('dk003', 'crm003', 'emp01', 'emp01_2025-01', 'PAST MRR', '2025-01-01', 'New Business', 'Other Acquisition', 'New', 'Platform', 'Starter', 800, TRUE, 'North America', 'Small Business', 800, TRUE),
('dk004', 'crm004', 'emp03', 'emp03_2025-02', 'PAST MRR', '2025-02-01', 'New Business', 'Rep Driven', 'New', 'Core', 'Professional', 7500, FALSE, 'APAC', 'Mid Market', 7500, TRUE),
('dk005', 'crm005', 'emp02', 'emp02_2025-02', 'PAST MRR', '2025-02-01', 'Expansion',   NULL, 'Install Base', 'Core', 'Enterprise', 15000, FALSE, 'EMEA', 'Corporate', 2000, TRUE),
('dk006', 'crm006', 'emp04', 'emp04_2025-02', 'FUTURE MRR', '2025-02-01', 'Pipeline',  NULL, 'New', 'Platform', 'Professional', 3000, FALSE, 'North America', 'Mid Market', 3000, FALSE);

-- Sales directory
CREATE TABLE analytics.sales_directory (
    employee_month_key TEXT PRIMARY KEY,
    employee_id TEXT,
    full_name TEXT,
    manager_name TEXT,
    geography TEXT,
    segment TEXT,
    report_month DATE,
    geography_target NUMERIC(12,2),
    segment_target NUMERIC(12,2)
);

INSERT INTO analytics.sales_directory VALUES
('emp01_2025-01', 'emp01', 'Alice Smith',   'Bob Manager',   'North America', 'Mid Market',      '2025-01-01', 50000, 30000),
('emp02_2025-01', 'emp02', 'Charlie Brown', 'Diana Director','EMEA',          'Corporate',       '2025-01-01', 80000, 60000),
('emp01_2025-02', 'emp01', 'Alice Smith',   'Bob Manager',   'North America', 'Mid Market',      '2025-02-01', 50000, 30000),
('emp02_2025-02', 'emp02', 'Charlie Brown', 'Diana Director','EMEA',          'Corporate',       '2025-02-01', 80000, 60000),
('emp03_2025-02', 'emp03', 'Eve Johnson',   'Bob Manager',   'APAC',          'Mid Market',      '2025-02-01', 40000, 25000),
('emp04_2025-02', 'emp04', 'Frank Wilson',  'Diana Director','North America', 'Mid Market',      '2025-02-01', 45000, 28000);

-- Deal attributes
CREATE TABLE analytics.deal_attributes (
    deal_key TEXT PRIMARY KEY,
    primary_source TEXT
);

INSERT INTO analytics.deal_attributes VALUES
('dk001', 'Rep Driven - Outbound'),
('dk002', 'Partner - Referral'),
('dk003', 'eCommerce - Self Serve'),
('dk004', 'BDR - Inbound'),
('dk005', 'Partner - Co-sell'),
('dk006', 'Rep Driven - Events');

-- Company targets
CREATE TABLE analytics.company_targets (
    target_month DATE PRIMARY KEY,
    company_target NUMERIC(12,2)
);

INSERT INTO analytics.company_targets VALUES
('2025-01-01', 100000),
('2025-02-01', 120000);
