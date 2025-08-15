-- =====================================================
-- OLIST E-COMMERCE DATA WAREHOUSE INITIALIZATION
-- =====================================================

-- Create schemas for different layers of data pipeline
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS data_quality;

-- Add schema comments
COMMENT ON SCHEMA raw_data IS 'Raw data layer - unchanged data from source CSV files';
COMMENT ON SCHEMA staging IS 'Staging layer - cleaned and standardized data with business logic';
COMMENT ON SCHEMA analytics IS 'Analytics layer - aggregated data marts and dimensional models';
COMMENT ON SCHEMA data_quality IS 'Data quality checks and monitoring tables';

-- Grant permissions to olist_user
GRANT ALL PRIVILEGES ON SCHEMA raw_data TO olist_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO olist_user;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO olist_user;
GRANT ALL PRIVILEGES ON SCHEMA data_quality TO olist_user;

-- Grant default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL ON TABLES TO olist_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO olist_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO olist_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_quality GRANT ALL ON TABLES TO olist_user;

-- Set search path for olist_user
ALTER USER olist_user SET search_path TO analytics, staging, raw_data, data_quality, public;

-- =====================================================
-- CREATE DATA QUALITY MONITORING TABLES
-- =====================================================

-- Table to track data loads
CREATE TABLE IF NOT EXISTS data_quality.load_history (
    load_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(50) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rows_loaded INTEGER,
    load_status VARCHAR(20),
    error_message TEXT,
    load_duration_seconds NUMERIC(10,2)
);

-- Table to track data quality metrics
CREATE TABLE IF NOT EXISTS data_quality.quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(100),
    metric_name VARCHAR(100),
    metric_value NUMERIC,
    metric_status VARCHAR(20),
    details JSONB
);

-- =====================================================
-- CREATE UTILITY FUNCTIONS
-- =====================================================

-- Function to calculate table statistics
CREATE OR REPLACE FUNCTION data_quality.get_table_stats(
    schema_name TEXT,
    table_name TEXT
)
RETURNS TABLE (
    row_count BIGINT,
    column_count INTEGER,
    table_size TEXT,
    last_analyzed TIMESTAMP
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        n_live_tup::BIGINT as row_count,
        (SELECT COUNT(*)::INTEGER 
         FROM information_schema.columns 
         WHERE table_schema = schema_name 
           AND information_schema.columns.table_name = get_table_stats.table_name) as column_count,
        pg_size_pretty(pg_total_relation_size(schema_name||'.'||table_name)) as table_size,
        last_analyze as last_analyzed
    FROM pg_stat_user_tables
    WHERE schemaname = schema_name 
      AND pg_stat_user_tables.tablename = get_table_stats.table_name;
END;
$$;

-- Function to check for duplicate orders
CREATE OR REPLACE FUNCTION data_quality.check_duplicates(
    schema_name TEXT,
    table_name TEXT,
    key_column TEXT
)
RETURNS TABLE (
    duplicate_count INTEGER,
    total_duplicates INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    EXECUTE format('
        SELECT 
            COUNT(*)::INTEGER as duplicate_count,
            SUM(cnt - 1)::INTEGER as total_duplicates
        FROM (
            SELECT %I, COUNT(*) as cnt
            FROM %I.%I
            GROUP BY %I
            HAVING COUNT(*) > 1
        ) t',
        key_column, schema_name, table_name, key_column
    );
END;
$$;

-- Function to profile date columns
CREATE OR REPLACE FUNCTION data_quality.profile_date_column(
    schema_name TEXT,
    table_name TEXT,
    date_column TEXT
)
RETURNS TABLE (
    min_date DATE,
    max_date DATE,
    date_range_days INTEGER,
    null_count BIGINT,
    null_percentage NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    EXECUTE format('
        SELECT 
            MIN(%I)::DATE as min_date,
            MAX(%I)::DATE as max_date,
            (MAX(%I)::DATE - MIN(%I)::DATE)::INTEGER as date_range_days,
            COUNT(*) FILTER (WHERE %I IS NULL) as null_count,
            ROUND(COUNT(*) FILTER (WHERE %I IS NULL) * 100.0 / COUNT(*), 2) as null_percentage
        FROM %I.%I',
        date_column, date_column, date_column, date_column, 
        date_column, date_column, schema_name, table_name
    );
END;
$$;

-- =====================================================
-- CREATE INDEXES FOR BETTER PERFORMANCE
-- =====================================================

-- Note: These will be created after data is loaded
-- Including them here as documentation

/*
-- Customers table indexes
CREATE INDEX idx_customers_unique_id ON raw_data.customers(customer_unique_id);
CREATE INDEX idx_customers_state ON raw_data.customers(customer_state);
CREATE INDEX idx_customers_zip ON raw_data.customers(customer_zip_code_prefix);

-- Orders table indexes
CREATE INDEX idx_orders_customer_id ON raw_data.orders(customer_id);
CREATE INDEX idx_orders_status ON raw_data.orders(order_status);
CREATE INDEX idx_orders_purchase_date ON raw_data.orders(order_purchase_timestamp);
CREATE INDEX idx_orders_delivery_date ON raw_data.orders(order_delivered_customer_date);

-- Order items table indexes
CREATE INDEX idx_order_items_order_id ON raw_data.order_items(order_id);
CREATE INDEX idx_order_items_product_id ON raw_data.order_items(product_id);
CREATE INDEX idx_order_items_seller_id ON raw_data.order_items(seller_id);

-- Products table indexes
CREATE INDEX idx_products_category ON raw_data.products(product_category_name);

-- Sellers table indexes
CREATE INDEX idx_sellers_state ON raw_data.sellers(seller_state);
CREATE INDEX idx_sellers_zip ON raw_data.sellers(seller_zip_code_prefix);

-- Reviews table indexes
CREATE INDEX idx_reviews_order_id ON raw_data.order_reviews(order_id);
CREATE INDEX idx_reviews_score ON raw_data.order_reviews(review_score);
CREATE INDEX idx_reviews_creation_date ON raw_data.order_reviews(review_creation_date);

-- Payments table indexes
CREATE INDEX idx_payments_order_id ON raw_data.order_payments(order_id);
CREATE INDEX idx_payments_type ON raw_data.order_payments(payment_type);

-- Geolocation indexes
CREATE INDEX idx_geolocation_zip ON raw_data.geolocation(geolocation_zip_code_prefix);
CREATE INDEX idx_geolocation_state ON raw_data.geolocation(geolocation_state);
*/

-- =====================================================
-- CREATE MATERIALIZED VIEWS FOR ANALYTICS
-- =====================================================

-- Note: Create these after data is loaded

/*
-- Daily order summary
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_daily_orders AS
SELECT 
    DATE(order_purchase_timestamp) as order_date,
    COUNT(*) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) as delivered_orders,
    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) as cancelled_orders,
    AVG(EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp))/86400)::NUMERIC(10,2) as avg_delivery_days
FROM raw_data.orders
GROUP BY DATE(order_purchase_timestamp);

-- Customer lifetime value
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_customer_ltv AS
SELECT 
    c.customer_unique_id,
    c.customer_state,
    COUNT(DISTINCT o.order_id) as total_orders,
    MIN(o.order_purchase_timestamp) as first_order_date,
    MAX(o.order_purchase_timestamp) as last_order_date,
    SUM(oi.price + oi.freight_value) as lifetime_value,
    AVG(r.review_score) as avg_review_score
FROM raw_data.customers c
JOIN raw_data.orders o ON c.customer_id = o.customer_id
LEFT JOIN raw_data.order_items oi ON o.order_id = oi.order_id
LEFT JOIN raw_data.order_reviews r ON o.order_id = r.order_id
GROUP BY c.customer_unique_id, c.customer_state;

-- Product category performance
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_category_performance AS
SELECT 
    COALESCE(pt.product_category_name_english, p.product_category_name) as category,
    COUNT(DISTINCT oi.order_id) as order_count,
    COUNT(DISTINCT oi.product_id) as unique_products,
    SUM(oi.price) as total_revenue,
    AVG(oi.price) as avg_price,
    AVG(r.review_score) as avg_review_score,
    RANK() OVER (ORDER BY SUM(oi.price) DESC) as revenue_rank
FROM raw_data.products p
LEFT JOIN raw_data.product_category_translation pt ON p.product_category_name = pt.product_category_name
JOIN raw_data.order_items oi ON p.product_id = oi.product_id
LEFT JOIN raw_data.order_reviews r ON oi.order_id = r.order_id
WHERE p.product_category_name IS NOT NULL
GROUP BY COALESCE(pt.product_category_name_english, p.product_category_name);
*/

-- =====================================================
-- CREATE STORED PROCEDURES FOR DATA PROCESSING
-- =====================================================

-- Procedure to refresh all materialized views
CREATE OR REPLACE PROCEDURE analytics.refresh_all_materialized_views()
LANGUAGE plpgsql
AS $$
DECLARE
    mv_record RECORD;
    refresh_count INTEGER := 0;
BEGIN
    FOR mv_record IN 
        SELECT schemaname, matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'analytics'
    LOOP
        EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I', 
                      mv_record.schemaname, mv_record.matviewname);
        refresh_count := refresh_count + 1;
        RAISE NOTICE 'Refreshed view: %.%', mv_record.schemaname, mv_record.matviewname;
    END LOOP;
    
    RAISE NOTICE 'Total materialized views refreshed: %', refresh_count;
END;
$$;

-- Procedure to analyze all tables in a schema
CREATE OR REPLACE PROCEDURE data_quality.analyze_schema_tables(schema_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    table_record RECORD;
    analyze_count INTEGER := 0;
BEGIN
    FOR table_record IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = schema_name
    LOOP
        EXECUTE format('ANALYZE %I.%I', schema_name, table_record.tablename);
        analyze_count := analyze_count + 1;
    END LOOP;
    
    RAISE NOTICE 'Analyzed % tables in schema %', analyze_count, schema_name;
END;
$$;

-- =====================================================
-- DATA VALIDATION RULES
-- =====================================================

-- Create a table to store validation rules
CREATE TABLE IF NOT EXISTS data_quality.validation_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    rule_sql TEXT NOT NULL,
    expected_result TEXT,
    severity VARCHAR(20) DEFAULT 'WARNING',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert basic validation rules
INSERT INTO data_quality.validation_rules (rule_name, table_name, rule_sql, expected_result, severity)
VALUES 
    ('Orders should have valid customer_id', 'orders', 
     'SELECT COUNT(*) FROM raw_data.orders WHERE customer_id IS NULL', 
     '0', 'ERROR'),
    
    ('Order items should have positive prices', 'order_items',
     'SELECT COUNT(*) FROM raw_data.order_items WHERE price <= 0',
     '0', 'ERROR'),
    
    ('Review scores should be between 1 and 5', 'order_reviews',
     'SELECT COUNT(*) FROM raw_data.order_reviews WHERE review_score NOT BETWEEN 1 AND 5',
     '0', 'ERROR'),
    
    ('Order dates should be reasonable', 'orders',
     'SELECT COUNT(*) FROM raw_data.orders WHERE order_purchase_timestamp < ''2016-01-01'' OR order_purchase_timestamp > ''2020-12-31''',
     '0', 'WARNING'),
    
    ('Delivered orders should have delivery date', 'orders',
     'SELECT COUNT(*) FROM raw_data.orders WHERE order_status = ''delivered'' AND order_delivered_customer_date IS NULL',
     '0', 'WARNING');

-- =====================================================
-- GRANT PERMISSIONS FOR ALL OBJECTS
-- =====================================================

-- Grant permissions on all functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA data_quality TO olist_user;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA analytics TO olist_user;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA data_quality TO olist_user;

-- =====================================================
-- FINAL SETUP MESSAGE
-- =====================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'OLIST DATA WAREHOUSE INITIALIZATION COMPLETE';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Schemas created: raw_data, staging, analytics, data_quality';
    RAISE NOTICE 'User permissions: Granted to olist_user';
    RAISE NOTICE 'Utility functions: Created in data_quality schema';
    RAISE NOTICE 'Validation rules: Loaded in data_quality.validation_rules';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Load data using the Python script';
    RAISE NOTICE '2. Create indexes after data load';
    RAISE NOTICE '3. Create and refresh materialized views';
    RAISE NOTICE '4. Run data quality checks';
    RAISE NOTICE '========================================';
END $$;