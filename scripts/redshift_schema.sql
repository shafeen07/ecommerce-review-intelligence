-- ============================================================================
-- FIXED REDSHIFT SCHEMA - Matches PySpark Parquet Output
-- Key Changes: Column names, data types, removed prev_avg_rating
-- ============================================================================

-- Drop existing tables if re-creating
DROP TABLE IF EXISTS agg_sentiment_trends CASCADE;
DROP TABLE IF EXISTS agg_competitive_analysis CASCADE;
DROP TABLE IF EXISTS agg_success_prediction CASCADE;
DROP TABLE IF EXISTS agg_review_velocity CASCADE;

-- ============================================================================
-- Table 1: Sentiment Trends Over Time (FIXED - VERIFIED WORKING)
-- ============================================================================
-- NOTE: year_month is VARCHAR because PySpark to_date() writes as string to Parquet
-- We'll convert to DATE after loading data
CREATE TABLE agg_sentiment_trends (
    store VARCHAR(100),
    year_month VARCHAR(20),            -- VARCHAR for CSV compatibility
    review_count INTEGER,
    avg_rating DECIMAL(10,2),          -- FIXED: (10,2) to handle float64 from Parquet
    positive_count INTEGER,
    negative_count INTEGER,
    neutral_count INTEGER,
    positive_rate DECIMAL(10,2),       -- FIXED: (10,2) for float64
    negative_rate DECIMAL(10,2),       -- FIXED: (10,2) for float64
    rating_change DECIMAL(10,3),       -- FIXED: (10,3) for precision
    trend_direction VARCHAR(20),
    PRIMARY KEY (store, year_month)
)
DISTSTYLE KEY
DISTKEY (store)
SORTKEY (year_month);

-- ============================================================================
-- Table 2: Competitive Product Analysis (FIXED - VERIFIED WORKING)
-- ============================================================================
CREATE TABLE agg_competitive_analysis (
    parent_asin VARCHAR(20) PRIMARY KEY,
    product_title VARCHAR(500),
    store VARCHAR(100),
    main_category VARCHAR(100),
    price_tier VARCHAR(20),
    total_reviews INTEGER,
    unique_reviewers INTEGER,
    avg_rating DECIMAL(10,2),          -- FIXED: (10,2) to handle float64
    rating_stddev DECIMAL(10,2),       -- FIXED: (10,2) to handle float64
    positive_reviews INTEGER,
    negative_reviews INTEGER,
    neutral_reviews INTEGER,
    satisfaction_rate DECIMAL(10,2),   -- FIXED: (10,2) for float64
    negative_rate DECIMAL(10,2),       -- FIXED: (10,2) for float64
    avg_review_length DECIMAL(10,2),   -- FIXED: (10,2) for float64
    detailed_reviews INTEGER,
    verified_purchases INTEGER,
    verified_rate DECIMAL(10,2),       -- FIXED: (10,2) for float64
    avg_helpful_votes DECIMAL(10,2),   -- FIXED: (10,2) for float64
    first_review_date DATE,
    last_review_date DATE,
    review_span_days INTEGER
)
DISTSTYLE KEY
DISTKEY (store)
SORTKEY (parent_asin);

-- ============================================================================
-- Table 3: Product Success Prediction (FIXED - VERIFIED WORKING)
-- ============================================================================
CREATE TABLE agg_success_prediction (
    parent_asin VARCHAR(20) PRIMARY KEY,
    product_title VARCHAR(500),
    store VARCHAR(100),
    success_score DECIMAL(10,2),       -- FIXED: (10,2) for float64
    success_category VARCHAR(20),
    satisfaction_rate DECIMAL(10,2),   -- FIXED: (10,2) for float64
    rating_stddev DECIMAL(10,2),       -- FIXED: (10,2) for float64
    verified_rate DECIMAL(10,2),       -- FIXED: (10,2) for float64
    review_velocity DECIMAL(10,2),     -- FIXED: (10,2) for float64
    engagement_score DECIMAL(10,2)     -- FIXED: (10,2) for float64
)
DISTSTYLE KEY
DISTKEY (store)
SORTKEY (success_score);

-- ============================================================================
-- Table 4: Review Velocity Analysis (FIXED - VERIFIED WORKING)
-- ============================================================================
CREATE TABLE agg_review_velocity (
    parent_asin VARCHAR(20),
    product_title VARCHAR(500),
    store VARCHAR(100),
    year_month VARCHAR(20),            -- VARCHAR for CSV compatibility
    monthly_reviews INTEGER,
    monthly_avg_rating DECIMAL(10,2), -- FIXED: (10,2) for float64
    prev_month_reviews INTEGER,        -- FIXED: INTEGER (was FLOAT from lag)
    velocity_change INTEGER,           -- FIXED: INTEGER
    velocity_change_pct DECIMAL(10,2), -- FIXED: (10,2) for float64
    momentum VARCHAR(20),
    PRIMARY KEY (parent_asin, year_month)
)
DISTSTYLE KEY
DISTKEY (parent_asin)
SORTKEY (year_month);

-- ============================================================================
-- Verify Tables Created
-- ============================================================================
SELECT table_name, column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name LIKE 'agg_%'
ORDER BY table_name, ordinal_position;

-- ============================================================================
-- LOAD DATA FROM S3 (USING CSV FILES - WORKAROUND FOR PARQUET COMPATIBILITY)
-- ============================================================================
-- NOTE: In production, Redshift should load Parquet directly from Spark.
-- We're using CSV as a workaround due to PySpark/Redshift Parquet schema compatibility.

-- Load Table 1: Sentiment Trends
COPY agg_sentiment_trends
FROM 's3://ecommerce-intelligence-sa/processed/sentiment_trends/sentiment_trends.csv'
IAM_ROLE 'arn:aws:iam::108782075105:role/service-role/AmazonRedshift-CommandsAccessRole-20251017T193609'
CSV
IGNOREHEADER 1
REGION 'us-east-1';

-- Verify load
SELECT COUNT(*) as total_rows FROM agg_sentiment_trends;
-- Expected: ~1,883 rows

SELECT store, year_month, review_count, avg_rating, trend_direction 
FROM agg_sentiment_trends 
ORDER BY year_month DESC 
LIMIT 10;

-- Load Table 2: Competitive Analysis
COPY agg_competitive_analysis
FROM 's3://ecommerce-intelligence-sa/processed/competitive_analysis/competitive_analysis.csv'
IAM_ROLE 'arn:aws:iam::108782075105:role/service-role/AmazonRedshift-CommandsAccessRole-20251017T193609'
CSV
IGNOREHEADER 1
REGION 'us-east-1';

-- Verify load
SELECT COUNT(*) FROM agg_competitive_analysis;
-- Expected: 47 rows (one per product)

SELECT parent_asin, product_title, store, total_reviews, satisfaction_rate
FROM agg_competitive_analysis
ORDER BY satisfaction_rate DESC
LIMIT 10;

-- Load Table 3: Success Prediction
COPY agg_success_prediction
FROM 's3://ecommerce-intelligence-sa/processed/prediction_features/prediction_features.csv'
IAM_ROLE 'arn:aws:iam::108782075105:role/service-role/AmazonRedshift-CommandsAccessRole-20251017T193609'
CSV
IGNOREHEADER 1
REGION 'us-east-1';

-- Verify load
SELECT COUNT(*) FROM agg_success_prediction;
-- Expected: 47 rows (one per product)

SELECT product_title, success_score, success_category
FROM agg_success_prediction
ORDER BY success_score DESC
LIMIT 10;

-- ============================================================================
-- PART 3: LOAD TABLE 4 - VELOCITY ANALYSIS (STAGING + TRANSFORM PATTERN)
-- ============================================================================
-- NOTE: This table requires a staging approach due to type conversion issues
-- in the CSV (e.g., '.' for NULL, '1.0' for integers)

-- Step 1: Create staging table with VARCHAR for all numeric columns
CREATE TABLE agg_review_velocity_staging (
    parent_asin VARCHAR(20),
    product_title VARCHAR(500),
    store VARCHAR(100),
    year_month VARCHAR(20),
    monthly_reviews VARCHAR(50),
    monthly_avg_rating VARCHAR(50),
    prev_month_reviews VARCHAR(50),
    velocity_change VARCHAR(50),
    velocity_change_pct VARCHAR(50),
    momentum VARCHAR(20)
);

-- Step 2: Load CSV into staging table
COPY agg_review_velocity_staging
FROM 's3://ecommerce-intelligence-sa/processed/velocity_analysis/velocity_analysis.csv'
IAM_ROLE 'arn:aws:iam::108782075105:role/service-role/AmazonRedshift-CommandsAccessRole-20251017T193609'
CSV
IGNOREHEADER 1
BLANKSASNULL  -- Convert blank cells to NULL
REGION 'us-east-1';

-- Verify staging load
SELECT COUNT(*) FROM agg_review_velocity_staging;
-- Expected: ~2,000-3,500 rows

SELECT * FROM agg_review_velocity_staging LIMIT 5;

-- Step 3: Create final table with proper data types
CREATE TABLE agg_review_velocity (
    parent_asin VARCHAR(20),
    product_title VARCHAR(500),
    store VARCHAR(100),
    year_month VARCHAR(20),
    monthly_reviews INTEGER,
    monthly_avg_rating DECIMAL(10,2),
    prev_month_reviews INTEGER,
    velocity_change INTEGER,
    velocity_change_pct DECIMAL(10,2),
    momentum VARCHAR(20),
    PRIMARY KEY (parent_asin, year_month)
)
DISTSTYLE KEY
DISTKEY (parent_asin)
SORTKEY (year_month);

-- Step 4: Transform and insert with type conversion
-- The ::DECIMAL::INTEGER pattern handles both NULL and '1.0' → 1 conversions
INSERT INTO agg_review_velocity
SELECT 
    parent_asin,
    product_title,
    store,
    year_month,
    monthly_reviews::INTEGER,
    monthly_avg_rating::DECIMAL(10,2),
    prev_month_reviews::DECIMAL::INTEGER,  -- Handles NULL and '1.0' → 1
    velocity_change::DECIMAL::INTEGER,     -- Handles NULL and '0.0' → 0
    velocity_change_pct::DECIMAL(10,2),
    momentum
FROM agg_review_velocity_staging;

-- Step 5: Verify final load
SELECT COUNT(*) FROM agg_review_velocity;
-- Expected: ~2,000-3,500 rows (products × months)

SELECT product_title, year_month, monthly_reviews, momentum
FROM agg_review_velocity
WHERE momentum IN ('Surging', 'Declining')
ORDER BY year_month DESC
LIMIT 10;

-- Step 6: Clean up staging table
DROP TABLE agg_review_velocity_staging;

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================

-- Check for NULL values in key columns
SELECT 
    'sentiment_trends' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) as null_dates,
    SUM(CASE WHEN avg_rating IS NULL THEN 1 ELSE 0 END) as null_ratings
FROM agg_sentiment_trends
UNION ALL
SELECT 
    'competitive_analysis',
    COUNT(*),
    SUM(CASE WHEN first_review_date IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN avg_rating IS NULL THEN 1 ELSE 0 END)
FROM agg_competitive_analysis;

-- Check data ranges
SELECT 
    'avg_rating' as metric,
    MIN(avg_rating) as min_value,
    MAX(avg_rating) as max_value,
    AVG(avg_rating) as avg_value
FROM agg_sentiment_trends
UNION ALL
SELECT 
    'positive_rate',
    MIN(positive_rate),
    MAX(positive_rate),
    AVG(positive_rate)
FROM agg_sentiment_trends;

-- ============================================================================
-- TROUBLESHOOTING QUERIES (if COPY fails)
-- ============================================================================

-- Check Parquet file metadata from S3
SELECT * FROM SVV_EXTERNAL_SCHEMAS LIMIT 5;

-- View detailed error messages
SELECT 
    query,
    filename,
    line_number,
    colname,
    type,
    raw_field_value,
    err_reason
FROM stl_load_errors
ORDER BY starttime DESC
LIMIT 10;

-- Check file structure with external table (if needed for debugging)
CREATE EXTERNAL SCHEMA spectrum_debug
FROM DATA CATALOG
DATABASE 'dev'
IAM_ROLE 'arn:aws:iam::108782075105:role/service-role/AmazonRedshift-CommandsAccessRole-20251017T193609'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Create external table to inspect Parquet schema
CREATE EXTERNAL TABLE spectrum_debug.sentiment_trends_external (
    store VARCHAR(100),
    year_month DATE,
    review_count INTEGER,
    avg_rating DECIMAL(5,2),
    positive_count INTEGER,
    negative_count INTEGER,
    neutral_count INTEGER,
    positive_rate DECIMAL(5,2),
    negative_rate DECIMAL(5,2),
    rating_change DECIMAL(5,3),
    trend_direction VARCHAR(20)
)
STORED AS PARQUET
LOCATION 's3://ecommerce-intelligence-sa/processed/sentiment_trends/';

-- Test external table
SELECT * FROM spectrum_debug.sentiment_trends_external LIMIT 5;

-- ============================================================================
-- GRANT PERMISSIONS (if needed for Bedrock/other services)
-- ============================================================================

-- Grant SELECT to IAM role for Bedrock queries
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO IAM_ROLE 'arn:aws:iam::108782075105:role/BedrockRedshiftRole';

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

-- Overall data summary
SELECT 
    'Sentiment Trends' as feature,
    COUNT(*) as row_count,
    COUNT(DISTINCT store) as unique_stores,
    MIN(year_month) as earliest_date,
    MAX(year_month) as latest_date
FROM agg_sentiment_trends
UNION ALL
SELECT 
    'Competitive Analysis',
    COUNT(*),
    COUNT(DISTINCT store),
    MIN(first_review_date),
    MAX(last_review_date)
FROM agg_competitive_analysis
UNION ALL
SELECT 
    'Success Prediction',
    COUNT(*),
    COUNT(DISTINCT store),
    NULL,
    NULL
FROM agg_success_prediction
UNION ALL
SELECT 
    'Review Velocity',
    COUNT(*),
    COUNT(DISTINCT store),
    MIN(year_month),
    MAX(year_month)
FROM agg_review_velocity;

-- ============================================================================
-- END OF FIXED SCHEMA
-- ============================================================================