-- Improved ETL Pipeline for Sales Data Warehouse

-- 1. Extract: Load raw sales data with validation
INSERT INTO raw_sales_staging (
    transaction_id, store_id, product_id, sale_date, 
    quantity, unit_price, customer_id, 
    is_valid, validation_errors, batch_id, load_timestamp
)
SELECT 
    transaction_id,
    store_id,
    product_id,
    sale_date,
    quantity,
    unit_price,
    customer_id,
    CASE 
        WHEN quantity <= 0 THEN 0
        WHEN unit_price <= 0 THEN 0
        WHEN sale_date > CURRENT_DATE THEN 0
        ELSE 1
    END as is_valid,
    CONCAT_WS(',',
        CASE WHEN quantity <= 0 THEN 'Invalid quantity' END,
        CASE WHEN unit_price <= 0 THEN 'Invalid price' END,
        CASE WHEN sale_date > CURRENT_DATE THEN 'Future date' END
    ) as validation_errors,
    @batch_id as batch_id,
    CURRENT_TIMESTAMP as load_timestamp
FROM source_system.sales_transactions
WHERE sale_date >= @last_loaded_date;

-- 2a. Transform: Update dimension tables (SCD Type 2)
MERGE INTO dim_store AS target
USING (
    SELECT DISTINCT 
        s.store_id,
        st.store_name,
        st.region,
        st.address,
        st.store_type
    FROM raw_sales_staging s
    JOIN source_system.store_details st ON s.store_id = st.store_id
    WHERE s.batch_id = @batch_id
) AS source
ON target.store_id = source.store_id 
    AND target.is_current = 1
WHEN MATCHED 
    AND (target.store_name != source.store_name 
    OR target.region != source.region 
    OR target.store_type != source.store_type)
THEN
    UPDATE SET 
        is_current = 0,
        end_date = CURRENT_DATE
WHEN NOT MATCHED THEN
    INSERT (
        store_id, store_name, region, address, store_type,
        start_date, end_date, is_current
    )
    VALUES (
        source.store_id, source.store_name, source.region, 
        source.address, source.store_type,
        CURRENT_DATE, NULL, 1
    );

-- Similar SCD Type 2 updates for product and customer dimensions

-- 2b. Transform: Clean and standardize data with error handling
WITH validated_sales AS (
    SELECT 
        t.transaction_id,
        t.store_id,
        t.product_id,
        t.customer_id,
        t.sale_date,
        t.quantity,
        t.unit_price,
        t.is_valid,
        t.validation_errors
    FROM raw_sales_staging t
    WHERE t.batch_id = @batch_id
)
INSERT INTO cleaned_sales_staging
SELECT 
    t.transaction_id,
    COALESCE(s.store_sk, -1) as store_sk,
    COALESCE(p.product_sk, -1) as product_sk,
    COALESCE(c.customer_sk, -1) as customer_sk,
    CAST(t.sale_date AS DATE) as sale_date,
    t.quantity,
    t.unit_price,
    t.quantity * t.unit_price as total_amount,
    t.is_valid,
    t.validation_errors,
    @batch_id as batch_id
FROM validated_sales t
LEFT JOIN dim_store s 
    ON t.store_id = s.store_id 
    AND s.is_current = 1
LEFT JOIN dim_product p 
    ON t.product_id = p.product_id 
    AND p.is_current = 1
LEFT JOIN dim_customer c 
    ON t.customer_id = c.customer_id 
    AND c.is_current = 1;

-- 3. Load: Partition fact table by date
INSERT INTO daily_sales_facts
PARTITION(sale_date)
SELECT 
    sale_date,
    store_sk,
    product_sk,
    customer_sk,
    SUM(CASE WHEN is_valid = 1 THEN quantity ELSE 0 END) as total_quantity,
    SUM(CASE WHEN is_valid = 1 THEN total_amount ELSE 0 END) as total_sales,
    COUNT(DISTINCT transaction_id) as transaction_count,
    SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as error_count,
    @batch_id as batch_id
FROM cleaned_sales_staging
WHERE batch_id = @batch_id
GROUP BY sale_date, store_sk, product_sk, customer_sk;

-- 4. Aggregate: Create materialized views for reporting
CREATE MATERIALIZED VIEW sales_metrics_mart
REFRESH ON DEMAND
AS
SELECT 
    store_sk,
    product_sk,
    DATE_TRUNC('month', sale_date) as sales_month,
    SUM(total_quantity) as monthly_quantity,
    SUM(total_sales) as monthly_sales,
    SUM(transaction_count) as monthly_transactions,
    SUM(error_count) as monthly_errors
FROM daily_sales_facts
GROUP BY 
    store_sk, 
    product_sk, 
    DATE_TRUNC('month', sale_date);

CREATE MATERIALIZED VIEW store_performance_mart
REFRESH ON DEMAND
AS
SELECT 
    s.store_sk,
    s.store_name,
    s.region,
    DATE_TRUNC('month', f.sale_date) as sales_month,
    COUNT(DISTINCT f.customer_sk) as unique_customers,
    SUM(f.total_sales) as total_sales,
    SUM(f.total_sales) / NULLIF(COUNT(DISTINCT f.customer_sk), 0) as avg_sales_per_customer,
    SUM(f.transaction_count) as total_transactions,
    SUM(f.error_count) as total_errors
FROM daily_sales_facts f
JOIN dim_store s 
    ON f.store_sk = s.store_sk 
    AND s.is_current = 1
GROUP BY 
    s.store_sk, 
    s.store_name, 
    s.region, 
    DATE_TRUNC('month', f.sale_date);

-- 5. Data Quality: Track pipeline metrics
INSERT INTO etl_metrics
SELECT
    @batch_id as batch_id,
    'sales_load' as pipeline_name,
    CURRENT_TIMESTAMP as run_timestamp,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as valid_records,
    SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as error_records,
    MIN(sale_date) as min_sale_date,
    MAX(sale_date) as max_sale_date
FROM raw_sales_staging
WHERE batch_id = @batch_id; 