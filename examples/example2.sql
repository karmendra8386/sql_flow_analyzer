-- Step 1: Create a logging table
CREATE TABLE IF NOT EXISTS etl_log (
    log_id SERIAL PRIMARY KEY,
    process_name TEXT,
    status TEXT,
    message TEXT,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Create the stored procedure
CREATE OR REPLACE PROCEDURE run_etl_pipeline()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    -- Log start
    INSERT INTO etl_log (process_name, status, message)
    VALUES ('ETL Pipeline', 'STARTED', 'ETL process started');

    -- Step 3: Extract
    WITH extracted_customers AS (
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            created_at
        FROM source_db.customers
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    ),

    -- Step 4: Transform
    cleaned_customers AS (
        SELECT 
            customer_id,
            UPPER(TRIM(first_name)) AS first_name,
            UPPER(TRIM(last_name)) AS last_name,
            LOWER(email) AS email,
            created_at
        FROM extracted_customers
    ),

    deduplicated_customers AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn
            FROM cleaned_customers
        ) sub
        WHERE rn = 1
    ),

    -- Step 5: Join
    joined_data AS (
        SELECT 
            o.order_id,
            o.customer_id,
            c.first_name,
            c.last_name,
            o.order_total,
            o.order_date
        FROM staging.orders o
        JOIN deduplicated_customers c ON o.customer_id = c.customer_id
    )

    -- Step 6: Load
    INSERT INTO warehouse.customer_orders (
        order_id,
        customer_id,
        customer_name,
        order_total,
        order_date
    )
    SELECT 
        order_id,
        customer_id,
        CONCAT(first_name, ' ', last_name),
        order_total,
        order_date
    FROM joined_data;

    -- Log success
    INSERT INTO etl_log (process_name, status, message)
    VALUES ('ETL Pipeline', 'SUCCESS', 'ETL process completed successfully');

EXCEPTION
    WHEN OTHERS THEN
        -- Log error
        INSERT INTO etl_log (process_name, status, message)
        VALUES ('ETL Pipeline', 'FAILED', SQLERRM);
        RAISE;
END;
$$;

-- Step 7: Execute the procedure
CALL run_etl_pipeline();
