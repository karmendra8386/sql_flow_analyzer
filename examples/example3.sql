-- Step 1: Create audit and error log tables
CREATE TABLE IF NOT EXISTS etl_audit (
    audit_id SERIAL PRIMARY KEY,
    process_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT,
    rows_inserted INT,
    rows_updated INT,
    error_message TEXT
);

-- Step 2: Create the stored procedure
CREATE OR REPLACE PROCEDURE run_advanced_etl(p_run_date DATE DEFAULT CURRENT_DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_rows_inserted INT := 0;
    v_rows_updated INT := 0;
    v_error_message TEXT := NULL;
BEGIN
    -- Begin transaction
    BEGIN

        -- Step 3: Extract and transform
        WITH extracted AS (
            SELECT 
                customer_id,
                UPPER(TRIM(first_name)) AS first_name,
                UPPER(TRIM(last_name)) AS last_name,
                LOWER(email) AS email,
                created_at
            FROM source_db.customers
            WHERE created_at::DATE = p_run_date
        ),
        deduplicated AS (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn
                FROM extracted
            ) sub
            WHERE rn = 1
        ),
        joined_orders AS (
            SELECT 
                o.order_id,
                o.customer_id,
                c.first_name,
                c.last_name,
                o.order_total,
                o.order_date
            FROM staging.orders o
            JOIN deduplicated c ON o.customer_id = c.customer_id
            WHERE o.order_date::DATE = p_run_date
        )

        -- Step 4: Upsert into target table
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
        FROM joined_orders
        ON CONFLICT (order_id) DO UPDATE
        SET 
            customer_id = EXCLUDED.customer_id,
            customer_name = EXCLUDED.customer_name,
            order_total = EXCLUDED.order_total,
            order_date = EXCLUDED.order_date;

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

        -- Step 5: Audit success
        v_end_time := clock_timestamp();
        INSERT INTO etl_audit (
            process_name, start_time, end_time, status, rows_inserted, rows_updated
        )
        VALUES (
            'Advanced ETL', v_start_time, v_end_time, 'SUCCESS', v_rows_inserted, v_rows_updated
        );

    EXCEPTION WHEN OTHERS THEN
        -- Rollback and log error
        v_end_time := clock_timestamp();
        v_error_message := SQLERRM;

        INSERT INTO etl_audit (
            process_name, start_time, end_time, status, error_message
        )
        VALUES (
            'Advanced ETL', v_start_time, v_end_time, 'FAILED', v_error_message
        );

        RAISE;
    END;
END;
$$;

-- Step 6: Execute the procedure
CALL run_advanced_etl(CURRENT_DATE - INTERVAL '1 day');
