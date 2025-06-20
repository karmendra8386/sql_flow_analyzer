WITH extracted_customers AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        created_at
    FROM 
        source_db.customers
    WHERE 
        created_at >= CURRENT_DATE - INTERVAL '1 day'
),
cleaned_customers AS (
    SELECT 
        customer_id,
        UPPER(TRIM(first_name)) AS first_name,
        UPPER(TRIM(last_name)) AS last_name,
        LOWER(email) AS email,
        created_at
    FROM 
        extracted_customers
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
joined_data AS (
    SELECT 
        o.order_id,
        o.customer_id,
        c.first_name,
        c.last_name,
        o.order_total,
        o.order_date
    FROM 
        staging.orders o
    JOIN 
        deduplicated_customers c ON o.customer_id = c.customer_id
)
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
    CONCAT(first_name, ' ', last_name) AS customer_name,
    order_total,
    order_date
FROM 
    joined_data;
