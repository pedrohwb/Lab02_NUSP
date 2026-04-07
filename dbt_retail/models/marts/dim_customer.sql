{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id NULLS LAST) AS customer_key,
    customer_id
FROM (
    SELECT DISTINCT customer_id
    FROM {{ ref('stg_sales') }}
) AS distinct_customers
