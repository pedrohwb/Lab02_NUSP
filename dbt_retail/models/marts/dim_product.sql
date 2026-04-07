{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY stock_code, description) AS product_id,
    stock_code,
    description
FROM (
    SELECT DISTINCT
        stock_code,
        description
    FROM {{ ref('stg_sales') }}
) AS distinct_products
