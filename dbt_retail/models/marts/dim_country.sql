{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY country) AS country_id,
    country AS country_name
FROM (
    SELECT DISTINCT country
    FROM {{ ref('stg_sales') }}
) AS distinct_countries
