{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_id,
    full_date,
    EXTRACT(YEAR    FROM full_date)::int AS year,
    EXTRACT(MONTH   FROM full_date)::int AS month,
    EXTRACT(DAY     FROM full_date)::int AS day,
    EXTRACT(QUARTER FROM full_date)::int AS quarter
FROM (
    SELECT DISTINCT invoice_date::date AS full_date
    FROM {{ ref('stg_sales') }}
) AS distinct_dates
