{{ config(materialized='view') }}

SELECT
    invoice,
    stock_code,
    COALESCE(description, 'unknown') AS description,
    quantity,
    invoice_date,
    price,
    customer_id,
    country,
    source_sheet,
    is_canceled,
    gross_revenue,
    invoice_year,
    invoice_month,
    invoice_year_month,
    {{ get_period_label('invoice_year', 'invoice_month') }} AS period
FROM {{ source('silver', 'online_retail_ii') }}
