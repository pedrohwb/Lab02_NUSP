{{ config(materialized='table', schema='gold') }}

SELECT
    s.invoice,
    dp.product_id,
    dc.customer_key,
    dco.country_id,
    dd.date_id,
    s.quantity,
    s.price,
    s.gross_revenue,
    s.is_canceled,
    s.source_sheet,
    s.period
FROM {{ ref('stg_sales') }} AS s
LEFT JOIN {{ ref('dim_product') }}  AS dp  ON s.stock_code = dp.stock_code
                                           AND s.description = dp.description
LEFT JOIN {{ ref('dim_customer') }}  AS dc  ON s.customer_id = dc.customer_id
LEFT JOIN {{ ref('dim_country') }}   AS dco ON s.country      = dco.country_name
LEFT JOIN {{ ref('dim_date') }}      AS dd  ON s.invoice_date::date = dd.full_date
