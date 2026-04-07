-- Singular test: Regra de negócio — vendas não canceladas não devem ter
-- receita bruta nula nem negativa (price > 0 é garantido pelo pipeline silver).
-- Qualquer linha retornada por esta query representa uma violação.
SELECT *
FROM {{ ref('fct_sales') }}
WHERE is_canceled = FALSE
  AND gross_revenue <= 0
