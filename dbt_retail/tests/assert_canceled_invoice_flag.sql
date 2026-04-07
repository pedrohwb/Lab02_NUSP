-- Singular test: Regra de negócio — notas fiscais cujo número começa com 'C'
-- devem obrigatoriamente ter a flag is_canceled = TRUE.
-- Qualquer linha retornada aqui indica inconsistência entre o número da nota
-- e a flag de cancelamento.
SELECT *
FROM {{ ref('stg_sales') }}
WHERE invoice LIKE 'C%'
  AND is_canceled = FALSE
