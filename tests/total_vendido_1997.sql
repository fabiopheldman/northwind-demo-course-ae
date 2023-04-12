{{
    config(
        severity = 'error'
    )
}}
WITH
    vendas_em_1997 AS (
        SELECT sum(total_bruto) AS total_vendido
        FROM {{ ref('fct_vendas') }}
        WHERE data_do_pedido BETWEEN '1997-01-01' AND '1997-12-31'
    )
SELECT total_vendido
FROM vendas_em_1997
WHERE total_vendido NOT BETWEEN 658388.00 AND 658389.00