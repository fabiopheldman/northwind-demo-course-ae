WITH
    fonte_ordem_detalhes AS (
        SELECT
            CAST(order_id AS INT) AS id_pedido
            , CAST(product_id AS INT) AS id_produto
            , CAST(discount AS NUMERIC) AS desconto_perc
            , CAST(unit_price AS NUMERIC) AS preco_da_unidade
            , CAST(quantity AS INT) AS quantidade
        FROM {{ source('erp', 'order_details') }}
    )
SELECT *
FROM fonte_ordem_detalhes