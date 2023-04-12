WITH
    clientes AS (
        SELECT *
        FROM {{ ref('stg_erp__clientes') }}
    )
    , transformacoes AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY id_cliente) AS sk_cliente
            , id_cliente
            , nome_cliente
            , empresa_cliente
            , endereco_cliente
            , cep_cliente
            , cidade_cliente
            , regiao_cliente
            , pais_cliente
        FROM clientes
    )
SELECT *
FROM transformacoes