WITH
    fonte_ordens AS (
        SELECT
            CAST(order_id AS INT) id_pedido
            , CAST(employee_id AS INT) AS id_funcionario
            , CAST(customer_id AS STRING) AS id_cliente
            , CAST(ship_via AS INT) AS id_transportadora
            , CAST(order_date AS DATE) AS data_do_pedido
            , CAST(freight AS NUMERIC) AS frete
            , CAST(ship_name AS STRING) AS destinatario
            , CAST(ship_address AS STRING) AS endereco_destinatario
            , CAST(ship_postal_code AS STRING) AS cep_destinatario
            , CAST(ship_city AS STRING) AS cidade_destinatario
            , CAST(ship_region AS STRING) AS regiao_destinatario
            , CAST(ship_country AS STRING) AS pais_destinatario
            , CAST(shipped_date AS DATE) AS data_do_envio
            , CAST(required_date AS DATE) AS data_requerida_entrega
        FROM {{ source('erp', 'orders') }}
    )
SELECT *
FROM fonte_ordens