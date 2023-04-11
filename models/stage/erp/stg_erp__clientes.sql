WITH
    fonte_clientes AS (
        SELECT
            CAST(customer_id AS STRING) id_cliente
            , CAST(contact_name AS STRING) AS nome_cliente
            --, contact_title
            , CAST(company_name AS STRING) AS empresa_cliente
            , CAST(address AS STRING) AS endereco_cliente
            , CAST(postal_code AS STRING) AS cep_cliente
            , CAST(city AS STRING) AS cidade_cliente
            , CAST(region AS STRING) AS regiao_cliente
            , CAST(country AS STRING) AS pais_cliente
            --, fax
            --, phone
        FROM {{ source('erp', 'customers') }}
    )
SELECT 
    *
FROM fonte_clientes