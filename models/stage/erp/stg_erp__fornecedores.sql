WITH
    fonte_fornecedores AS (
        SELECT
            CAST(supplier_id AS INT) AS id_fornecedor
            , CAST(company_name AS STRING) AS nome_fornecedor
            , CAST(contact_name AS STRING) AS contato_fornecedor
            --, contact_title
            , CAST(address AS STRING) AS endereco_fornecedor
            , CAST(postal_code AS STRING) AS cep_fornecedor
            , CAST(city AS STRING) AS cidade_fornecedor
            , CAST(region AS STRING) AS regiao_fornecedor
            , CAST(country AS STRING) AS pais_fornecedor
            --, phone
            --, fax
            --, homepage
        FROM {{ source('erp', 'suppliers') }}
    )
SELECT 
    *
FROM fonte_fornecedores