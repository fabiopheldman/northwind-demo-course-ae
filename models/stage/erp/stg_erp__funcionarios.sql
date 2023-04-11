WITH
    fonte_funcionarios AS (
        SELECT
            *
        FROM {{ source('erp', 'employees') }}
    )

    , renomear AS (
        SELECT
            CAST(employee_id as INT) AS id_funcionario
            , CAST(reports_to AS INT) AS id_gerente
            , CAST((first_name || ' ' || last_name) AS STRING) AS funcionario
            , CAST(birth_date AS DATE) AS func_data_nascimento
            , CAST(hire_date AS DATE) AS func_data_contratacao
            , CAST(address AS STRING) AS func_endereco
            , CAST(city	AS STRING) AS func_cidade
            , CAST(region AS STRING) AS func_regiao
            , CAST(postal_code AS STRING) AS func_cep
            , CAST(country AS STRING) AS func_pais
            , CAST(notes AS STRING) AS func_notas

        FROM fonte_funcionarios
    )

SELECT
    *
FROM renomear