WITH
    funcionarios AS (
        SELECT
            id_funcionario
            , id_gerente
            , funcionario
            , func_data_nascimento
            , func_data_contratacao
            , func_endereco
            , func_cidade
            , func_regiao
            , func_cep
            , func_pais
            , func_notas
        FROM {{ ref('stg_erp__funcionarios') }}
    )

    , self_join_gerentes AS (
        SELECT
             funcionarios.id_funcionario
            , funcionarios.id_gerente
            , funcionarios.funcionario
            , gerentes.funcionario AS gerente
            , funcionarios.func_data_nascimento
            , funcionarios.func_data_contratacao
            , funcionarios.func_endereco
            , funcionarios.func_cidade
            , funcionarios.func_regiao
            , funcionarios.func_cep
            , funcionarios.func_pais
            , funcionarios.func_notas
        FROM funcionarios
        LEFT JOIN funcionarios AS gerentes
            ON gerentes.id_funcionario = funcionarios.id_gerente
    )

    , transformacoes AS (
        SELECT
            row_number() OVER (ORDER BY id_funcionario) AS sk_funcionario
            , *
        FROM self_join_gerentes
    )

    SELECT
        *
    FROM transformacoes