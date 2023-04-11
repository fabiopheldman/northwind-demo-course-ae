WITH
    produtos AS (
        SELECT *
        FROM {{ ref('stg_erp__produtos') }}
    )
    , categorias AS (
        SELECT *
        FROM {{ ref('stg_erp__categorias') }}
    )
    , fornecedores AS (
        SELECT *
        FROM {{ ref('stg_erp__fornecedores') }}
    )
    , join_tabelas AS (
        SELECT
            produtos.id_produto
            , produtos.id_fornecedor
            , produtos.id_categoria
            , produtos.nome_produto
            , produtos.quantidade_por_unidade
            , produtos.preco_por_unidade
            , produtos.unidades_em_estoque
            , produtos.unidades_por_ordem
            , produtos.nivel_reabastecimento
            , produtos.is_discontinuado
            , categorias.nome_categoria
            , categorias.descricao_categoria
            , fornecedores.nome_fornecedor
            , fornecedores.contato_fornecedor
            , fornecedores.endereco_fornecedor
            , fornecedores.cep_fornecedor
            , fornecedores.cidade_fornecedor
            , fornecedores.regiao_fornecedor
            , fornecedores.pais_fornecedor
        FROM produtos
        LEFT JOIN categorias
            ON produtos.id_categoria = categorias.id_categoria
        LEFT JOIN fornecedores
            ON produtos.id_fornecedor = fornecedores.id_fornecedor
    )
    , transformacoes AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY id_produto) AS sk_produtos
            , *
        FROM join_tabelas
    )
SELECT *
FROM transformacoes