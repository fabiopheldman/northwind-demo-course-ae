WITH
    clientes AS (
        SELECT *
        FROM {{ ref('dim_clientes') }}
    )
    , funcionarios AS (
        SELECT *
        FROM {{ ref('dim_funcionarios') }}
    )
    , produtos AS (
        SELECT *
        FROM {{ ref('dim_produtos') }}
    )
    , pedido_item AS (
        SELECT *
        FROM {{ ref('int_vendas__pedidos_itens') }}
    )
    , joined_tabelas AS (
        SELECT
            pedido_item.id_pedido
            , clientes.sk_cliente AS fk_cliente
            , funcionarios.sk_funcionario AS fk_funcionario
            , produtos.sk_produtos AS fk_produto
            , pedido_item.id_transportadora
            , pedido_item.desconto_perc
            , pedido_item.preco_da_unidade
            , pedido_item.quantidade
            , pedido_item.frete
            , pedido_item.data_do_pedido
            , pedido_item.data_do_envio
            , pedido_item.data_requerida_entrega
            , pedido_item.destinatario
            , pedido_item.endereco_destinatario
            , pedido_item.cep_destinatario
            , pedido_item.cidade_destinatario
            , pedido_item.regiao_destinatario
            , pedido_item.pais_destinatario
            , clientes.nome_cliente
            , funcionarios.funcionario
            , funcionarios.gerente
            , produtos.nome_produto
            , produtos.nome_categoria
            , produtos.nome_fornecedor
            , produtos.is_discontinuado
        FROM pedido_item
        LEFT JOIN clientes ON pedido_item.id_cliente = clientes.id_cliente
        LEFT JOIN funcionarios ON pedido_item.id_funcionario = funcionarios.id_funcionario
        LEFT JOIN produtos ON pedido_item.id_produto = produtos.id_produto
    )
    , transformacoes AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['id_pedido', 'fk_produto']) }} AS sk_venda
            , *
            , preco_da_unidade * quantidade AS total_bruto
            , (1 - desconto_perc) * preco_da_unidade * quantidade AS total_liquido
            , CASE
                WHEN desconto_perc > 0 THEN TRUE
                WHEN desconto_perc = 0 THEN FALSE
                ELSE FALSE
            END AS eh_desconto
        FROM joined_tabelas
    )
SELECT *
FROM transformacoes