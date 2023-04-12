WITH
    pedidos AS (
        SELECT *
        FROM {{ ref('stg_erp__ordens') }}
    )
    , pedido_itens AS (
        SELECT *
        FROM {{ ref('stg_erp__ordem_detalhes') }}
    )
    , joined_pedidos_itens AS (
        SELECT
            pedidos.id_pedido
            , pedidos.id_funcionario
            , pedidos.id_cliente
            , pedidos.id_transportadora
            , pedido_itens.id_produto
            , pedido_itens.desconto_perc
            , pedido_itens.preco_da_unidade
            , pedido_itens.quantidade
            , pedidos.frete
            , pedidos.data_do_pedido
            , pedidos.data_do_envio
            , pedidos.data_requerida_entrega
            , pedidos.destinatario
            , pedidos.endereco_destinatario
            , pedidos.cep_destinatario
            , pedidos.cidade_destinatario
            , pedidos.regiao_destinatario
            , pedidos.pais_destinatario
        FROM pedido_itens
        LEFT JOIN pedidos
            ON pedido_itens.id_pedido = pedidos.id_pedido
    )
SELECT *
FROM joined_pedidos_itens