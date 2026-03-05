{{ config(tags=['aggregations', 'transparencia', 'contratos']) }}

-- Grain: 1 row per (nome_orgao_superior × ano_contrato × modalidade_compra)
-- Powers the Contratos Federais dashboard tab-by-ministry view

select
    nome_orgao_superior,
    codigo_orgao_superior,
    ano_contrato,
    modalidade_compra,
    cast(count(*)                         as bigint) as num_contratos,
    cast(count(distinct cnpj_contratado)  as bigint) as num_fornecedores,
    cast(sum(valor_inicial)               as double) as total_contratado,
    cast(avg(valor_inicial)               as double) as media_valor_contrato,
    cast(max(valor_inicial)               as double) as max_valor_contrato
from {{ ref('fct_contrato_federal') }}
where valor_inicial is not null
  and ano_contrato  is not null
group by 1, 2, 3, 4
