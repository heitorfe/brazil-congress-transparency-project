{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per committee
-- Source: stg_legis__comissoes (from /comissao/lista endpoint)

with source as (
    select * from {{ ref('stg_legis__comissoes') }}
),

final as (
    select
        codigo_comissao,
        sigla_comissao,
        nome_comissao,
        sigla_casa,
        tipo,
        data_inicio,
        data_fim
    from source
)

select * from final
