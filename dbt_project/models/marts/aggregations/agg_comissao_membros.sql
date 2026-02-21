{{
  config(
    materialized='table',
    tags=['aggregations']
  )
}}

-- Grain: 1 row per committee (current members only)
-- Pre-aggregated committee membership counts for dashboard cards and filters
-- Source: dim_membro_comissao filtered to is_current = true

with membros as (
    select * from {{ ref('dim_membro_comissao') }}
    where is_current = true
),

final as (
    select
        codigo_comissao,
        sigla_comissao,
        nome_comissao,
        sigla_casa,
        count(distinct senador_id)                                               as num_membros_atuais,
        count(case when descricao_participacao = 'Titular' then 1 end)           as num_titulares,
        count(case when descricao_participacao = 'Suplente' then 1 end)          as num_suplentes
    from membros
    group by 1, 2, 3, 4
)

select * from final
