{{
  config(
    materialized='table',
    tags=['aggregations', 'expenses']
  )
}}

-- Grain: 1 row per senator × year × month × expense category
-- Pre-aggregated for Streamlit dashboard performance — avoids scanning 100k+ rows at render time
-- Source: fct_ceaps

with ceaps as (
    select * from {{ ref('fct_ceaps') }}
),

final as (
    select
        senador_id,
        nome_senador,
        nome_parlamentar_dim,
        estado_sigla_dim,
        ano,
        mes,
        tipo_despesa,
        count(*)            as qtd_recibos,
        sum(valor_reembolsado) as total_reembolsado
    from ceaps
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from final
