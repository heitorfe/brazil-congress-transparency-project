{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per leadership position record (current snapshot — API provides no end date)
-- Source: stg_legis__liderancas (from /composicao/lideranca, 314 records)

with source as (
    select * from {{ ref('stg_legis__liderancas') }}
),

final as (
    select
        codigo,
        casa,
        sigla_tipo_unidade,
        descricao_tipo_unidade,
        senador_id,
        nome_parlamentar,
        data_designacao,
        sigla_tipo_lideranca,
        descricao_tipo_lideranca,
        codigo_partido,
        sigla_partido,
        nome_partido,
        sigla_partido_filiacao,
        nome_partido_filiacao
    from source
)

select * from final
