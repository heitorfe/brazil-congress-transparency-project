{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per legislative proposal
-- Source: stg_legis__processos (from /processo for PL, PEC, PLP, MPV from 2019)
-- Join key: dim_processo.id_processo = stg_legis__votacoes.id_processo

with source as (
    select * from {{ ref('stg_legis__processos') }}
),

final as (
    select
        id_processo,
        codigo_materia,
        identificacao,
        sigla_materia,
        numero_materia,
        ano_materia,
        ementa,
        tipo_documento,
        data_apresentacao,
        autoria,
        casa_identificadora,
        tramitando,
        data_ultima_atualizacao,
        url_documento
    from source
)

select * from final
