{{
  config(
    materialized='table',
    tags=['facts', 'expenses']
  )
}}

-- Grain: 1 row per senator (current snapshot — ADM API has no date dimension)
-- Source: stg_adm__auxilio_moradia
-- Note: no senator ID in ADM housing API — matched to dim_senador via nome_parlamentar
-- Both sides are uppercased in staging/mart to ensure case-insensitive matching

with auxilio as (
    select * from {{ ref('stg_adm__auxilio_moradia') }}
),

senadores as (
    select
        senador_id,
        upper(trim(nome_parlamentar))  as nome_parlamentar_upper,
        estado_sigla,
        partido_sigla
    from {{ ref('dim_senador') }}
),

final as (
    select
        a.nome_parlamentar,
        a.estado_eleito,
        a.partido_eleito,
        a.auxilio_moradia,
        a.imovel_funcional,
        s.senador_id
    from auxilio a
    left join senadores s
        on s.nome_parlamentar_upper = a.nome_parlamentar
)

select * from final
