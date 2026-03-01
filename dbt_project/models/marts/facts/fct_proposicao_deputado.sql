{{
  config(
    materialized='table',
    tags=['facts']
  )
}}

-- Grain: 1 row per proposicao_id × deputado_id_autor
-- A proposal can be authored by multiple deputies; this fact keeps one row per
-- (proposal, author) pair as recorded during extraction.
-- Materialized as table (not incremental) since proposals are relatively few
-- and are refetched in full per extraction run.

with proposicoes as (
    select * from {{ ref('stg_camara__proposicoes') }}
),

deputados as (
    select deputado_id, nome_parlamentar, sigla_uf, sigla_partido
    from {{ ref('dim_deputado') }}
),

final as (
    select
        p.proposicao_id,
        p.deputado_id_autor,
        dep.nome_parlamentar                as nome_parlamentar_autor,
        dep.sigla_uf                        as sigla_uf_autor,
        dep.sigla_partido                   as sigla_partido_autor,
        p.sigla_tipo,
        p.numero,
        p.ano,
        p.ementa,
        p.keywords,
        p.data_apresentacao,
        p.sigla_orgao_status,
        p.regime_status,
        p.descricao_situacao,
        p.apreciacao,
        p.url_inteiro_teor
    from proposicoes p
    left join deputados dep
        on p.deputado_id_autor = dep.deputado_id
)

select * from final
