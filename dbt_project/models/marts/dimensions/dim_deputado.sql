{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per unique deputy across legislatures 56 and 57
-- Sources:
--   stg_camara__deputados      — biographical detail (ultimoStatus)
--   stg_camara__deputados_lista — which legislatures each deputy served
-- Note: a deputy may have served in both legislature 56 and 57; the
--   legislatura_min/max columns capture that range without creating duplicate rows.

with deputados as (
    select * from {{ ref('stg_camara__deputados') }}
),

lista as (
    select
        deputado_id,
        min(id_legislatura) as legislatura_min,
        max(id_legislatura) as legislatura_max,
        -- Flag: appears in more than one legislature
        count(distinct id_legislatura) > 1 as multi_legislatura
    from {{ ref('stg_camara__deputados_lista') }}
    group by deputado_id
),

final as (
    select
        d.deputado_id,
        d.nome_civil,
        d.nome_parlamentar,
        d.nome_eleitoral,
        d.sigla_partido,
        d.sigla_uf,
        d.url_foto,
        d.email,
        d.situacao,
        d.condicao_eleitoral,
        d.data_status,
        d.sexo,
        d.data_nascimento,
        d.uf_nascimento,
        d.municipio_nascimento,
        d.escolaridade,
        d.telefone_gabinete,
        -- Legislature membership (from the list table)
        coalesce(l.legislatura_min, d.id_legislatura)   as legislatura_min,
        coalesce(l.legislatura_max, d.id_legislatura)   as legislatura_max,
        coalesce(l.multi_legislatura, false)             as multi_legislatura,
        -- em_exercicio: currently in office
        case
            when d.situacao in ('Exercício', 'Licença Parlamentar') then true
            else false
        end                                              as em_exercicio
    from deputados d
    left join lista l using (deputado_id)
)

select * from final
