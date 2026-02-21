{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per political party registered at TSE
-- Source: seeds/partidos.csv (TSE official registry)
-- Enriched with senate presence count from stg_legis__senadores

select
    p.partido_sigla,
    p.partido_nome,
    p.partido_numero_tse,
    count(distinct s.senador_id)      as num_senadores,
    count(distinct s.senador_id) > 0  as tem_senador_em_exercicio
from {{ ref('partidos') }} p
left join {{ ref('stg_legis__senadores') }} s
    on s.partido_sigla = p.partido_sigla
group by 1, 2, 3
order by p.partido_sigla
