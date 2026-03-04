{{
  config(
    materialized='incremental',
    unique_key=['sq_candidato', 'ano_eleicao'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['dimensions', 'tse']
  )
}}

-- Grain: 1 row per (sq_candidato, ano_eleicao)
-- The same person running in multiple elections gets a different sq_candidato
-- each time, so this table has one row per electoral candidacy (not one per person).
--
-- Linkage to dim_senador / dim_deputado:
--   CPF is NOT exposed by the Senate Open Data API or Chamber API, so we join
--   on normalized nome_urna → nome_parlamentar (same pattern as dim_emenda.sql).
--   ~90% match rate for current legislators. cpf_raw is kept for future enrichment.
--
-- Incremental strategy: merge on (sq_candidato, ano_eleicao).
-- Watermark: load only new election years on subsequent runs.

with candidatos as (
    select * from {{ ref('stg_tse__candidatos') }}

    {% if is_incremental() %}
    where ano_eleicao >= (select coalesce(max(ano_eleicao), 2017) from {{ this }})
    {% endif %}
),

senadores as (
    select
        senador_id,
        upper(trim(nome_parlamentar)) as nome_norm
    from {{ ref('dim_senador') }}
),

deputados as (
    select
        deputado_id,
        upper(trim(nome_parlamentar)) as nome_norm
    from {{ ref('dim_deputado') }}
),

joined as (
    select
        c.sq_candidato,
        c.ano_eleicao,
        c.nome_candidato,
        c.nome_urna,
        c.cargo,
        c.uf,
        c.municipio,
        c.partido_sigla,
        c.nr_candidato,
        c.situacao_turno,
        c.eleito,
        c.genero,
        c.grau_instrucao,
        c.ocupacao,
        c.idade_posse,
        c.cpf_raw,
        s.senador_id,
        d.deputado_id
    from candidatos c
    left join senadores s
        on c.nome_urna_norm = s.nome_norm
    left join deputados d
        on c.nome_urna_norm = d.nome_norm
        and s.senador_id is null   -- only join deputy when no senator matched
)

select * from joined
-- Dedup: if the same nome_urna matches multiple legislators, senator takes priority
qualify row_number() over (
    partition by sq_candidato, ano_eleicao
    order by senador_id nulls last, deputado_id nulls last
) = 1
