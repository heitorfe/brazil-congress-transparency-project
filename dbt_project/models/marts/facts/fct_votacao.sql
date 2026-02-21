{{
  config(
    materialized='incremental',
    unique_key=['codigo_sessao_votacao', 'codigo_parlamentar'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'votes']
  )
}}

-- Grain: 1 row per senator × voting session
-- Joins senator votes with session metadata and the senator dimension.
-- Party is taken from the vote record itself (reflects the party at time of vote,
-- not the current party stored in dim_senador).

with votos as (
    select * from {{ ref('stg_legis__votos') }}
),

votacoes as (
    select * from {{ ref('stg_legis__votacoes') }}

    {% if is_incremental() %}
    -- On incremental runs, only load sessions newer than the latest already loaded
    where data_sessao > (
        select coalesce(max(data_sessao), '2019-01-01'::date)
        from {{ this }}
    )
    {% endif %}
),

senadores as (
    select
        senador_id,
        nome_parlamentar,
        estado_sigla
    from {{ ref('dim_senador') }}
),

final as (
    select
        -- Keys
        vt.codigo_sessao_votacao,
        vt.codigo_parlamentar,
        vt.senador_id,

        -- Session context (denormalized for query convenience)
        vs.data_sessao,
        vs.identificacao                  as materia_identificacao,
        vs.sigla_materia,
        vs.numero_materia,
        vs.ano_materia,
        vs.ementa                         as materia_ementa,
        vs.descricao_votacao,
        vs.resultado_votacao,
        vs.sequencial_sessao,
        vs.votacao_secreta,
        vs.sigla_tipo_sessao,
        vs.total_votos_sim,
        vs.total_votos_nao,
        vs.total_votos_abstencao,

        -- Senator vote
        vt.sigla_voto,
        vt.descricao_voto,
        vt.voto_afirmativo,
        vt.voto_negativo,
        vt.ausente,
        vt.presente_sem_voto,

        -- Party at time of vote (historical — may differ from current party)
        vt.sigla_partido                  as partido_sigla_voto,

        -- Current senator info from dim (may be null if senator left office)
        s.nome_parlamentar                as nome_parlamentar_dim,
        s.estado_sigla                    as estado_sigla_dim

    from votos vt
    inner join votacoes vs using (codigo_sessao_votacao)
    left join  senadores s  using (senador_id)
)

select * from final
