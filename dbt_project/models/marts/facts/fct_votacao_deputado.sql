{{
  config(
    materialized='incremental',
    unique_key=['votacao_id', 'deputado_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'votes']
  )
}}

-- Grain: 1 row per deputy × voting session
-- Mirrors fct_votacao (Senate) for the Chamber side.
-- Party is taken from the vote record itself (reflects party at time of vote,
-- not the current party stored in dim_deputado).

with votos as (
    select * from {{ ref('stg_camara__votos') }}
),

votacoes as (
    select * from {{ ref('stg_camara__votacoes') }}

    {% if is_incremental() %}
    where data_votacao > (
        select coalesce(max(data_votacao), '2019-01-01'::date)
        from {{ this }}
    )
    {% endif %}
),

deputados as (
    select deputado_id, nome_parlamentar, sigla_uf
    from {{ ref('dim_deputado') }}
),

final as (
    select
        -- Keys
        vt.votacao_id,
        vt.deputado_id,

        -- Session context (denormalized for query convenience)
        vs.data_votacao,
        vs.sigla_orgao,
        vs.proposicao_objeto,
        vs.descricao,
        vs.aprovacao,

        -- Deputy vote
        vt.tipo_voto,
        vt.voto_afirmativo,
        vt.voto_negativo,
        vt.presente_sem_voto,
        vt.data_registro,

        -- Party at time of vote (historical — may differ from current party)
        vt.sigla_partido                as partido_sigla_voto,
        vt.sigla_uf                     as uf_voto,

        -- Current deputy info from dim (may be null if deputy left office)
        d.nome_parlamentar              as nome_parlamentar_dim,
        d.sigla_uf                      as sigla_uf_dim

    from votos vt
    inner join votacoes vs using (votacao_id)
    left join  deputados d  using (deputado_id)
)

select * from final
