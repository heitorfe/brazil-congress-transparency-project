{{ config(materialized='view') }}

-- Grain: 1 row per plenary voting session (votacao_id)
-- Source: data/raw/camara_votacoes.parquet
-- The aprovacao field: 1 = approved, 0 = rejected (may be null for procedural votes).

with source as (
    select * from read_parquet('../data/raw/camara_votacoes.parquet')
),

renamed as (
    select
        trim(votacao_id)                                as votacao_id,
        -- "data" is quoted because it is a reserved keyword in some SQL dialects
        try_cast(left("data", 10) as date)              as data_votacao,
        try_cast(data_hora_registro as timestamp)       as data_hora_registro,
        upper(trim(sigla_orgao))                        as sigla_orgao,
        trim(uri_evento)                                as uri_evento,
        trim(proposicao_objeto)                         as proposicao_objeto,
        trim(uri_proposicao)                            as uri_proposicao,
        trim(descricao)                                 as descricao,
        cast(aprovacao as integer)                      as aprovacao
    from source
    where votacao_id is not null
      and votacao_id != ''
)

select * from renamed
