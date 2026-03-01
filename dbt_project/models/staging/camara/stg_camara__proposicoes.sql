{{ config(materialized='view') }}

-- Grain: 1 row per proposicao_id × deputado_id_autor
-- Source: data/raw/camara_proposicoes.parquet
-- A proposal may have multiple authors; this staging model keeps one row
-- per (proposal, author) pair as extracted.

with source as (
    select * from read_parquet('../data/raw/camara_proposicoes.parquet')
),

renamed as (
    select
        trim(proposicao_id)                                         as proposicao_id,
        cast(deputado_id_autor as varchar)                          as deputado_id_autor,
        upper(trim(try_cast(sigla_tipo as varchar)))                as sigla_tipo,
        cast(cod_tipo as integer)                                   as cod_tipo,
        cast(numero as integer)                                     as numero,
        cast(ano as integer)                                        as ano,
        trim(try_cast(ementa as varchar))                           as ementa,
        -- Optional fields: Polars infers Null type when all-null in first records
        trim(try_cast(ementa_detalhada as varchar))                 as ementa_detalhada,
        trim(try_cast(keywords as varchar))                         as keywords,
        try_cast(left(try_cast(data_apresentacao as varchar), 10)
                 as date)                                           as data_apresentacao,
        upper(trim(try_cast(sigla_orgao_status as varchar)))        as sigla_orgao_status,
        trim(try_cast(regime_status as varchar))                    as regime_status,
        trim(try_cast(descricao_situacao as varchar))               as descricao_situacao,
        cast(cod_situacao as integer)                               as cod_situacao,
        trim(try_cast(apreciacao as varchar))                       as apreciacao,
        trim(try_cast(url_inteiro_teor as varchar))                 as url_inteiro_teor
    from source
    where proposicao_id is not null
      and proposicao_id != ''
)

select * from renamed
