{{ config(materialized='view') }}

-- Grain: 1 row per leadership position record (current snapshot — no history in API)
-- Source: data/raw/liderancas.parquet (from /composicao/lideranca, 314 records)

with source as (
    select * from read_parquet('../data/raw/liderancas.parquet')
),

renamed as (
    select
        cast(codigo as integer)                    as codigo,
        upper(trim(casa))                          as casa,
        upper(trim(sigla_tipo_unidade_lideranca))  as sigla_tipo_unidade,
        trim(descricao_tipo_unidade)               as descricao_tipo_unidade,
        cast(codigo_parlamentar as varchar)        as senador_id,
        trim(nome_parlamentar)                     as nome_parlamentar,
        try_cast(data_designacao as date)          as data_designacao,
        upper(trim(sigla_tipo_lideranca))          as sigla_tipo_lideranca,
        trim(descricao_tipo_lideranca)             as descricao_tipo_lideranca,
        cast(codigo_partido as varchar)            as codigo_partido,
        upper(trim(sigla_partido))                 as sigla_partido,
        trim(nome_partido)                         as nome_partido,
        upper(trim(sigla_partido_filiacao))        as sigla_partido_filiacao,
        trim(nome_partido_filiacao)                as nome_partido_filiacao
    from source
    where codigo is not null
)

select * from renamed
