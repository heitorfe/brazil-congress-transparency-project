{{ config(materialized='view') }}

-- Grain: 1 row per committee
-- Source: data/raw/comissoes.parquet (extracted from /comissao/lista)
-- Responsibility: type casting and normalization only

with source as (
    select * from read_parquet('../data/raw/comissoes.parquet')
),

renamed as (
    select
        cast(codigo_comissao as varchar)   as codigo_comissao,
        upper(trim(sigla_comissao))        as sigla_comissao,
        trim(nome_comissao)                as nome_comissao,
        upper(trim(sigla_casa))            as sigla_casa,
        trim(tipo)                         as tipo,
        try_cast(data_inicio as date)      as data_inicio,
        try_cast(data_fim as date)         as data_fim
    from source
    where codigo_comissao is not null
)

select * from renamed
